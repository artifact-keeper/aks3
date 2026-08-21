#!/usr/bin/env bash
#
# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only
#
# Runs the allowlisted subset of ceph/s3-tests against a freshly built aks3.
#
# The store, the config and the server all live for the length of this script
# and nothing else: a compliance run that inherited state from the last one
# would report on the wrong thing. Everything is torn down on exit, including
# the server, whether the suite passed or not.
#
# The suite itself is cloned beside this script (and gitignored) rather than
# vendored, pinned to S3_TESTS_REF so an upstream rename cannot turn a green CI
# into a red one overnight. Bumping the pin is a deliberate commit that comes
# with whatever allowlist edits the new revision needs.
#
# Its Python dependencies come from whatever index pip is configured with; set
# PIP_INDEX_URL to point somewhere else.

set -euo pipefail

# Upstream revision the allowlist's node IDs are written against. Upstream
# renamed the test package (s3tests_boto3 -> s3tests) in September 2025, so the
# IDs are only meaningful next to a known revision.
S3_TESTS_REPO="https://github.com/ceph/s3-tests.git"
S3_TESTS_REF="5522d1c351f75bc00ae0f64f742f3f095f5939d9"

# Port the server under test listens on. Substituted into the config template,
# so the two cannot drift.
PORT=19000

cd "$(dirname "$0")"
ROOT="$(git rev-parse --show-toplevel)"

DATA="$(mktemp -d)"
SERVER_LOG="$DATA/aks3.log"
SERVER_PID=""

# Both are set before anything can fail, so the trap never fires on an unset
# variable. The `wait` is what keeps the shell from printing its own
# "Terminated" line over the test output once the server is reaped.
# shellcheck disable=SC2329  # invoked by the EXIT trap below
cleanup() {
  if [ -n "$SERVER_PID" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$DATA"
}
trap cleanup EXIT

cargo build --release --manifest-path "$ROOT/Cargo.toml" -p aks3-server

cat > "$DATA/aks3.toml" <<EOF
listen = "127.0.0.1:$PORT"
data_dir = "$DATA/store"
root_access_key = "s3testsroot"
root_secret_key = "s3testsrootsecret"
EOF

# The server logs a line per rejected request, and the allowlist is mostly
# rejection paths, so its output would bury pytest's. It goes to a file and is
# only shown when the suite fails, which is the only time it says anything.
"$ROOT/target/release/aks3" --config "$DATA/aks3.toml" > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!

# Readiness is a TCP connect, not a successful request: an unsigned GET / is
# answered with 403, which is all the proof needed that the server is up.
ready=""
for _ in $(seq 1 50); do
  if curl -s -o /dev/null "http://127.0.0.1:$PORT"; then
    ready=1
    break
  fi
  sleep 0.2
done
if [ -z "$ready" ]; then
  echo "aks3 did not start listening on 127.0.0.1:$PORT; server log follows" >&2
  cat "$SERVER_LOG" >&2
  exit 1
fi

if [ -d s3-tests/.git ]; then
  git -C s3-tests fetch --depth 1 origin "$S3_TESTS_REF"
else
  rm -rf s3-tests
  git init -q s3-tests
  git -C s3-tests remote add origin "$S3_TESTS_REPO"
  git -C s3-tests fetch --depth 1 origin "$S3_TESTS_REF"
fi
git -C s3-tests checkout -q FETCH_HEAD

sed "s/@PORT@/$PORT/" s3tests.conf.in > "$DATA/s3tests.conf"

# The allowlist is a file of node IDs with '#' comments; pytest wants them as
# separate arguments. Read rather than word-split so a stray space in a line
# cannot silently become two arguments. The `|| [ -n "$line" ]` keeps a final
# line with no newline after it, which would otherwise drop off the gate
# without a word.
nodes=()
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in
    ''|'#'*) continue ;;
  esac
  nodes+=("$line")
done < allowlist.txt
if [ "${#nodes[@]}" -eq 0 ]; then
  echo "allowlist.txt names no tests" >&2
  exit 1
fi

cd s3-tests
python3 -m venv .venv
# shellcheck disable=SC1091  # created just above, so shellcheck cannot see it
. .venv/bin/activate
pip install -q -r requirements.txt
pip install -q -e .

status=0
S3TEST_CONF="$DATA/s3tests.conf" python -m pytest -q --no-header -p no:cacheprovider "${nodes[@]}" || status=$?
if [ "$status" -ne 0 ]; then
  echo "--- aks3 server log ---" >&2
  cat "$SERVER_LOG" >&2
fi
exit "$status"

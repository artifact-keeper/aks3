#!/usr/bin/env bash
#
# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only
#
# Runs ceph/s3-tests against a freshly built aks3, in one of two modes.
#
# By default it runs the allowlisted subset and gates on it: every listed test
# must run and pass. With --full it runs the whole suite instead and reports
# rather than gates, which is what the nightly recon does to find tests that
# have started passing without anyone noticing. Both modes share this file
# rather than a copy of it, so the thing the recon measures cannot drift from
# the thing the gate enforces.
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
# Its Python dependencies come from the hash-pinned requirements.txt beside
# this script, off whatever index pip is configured with; set PIP_INDEX_URL to
# point somewhere else.
#
# Usage:
#
#   run-s3-tests.sh [--full] [--junit PATH] [TARGET...]
#
#   --full         run the whole suite, non-gating on its results.
#   --junit PATH   also write the JUnit report there. Everything else this
#                  script makes is deleted on exit.
#   TARGET         with --full, run these pytest targets instead of everything.
#                  For proving the pipeline works without waiting for the whole
#                  suite; the nightly passes none.

set -euo pipefail

# Upstream revision the allowlist's node IDs are written against. Upstream
# renamed the test package (s3tests_boto3 -> s3tests) in September 2025, so the
# IDs are only meaningful next to a known revision.
#
# requirements.in mirrors this revision's own dependency list, so moving the pin
# means re-reading that list and recompiling requirements.txt in the same
# commit.
S3_TESTS_REPO="https://github.com/ceph/s3-tests.git"
S3_TESTS_REF="5522d1c351f75bc00ae0f64f742f3f095f5939d9"

# Port the server under test listens on. Substituted into the config template,
# so the two cannot drift.
PORT=19000

# Per-test limit in --full mode. The suite is written against servers that
# implement everything it asks for, and a request aks3 answers in a way some
# fixture loops on would otherwise spend the job's whole hour in one test.
# Comfortably above what any test here legitimately takes.
FULL_TEST_TIMEOUT=120

FULL=""
JUNIT_OUT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --full) FULL=1; shift ;;
    --junit) JUNIT_OUT="$2"; shift 2 ;;
    -*) echo "run-s3-tests.sh: unknown argument '$1'" >&2; exit 2 ;;
    *) break ;;
  esac
done

TARGETS=("$@")
if [ "${#TARGETS[@]}" -gt 0 ] && [ -z "$FULL" ]; then
  echo "run-s3-tests.sh: pytest targets only mean something with --full; the gate runs allowlist.txt" >&2
  exit 2
fi

# Resolved against the caller's directory, before the cd below moves out of it.
case "$JUNIT_OUT" in
  ''|/*) ;;
  *) JUNIT_OUT="$PWD/$JUNIT_OUT" ;;
esac

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

# Something answering on the port is not proof that it is *our* server: a stale
# aks3 from an interrupted run, or any other local service on this port, would
# take the connection while the process just built exits on a bind error. A
# suite that went green against that would be reporting on someone else's
# binary. So the child has to still be running for a connection to count.
#
# The caller says what it was doing, because in --full mode this is asked again
# after the suite has run, where a dead server means something else entirely.
# shellcheck disable=SC2329  # invoked below and again around the suite
require_server_alive() {
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "aks3 is not running ($1)" >&2
    echo "--- aks3 server log ---" >&2
    cat "$SERVER_LOG" >&2
    exit 1
  fi
}

# Readiness is a TCP connect, not a successful request: an unsigned GET / is
# answered with 403, which is all the proof needed that the server is up.
ready=""
for _ in $(seq 1 50); do
  require_server_alive "exited while waiting for it to listen; is 127.0.0.1:$PORT already in use?"
  if curl -s -o /dev/null "http://127.0.0.1:$PORT"; then
    ready=1
    break
  fi
  sleep 0.2
done
if [ -z "$ready" ]; then
  echo "aks3 did not start listening on 127.0.0.1:$PORT within 10s; log follows" >&2
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

COMPLIANCE_DIR="$PWD"
LOCKFILE="$COMPLIANCE_DIR/requirements.txt"

cd s3-tests
python3 -m venv .venv
# shellcheck disable=SC1091  # created just above, so shellcheck cannot see it
. .venv/bin/activate

# The lockfile is compiled from requirements.in, which mirrors the pinned
# revision's own requirements.txt, so this installs the same dependency set the
# suite asks for at versions that only move when a commit moves them. Hashes
# put --require-hashes in play, which then covers every transitive package too.
#
# s3tests itself is installed with neither its dependencies nor a build
# environment of its own: everything setup.py needs, setuptools included, is
# already here and pinned, and letting pip fetch a build backend would put an
# unhashed download back in the middle of a hash-pinned install.
#
# The fallback is for a bumped S3_TESTS_REF whose lockfile has not been
# recompiled yet, and says so loudly rather than quietly installing whatever
# the index offers today.
if [ -f "$LOCKFILE" ]; then
  pip install -q --require-hashes -r "$LOCKFILE"
  pip install -q -e . --no-deps --no-build-isolation
else
  echo "warning: no lockfile at $LOCKFILE; installing unpinned dependencies" >&2
  pip install -q -r requirements.txt
  pip install -q -e .
fi

# The check in the readiness loop can race a server that is still on its way
# out: it answers one connection, then exits on a bind error. This one cannot,
# because the clone and the pip install stand between the two, and it doubles
# as a guard against the server having died during setup.
require_server_alive "exited during setup; is 127.0.0.1:$PORT already in use?"

JUNIT="$DATA/pytest.xml"
status=0
if [ -n "$FULL" ]; then
  # With no targets pytest collects the whole suite from its rootdir, which is
  # what the nightly wants.
  #
  # --continue-on-collection-errors because a module that cannot even be
  # imported is a fact worth having in the report rather than a reason to
  # abandon the other thousand tests, and xunit1 because that family keeps the
  # per-test `file` attribute the delta rebuilds node IDs from; xunit2, the
  # default, drops it.
  #
  # recon_isolation sits next to this script, hence PYTHONPATH; the docstring
  # in it explains what a full run measures without it, which is nothing.
  S3TEST_CONF="$DATA/s3tests.conf" PYTHONPATH="$COMPLIANCE_DIR" \
    python -m pytest -q --no-header -p no:cacheprovider \
    -p recon_isolation \
    --continue-on-collection-errors \
    --timeout="$FULL_TEST_TIMEOUT" \
    -o junit_family=xunit1 \
    --junitxml="$JUNIT" ${TARGETS[@]+"${TARGETS[@]}"} || status=$?
else
  S3TEST_CONF="$DATA/s3tests.conf" python -m pytest -q --no-header -p no:cacheprovider \
    --junitxml="$JUNIT" "${nodes[@]}" || status=$?
fi

if [ -n "$JUNIT_OUT" ] && [ -f "$JUNIT" ]; then
  cp "$JUNIT" "$JUNIT_OUT"
fi

# In --full mode the suite's own result is not a verdict on anything: hundreds
# of these tests are meant to fail against a Phase 0 server, and which ones is
# the report, not the exit code. What still has to hold is that the run was
# real, so the two infrastructure facts are checked and everything else is
# handed to the caller.
if [ -n "$FULL" ]; then
  if [ ! -f "$JUNIT" ]; then
    echo "pytest wrote no report to $JUNIT; it did not get as far as running the suite" >&2
    echo "--- aks3 server log ---" >&2
    cat "$SERVER_LOG" >&2
    exit 1
  fi
  # A server that died partway through turns every later test into a failure
  # for a reason that has nothing to do with the test, which would read as a
  # cliff in the delta rather than as the crash it is.
  require_server_alive "after the suite"
  exit 0
fi

# pytest's exit status is not the gate on its own. A run whose tests all
# skipped exits 0, and a skip is exactly what a missing feature looks like from
# the suite's side: a fixture gives up, the test never runs, and CI goes green
# over an allowlist that proved nothing. A node ID that no longer resolves after
# an upstream rename is the same hazard from the other direction. So the gate is
# the count: every allowlisted node has to have run and passed.
#
# The counts come from the JUnit report rather than the "N passed in Ms" line
# because they are attributes there, not a sentence whose wording and pluralisation
# move between pytest releases.
#
# Reads one attribute off `suite_tag`, the opening <testsuite ...> tag set
# below. Working from that tag rather than from the whole file is what keeps an
# assertion message that happens to contain `tests="3"` from being read as a
# count; the enclosing <testsuites> carries no attributes of its own.
count_of() {
  printf '%s\n' "$suite_tag" | tr ' ' '\n' | sed -n "s/^$1=\"\([0-9]*\)\"\$/\1/p"
}

if [ ! -f "$JUNIT" ]; then
  echo "pytest wrote no report to $JUNIT; it did not get as far as running the suite" >&2
  status=1
else
  suite_tag="$(tr '\n' ' ' < "$JUNIT" | sed -n 's/.*<testsuite \([^>]*\)>.*/\1/p')"
  ran="$(count_of tests)"
  skipped="$(count_of skipped)"
  failures="$(count_of failures)"
  errors="$(count_of errors)"
  for count in "$ran" "$skipped" "$failures" "$errors"; do
    case "$count" in
      ''|*[!0-9]*)
        echo "could not read the test counts out of $JUNIT" >&2
        exit 1
        ;;
    esac
  done

  passed=$((ran - skipped - failures - errors))
  if [ "$passed" -ne "${#nodes[@]}" ]; then
    echo "compliance gate: allowlist names ${#nodes[@]} tests, $passed passed" >&2
    echo "($ran ran, $skipped skipped, $failures failed, $errors errored)" >&2
    echo "Every allowlisted test must run and pass; a skip is not a pass." >&2
    status=1
  fi
fi

if [ "$status" -ne 0 ]; then
  echo "--- aks3 server log ---" >&2
  cat "$SERVER_LOG" >&2
fi
exit "$status"

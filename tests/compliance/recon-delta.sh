#!/usr/bin/env bash
#
# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only
#
# Turns a full-suite ceph/s3-tests run into the difference between what passes
# and what the allowlist claims.
#
# The allowlist is the compliance roadmap, and its risk is rot in both
# directions. A test that started passing because some other change happened to
# implement what it wanted stays invisible, so the list understates the server;
# a test on the list that stopped passing would be caught by the pull-request
# gate, but only if the two runs still agree about which tests exist. Diffing
# the whole suite against the list once a night is what keeps it a ratchet.
#
# Reads a JUnit report and allowlist.txt, writes a markdown body for the
# tracking issue, a machine-readable delta, and KEY=VALUE lines on stdout for
# $GITHUB_OUTPUT:
#
#   passing, collected, promotions, regressions, fingerprint
#
# Nothing here talks to GitHub or to the network, so it runs standalone against
# a fabricated report, which is how its own behaviour is checked.
#
# Usage:
#
#   recon-delta.sh --junit FILE --allowlist FILE --suite-root DIR \
#     --body FILE [--delta FILE]

set -euo pipefail

JUNIT=""
ALLOWLIST=""
SUITE_ROOT=""
BODY=""
DELTA=""

while [ $# -gt 0 ]; do
  case "$1" in
    --junit) JUNIT="$2"; shift 2 ;;
    --allowlist) ALLOWLIST="$2"; shift 2 ;;
    --suite-root) SUITE_ROOT="$2"; shift 2 ;;
    --body) BODY="$2"; shift 2 ;;
    --delta) DELTA="$2"; shift 2 ;;
    *) echo "recon-delta.sh: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

if [ -z "$JUNIT" ] || [ -z "$ALLOWLIST" ] || [ -z "$SUITE_ROOT" ] || [ -z "$BODY" ]; then
  echo "recon-delta.sh: --junit, --allowlist, --suite-root and --body are all required" >&2
  exit 2
fi
if [ ! -f "$JUNIT" ]; then
  echo "recon-delta.sh: no JUnit report at $JUNIT" >&2
  exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# At most this many node IDs are listed in the issue body. The full lists are
# always in the delta file, which the workflow uploads as an artifact; an issue
# body long enough to need scrolling past is one nobody reads to the end of.
BODY_LIST_CAP=100

# The JUnit report names a test by a dotted classname and a bare name, and what
# the allowlist speaks is pytest node IDs. Rebuilding one from the other is only
# ambiguous over where the module path stops and a class begins, and the `file`
# attribute settles it outright, which is why the run asks for the xunit1
# family; the xunit2 default drops that attribute, and a report written without
# it falls back to the longest dotted prefix that is a file on disk.
#
# Aggregated per node ID rather than per element, because pytest emits one
# element per phase and an s3-tests teardown that cannot empty its bucket is a
# separate element from the call that passed. A node counts as passing only if
# nothing it emitted failed, errored or skipped: the same skip-is-not-a-pass
# rule the gate applies, and the reason this count sits well below the one
# pytest prints.
SUITE_ROOT="$SUITE_ROOT" JUNIT="$JUNIT" python3 - > "$WORK/status.tsv" <<'PY'
import os
import sys
import xml.etree.ElementTree as ET

root_dir = os.environ["SUITE_ROOT"]
report = os.environ["JUNIT"]

try:
    tree = ET.parse(report)
except ET.ParseError as exc:
    sys.exit(f"could not parse {report}: {exc}")


def node_id(classname, name, filename):
    if not classname:
        return name
    parts = classname.split(".")
    if filename and filename.endswith(".py"):
        module = filename[:-3].split("/")
        if parts[: len(module)] == module:
            return "::".join([filename] + parts[len(module) :] + [name])
    for stop in range(len(parts), 0, -1):
        candidate = os.path.join(root_dir, *parts[:stop]) + ".py"
        if os.path.isfile(candidate):
            found = "/".join(parts[:stop]) + ".py"
            return "::".join([found] + parts[stop:] + [name])
    # Nothing matched, so the classname does not describe a module this suite
    # collected from disk. Emitting the dotted form unchanged keeps it visible
    # in the delta rather than silently reshaping it into a node ID that
    # resolves to nothing.
    return classname + "::" + name


bad = {"failure", "error", "skipped"}
seen = {}
for case in tree.iter("testcase"):
    ident = node_id(
        case.get("classname", ""), case.get("name", ""), case.get("file", "")
    )
    clean = not any(child.tag in bad for child in case)
    seen[ident] = seen.get(ident, True) and clean

for ident, clean in seen.items():
    print(f"{'pass' if clean else 'other'}\t{ident}")
PY

awk -F'\t' '$1 == "pass" { print $2 }' "$WORK/status.tsv" | LC_ALL=C sort -u > "$WORK/passing.txt"
LC_ALL=C sort -u "$WORK/status.tsv" | wc -l | tr -d ' ' > "$WORK/collected.txt"

# Same parsing rule as run-s3-tests.sh: '#' comments and blank lines out, one
# node ID per line, no word splitting.
: > "$WORK/allowed.txt"
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in
    ''|'#'*) continue ;;
  esac
  printf '%s\n' "$line" >> "$WORK/allowed.txt"
done < "$ALLOWLIST"
LC_ALL=C sort -u "$WORK/allowed.txt" -o "$WORK/allowed.txt"

LC_ALL=C comm -23 "$WORK/passing.txt" "$WORK/allowed.txt" > "$WORK/promotions.txt"
LC_ALL=C comm -13 "$WORK/passing.txt" "$WORK/allowed.txt" > "$WORK/regressions.txt"

passing=$(wc -l < "$WORK/passing.txt" | tr -d ' ')
collected=$(cat "$WORK/collected.txt")
promotions=$(wc -l < "$WORK/promotions.txt" | tr -d ' ')
regressions=$(wc -l < "$WORK/regressions.txt" | tr -d ' ')

# GNU coreutils on the runner, BSD on a maintainer's laptop, and the digest has
# to be the same on both or a local check of this script would say nothing
# about what CI computes.
digest() {
  if command -v sha256sum > /dev/null 2>&1; then
    sha256sum
  else
    shasum -a 256
  fi
}

# Over the delta only. The number of tests the suite collected, how long it
# took and which run produced it all move on their own, and an issue rewritten
# nightly for any of those is an issue with a muted thread.
fingerprint=$({
  echo "promotions"
  cat "$WORK/promotions.txt"
  echo "regressions"
  cat "$WORK/regressions.txt"
} | digest | cut -c1-16)

if [ -n "$DELTA" ]; then
  {
    echo "# Full s3-tests recon delta against tests/compliance/allowlist.txt."
    echo "# collected=${collected} passing=${passing} promotions=${promotions} regressions=${regressions}"
    echo
    echo "[promotion candidates: passing, not on the allowlist]"
    cat "$WORK/promotions.txt"
    echo
    echo "[regressions: on the allowlist, not passing]"
    cat "$WORK/regressions.txt"
  } > "$DELTA"
fi

# Prints a fenced block of at most BODY_LIST_CAP entries, saying so when it
# leaves some out.
list_block() {
  local file="$1"
  local total
  total=$(wc -l < "$file" | tr -d ' ')
  echo '```'
  head -n "$BODY_LIST_CAP" "$file"
  echo '```'
  if [ "$total" -gt "$BODY_LIST_CAP" ]; then
    echo
    echo "Showing ${BODY_LIST_CAP} of ${total}. The whole list is the \`s3-tests-recon\` artifact on the run below."
  fi
}

{
  echo "The nightly recon ran the whole pinned ceph/s3-tests suite against \`main\`. Of the ${collected} tests in the report, ${passing} passed outright."
  echo
  echo "Outright means nothing the test emitted failed, errored or skipped, so a test whose assertions held and whose teardown then could not empty its bucket counts here the same as one that failed. That is the bar \`allowlist.txt\` is held to, and it is why this number is well below the passed count pytest prints."
  echo

  if [ "$regressions" -gt 0 ]; then
    echo "## Regressions"
    echo
    echo "These ${regressions} test(s) are on \`tests/compliance/allowlist.txt\` and did not pass here. The pull-request gate runs the same list, so this should not be reachable: either something merged without the gate, or the two runs disagree about what a node ID resolves to. The recon job fails on this."
    echo
    list_block "$WORK/regressions.txt"
    echo
  fi

  if [ "$promotions" -gt 0 ]; then
    echo "## Promotion candidates"
    echo
    echo "These ${promotions} test(s) passed and are not on the allowlist. Adding one to \`tests/compliance/allowlist.txt\` turns it into a gate, which is the point: the list is the ratchet, and anything that passes and is left off it can quietly stop passing."
    echo
    echo "Passing once is not the bar. A test that only passes because it ends without leaving a bucket behind, or one that happened to race well tonight, will start flaking the moment it becomes a gate, so promote in batches and let the pull-request run confirm each one."
    echo
    list_block "$WORK/promotions.txt"
    echo
  fi

  echo "This body is rewritten by the nightly workflow whenever the delta changes, so notes are better left as comments. Last run: ${RUN_URL:-not run from CI}"
} > "$BODY"

{
  echo "passing=${passing}"
  echo "collected=${collected}"
  echo "promotions=${promotions}"
  echo "regressions=${regressions}"
  echo "fingerprint=${fingerprint}"
}

#!/usr/bin/env bash
#
# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only
#
# Checks recon-delta.sh against fabricated JUnit reports.
#
# The nightly's tracking issue is only as good as this arithmetic, and the run
# that produces the real input takes the better part of an hour, so the cases
# that matter are the ones a real run mostly does not contain: an empty delta,
# a regression, a test that passed its call and then failed to clean up, a
# report with no `file` attributes at all. Each takes milliseconds here.
#
# Needs nothing but python3 and the script beside it. Run it directly.

set -euo pipefail

cd "$(dirname "$0")"
DELTA_SCRIPT="$PWD/recon-delta.sh"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

failures=0

# A suite tree to resolve node IDs against, for the case where the report
# carries no `file` attribute and the module path has to be found on disk.
mkdir -p "$WORK/suite/s3tests/functional"
touch "$WORK/suite/s3tests/functional/test_s3.py"

# Writes a JUnit report. Every argument is `status:classname:name[:file]`,
# where status is pass, failure, error or skipped. Repeating a classname and
# name emits a second element for the same test, which is how pytest reports a
# test whose call and teardown disagree.
write_report() {
  local path="$1"
  shift
  {
    echo '<?xml version="1.0" encoding="utf-8"?>'
    echo '<testsuites><testsuite name="pytest">'
    local spec status classname name file
    for spec in "$@"; do
      IFS=: read -r status classname name file <<< "$spec"
      printf '<testcase classname="%s" name="%s"' "$classname" "$name"
      if [ -n "${file:-}" ]; then
        printf ' file="%s"' "$file"
      fi
      if [ "$status" = pass ]; then
        echo '/>'
      else
        printf '><%s message="x">detail</%s></testcase>\n' "$status" "$status"
      fi
    done
    echo '</testsuite></testsuites>'
  } > "$path"
}

write_allowlist() {
  local path="$1"
  shift
  {
    echo "# a comment, and a blank line below"
    echo
    printf '%s\n' "$@"
  } > "$path"
}

# Runs the script and checks the KEY=VALUE lines it prints. `expected` is the
# subset that has to match; anything else it prints is ignored.
check() {
  local name="$1" report="$2" allowlist="$3"
  shift 3
  local out pair key want got
  if ! out=$("$DELTA_SCRIPT" --junit "$report" --allowlist "$allowlist" \
    --suite-root "$WORK/suite" --body "$WORK/body.md" --delta "$WORK/delta.txt"); then
    echo "FAIL ${name}: the script exited nonzero"
    failures=$((failures + 1))
    return
  fi
  for pair in "$@"; do
    key="${pair%%=*}"
    want="${pair#*=}"
    got=$(printf '%s\n' "$out" | sed -n "s/^${key}=//p")
    if [ "$got" != "$want" ]; then
      echo "FAIL ${name}: ${key} is '${got}', expected '${want}'"
      failures=$((failures + 1))
      return
    fi
  done
  echo "ok   ${name}"
}

MOD=s3tests.functional.test_s3
FILE=s3tests/functional/test_s3.py

# Everything the allowlist names passes, and nothing else does: the state the
# tracking issue should be closed in.
write_report "$WORK/empty.xml" \
  "pass:$MOD:test_a:$FILE" \
  "pass:$MOD:test_b:$FILE" \
  "failure:$MOD:test_c:$FILE"
write_allowlist "$WORK/both.txt" "$FILE::test_a" "$FILE::test_b"
check "empty delta" "$WORK/empty.xml" "$WORK/both.txt" \
  passing=2 promotions=0 regressions=0

# The same report against a shorter allowlist: one promotion candidate.
write_allowlist "$WORK/one.txt" "$FILE::test_a"
check "promotion candidate" "$WORK/empty.xml" "$WORK/one.txt" \
  passing=2 promotions=1 regressions=0

# A test on the allowlist that did not pass. Should be unreachable, since the
# gate runs the same list on every pull request, which is exactly why the
# nightly has to be loud about it.
write_allowlist "$WORK/three.txt" "$FILE::test_a" "$FILE::test_b" "$FILE::test_c"
check "regression" "$WORK/empty.xml" "$WORK/three.txt" \
  passing=2 promotions=0 regressions=1

# The case a plain pass count gets wrong: pytest reports the call and the
# teardown as separate elements, and an s3-tests test that cannot clean up
# after itself is not a test that can go on the allowlist.
write_report "$WORK/teardown.xml" \
  "pass:$MOD:test_a:$FILE" \
  "error:$MOD:test_a:$FILE"
write_allowlist "$WORK/none.txt" "# nothing"
check "teardown error is not a pass" "$WORK/teardown.xml" "$WORK/none.txt" \
  passing=0 promotions=0 regressions=0

# A skip is not a pass either, the same rule the gate applies.
write_report "$WORK/skipped.xml" "skipped:$MOD:test_a:$FILE"
check "skip is not a pass" "$WORK/skipped.xml" "$WORK/none.txt" \
  passing=0 promotions=0

# Class-based tests: the module path comes from the `file` attribute and
# whatever the classname carries beyond it is the class.
write_report "$WORK/class.xml" "pass:${MOD}.TestThing:test_a:$FILE"
write_allowlist "$WORK/class.txt" "$FILE::TestThing::test_a"
check "class node ID" "$WORK/class.xml" "$WORK/class.txt" \
  passing=1 promotions=0 regressions=0

# No `file` attribute, which is what a report written by the xunit2 family
# looks like: the module path is found on disk instead.
write_report "$WORK/nofile.xml" "pass:$MOD:test_a"
check "node ID without a file attribute" "$WORK/nofile.xml" "$WORK/one.txt" \
  passing=1 promotions=0 regressions=0

# The fingerprint tracks the delta and nothing else, so a night that finds the
# same delta among a different number of tests leaves the issue alone.
write_report "$WORK/more.xml" \
  "pass:$MOD:test_a:$FILE" \
  "pass:$MOD:test_b:$FILE" \
  "failure:$MOD:test_c:$FILE" \
  "failure:$MOD:test_d:$FILE"
first=$("$DELTA_SCRIPT" --junit "$WORK/empty.xml" --allowlist "$WORK/one.txt" \
  --suite-root "$WORK/suite" --body "$WORK/body.md" | sed -n 's/^fingerprint=//p')
second=$("$DELTA_SCRIPT" --junit "$WORK/more.xml" --allowlist "$WORK/one.txt" \
  --suite-root "$WORK/suite" --body "$WORK/body.md" | sed -n 's/^fingerprint=//p')
if [ "$first" = "$second" ]; then
  echo "ok   fingerprint ignores everything but the delta"
else
  echo "FAIL fingerprint changed when only the surrounding counts did"
  failures=$((failures + 1))
fi

# And moves when the delta does.
third=$("$DELTA_SCRIPT" --junit "$WORK/empty.xml" --allowlist "$WORK/both.txt" \
  --suite-root "$WORK/suite" --body "$WORK/body.md" | sed -n 's/^fingerprint=//p')
if [ "$first" != "$third" ]; then
  echo "ok   fingerprint moves with the delta"
else
  echo "FAIL fingerprint did not change when the delta did"
  failures=$((failures + 1))
fi

# A long list is truncated in the body and whole in the delta file, so the
# issue stays readable without the artifact stopping being the record.
many=()
for i in $(seq 1 150); do
  many+=("pass:$MOD:test_${i}:$FILE")
done
write_report "$WORK/many.xml" "${many[@]}"
check "large delta" "$WORK/many.xml" "$WORK/none.txt" promotions=150
if grep -q "Showing 100 of 150" "$WORK/body.md"; then
  echo "ok   body says what it left out"
else
  echo "FAIL body did not say it truncated the list"
  failures=$((failures + 1))
fi
if [ "$(grep -c '::test_' "$WORK/delta.txt")" -eq 150 ]; then
  echo "ok   delta file carries the whole list"
else
  echo "FAIL delta file is not complete"
  failures=$((failures + 1))
fi

# The floor. A run that collected only what the allowlist names produces an
# empty delta, which is indistinguishable from a healthy night and would close
# the tracking issue on the strength of a suite that never ran.
write_report "$WORK/short.xml" \
  "pass:$MOD:test_a:$FILE" \
  "pass:$MOD:test_b:$FILE"
if "$DELTA_SCRIPT" --junit "$WORK/short.xml" --allowlist "$WORK/both.txt" \
  --suite-root "$WORK/suite" --body "$WORK/body.md" --min-collected 900 \
  > "$WORK/out" 2>&1; then
  echo "FAIL a collection that fell off a cliff was accepted"
  failures=$((failures + 1))
else
  echo "ok   floor rejects a truncated collection"
fi
# And it still says what it found, so the run is not silent about why.
if grep -q "collected=2" "$WORK/out"; then
  echo "ok   floor still reports the counts it rejected"
else
  echo "FAIL floor failed without saying what it counted"
  failures=$((failures + 1))
fi

# The same report is fine under a floor it clears, and fine with none at all.
check "floor that is met" "$WORK/short.xml" "$WORK/both.txt" collected=2
if "$DELTA_SCRIPT" --junit "$WORK/short.xml" --allowlist "$WORK/both.txt" \
  --suite-root "$WORK/suite" --body "$WORK/body.md" --min-collected 2 \
  > /dev/null 2>&1; then
  echo "ok   floor exactly met passes"
else
  echo "FAIL floor rejected a report that met it exactly"
  failures=$((failures + 1))
fi

# A report that is not XML at all is a broken run, not an empty delta.
echo "not xml" > "$WORK/broken.xml"
if "$DELTA_SCRIPT" --junit "$WORK/broken.xml" --allowlist "$WORK/one.txt" \
  --suite-root "$WORK/suite" --body "$WORK/body.md" > /dev/null 2>&1; then
  echo "FAIL an unparseable report was accepted"
  failures=$((failures + 1))
else
  echo "ok   unparseable report fails"
fi

if [ "$failures" -ne 0 ]; then
  echo "${failures} check(s) failed" >&2
  exit 1
fi
echo "all checks passed"

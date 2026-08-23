#!/usr/bin/env bash
#
# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only
#
# Keeps one GitHub issue matched to what a scheduled job currently finds.
#
# A scheduled job whose only output is a tick in the Actions tab is a job
# nobody reads, and one that files a fresh issue every run is a job everybody
# mutes. What is left is a single standing issue per job: opened on the first
# finding, rewritten when the finding set changes, left alone when it has not,
# closed when the findings are gone.
#
# "Has not changed" is a fingerprint the caller computes over its findings and
# passes in. It is written into the issue body as an HTML comment, so the next
# run can read back what the last one described without keeping state anywhere
# else.
#
# Usage:
#
#   tracking-issue.sh --title TITLE --body FILE --fingerprint HEX \
#     [--empty] [--close-comment TEXT] [--change-comment TEXT]
#
#   --empty          there is nothing to report: close the issue if it is open.
#                    --body is then unused and may be omitted.
#
# Needs GH_TOKEN in the environment, and REPO (defaulting to
# GITHUB_REPOSITORY) naming the repository to file against.

set -euo pipefail

TITLE=""
BODY=""
FINGERPRINT=""
EMPTY=""
CLOSE_COMMENT="This no longer finds anything to report. Closing; it will reopen as a new issue if that changes."
CHANGE_COMMENT="The set of findings changed. The description above has been rewritten to match the latest run."

while [ $# -gt 0 ]; do
  case "$1" in
    --title) TITLE="$2"; shift 2 ;;
    --body) BODY="$2"; shift 2 ;;
    --fingerprint) FINGERPRINT="$2"; shift 2 ;;
    --close-comment) CLOSE_COMMENT="$2"; shift 2 ;;
    --change-comment) CHANGE_COMMENT="$2"; shift 2 ;;
    --empty) EMPTY=1; shift ;;
    *) echo "tracking-issue.sh: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

REPO="${REPO:-${GITHUB_REPOSITORY:-}}"
if [ -z "$TITLE" ] || [ -z "$REPO" ]; then
  echo "tracking-issue.sh: --title and REPO are required" >&2
  exit 2
fi

# Exact title match rather than whatever the search ranked first, so an
# unrelated issue that happens to mention the same words is never mistaken for
# this one.
number=$(gh issue list --repo "$REPO" --state open --limit 100 \
  --search "in:title $TITLE" --json number,title \
  | jq -r --arg t "$TITLE" 'map(select(.title == $t)) | .[0].number // empty')

if [ -n "$EMPTY" ]; then
  if [ -n "$number" ]; then
    gh issue comment "$number" --repo "$REPO" --body "$CLOSE_COMMENT"
    gh issue close "$number" --repo "$REPO"
    echo "Closed #${number}: nothing left to report."
  else
    echo "Nothing to report, and no open issue to close."
  fi
  exit 0
fi

if [ -z "$BODY" ] || [ ! -f "$BODY" ] || [ -z "$FINGERPRINT" ]; then
  echo "tracking-issue.sh: --body must name a readable file and --fingerprint must be set" >&2
  exit 2
fi

# The marker is appended here rather than written by every caller, so the one
# place that reads it is the one place that writes it. A copy, because the
# caller usually also puts its body in the job summary and has no use for it
# there.
marked="$(mktemp)"
trap 'rm -f "$marked"' EXIT
cat "$BODY" > "$marked"
printf '\n<!-- findings-fingerprint: %s -->\n' "$FINGERPRINT" >> "$marked"

if [ -z "$number" ]; then
  url=$(gh issue create --repo "$REPO" --title "$TITLE" --body-file "$marked")
  echo "Opened ${url}"
  exit 0
fi

if gh issue view "$number" --repo "$REPO" --json body --jq .body \
  | grep -qF "findings-fingerprint: ${FINGERPRINT}"; then
  echo "#${number} already describes these findings; left alone."
  exit 0
fi

gh issue edit "$number" --repo "$REPO" --body-file "$marked"
gh issue comment "$number" --repo "$REPO" --body "$CHANGE_COMMENT"
echo "Updated #${number}."

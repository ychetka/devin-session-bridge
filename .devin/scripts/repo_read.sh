#!/usr/bin/env bash
# repo_read.sh - read repository metadata for other agents.
# Usage: repo_read.sh <pr|issue|checks|reviews|diff|releases|tags> <ref>
# Output: raw gh json/text under {data, status} envelope.
set -uo pipefail
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

WHAT="${1:?usage: repo_read.sh <pr|issue|checks|reviews|diff|releases|tags> <ref>}"
REF="${2:-}"

case "$WHAT" in
  pr|issue|checks|reviews|diff)
    [ -n "$REF" ] || { echo "ref required for $WHAT" >&2; exit 2; } ;;
esac

case "$WHAT" in
  pr)      gh pr view "$REF" --json number,title,body,state,headRefName,baseRefName,headRefOid,mergeable,mergeCommit,reviews,comments ;;
  issue)   gh issue view "$REF" --json number,title,body,state,labels,comments ;;
  checks)  gh pr view "$REF" --json statusCheckRollup ;;
  reviews) gh pr view "$REF" --json reviews,latestReviews,reviewRequests ;;
  diff)    gh pr diff "$REF" ;;
  # release/tag reads take no ref: `latest` narrows releases to the
  # newest one, anything else lists all
  releases)
    if [ "$REF" = "latest" ]; then
      gh release list --limit 1
    else
      gh release list
    fi ;;
  tags)
    git tag --list 'v*' --sort=-v:refname ;;
  *)       echo "unknown what: $WHAT" >&2; exit 2 ;;
esac

#!/usr/bin/env bash
# tools/check_changes_entry.sh - require a release-notes record per PR
# (issue #135).
#
# Every PR that changes files must carry ONE of:
#   1. a `.changes/<slug>.md` fragment in the diff (added, modified or
#      renamed - not `_template.md`, not `README.md`),
#   2. a `CHANGELOG.md` change - release-prep PRs fold fragments into
#      the changelog directly instead of carrying a pending file,
#   3. the `no-changes-entry` marker in the PR title, body, or a label
#      of the same name - the documented opt-out for pure docs/ops
#      changes that must not produce release notes.
#
# Modes:
#   --base <ref>   local/CI mode: inspect `git diff <ref>...HEAD`
#                  (three-dot when a merge-base exists, two-dot
#                  otherwise). Exemption text comes from the
#                  PR_TITLE / PR_BODY / PR_LABELS env vars.
#   --pr <number>  merge-gate mode: inspect the PR's file list and
#                  metadata via `gh api`/`gh pr view`.
#
# Output JSON: {"status":"pass","reason":...} on success; a FAIL line
# and exit 1 otherwise. See .changes/README.md for the convention.
set -uo pipefail

fail() { echo "FAIL: $1" >&2; REASON="$1" python3 -c \
  'import json,os;print(json.dumps({"status":"fail","reason":os.environ["REASON"]}))'; exit 1; }

MODE=""
BASE=""
PR=""
while [ $# -gt 0 ]; do
  case "$1" in
    --base) MODE="base"; BASE="${2:?--base needs a ref}"; shift 2 ;;
    --pr) MODE="pr"; PR="${2:?--pr needs a number}"; shift 2 ;;
    *) echo "usage: check_changes_entry.sh --base <ref> | --pr <n>" >&2; exit 2 ;;
  esac
done
[ -n "$MODE" ] || { echo "usage: check_changes_entry.sh --base <ref> | --pr <n>" >&2; exit 2; }

TOP=$(git rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

if [ "$MODE" = "base" ]; then
  if git merge-base "$BASE" HEAD >/dev/null 2>&1; then
    FILES=$(git diff --name-status "$BASE...HEAD" 2>/dev/null) \
      || fail "cannot diff $BASE...HEAD"
  else
    FILES=$(git diff --name-status "$BASE" HEAD 2>/dev/null) \
      || fail "cannot diff $BASE HEAD"
  fi
  META_TITLE="${PR_TITLE:-}"
  META_BODY="${PR_BODY:-}"
  META_LABELS="${PR_LABELS:-}"
else
  FILES=$(gh api "repos/{owner}/{repo}/pulls/$PR/files?per_page=100" --paginate \
            --jq '.[] | [.status, .filename] | @tsv' 2>/dev/null) \
    || fail "cannot list files for PR $PR"
  # normalise gh statuses to git name-status letters, preserving the
  # target path for renames
  FILES=$(printf '%s\n' "$FILES" | python3 -c '
import sys
for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) < 2:
        continue
    m = {"added": "A", "modified": "M", "removed": "D",
         "renamed": "R", "changed": "M", "copied": "A"}
    print(f"{m.get(parts[0], parts[0])}\t{parts[-1]}")
')
  PRMETA=$(gh pr view "$PR" --json title,body,labels 2>/dev/null) \
    || fail "cannot read metadata for PR $PR"
  META_TITLE=$(echo "$PRMETA" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("title") or "")')
  META_BODY=$(echo "$PRMETA" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("body") or "")')
  META_LABELS=$(echo "$PRMETA" | python3 -c 'import json,sys;print(" ".join(l["name"] for l in json.load(sys.stdin).get("labels") or []))')
fi

FILES="$FILES" META_TITLE="$META_TITLE" META_BODY="$META_BODY" \
META_LABELS="$META_LABELS" python3 - <<'PYEOF'
import os, re, sys

files = os.environ.get("FILES", "")
meta_title = os.environ.get("META_TITLE", "")
meta_body = os.environ.get("META_BODY", "")
meta_labels = os.environ.get("META_LABELS", "")

fragment = False
changelog = False
entries = []
for line in files.splitlines():
    parts = line.split("\t")
    if len(parts) < 2:
        continue
    status, path = parts[0], parts[-1]
    entries.append((status, path))
    if path.startswith(".changes/") and status[0] in ("A", "M", "R"):
        name = path.rsplit("/", 1)[-1]
        if name.endswith(".md") and not name.startswith("_") \
                and name != "README.md":
            fragment = True
    if path == "CHANGELOG.md" and status[0] in ("A", "M", "R"):
        changelog = True

if not entries:
    print('{"status":"pass","reason":"no file changes in diff"}')
    sys.exit(0)
if fragment:
    print('{"status":"pass","reason":"new .changes fragment in diff"}')
    sys.exit(0)
if changelog:
    print('{"status":"pass","reason":"CHANGELOG.md changed - folded release record"}')
    sys.exit(0)
# the marker must be a deliberate declaration: a whole line in the
# title or body, or an exact label name - a mention inside prose does
# not exempt
if re.search(r"(?im)^[ \t]*no-changes-entry[ \t]*$",
             meta_title + "\n" + meta_body) \
        or "no-changes-entry" in meta_labels.split():
    print('{"status":"pass","reason":"no-changes-entry marker present"}')
    sys.exit(0)

print(
    "FAIL: PR changes files but carries no release-notes record - add a "
    ".changes/<slug>.md fragment (user_visible: false is fine for "
    "tooling/chore), fold it into CHANGELOG.md on a release-prep "
    "branch, or opt out with the `no-changes-entry` marker in the PR "
    "title/body/label for pure docs/ops changes",
    file=sys.stderr,
)
print('{"status":"fail","reason":"missing_changes_entry"}')
sys.exit(1)
PYEOF

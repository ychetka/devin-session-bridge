#!/usr/bin/env bash
# verify_constraints.sh - mechanical pre-completion checks.
# PASS / FAIL (exit 1) / ESCALATE (protected paths need cited auth).
set -uo pipefail
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP" 2>/dev/null || { echo "FAIL: not a git repo"; exit 1; }

fails=0; escalates=0
fail()     { echo "FAIL: $1"; echo "  fix: $2"; fails=$((fails+1)); }
escalate() { echo "ESCALATE: $1"; echo "  required: $2"; escalates=$((escalates+1)); }
pass()     { echo "PASS: $1"; }

PJ="$DEVIN_DIR/project.json"

# 1. Branch name = issue_<issue_id>_v<base_version> (skip on base branch)
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ "$BRANCH" =~ ^issue_([0-9]+)_v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  pass "branch $BRANCH"
  ISSUE_N="${BASH_REMATCH[1]}"
elif [ "$BRANCH" = "main" ] || [ "$BRANCH" = "master" ]; then
  echo "SKIP: on base branch $BRANCH (no item branch)"
else
  fail "branch '$BRANCH' != issue_<N>_v<X.Y.Z>" \
       "rename via git-agent, or state the task is branchless"
fi

# 2. Issue exists and is linked from the open PR
if [ -n "${ISSUE_N:-}" ]; then
  # Branch zero-pads the number (issue_044) but GitHub numbers have no
  # leading zeros - normalize or every check below false-fails
  ISSUE_NUM=$((10#$ISSUE_N))
  # --json number: a plain `gh issue view` queries the deprecated
  # projectCards field and exits 1 on every issue (gh 2.46+), which
  # this check would misread as "not found"
  gh issue view "$ISSUE_NUM" --json number >/dev/null 2>&1 \
    && pass "issue #$ISSUE_NUM exists" \
    || fail "issue #$ISSUE_NUM not found" \
            "create it via repository-agent"
  PR_BODY=$(gh pr view --json body -q .body 2>/dev/null || echo "")
  if [ -n "$PR_BODY" ]; then
    echo "$PR_BODY" | grep -qiE "(closes|fixes|resolves|relates[[:space:]]+to)[[:space:]]+#$ISSUE_NUM" \
      && pass "PR links issue #$ISSUE_NUM" \
      || fail "PR body lacks 'Closes #$ISSUE_NUM' link" \
              "add the link via repository-agent"
  else
    echo "SKIP: no open PR yet - link check deferred"
  fi
fi

# 3. Hygiene over outbound text (commit subjects + diff vs base)
BASE=$(git merge-base HEAD origin/main 2>/dev/null || git merge-base HEAD main 2>/dev/null || echo "")
if [ -n "$BASE" ]; then
  "$DEVIN_DIR/scripts/hygiene_check.sh" --diff-base "$BASE" \
    && pass "hygiene" \
    || fail "hygiene violations in outbound text" "fix flagged lines"
else
  echo "SKIP: cannot resolve merge-base - hygiene deferred"
fi

# 4. Forbidden secrets/files = hard FAIL; protected_paths = ESCALATE
CHANGED=$( { git diff --name-only --cached; git diff --name-only HEAD; } 2>/dev/null | sort -u )
SECRETS=$(echo "$CHANGED" | grep -iE '\.(env|pem|key|pat)$|(^|/)(ENV|id_rsa)$|token' || true)
if [ -n "$SECRETS" ]; then
  fail "secret/sensitive files in change: $SECRETS" \
       "unstage immediately - secrets never enter the repository"
else
  pass "no secret files in change"
fi

if [ -f "$PJ" ]; then
  while IFS= read -r pp; do
    [ -z "$pp" ] && continue
    HIT=$(echo "$CHANGED" | grep -F "$pp" || true)
    [ -n "$HIT" ] && escalate "protected path modified: $HIT" \
      "cite the user instruction/task that authorized this change"
  done < <(python3 -c "import json;[print(p) for p in json.load(open('$PJ')).get('protected_paths',[])]" 2>/dev/null)
fi

# 5. No stray untracked files (mid-migration scratch docs are exempt;
#    true local-only .devin state is covered by .gitignore, so a new
#    untracked .devin canon file still surfaces here)
UNTRACKED=$(git status --porcelain | grep '^??' \
            | grep -vE 'scope_whitelist\.txt|migration_rollforward\.md' || true)
[ -z "$UNTRACKED" ] \
  && pass "tree clean (no untracked)" \
  || fail "untracked files present" "commit intended ones, remove leftovers"

# 6. No open FAILURE in cycle_log
if [ -f "$DEVIN_DIR/cycle_log.jsonl" ]; then
  OPEN_FAIL=$(grep '"status": *"open"' "$DEVIN_DIR/cycle_log.jsonl" | grep '"verdict": *"failure"' || true)
  [ -z "$OPEN_FAIL" ] \
    && pass "cycle_log has no open failures" \
    || fail "open failure events in cycle_log" "resolve or close them"
fi

# 7. Spec impact anchors resolve (issue #107): every pending
#    .changes entry must declare a Spec impact section whose
#    adds/modifies anchors exist in docs/specs/ and whose removes
#    anchors are gone (the PR applies its own spec delta).
if [ -x "$TOP/tools/check_spec_anchors.sh" ]; then
  if ls "$TOP"/.changes/*.md >/dev/null 2>&1; then
    "$TOP/tools/check_spec_anchors.sh" >/dev/null 2>&1 \
      && pass "spec anchors" \
      || fail "Spec impact anchors do not resolve" \
              "run tools/check_spec_anchors.sh and fix the listed refs"
  else
    echo "SKIP: no pending .changes entries"
  fi
else
  echo "SKIP: tools/check_spec_anchors.sh not present"
fi

echo "---"
[ $fails -gt 0 ] && { echo "VERIFY: $fails FAILURES, $escalates ESCALATIONS"; exit 1; }
[ $escalates -gt 0 ] && echo "VERIFY: PASS with $escalates ESCALATIONS (cite authorization in report)" || echo "VERIFY: ALL PASS"

#!/usr/bin/env bash
# repo_branch_delete.sh - delete an issue_<N>_v<X.Y.Z> branch locally
# and on the github remote. Base branches and non-convention names
# are refused unless --force is given (records the override reason).
# Usage: repo_branch_delete.sh <branch> [--remote-only|--local-only] [--force]
# Output JSON: {branch, deleted, status}
set -uo pipefail
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

BRANCH="${1:?usage: repo_branch_delete.sh <branch> [--remote-only|--local-only] [--force]}"
shift || true
SCOPE=both; FORCE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --remote-only) SCOPE=remote; shift ;;
    --local-only)  SCOPE=local; shift ;;
    --force)       FORCE=1; shift ;;
    *) echo '{"status":"error","failure_class":"malformed_input","message":"unknown arg"}'; exit 2 ;;
  esac
done

BASE=$(gh repo view --json defaultBranchRef -q .defaultBranchRef.name 2>/dev/null || echo main)
if [ "$BRANCH" = "$BASE" ] || [ "$BRANCH" = "main" ] || [ "$BRANCH" = "master" ]; then
  echo '{"status":"error","failure_class":"gate_blocked","message":"refusing to delete base branch"}'
  exit 2
fi
if [ "$FORCE" -eq 0 ] && [[ ! "$BRANCH" =~ ^issue_[0-9]+_v[0-9]+\.[0-9]+\.[0-9]+$|^i_[0-9]+$ ]]; then
  echo "{\"status\":\"error\",\"failure_class\":\"gate_blocked\",\"message\":\"branch '$BRANCH' is not an issue branch - pass --force to delete anyway\"}"
  exit 2
fi

REMOTE=$(git remote get-url origin >/dev/null 2>&1 && echo origin \
         || git remote -v | awk '$2 ~ /github\.com/ {print $1; exit}')
DELETED=()
if [ "$SCOPE" != "local" ] && [ -n "${REMOTE:-}" ]; then
  if git ls-remote --exit-code --heads "$REMOTE" "$BRANCH" >/dev/null 2>&1; then
    git push "$REMOTE" --delete "$BRANCH" >/dev/null 2>&1 && DELETED+=("remote")
  fi
fi
if [ "$SCOPE" != "remote" ] && git show-ref --verify --quiet "refs/heads/$BRANCH"; then
  git branch -D "$BRANCH" >/dev/null 2>&1 && DELETED+=("local")
fi

if [ ${#DELETED[@]} -eq 0 ]; then
  python3 -c 'import json,sys;print(json.dumps({"branch":sys.argv[1],"status":"not_found"}))' "$BRANCH"
  exit 1
fi
python3 -c 'import json,sys;print(json.dumps({"branch":sys.argv[1],"deleted":sys.argv[2].split(","),"status":"ok"}))' "$BRANCH" "$(IFS=,; echo "${DELETED[*]}")"

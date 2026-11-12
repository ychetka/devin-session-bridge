#!/usr/bin/env bash
# ship_push.sh - push wrapper. Verifies branch convention and clean
# state, pushes current branch with upstream tracking.
set -uo pipefail
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ ! "$BRANCH" =~ ^issue_[0-9]+_v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "PUSH BLOCKED: branch '$BRANCH' is not issue_<N>_v<X.Y.Z>" >&2
  exit 2
fi

DIRTY=$(git status --porcelain | grep -c . || true)
if [ "${DIRTY:-0}" -gt 0 ]; then
  echo "PUSH BLOCKED: uncommitted or untracked changes present" >&2
  exit 2
fi

# Remote: use `origin` when it exists, otherwise the first remote
# whose URL points at github.com (this repo names it `github`).
if git remote get-url origin >/dev/null 2>&1; then
  REMOTE=origin
else
  REMOTE=$(git remote -v | awk '$2 ~ /github\.com/ {print $1; exit}')
fi
if [ -z "${REMOTE:-}" ]; then
  echo "PUSH BLOCKED: no github remote configured" >&2
  exit 2
fi

git push -u "$REMOTE" "$BRANCH" "$@"

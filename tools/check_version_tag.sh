#!/usr/bin/env bash
# check_version_tag.sh - assert the current [project].version has a
# git tag on the remote. Runs after merges to the base branch so a
# version bump merged without a release cannot stay invisible (the
# 2026-11-12 version-tag-drift incident). Polls the remote for up to
# VERSION_TAG_TIMEOUT seconds (default 600) because the release step
# lands a few seconds after the merge push.
# Usage: check_version_tag.sh [timeout_seconds]
set -uo pipefail

TIMEOUT="${1:-${VERSION_TAG_TIMEOUT:-600}}"

VER=$(awk -F= '
  /^\[project\]/ {inp=1; next}
  /^\[/ {inp=0}
  inp && /^[[:space:]]*version[[:space:]]*=/ {
    v=$2; gsub(/["'"'"'[:space:]]/, "", v); print v; exit }' pyproject.toml 2>/dev/null)

if [[ ! "$VER" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "FAIL: cannot parse [project].version from pyproject.toml"
  exit 1
fi

TAG="v$VER"
echo "checking for remote tag $TAG (timeout ${TIMEOUT}s)"

elapsed=0
while [ "$elapsed" -lt "$TIMEOUT" ]; do
  git fetch --tags --quiet 2>/dev/null || true
  if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null 2>&1 \
     && git merge-base --is-ancestor "$TAG^{commit}" HEAD 2>/dev/null; then
    echo "PASS: tag $TAG exists on an ancestor of HEAD ($(git rev-parse --short "$TAG^{commit}"))"
    exit 0
  fi
  sleep 30
  elapsed=$((elapsed + 30))
done

echo "FAIL: pyproject version $VER has no remote tag $TAG after ${TIMEOUT}s"
echo "A merged version bump without a release is the version-tag drift"
echo "incident - tag $TAG on the merge commit and push it (repo_release.sh)."
exit 1

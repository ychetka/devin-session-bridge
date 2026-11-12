#!/usr/bin/env bash
# repo_release.sh - tag + GitHub release after a merged PR.
# Usage: repo_release.sh [patch|minor|major|auto] [--notes "<text>"]
# Release-prep step (issue #96): run `tools/release_notes.sh
# --version X.Y.Z --apply` on the release branch BEFORE tagging -
# it folds pending .changes/ entries into CHANGELOG.md so this
# script tags a head that already carries the changelog.
# Every pyproject version means exactly one tag: derives the tag from
# [project].version on the base-branch head, creates an ANNOTATED tag
# there, pushes it and creates a GitHub release (--notes text or
# --generate-notes when no notes given). Fails (not warns) on drift:
# version behind the newest tag, unparseable/missing pyproject, or the
# tag already existing on a different commit. The patch|minor|major
# argument is accepted for compatibility; the tag always follows
# pyproject, not latest_tag + 1 (the devin-telegram version-drift
# incident of 2026-11-12 - same fix ported here, issue #155).
# Output JSON: {status: released|error, tag?, from?, message?}
set -uo pipefail
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

PART="${1:-auto}"
case "$PART" in
  patch|minor|major|auto) ;;
  --notes) PART=auto ;;
  *) echo '{"status":"error","message":"part must be patch|minor|major|auto"}'; exit 2 ;;
esac

NOTES=""
while [ $# -gt 0 ]; do
  case "$1" in
    --notes) NOTES="${2:?--notes needs text}"; shift 2 ;;
    patch|minor|major|auto) shift ;;
    *) shift ;;
  esac
done

REMOTE=$(git remote get-url origin >/dev/null 2>&1 && echo origin \
         || git remote -v | awk '$2 ~ /github\.com/ {print $1; exit}')
[ -z "${REMOTE:-}" ] && { echo '{"status":"error","message":"no github remote"}' >&2; exit 1; }

BASE=$(gh repo view --json defaultBranchRef -q .defaultBranchRef.name 2>/dev/null || echo main)
if ! git fetch "$REMOTE" "$BASE" --tags >/dev/null 2>&1; then
  echo '{"status":"error","failure_class":"upstream_error","message":"fetch of base/tags failed - refusing to guess next version"}'
  exit 1
fi

LATEST=$(git tag --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-v:refname | head -n1)
LATEST=${LATEST:-v0.0.0}
if [[ ! "$LATEST" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "{\"status\":\"error\",\"failure_class\":\"upstream_error\",\"message\":\"cannot parse latest tag '$LATEST'\"}"
  exit 1
fi

SHA=$(git rev-parse "$REMOTE/$BASE" 2>/dev/null || git rev-parse "$BASE")

# The tag is derived from [project].version on the base head - it is
# the single source of truth. A version-bump PR carries its own bump;
# the release step only turns the merged version into a tag + Release.
# Deriving "latest_tag + 1" instead is what made the 2026-11-12
# version-drift incident in devin-telegram unrecoverable without
# manual tags (documented there as
# docs/incidents/2026-11-12-version-tag-drift.md). The PART argument
# is accepted for compatibility but no longer steers the tag.
if ! git cat-file -e "$SHA:pyproject.toml" 2>/dev/null; then
  echo '{"status":"error","failure_class":"upstream_error","message":"pyproject.toml missing on base head"}'
  exit 1
fi
PROJ_VER=$(git show "$SHA:pyproject.toml" 2>/dev/null | awk -F= '
  /^\[project\]/ {inp=1; next}
  /^\[/ {inp=0}
  inp && /^[[:space:]]*version[[:space:]]*=/ {
    v=$2; gsub(/["'"'"'[:space:]]/, "", v); print v; exit }')
if [[ ! "$PROJ_VER" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "{\"status\":\"error\",\"failure_class\":\"upstream_error\",\"message\":\"cannot parse [project].version '$PROJ_VER' on base head\"}"
  exit 1
fi
TAG="v$PROJ_VER"

# Version went backwards relative to the newest tag - stale package
# metadata, refuse rather than silently retagging an old release.
if [ "$PROJ_VER" != "${LATEST#v}" ] && \
   [ "$(printf '%s\n%s\n' "$PROJ_VER" "${LATEST#v}" | sort -V | head -n1)" = "$PROJ_VER" ]; then
  echo "{\"status\":\"error\",\"failure_class\":\"version_drift\",\"message\":\"pyproject [project].version=$PROJ_VER is behind latest tag $LATEST - stale version on base head\"}"
  exit 1
fi

if git rev-parse "$TAG" >/dev/null 2>&1; then
  # Idempotent retry: tag pushed on a previous run but the GitHub
  # Release step failed. Only continue if the tag points at the
  # expected base-branch head; otherwise it is a real conflict.
  EXISTING=$(git rev-parse "$TAG^{commit}")
  if [ "$EXISTING" != "$SHA" ]; then
    echo "{\"status\":\"error\",\"failure_class\":\"conflict\",\"message\":\"tag $TAG already exists on different sha\"}"
    exit 1
  fi
else
  git tag -a "$TAG" -m "Release $TAG" "$SHA" \
    && git push "$REMOTE" "$TAG" >/dev/null 2>&1 || {
    echo '{"status":"error","failure_class":"upstream_error","message":"tag create/push failed"}'; exit 1; }
fi

if gh release view "$TAG" >/dev/null 2>&1; then
  # Idempotent re-run: the release landed on a previous attempt but
  # the success output was lost - report released instead of failing
  # on the already-existing release. Kept from the pre-port canon
  # version; the upstream rewrite dropped it (issue #155).
  echo "{\"status\":\"released\",\"tag\":\"$TAG\",\"from\":\"$LATEST\",\"sha\":\"$SHA\",\"note\":\"already-existed\"}"
  exit 0
fi

if [ -n "$NOTES" ]; then
  gh release create "$TAG" --notes "$NOTES" >/dev/null 2>&1
else
  gh release create "$TAG" --generate-notes >/dev/null 2>&1
fi \
  && echo "{\"status\":\"released\",\"tag\":\"$TAG\",\"from\":\"$LATEST\",\"sha\":\"$SHA\"}" \
  || { echo '{"status":"error","failure_class":"upstream_error","message":"release create failed"}'; exit 1; }

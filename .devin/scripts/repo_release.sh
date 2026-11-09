#!/usr/bin/env bash
# repo_release.sh - tag + GitHub release after a merged PR.
# Usage: repo_release.sh [patch|minor|major] [--notes "<text>"]
# Release-prep step (issue #96): run `tools/release_notes.sh
# --version X.Y.Z --apply` on the release branch BEFORE tagging -
# it folds pending .changes/ entries into CHANGELOG.md so this
# script tags a head that already carries the changelog.
# Every merged PR means a release: reads the latest vX.Y.Z tag, bumps
# the requested component, creates an ANNOTATED tag on the base-branch
# head, pushes it and creates a GitHub release (--notes text or
# --generate-notes when no notes given).
# Output JSON: {status: released|error, tag?, from?, message?}
set -uo pipefail
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

PART="${1:-patch}"
case "$PART" in
  patch|minor|major) ;;
  --notes) PART=patch ;;
  *) echo '{"status":"error","message":"part must be patch|minor|major"}'; exit 2 ;;
esac

NOTES=""
while [ $# -gt 0 ]; do
  case "$1" in
    --notes) NOTES="${2:?--notes needs text}"; shift 2 ;;
    patch|minor|major) shift ;;
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
IFS=. read -r MAJ MIN PAT <<< "${LATEST#v}"
case "$PART" in
  patch) PAT=$((PAT+1)) ;;
  minor) MIN=$((MIN+1)); PAT=0 ;;
  major) MAJ=$((MAJ+1)); MIN=0; PAT=0 ;;
esac
TAG="v$MAJ.$MIN.$PAT"

SHA=$(git rev-parse "$REMOTE/$BASE" 2>/dev/null || git rev-parse "$BASE")

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
  # Version-drift guard (item_102b): the pyproject bump lives on the
  # item branch per convention - if the merged base head still carries
  # a different [project].version, tagging would publish a release on
  # stale package metadata (the devin-telegram fix-PR + tag-move
  # incident, twice). Refuse and name the remediation; re-running after
  # the bump-PR merge is safe (the idempotent path above).
  if git cat-file -e "$SHA:pyproject.toml" 2>/dev/null; then
    PROJ_VER=$(git show "$SHA:pyproject.toml" 2>/dev/null | awk -F= '
      /^\[project\]/ {inp=1; next}
      /^\[/ {inp=0}
      inp && /^[[:space:]]*version[[:space:]]*=/ {
        v=$2; gsub(/["'"'"'[:space:]]/, "", v); print v; exit }')
    if [ -n "$PROJ_VER" ] && [ "$PROJ_VER" != "$MAJ.$MIN.$PAT" ]; then
      echo "{\"status\":\"error\",\"failure_class\":\"version_drift\",\"message\":\"pyproject [project].version=$PROJ_VER but release tag would be $TAG - bump pyproject on a fix branch, merge it, then re-run repo_release.sh\"}"
      exit 1
    fi
  fi
  git tag -a "$TAG" -m "Release $TAG" "$SHA" \
    && git push "$REMOTE" "$TAG" >/dev/null 2>&1 || {
    echo '{"status":"error","failure_class":"upstream_error","message":"tag create/push failed"}'; exit 1; }
fi

if [ -n "$NOTES" ]; then
  gh release create "$TAG" --notes "$NOTES" >/dev/null 2>&1
else
  gh release create "$TAG" --generate-notes >/dev/null 2>&1
fi \
  && echo "{\"status\":\"released\",\"tag\":\"$TAG\",\"from\":\"$LATEST\",\"sha\":\"$SHA\"}" \
  || echo '{"status":"error","failure_class":"upstream_error","message":"release create failed"}'

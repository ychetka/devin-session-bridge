#!/usr/bin/env bash
# ship_commit.sh - commit wrapper with enforced gates.
# Usage: ship_commit.sh "message" [file ...]
# Stages given files (or keeps existing index), then enforces, in order:
#   1. precommit re-grounding (mechanical, unconditional): the caller is
#      shown the real staged footprint before any commit happens
#   2. scope gate: if .devin/scope_whitelist.txt exists (worktree,
#      index, or HEAD), every staged file must be inside it (exact
#      path or dir/ prefix entries); the rules evaluated are the ones
#      that will actually be committed (index, else HEAD)
#   3. migration gate: staged migration files are re-verified live via
#      migration_gate.sh --check (paired/embedded/rollforward coverage;
#      the migration_ok stamp is not trusted - it is forgeable)
#   4. hygiene gate: message and staged diff are checked by
#      hygiene_check.sh in SEPARATE passes (message in stdin mode
#      with no path exemptions, diff in --diff mode)
# Exit codes: 1 usage/env, 2 scope or hygiene reject, 3 migration gate.
set -uo pipefail
# .devin may live outside the git toplevel (shared_person layout) -
# resolve it from this script's own location BEFORE cd changes CWD.
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

# Shared pattern - single source, do not fork it here.
. "$DEVIN_DIR/scripts/migration_pattern.sh"

MSG="${1:?usage: ship_commit.sh \"message\" [file ...]}"
shift || true

# Stage explicit file list if given
[ $# -gt 0 ] && { git add -- "$@" || { echo "git add failed" >&2; exit 1; }; }

# --- Gate 0: precommit re-grounding (mechanical, cannot be skipped) ---
# The caller is always confronted with the real staged footprint.
echo "=== RE-GROUNDING: staged footprint ===" >&2
git status --porcelain >&2
git diff --cached --stat >&2
echo "=== END RE-GROUNDING ===" >&2

# --- Gate 1: whitelist scope ---
# The gate stays active while the whitelist exists in the worktree,
# the index, or HEAD: an unstaged `rm` or a staged `git rm` must not
# silently disable it (the deletion is only effective once committed,
# and the self-edit rule below makes that a standalone commit).
WL="$DEVIN_DIR/scope_whitelist.txt"
# Canonicalize only the parent directory, never the leaf: resolving
# the leaf would let an unstaged symlink at the whitelist path push
# REL_WL outside the toplevel and substitute attacker-controlled
# "rules" via the worktree fallback.
WL_DIR=$(dirname "$WL"); WL_BASE=$(basename "$WL")
REL_DIR=$(realpath -m --relative-to="$TOP" "$WL_DIR" 2>/dev/null || echo "")
case "$REL_DIR" in
  ../*|"") REL_WL="" ;;
  .)       REL_WL="$WL_BASE" ;;
  *)       REL_WL="$REL_DIR/$WL_BASE" ;;
esac
wl_in_index=0; wl_in_head=0
if [ -n "$REL_WL" ]; then
  git cat-file -e ":$REL_WL" 2>/dev/null && wl_in_index=1
  git cat-file -e "HEAD:$REL_WL" 2>/dev/null && wl_in_head=1
fi
if [ -f "$WL" ] || [ "$wl_in_index" -eq 1 ] || [ "$wl_in_head" -eq 1 ]; then
  # A commit that edits OR removes the whitelist itself must touch
  # nothing else: otherwise one commit could widen scope and ship the
  # payload under its own new rule, or silently rename/remove the
  # whitelist out from under the gate. `git diff --cached --name-only`
  # lists renames under the new name only, so a staged removal is
  # detected structurally: HEAD still holds the blob, the index does
  # not. Only applies when .devin/ lives inside the repo (a whitelist
  # outside the toplevel cannot appear in the diff).
  if [ -n "$REL_WL" ]; then
    staged=$(git diff --cached --name-only)
    if { [ "$wl_in_head" -eq 1 ] && [ "$wl_in_index" -eq 0 ]; } \
       || grep -qxF "$REL_WL" <<< "$staged"; then
      if [ "$(wc -l <<< "$staged")" -gt 1 ]; then
        echo "SCOPE_GATE: $REL_WL changes must be the only staged file" >&2
        exit 2
      fi
    fi
  fi
  # Evaluate the whitelist content that will actually be committed:
  # the index version, then HEAD (a staged deletion still owes the
  # pre-deletion rules to the payload it rides with). Reading the
  # worktree file would let an unstaged edit, deletion, or symlink
  # change scope invisibly. A whitelist outside the toplevel or never
  # tracked falls back to the worktree content (documented residual
  # boundary).
  wl_rules=""
  if [ "$wl_in_index" -eq 1 ]; then
    wl_rules=$(git show ":$REL_WL" 2>/dev/null || true)
  elif [ "$wl_in_head" -eq 1 ]; then
    wl_rules=$(git show "HEAD:$REL_WL" 2>/dev/null || true)
  elif [ -f "$WL" ]; then
    wl_rules=$(cat "$WL")
  fi
  bad=""
  # --no-renames expands a rename into delete+add so BOTH the source
  # and destination paths are scope-checked (renaming an out-of-scope
  # file into an allowed path cannot smuggle it); -z keeps filenames
  # with whitespace/quotes intact, not git-quoted.
  while IFS= read -r -d '' f; do
    [ -z "$f" ] && continue
    # The whitelist file itself is always in-scope: the self-edit
    # check above already forces it to be the only staged file.
    [ -n "$REL_WL" ] && [ "$f" = "$REL_WL" ] && continue
    ok=0
    while IFS= read -r line; do
      case "$line" in ''|'#'*) continue ;; esac
      line="${line%/}"
      if [ "$f" = "$line" ]; then ok=1; break; fi
      case "$f" in "$line"/*) ok=1; break ;; esac
    done <<< "$wl_rules"
    [ "$ok" -eq 0 ] && bad="$bad$f"$'\n'
  done < <(git diff --cached --name-only --no-renames -z)
  if [ -n "$bad" ]; then
    echo "SCOPE_GATE: staged files outside $WL:" >&2
    printf '%s' "$bad" >&2
    exit 2
  fi
fi

# --- Gate 2: migration gate ---
# Deletions are in scope (D): dropping an applied migration needs
# coverage just like adding one.
migs=$(git diff --cached --name-only -z --diff-filter=ADMR | grep -zE "$MIG_PAT" | tr '\0' '\n' || true)
if [ -n "$migs" ]; then
  # The coverage check runs synchronously on every commit instead of
  # trusting .devin/migration_ok: the stamp is a self-attested value
  # the same actor can forge from public index data. Live --check
  # verifies the staged blobs themselves each time.
  if ! "$DEVIN_DIR/scripts/migration_gate.sh" --check; then
    echo "MIGRATION_GATE: staged migration files failed coverage" >&2
    echo "verification. Each needs a paired rollback file, an embedded" >&2
    echo "rollback section, or .devin/migration_rollforward.md." >&2
    echo "$migs" >&2
    exit 3
  fi
fi

# --- Gate 3: hygiene. Message and diff are checked SEPARATELY: a
# multi-line MSG could forge a fake "diff --git"/"+++" header (or any
# framing line) and smuggle unscanned content under an exempt path
# into `git commit -m`. Stdin mode scans the whole message with no
# path context (no exemptions); the raw diff gets path tracking for
# per-file language exemptions.
printf '%s\n' "$MSG" | "$DEVIN_DIR/scripts/hygiene_check.sh" || {
  echo "HYGIENE_GATE: commit message rejected" >&2; exit 2; }
git diff --cached | "$DEVIN_DIR/scripts/hygiene_check.sh" --diff || {
  echo "HYGIENE_GATE: staged content rejected" >&2; exit 2; }

git commit -m "$MSG"

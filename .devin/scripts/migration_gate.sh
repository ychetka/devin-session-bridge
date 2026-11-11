#!/usr/bin/env bash
# migration_gate.sh - isolated verification for staged schema/data
# migrations. Produces .devin/migration_ok stamp that ship_commit.sh
# requires before committing migration files.
#
# The gate NEVER applies a migration and never touches any live or
# production database. It only verifies that for every staged
# migration file one of these holds:
#   a) a paired rollback path is staged (down/undo/rollback file), or
#   b) the file itself contains a rollback/downgrade section
#      (down|undo|rollback|revert|downgrade), or
#   c) an approved roll-forward note exists at
#      .devin/migration_rollforward.md (human-approved, contains the
#      justification for going forward-only)
#
# Optional: if the project provides a native dry-run entrypoint via
# DRY_RUN_CMD env var, it is executed in a subshell with a timeout —
# it must be the project's own manifest-declared command, never an
# ad-hoc production command.
#
# Usage: migration_gate.sh [--check]   # --check: verify only, no stamp
# Exit: 0 verified (stamp written unless --check), 1 violations found.
set -uo pipefail
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

CHECK_ONLY=0
[ "${1:-}" = "--check" ] && CHECK_ONLY=1

MIG_PAT='(^|/)(migrations?|alembic/versions|db/migrate|flyway|liquibase)/|(^|/)[VU][0-9]+(_[0-9]+)*__.*\.sql$'

# A basename that denotes a rollback artifact rather than a migration
# that needs coverage (001_down.sql, U2__undo.sql, *_rollback.sql).
is_rollback_name() {
  local b; b=$(basename "$1")
  echo "$b" | grep -qiE 'down|undo|rollback|revert' && return 0
  echo "$b" | grep -qE '^U[0-9]+(_[0-9]+)*__' && return 0
  return 1
}

# Staged files needing coverage: migration-pattern paths that are not
# themselves rollback artifacts.
mapfile -t all_mig < <(git diff --cached --name-only --diff-filter=AMR | grep -E "$MIG_PAT" || true)
staged=(); pairs=()
for f in ${all_mig[@]+"${all_mig[@]}"}; do
  if is_rollback_name "$f"; then pairs+=("$f"); else staged+=("$f"); fi
done
if [ ${#staged[@]} -eq 0 ]; then
  if [ ${#pairs[@]} -gt 0 ]; then
    echo "only rollback files staged - nothing to cover" >&2
  else
    echo "no staged migration files - nothing to verify" >&2
  fi
  exit 1
fi

# Rollback vocabulary, language-neutral heuristic.
ROLLBACK_RE='downgrade|rollback|revert|\bundo\b|\bdown\b'

# Stem of a migration filename: the leading identifier that a paired
# rollback file must share (001_x <-> 001_y, V2__x <-> U2__y share "2",
# alembic rev hash shares its hex prefix).
stemof() {
  basename "$1" | sed -E 's/^[VU]?([0-9]+(_[0-9]+)*).*/\1/; t; s/[^A-Za-z0-9].*//'
}

fail=0
for f in "${staged[@]}"; do
  covered=""
  stem=$(stemof "$f")
  # a) staged paired rollback file sharing the same stem
  for p in ${pairs[@]+"${pairs[@]}"}; do
    [ "$p" = "$f" ] && continue
    [ "$(stemof "$p")" = "$stem" ] && { covered="paired rollback: $p"; break; }
  done
  # b) embedded rollback/downgrade section in the STAGED blob
  #    (index content, not the working copy - they may differ)
  if [ -z "$covered" ] && git show ":$f" 2>/dev/null | grep -qiE "$ROLLBACK_RE"; then
    covered="embedded rollback section"
  fi
  # c) approved roll-forward note
  if [ -z "$covered" ] && [ -s "$DEVIN_DIR/migration_rollforward.md" ]; then
    covered="approved roll-forward (.devin/migration_rollforward.md)"
  fi
  if [ -z "$covered" ]; then
    echo "MIGRATION GATE FAIL: $f has no rollback path" >&2
    fail=1
  else
    echo "OK: $f -> $covered"
  fi
done
[ $fail -ne 0 ] && exit 1

# Optional native dry-run, only via project-declared command.
if [ -n "${DRY_RUN_CMD:-}" ]; then
  echo "running project dry-run: $DRY_RUN_CMD"
  timeout 120 env DRY_RUN=1 bash -c "$DRY_RUN_CMD" || {
    echo "MIGRATION GATE FAIL: dry-run exited non-zero" >&2; exit 1; }
fi

if [ $CHECK_ONLY -eq 1 ]; then
  echo "check-only: all ${#staged[@]} migration file(s) verified"
  exit 0
fi

# Stamp binds the full staged footprint of migration-pattern files
# (migrations + their rollback artifacts) - the same set ship_commit
# recomputes via MIG_PAT.
want=$(for f in "${all_mig[@]}"; do git ls-files -s -- "$f"; done | awk '{print $2}' | sort | sha256sum | cut -d' ' -f1)
{
  echo "files=$want"
  echo "verified_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'list=%s\n' "${staged[*]}"
} > "$DEVIN_DIR/migration_ok"
echo "stamp written: $DEVIN_DIR/migration_ok"

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
#      (downgrade|rollback|revert|undo, or "down" inside a -- comment
#      line like goose's "-- +goose Down"), or
#   c) an approved roll-forward note exists at
#      .devin/migration_rollforward.md that names the file's
#      repo-relative path explicitly (human-approved, contains the
#      justification for going forward-only)
# A staged DELETION of a migration file is covered only by (c): the
# note must name the deleted path explicitly.
#
# Optional: if the project provides a native dry-run entrypoint via
# the "dry_run_cmd" key in .devin/project.json, it is executed in a
# subshell with a timeout — it must be the project's own
# manifest-declared command, never an ad-hoc production command.
# Absent file or absent key = no dry-run.
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

NOTE="$DEVIN_DIR/migration_rollforward.md"

# Staged files needing coverage: migration-pattern paths that are not
# themselves rollback artifacts. all_mig stays AMR-only for the
# pairs/staged logic; deletions are tracked separately because a
# deleted file can never carry an embedded rollback section or a
# staged pair - only a roll-forward note naming it can cover it.
mapfile -t all_mig < <(git diff --cached --name-only --diff-filter=AMR | grep -E "$MIG_PAT" || true)
mapfile -t deleted < <(git diff --cached --name-only --diff-filter=D | grep -E "$MIG_PAT" || true)
staged=(); pairs=()
for f in ${all_mig[@]+"${all_mig[@]}"}; do
  if is_rollback_name "$f"; then pairs+=("$f"); else staged+=("$f"); fi
done
if [ ${#staged[@]} -eq 0 ] && [ ${#deleted[@]} -eq 0 ]; then
  if [ ${#pairs[@]} -gt 0 ]; then
    echo "only rollback files staged - nothing to cover" >&2
  else
    echo "no staged migration files - nothing to verify" >&2
  fi
  exit 1
fi

# Rollback vocabulary, language-neutral heuristic. Bare "down" only
# counts inside a "--" comment line (goose "-- +goose Down") - a bare
# word match would let string literals like 'shutdown' pose as
# rollback coverage.
ROLLBACK_RE='downgrade|rollback|revert|\bundo\b|--.*\bdown\b'

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
  # c) approved roll-forward note naming this file explicitly
  if [ -z "$covered" ] && [ -s "$NOTE" ] && grep -qF -- "$f" "$NOTE"; then
    covered="approved roll-forward (.devin/migration_rollforward.md)"
  fi
  if [ -z "$covered" ]; then
    echo "MIGRATION GATE FAIL: $f has no rollback path" >&2
    fail=1
  else
    echo "OK: $f -> $covered"
  fi
done

# Staged deletions: covered ONLY by the roll-forward note naming each
# deleted path explicitly - pairs and embedded sections cannot apply
# to a file that no longer exists.
for f in ${deleted[@]+"${deleted[@]}"}; do
  if [ -s "$NOTE" ] && grep -qF -- "$f" "$NOTE"; then
    echo "OK: $f -> approved roll-forward (.devin/migration_rollforward.md)"
  else
    echo "MIGRATION GATE FAIL: deleted migration $f has no roll-forward coverage" >&2
    fail=1
  fi
done
[ $fail -ne 0 ] && exit 1

# Optional native dry-run, only via the command the project declares
# in its own manifest (.devin/project.json key "dry_run_cmd"). Absent
# file or absent key = no dry-run; never an ad-hoc env var.
DRY_RUN_CMD=""
if [ -f "$DEVIN_DIR/project.json" ]; then
  DRY_RUN_CMD=$(python3 -c 'import json,sys
try:
    print(json.load(open(sys.argv[1])).get("dry_run_cmd",""))
except Exception:
    print("")' "$DEVIN_DIR/project.json" 2>/dev/null || true)
fi
if [ -n "$DRY_RUN_CMD" ]; then
  echo "running project dry-run: $DRY_RUN_CMD"
  timeout 120 env DRY_RUN=1 bash -c "$DRY_RUN_CMD" || {
    echo "MIGRATION GATE FAIL: dry-run exited non-zero" >&2; exit 1; }
fi

if [ $CHECK_ONLY -eq 1 ]; then
  echo "check-only: all ${#staged[@]} migration file(s) verified"
  exit 0
fi

# Stamp binds the full staged footprint of migration-pattern files
# (migrations + their rollback artifacts + staged deletions as D:<path>
# lines) - the same set ship_commit recomputes via MIG_PAT.
want=$( { for f in "${all_mig[@]}"; do git ls-files -s -- "$f" | awk '{print $2}'; done; for d in ${deleted[@]+"${deleted[@]}"}; do printf 'D:%s\n' "$d"; done; } | sort | sha256sum | cut -d' ' -f1)
{
  echo "files=$want"
  echo "verified_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'list=%s\n' "${staged[*]}"
} > "$DEVIN_DIR/migration_ok"
echo "stamp written: $DEVIN_DIR/migration_ok"

#!/usr/bin/env bash
# migration_gate.sh - isolated verification for staged schema/data
# migrations. ship_commit.sh invokes it with --check on every commit
# that touches migration-pattern files; verification always runs live
# on the staged index - the .devin/migration_ok stamp this script also
# writes is an informational audit record, not the enforcement path
# (a self-attested stamp could be forged from public index data).
#
# The gate NEVER applies a migration and never touches any live or
# production database. It only verifies that for every staged
# migration file one of these holds:
#   a) a paired rollback path is staged (down/undo/rollback file with
#      the same stem AND a non-empty staged blob), or
#   b) the file itself contains a rollback/downgrade MARKER: a
#      rollback keyword acting as a directive (immediately followed
#      by ':'/'('/'='), or a "down" word inside a --/#/// comment line
#      (goose "-- +goose Down"). A keyword in prose ("-- no rollback
#      needed", a column named 'countdown') does NOT qualify, or
#   c) an approved roll-forward note exists at
#      .devin/migration_rollforward.md, is already tracked in HEAD
#      (i.e. passed review in an earlier commit - an ad-hoc note
#      written alongside the migration proves nothing), and lists
#      this migration's exact repo-relative path on its own line.
#      A blanket note, a substring (.bak name), or a bare basename
#      covers nothing. When .devin/ lives outside the repo toplevel
#      the note cannot be tracked and coverage (c) is unavailable
#      by design.
# A staged DELETION of a migration file is covered only by (c): a
# deleted file can carry no embedded section and no staged pair.
#
# Note on strength: this gate verifies that a rollback path EXISTS for
# every staged migration; it does not verify the rollback is correct.
# It is a tripwire for a hasty-but-honest caller, not a cryptographic
# control - the caller can already run arbitrary commands.
#
# Optional: if .devin/project.json declares a native dry-run
# entrypoint under "migration_dry_run_cmd", it is executed in a
# subshell with a timeout - it must be the project's own
# manifest-declared command, never an ad-hoc production command.
# The command is read ONLY from project.json, never from the
# environment (env vars are caller-controllable). A present but
# malformed project.json fails closed.
#
# Usage: migration_gate.sh [--check]   # --check: verify only, no stamp
# Exit: 0 verified (stamp written unless --check), 1 violations found.
set -uo pipefail
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"
# Path of .devin relative to the repo root, for HEAD lookups of
# tracked files like migration_rollforward.md. Empty when .devin sits
# outside the repo (shared_person layout) - the note then cannot be
# tracked and coverage (c) is unavailable by design.
DEVIN_DIR_REL=""
case "$DEVIN_DIR" in
  "$TOP")   DEVIN_DIR_REL="." ;;
  "$TOP/"*) DEVIN_DIR_REL="${DEVIN_DIR#"$TOP"/}" ;;
esac

CHECK_ONLY=0
[ "${1:-}" = "--check" ] && CHECK_ONLY=1

# Shared pattern - single source, do not fork it here.
. "$DEVIN_DIR/scripts/migration_pattern.sh"

# A basename that denotes a rollback artifact rather than a migration
# that needs coverage (001_down.sql, U2__undo.sql, *_rollback.sql).
# Markers must match as delimited segments, not substrings: a plain
# migration named "007_file_downloads.sql" or "003_countdown.sql" must
# NOT be misread as a rollback file - aside from false-positive blocks,
# a same-stem "downloads" file could otherwise masquerade as paired
# rollback coverage for a real migration that has none.
is_rollback_name() {
  local b; b=$(basename "$1")
  echo "$b" | grep -qiE '(^|[^A-Za-z])(down|undo|rollback|revert|downgrade)([^A-Za-z]|$)' && return 0
  echo "$b" | grep -qE '^U[0-9]+(_[0-9]+)*__' && return 0
  return 1
}

NOTE="$DEVIN_DIR/migration_rollforward.md"

# The roll-forward note covers a repo-relative path only when the note
# is tracked in HEAD (reviewed in an earlier commit) and lists that
# exact path on its own line (grep -qxF: a .bak substring cannot bind).
note_covers() {
  [ -n "$DEVIN_DIR_REL" ] || return 1
  git cat-file -e "HEAD:$DEVIN_DIR_REL/migration_rollforward.md" 2>/dev/null || return 1
  git show "HEAD:$DEVIN_DIR_REL/migration_rollforward.md" 2>/dev/null \
    | grep -qxF -- "$1"
}

# Staged files needing coverage: migration-pattern paths that are not
# themselves rollback artifacts. all_mig stays AMR-only for the
# pairs/staged logic; deletions are tracked separately because a
# deleted file can never carry an embedded rollback section or a
# staged pair - only the roll-forward note naming it can cover it.
# -z listings keep filenames with whitespace/quotes intact.
mapfile -t -d '' all_mig < <(git diff --cached --name-only -z --diff-filter=AMR | grep -zE "$MIG_PAT" || true)
mapfile -t -d '' deleted < <(git diff --cached --name-only -z --diff-filter=D | grep -zE "$MIG_PAT" || true)
staged=(); pairs=()
for f in ${all_mig[@]+"${all_mig[@]}"}; do
  if is_rollback_name "$f"; then pairs+=("$f"); else staged+=("$f"); fi
done
if [ ${#staged[@]} -eq 0 ] && [ ${#deleted[@]} -eq 0 ]; then
  if [ ${#pairs[@]} -gt 0 ]; then
    # Rollback artifacts are themselves the rollback path - a commit
    # that reverts a previously applied migration is legitimate and
    # must not be blocked by the synchronous --check in ship_commit.
    echo "only rollback files staged - nothing to cover" >&2
    exit 0
  fi
  echo "no staged migration files - nothing to verify" >&2
  exit 1
fi

# Rollback marker, language-neutral heuristic. First alternative: the
# keyword must act as a directive - immediately followed (within 3
# non-alnum chars) by ':'/'('/'=' - so "rollback:", "def downgrade()",
# "down:" count but a passing comment like "-- no way to rollback
# this" does not. Second alternative: a "down" word inside a comment
# line (goose "-- +goose Down", sql-migrate "-- +migrate Down").
ROLLBACK_RE='(rollback|downgrade|revert|\bundo\b)[^A-Za-z0-9_]{0,3}[:=(]|(--|#|//).*\bdown\b'

# Stem of a migration filename: the leading identifier that a paired
# rollback file must share (001_x <-> 001_y, V2__x <-> U2__y share "2",
# alembic rev hash shares its hex prefix).
stemof() {
  basename "$1" | sed -E 's/^[VU]?([0-9]+(_[0-9]+)*).*/\1/; t; s/[^A-Za-z0-9].*//'
}

fail=0
for f in ${staged[@]+"${staged[@]}"}; do
  covered=""
  stem=$(stemof "$f")
  # a) staged paired rollback file sharing the same stem, with a
  #    non-empty staged blob (an empty rollback file is no coverage)
  for p in ${pairs[@]+"${pairs[@]}"}; do
    [ "$p" = "$f" ] && continue
    [ "$(stemof "$p")" = "$stem" ] \
      && git show ":$p" 2>/dev/null | grep -q '[^[:space:]]' \
      && { covered="paired rollback: $p"; break; }
  done
  # b) embedded rollback/downgrade marker in the STAGED blob
  #    (index content, not the working copy - they may differ)
  if [ -z "$covered" ] && git show ":$f" 2>/dev/null | grep -qiE "$ROLLBACK_RE"; then
    covered="embedded rollback section"
  fi
  # c) approved roll-forward note, HEAD-tracked, naming this exact
  #    repo-relative path on its own line
  if [ -z "$covered" ] && note_covers "$f"; then
    covered="approved roll-forward (.devin/migration_rollforward.md)"
  fi
  if [ -z "$covered" ]; then
    echo "MIGRATION GATE FAIL: $f has no rollback path" >&2
    fail=1
  else
    echo "OK: $f -> $covered"
  fi
done

# Staged deletions: covered ONLY by the roll-forward note listing each
# deleted path on its own line - pairs and embedded sections cannot
# apply to a file that no longer exists.
for f in ${deleted[@]+"${deleted[@]}"}; do
  if note_covers "$f"; then
    echo "OK: $f -> approved roll-forward (.devin/migration_rollforward.md)"
  else
    echo "MIGRATION GATE FAIL: deleted migration $f has no roll-forward coverage" >&2
    fail=1
  fi
done
[ $fail -ne 0 ] && exit 1

# Optional native dry-run, only via the project-declared command in
# .devin/project.json ("migration_dry_run_cmd"). The environment is
# caller-controllable, so a DRY_RUN_CMD env var is never honored.
# Fail closed: an existing but unreadable/malformed project.json is
# an error, not a silent skip - a corrupted manifest must not weaken
# the gate. Absent file = no dry-run declared, skip as before.
DRY_RUN=""
if [ -f "$DEVIN_DIR/project.json" ]; then
  if ! command -v python3 >/dev/null 2>&1; then
    echo "MIGRATION GATE FAIL: python3 required to read $DEVIN_DIR/project.json" >&2
    exit 1
  fi
  if ! DRY_RUN=$(HYGIENE_PJ="$DEVIN_DIR/project.json" python3 -c "import json,os;print(json.load(open(os.environ['HYGIENE_PJ'])).get('migration_dry_run_cmd','') or '')" 2>/dev/null); then
    echo "MIGRATION GATE FAIL: $DEVIN_DIR/project.json exists but is not valid JSON" >&2
    exit 1
  fi
fi
if [ -n "$DRY_RUN" ]; then
  # GNU coreutils 'timeout' or its BSD/gnu-getopt twin 'gtimeout'.
  TIMEOUT_BIN=$(command -v timeout || command -v gtimeout || true)
  if [ -z "$TIMEOUT_BIN" ]; then
    echo "MIGRATION GATE FAIL: dry-run declared but neither 'timeout'" >&2
    echo "nor 'gtimeout' is available on PATH - cannot bound it" >&2
    exit 1
  fi
  echo "running project dry-run: $DRY_RUN"
  "$TIMEOUT_BIN" 120 env DRY_RUN=1 bash -c "$DRY_RUN" || {
    echo "MIGRATION GATE FAIL: dry-run exited non-zero" >&2; exit 1; }
fi

if [ $CHECK_ONLY -eq 1 ]; then
  echo "check-only: all ${#staged[@]} migration file(s) verified"
  exit 0
fi

# Informational stamp: binds the full staged footprint of
# migration-pattern files (migrations + rollback artifacts + staged
# deletions as D:<path> lines). NOT consulted by ship_commit - kept
# only as an audit record of what was verified and when.
want=$( { for f in ${all_mig[@]+"${all_mig[@]}"}; do git ls-files -s -- "$f" | awk '{print $2}'; done; for d in ${deleted[@]+"${deleted[@]}"}; do printf 'D:%s\n' "$d"; done; } | sort | sha256sum | cut -d' ' -f1)
{
  echo "files=$want"
  echo "verified_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'list=%s\n' "${staged[*]:-}"
} > "$DEVIN_DIR/migration_ok"
echo "stamp written: $DEVIN_DIR/migration_ok"

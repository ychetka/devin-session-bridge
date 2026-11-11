#!/usr/bin/env bash
# ship_commit.sh - commit wrapper with enforced gates.
# Usage: ship_commit.sh "message" [file ...]
# Stages given files (or keeps existing index), then enforces, in order:
#   1. precommit re-grounding (mechanical, unconditional): the caller is
#      shown the real staged footprint before any commit happens
#   2. scope gate: if .devin/scope_whitelist.txt exists, every staged
#      file must be inside it (exact path or dir/ prefix entries)
#   3. migration gate: staged migration files require a valid
#      .devin/migration_ok stamp produced by migration_gate.sh
#   4. hygiene gate: message + staged diff through hygiene_check.sh
# Exit codes: 1 usage/env, 2 scope or hygiene reject, 3 migration gate.
set -uo pipefail
# .devin may live outside the git toplevel (shared_person layout) -
# resolve it from this script's own location BEFORE cd changes CWD.
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

MSG="${1:?usage: ship_commit.sh \"message\" [file ...]}"
shift || true

# Stage explicit file list if given
[ $# -gt 0 ] && git add -- "$@"

# --- Gate 0: precommit re-grounding (mechanical, cannot be skipped) ---
# The caller is always confronted with the real staged footprint.
echo "=== RE-GROUNDING: staged footprint ===" >&2
git status --porcelain >&2
git diff --cached --stat >&2
echo "=== END RE-GROUNDING ===" >&2

# --- Gate 1: whitelist scope ---
WL="$DEVIN_DIR/scope_whitelist.txt"
if [ -f "$WL" ]; then
  bad=""
  while IFS= read -r f; do
    [ -z "$f" ] && continue
    ok=0
    while IFS= read -r line; do
      case "$line" in ''|'#'*) continue ;; esac
      line="${line%/}"
      if [ "$f" = "$line" ]; then ok=1; break; fi
      case "$f" in "$line"/*) ok=1; break ;; esac
    done < "$WL"
    [ "$ok" -eq 0 ] && bad="$bad$f"$'\n'
  done <<EOF
$(git diff --cached --name-only)
EOF
  if [ -n "$bad" ]; then
    echo "SCOPE_GATE: staged files outside $WL:" >&2
    printf '%s' "$bad" >&2
    exit 2
  fi
fi

# --- Gate 2: migration gate ---
MIG_PAT='(^|/)(migrations?|alembic/versions|db/migrate|flyway|liquibase)/|(^|/)[VU][0-9]+(_[0-9]+)*__.*\.sql$'
migs=$(git diff --cached --name-only --diff-filter=AM | grep -E "$MIG_PAT" || true)
if [ -n "$migs" ]; then
  stamp="$DEVIN_DIR/migration_ok"
  # Bind to staged blob hashes (index), not working-copy files.
  want=$(git ls-files -s -- $migs 2>/dev/null | awk '{print $2}' | sort | sha256sum | cut -d' ' -f1)
  have=""
  [ -f "$stamp" ] && have=$(grep -m1 '^files=' "$stamp" | cut -d= -f2)
  if [ "$want" != "$have" ] || [ -z "$have" ]; then
    echo "MIGRATION_GATE: staged migration files lack a valid stamp." >&2
    echo "Run .devin/scripts/migration_gate.sh first (verifies rollback" >&2
    echo "or approved roll-forward for exactly these files)." >&2
    echo "$migs" >&2
    exit 3
  fi
fi

# --- Gate 3: hygiene (message + staged content; raw diff keeps paths so
# per-file language exemptions from project.json apply)
{ echo "$MSG"; git diff --cached; } | "$DEVIN_DIR/scripts/hygiene_check.sh" --diff
rc=$?
if [ $rc -ne 0 ]; then
  echo "HYGIENE_GATE: rejected" >&2
  exit 2
fi

git commit -m "$MSG"

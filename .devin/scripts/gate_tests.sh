#!/usr/bin/env bash
# gate_tests.sh - negative tests for the commit-wrapper gates.
# Proves each gate blocks its bypass path in an isolated scratch repo:
#   T1  scope gate: file outside whitelist is rejected (exit 2)
#   T2  scope gate: dir-prefix whitelist entry allows the file
#   T3  migration gate: uncovered staged migration is rejected (exit 3)
#   T4  migration gate: rollback-covered migration verifies + commits
#   T5  migration gate: coverage lost on edit is blocked on next commit
#   T6  migration gate: rename+strip of a verified migration is blocked
#   T7  migration gate: staged up+down pair verifies via paired coverage
#   T8  migration gate: rollback-only commit passes (its own coverage)
#   T9  re-grounding output is emitted on every call
#   T10 hygiene gate rejects cyrillic message (en-language repo)
#   T11 forged sentinel + fake diff + secret in message is rejected
#   T12 forged sentinel + exempt-path cyrillic in message is rejected
#   T13 basename merely containing "down" is not rollback coverage
#   T14 correctly-hashed forged migration_ok stamp does not unlock gate
#   T15 staged whitelist edit must be the only staged file (exit 2)
#   T15b a whitelist-only commit is allowed
#   T16 unstaged whitelist widening does not bypass the scope gate
#   T17 unstaged whitelist deletion does not disable the scope gate
#   T18 staged whitelist deletion + payload is rejected (exit 2)
#   T19 unstaged symlink at the whitelist path cannot feed fake rules
#   T20 staged whitelist rename + payload is rejected (exit 2)
#   T20b standalone whitelist rename-away and rename-back are allowed
#   T21 a standalone staged whitelist removal is allowed
#   T22 roll-forward note: untracked/wrong-path/.bak-substring rejected,
#       HEAD-tracked own-line path accepted
#   T23 staged migration deletion needs a naming roll-forward note
#   T24 prose rollback comment is not coverage
#   T25 empty paired rollback file is not coverage
#   T26 'task-' wording does not trip the sk- secret rule (exit 0)
#   T27 a real sk- key in staged content is still blocked (exit 2)
#   T28 lockfile added-lines are skipped by hygiene (exit 0)
#   T29 --diff-base cannot be desynced by forged subjects
#   T30 --diff-base on an unresolvable ref fails closed
#   T31 API-key patterns actually fire (xoxb, AKIA, sk- bodies)
#   T32 scope gate inspects the OLD path of a rename too
#   T33 cyrillic under an exempt path (.devin/agents/) is allowed
#   T34 malformed project.json fails closed
#   T35 DRY_RUN_CMD env var is ignored; only the project.json-declared
#       migration_dry_run_cmd runs
# Exits non-zero if any expectation fails. Run from anywhere:
#   .devin/scripts/gate_tests.sh
set -uo pipefail
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

SCRATCH=$(mktemp -d)
trap 'rm -rf "$SCRATCH"' EXIT
mkdir -p "$SCRATCH/repo" "$SCRATCH/devin/scripts"
cd "$SCRATCH/repo" && git init -q && git config user.email t@t && git config user.name t && git commit -q --allow-empty -m init
cp "$DEVIN_DIR/scripts/ship_commit.sh" "$DEVIN_DIR/scripts/migration_gate.sh" \
   "$DEVIN_DIR/scripts/migration_pattern.sh" "$DEVIN_DIR/scripts/hygiene_check.sh" \
   "$SCRATCH/devin/scripts/"
echo '{"text_language":"en"}' > "$SCRATCH/devin/project.json"

pass=0; fail=0
expect() { # name expected_rc actual_rc [pattern_in_output]
  local name="$1" want="$2" got="$3" pat="${4:-}"
  if [ "$got" != "$want" ]; then
    echo "FAIL $name: expected exit $want, got $got"; fail=$((fail+1)); return
  fi
  if [ -n "$pat" ] && ! grep -q "$pat" "$SCRATCH/out"; then
    echo "FAIL $name: output missing '$pat'"; fail=$((fail+1)); return
  fi
  echo "PASS $name"; pass=$((pass+1))
}

SHIP="$SCRATCH/devin/scripts/ship_commit.sh"
GATE="$SCRATCH/devin/scripts/migration_gate.sh"
HYG="$SCRATCH/devin/scripts/hygiene_check.sh"

# --- T1: scope gate blocks out-of-whitelist staged file
echo 'docs/x.md' > "$SCRATCH/devin/scope_whitelist.txt"
mkdir -p src && echo x > src/evil.py && git add src/evil.py
"$SHIP" "test msg" >"$SCRATCH/out" 2>&1; expect T1-scope-block 2 $? "SCOPE_GATE"
git reset -q

# --- T2: dir-prefix entry allows the file
printf 'src/\ndocs/x.md\n' > "$SCRATCH/devin/scope_whitelist.txt"
git add src/evil.py
"$SHIP" "test msg" >"$SCRATCH/out" 2>&1; expect T2-scope-prefix 0 $?

# --- T3: migration gate blocks a staged migration with no coverage
rm "$SCRATCH/devin/scope_whitelist.txt"
mkdir -p db/migrations && printf 'CREATE TABLE t(id int);\n' > db/migrations/001_up.sql
git add db/migrations/001_up.sql
"$SHIP" "add migration" >"$SCRATCH/out" 2>&1; expect T3-migration-block 3 $? "MIGRATION_GATE"
git reset -q

# --- T4: rollback-covered migration verifies live and commits
printf 'CREATE TABLE t(id int);\n-- rollback: DROP TABLE t;\n' > db/migrations/001_up.sql
git add db/migrations/001_up.sql
"$GATE" >"$SCRATCH/out" 2>&1; expect T4a-gate-verifies 0 $? "embedded rollback"
"$SHIP" "add migration" >"$SCRATCH/out" 2>&1; expect T4b-commit-ok 0 $?
# The stamp is informational only: it may exist, but is never consulted.
[ -f "$SCRATCH/devin/migration_ok" ] \
  && { echo "PASS T4c-stamp-informational"; pass=$((pass+1)); } \
  || { echo "FAIL T4c-stamp-informational"; fail=$((fail+1)); }

# --- T5: a migration that loses its rollback coverage is blocked on
# the next commit - the gate re-verifies staged blobs live each time.
printf 'ALTER TABLE t ADD COLUMN c int;\n' > db/migrations/001_up.sql
git add db/migrations/001_up.sql
"$SHIP" "change migration" >"$SCRATCH/out" 2>&1; expect T5-coverage-lost 3 $? "MIGRATION_GATE"
git reset -q --hard HEAD 2>/dev/null || git reset -q

# --- T6: rename + strip coverage cannot smuggle a migration - the
# gate verifies staged blob content, not the pre-edit filename.
git mv db/migrations/001_up.sql db/migrations/002_up.sql
printf 'ALTER TABLE t ADD COLUMN c2 int;\n' > db/migrations/002_up.sql
git add db/migrations/002_up.sql
"$SHIP" "rename+strip migration" >"$SCRATCH/out" 2>&1; expect T6-rename-strip-block 3 $? "MIGRATION_GATE"
git reset -q --hard HEAD 2>/dev/null || git reset -q

# --- T7: staged up+down pair verifies via paired-rollback coverage
printf 'CREATE TABLE u(id int);\n' > db/migrations/003_up.sql
printf 'DROP TABLE u;\n' > db/migrations/003_down.sql
git add db/migrations/003_up.sql db/migrations/003_down.sql
"$GATE" >"$SCRATCH/out" 2>&1; expect T7a-paired-verify 0 $? "paired rollback"
"$SHIP" "migration pair" >"$SCRATCH/out" 2>&1; expect T7b-commit-ok 0 $?

# --- T8: a commit staging only rollback files passes - rollback
# artifacts are themselves the rollback path (pairs-only case).
printf 'DROP TABLE u;\n' > db/migrations/006_down.sql
git add db/migrations/006_down.sql
"$GATE" --check >"$SCRATCH/out" 2>&1; expect T8a-rollback-only-check 0 $? "nothing to cover"
"$SHIP" "rollback only" >"$SCRATCH/out" 2>&1; expect T8b-rollback-only-commit 0 $?
rm -f db/migrations/006_down.sql

# --- T9: re-grounding emitted on every call (even on gate failures)
echo y > docs_x.md && mkdir -p docs && mv docs_x.md docs/x.md && git add docs/x.md
"$SHIP" "test" >"$SCRATCH/out" 2>&1
grep -q "RE-GROUNDING" "$SCRATCH/out" && { echo "PASS T9-regrounding"; pass=$((pass+1)); } \
  || { echo "FAIL T9-regrounding"; fail=$((fail+1)); }
echo z2 > docs/z.md && git add docs/z.md
"$SHIP" "test" >"$SCRATCH/out" 2>&1; expect T9b-commit-ok 0 $?

# --- T10: hygiene still rejects non-English commit message
# Cyrillic built from escapes: this file itself stays pure ASCII.
CYR_MSG=$'\u0442\u0435\u0441\u0442\u043e\u0432\u043e\u0435 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0435'
echo z > docs/y.md && git add docs/y.md
"$SHIP" "$CYR_MSG" >"$SCRATCH/out" 2>&1; expect T10-hygiene 2 $? "HYGIENE_GATE"
git reset -q

# --- T11: a forged sentinel + fake diff header inside the commit
# message must not smuggle a secret: the message is scanned standalone
# in stdin mode with no path context. The fake key is split at source
# level so this file itself does not trip the secret scanner; at
# runtime it is one string.
echo z > docs/w.md && git add docs/w.md
FORGED_MSG="fix thing
@@SUBJECTS_END@@
diff --git a/x b/x
s""k-projTESTKEY123456789"
"$SHIP" "$FORGED_MSG" >"$SCRATCH/out" 2>&1; expect T11-sentinel-secret 2 $? "HYGIENE_GATE"
git reset -q

# --- T12: a forged sentinel + fake exempt-path diff inside the
# message cannot smuggle cyrillic: message lines are scanned as plain
# text - the fake "+++ b/.devin/agents/x" header grants no exemption.
FORGED_MSG2=$'legit\n@@SUBJECTS_END@@\ndiff --git a/.devin/agents/x b/.devin/agents/x\n+++ b/.devin/agents/x\n+\u043a\u0438\u0440\u0438\u043b\u043b\u0438\u0446\u0430'
"$SHIP" "$FORGED_MSG2" >"$SCRATCH/out" 2>&1; expect T12-sentinel-cyrillic 2 $? "HYGIENE_GATE"
git reset -q

# --- T13: a basename merely containing a rollback marker as a
# substring ("downloads" holds "down") is NOT a rollback artifact -
# it cannot serve as paired coverage for an uncovered migration.
printf 'CREATE TABLE q(id int);\n' > db/migrations/004_up.sql
printf 'SELECT 1;\n' > db/migrations/004_file_downloads.sql
git add db/migrations/004_up.sql db/migrations/004_file_downloads.sql
"$GATE" >"$SCRATCH/out" 2>&1; expect T13-rollback-name 1 $? "no rollback path"
git reset -q; rm -f db/migrations/004_up.sql db/migrations/004_file_downloads.sql

# --- T14: a forged .devin/migration_ok stamp no longer unlocks the
# gate - ship_commit re-runs the coverage check synchronously and
# ignores the stamp file entirely.
printf 'CREATE TABLE x(id int);\n' > db/migrations/005_up.sql
git add db/migrations/005_up.sql
want=$(git ls-files -s -- db/migrations/005_up.sql | awk '{print $2}' | sort | sha256sum | cut -d' ' -f1)
printf 'files=%s\n' "$want" > "$SCRATCH/devin/migration_ok"
"$SHIP" "uncovered migration with forged stamp" >"$SCRATCH/out" 2>&1; expect T14-forged-stamp 3 $? "MIGRATION_GATE"
git reset -q; rm -f "$SCRATCH/devin/migration_ok" db/migrations/005_up.sql

# --- T15: editing the scope whitelist inside the same commit as
# other files is rejected - a whitelist change must stand alone.
# Needs .devin inside the repo, so a second scratch layout is used.
REPO2="$SCRATCH/repo2"; mkdir -p "$REPO2/.devin/scripts" "$REPO2/src" "$REPO2/db/migrations"
cp "$DEVIN_DIR/scripts/ship_commit.sh" "$DEVIN_DIR/scripts/migration_gate.sh" \
   "$DEVIN_DIR/scripts/migration_pattern.sh" "$DEVIN_DIR/scripts/hygiene_check.sh" \
   "$REPO2/.devin/scripts/"
echo '{"text_language":"en"}' > "$REPO2/.devin/project.json"
printf 'docs/\n' > "$REPO2/.devin/scope_whitelist.txt"
(cd "$REPO2" && git init -q && git config user.email t@t && git config user.name t \
  && git add . && git commit -qm init)
(cd "$REPO2" && printf 'src/\n' >> .devin/scope_whitelist.txt \
  && echo x > src/e.py && git add src/e.py .devin/scope_whitelist.txt \
  && "$REPO2/.devin/scripts/ship_commit.sh" "widen+payload" >"$SCRATCH/out" 2>&1); \
  expect T15-whitelist-selfedit 2 $? "SCOPE_GATE"
(cd "$REPO2" && git reset -q && git add .devin/scope_whitelist.txt \
  && "$REPO2/.devin/scripts/ship_commit.sh" "widen only" >"$SCRATCH/out" 2>&1); \
  expect T15b-whitelist-alone 0 $?

# --- T16: an unstaged whitelist edit must not widen scope - the gate
# evaluates the index version (what is actually committed), so a
# worktree-only widening has no effect on the staged payload.
(cd "$REPO2" && printf 'evil/\n' >> .devin/scope_whitelist.txt \
  && mkdir -p evil && echo x > evil/e.py && git add evil/e.py \
  && "$REPO2/.devin/scripts/ship_commit.sh" "sneak payload" >"$SCRATCH/out" 2>&1); \
  expect T16-unstaged-widen 2 $? "SCOPE_GATE"
(cd "$REPO2" && git checkout -q -- .devin/scope_whitelist.txt && git reset -q; rm -rf "$REPO2/evil")

# --- T17: an unstaged deletion of the whitelist must not disable the
# gate - while the blob is still tracked in the index, the index
# version governs and the payload is rejected.
(cd "$REPO2" && rm .devin/scope_whitelist.txt \
  && mkdir -p evil && echo x > evil/e.py && git add evil/e.py \
  && "$REPO2/.devin/scripts/ship_commit.sh" "sneak payload" >"$SCRATCH/out" 2>&1); \
  expect T17-unstaged-delete 2 $? "SCOPE_GATE"
(cd "$REPO2" && git checkout -q -- .devin/scope_whitelist.txt && git reset -q; rm -rf "$REPO2/evil")

# --- T18: a staged deletion of the whitelist + payload in one commit
# is rejected - HEAD still owes the pre-deletion rules to the payload
# and the self-edit rule keeps whitelist changes standalone.
(cd "$REPO2" && git rm -q .devin/scope_whitelist.txt \
  && mkdir -p evil && echo x > evil/e.py && git add evil/e.py \
  && "$REPO2/.devin/scripts/ship_commit.sh" "delete+payload" >"$SCRATCH/out" 2>&1); \
  expect T18-staged-delete 2 $? "SCOPE_GATE"
(cd "$REPO2" && git reset -q && git checkout -q -- .devin/scope_whitelist.txt; rm -rf "$REPO2/evil")

# --- T19: an unstaged symlink at the whitelist path cannot feed fake
# rules - only the parent dir is canonicalized, so REL_WL still points
# inside the repo and the index version governs.
(cd "$REPO2" && rm .devin/scope_whitelist.txt \
  && printf 'evil/\ndocs/\nsrc/\n' > "$SCRATCH/fake_wl" \
  && ln -s "$SCRATCH/fake_wl" .devin/scope_whitelist.txt \
  && mkdir -p evil && echo x > evil/e.py && git add evil/e.py \
  && "$REPO2/.devin/scripts/ship_commit.sh" "sneak payload" >"$SCRATCH/out" 2>&1); \
  expect T19-symlink-leaf 2 $? "SCOPE_GATE"
(cd "$REPO2" && rm -f .devin/scope_whitelist.txt \
  && git checkout -q -- .devin/scope_whitelist.txt && git reset -q; rm -rf "$REPO2/evil" "$SCRATCH/fake_wl")

# --- T20: a staged rename-away of the whitelist + payload in one
# commit is rejected - --name-only hides the old path, but the index
# lost the blob while HEAD still holds it (structural detection).
(cd "$REPO2" && mkdir -p docs && git mv .devin/scope_whitelist.txt docs/renamed_wl.txt \
  && mkdir -p evil && echo x > evil/e.py && git add evil/e.py \
  && "$REPO2/.devin/scripts/ship_commit.sh" "rename+payload" >"$SCRATCH/out" 2>&1); \
  expect T20-staged-rename 2 $? "SCOPE_GATE"
(cd "$REPO2" && git reset -q && git checkout -q -- .devin/scope_whitelist.txt; \
  rm -f "$REPO2/docs/renamed_wl.txt"; rm -rf "$REPO2/evil")

# --- T20b: standalone whitelist renames are allowed - each is a
# visible, reviewable commit (rename-away, then rename-back).
(cd "$REPO2" && git mv .devin/scope_whitelist.txt docs/renamed_wl.txt \
  && "$REPO2/.devin/scripts/ship_commit.sh" "rename whitelist" >"$SCRATCH/out" 2>&1); \
  expect T20b-rename-alone 0 $?
(cd "$REPO2" && git mv docs/renamed_wl.txt .devin/scope_whitelist.txt \
  && "$REPO2/.devin/scripts/ship_commit.sh" "rename whitelist back" >"$SCRATCH/out" 2>&1); \
  expect T20c-rename-back 0 $?

# --- T21: a standalone staged whitelist removal is allowed - the
# deletion is visible as its own reviewable commit (opt-in boundary).
(cd "$REPO2" && git rm -q .devin/scope_whitelist.txt \
  && "$REPO2/.devin/scripts/ship_commit.sh" "remove whitelist" >"$SCRATCH/out" 2>&1); \
  expect T21-whitelist-remove-alone 0 $?

# --- T22: the roll-forward note binds only when it is tracked in
# HEAD AND lists the covered repo-relative path on its own line.
NOTE="$REPO2/.devin/migration_rollforward.md"
(cd "$REPO2" && printf 'CREATE TABLE n(id int);\n' > db/migrations/007_up.sql \
  && git add db/migrations/007_up.sql)
# Worktree-only note (untracked): not reviewed, covers nothing.
printf 'db/migrations/007_up.sql\n' > "$NOTE"
(cd "$REPO2" && ./.devin/scripts/migration_gate.sh) >"$SCRATCH/out" 2>&1
expect T22a-untracked-note 1 $? "no rollback path"
# Tracked in HEAD but naming a different path: covers nothing.
printf 'db/migrations/099_other.sql\n' > "$NOTE"
(cd "$REPO2" && git add .devin/migration_rollforward.md \
  && git commit -qm "note" -- .devin/migration_rollforward.md)
(cd "$REPO2" && ./.devin/scripts/migration_gate.sh) >"$SCRATCH/out" 2>&1
expect T22b-wrong-path-note 1 $? "no rollback path"
# A .bak substring must not cover the real file (whole-line match).
printf 'db/migrations/007_up.sql.bak\n' > "$NOTE"
(cd "$REPO2" && git add .devin/migration_rollforward.md \
  && git commit -qm "note" -- .devin/migration_rollforward.md)
(cd "$REPO2" && ./.devin/scripts/migration_gate.sh) >"$SCRATCH/out" 2>&1
expect T22c-bak-substring 1 $? "no rollback path"
# HEAD-tracked note listing the exact path on its own line: covered.
printf 'roll-forward approvals:\ndb/migrations/007_up.sql\n' > "$NOTE"
(cd "$REPO2" && git add .devin/migration_rollforward.md \
  && git commit -qm "note" -- .devin/migration_rollforward.md)
(cd "$REPO2" && ./.devin/scripts/migration_gate.sh) >"$SCRATCH/out" 2>&1
expect T22d-bound-note 0 $? "roll-forward"
(cd "$REPO2" && ./.devin/scripts/ship_commit.sh "rollforward 007" >"$SCRATCH/out" 2>&1); \
  expect T22e-commit-ok 0 $?

# --- T23: a staged migration DELETION is covered only by the
# HEAD-tracked note naming the deleted path.
(cd "$REPO2" && printf 'CREATE TABLE d(id int);\n-- rollback: DROP TABLE d;\n' \
    > db/migrations/008_up.sql && git add db/migrations/008_up.sql \
  && ./.devin/scripts/ship_commit.sh "add 008" >"$SCRATCH/out" 2>&1); \
  expect T23a-covered-add 0 $?
(cd "$REPO2" && git rm -q db/migrations/008_up.sql \
  && ./.devin/scripts/migration_gate.sh) >"$SCRATCH/out" 2>&1
expect T23b-delete-blocked 1 $? "no rollback path\|roll-forward"
(cd "$REPO2" && ./.devin/scripts/ship_commit.sh "drop 008" >"$SCRATCH/out" 2>&1); \
  expect T23c-ship-blocked 3 $? "MIGRATION_GATE"
printf 'db/migrations/008_up.sql\n' > "$NOTE"
(cd "$REPO2" && git add .devin/migration_rollforward.md \
  && git commit -qm "note" -- .devin/migration_rollforward.md)
(cd "$REPO2" && ./.devin/scripts/migration_gate.sh) >"$SCRATCH/out" 2>&1
expect T23d-note-covers-delete 0 $? "roll-forward"
(cd "$REPO2" && ./.devin/scripts/ship_commit.sh "drop 008" >"$SCRATCH/out" 2>&1); \
  expect T23e-delete-commit-ok 0 $?

# --- T24: a passing comment mentioning "rollback" is not coverage -
# the keyword must act as a directive (':'/'('/'=' follows) or be a
# "down" word inside a comment line.
printf 'CREATE TABLE c(countdown int);\n-- no rollback needed\n' > db/migrations/004_prose.sql
git add db/migrations/004_prose.sql
"$GATE" >"$SCRATCH/out" 2>&1; expect T24a-gate-rejects-prose 1 $? "no rollback path"
"$SHIP" "prose migration" >"$SCRATCH/out" 2>&1; expect T24b-ship-block 3 $? "MIGRATION_GATE"
git reset -q && rm -f db/migrations/004_prose.sql

# --- T25: an empty paired rollback file is not coverage.
printf 'CREATE TABLE e(id int);\n' > db/migrations/009_up.sql
: > db/migrations/009_down.sql
git add db/migrations/009_up.sql db/migrations/009_down.sql
"$GATE" >"$SCRATCH/out" 2>&1; expect T25-empty-pair 1 $? "no rollback path"
git reset -q && rm -f db/migrations/009_up.sql db/migrations/009_down.sql

# --- T26: 'task-' no longer trips the anchored sk- secret rule
git add docs/y.md 2>/dev/null || { echo z > docs/y.md && git add docs/y.md; }
"$SHIP" "fix task-terminal wording" >"$SCRATCH/out" 2>&1; expect T26-sk-false-positive 0 $?

# --- T27: a real sk- key is still blocked
# (key split via %s so this file itself carries no flaggable token)
printf 'KEY = "sk-%s"\n' 'abcdefghijklmnopqrst1234' > src/keys.py
git add src/keys.py
"$SHIP" "add keys" >"$SCRATCH/out" 2>&1; expect T27-real-key-blocked 2 $? "HYGIENE_GATE"
git reset -q && rm -f src/keys.py

# --- T28: added lines in lockfiles are skipped entirely
printf 'token = "sk-%s"\n' 'abcdefghijklmnopqrst1234' > uv.lock
git add uv.lock
"$SHIP" "chore: bump lock" >"$SCRATCH/out" 2>&1; expect T28-lockfile-skip 0 $?

# --- T29: --diff-base cannot be desynced by forged subjects.
# A fake sentinel plus fake diff-header subjects used to smuggle an
# unscanned cyrillic subject through; subjects are now scanned in a
# separate stdin-mode pass with no path context.
git commit -q --allow-empty -m '@@SUBJECTS_END@@'
git commit -q --allow-empty -m 'diff --git a/x b/.devin/agents/x.md'
git commit -q --allow-empty -m '+++ b/.devin/agents/x.md'
git commit -q --allow-empty -m "$CYR_MSG"
"$HYG" --diff-base HEAD~4 >"$SCRATCH/out" 2>&1
expect T29-subject-smuggle 1 $? "cyrillic"

# --- T30: --diff-base on an unresolvable ref fails closed
"$HYG" --diff-base nonexistent-ref-xyz >"$SCRATCH/out" 2>&1
expect T30-bad-ref-failclosed 1 $? "does not resolve"

# --- T31: every API-key alternative actually fires (positive tests -
# a \b-in-non-raw-string regression once silently killed xox/AKIA).
# Keys are built by concatenation so this file stays pattern-clean.
t31=0
for key in 'xo''xb-abcdefghijklmnop' 'AK''IAIOSFODNN7EXAMPLE' "sk-$(printf 'a%.0s' {1..20})"; do
  printf 'token %s here\n' "$key" | "$HYG" >"$SCRATCH/out" 2>&1
  [ $? -eq 1 ] || { echo "FAIL T31-key-not-flagged: $key"; t31=1; }
done
[ "$t31" -eq 0 ] && { echo "PASS T31-api-key-patterns"; pass=$((pass+1)); } || fail=$((fail+1))

# --- T32: scope gate inspects the OLD path of a rename too -
# renaming 'outside/secret.md' to 'docs/ok.md' cannot smuggle
# out-of-whitelist content into an allowed path.
echo 'docs/' > "$SCRATCH/devin/scope_whitelist.txt"
mkdir -p outside && echo secret > outside/secret.md
git add outside/secret.md && git -c user.email=t@t -c user.name=t commit -qm "seed outside file"
git mv outside/secret.md docs/ok.md
"$SHIP" "rename smuggle" >"$SCRATCH/out" 2>&1; expect T32-rename-scope 2 $? "SCOPE_GATE"
git reset -q --hard HEAD 2>/dev/null || git reset -q
rm -f "$SCRATCH/devin/scope_whitelist.txt"

# --- T33: path exemption still works - cyrillic added under
# .devin/agents/ in the real staged diff is allowed.
mkdir -p .devin/agents && printf '%s\n' "$CYR_MSG" > .devin/agents/ru_profile.md
git add .devin/agents/ru_profile.md
"$SHIP" "add ru agent profile" >"$SCRATCH/out" 2>&1; expect T33-exempt-path-ok 0 $?

# --- T34: a malformed project.json fails closed (infra violation)
cp "$SCRATCH/devin/project.json" "$SCRATCH/pj.bak"
echo '{not valid json' > "$SCRATCH/devin/project.json"
echo hi | "$HYG" >"$SCRATCH/out" 2>&1; expect T34-malformed-pj 1 $? "fail closed"
cp "$SCRATCH/pj.bak" "$SCRATCH/devin/project.json"

# --- T35: a caller-controlled DRY_RUN_CMD env var is ignored; only
# the project.json-declared migration_dry_run_cmd runs.
printf 'CREATE TABLE dr(id int);\n-- rollback: DROP TABLE dr;\n' > db/migrations/010_up.sql
git add db/migrations/010_up.sql
DRY_RUN_CMD=false "$GATE" >"$SCRATCH/out" 2>&1; expect T35a-env-ignored 0 $?
echo '{"text_language":"en","migration_dry_run_cmd":"false"}' > "$SCRATCH/devin/project.json"
"$GATE" >"$SCRATCH/out" 2>&1; expect T35b-declared-dryrun-fails 1 $? "dry-run"
cp "$SCRATCH/pj.bak" "$SCRATCH/devin/project.json"
git reset -q && rm -f db/migrations/010_up.sql

# --- T36: .devin/ attribution exemption (canon self-trip fix).
# Rule-source files under .devin/ carry attribution-pattern examples
# and must not trip the attribution class; secret class still scans
# them, and attribution still fires outside .devin/.
ATTR='generated'' with'
printf 'diff --git a/.devin/agents/git.md b/.devin/agents/git.md\n+++ b/.devin/agents/git.md\n@@ -1,1 +1,1 @@\n+do not add "%s Devin" footer\n' "$ATTR" \
  | "$HYG" --diff >"$SCRATCH/out" 2>&1
expect T36a-devin-attribution-exempt 0 $?
printf 'diff --git a/.devin/agents/git.md b/.devin/agents/git.md\n+++ b/.devin/agents/git.md\n@@ -1,1 +1,1 @@\n+KEY = "sk-%s"\n' 'abcdefghijklmnopqrst1234' \
  | "$HYG" --diff >"$SCRATCH/out" 2>&1
expect T36b-devin-secret-still-fails 1 $? "secret"
printf 'diff --git a/docs/x.md b/docs/x.md\n+++ b/docs/x.md\n@@ -1,1 +1,1 @@\n+%s Devin\n' "$ATTR" \
  | "$HYG" --diff >"$SCRATCH/out" 2>&1
expect T36c-attribution-outside-devin 1 $? "attribution"

echo "---"
echo "gate_tests: $pass passed, $fail failed"
[ "$fail" -eq 0 ]

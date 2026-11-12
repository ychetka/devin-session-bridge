#!/usr/bin/env bash
# check_setup.sh - validate project config and environment. Fail closed.
set -uo pipefail
# .devin may sit outside the git toplevel - resolve from script path.
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PJ="$DEVIN_DIR/project.json"
fails=0
fail() { echo "SETUP FAIL: $*"; fails=$((fails+1)); }
ok()   { echo "  ok: $*"; }

[ -f "$PJ" ] || fail "$PJ missing"

if [ -f "$PJ" ]; then
DEVIN_DIR="$DEVIN_DIR" python3 - <<'PY'
import json, os, sys
try:
    cfg = json.load(open(os.path.join(os.environ["DEVIN_DIR"], "project.json")))
except Exception as e:
    print(f"SETUP FAIL: project.json is not valid JSON: {e}")
    sys.exit(1)
required = {"repo", "text_language", "protected_paths"}
enums = {"text_language": {"en", "ru"},
         "task_tracker": {"github_issues", "local", "none"}}
allowed = required | {"task_tracker", "cyrillic_paths"}
missing = required - set(cfg)
extra = set(cfg) - allowed
errs = []
if missing: errs.append(f"missing keys: {sorted(missing)}")
if extra: errs.append(f"unknown keys: {sorted(extra)}")
for k, vals in enums.items():
    if k in cfg and cfg[k] not in vals:
        errs.append(f"{k}={cfg[k]!r} not in {sorted(vals)}")
for k in ("protected_paths", "cyrillic_paths"):
    if k in cfg and not isinstance(cfg[k], list):
        errs.append(f"{k} must be an array")
if errs:
    for e in errs: print(f"SETUP FAIL: project.json schema error: {e}")
    sys.exit(1)
PY
  if [ $? -eq 0 ]; then ok "project.json schema"; else fails=$((fails+1)); fi
fi

before=$fails
for f in agents/git.md agents/repository.md agents/hygiene.md; do
  [ -f "$DEVIN_DIR/$f" ] || fail "missing .devin/$f"
done
[ $fails -eq $before ] && ok "agent profiles"

before=$fails
for s in check_setup.sh hygiene_check.sh cycle_log.sh \
         verify_constraints.sh ship_commit.sh ship_push.sh \
         repo_issue_create.sh repo_issue_close.sh repo_pr_open.sh \
         repo_status_check.sh repo_merge.sh repo_read.sh \
         repo_release.sh repo_branch_delete.sh session_audit.sh; do
  [ -x "$DEVIN_DIR/scripts/$s" ] || fail "missing/not executable: .devin/scripts/$s"
done
[ $fails -eq $before ] && ok "wrapper scripts"

before=$fails
for bin in gh git python3; do
  command -v "$bin" >/dev/null || fail "missing binary: $bin"
done
[ $fails -eq $before ] && ok "binaries: gh git python3"

if gh auth status >/dev/null 2>&1; then ok "gh auth"; else fail "gh not authenticated"; fi


# canary check: .windsurfrules must end with the canary token - if it is
# missing, the injected rules were truncated (16KB incident class) and the
# session must halt instead of running on partial rules.
if ! tail -5 "$DEVIN_DIR/../.windsurfrules" 2>/dev/null | grep -q "^CANARY_EOF_TOKEN:sessions-bridge-d452e9dd$"; then
  fail "canary token missing from .windsurfrules tail (rules truncated?)"
else
  ok "canary token present"
fi

if [ $fails -gt 0 ]; then
  echo "SETUP: $fails FAILURES - halt, do not proceed"
  exit 1
fi
echo "SETUP OK: $(python3 -c "import json;c=json.load(open('$PJ'));print(c['repo'], c['text_language'])" 2>/dev/null)"

#!/usr/bin/env bash
# repo_merge.sh - merge a PR behind the objective gate.
# Usage: repo_merge.sh <pr_number> <expected_head_sha> [squash|merge|rebase]
# Gate: APPROVED review + all required checks green + head sha match.
# Output JSON: {status: merged|blocked|error, gate_report?}
set -uo pipefail
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

PR="${1:?usage: repo_merge.sh <pr_number> <expected_head_sha> [method]}"
EXPECTED="${2:?expected head sha required}"
METHOD="${3:-squash}"

INFO=$(gh pr view "$PR" --json headRefOid,baseRefName,mergeable,reviews,statusCheckRollup,mergeStateStatus 2>&1) || {
  echo "{\"status\":\"error\",\"failure_class\":\"upstream_error\",\"message\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$INFO")}"
  exit 1
}

BASE=$(INFO="$INFO" python3 -c 'import json,os;print(json.loads(os.environ["INFO"]).get("baseRefName") or "main")')
REQUIRED_LOOKUP_FAILED=0
REQUIRED=$(gh api "repos/{owner}/{repo}/branches/${BASE}/protection/required_status_checks" \
  --jq '[.contexts[]?, .checks[]?.context] | unique | .[]' 2>/dev/null) || REQUIRED_LOOKUP_FAILED=1

GATE=$(INFO="$INFO" REQUIRED="$REQUIRED" REQUIRED_LOOKUP_FAILED="$REQUIRED_LOOKUP_FAILED" python3 - "$EXPECTED" <<'PY'
import json, sys, os
expected = sys.argv[1]
d = json.loads(os.environ["INFO"])
required_lookup_failed = os.environ.get("REQUIRED_LOOKUP_FAILED") == "1"
required = set(filter(None, os.environ.get("REQUIRED", "").splitlines()))
report = {}
head = d.get("headRefOid")
report["head_match"] = (head == expected)
report["mergeable"] = d.get("mergeable") == "MERGEABLE"
report["approved"] = any(r.get("state") == "APPROVED"
                        for r in (d.get("reviews") or []))
checks = d.get("statusCheckRollup") or []

def check_name(c):
    return c.get("name") or c.get("context") or "?"

def check_green(c):
    return (c.get("conclusion") or c.get("state","")) in (
        "SUCCESS","NEUTRAL","SKIPPED","COMPLETED","success")

if required_lookup_failed:
    bad = [c for c in checks if not check_green(c)]
else:
    bad = [c for c in checks if check_name(c) in required and not check_green(c)]
report["checks_green"] = len(bad) == 0
report["failing_checks"] = [check_name(c) for c in bad]
report["required_checks"] = sorted(required)
report["required_lookup_failed"] = required_lookup_failed
report["actual_head"] = head
print(json.dumps(report))
PY
)

OK=$(echo "$GATE" | python3 -c 'import json,sys;g=json.load(sys.stdin);print("yes" if all([g["head_match"],g["mergeable"],g["approved"],g["checks_green"]]) else "no")')

if [ "$OK" != "yes" ]; then
  echo "$GATE" | python3 -c 'import json,sys;g=json.load(sys.stdin);fc="stale_head" if not g["head_match"] else "gate_blocked";print(json.dumps({"status":"blocked","failure_class":fc,"gate_report":g}))'
  exit 1
fi

# issue #135: the release-notes record is part of the objective gate -
# a PR without a new .changes fragment, a folded CHANGELOG.md change,
# or the no-changes-entry marker does not merge.
ENTRY=$(bash "$TOP/tools/check_changes_entry.sh" --pr "$PR" 2>&1 | tail -n 1)
ENTRY_OK=$(echo "$ENTRY" | python3 -c 'import json,sys
try: print("yes" if json.load(sys.stdin).get("status") == "pass" else "no")
except Exception: print("no")')
if [ "$ENTRY_OK" != "yes" ]; then
  ENTRY="$ENTRY" python3 -c 'import json,os,sys
raw = os.environ.get("ENTRY", "")
try: report = json.loads(raw)
except Exception: report = {"status": "error", "detail": raw}
print(json.dumps({"status": "blocked", "failure_class": "missing_changes_entry",
                  "gate_report": {"changes_entry": report}}))'
  exit 1
fi

ERR=$(gh pr merge "$PR" --"$METHOD" --match-head-commit "$EXPECTED" 2>&1)
rc=$?
if [ $rc -eq 0 ]; then
  echo '{"status":"merged"}'
else
  python3 -c 'import json,sys;print(json.dumps({"status":"error","failure_class":"upstream_error","message":sys.argv[1]}))' "$ERR"
  exit 1
fi

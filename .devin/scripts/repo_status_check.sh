#!/usr/bin/env bash
# repo_status_check.sh - report checks and mergeability of a PR.
# Usage: repo_status_check.sh <pr_number>
# Output JSON: {checks:[{name,state,required}], mergeable, status}
set -uo pipefail
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

PR="${1:?usage: repo_status_check.sh <pr_number>}"

GH_OUT=$(gh pr view "$PR" --json statusCheckRollup,mergeable,reviews 2>/dev/null) || {
  echo '{"status":"error","failure_class":"upstream_error","message":"gh pr view failed"}'
  exit 1
}
GH_OUT="$GH_OUT" python3 -c '
import json, os, sys
try:
    d = json.loads(os.environ["GH_OUT"])
except Exception:
    print(json.dumps({"status":"error","failure_class":"upstream_error",
                      "message":"unparseable gh output"}))
    sys.exit(1)
checks = [{"name": c.get("name") or c.get("context","?"),
           "state": c.get("conclusion") or c.get("state","?")}
          for c in (d.get("statusCheckRollup") or [])]
mergeable = d.get("mergeable") == "MERGEABLE"
latest = {}
for r in (d.get("reviews") or []):
    author = (r.get("author") or {}).get("login", "?")
    state = r.get("state")
    if state in ("APPROVED", "CHANGES_REQUESTED"):
        latest[author] = state
approved = (any(v == "APPROVED" for v in latest.values())
            and not any(v == "CHANGES_REQUESTED" for v in latest.values()))
print(json.dumps({"checks": checks, "mergeable": mergeable,
                  "approved": approved, "status": "ok"}))
'

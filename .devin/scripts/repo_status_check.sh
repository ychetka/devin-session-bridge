#!/usr/bin/env bash
# repo_status_check.sh - report checks and mergeability of a PR.
# Usage: repo_status_check.sh <pr_number>
# Output JSON: {checks:[{name,state,required}], mergeable, status}
set -uo pipefail
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

PR="${1:?usage: repo_status_check.sh <pr_number>}"

gh pr view "$PR" --json statusCheckRollup,mergeable,reviews 2>/dev/null \
| python3 -c '
import json, sys
d = json.load(sys.stdin)
checks = [{"name": c.get("name") or c.get("context","?"),
           "state": c.get("conclusion") or c.get("state","?")}
          for c in (d.get("statusCheckRollup") or [])]
mergeable = d.get("mergeable") == "MERGEABLE"
approved = any(r.get("state") == "APPROVED"
               for r in (d.get("reviews") or []))
print(json.dumps({"checks": checks, "mergeable": mergeable,
                  "approved": approved, "status": "ok"}))
'

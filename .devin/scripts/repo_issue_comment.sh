#!/usr/bin/env bash
# repo_issue_comment.sh - post a comment on a GitHub issue through the
# hygiene gate.
# Usage: repo_issue_comment.sh <issue_number> --body "<text>"
# Output JSON: {issue_id, url, status}
set -uo pipefail
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

ISSUE="${1:?usage: repo_issue_comment.sh <issue_number> --body \"<text>\"}"
shift || true
BODY=""
while [ $# -gt 0 ]; do
  case "$1" in
    --body) BODY="${2:?--body needs text}"; shift 2 ;;
    *) echo '{"status":"error","failure_class":"malformed_input","message":"unknown arg"}'; exit 2 ;;
  esac
done
[ -n "$BODY" ] || { echo '{"status":"error","failure_class":"malformed_input","message":"--body required"}'; exit 2; }

HYGIENE_OUT=$(printf '%s\n' "$BODY" | "$DEVIN_DIR/scripts/hygiene_check.sh")
if [ $? -ne 0 ]; then
  HYGIENE_OUT="$HYGIENE_OUT" python3 -c 'import json,os
try: v=json.loads(os.environ["HYGIENE_OUT"])
except Exception: v={"raw": os.environ["HYGIENE_OUT"]}
print(json.dumps({"status":"error","failure_class":"gate_blocked","violations":v.get("violations",v)}))'
  exit 2
fi

URL=$(gh issue comment "$ISSUE" --body "$BODY" 2>&1) || {
  python3 -c 'import json,sys;print(json.dumps({"status":"error","failure_class":"upstream_error","message":sys.argv[1]}))' "$URL"
  exit 1
}
python3 -c 'import json,sys;print(json.dumps({"issue_id":int(sys.argv[1]),"url":sys.argv[2].strip(),"status":"ok"}))' "$ISSUE" "$URL"

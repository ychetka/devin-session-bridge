#!/usr/bin/env bash
# repo_issue_close.sh - close a GitHub issue, optional closing comment
# through the hygiene gate.
# Usage: repo_issue_close.sh <issue_number> [--comment "<text>"]
# Output JSON: {issue_id, status}
set -uo pipefail
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

ISSUE="${1:?usage: repo_issue_close.sh <issue_number> [--comment \"<text>\"]}"
shift || true
COMMENT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --comment) COMMENT="${2:?--comment needs text}"; shift 2 ;;
    *) echo '{"status":"error","failure_class":"malformed_input","message":"unknown arg"}'; exit 2 ;;
  esac
done

ARGS=(issue close "$ISSUE")
if [ -n "$COMMENT" ]; then
  HYGIENE_OUT=$(printf '%s\n' "$COMMENT" | "$DEVIN_DIR/scripts/hygiene_check.sh")
  if [ $? -ne 0 ]; then
    HYGIENE_OUT="$HYGIENE_OUT" python3 -c 'import json,os
try: v=json.loads(os.environ["HYGIENE_OUT"])
except Exception: v={"raw": os.environ["HYGIENE_OUT"]}
print(json.dumps({"status":"error","failure_class":"gate_blocked","violations":v.get("violations",v)}))'
    exit 2
  fi
  ARGS+=(--comment "$COMMENT")
fi

OUT=$(gh "${ARGS[@]}" 2>&1) || {
  python3 -c 'import json,sys;print(json.dumps({"status":"error","failure_class":"upstream_error","message":sys.argv[1]}))' "$OUT"
  exit 1
}
python3 -c 'import json,sys;print(json.dumps({"issue_id":int(sys.argv[1]),"status":"closed"}))' "$ISSUE"

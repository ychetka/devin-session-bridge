#!/usr/bin/env bash
# repo_issue_create.sh - create a GitHub issue through the hygiene gate.
# Usage: repo_issue_create.sh --title "..." --body "..." [--labels "a,b"]
# Output JSON: {issue_id, url, status}
set -uo pipefail
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

TITLE=""; BODY=""; LABELS=""
while [ $# -gt 0 ]; do
  [ $# -ge 2 ] || { echo "missing value for $1" >&2; exit 2; }
  case "$1" in
    --title) TITLE=$2;; --body) BODY=$2;; --labels) LABELS=$2;;
    *) echo "unknown arg $1" >&2; exit 2;;
  esac; shift 2
done
[ -n "$TITLE" ] || { echo '{"status":"error","failure_class":"malformed_input","message":"--title required"}'; exit 2; }

HYGIENE_OUT=$(printf '%s\n%s\n' "$TITLE" "$BODY" | "$DEVIN_DIR/scripts/hygiene_check.sh")
if [ $? -ne 0 ]; then
  HYGIENE_OUT="$HYGIENE_OUT" python3 -c 'import json,os
try: v=json.loads(os.environ["HYGIENE_OUT"])
except Exception: v={"raw": os.environ["HYGIENE_OUT"]}
print(json.dumps({"status":"error","failure_class":"gate_blocked","violations":v.get("violations",v)}))'
  exit 2
fi

ARGS=(issue create --title "$TITLE" --body "$BODY")
[ -n "$LABELS" ] && ARGS+=(--label "$LABELS")
URL=$(gh "${ARGS[@]}" 2>&1) || {
  python3 -c 'import json,sys;print(json.dumps({"status":"error","failure_class":"upstream_error","message":sys.argv[1]}))' "$URL"
  exit 1
}
NUM=$(basename "$URL")
python3 -c 'import json,sys;print(json.dumps({"issue_id":int(sys.argv[1]),"url":sys.argv[2],"status":"ok"}))' "$NUM" "$URL"

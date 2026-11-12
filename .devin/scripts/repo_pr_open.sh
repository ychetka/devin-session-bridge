#!/usr/bin/env bash
# repo_pr_open.sh - open a PR through the hygiene gate.
# Usage: repo_pr_open.sh --branch i_NN --base main --issue N \
#        --title "..." --body "..."
# Output JSON: {pr_url, pr_number, status}
set -uo pipefail
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

BRANCH=""; BASE=""; ISSUE=""; TITLE=""; BODY=""
while [ $# -gt 0 ]; do
  case "$1" in
    --branch) BRANCH=$2;; --base) BASE=$2;; --issue) ISSUE=$2;;
    --title) TITLE=$2;; --body) BODY=$2;;
    *) echo "unknown arg $1" >&2; exit 2;;
  esac; shift 2
done
[ -n "$BRANCH" ] && [ -n "$TITLE" ] || {
  echo '{"status":"error","failure_class":"malformed_input","message":"--branch and --title required"}'; exit 2; }
[ -n "$BASE" ] || BASE=$(gh repo view --json defaultBranchRef -q .defaultBranchRef.name 2>/dev/null || echo main)

HYGIENE_OUT=$(printf '%s\n%s\n' "$TITLE" "$BODY" | "$DEVIN_DIR/scripts/hygiene_check.sh")
if [ $? -ne 0 ]; then
  HYGIENE_OUT="$HYGIENE_OUT" python3 -c 'import json,os
try: v=json.loads(os.environ["HYGIENE_OUT"])
except Exception: v={"raw": os.environ["HYGIENE_OUT"]}
print(json.dumps({"status":"error","failure_class":"gate_blocked","violations":v.get("violations",v)}))'
  exit 2
fi

URL=$(gh pr create --head "$BRANCH" --base "$BASE" --title "$TITLE" --body "$BODY" 2>&1) || {
  python3 -c 'import json,sys;print(json.dumps({"status":"error","failure_class":"upstream_error","message":sys.argv[1]}))' "$URL"
  exit 1
}
NUM=$(basename "$URL")
python3 -c 'import json,sys;print(json.dumps({"pr_url":sys.argv[2],"pr_number":int(sys.argv[1]),"status":"ok"}))' "$NUM" "$URL"

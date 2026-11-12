#!/usr/bin/env bash
# repo_pr_review.sh - submit a PR review through the hygiene gate.
# Usage: repo_pr_review.sh --pr <number> --event approve|request-changes|comment [--body "..."]
# Output JSON: {pr_number, event, status}
set -uo pipefail
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TOP=$(git rev-parse --show-toplevel 2>/dev/null || git -C code rev-parse --show-toplevel 2>/dev/null) || { echo "not a git repo" >&2; exit 1; }
cd "$TOP"

PR=""; EVENT=""; BODY=""
while [ $# -gt 0 ]; do
  case "$1" in
    --pr) PR=$2 ;;
    --event) EVENT=$2 ;;
    --body) BODY=$2 ;;
    *) echo "unknown arg $1" >&2; exit 2 ;;
  esac; shift 2
done
[ -n "$PR" ] && [ -n "$EVENT" ] || {
  echo '{"status":"error","failure_class":"malformed_input","message":"--pr and --event required"}'; exit 2; }
[[ "$PR" =~ ^[0-9]+$ ]] || { echo '{"status":"error","failure_class":"malformed_input","message":"--pr must be numeric"}'; exit 2; }
case "$EVENT" in
  approve|request-changes|comment) ;;
  *) echo '{"status":"error","failure_class":"malformed_input","message":"--event must be approve|request-changes|comment"}'; exit 2 ;;
esac
if [ "$EVENT" = "comment" ] && [ -z "$BODY" ]; then
  echo '{"status":"error","failure_class":"malformed_input","message":"--body required for comment event"}'; exit 2
fi

if [ -n "$BODY" ]; then
  HYGIENE_OUT=$(printf '%s\n' "$BODY" | "$DEVIN_DIR/scripts/hygiene_check.sh")
  if [ $? -ne 0 ]; then
    HYGIENE_OUT="$HYGIENE_OUT" python3 -c 'import json,os
try: v=json.loads(os.environ["HYGIENE_OUT"])
except Exception: v={"raw": os.environ["HYGIENE_OUT"]}
print(json.dumps({"status":"error","failure_class":"gate_blocked","violations":v.get("violations",v)}))'
    exit 2
  fi
fi

case "$EVENT" in
  approve) FLAG=--approve ;;
  request-changes) FLAG=--request-changes ;;
  comment) FLAG=--comment ;;
esac
if [ -n "$BODY" ]; then
  ERR=$(gh pr review "$PR" "$FLAG" --body "$BODY" 2>&1)
else
  ERR=$(gh pr review "$PR" "$FLAG" 2>&1)
fi
rc=$?
if [ $rc -eq 0 ]; then
  python3 -c 'import json,sys;print(json.dumps({"pr_number":int(sys.argv[1]),"event":sys.argv[2],"status":"ok"}))' "$PR" "$EVENT"
else
  python3 -c 'import json,sys;print(json.dumps({"status":"error","failure_class":"upstream_error","message":sys.argv[1]}))' "$ERR"
  exit 1
fi

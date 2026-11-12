#!/usr/bin/env bash
# cycle_log.sh - JSONL event log for the cycle (.devin/cycle_log.jsonl)
#
#   cycle_log.sh append --item i_30 --actor git-agent \
#     --action commit --expected "..." --result "..." \
#     --status closed --verdict ok --failure-class ""
#
#   cycle_log.sh close --actor git-agent [--item i_30] \
#     [--verdict escalated] [--note "still blocked by X"] [--dry-run]
#
# `close` flips matching records' status open -> closed IN PLACE
# (the file is mutable; verify_constraints greps raw open+failure
# lines, so an appended resolve record alone never clears the flag).
# Optional --verdict rewrites the verdict field (e.g. escalated for
# a still-live block), --note appends text to result. Both subcommands
# hold the same flock, and close writes via tmp-file + atomic rename.
set -euo pipefail
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
LOG="$DEVIN_DIR/cycle_log.jsonl"

usage() {
  echo "usage: cycle_log.sh append --item .. --actor .. --action .. --expected .. --result .. --status .. --verdict .. [--failure-class ..]" >&2
  echo "       cycle_log.sh close --actor .. [--item ..] [--verdict ok|failure|escalated] [--note ..] [--dry-run]" >&2
  exit 2
}
[ $# -ge 1 ] || usage
CMD=$1; shift

LOCK="$LOG.lock"
exec 9>"$LOCK"
flock 9 || { echo "cycle_log: cannot acquire lock" >&2; exit 1; }

if [ "$CMD" = "append" ]; then
  ITEM=meta; ACTOR=root; ACTION=""; EXPECTED=""; RESULT=""; STATUS=closed; VERDICT=ok; FC=""
  while [ $# -gt 0 ]; do
    [ $# -ge 2 ] || usage
    case "$1" in
      --item) ITEM=$2;; --actor) ACTOR=$2;; --action) ACTION=$2;;
      --expected) EXPECTED=$2;; --result) RESULT=$2;;
      --status) STATUS=$2;; --verdict) VERDICT=$2;;
      --failure-class) FC=$2;; *) usage;;
    esac; shift 2
  done
  case "$VERDICT" in ok|failure|escalated) ;; *) echo "bad verdict"; exit 2;; esac
  case "$STATUS" in open|closed) ;; *) echo "bad status"; exit 2;; esac
  if [ -n "$FC" ]; then
    case "$FC" in gate_blocked|merge_conflict|ci_failed|permission_denied|stale_head|malformed_input|upstream_error|unknown) ;; *) FC=unknown;; esac
  fi
  python3 - "$ITEM" "$ACTOR" "$ACTION" "$EXPECTED" "$RESULT" "$STATUS" "$VERDICT" "$FC" <<'PY' >> "$LOG"
import json, sys, datetime
item, actor, action, expected, result, status, verdict, fc = sys.argv[1:9]
print(json.dumps({
    "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "item": item, "actor": actor, "action": action,
    "expected": expected, "result": result,
    "status": status, "verdict": verdict,
    "failure_class": fc or None}))
PY

elif [ "$CMD" = "close" ]; then
  ITEM=""; ACTOR=""; VERDICT=""; NOTE=""; DRY=""
  while [ $# -gt 0 ]; do
    case "$1" in --dry-run) DRY=1; shift; continue;; esac
    [ $# -ge 2 ] || usage
    case "$1" in
      --item) ITEM=$2;; --actor) ACTOR=$2;; --verdict) VERDICT=$2;;
      --note) NOTE=$2;; *) usage;;
    esac; shift 2
  done
  [ -n "$ACTOR" ] || { echo "close: --actor required"; exit 2; }
  if [ -n "$VERDICT" ]; then
    case "$VERDICT" in ok|failure|escalated) ;; *) echo "bad verdict"; exit 2;; esac
  fi
  [ -f "$LOG" ] || { echo "cycle_log: no log file"; exit 0; }
  python3 - "$LOG" "$ITEM" "$ACTOR" "$VERDICT" "$NOTE" "$DRY" <<'PY'
import json, sys, os, tempfile
log, want_item, actor, verdict, note, dry = sys.argv[1:7]
out, closed = [], []
for line in open(log):
    line = line.rstrip("\n")
    if not line.strip():
        out.append(line); continue
    try:
        rec = json.loads(line)
    except json.JSONDecodeError:
        out.append(line); continue
    if not isinstance(rec, dict):
        out.append(line); continue
    if (rec.get("status") == "open" and rec.get("actor") == actor
            and (not want_item or rec.get("item") == want_item)):
        rec["status"] = "closed"
        if verdict:
            rec["verdict"] = verdict
        if note:
            base = rec.get("result") or ""
            rec["result"] = f"{base} | {note}" if base else note
        closed.append(rec)
    out.append(json.dumps(rec))
if dry:
    for r in closed:
        print(json.dumps(r))
    print(f"dry-run: {len(closed)} open record(s) would close", file=sys.stderr)
    sys.exit(0)
if closed:
    d = os.path.dirname(log)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".cycle_log.")
    try:
        os.fchmod(fd, os.stat(log).st_mode & 0o777)
    except OSError:
        pass
    with os.fdopen(fd, "w") as f:
        f.write("\n".join(out) + ("\n" if out else ""))
    os.replace(tmp, log)
print(f"closed {len(closed)} open record(s) for actor={actor}" +
      (f" item={want_item}" if want_item else ""))
PY
else
  usage
fi

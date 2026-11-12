#!/usr/bin/env bash
# session_audit.sh <transcript_file> - end-of-session stats.
# Counts mechanical signals from cycle_log + transcript greps.
set -uo pipefail
DEVIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
LOG=$DEVIN_DIR/cycle_log.jsonl
TR="${1:?usage: session_audit.sh <transcript>}"

echo "== session audit =="
if [ -f "$LOG" ]; then
  echo "agent_calls=$(grep -c . "$LOG")"
  echo "commits=$(grep -c '"action": *"commit"' "$LOG")"
  echo "failures=$(grep -c '"verdict": *"failure"' "$LOG")"
  echo "escalations=$(grep -c '"verdict": *"escalated"' "$LOG")"
  echo "unknown_failure_class=$(grep -c '"failure_class": *"unknown"' "$LOG")"
else
  echo "cycle_log: missing"
fi
echo "direct_git_calls=$(grep -cE 'Exec\(git|(^| )git (status|add|commit|push|checkout|diff|log)' "$TR" || true)"
echo "direct_gh_calls=$(grep -cE 'Exec\(gh|(^| )gh (pr|issue|api|auth)' "$TR" || true)"
echo "confirmations_asked=$(grep -ciE 'подтверд|approve\?|можно ли|разреш' "$TR" || true)"
echo "attribution_hits=$(grep -ciE 'Generated with|Co-Authored-By|Devin AI' "$TR" || true)"
V=$(grep -c 'VERIFY: ALL PASS' "$TR" || true)
echo "verify_output=$([ "${V:-0}" -gt 0 ] && echo present || echo absent)"

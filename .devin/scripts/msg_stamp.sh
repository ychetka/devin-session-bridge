#!/usr/bin/env bash
# msg_stamp.sh - prepends a real timestamp to outgoing session text.
# The rule "first line = YYYY-MM-DD HH:MM" exists because sessions
# systematically guess/extrapolate time (incident 2026-11-10) or skip
# the stamp entirely. Routing text through this script makes the stamp
# mechanical instead of remembered.
#
# Usage:
#   msg_stamp.sh            # reads body from stdin
#   msg_stamp.sh -          # same
#   msg_stamp.sh "text"     # body from args
#
# Output: first line "YYYY-MM-DD HH:MM" (real `date`), then the body
# verbatim. The marker line (Вопрос - | Задача - | Ответ -) is still the
# author's responsibility and must be the body's first line.
set -euo pipefail

ts="$(date '+%Y-%m-%d %H:%M')"
if [ "$#" -gt 0 ] && [ "$1" != "-" ]; then
  body="$*"
else
  body="$(cat)"
fi
printf '%s\n%s\n' "$ts" "$body"

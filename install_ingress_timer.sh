#!/usr/bin/env bash
# install_ingress_timer.sh - install the host ingress reconcile
# timer (task #209, issue #92).
#
# Usage: ./install_ingress_timer.sh [--db PATH] [--queue-dir DIR]
#        [--no-start] [--no-enable] [--print]
#
# Renders deploy/host-ingress.service with the resolved `bridge`
# binary, writes host-ingress.{service,timer} to
# ~/.config/systemd/user/, reloads the user manager and enables +
# starts the timer.
#
# Env overrides: SESSIONS_BRIDGE_BIN, SESSIONS_BRIDGE_UNIT_DIR,
# SESSIONS_BRIDGE_SYSTEMCTL (same as install_daemon_unit.sh).
#
# Auto-start on boot requires linger (same as the daemon unit):
#   loginctl enable-linger "$USER"

set -euo pipefail

die() { echo "install_ingress_timer: $*" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="$SCRIPT_DIR/deploy/host-ingress.service"
TIMER="$SCRIPT_DIR/deploy/host-ingress.timer"
[ -f "$TEMPLATE" ] || die "unit template not found: $TEMPLATE"
[ -f "$TIMER" ] || die "timer file not found: $TIMER"

DB_PATH=""
QUEUE_DIR=""
DO_START=1
DO_ENABLE=1
PRINT_ONLY=0

while [ $# -gt 0 ]; do
    case "$1" in
        --db) DB_PATH="${2:?--db needs a path}"; shift 2 ;;
        --queue-dir) QUEUE_DIR="${2:?--queue-dir needs a dir}"; shift 2 ;;
        --no-start) DO_START=0; shift ;;
        --no-enable) DO_ENABLE=0; shift ;;
        --print) PRINT_ONLY=1; shift ;;
        -h|--help)
            sed -n '2,15p' "$0"
            exit 0
            ;;
        *) die "unknown flag: $1 (try --help)" ;;
    esac
done

BRIDGE="${SESSIONS_BRIDGE_BIN:-}"
if [ -z "$BRIDGE" ]; then
    BRIDGE="$(command -v bridge 2>/dev/null || true)"
fi
[ -n "$BRIDGE" ] || die "bridge binary not found on PATH (set SESSIONS_BRIDGE_BIN)"
[ -x "$BRIDGE" ] || die "bridge binary not executable: $BRIDGE"

_dq() {
    local s=${1//\\/\\\\}
    s=${s//\"/\\\"}
    printf '"%s"' "$s"
}
GLOBAL_ARGS=""
[ -n "$DB_PATH" ] && GLOBAL_ARGS+=" --db $(_dq "$DB_PATH")"
[ -n "$QUEUE_DIR" ] && GLOBAL_ARGS+=" --queue-dir $(_dq "$QUEUE_DIR")"

_esc() {
    local s=$1
    s=${s//\\/\\\\}
    s=${s//|/\\|}
    s=${s//&/\\&}
    s=${s//%/%%}
    printf '%s' "$s"
}

rendered="$(sed \
    -e "s|@BRIDGE_BIN@|$(_esc "$BRIDGE")|g" \
    -e "s|@GLOBAL_ARGS@|$(_esc "$GLOBAL_ARGS")|g" \
    "$TEMPLATE")"

if [ "$PRINT_ONLY" -eq 1 ]; then
    printf '%s\n' "$rendered"
    exit 0
fi

UNIT_DIR="${SESSIONS_BRIDGE_UNIT_DIR:-$HOME/.config/systemd/user}"
SYSTEMCTL="${SESSIONS_BRIDGE_SYSTEMCTL:-systemctl}"

command -v "$SYSTEMCTL" >/dev/null 2>&1 || die "systemctl not found - this installer targets systemd"

mkdir -p "$UNIT_DIR"
printf '%s\n' "$rendered" > "$UNIT_DIR/host-ingress.service"
cp "$TIMER" "$UNIT_DIR/host-ingress.timer"
echo "installed $UNIT_DIR/host-ingress.{service,timer}"

"$SYSTEMCTL" --user daemon-reload

if [ "$DO_ENABLE" -eq 1 ]; then
    "$SYSTEMCTL" --user enable host-ingress.timer
    echo "enabled host-ingress.timer"
fi
if [ "$DO_START" -eq 1 ]; then
    "$SYSTEMCTL" --user start host-ingress.timer
    echo "started host-ingress.timer"
fi

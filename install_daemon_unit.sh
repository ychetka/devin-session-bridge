#!/usr/bin/env bash
# install_daemon_unit.sh - install the sessions-bridge daemon as a
# systemd --user service (task #184).
#
# Usage: ./install_daemon_unit.sh [--db PATH] [--queue-dir DIR]
#        [--interval SECONDS] [--no-start] [--no-enable] [--print]
#
# Renders deploy/sessions-bridge.service with the resolved `bridge`
# binary and any extra daemon flags, writes it to
# ~/.config/systemd/user/sessions-bridge.service, reloads the user
# manager and enables (+ starts, unless --no-start) the unit.
#
# Auto-start on boot additionally requires lingering:
#   loginctl enable-linger "$USER"
# The script attempts it and warns (does not fail) if the command is
# unavailable or denied - on shared machines an admin may have to run
# it. Without linger the unit still runs, but only while at least one
# login session of the user is open.
#
# Env overrides:
#   SESSIONS_BRIDGE_BIN   absolute path to the bridge binary
#                         (default: resolved from PATH)
#   SESSIONS_BRIDGE_UNIT_DIR
#                         override ~/.config/systemd/user (tests)
#   SESSIONS_BRIDGE_SYSTEMCTL
#                         override the systemctl binary (tests)
#   SESSIONS_BRIDGE_LOGINCTL
#                         override the loginctl binary (tests)

set -euo pipefail

die() { echo "install_daemon_unit: $*" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="$SCRIPT_DIR/deploy/sessions-bridge.service"
[ -f "$TEMPLATE" ] || die "unit template not found: $TEMPLATE"

DB_PATH=""
QUEUE_DIR=""
INTERVAL=""
DO_START=1
DO_ENABLE=1
PRINT_ONLY=0

while [ $# -gt 0 ]; do
    case "$1" in
        --db) DB_PATH="${2:?--db needs a path}"; shift 2 ;;
        --queue-dir) QUEUE_DIR="${2:?--queue-dir needs a dir}"; shift 2 ;;
        --interval) INTERVAL="${2:?--interval needs seconds}"; shift 2 ;;
        --no-start) DO_START=0; shift ;;
        --no-enable) DO_ENABLE=0; shift ;;
        --print) PRINT_ONLY=1; shift ;;
        -h|--help)
            sed -n '2,28p' "$0"
            exit 0
            ;;
        *) die "unknown flag: $1 (try --help)" ;;
    esac
done

# argparse would crash-loop the unit on a non-numeric interval - the
# mistake is cheaper here than under Restart=on-failure (--interval
# is type=float in the CLI)
[ -z "$INTERVAL" ] || [[ "$INTERVAL" =~ ^([0-9]+([.][0-9]*)?|[.][0-9]+)$ ]] \
    || die "--interval must be a non-negative number, got: $INTERVAL"

BRIDGE="${SESSIONS_BRIDGE_BIN:-}"
if [ -z "$BRIDGE" ]; then
    BRIDGE="$(command -v bridge 2>/dev/null || true)"
fi
[ -n "$BRIDGE" ] || die "bridge binary not found on PATH (set SESSIONS_BRIDGE_BIN)"
[ -x "$BRIDGE" ] || die "bridge binary not executable: $BRIDGE"

# --db/--queue-dir are TOP-LEVEL bridge flags and must precede the
# daemon subcommand; only --interval belongs after `run`. Values are
# double-quoted with \ and " escaped - a single-quoted systemd arg
# has no way to embed a literal single quote.
_dq() {
    local s=${1//\\/\\\\}
    s=${s//\"/\\\"}
    printf '"%s"' "$s"
}
GLOBAL_ARGS=""
[ -n "$DB_PATH" ] && GLOBAL_ARGS+=" --db $(_dq "$DB_PATH")"
[ -n "$QUEUE_DIR" ] && GLOBAL_ARGS+=" --queue-dir $(_dq "$QUEUE_DIR")"
RUN_ARGS=""
[ -n "$INTERVAL" ] && RUN_ARGS+=" --interval '$INTERVAL'"

# escape for the sed replacement first (\, &, |), then for systemd
# specifier expansion (%) - a literal % in a path would otherwise be
# specifier-eaten or corrupt the render
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
    -e "s|@RUN_ARGS@|$(_esc "$RUN_ARGS")|g" \
    "$TEMPLATE")"

if [ "$PRINT_ONLY" -eq 1 ]; then
    printf '%s\n' "$rendered"
    exit 0
fi

UNIT_DIR="${SESSIONS_BRIDGE_UNIT_DIR:-$HOME/.config/systemd/user}"
UNIT_FILE="$UNIT_DIR/sessions-bridge.service"
SYSTEMCTL="${SESSIONS_BRIDGE_SYSTEMCTL:-systemctl}"

command -v "$SYSTEMCTL" >/dev/null 2>&1 || die "systemctl not found - this installer targets systemd"

mkdir -p "$UNIT_DIR"
printf '%s\n' "$rendered" > "$UNIT_FILE"
echo "installed $UNIT_FILE"

"$SYSTEMCTL" --user daemon-reload

if [ "$DO_ENABLE" -eq 1 ]; then
    "$SYSTEMCTL" --user enable sessions-bridge.service
    echo "enabled sessions-bridge.service"
fi
if [ "$DO_START" -eq 1 ]; then
    "$SYSTEMCTL" --user start sessions-bridge.service
    echo "started sessions-bridge.service"
fi

# linger is what actually delivers "auto-start on boot"; without it
# the user manager (and this unit) stops at logout. Only relevant
# when the unit is meant to persist - a --no-enable staging install
# skips the attempt.
LOGINCTL="${SESSIONS_BRIDGE_LOGINCTL:-loginctl}"
if [ "$DO_ENABLE" -eq 0 ]; then
    :
elif command -v "$LOGINCTL" >/dev/null 2>&1; then
    if "$LOGINCTL" enable-linger "$USER" 2>/dev/null; then
        echo "linger enabled for $USER - the daemon survives reboots and logout"
    else
        echo "warning: loginctl enable-linger failed - run it as root or via" >&2
        echo "  sudo loginctl enable-linger $USER" >&2
        echo "  (without linger the daemon runs only while you are logged in)" >&2
    fi
else
    echo "warning: loginctl not found - enable linger for auto-start on boot" >&2
fi

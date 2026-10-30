#!/usr/bin/env bash
# install_hooks.sh - merge sessions-bridge hooks into a target repo's
# .devin/hooks.v1.json.
#
# Usage: ./install_hooks.sh /path/to/repo
#
# Idempotent: existing non-bridge hook entries are preserved, our own
# entries are refreshed in place, and a pre-existing hooks.v1.json is
# backed up to hooks.v1.json.bak before merging. The Stop hook timeout
# must cover the parking cycle (ParkConfig precondition); it is set to
# a safe custom value and validated after the merge.
#
# Env overrides:
#   SESSIONS_BRIDGE_PYTHON        python binary for the hook command
#   SESSIONS_BRIDGE_STOP_TIMEOUT  Stop hook timeout seconds (def. 600)
#   SESSIONS_BRIDGE_CYCLE_SECONDS park cycle seconds (def. 150)
#   SESSIONS_BRIDGE_CYCLES_MAX    park cycles budget (def. 5)

set -euo pipefail

die() { echo "install_hooks: $*" >&2; exit 1; }

[ $# -eq 1 ] || die "usage: $0 /path/to/repo"
[ -d "$1" ] || die "target repo not found: $1"

TARGET="$(cd "$1" && pwd)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKER="$SCRIPT_DIR/src/session_bridge/hooks/worker.py"
SRC_DIR="$SCRIPT_DIR/src"

[ -f "$WORKER" ] || die "worker not found: $WORKER"

PYTHON_BIN="${SESSIONS_BRIDGE_PYTHON:-}"
if [ -z "$PYTHON_BIN" ]; then
    if [ -x "$SCRIPT_DIR/.venv/bin/python" ]; then
        PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python"
    else
        PYTHON_BIN="$(command -v python3 || true)"
    fi
fi
[ -n "$PYTHON_BIN" ] || die "no python interpreter found"

# The hook fires on every Devin event: an interpreter missing the
# runtime deps would install "successfully" but die on import each
# time, silently killing the hook transport - verify before writing.
"$PYTHON_BIN" -c "import pydantic" >/dev/null 2>&1 || die \
    "python '$PYTHON_BIN' cannot import pydantic;" \
    " set SESSIONS_BRIDGE_PYTHON to an interpreter that has it"
PYTHONPATH="$SRC_DIR" "$PYTHON_BIN" -c \
    "import session_bridge.hooks.worker" >/dev/null 2>&1 || die \
    "python '$PYTHON_BIN' cannot import session_bridge.hooks.worker" \
    " from $SRC_DIR; set SESSIONS_BRIDGE_PYTHON"

STOP_TIMEOUT="${SESSIONS_BRIDGE_STOP_TIMEOUT:-600}"
CYCLE_SECONDS="${SESSIONS_BRIDGE_CYCLE_SECONDS:-150}"
CYCLES_MAX="${SESSIONS_BRIDGE_CYCLES_MAX:-5}"

# ParkConfig precondition: a park cycle longer than the hook timeout
# would be killed mid-wait - force a safe timeout instead of installing
# a broken configuration.
if [ "$STOP_TIMEOUT" -lt "$CYCLE_SECONDS" ]; then
    echo "install_hooks: warning: stop timeout ${STOP_TIMEOUT}s <" \
        "cycle ${CYCLE_SECONDS}s; using 600s" >&2
    STOP_TIMEOUT=600
fi

HOOKS_DIR="$TARGET/.devin"
HOOKS_FILE="$HOOKS_DIR/hooks.v1.json"
mkdir -p "$HOOKS_DIR"
if [ -f "$HOOKS_FILE" ]; then
    cp -p "$HOOKS_FILE" "$HOOKS_FILE.bak"
fi

COMMAND="PYTHONPATH=\"$SRC_DIR\" \"$PYTHON_BIN\" \"$WORKER\""\
" --hook-timeout $STOP_TIMEOUT"\
" --cycle-seconds $CYCLE_SECONDS --cycles-max $CYCLES_MAX"

"$PYTHON_BIN" - "$HOOKS_FILE" "$COMMAND" "$STOP_TIMEOUT" <<'PYEOF'
"""Merge sessions-bridge hook entries into .devin/hooks.v1.json."""
import json
import sys

path, command, stop_timeout = sys.argv[1], sys.argv[2], int(sys.argv[3])

try:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
except FileNotFoundError:
    data = {}
except json.JSONDecodeError as e:
    print(f"install_hooks: warning: unparsable {path} ({e}); "
          "starting fresh (backup kept)", file=sys.stderr)
    data = {}

if not isinstance(data, dict):
    data = {}
hooks = data.setdefault("hooks", {})
if not isinstance(hooks, dict):
    hooks = data["hooks"] = {}

MARKER = "session_bridge/hooks/worker.py"
EVENTS = {
    "SessionStart": None,
    "SessionEnd": None,
    "UserPromptSubmit": None,
    "Stop": stop_timeout,
}

for event, timeout in EVENTS.items():
    entries = hooks.setdefault(event, [])
    if not isinstance(entries, list):
        entries = hooks[event] = []
    found = False
    for entry in entries:
        for h in entry.get("hooks", []):
            if MARKER in h.get("command", ""):
                # refresh our entry in place (idempotent re-install)
                h["type"] = "command"
                h["command"] = command
                if timeout is not None:
                    if h.get("timeout", 0) != timeout:
                        h["timeout"] = timeout
                found = True
    if not found:
        hook = {"type": "command", "command": command}
        if timeout is not None:
            hook["timeout"] = timeout
        entries.append({"matcher": "", "hooks": [hook]})

with open(path, "w", encoding="utf-8") as fh:
    json.dump(data, fh, indent=2, ensure_ascii=False)
    fh.write("\n")
PYEOF

# Post-merge validation: the installed Stop timeout must cover a cycle.
"$PYTHON_BIN" - "$HOOKS_FILE" "$COMMAND" "$STOP_TIMEOUT" \
    "$CYCLE_SECONDS" <<'PYEOF'
import json
import sys

path, command, timeout, cycle = (
    sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
)
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)
MARKER = "session_bridge/hooks/worker.py"
ours = [
    h.get("timeout", 60)
    for entry in data.get("hooks", {}).get("Stop", [])
    for h in entry.get("hooks", [])
    if MARKER in h.get("command", "")
]
actual = max(ours) if ours else 0
if actual < cycle:
    print(f"install_hooks: error: Stop hook timeout {actual}s < "
          f"cycle {cycle}s; refusing to leave a broken config",
          file=sys.stderr)
    sys.exit(1)
print(f"install_hooks: hooks installed into {path} "
      f"(stop timeout={actual}s, cycle={cycle}s)")
PYEOF

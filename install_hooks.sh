#!/usr/bin/env bash
# install_hooks.sh - merge sessions-bridge hooks into a target repo's
# .devin/hooks.v1.json.
#
# Usage: ./install_hooks.sh /path/to/repo
#
# hooks.v1.json format (issue #7): the hooks object is the entire
# file - top-level keys are event names ("SessionStart", "Stop", ...)
# mapped to lists of {"matcher", "hooks"} entries. The "hooks" wrapper
# key belongs to the config.json family only; files produced by older
# versions of this script ({"hooks": {...}}) are migrated in place -
# every entry under the wrapper moves to the top level, foreign ones
# included.
#
# Idempotent: existing non-bridge hook entries are preserved, our own
# entries are refreshed in place (duplicates of our hook are dropped),
# and a pre-existing hooks.v1.json is backed up to hooks.v1.json.bak
# before merging. The Stop hook timeout must cover the parking cycle
# (ParkConfig precondition); it is set to a safe custom value and
# validated after the merge.
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

# Legacy migration (issue #7): pre-fix installs nested the event map
# under a "hooks" wrapper key. In hooks.v1.json that key is invalid -
# the whole file IS the hooks object - so every event stored under the
# wrapper moves to the top level. Foreign entries migrate along with
# ours: they are hook data written under the wrong key, and moving
# them repairs the file instead of discarding working hooks. A stray
# non-dict "hooks" value cannot be hook data; it is kept under a
# clearly-namespaced key rather than silently dropped.
legacy = data.pop("hooks", None)
if isinstance(legacy, dict):
    for event, entries in legacy.items():
        top = data.get(event)
        if isinstance(top, list) and isinstance(entries, list):
            top.extend(entries)
        elif event not in data:
            data[event] = entries
        elif isinstance(entries, list):
            # a non-list top-level value cannot be a hook entry list;
            # the legacy list is the usable data
            data[event] = entries
        # else both values are malformed: the top-level one wins
elif legacy is not None:
    data["_legacy_hooks"] = legacy

MARKER = "session_bridge/hooks/worker.py"
EVENTS = {
    "SessionStart": None,
    "SessionEnd": None,
    "UserPromptSubmit": None,
    "Stop": stop_timeout,
}

for event, timeout in EVENTS.items():
    entries = data.get(event)
    if not isinstance(entries, list):
        entries = data[event] = []
    found = False
    kept = []
    for entry in entries:
        inner = entry.get("hooks") if isinstance(entry, dict) else None
        if not isinstance(inner, list) or not any(
            isinstance(h, dict) and MARKER in h.get("command", "")
            for h in inner
        ):
            kept.append(entry)  # foreign entry passes through untouched
            continue
        # entry carries our hook: the first copy is refreshed in place,
        # later copies (e.g. merged from a legacy wrapper plus a fresh
        # top-level entry) are dropped so the hook fires exactly once
        new_inner = []
        for h in inner:
            if isinstance(h, dict) and MARKER in h.get("command", ""):
                if found:
                    continue
                found = True
                h["type"] = "command"
                h["command"] = command
                if timeout is not None:
                    h["timeout"] = timeout
            new_inner.append(h)
        if new_inner:
            entry["hooks"] = new_inner
            kept.append(entry)
        # else the entry held only duplicate copies of our hook -
        # dropping it removes an empty husk
    entries[:] = kept
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

# Sanity check: hooks.v1.json must hold the event map at the top level
# - a surviving "hooks" wrapper key or a missing/broken event means the
# merge produced a file Devin will silently ignore.
EVENTS = ("SessionStart", "SessionEnd", "UserPromptSubmit", "Stop")
broken = [
    event for event in EVENTS
    if not isinstance(data.get(event), list)
    or not any(
        MARKER in h.get("command", "")
        for entry in data[event]
        if isinstance(entry, dict)
        for h in entry.get("hooks", [])
        if isinstance(h, dict)
    )
]
if "hooks" in data or broken:
    print(f"install_hooks: error: invalid hooks.v1.json at {path}: "
          f"'hooks' wrapper key "
          f"{'present' if 'hooks' in data else 'absent'}, "
          f"events missing our hook: {broken or 'none'}",
          file=sys.stderr)
    sys.exit(1)

ours = [
    h.get("timeout", 60)
    for entry in data.get("Stop", [])
    if isinstance(entry, dict)
    for h in entry.get("hooks", [])
    if isinstance(h, dict) and MARKER in h.get("command", "")
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

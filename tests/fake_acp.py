#!/usr/bin/env python3
"""Scriptable fake ``devin acp`` subprocess for client tests.

Usage: ``fake_acp.py <scenario.json>``. Reads newline-delimited
JSON-RPC requests on stdin. ``initialize`` is answered automatically
with a canned result; ``authenticate`` and ``session/cancel`` are
swallowed (the client treats them as fire-and-forget). Every other
request is matched on ``method`` against ``rules`` in the scenario,
and the first matching rule's action list runs.

Actions:

- ``{"send": <obj>}``   - obj serialized as one NDJSON line; the
  string ``"$id"`` anywhere inside is replaced by the request id
- ``{"raw": "<text>"}`` - written verbatim plus newline (malformed wire)
- ``{"partial": "<text>"}`` - written WITHOUT a newline (truncated wire)
- ``{"sleep": <sec>}``  - pause before the next action
- ``{"close_stdout": true}`` - close stdout (the client sees EOF)
- ``{"exit": <code>}``  - exit immediately

``start`` is an optional action list executed before the read loop
(e.g. an instant crash). EOF on stdin exits 0.
"""

import json
import sys
import time


def _subst(obj, req_id):
    if isinstance(obj, dict):
        return {k: _subst(v, req_id) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_subst(v, req_id) for v in obj]
    return req_id if obj == "$id" else obj


def _emit(text):
    try:
        sys.stdout.write(text)
        sys.stdout.flush()
    except (BrokenPipeError, OSError, ValueError):
        pass  # stdout closed by a previous action or pipe is dead


def _run_actions(actions, req):
    req_id = req.get("id") if isinstance(req, dict) else None
    for action in actions or []:
        if "send" in action:
            _emit(json.dumps(_subst(action["send"], req_id)) + "\n")
        if "raw" in action:
            _emit(str(action["raw"]) + "\n")
        if "partial" in action:
            _emit(str(action["partial"]))
        if "sleep" in action:
            time.sleep(float(action["sleep"]))
        if action.get("close_stdout"):
            try:
                sys.stdout.close()
            except OSError:
                pass
        if "exit" in action:
            sys.exit(int(action["exit"]))


def main() -> None:
    scenario = {}
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as fh:
            scenario = json.load(fh)
    _run_actions(scenario.get("start"), {})
    rules = scenario.get("rules", [])
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(req, dict):
            continue
        method = req.get("method")
        if method == "initialize":
            _emit(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": req.get("id"),
                        "result": {"protocolVersion": 1},
                    }
                )
                + "\n"
            )
            continue
        # authenticate/session/cancel are fire-and-forget traffic:
        # consume and stay silent
        if method is None or method in ("authenticate", "session/cancel"):
            continue
        for rule in rules:
            if rule.get("method") == method:
                _run_actions(rule.get("actions"), req)
                break


if __name__ == "__main__":
    main()

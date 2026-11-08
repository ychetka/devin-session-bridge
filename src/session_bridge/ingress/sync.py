"""Ingress reconciler: registry rows <-> running forwarder processes.

``bridge ingress sync`` is the host-level materialization loop for
the allocation layer (task #209). For every ``tcp_port`` allocation
carrying a ``fwd=`` token it ensures a detached
``session_bridge.ingress.forwarder`` process exists; when the row is
released or its spec/target changes, the stale forwarder is killed.
Reconciler state lives in ``<state_dir>/ingress.json``; a flock on
``ingress.lock`` serializes concurrent syncs (timer + manual calls).

The reconciler only ever kills processes it spawned: a tracked pid
must still map to a process whose cmdline contains the forwarder
marker before SIGTERM is sent.
"""

from __future__ import annotations

import fcntl
import json
import os
import signal
import subprocess
import sys
import time

from session_bridge.ingress.spec import ForwardSpec, parse_purpose, resolve_target

FORWARDER_MARKER = "session_bridge.ingress.forwarder"
STATE_FILE = "ingress.json"
LOCK_FILE = "ingress.lock"
#: a forwarder that died within this window of its spawn is recorded
#: as an error instead of being treated as healthy
SPAWN_GRACE_S = 0.4


def _src_root() -> str:
    """Directory containing the ``session_bridge`` package - used as
    PYTHONPATH so detached forwarders import the same code even when
    the package is not installed into the interpreter."""
    import session_bridge

    return os.path.dirname(os.path.dirname(os.path.abspath(session_bridge.__file__)))


def _state_path(state_dir: str) -> str:
    return os.path.join(state_dir, STATE_FILE)


def _log_path(state_dir: str, port: int) -> str:
    return os.path.join(state_dir, "logs", f"ingress-fwd-{port}.log")


def _load_state(state_dir: str) -> dict:
    """Reconciler bookkeeping; a corrupt/malformed file degrades to
    empty state rather than crash-looping the 30s timer."""
    try:
        data = json.load(open(_state_path(state_dir)))
    except (OSError, ValueError):
        return {"forwards": {}}
    forwards = data.get("forwards") if isinstance(data, dict) else None
    if not isinstance(forwards, dict):
        return {"forwards": {}}
    return {
        "forwards": {
            str(k): v
            for k, v in forwards.items()
            if isinstance(v, dict) and isinstance(v.get("pid"), int)
        }
    }


def _save_state(state_dir: str, state: dict) -> None:
    tmp = _state_path(state_dir) + ".tmp"
    with open(tmp, "w") as fh:
        json.dump(state, fh, indent=1, sort_keys=True)
    os.replace(tmp, _state_path(state_dir))


def _cmdline(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as fh:
            return fh.read().decode(errors="replace").replace("\x00", " ")
    except OSError:
        return ""


def _is_our_forwarder(pid: int, port: str) -> bool:
    """True when pid exists and looks like a forwarder we spawned for
    this port. Guards SIGTERM against pid reuse by a foreign process."""
    cmdline = _cmdline(pid)
    return FORWARDER_MARKER in cmdline and f" {port} " in cmdline


def _reap(pid: int) -> None:
    """Reap the child when it is ours - a killed forwarder otherwise
    lingers as a zombie under the spawning process."""
    try:
        os.waitpid(pid, os.WNOHANG)
    except (ChildProcessError, OSError):
        pass


def _kill(pid: int, port: str) -> None:
    if not _is_our_forwarder(pid, port):
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return
    deadline = time.time() + 1.0
    while time.time() < deadline:
        _reap(pid)
        if not os.path.exists(f"/proc/{pid}"):
            return
        time.sleep(0.05)
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        pass
    _reap(pid)


def _spawn(state_dir: str, port: int, spec: ForwardSpec) -> int:
    os.makedirs(os.path.join(state_dir, "logs"), exist_ok=True)
    env = dict(os.environ)
    src = _src_root()
    prev = env.get("PYTHONPATH")
    env["PYTHONPATH"] = f"{src}{os.pathsep}{prev}" if prev else src
    log_fh = open(_log_path(state_dir, port), "ab")
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            FORWARDER_MARKER,
            spec.bind,
            str(port),
            spec.spec_text,
        ],
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        env=env,
    )
    return proc.pid


def desired_forwards(rows) -> tuple[dict, list]:
    """({port: ForwardSpec}, row_errors) for tcp_port rows carrying a
    fwd token. Per-row isolation: a malformed row or a stray fwd=
    substring in free text lands in row_errors - it must never poison
    the whole reconcile for every other project."""
    out, errors = {}, []
    for r in rows:
        if r["resource_type"] != "tcp_port":
            continue
        try:
            spec = parse_purpose(r["purpose"])
            port = int(r["resource_id"])
            if spec is not None:
                out[port] = spec
        except (ValueError, TypeError) as e:
            errors.append({"row": r["resource_id"], "error": str(e)})
    return out, errors


def sync(state_dir: str, rows) -> dict:
    """One reconciliation pass. Returns a summary dict."""
    os.makedirs(state_dir, exist_ok=True)
    with open(os.path.join(state_dir, LOCK_FILE), "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return {"status": "skipped", "reason": "another sync holds ingress.lock"}
        state = _load_state(state_dir)
        tracked = state["forwards"]
        desired, row_errors = desired_forwards(rows)
        summary = {"started": [], "stopped": [], "kept": [], "errors": row_errors}
        now = int(time.time())

        # stop forwards whose row is gone or lost its fwd declaration
        for port_s in list(tracked):
            if int(port_s) not in desired:
                rec = tracked.pop(port_s)
                _kill(rec["pid"], port_s)
                summary["stopped"].append(int(port_s))

        for port, spec in desired.items():
            port_s = str(port)
            resolved = resolve_target(spec)
            rec = tracked.get(port_s)
            if not resolved:
                # target gone (stack down / pidfile unreadable): a
                # listener pointing at a dead netns is worse than no
                # listener - drop it; it respawns when the target is
                # resolvable again
                if rec is not None:
                    tracked.pop(port_s)
                    _kill(rec["pid"], port_s)
                    summary["stopped"].append(port)
                summary["errors"].append(
                    {"port": port, "error": "forward target unresolved", "spec": spec.spec_text}
                )
                continue
            alive = rec is not None and _is_our_forwarder(rec["pid"], port_s)
            if (
                alive
                and rec["spec"] == spec.spec_text
                and rec.get("bind") == spec.bind
                and rec.get("resolved") == resolved
            ):
                summary["kept"].append(port)
                continue
            if rec is not None:
                _kill(rec["pid"], port_s)
            pid = _spawn(state_dir, port, spec)
            time.sleep(SPAWN_GRACE_S)
            if _is_our_forwarder(pid, port_s):
                tracked[port_s] = {
                    "pid": pid,
                    "spec": spec.spec_text,
                    "bind": spec.bind,
                    "resolved": resolved,
                    "started_at": now,
                }
                summary["started"].append(port)
            else:
                tracked.pop(port_s, None)
                _reap(pid)
                summary["errors"].append(
                    {
                        "port": port,
                        "error": "forwarder exited at startup (see ingress-fwd log)",
                        "spec": spec.spec_text,
                    }
                )

        _save_state(state_dir, state)
        summary["status"] = "ok"
        return summary


def status(state_dir: str, rows) -> dict:
    """Read-only view: desired rows vs tracked forwarder state."""
    tracked = _load_state(state_dir)["forwards"]
    desired, row_errors = desired_forwards(rows)
    items = []
    for port, spec in sorted(desired.items()):
        rec = tracked.get(str(port))
        items.append(
            {
                "port": port,
                "spec": spec.spec_text,
                "bind": spec.bind,
                "resolved": resolve_target(spec) or None,
                "forwarder_pid": rec["pid"] if rec else None,
                "alive": bool(rec and _is_our_forwarder(rec["pid"], str(port))),
                "started_at": rec.get("started_at") if rec else None,
            }
        )
    # tracked forwarders whose row disappeared (should not happen
    # after a successful sync, but visible if it does)
    orphans = [int(p) for p in tracked if int(p) not in desired]
    return {"forwards": items, "orphaned": orphans, "row_errors": row_errors}

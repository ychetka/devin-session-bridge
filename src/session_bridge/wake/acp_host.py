"""acp-host wake transport (item_16).

Closes the last delivery gap of the mailbox design: an idle managed
session has no turns, so relayed mail is never read until a human
types. The wake path attaches a transient client to the detached
``acp-host`` that owns every local Devin session and injects a short
"drain your mailbox" prompt - zero-touch, no resident watcher, no
payload over the wire (the fs mailbox stays the only data channel).

Wire protocol (verified against the production host):

1. discovery: ``<lock_dir>/acp-host-*.lock`` - JSON
   ``{pid, port, token, ...}``;
2. TCP connect to ``127.0.0.1:<port>``, first NDJSON line
   ``{"token": ..., "agent": {"id": "devin-cli"}, "protocol": 1}``;
   the host answers ``{"ok": true, ...}`` and the socket then carries
   plain ACP JSON-RPC multiplexed into the shared ``devin acp``
   process;
3. ACP: ``initialize`` -> ``session/list`` (busy check via
   ``cognition.ai/statusEnum``) -> ``session/load`` (replayed
   ``session/update`` notifications are drained and discarded) ->
   ``session/prompt`` with the nudge -> detach.

Every step is bounded - a wake attempt has an overall deadline and
can never stall the daemon loop. ``ATTACH_LOCK`` serializes all
attaches process-wide (daemon pass and manual ``bridge wake`` can
never overlap).
"""

import glob
import json
import os
import select
import socket
import threading
import time

__all__ = [
    "ATTACH_LOCK",
    "DEFAULT_AGENT_ID",
    "DEFAULT_LOCK_DIR",
    "PROMPT_CONFIRM_SECONDS",
    "REPLAY_CAP",
    "REPLAY_TIMEOUT_SECONDS",
    "WakeError",
    "find_lockfile",
    "nudge_text",
    "pid_alive",
    "wake_session",
]

#: default directory holding ``acp-host-*.lock`` files (one per
#: quality channel; newest live one wins).
DEFAULT_LOCK_DIR = "~/.devin-server/cli"

#: the only agent the production acp-host currently serves.
DEFAULT_AGENT_ID = "devin-cli"

#: env override for tests / non-standard installs.
_LOCK_DIR_ENV = "SESSIONS_BRIDGE_ACP_LOCK_DIR"

#: wire hello protocol version the host requires.
_WIRE_PROTOCOL = 1

_CONNECT_TIMEOUT = 5.0
_HELLO_TIMEOUT = 10.0
_REQUEST_TIMEOUT = 15.0

#: bounds for draining the replayed session/update history after
#: session/load - large transcripts replay hundreds of messages, so
#: both a count cap and a wall-clock cap apply; whichever hits first
#: aborts the load wait (the prompt can still be sent afterwards -
#: replay is display data only).
REPLAY_CAP = 2000
REPLAY_TIMEOUT_SECONDS = 15.0

#: how long we wait for evidence that the injected prompt produced a
#: turn (the prompt response itself, or session/update activity).
PROMPT_CONFIRM_SECONDS = 15.0

#: statusEnum values that mean "session is mid-turn / must not be
#: prompted". Anything else except an explicit idle value is treated
#: conservatively as busy.
_IDLE_STATUS = {"finished", "idle", "created"}

_READ_CHUNK = 65536

#: process-wide mutex: at most one acp-host attach at a time, shared
#: by the daemon pass and the manual ``bridge wake`` command.
ATTACH_LOCK = threading.Lock()


class WakeError(Exception):
    """Wake attempt failed; ``reason`` maps 1:1 to a decision event."""

    def __init__(self, reason: str, message: str = ""):
        self.reason = reason
        super().__init__(message or reason)


def find_lockfile(lock_dir: str | None = None):
    """Newest readable ``acp-host-*.lock`` dict, or ``None``.

    Re-read on every call - the host can restart with a fresh port and
    token between daemon passes, so nothing is cached.
    """
    base = lock_dir or os.environ.get(_LOCK_DIR_ENV) or DEFAULT_LOCK_DIR
    candidates = glob.glob(os.path.join(os.path.expanduser(base), "acp-host-*.lock"))
    best = None
    for path in candidates:
        try:
            with open(path) as fh:
                data = json.load(fh)
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict) or not data.get("port") or not data.get("token"):
            continue
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        if best is None or mtime > best[0]:
            best = (mtime, data, path)
    if best is None:
        return None
    data = dict(best[1])
    data["_path"] = best[2]
    return data


def pid_alive(pid) -> bool:
    """True while a live process owns ``pid`` (stale-lockfile check)."""
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def nudge_text(session_id: str) -> str:
    """The fixed wake nudge - a trigger only, never task payload."""
    return (
        "[sessions-bridge] You have pending bridge mail. Per PLAYBOOK "
        f"drain mode: drain your mailbox now (ls -v the msg_*.txt files, "
        f"cat+rm each, handle by kind, then `bridge ack --session "
        f"{session_id}`), then continue."
    )


class _SocketChannel:
    """Minimal NDJSON reader/writer over one TCP socket."""

    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._rbuf = b""

    def send(self, payload: dict) -> None:
        try:
            self._sock.sendall(json.dumps(payload).encode() + b"\n")
        except OSError as e:
            raise WakeError("connect_failed", f"socket write failed: {e}") from e

    def read_message(self, timeout: float) -> dict:
        """Next complete NDJSON object; WakeError on timeout/eof/bad json."""
        deadline = time.monotonic() + timeout
        while True:
            nl = self._rbuf.find(b"\n")
            if nl >= 0:
                line, self._rbuf = self._rbuf[:nl], self._rbuf[nl + 1 :]
                if not line.strip():
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError as e:
                    raise WakeError("malformed_message", f"bad NDJSON line: {e}") from e
                if not isinstance(msg, dict):
                    raise WakeError("malformed_message", "non-object NDJSON message")
                return msg
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise WakeError("timeout", f"no traffic for {timeout}s")
            try:
                ready, _, _ = select.select([self._sock], [], [], remaining)
            except (OSError, ValueError) as e:
                raise WakeError("connect_failed", f"socket poll failed: {e}") from e
            if not ready:
                raise WakeError("timeout", f"no traffic for {timeout}s")
            try:
                chunk = self._sock.recv(_READ_CHUNK)
            except OSError as e:
                raise WakeError("connect_failed", f"socket read failed: {e}") from e
            if not chunk:
                raise WakeError("eof", "host closed the connection")
            self._rbuf += chunk


def _rpc(chan: _SocketChannel, req_id: int, method: str, params: dict | None = None) -> None:
    chan.send({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params or {}})


def _await_response(chan: _SocketChannel, req_id: int, timeout: float, max_messages: int):
    """Read until the response for ``req_id``; drain notifications.

    Returns the result dict. Notifications carry ``method`` and are
    discarded (replay/display data). ``max_messages`` bounds the drain
    so an endless notification stream cannot stall the caller.
    """
    seen = 0
    while True:
        msg = chan.read_message(timeout)
        seen += 1
        if seen > max_messages:
            raise WakeError("timeout", f"response cap exceeded ({max_messages} messages)")
        if "method" in msg:
            continue
        if msg.get("id") != req_id:
            continue
        if "error" in msg and msg["error"] is not None:
            err = msg["error"]
            detail = err.get("message") if isinstance(err, dict) else str(err)
            raise WakeError("rpc_error", f"{detail or err}")
        result = msg.get("result")
        return result if isinstance(result, dict) else {}


def _session_status(entry: dict):
    meta = entry.get("_meta") or {}
    return meta.get("cognition.ai/statusEnum")


def _is_busy(entry: dict | None) -> bool:
    """Conservative busy check: only an explicit idle status wakes."""
    if entry is None:
        return True  # unknown session - do not inject blind
    return _session_status(entry) not in _IDLE_STATUS


def _attach(lock_data: dict, agent_id: str):
    """Connect + host hello; returns the open _SocketChannel."""
    port = lock_data.get("port")
    token = lock_data.get("token")
    pid = lock_data.get("pid")
    if not pid_alive(pid):
        raise WakeError(
            "wake_host_down",
            f"stale lockfile {lock_data.get('_path', '?')}: pid {pid} is not running",
        )
    try:
        sock = socket.create_connection(("127.0.0.1", int(port)), timeout=_CONNECT_TIMEOUT)
    except (ConnectionRefusedError, OSError) as e:
        raise WakeError("wake_host_down", f"cannot reach acp-host at :{port}: {e}") from e
    chan = _SocketChannel(sock)
    try:
        chan.send({"token": token, "agent": {"id": agent_id}, "protocol": _WIRE_PROTOCOL})
        hello = chan.read_message(_HELLO_TIMEOUT)
    except WakeError:
        sock.close()
        raise
    if not hello.get("ok"):
        err = hello.get("error") or "attach refused"
        reason = "unauthorized" if "unauthorized" in str(err) else "attach_failed"
        sock.close()
        raise WakeError(reason, str(err))
    return chan


def wake_session(
    session_id: str,
    cwd: str | None = None,
    *,
    nudge: str | None = None,
    lock_data: dict | None = None,
    lock_dir: str | None = None,
    agent_id: str = DEFAULT_AGENT_ID,
    replay_timeout: float = REPLAY_TIMEOUT_SECONDS,
    replay_cap: int = REPLAY_CAP,
    confirm_timeout: float = PROMPT_CONFIRM_SECONDS,
) -> str:
    """Attach -> busy check -> load -> nudge prompt -> detach.

    Returns ``"sent"`` when the prompt reached the session (its own
    response or turn activity was observed) or ``"sent_unconfirmed"``
    when the prompt was written but no confirmation arrived inside
    ``confirm_timeout`` - both count as delivered because the mail is
    authoritative and a double wake is harmless. Returns ``"busy"``
    when the session reports an active turn; raises ``WakeError``
    (``reason`` in host_down/unauthorized/attach_failed/connect_failed
    /timeout/rpc_error/malformed_message/eof/no_session) otherwise.

    Never blocks longer than the sum of its step timeouts and never
    mutates bridge state - the mailbox stays authoritative.
    """
    if lock_data is None:
        lock_data = find_lockfile(lock_dir)
    if lock_data is None:
        raise WakeError("wake_host_down", "no acp-host lockfile found")

    with ATTACH_LOCK:
        chan = _attach(lock_data, agent_id)
        sock = chan._sock
        try:
            req = 0

            def call(method, params=None, timeout=_REQUEST_TIMEOUT, cap=256):
                nonlocal req
                req += 1
                _rpc(chan, req, method, params)
                return _await_response(chan, req, timeout, cap)

            call(
                "initialize",
                {
                    "protocolVersion": 1,
                    "clientCapabilities": {
                        "fs": {"readTextFile": False, "writeTextFile": False}
                    },
                    "clientInfo": {"name": "sessions-bridge-wake", "version": "0"},
                },
            )
            chan.send({"jsonrpc": "2.0", "method": "initialized", "params": {}})

            listing = call("session/list", {})
            entry = None
            for item in listing.get("sessions", []):
                if item.get("sessionId") == session_id:
                    entry = item
                    break
            if entry is None:
                raise WakeError("no_session", f"{session_id} not in session/list")
            if _is_busy(entry):
                return "busy"
            wake_cwd = cwd or entry.get("cwd")
            if not wake_cwd:
                raise WakeError("no_session", f"no cwd known for {session_id}")

            # load replays stored session/update notifications to this
            # client - drained with a hard cap; a transcript too big to
            # finish within the bound still lets the prompt go out
            req += 1
            _rpc(
                chan,
                req,
                "session/load",
                {"sessionId": session_id, "cwd": wake_cwd, "mcpServers": []},
            )
            load_id = req
            try:
                _await_response(chan, load_id, replay_timeout, replay_cap)
            except WakeError as e:
                if e.reason != "timeout":
                    raise
                # replay overflow: the load response is display data,
                # so a flooded channel is tolerated - press on

            nudge = nudge or nudge_text(session_id)
            req += 1
            _rpc(
                chan,
                req,
                "session/prompt",
                {
                    "sessionId": session_id,
                    "prompt": [{"type": "text", "text": nudge}],
                },
            )
            prompt_id = req
            # confirmation: the prompt response itself (fast turns) or
            # any live session/update activity for this session (the
            # turn started). Neither arriving within the bound still
            # leaves the prompt server-side - report unconfirmed.
            deadline = time.monotonic() + confirm_timeout
            while time.monotonic() < deadline:
                try:
                    msg = chan.read_message(deadline - time.monotonic())
                except WakeError as e:
                    if e.reason == "timeout":
                        return "sent_unconfirmed"
                    raise
                if "method" in msg:
                    params = msg.get("params") or {}
                    if params.get("sessionId") == session_id:
                        return "sent"
                    continue
                if msg.get("id") == prompt_id:
                    return "sent"
            return "sent_unconfirmed"
        finally:
            try:
                sock.close()
            except OSError:
                pass

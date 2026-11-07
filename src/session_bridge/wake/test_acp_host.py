"""Tests for the acp-host wake transport (item_16).

A threaded TCP server fakes the real acp-host wire: an NDJSON
``{token, agent, protocol}`` hello followed by raw ACP JSON-RPC.
Bridge state.db and the decision log are real in tmp_path; the
daemon's ``_wake_pass`` is invoked directly (no loop) with a real
``WakePolicy``.
"""

import json
import os
import socket
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from session_bridge.cli import bridge
from session_bridge.daemon import core as daemon
from session_bridge.state import db
from session_bridge.state.contract import (
    NotificationKind,
    SessionMode,
    SessionRole,
    TaskStatus,
    Transport,
)
from session_bridge.wake import (
    SESSION_ABSENT,
    WakeError,
    WakePolicy,
    find_lockfile,
    nudge_text,
    pid_alive,
    status_is_idle,
    verify_sessions,
    wake_session,
)

SID = "s1"


# --- fake acp-host -------------------------------------------------------------


class FakeAcpHost:
    """Minimal NDJSON acp-host: hello gate + scripted ACP answers."""

    def __init__(
        self,
        sessions=(),
        hello_error=None,
        hang=False,
        replay_count=0,
        prompt_update=True,
        truncate_hello=False,
        on_prompt=None,
    ):
        self.sessions = list(sessions)
        self.hello_error = hello_error
        self.hang = hang
        self.replay_count = replay_count
        self.prompt_update = prompt_update
        self.truncate_hello = truncate_hello
        self.on_prompt = on_prompt
        self.hellos: list[dict] = []
        self.calls: list[tuple[str, dict]] = []
        self.errors: list[str] = []
        self._stop = threading.Event()
        self._sock = socket.socket()
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self.port = self._sock.getsockname()[1]
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self):
        while not self._stop.is_set():
            try:
                self._sock.settimeout(0.5)
                conn, _ = self._sock.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            threading.Thread(target=self._client, args=(conn,), daemon=True).start()

    def _client(self, conn):
        f = conn.makefile("rw")
        try:
            hello = json.loads(f.readline())
            self.hellos.append(hello)
            if self.truncate_hello:
                f.write('{"ok": tru')  # truncated NDJSON then drop
                f.flush()
                return
            if self.hello_error is not None:
                f.write(json.dumps({"ok": False, "protocol": 1, "error": self.hello_error}) + "\n")
                f.flush()
                return
            f.write(json.dumps({"ok": True, "protocol": 1, "pid": os.getpid()}) + "\n")
            f.flush()
            if self.hang:
                self._stop.wait(30)  # hold the socket open, never answer ACP
                return
            for line in f:
                line = line.strip()
                if not line:
                    continue
                msg = json.loads(line)
                method = msg.get("method")
                params = msg.get("params") or {}
                if "id" not in msg:
                    continue  # notification (initialized)
                self.calls.append((method, params))
                out = {"jsonrpc": "2.0", "id": msg["id"]}
                if method == "initialize":
                    out["result"] = {"protocolVersion": 1, "agentCapabilities": {}}
                elif method == "session/list":
                    out["result"] = {"sessions": self.sessions}
                elif method == "session/load":
                    for _ in range(self.replay_count):
                        f.write(
                            json.dumps(
                                {
                                    "jsonrpc": "2.0",
                                    "method": "session/update",
                                    "params": {
                                        "sessionId": params.get("sessionId"),
                                        "update": {"sessionUpdate": "agent_message_chunk"},
                                    },
                                }
                            )
                            + "\n"
                        )
                    out["result"] = {}
                elif method == "session/prompt":
                    if self.on_prompt is not None:
                        self.on_prompt()
                    if self.prompt_update:
                        f.write(
                            json.dumps(
                                {
                                    "jsonrpc": "2.0",
                                    "method": "session/update",
                                    "params": {
                                        "sessionId": params.get("sessionId"),
                                        "update": {"sessionUpdate": "tool_call"},
                                    },
                                }
                            )
                            + "\n"
                        )
                    out["result"] = {"stopReason": "end_turn"}
                else:
                    out["result"] = {}
                f.write(json.dumps(out) + "\n")
                f.flush()
        except (OSError, json.JSONDecodeError) as e:
            self.errors.append(str(e))
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def lock_data(self):
        return {
            "pid": os.getpid(),
            "port": self.port,
            "token": "fake-token",
            "_path": "/fake/acp-host-test.lock",
        }

    def methods(self):
        return [m for m, _ in self.calls]


# --- fixtures ------------------------------------------------------------------


@pytest.fixture()
def env(tmp_path):
    db_path = str(tmp_path / "state.db")
    db.init(db_path)
    return SimpleNamespace(db=db_path, sdir=str(tmp_path))


def _session(path, sid, *, managed=True, cwd="/tmp/ws"):
    db.register(sid, cwd, role=SessionRole.WORKER, path=path)
    if managed:
        db.set_session_mode(sid, SessionMode.MANAGED, path=path)


def _heartbeat(path, sid, *, age):
    db.heartbeat(sid, path=path)
    con = db.connect(path)
    try:
        con.execute(
            "UPDATE worker_heartbeats SET last_seen_at=? WHERE session_id=?",
            (int(time.time()) - age, sid),
        )
        con.commit()
    finally:
        con.close()


def _relayed_mail(path, sid):
    nid = db.notify(sid, NotificationKind.TASK_DISPATCH, "do the thing", task_id=1, path=path)
    db.mark_relayed(nid, path=path)
    return nid


def _notification(path, nid):
    con = db.connect(path)
    try:
        return con.execute(
            "SELECT relayed_at, ack_at FROM notifications WHERE id=?", (nid,)
        ).fetchone()
    finally:
        con.close()


def _decisions(env):
    path = Path(env.sdir) / daemon.DECISION_LOG_FILENAME
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _lockdir(tmp_path, host, monkeypatch):
    lockdir = tmp_path / "locks"
    lockdir.mkdir()
    data = host.lock_data()
    data.pop("_path", None)
    (lockdir / "acp-host-test.lock").write_text(json.dumps(data))
    monkeypatch.setenv("SESSIONS_BRIDGE_ACP_LOCK_DIR", str(lockdir))


IDLE_SESSION = {
    "sessionId": SID,
    "cwd": "/tmp/ws",
    "_meta": {"cognition.ai/statusEnum": "finished"},
}
BUSY_SESSION = {
    "sessionId": SID,
    "cwd": "/tmp/ws",
    "_meta": {"cognition.ai/statusEnum": "working"},
}


# --- transport unit tests ------------------------------------------------------


def test_find_lockfile_picks_newest_valid(tmp_path):
    (tmp_path / "acp-host-a.lock").write_text(json.dumps({"pid": 1, "port": 1, "token": "a"}))
    time.sleep(0.01)
    (tmp_path / "acp-host-b.lock").write_text(json.dumps({"pid": 2, "port": 2, "token": "b"}))
    found = find_lockfile(str(tmp_path))
    assert found is not None and found["token"] == "b"
    assert found["_path"].endswith("acp-host-b.lock")


def test_find_lockfile_skips_garbage(tmp_path):
    (tmp_path / "acp-host-bad.lock").write_text("not json")
    (tmp_path / "acp-host-noport.lock").write_text(json.dumps({"pid": 1}))
    assert find_lockfile(str(tmp_path)) is None


def test_find_lockfile_prefers_live_pid(tmp_path):
    """A stale (dead-pid) newest lockfile must not mask a live older
    channel host."""
    (tmp_path / "acp-host-old.lock").write_text(
        json.dumps({"pid": os.getpid(), "port": 1, "token": "live"})
    )
    time.sleep(0.01)
    (tmp_path / "acp-host-new.lock").write_text(
        json.dumps({"pid": 2**22, "port": 2, "token": "dead"})
    )
    found = find_lockfile(str(tmp_path))
    assert found is not None and found["token"] == "live"
    # ...but when every pid is dead the newest file is still
    # returned so _attach reports a distinct wake_host_down
    found2 = find_lockfile(str(tmp_path))
    assert found2 is not None


def test_find_lockfile_all_dead_returns_newest(tmp_path):
    (tmp_path / "acp-host-a.lock").write_text(json.dumps({"pid": 2**22, "port": 1, "token": "a"}))
    time.sleep(0.01)
    (tmp_path / "acp-host-b.lock").write_text(json.dumps({"pid": 2**22, "port": 2, "token": "b"}))
    found = find_lockfile(str(tmp_path))
    assert found is not None and found["token"] == "b"


def test_wake_corrupt_port_is_wake_error():
    with pytest.raises(WakeError) as exc:
        wake_session(SID, "/tmp", lock_data={"pid": os.getpid(), "port": "x", "token": "t"})
    assert exc.value.reason == "malformed_lockfile"


def test_pid_alive():
    assert pid_alive(os.getpid())
    assert not pid_alive(2**22)
    assert not pid_alive(-1)


def test_wake_host_down_on_dead_pid():
    with pytest.raises(WakeError) as exc:
        wake_session(SID, "/tmp", lock_data={"pid": 2**22, "port": 1, "token": "t"})
    assert exc.value.reason == "wake_host_down"


def test_wake_host_down_on_refused():
    dead = socket.socket()
    dead.bind(("127.0.0.1", 0))
    port = dead.getsockname()[1]
    dead.close()
    with pytest.raises(WakeError) as exc:
        wake_session(SID, "/tmp", lock_data={"pid": os.getpid(), "port": port, "token": "t"})
    assert exc.value.reason == "wake_host_down"


def test_wake_full_flow_sent():
    host = FakeAcpHost(sessions=[IDLE_SESSION])
    outcome = wake_session(SID, "/tmp/ws", lock_data=host.lock_data())
    assert outcome == "sent"
    assert host.methods() == ["initialize", "session/list", "session/load", "session/prompt"]
    hello = host.hellos[0]
    assert hello["token"] == "fake-token"
    assert hello["agent"] == {"id": "devin-cli"}
    assert hello["protocol"] == 1


def test_wake_busy_session_not_prompted():
    host = FakeAcpHost(sessions=[BUSY_SESSION])
    outcome = wake_session(SID, "/tmp/ws", lock_data=host.lock_data())
    assert outcome == "busy"
    assert "session/prompt" not in host.methods()


def test_wake_unknown_session():
    host = FakeAcpHost(sessions=[])
    with pytest.raises(WakeError) as exc:
        wake_session("ghost", "/tmp", lock_data=host.lock_data())
    assert exc.value.reason == "no_session"


def test_wake_unauthorized():
    host = FakeAcpHost(sessions=[IDLE_SESSION], hello_error="unauthorized")
    with pytest.raises(WakeError) as exc:
        wake_session(SID, "/tmp/ws", lock_data=host.lock_data())
    assert exc.value.reason == "unauthorized"


def test_item172_gwt2_hello_structured_error_code(tmp_path):
    """[ITEM-172.GWT-2] a structured {"code", "message"} hello error is
    classified by its code verbatim - the stable contract replaces the
    substring match whenever the host provides it."""
    host = FakeAcpHost(
        sessions=[IDLE_SESSION],
        hello_error={"code": "unauthorized", "message": "token rotated"},
    )
    with pytest.raises(WakeError) as exc:
        wake_session(SID, "/tmp/ws", lock_data=host.lock_data())
    assert exc.value.reason == "unauthorized"
    assert "token rotated" in str(exc.value)


def test_item172_gwt3_hello_unknown_code_maps_to_attach_failed(tmp_path):
    """[ITEM-172.GWT-3] a structured code we do not know maps to the
    generic attach failure - a novel host code cannot smuggle an
    unrecognized reason into the daemon's switch arms."""
    host = FakeAcpHost(
        sessions=[IDLE_SESSION],
        hello_error={"code": "quota_exceeded", "message": "nope"},
    )
    with pytest.raises(WakeError) as exc:
        wake_session(SID, "/tmp/ws", lock_data=host.lock_data())
    assert exc.value.reason == "attach_failed"


def test_wake_truncated_hello_clean_error():
    host = FakeAcpHost(sessions=[IDLE_SESSION], truncate_hello=True)
    with pytest.raises(WakeError):
        wake_session(SID, "/tmp/ws", lock_data=host.lock_data())


def test_wake_stalled_response_is_timeout(monkeypatch):
    import session_bridge.wake.acp_host as acp_host

    monkeypatch.setattr(acp_host, "_REQUEST_TIMEOUT", 0.3)
    host = FakeAcpHost(sessions=[IDLE_SESSION], hang=True)
    with pytest.raises(WakeError) as exc:
        wake_session(SID, "/tmp/ws", lock_data=host.lock_data())
    assert exc.value.reason in ("timeout", "malformed_message")


def test_wake_replay_overflow_still_prompts():
    """A flooded replay never wedges the wake: timeout tolerated, the
    prompt goes out, no crash (the fake's own write blocking makes a
    strict server-side assert unreliable - the contract is client
    behavior)."""
    host = FakeAcpHost(sessions=[IDLE_SESSION], replay_count=500)
    outcome = wake_session(
        SID, "/tmp/ws", lock_data=host.lock_data(), replay_timeout=0.05, replay_cap=10
    )
    assert outcome in ("sent", "sent_unconfirmed")


def test_wake_prompt_error_is_not_sent():
    """A session/prompt error response (e.g. host rejected a
    mid-turn prompt) must surface as rpc_error, not 'sent'."""

    class ErrHost(FakeAcpHost):
        def _client(self, conn):
            f = conn.makefile("rw")
            try:
                json.loads(f.readline())  # hello
                f.write(json.dumps({"ok": True, "protocol": 1}) + "\n")
                f.flush()
                for line in f:
                    msg = json.loads(line)
                    if "id" not in msg:
                        continue
                    method = msg.get("method")
                    out = {"jsonrpc": "2.0", "id": msg["id"]}
                    if method == "session/prompt":
                        out["error"] = {"code": -32000, "message": "session is busy"}
                    elif method == "session/list":
                        out["result"] = {"sessions": self.sessions}
                    else:
                        out["result"] = {}
                    f.write(json.dumps(out) + "\n")
                    f.flush()
            except (OSError, json.JSONDecodeError):
                pass
            finally:
                conn.close()

    host = ErrHost(sessions=[IDLE_SESSION])
    with pytest.raises(WakeError) as exc:
        wake_session(SID, "/tmp/ws", lock_data=host.lock_data())
    assert exc.value.reason == "rpc_error"


def test_wake_prompt_unconfirmed_still_reports():
    host = FakeAcpHost(sessions=[IDLE_SESSION], prompt_update=True)
    outcome = wake_session(SID, "/tmp/ws", lock_data=host.lock_data(), confirm_timeout=0.5)
    # the update notification or the response itself confirms "sent"
    assert outcome in ("sent", "sent_unconfirmed")


def test_nudge_text_mentions_sid_and_ack():
    text = nudge_text("abc-1")
    assert "abc-1" in text and "bridge ack" in text and "msg_" in text


# --- verify_sessions (item_21) ---------------------------------------------------


def test_verify_sessions_returns_status_map():
    """One shared attach answers statusEnum for the whole batch."""
    host = FakeAcpHost(
        sessions=[
            IDLE_SESSION,
            {
                "sessionId": "other",
                "cwd": "/tmp/ws2",
                "_meta": {"cognition.ai/statusEnum": "working"},
            },
        ]
    )
    out = verify_sessions([SID, "other", "ghost"], lock_data=host.lock_data())
    assert out == {SID: "finished", "other": "working", "ghost": SESSION_ABSENT}
    # one attach, one session/list - not per-session attaches
    assert host.methods().count("session/list") == 1


def test_verify_sessions_listed_statusless_is_not_absent():
    """A session present in the list but missing statusEnum meta reads
    as None - distinct from the SESSION_ABSENT marker (item_21 r2)."""
    host = FakeAcpHost(
        sessions=[
            {"sessionId": SID, "cwd": "/tmp/ws", "_meta": {}},
        ]
    )
    out = verify_sessions([SID], lock_data=host.lock_data())
    assert out == {SID: None}


def test_verify_sessions_attach_failure_is_wake_error():
    """A dead host raises, it does not report fake idle statuses."""
    host = FakeAcpHost(sessions=[IDLE_SESSION], hello_error="unauthorized")
    with pytest.raises(WakeError) as exc:
        verify_sessions([SID], lock_data=host.lock_data())
    assert exc.value.reason == "unauthorized"


def test_status_is_idle_values():
    assert status_is_idle("finished")
    assert status_is_idle("idle")
    assert not status_is_idle("working")
    assert not status_is_idle(None)


# --- WakePolicy ----------------------------------------------------------------


def test_policy_cooldown_blocks_second_attempt():
    clock = [1000.0]
    p = WakePolicy(cooldown_seconds=300, clock=lambda: clock[0])
    assert p.check("s", "h") is None
    p.record_attempt("s")
    assert p.check("s", "h") == "cooldown"
    clock[0] += 301
    assert p.check("s", "h") is None


def test_policy_retry_soon_shortens_cooldown():
    clock = [1000.0]
    p = WakePolicy(cooldown_seconds=300, clock=lambda: clock[0])
    p.record_attempt("s")
    p.record_retry_soon("s")
    assert p.check("s", "h") == "cooldown"  # still inside 60s
    clock[0] += 61
    assert p.check("s", "h") is None  # retried well before 300s


def test_policy_confirmed_rearms_full_cooldown():
    """[ITEM-162.GWT-4] A verified wake must not inherit the retry_soon
    window: record_confirmed re-arms the full cooldown, so a session
    still qualifying as a candidate is paced at 300s, not ~60s -
    the second half of the buttered-tilapia wake-loop fix."""
    clock = [1000.0]
    p = WakePolicy(cooldown_seconds=300, clock=lambda: clock[0])
    p.record_attempt("s")
    p.record_retry_soon("s")  # next allowed at t+60
    p.record_confirmed("s")  # verified working -> full cooldown
    clock[0] += 61
    assert p.check("s", "h") == "cooldown"  # was None before the fix
    clock[0] += 240
    assert p.check("s", "h") is None


def test_policy_unconfirmed_streak_grows_backoff():
    """Repeated unconfirmed wakes back off exponentially (60,120,240,
    capped at cooldown) instead of polling the host every 60s."""
    clock = [1000.0]
    p = WakePolicy(cooldown_seconds=300, clock=lambda: clock[0])
    p.record_attempt("s")
    p.record_retry_soon("s")
    clock[0] += 61  # pass 2 allowed
    p.record_attempt("s")
    p.record_retry_soon("s")  # streak 2 -> 120s
    clock[0] += 61
    assert p.check("s", "h") == "cooldown"
    clock[0] += 61
    assert p.check("s", "h") is None
    p.record_attempt("s")
    p.record_retry_soon("s")  # streak 3 -> 240s
    clock[0] += 121
    assert p.check("s", "h") == "cooldown"
    clock[0] += 121
    assert p.check("s", "h") is None
    p.record_confirmed("s")  # confirmed wake resets the streak
    p.record_attempt("s")
    p.record_retry_soon("s")
    clock[0] += 61
    assert p.check("s", "h") is None


def test_policy_breaker_opens_after_threshold():
    clock = [1000.0]
    p = WakePolicy(breaker_threshold=2, breaker_cooldown_seconds=600, clock=lambda: clock[0])
    assert p.record_result("h", False) is False
    assert p.record_result("h", False) is True  # breaker just opened
    assert p.check("s", "h") == "breaker_open"
    clock[0] += 601
    assert p.check("s", "h") is None  # window elapsed


def test_policy_success_resets_failures():
    p = WakePolicy(breaker_threshold=2)
    p.record_result("h", False)
    p.record_result("h", True)
    assert p.record_result("h", False) is False  # counter was reset


# --- daemon _wake_pass (G/W/T) --------------------------------------------------


def test_item16_gwt1_daemon_wakes_idle_managed(env, tmp_path, monkeypatch):
    """[ITEM-16.GWT-1] stale-hb managed session + relayed mail -> one wake."""
    host = FakeAcpHost(sessions=[IDLE_SESSION])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _relayed_mail(env.db, SID)

    daemon._wake_pass(env.db, env.sdir, int(time.time()), WakePolicy())

    assert "session/prompt" in host.methods()
    events = [d["event"] for d in _decisions(env)]
    assert "wake_sent" in events


def test_item16_gwt2_missing_lockfile(env, tmp_path, monkeypatch):
    """[ITEM-16.GWT-2] no lockfile -> wake_failed, mail stays relayed."""
    monkeypatch.setenv("SESSIONS_BRIDGE_ACP_LOCK_DIR", str(tmp_path / "empty"))
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    nid = _relayed_mail(env.db, SID)

    daemon._wake_pass(env.db, env.sdir, int(time.time()), WakePolicy())

    events = [d["event"] for d in _decisions(env)]
    assert "wake_failed" in events
    row = _notification(env.db, nid)
    assert row["relayed_at"] is not None and row["ack_at"] is None


def test_item16_gwt3_fresh_heartbeat_no_wake(env, tmp_path, monkeypatch):
    """[ITEM-16.GWT-3] fresh heartbeat -> no attach attempted.

    item_102c narrowed the guarantee: actionable mail now wakes a
    fresh-heartbeat DRAIN session, so this test seeds only a
    heartbeat_tick - the kind that must never wake on its own."""
    host = FakeAcpHost(sessions=[IDLE_SESSION])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=10)  # fresh
    nid = db.notify(SID, NotificationKind.HEARTBEAT_TICK, "tick", path=env.db)
    db.mark_relayed(nid, path=env.db)

    daemon._wake_pass(env.db, env.sdir, int(time.time()), WakePolicy())

    assert host.hellos == []


def test_item16_gwt4_cooldown_suppresses_second_wake(env, tmp_path, monkeypatch):
    """[ITEM-16.GWT-4] second pass inside the window -> no second attach."""
    host = FakeAcpHost(sessions=[IDLE_SESSION])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _relayed_mail(env.db, SID)
    policy = WakePolicy()

    daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)
    daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)

    assert len(host.hellos) == 1


def test_item16_gwt5_unauthorized_clean_failure(env, tmp_path, monkeypatch):
    """[ITEM-16.GWT-5] bad token -> wake_failed decision, daemon keeps running."""
    host = FakeAcpHost(sessions=[IDLE_SESSION], hello_error="unauthorized")
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _relayed_mail(env.db, SID)

    daemon._wake_pass(env.db, env.sdir, int(time.time()), WakePolicy())  # must not raise

    events = [d["event"] for d in _decisions(env)]
    assert "wake_failed" in events


def test_item16_busy_session_skipped_with_decision(env, tmp_path, monkeypatch):
    """Partner blocker: mid-turn session is never prompted."""
    host = FakeAcpHost(sessions=[BUSY_SESSION])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _relayed_mail(env.db, SID)

    daemon._wake_pass(env.db, env.sdir, int(time.time()), WakePolicy())

    assert "session/prompt" not in host.methods()
    events = [d["event"] for d in _decisions(env)]
    assert "wake_skipped_busy" in events


def test_item16_gwt6_bridge_wake_cli(env, tmp_path, monkeypatch, capsys):
    """[ITEM-16.GWT-6] `bridge wake <sid>` drives attach+load+prompt."""
    host = FakeAcpHost(sessions=[IDLE_SESSION])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)

    code = bridge.main(["--db", env.db, "wake", SID])

    assert code == 0
    assert "session/prompt" in host.methods()
    assert "prompt sent" in capsys.readouterr().out


def test_item16_bridge_wake_busy_conflict(env, tmp_path, monkeypatch, capsys):
    """[ITEM-16.GWT-6b] manual wake keeps the busy check."""
    host = FakeAcpHost(sessions=[BUSY_SESSION])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)

    code = bridge.main(["--db", env.db, "wake", SID])

    assert code != 0
    assert "session/prompt" not in host.methods()
    assert "mid-turn" in capsys.readouterr().out


def test_item16_no_session_does_not_trip_breaker(env, tmp_path, monkeypatch):
    """no_session is session-level, not a host failure: a fleet of
    dead tabs must not open the circuit breaker."""
    host = FakeAcpHost(sessions=[])  # session absent from list
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _relayed_mail(env.db, SID)
    policy = WakePolicy(cooldown_seconds=0, breaker_threshold=2)

    for _ in range(3):
        daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)

    events = [d["event"] for d in _decisions(env)]
    assert "wake_breaker_open" not in events
    assert events.count("wake_failed") == 3


def test_item16_lockfile_reread_mid_pass(env, tmp_path, monkeypatch):
    """Host rotates between candidates of one pass: s1 wakes against
    host A; during that wake the lockfile is replaced by host B; s2's
    attempt must use B (the re-read), not the pass-start snapshot."""
    s2 = "s2"
    lockdir = tmp_path / "locks"
    lockdir.mkdir()
    lockfile = lockdir / "acp-host-test.lock"

    host_a = FakeAcpHost(sessions=[IDLE_SESSION])
    host_b = FakeAcpHost(
        sessions=[{**IDLE_SESSION, "sessionId": s2}],
    )

    def write_lock(data):
        data = dict(data)
        data.pop("_path", None)
        lockfile.write_text(json.dumps(data))

    write_lock(host_a.lock_data())
    monkeypatch.setenv("SESSIONS_BRIDGE_ACP_LOCK_DIR", str(lockdir))
    # during s1's wake the host rotates: lockfile now points at B
    host_a.on_prompt = lambda: write_lock(host_b.lock_data())

    for s in (SID, s2):
        _session(env.db, s, managed=True)
        _heartbeat(env.db, s, age=999)
        _relayed_mail(env.db, s)

    daemon._wake_pass(env.db, env.sdir, int(time.time()), WakePolicy())

    assert "session/prompt" in host_a.methods()
    assert "session/prompt" in host_b.methods()


def test_item16_breaker_suspends_after_failures(env, tmp_path, monkeypatch):
    """Circuit breaker: consecutive failures suspend the host key."""
    host = FakeAcpHost(sessions=[IDLE_SESSION], hello_error="unauthorized")
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _relayed_mail(env.db, SID)
    policy = WakePolicy(cooldown_seconds=0, breaker_threshold=2)

    daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)
    daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)
    daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)

    # two consecutive host failures open the breaker; the third pass
    # is suspended before it can even try to attach
    events = [d["event"] for d in _decisions(env)]
    assert "wake_breaker_open" in events
    assert len(host.hellos) == 2


# --- item_17: managed dead-path via no_session streak --------------------------


def _live_task(path, sid):
    tid = db.enqueue(sid, "work", path=path)
    assert db.claim_queued(tid, "w1", Transport.ACP_PUSH, path=path)
    assert db.mark_running(tid, "w1", path=path)
    return tid


def test_policy_no_session_streak_reset():
    p = WakePolicy()
    assert p.record_no_session("s") == 1
    assert p.record_no_session("s") == 2
    p.record_session_seen("s")
    assert p.record_no_session("s") == 1


def test_item17_gwt4_no_session_streak_marks_dead(env, tmp_path, monkeypatch):
    """[ITEM-17.GWT-4] no_session x3 -> managed session marked dead,
    live task requeued like the unmanaged sweep would do."""
    host = FakeAcpHost(sessions=[])  # SID absent -> no_session
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _relayed_mail(env.db, SID)
    tid = _live_task(env.db, SID)
    policy = WakePolicy(cooldown_seconds=0)

    for _ in range(3):
        daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)

    session = db.get_session(SID, path=env.db)
    assert session["status"] == "dead"
    dead = [d for d in _decisions(env) if d["event"] == "worker_dead"]
    assert dead and "no_session_streak" in dead[0]["detail"]
    task = db.get_task(tid, path=env.db)
    assert task["status"] == TaskStatus.QUEUED.value  # requeued, not failed


def test_item17_seen_outcome_resets_no_session_streak(env, tmp_path, monkeypatch):
    """A session that reappears in session/list resets the streak -
    intermittent absence never kills it."""
    host = FakeAcpHost(sessions=[])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _relayed_mail(env.db, SID)
    _live_task(env.db, SID)
    policy = WakePolicy(cooldown_seconds=0)

    for _ in range(2):
        daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)
    host.sessions.append(IDLE_SESSION)  # session reappears
    daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)
    host.sessions.remove(IDLE_SESSION)  # and vanishes again
    for _ in range(2):
        daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)

    assert db.get_session(SID, path=env.db)["status"] == "active"  # streak reset
    daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)
    assert db.get_session(SID, path=env.db)["status"] == "dead"  # 3rd consecutive


def test_item162_live_task_only_probes_never_wakes(env, tmp_path, monkeypatch):
    """[ITEM-162.GWT-1] A managed session with a live task but an empty
    mailbox gets a session/list presence probe - never a drain prompt:
    the stale-hb + live-task combination is structural for a DRAIN
    session between turns, so waking on it looped drain prompts on a
    physically empty queue (the buttered-tilapia incident)."""
    host = FakeAcpHost(sessions=[IDLE_SESSION])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _live_task(env.db, SID)  # no notification at all

    daemon._wake_pass(env.db, env.sdir, int(time.time()), WakePolicy())

    assert "session/prompt" not in host.methods()
    assert any(d["event"] == "wake_probe" for d in _decisions(env))
    assert not any(d["event"] == "wake_sent" for d in _decisions(env))


def test_item162_probe_absent_marks_dead_without_prompt(env, tmp_path, monkeypatch):
    """[ITEM-162.GWT-2] The probe keeps no_session dead-marking alive on
    an empty outbox: a session absent from session/list across the
    streak threshold is marked dead - and no prompt is ever sent."""
    host = FakeAcpHost(sessions=[])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _live_task(env.db, SID)
    policy = WakePolicy(cooldown_seconds=0)

    for _ in range(3):
        daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)
    assert db.get_session(SID, path=env.db)["status"] == "dead"
    assert "session/prompt" not in host.methods()
    assert any(d["event"] == "wake_probe_absent" for d in _decisions(env))


def test_item162_live_task_plus_mail_still_wakes(env, tmp_path, monkeypatch):
    """[ITEM-162.GWT-3] Mail keeps the prompt path: a live task session
    with unacked relayed mail is a wake row, not a probe."""
    host = FakeAcpHost(sessions=[IDLE_SESSION])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _live_task(env.db, SID)
    _relayed_mail(env.db, SID)

    daemon._wake_pass(env.db, env.sdir, int(time.time()), WakePolicy())

    assert "session/prompt" in host.methods()
    assert any(d["event"] == "wake_sent" for d in _decisions(env))
    assert not any(d["event"] == "wake_probe" for d in _decisions(env))


def test_item162_probe_paced_by_cooldown(env, tmp_path, monkeypatch):
    """[ITEM-162.GWT-5] The probe shares the per-session cooldown: a
    second pass inside the window does not re-attach session/list."""
    host = FakeAcpHost(sessions=[IDLE_SESSION])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _live_task(env.db, SID)
    policy = WakePolicy()

    daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)
    probes = sum(d["event"] == "wake_probe" for d in _decisions(env))
    assert probes == 1

    daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)
    assert sum(d["event"] == "wake_probe" for d in _decisions(env)) == 1


def test_item162_probe_absent_with_fresh_ack_is_live(env, tmp_path, monkeypatch):
    """[ITEM-162.GWT-7] Parity with _verify_unconfirmed_wakes: a probe
    whose session is absent from session/list but acked within the
    grace window is alive evidence, not no_session - the streak does
    not move and no dead-marking happens."""
    host = FakeAcpHost(sessions=[])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _live_task(env.db, SID)
    _relayed_mail(env.db, SID)
    db.ack_notifications(SID, path=env.db)  # fresh watermark, mail drained

    daemon._wake_pass(env.db, env.sdir, int(time.time()), WakePolicy())

    probe = [d for d in _decisions(env) if d["event"] == "wake_probe"]
    assert probe and probe[0]["detail"] == "absent_but_acked"
    assert db.get_session(SID, path=env.db)["status"] == "active"
    assert not any(d["event"] == "wake_probe_absent" for d in _decisions(env))


def test_item162_probe_attach_failure_leaves_streak(env, tmp_path, monkeypatch):
    """[ITEM-162.GWT-6] A failed probe attach is host evidence, not
    session evidence: wake_probe_failed is journaled, the no_session
    streak stays at zero, the session is not dead-marked, and the
    failure still counts toward the host breaker."""
    host = FakeAcpHost(sessions=[IDLE_SESSION])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _live_task(env.db, SID)
    policy = WakePolicy()
    monkeypatch.setattr(
        daemon,
        "verify_sessions",
        lambda sids, **k: (_ for _ in ()).throw(WakeError("attach_failed", "boom")),
    )

    daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)

    assert any(d["event"] == "wake_probe_failed" for d in _decisions(env))
    assert db.get_session(SID, path=env.db)["status"] == "active"
    assert policy._no_session_streak.get(SID, 0) == 0
    assert list(policy._consecutive_failures.values()) == [1]


def test_item17_dead_session_not_a_wake_candidate(env, tmp_path, monkeypatch):
    """Once marked dead the session leaves the candidate set - no
    repeated attaches or wake_failed spam while mail sits unacked."""
    host = FakeAcpHost(sessions=[])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _relayed_mail(env.db, SID)
    _live_task(env.db, SID)
    policy = WakePolicy(cooldown_seconds=0)

    for _ in range(3):
        daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)
    assert db.get_session(SID, path=env.db)["status"] == "dead"
    hellos_after_dead = len(host.hellos)
    for _ in range(3):
        daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)
    assert len(host.hellos) == hellos_after_dead  # no further attempts


def test_item17_no_cwd_counts_as_seen_not_absent(env, tmp_path, monkeypatch):
    """Session in the list but with no cwd -> no_cwd, streak stays 0:
    a present-but-unwakeable session is never marked dead."""
    host = FakeAcpHost(
        sessions=[{"sessionId": SID, "_meta": {"cognition.ai/statusEnum": "finished"}}]
    )
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _relayed_mail(env.db, SID)
    _live_task(env.db, SID)
    con = db.connect(env.db)
    try:
        con.execute("UPDATE sessions SET cwd=NULL WHERE session_id=?", (SID,))
        con.commit()
    finally:
        con.close()
    policy = WakePolicy(cooldown_seconds=0)

    for _ in range(5):
        daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)

    assert db.get_session(SID, path=env.db)["status"] == "active"
    failed = [d for d in _decisions(env) if d["event"] == "wake_failed"]
    assert failed and all("no_cwd" in d["detail"] for d in failed)
    assert not any(d["event"] == "worker_dead" for d in _decisions(env))


# --- item_139: statusless sessions are wakeable, not busy ---------------------


def test_item139_gwt1_statusless_entry_is_not_busy():
    """[ITEM-139.GWT-1] a persisted session row with no statusEnum is
    inert (closed client, acp-host resumes on demand), so _is_busy
    must return False; None stays conservatively busy and an explicit
    working status still blocks."""
    from session_bridge.wake.acp_host import _is_busy

    assert not _is_busy({"sessionId": SID, "cwd": "/tmp/ws"})
    assert not _is_busy({"sessionId": SID, "cwd": "/tmp/ws", "_meta": {}})
    assert not _is_busy(IDLE_SESSION)
    assert _is_busy(BUSY_SESSION)
    assert _is_busy(None)


def test_item139_gwt2_statusless_session_gets_wake_injected(env, tmp_path, monkeypatch):
    """[ITEM-139.GWT-2] managed session + stale hb + relayed mail +
    a statusless session/list entry -> attach+load+prompt: the exact
    shore-felidae/elegant-scallion incident shape that used to be
    skipped as busy forever."""
    statusless = {"sessionId": SID, "cwd": "/tmp/ws"}  # no _meta at all
    host = FakeAcpHost(sessions=[statusless])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _relayed_mail(env.db, SID)
    policy = WakePolicy(cooldown_seconds=0)

    daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)

    assert "session/prompt" in host.methods()
    events = [d["event"] for d in _decisions(env)]
    assert "wake_skipped_busy" not in events


def test_item139_gwt3_explicit_busy_status_still_skipped(env, tmp_path, monkeypatch):
    """[ITEM-139.GWT-3] the fix widens only the statusless case: an
    explicit non-idle statusEnum still skips the injection."""
    host = FakeAcpHost(sessions=[BUSY_SESSION])
    _lockdir(tmp_path, host, monkeypatch)
    _session(env.db, SID, managed=True)
    _heartbeat(env.db, SID, age=999)
    _relayed_mail(env.db, SID)
    policy = WakePolicy(cooldown_seconds=0)

    daemon._wake_pass(env.db, env.sdir, int(time.time()), policy)

    assert "session/prompt" not in host.methods()
    events = [d["event"] for d in _decisions(env)]
    assert "wake_skipped_busy" in events

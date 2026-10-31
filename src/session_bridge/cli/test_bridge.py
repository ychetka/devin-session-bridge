"""Tests for the bridge CLI + dispatcher (item_07).

Boundaries are mocked at their contract edges: ``AcpClient`` (acp
transport), ``db.is_locked`` + ``db.sessions_db_lookup`` (devin's own
storage), ``bridge._spawn_worker`` (subprocess boundary) and
``bridge._notify_send`` (notify-send). state.db itself stays real in a
tmp_path via ``SESSIONS_BRIDGE_DB``.
"""

import io
import json
import os
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest

from session_bridge.acp.contract import AcpError, PromptResult
from session_bridge.cli import bridge
from session_bridge.cli.contract import (
    EventKind,
    ExitCode,
    JsonResponse,
)
from session_bridge.daemon import core as daemon
from session_bridge.state import db
from session_bridge.state.contract import (
    AuditEventKind,
    NotificationKind,
    SessionMode,
    SessionStatus,
    TaskStatus,
    Transport,
)

SID = "s1"


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """Isolated state.db + trigger dir + mocked dispatch boundaries."""
    db_path = str(tmp_path / "state.db")
    db.init(db_path)
    qdir = tmp_path / "queue"
    monkeypatch.setenv("SESSIONS_BRIDGE_DB", db_path)
    monkeypatch.setenv("SESSIONS_BRIDGE_QUEUE_DIR", str(qdir))
    spawned = []
    monkeypatch.setattr(
        bridge,
        "_spawn_worker",
        lambda tid, owner, args: spawned.append((tid, owner)),
    )
    locks = {}
    monkeypatch.setattr(db, "is_locked", lambda sid: locks.get(sid, (False, None)))
    devin = {}
    monkeypatch.setattr(db, "sessions_db_lookup", lambda sid: devin.get(sid))
    sent = []
    monkeypatch.setattr(bridge, "_notify_send", lambda title, body: sent.append((title, body)))
    return SimpleNamespace(
        db=db_path,
        qdir=qdir,
        spawned=spawned,
        locks=locks,
        devin=devin,
        sent=sent,
    )


def _run(argv, capsys):
    rc = bridge.main(argv)
    out = capsys.readouterr().out.strip()
    return rc, (json.loads(out) if out else None)


def _task(env, sid=SID, text="do the thing", **kw):
    tid = db.enqueue(sid, text, path=env.db, **kw)
    assert tid is not None
    return tid


def _claim_acp(env, tid, owner="acp-9-1"):
    assert db.claim_queued(tid, owner, Transport.ACP_PUSH, path=env.db)
    assert db.mark_running(tid, owner, path=env.db)
    return owner


def _trigger(env, sid=SID):
    return Path(env.qdir) / f"{sid}.json"


class _OkClient:
    """AcpClient stand-in: load + prompt succeed with end_turn."""

    prompts: ClassVar[list] = []

    def __init__(self, *a, **kw):
        pass

    def load_session(self, sid, cwd):
        pass

    def prompt(self, sid, text, timeout=None):
        type(self).prompts.append((sid, text))
        return PromptResult(text="assistant tail text", stop_reason="end_turn", usage={})

    def close_session(self, sid):
        pass

    def terminate(self):
        pass


class _LockedClient(_OkClient):
    """AcpClient stand-in: session/load answers session_locked."""

    def load_session(self, sid, cwd):
        raise AcpError(
            "session is locked",
            kind="session_locked",
            lock_holder_pid=4242,
        )


# --- GWT-1: push on an unlocked session -> acp-push running + worker --------


def test_item07_gwt1_push_unlocked_dispatches_acp_worker(env, capsys):
    env.devin[SID] = {"working_directory": "/repo"}
    rc, out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)
    assert resp.session == SID
    assert resp.status == TaskStatus.RUNNING.value
    tid = out["data"]["task_id"]
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["transport"] == Transport.ACP_PUSH.value
    assert row["lease_owner"].startswith(bridge.ACP_OWNER_PREFIX)
    assert row["lease_expires_at"] > int(time.time())
    # the detached worker was spawned with the lease owner as argv
    assert env.spawned == [(tid, row["lease_owner"])]


def test_item07_gwt1_push_with_task_file(env, capsys):
    env.devin[SID] = {"working_directory": "/repo"}
    rc, out = _run(["push", SID, "--file", "/specs/plan.md", "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(out["data"]["task_id"], path=env.db)
    assert row["task_file"] == "/specs/plan.md"
    assert row["status"] == TaskStatus.RUNNING.value


def test_item07_gwt1_second_push_on_live_session_conflicts(env, capsys):
    env.devin[SID] = {"working_directory": "/repo"}
    _task(env)
    rc, out = _run(["push", SID, "--task", "second", "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert "live task" in out["error"]
    # only one task exists; the second text never entered the db
    tasks = db.list_tasks(path=env.db)
    assert len(tasks) == 1


# --- GWT-2: push on a locked session -> hook-queue + trigger -----------------


def test_item07_gwt2_push_locked_goes_hook_queue(env, capsys):
    env.locks[SID] = (True, 1234)
    rc, out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)
    tid = out["data"]["task_id"]
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.QUEUED.value
    assert row["transport"] == Transport.HOOK_QUEUE.value
    assert row["lease_owner"] is None
    assert "do not retry" in resp.agent_instruction
    # the trigger file was written after the db commit
    trigger = _trigger(env)
    assert json.loads(trigger.read_text())["session_id"] == SID
    assert env.spawned == []


# --- GWT-3: session_locked on load -> atomic hook fallback --------------------


def test_item07_gwt3_work_session_locked_falls_back_to_hook(env, capsys, monkeypatch):
    env.devin[SID] = {"working_directory": "/repo"}
    env.locks[SID] = (True, 777)
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(bridge, "AcpClient", _LockedClient)
    rc, out = _run(["_work", str(tid), "--owner", owner, "--json"], capsys)
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)
    assert "do not retry" in resp.agent_instruction
    row = db.get_task(tid, path=env.db)
    # no half-state: back to queued on the hook transport, lease cleared
    assert row["status"] == TaskStatus.QUEUED.value
    assert row["transport"] == Transport.HOOK_QUEUE.value
    assert row["lease_owner"] is None
    assert row["lease_expires_at"] is None
    assert json.loads(_trigger(env).read_text())["session_id"] == SID


def test_item07_gwt3_work_session_locked_from_dispatch_decided(env, capsys, monkeypatch):
    # the fallback also wins while the task is still dispatch_decided
    env.devin[SID] = {"working_directory": "/repo"}
    env.locks[SID] = (True, 777)
    tid = _task(env)
    owner = "acp-9-7"
    assert db.claim_queued(tid, owner, Transport.ACP_PUSH, path=env.db)
    monkeypatch.setattr(bridge, "AcpClient", _LockedClient)
    rc, _out = _run(["_work", str(tid), "--owner", owner, "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.QUEUED.value
    assert row["transport"] == Transport.HOOK_QUEUE.value
    assert _trigger(env).exists()


# --- GWT-4: worker exit pumps the next queued task (semaphore) ----------------


def test_item07_gwt4_finished_worker_dispatches_next_queued(env, capsys, monkeypatch):
    env.devin["s1"] = {"working_directory": "/r1"}
    env.devin["s2"] = {"working_directory": "/r2"}
    t1 = _task(env, sid="s1")
    owner1 = _claim_acp(env, t1, owner="acp-1-1")
    t2 = _task(env, sid="s2")
    monkeypatch.setattr(bridge, "AcpClient", _OkClient)
    _OkClient.prompts.clear()
    rc, _ = _run(["_work", str(t1), "--owner", owner1], capsys)
    assert rc == ExitCode.OK
    r1 = db.get_task(t1, path=env.db)
    assert r1["status"] == TaskStatus.DONE.value
    assert r1["last_assistant_message"] == "assistant tail text"
    # the worker's exit pump dispatched the next queued task
    r2 = db.get_task(t2, path=env.db)
    assert r2["status"] == TaskStatus.RUNNING.value
    assert r2["transport"] == Transport.ACP_PUSH.value
    assert (t2, r2["lease_owner"]) in env.spawned


def test_item07_gwt4_semaphore_keeps_extra_tasks_queued(env, capsys):
    # three running acp-push tasks fill the semaphore
    for i in range(3):
        sid = f"busy{i}"
        env.devin[sid] = {"working_directory": f"/r{i}"}
        _claim_acp(env, _task(env, sid=sid), owner=f"acp-old-{i}")
    env.devin["s4"] = {"working_directory": "/r4"}
    rc, out = _run(["push", "s4", "--task", "later", "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(out["data"]["task_id"], path=env.db)
    # no free slot: the task stays queued, no worker spawned
    assert row["status"] == TaskStatus.QUEUED.value
    assert row["transport"] is None
    assert env.spawned == []


# --- GWT-5: status --json schema + events --------------------------------------


def test_item07_gwt5_status_json_schema_and_events(env, capsys):
    # a parked task awaiting a user kick
    t1 = _task(env, sid="s1")
    assert db.claim_queued(t1, "hook-1", Transport.HOOK_QUEUE, path=env.db)
    assert db.mark_running(t1, "hook-1", path=env.db)
    assert db.set_awaiting_user(t1, "hook-1", note="park budget exhausted", path=env.db)
    # a terminally failed task
    t2 = _task(env, sid="s2")
    o2 = _claim_acp(env, t2, owner="acp-2-2")
    assert db.finish_task(t2, o2, ok=False, note="boom", path=env.db)
    rc, out = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)  # schema conforming
    kinds = {(e.kind, e.session_id) for e in resp.events}
    assert (EventKind.NEEDS_KICK, "s1") in kinds
    assert (EventKind.FAILED, "s2") in kinds
    states = {v["session"]: v["state"] for v in resp.data["sessions"]}
    assert states["s1"] == bridge.STATE_NEEDS_KICK
    assert states["s2"] == bridge.STATE_FAILED
    # notifications went out once per transitioned task
    titles = [t for t, _ in env.sent]
    assert "sessions-bridge NEEDS_KICK" in titles
    assert "sessions-bridge FAILED" in titles


def test_item07_gwt5_status_human_table(env, capsys):
    _task(env)
    rc = bridge.main(["status"])
    assert rc == ExitCode.OK
    table = capsys.readouterr().out
    header = next(line for line in table.splitlines() if "STATE" in line)
    for col in ("SID", "REPO", "TASK", "STATE", "Q", "FAIL", "HB"):
        assert col in header
    # the pushed-but-never-registered session shows as a fleet row
    assert any(line.startswith("*s1") for line in table.splitlines())


# --- GWT-6: push on an unknown session -> not_found, task preserved -----------


def test_item07_gwt6_push_unknown_session_not_found(env, capsys):
    # no lock, no devin sessions.db row, no bridge session row
    rc, out = _run(["push", "ghost", "--task", "do it", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND
    resp = JsonResponse.model_validate(out)
    assert resp.error.startswith("not_found")
    assert resp.agent_instruction is not None
    # the task is preserved in the queue, not lost
    live = db.get_live_task("ghost", path=env.db)
    assert live is not None
    assert live["status"] == TaskStatus.QUEUED.value
    assert env.spawned == []


def test_item07_gwt6_work_missing_session_fails_task(env, capsys, monkeypatch):
    # _work cannot resolve a cwd: controlled not_found failure
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(bridge, "AcpClient", _OkClient)
    rc, out = _run(["_work", str(tid), "--owner", owner, "--json"], capsys)
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)
    assert resp.status == TaskStatus.FAILED.value
    assert "not_found" in resp.error
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.FAILED.value
    assert env.sent  # operator was notified


# --- GWT-7: clear / unpark / retry ---------------------------------------------


def test_item07_gwt7_clear_removes_queued_task(env, capsys):
    tid = _task(env)
    rc, out = _run(["clear", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["status"] == "cleared"
    assert db.get_task(tid, path=env.db) is None


def test_item07_gwt7_clear_non_queued_conflicts(env, capsys):
    tid = _task(env)
    _claim_acp(env, tid)
    rc, _out = _run(["clear", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert db.get_task(tid, path=env.db)["status"] == TaskStatus.RUNNING.value


def test_item07_gwt7_unpark_resumes_queue_and_rewrites_trigger(env, capsys):
    env.locks[SID] = (True, 555)
    tid = _task(env)
    owner = "hook-1"
    assert db.claim_queued(tid, owner, Transport.HOOK_QUEUE, path=env.db)
    assert db.mark_running(tid, owner, path=env.db)
    assert db.set_awaiting_user(tid, owner, path=env.db)
    db.session_pause(SID, True, path=env.db)
    rc, _out = _run(["unpark", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    # queue pause cleared and the awaiting task is back on the hook queue
    assert db.get_session(SID, path=env.db)["queue_paused"] == 0
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.QUEUED.value
    assert row["transport"] == Transport.HOOK_QUEUE.value
    assert _trigger(env).exists()
    assert env.spawned == []


def test_item07_gwt7_retry_failed_requeues_with_attempt_reset(env, capsys):
    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    owner = _claim_acp(env, tid)
    assert db.finish_task(tid, owner, ok=False, note="boom", path=env.db)
    rc, _out = _run(["retry", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    assert row["attempt"] == 0
    # pump ran: the unlocked session got an acp worker
    assert row["status"] == TaskStatus.RUNNING.value
    assert (tid, row["lease_owner"]) in env.spawned


def test_item07_gwt7_retry_non_failed_conflicts(env, capsys):
    tid = _task(env)
    rc, _out = _run(["retry", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert db.get_task(tid, path=env.db)["status"] == TaskStatus.QUEUED.value


# --- worker failure paths --------------------------------------------------------


def test_item07_work_failed_turn_finishes_failed_and_notifies(env, capsys, monkeypatch):
    class RefusalClient(_OkClient):
        def prompt(self, sid, text, timeout=None):
            return PromptResult(text="", stop_reason="refusal", usage={})

    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(bridge, "AcpClient", RefusalClient)
    rc, out = _run(["_work", str(tid), "--owner", owner, "--json"], capsys)
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)
    assert resp.status == TaskStatus.FAILED.value
    assert any(e.kind == EventKind.FAILED for e in resp.events)
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.FAILED.value
    assert "refusal" in (row["note"] or "")
    assert env.sent


def test_item07_work_transient_acp_error_waits_for_lease_recovery(env, capsys, monkeypatch):
    class FlakyClient(_OkClient):
        def load_session(self, sid, cwd):
            raise AcpError("silence", kind="timeout", retryable=True)

    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(bridge, "AcpClient", FlakyClient)
    rc, _out = _run(["_work", str(tid), "--owner", owner, "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    # left running under its lease: recover_stale owns the requeue
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["lease_owner"] == owner


def test_item07_work_prompt_timeout_is_terminal(env, capsys, monkeypatch):
    class SlowClient(_OkClient):
        def prompt(self, sid, text, timeout=None):
            raise AcpError("no traffic", kind="timeout", retryable=True)

    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(bridge, "AcpClient", SlowClient)
    rc, _out = _run(["_work", str(tid), "--owner", owner, "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.FAILED.value
    assert "timeout" in row["note"]


# --- inspect ----------------------------------------------------------------------


def test_item07_inspect_by_session_and_task_id(env, capsys):
    tid = _task(env)
    rc, out = _run(["inspect", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)
    assert resp.data["task_id"] == tid
    assert resp.data["task_text"] == "do the thing"
    assert resp.data["wait_seconds"] >= 0
    assert resp.data["next_action"]
    rc, out2 = _run(["inspect", str(tid), "--json"], capsys)
    assert rc == ExitCode.OK
    assert out2["data"]["task_id"] == tid


def test_item07_inspect_unknown_ref_not_found(env, capsys):
    rc, _out = _run(["inspect", "nope", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND


# --- recovery inside pump ----------------------------------------------------------


def test_item07_pump_recovers_stale_running(env, capsys):
    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    _claim_acp(env, tid)
    con = db.connect(env.db)
    try:
        con.execute(
            "UPDATE tasks SET lease_expires_at=? WHERE id=?",
            (int(time.time()) - 1, tid),
        )
    finally:
        con.close()
    rc, _out = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    # lease expired -> requeued by recover_stale (attempt 1 < MAX)
    assert row["status"] in (
        TaskStatus.QUEUED.value,
        TaskStatus.RUNNING.value,  # redispatched in the same pass
    )
    assert row["attempt"] == 1


# --- item_10: hardening follow-ups -------------------------------------------


def _run_status_json(args_db):
    """Invoke ``bridge --db <db> status --json``; returns (rc, dict)."""
    buf = io.StringIO()
    old = sys.stdout
    try:
        sys.stdout = buf
        rc = bridge.main(["--db", args_db, "status", "--json"])
    finally:
        sys.stdout = old
    return rc, json.loads(buf.getvalue().strip())


def test_item10_gwt1_queue_depth_true_count_pump_processes_all(env, capsys):
    # 60 queued tasks on locked sessions: beyond the old limit=50 scan
    for i in range(60):
        sid = f"lock{i}"
        env.locks[sid] = (True, 1000 + i)
        _task(env, sid=sid)
    rc, out = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    # the true COUNT(*) is reported, not the capped task list
    assert out["queue_depth"] == 60
    assert out["data"]["queue_shown"] == 60
    assert out["data"]["queue_truncated"] is False
    assert out["data"]["in_flight_dispatch"] == 0
    assert out["data"]["concurrency"] == {
        "active": 0,
        "max": bridge.SEMAPHOR,
    }
    # one pump pass folded every queued task onto the hook transport
    tasks = db.list_tasks(status=TaskStatus.QUEUED, path=env.db, limit=100)
    assert len(tasks) == 60
    assert all(t["transport"] == Transport.HOOK_QUEUE.value for t in tasks)
    assert len(list(Path(env.qdir).glob("*.json"))) == 60
    assert env.spawned == []


def test_item10_gwt2_concurrent_pumps_never_exceed_semaphore(env):
    for i in range(8):
        sid = f"s{i}"
        env.devin[sid] = {"working_directory": f"/r{i}"}
        _task(env, sid=sid)
    args = SimpleNamespace(db=env.db, queue_dir=str(env.qdir), timeout=None)
    max_seen = [0]

    threads = [threading.Thread(target=lambda: bridge.pump(args)) for _ in range(3)]
    for t in threads:
        t.start()
    while any(t.is_alive() for t in threads):
        n = db.count_tasks(
            status=TaskStatus.RUNNING,
            transport=Transport.ACP_PUSH,
            path=env.db,
        )
        max_seen[0] = max(max_seen[0], n)
        time.sleep(0.005)
    for t in threads:
        t.join(30)

    # atomic admission: racing pumps can never oversubscribe the
    # semaphore, sampled or final
    running = db.count_tasks(
        status=TaskStatus.RUNNING,
        transport=Transport.ACP_PUSH,
        path=env.db,
    )
    assert running == bridge.SEMAPHOR
    assert max_seen[0] <= bridge.SEMAPHOR
    assert len(env.spawned) == bridge.SEMAPHOR
    # unadmitted tasks folded back to queued, never wedged
    assert db.count_tasks(status=TaskStatus.QUEUED, path=env.db) == 5
    assert db.count_tasks(status=TaskStatus.DISPATCH_DECIDED, path=env.db) == 0

    # and status surfaces the saturation to the operator
    _rc, out = _run_status_json(args_db=env.db)
    assert out["data"]["saturation_reason"] == "MAX_CONCURRENT_REACHED"
    assert out["data"]["concurrency"]["active"] == bridge.SEMAPHOR


def test_item10_mark_running_lease_refusal_is_not_saturation(env, capsys, monkeypatch):
    # a mark_running refusal on an expired claim lease is not
    # semaphore saturation: status --json must not report
    # MAX_CONCURRENT_REACHED for a non-semaphore CAS loss
    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)

    real_claim = db.claim_queued

    def claim_then_expire(tid_, owner, transport, *, path=None):
        ok = real_claim(tid_, owner, transport, path=path)
        if ok:
            con = db.connect(path)
            try:
                con.execute(
                    "UPDATE tasks SET lease_expires_at=? WHERE id=?",
                    (int(time.time()) - 1, tid_),
                )
            finally:
                con.close()
        return ok

    monkeypatch.setattr(db, "claim_queued", claim_then_expire)

    rc, out = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    assert "saturation_reason" not in out["data"]
    # the refused claim folded back to queued, never wedged
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.QUEUED.value
    assert env.spawned == []


def test_item10_gwt4_detached_worker_stderr_log(tmp_path, monkeypatch):
    db_path = str(tmp_path / "state.db")
    db.init(db_path)
    # a python wrapper that emits a deterministic stderr line, then
    # execs the real interpreter - proves stderr lands in the log file
    real_python = sys.executable
    wrapper = tmp_path / "fakepy.sh"
    wrapper.write_text(f'#!/bin/sh\necho "worker stderr marker" >&2\nexec "{real_python}" "$@"\n')
    wrapper.chmod(0o755)
    monkeypatch.setattr(sys, "executable", str(wrapper))

    tid = db.enqueue(SID, "do the thing", path=db_path)
    owner = "acp-9-9"
    assert db.claim_queued(tid, owner, Transport.ACP_PUSH, path=db_path)
    args = SimpleNamespace(
        db=db_path,
        queue_dir=str(tmp_path / "queue"),
        timeout=5,
    )
    proc = bridge._spawn_worker(tid, owner, args)
    assert proc.wait(timeout=120) == 0

    logs = list((tmp_path / "logs").glob(f"worker_{tid}_*_*.log"))
    assert len(logs) == 1
    content = logs[0].read_text()
    assert "worker stderr marker" in content
    assert logs[0].stat().st_size <= bridge.WORKER_LOG_MAX_BYTES

    # the crashed/failed task surfaces log_path + stderr_tail
    rc, out = _run_status_json(args_db=db_path)
    assert rc == ExitCode.OK
    view = next(v for v in out["data"]["sessions"] if v["session"] == SID)
    assert view["state"] == bridge.STATE_FAILED
    assert view["log_path"] == str(logs[0])
    assert "worker stderr marker" in view["stderr_tail"]


def test_item10_gwt4_log_sweep_cap_and_retention(tmp_path):
    db_path = str(tmp_path / "state.db")
    db.init(db_path)
    args = SimpleNamespace(db=db_path)
    log_dir = Path(bridge._logs_dir(args))
    log_dir.mkdir(parents=True)
    # 12 small logs with increasing mtime + one oversized log (newest)
    for i in range(12):
        p = log_dir / f"worker_{i}_0_{100 + i}.log"
        p.write_bytes(f"log line {i}\n".encode())
        os.utime(p, (1000 + i, 1000 + i))
    big = log_dir / "worker_99_0_999.log"
    big.write_bytes(b"HEAD\n" + b"y" * (bridge.WORKER_LOG_MAX_BYTES + 64) + b"TAIL\n")
    os.utime(big, (2000, 2000))
    bridge._sweep_worker_logs(args)

    files = sorted(log_dir.glob("worker_*.log"))
    # retention keeps only the newest WORKER_LOG_KEEP files
    assert len(files) == bridge.WORKER_LOG_KEEP
    assert big in files
    # the oversized file keeps its tail within the cap
    data = big.read_bytes()
    assert len(data) <= bridge.WORKER_LOG_MAX_BYTES
    assert data.rstrip().endswith(b"TAIL")
    oldest_gone = log_dir / "worker_0_0_100.log"
    assert not oldest_gone.exists()


def test_item10_gwt4_log_sweep_never_unlinks_fresh_logs(tmp_path):
    db_path = str(tmp_path / "state.db")
    db.init(db_path)
    args = SimpleNamespace(db=db_path)
    log_dir = Path(bridge._logs_dir(args))
    log_dir.mkdir(parents=True)
    now = time.time()
    # more files than WORKER_LOG_KEEP, but the newest three are fresh
    # enough to belong to a live worker and must survive retention
    old = []
    for i in range(12):
        p = log_dir / f"worker_{i}_0_{100 + i}.log"
        p.write_bytes(f"old {i}\n".encode())
        mtime = now - 2 * bridge.WORKER_LOG_MIN_AGE_SECONDS + i
        os.utime(p, (mtime, mtime))
        old.append(p)
    fresh = []
    for i in range(3):
        p = log_dir / f"worker_9{i}_0_200.log"
        p.write_bytes(f"fresh {i}\n".encode())
        fresh.append(p)  # mtime=now: inside the lease window
    bridge._sweep_worker_logs(args)

    files = sorted(log_dir.glob("worker_*.log"))
    # every fresh log survives; only stale files are trimmed to KEEP
    assert all(p.exists() for p in fresh)
    assert len(files) == bridge.WORKER_LOG_KEEP + len(fresh)
    assert not old[0].exists()
    assert not old[1].exists()
    assert all(p.exists() for p in old[2:])


# --- item_13c: managed mode - arm / disarm / managed dispatch ----------------


def test_item13c_arm_sets_managed_mode(env, capsys):
    rc, out = _run(["arm", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["status"] == SessionMode.MANAGED.value
    assert out["session"] == SID
    row = db.get_session(SID, path=env.db)
    assert row["mode"] == SessionMode.MANAGED.value


def test_item13c_disarm_clears_mode_and_notifies(env, capsys):
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    rc, out = _run(["disarm", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["status"] == SessionMode.UNMANAGED.value
    assert out["data"]["disarmed"] == [SID]
    assert db.get_session(SID, path=env.db)["mode"] == SessionMode.UNMANAGED.value
    # a DISARM outbox row tells the live watcher to exit
    rows = db.unrelayed_notifications(SID, path=env.db)
    assert len(rows) == 1
    assert rows[0]["kind"] == NotificationKind.DISARM.value
    assert rows[0]["relayed_at"] is None


def test_item13c_gwt3_disarm_all_disarms_every_managed(env, capsys):
    for sid in ("m1", "m2"):
        db.set_session_mode(sid, SessionMode.MANAGED, path=env.db)
    db.set_session_mode("w1", SessionMode.UNMANAGED, path=env.db)
    rc, out = _run(["disarm", "--all", "--json"], capsys)
    assert rc == ExitCode.OK
    assert sorted(out["data"]["disarmed"]) == ["m1", "m2"]
    for sid in ("m1", "m2"):
        row = db.get_session(sid, path=env.db)
        assert row["mode"] == SessionMode.UNMANAGED.value
        rows = db.unrelayed_notifications(sid, path=env.db)
        assert [r["kind"] for r in rows] == [NotificationKind.DISARM.value]
    # the unmanaged session got no DISARM notice
    assert db.unrelayed_notifications("w1", path=env.db) == []


def test_item13c_disarm_all_empty_fleet(env, capsys):
    rc, out = _run(["disarm", "--all", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["disarmed"] == []


def test_item13c_disarm_without_ref_is_usage_error(env, capsys):
    rc, out = _run(["disarm", "--json"], capsys)
    assert rc == ExitCode.USAGE
    assert out["error"]


def test_item13c_push_managed_goes_to_outbox(env, capsys):
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    rc, out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    assert rc == ExitCode.OK
    tid = out["data"]["task_id"]
    row = db.get_task(tid, path=env.db)
    # the task stays queued as the durable record; no transport runs
    assert row["status"] == TaskStatus.QUEUED.value
    assert row["transport"] is None
    assert row["lease_owner"] is None
    assert env.spawned == []
    assert not _trigger(env).exists()  # no unmanaged queue trigger
    # the durable delivery is a TASK_DISPATCH outbox row (the daemon
    # relay turns it into the fs mailbox file)
    rows = db.unrelayed_notifications(SID, path=env.db)
    assert len(rows) == 1
    assert rows[0]["kind"] == NotificationKind.TASK_DISPATCH.value
    assert rows[0]["task_id"] == tid
    assert "do it" in rows[0]["body"]
    assert "do not retry" in out["agent_instruction"]


def test_item13c_managed_dispatch_is_deduped(env, capsys):
    # repeated pump passes must not spam the mailbox while the task
    # still has an open (unacked) dispatch notification
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    _run(["push", SID, "--task", "do it", "--json"], capsys)
    _run(["status", "--json"], capsys)
    _run(["status", "--json"], capsys)
    rows = db.unrelayed_notifications(SID, path=env.db)
    assert len(rows) == 1


def test_item13c_managed_session_never_takes_acp_or_hook(env, capsys):
    # even a locked/known managed session bypasses both transports
    env.locks[SID] = (True, 1234)
    env.devin[SID] = {"working_directory": "/repo"}
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    rc, out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(out["data"]["task_id"], path=env.db)
    assert row["status"] == TaskStatus.QUEUED.value
    assert row["transport"] is None
    assert env.spawned == []
    assert not _trigger(env).exists()


def test_item13c_unmanaged_push_unchanged(env, capsys):
    # an unmanaged session still gets the hook transport + trigger
    env.locks[SID] = (True, 1234)
    db.set_session_mode(SID, SessionMode.UNMANAGED, path=env.db)
    rc, out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(out["data"]["task_id"], path=env.db)
    assert row["transport"] == Transport.HOOK_QUEUE.value
    assert _trigger(env).exists()
    assert db.unrelayed_notifications(SID, path=env.db) == []


# --- item_13e1: register / ask / ack / escalate -------------------------------


def test_item13e1_gwt1_register_orchestrator_cas(env, capsys):
    rc, out = _run(["register", "orc1", "--role", "orchestrator", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["status"] == "registered"
    assert out["data"]["role"] == "orchestrator"
    assert out["data"]["generation"] == 1
    # a second orchestrator is refused while the holder's heartbeat is fresh
    rc, out = _run(["register", "orc2", "--role", "orchestrator", "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert out["data"]["holder"] == "orc1"
    assert out["data"]["heartbeat_age"] is not None
    assert "--force" in out["agent_instruction"]
    # --force takes over and demotes the previous holder
    rc, out = _run(["register", "orc2", "--role", "orchestrator", "--force", "--json"], capsys)
    assert rc == ExitCode.OK
    assert db.get_session("orc2", path=env.db)["role"] == "orchestrator"
    assert db.get_session("orc1", path=env.db)["role"] == "worker"


def test_item13e1_register_worker_defaults(env, capsys):
    rc, out = _run(["register", "w1", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["status"] == "registered"
    assert out["data"]["role"] == "worker"
    row = db.get_session("w1", path=env.db)
    assert row["role"] == "worker"
    assert row["cwd"] == os.getcwd()
    # re-registering bumps the heartbeat generation
    rc, out = _run(["register", "w1", "--json"], capsys)
    assert out["data"]["generation"] == 2


def test_item13e1_ask_posts_question_and_refusals(env, capsys):
    tid = _task(env)
    rc, out = _run(
        ["ask", "--session", SID, "--task", str(tid), "which branch?", "--json"],
        capsys,
    )
    assert rc == ExitCode.OK
    qid = out["data"]["question_id"]
    row = db.get_question(qid, path=env.db)
    assert row["state"] == "pending"
    assert row["text"] == "which branch?"
    assert row["session_id"] == SID
    # a second pending question on the same task is refused
    rc, out = _run(["ask", "--session", SID, "--task", str(tid), "more?", "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert out["data"]["reason"] == "pending_exists"
    # cycle the question budget: answer each, ask again
    assert db.claim_question(qid, "orc", path=env.db)
    assert db.answer_question(qid, "main", path=env.db)
    for i in (2, 3):
        rc, out = _run(
            ["ask", "--session", SID, "--task", str(tid), f"q{i}", "--json"],
            capsys,
        )
        assert rc == ExitCode.OK
        qi = out["data"]["question_id"]
        assert db.claim_question(qi, "orc", path=env.db)
        assert db.answer_question(qi, f"a{i}", path=env.db)
    # the task carries MAX_QUESTIONS_PER_TASK questions now - refused
    rc, out = _run(["ask", "--session", SID, "--task", str(tid), "q4", "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert out["data"]["reason"] == "question_limit"
    assert "escalate" in out["agent_instruction"]


def test_item13e1_gwt4_ack_watermark(env, capsys):
    db.notify(SID, NotificationKind.DIRECTIVE, "one", path=env.db)
    n2 = db.notify(SID, NotificationKind.DIRECTIVE, "two", path=env.db)
    n3 = db.notify(SID, NotificationKind.DIRECTIVE, "three", path=env.db)
    rc, out = _run(["ack", "--session", SID, "--up-to", str(n2), "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["acked"] == 2
    # rows above the watermark stay unacked
    unacked = db.unacked_notifications(SID, path=env.db)
    assert [r["id"] for r in unacked] == [n3]
    # a bare ack covers what is left; a repeat is a no-op
    rc, out = _run(["ack", "--session", SID, "--json"], capsys)
    assert out["data"]["acked"] == 1
    rc, out = _run(["ack", "--session", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["acked"] == 0
    # the watermark persisted on the rows
    rows = db.unacked_notifications(SID, path=env.db)
    assert rows == []


def _escalation(env, session_id="w1", task_id=None, question_id=None, directive=None):
    return db.audit(
        AuditEventKind.ESCALATION,
        session_id=session_id,
        task_id=task_id,
        question_id=question_id,
        question_text="which branch?",
        partner_outputs={"copilot": "A", "gemini": "B"},
        synthesis="partners disagree",
        directive=directive,
        doa_trigger="question_limit",
        path=env.db,
    )


def test_item13e1_escalate_show_lists_open_only(env, capsys):
    eid = _escalation(env)
    _escalation(env, session_id="w2", directive="A")  # already resolved
    rc, out = _run(["escalate", "show", "--json"], capsys)
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)
    items = resp.data["escalations"]
    assert [i["id"] for i in items] == [eid]
    item = items[0]
    assert item["session_id"] == "w1"
    assert item["doa_trigger"] == "question_limit"
    assert item["question_text"] == "which branch?"
    assert item["synthesis"] == "partners disagree"
    assert item["partner_outputs"] == {"copilot": "A", "gemini": "B"}
    assert item["reply_command"] == (f"bridge escalate reply {eid} A|B|defer|abort|override:<text>")


def test_item13e1_escalate_show_empty(env, capsys):
    rc, out = _run(["escalate", "show", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["escalations"] == []


def test_item13e1_gwt2_defer_pauses_task_frees_slot(env, capsys):
    tid = _task(env)
    other = _task(env, sid="s2")
    eid = _escalation(env, task_id=tid)
    rc, out = _run(["escalate", "reply", str(eid), "defer", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["directive"] == "defer"
    assert out["data"]["task_status"] == TaskStatus.PAUSED.value
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.PAUSED.value
    assert row["note"] == "blocked: deferred by human"
    # paused is not live: the session slot frees for the next task
    assert db.get_live_task(SID, path=env.db) is None
    # the next queued task on another session stays available
    assert db.get_task(other, path=env.db)["status"] == TaskStatus.QUEUED.value
    # the escalation row carries the directive
    assert db.get_audit_event(eid, path=env.db)["directive"] == "defer"
    # the calling session was notified
    rows = db.unrelayed_notifications("w1", path=env.db)
    assert len(rows) == 1
    assert rows[0]["kind"] == NotificationKind.DIRECTIVE.value


def test_item13e1_gwt3_abort_fails_task_with_cleanup_note(env, capsys):
    tid = _task(env)
    owner = _claim_acp(env, tid)  # running under a worker lease
    eid = _escalation(env, task_id=tid)
    rc, out = _run(["escalate", "reply", str(eid), "abort", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["task_status"] == TaskStatus.FAILED.value
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.FAILED.value
    assert row["note"] == "aborted by human: cleanup branch"
    assert row["lease_owner"] is None
    # a zombie worker cannot overwrite the human's verdict
    assert db.finish_task(tid, owner, ok=True, path=env.db) is False


def test_item13e1_gwt5_reply_on_resolved_refused(env, capsys):
    eid = _escalation(env)
    rc, _out = _run(["escalate", "reply", str(eid), "A", "--json"], capsys)
    assert rc == ExitCode.OK
    rc, out = _run(["escalate", "reply", str(eid), "B", "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert "already resolved" in out["error"]
    # the first directive stands
    assert db.get_audit_event(eid, path=env.db)["directive"] == "A"


def test_item13e1_reply_not_found_and_bad_directive(env, capsys):
    rc, _out = _run(["escalate", "reply", "999", "A", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND
    eid = _escalation(env)
    rc, _out = _run(["escalate", "reply", str(eid), "bogus", "--json"], capsys)
    assert rc == ExitCode.USAGE
    rc, _out = _run(["escalate", "reply", str(eid), "override:", "--json"], capsys)
    assert rc == ExitCode.USAGE
    # invalid replies leave the escalation open
    assert db.get_audit_event(eid, path=env.db)["directive"] is None


def test_item13e1_reply_override_answers_claimed_question(env, capsys):
    tid = _task(env)
    res = db.ask("w1", tid, "which branch?", path=env.db)
    qid = res.question_id
    assert db.claim_question(qid, "orc1", path=env.db)
    eid = _escalation(env, task_id=tid, question_id=qid)
    rc, out = _run(["escalate", "reply", str(eid), "override:use main", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["question_answered"] is True
    q = db.get_question(qid, path=env.db)
    assert q["state"] == "answered"
    assert q["answer"] == "use main"
    rows = db.unrelayed_notifications("w1", path=env.db)
    assert len(rows) == 1
    assert rows[0]["kind"] == NotificationKind.DIRECTIVE.value
    assert "use main" in rows[0]["body"]
    assert rows[0]["task_id"] == tid
    assert rows[0]["question_id"] == qid


def test_item13e1_reply_answers_pending_question(env, capsys):
    tid = _task(env)
    res = db.ask("w1", tid, "which branch?", path=env.db)
    eid = _escalation(env, task_id=tid, question_id=res.question_id)
    rc, out = _run(["escalate", "reply", str(eid), "A", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["question_answered"] is True
    q = db.get_question(res.question_id, path=env.db)
    assert q["state"] == "answered"
    assert q["answer"] == "A"
    assert q["claimed_by"] == bridge._ESCALATE_CLAIMANT


# --- item_13e2: bridge status fleet view --------------------------------------


def _status_human(capsys):
    """``bridge status`` human render; returns the output line list."""
    rc = bridge.main(["status"])
    assert rc == ExitCode.OK
    return capsys.readouterr().out.strip().splitlines()


def _stale_heartbeat(path, sid):
    """Age a session's heartbeat past the bounded stale window."""
    con = db.connect(path)
    try:
        con.execute(
            "UPDATE worker_heartbeats SET last_seen_at=? WHERE session_id=?",
            (int(time.time()) - db.HEARTBEAT_STALE_SECONDS - 5, sid),
        )
    finally:
        con.close()


def _age_row(path, sql, params, seconds=25 * 3600):
    """Push a row's timestamp outside the 24h digest window."""
    con = db.connect(path)
    try:
        con.execute(sql, (int(time.time()) - seconds, *params))
    finally:
        con.close()


def test_item13e2_gwt1_escalation_banner_shows_exact_reply_command(env, capsys):
    eid = _escalation(env)
    lines = _status_human(capsys)
    assert len(lines) < 25
    banner = lines[0]
    assert banner.startswith(f"ESCALATED #{eid}: question_limit")
    assert f"reply: bridge escalate reply {eid} A|B|defer|abort|override:<text>" in banner
    # --json carries the same escalation, structured
    _rc, out = _run(["status", "--json"], capsys)
    esc = [a for a in out["data"]["attention"] if a["kind"] == "escalation"]
    assert esc[0]["audit_id"] == eid
    assert esc[0]["reply_command"] == (
        f"bridge escalate reply {eid} A|B|defer|abort|override:<text>"
    )
    # resolving the escalation drops the banner
    assert db.resolve_escalation(eid, "A", path=env.db)
    lines = _status_human(capsys)
    assert not any(line.startswith("ESCALATED") for line in lines)


def test_item13e2_gwt2_healthy_fleet_compact_view(env, capsys):
    _run(
        ["register", "orc1", "--role", "orchestrator", "--workspace", "/repos/ctl", "--json"],
        capsys,
    )
    _run(["register", "w1", "--workspace", "/repos/alpha", "--json"], capsys)
    tid = _task(env, sid="w1", text="implement login")
    res = db.ask("w1", tid, "which branch?", path=env.db)
    assert res.accepted
    # daemon vitals: a held flock + a fresh heartbeat stamp
    sdir = str(Path(env.db).parent)
    lock_fd = daemon._acquire_lock(sdir)
    daemon._write_heartbeat(sdir)
    try:
        lines = _status_human(capsys)
    finally:
        os.close(lock_fd)
    assert len(lines) < 25
    text = "\n".join(lines)
    assert "daemon: running (pid " in text
    assert "orchestrator: orc1 active (hb " in text
    header = next(line for line in lines if "STATE" in line)
    for col in ("SID", "REPO", "TASK", "STATE", "Q", "FAIL", "HB"):
        assert col in header
    # orchestrator sorts first, worker row carries task + budgets
    assert lines[lines.index(header) + 1].startswith("orc1")
    w1 = next(line for line in lines if line.startswith("w1"))
    assert "alpha" in w1
    assert f"#{tid} implement login" in w1
    # a registered unmanaged session is "known": pump dispatched the
    # task to an acp worker, so the row shows running
    assert "running" in w1
    assert "1/3" in w1
    assert "0/2" in w1
    assert env.spawned == [(tid, db.get_task(tid, path=env.db)["lease_owner"])]
    assert lines[-1].startswith("24h: done ")


def test_item13e2_gwt3_empty_db_minimal_view_no_crash(env, capsys):
    lines = _status_human(capsys)
    assert len(lines) < 25
    text = "\n".join(lines)
    assert "daemon: stopped" in text
    assert "orchestrator: none" in text
    assert "(no active sessions)" in text
    assert "24h: done 0 | auto-decisions 0 | escalations 0" in text


def test_item13e2_orchestrator_dead_banner_and_hint(env, capsys):
    _run(["register", "orc1", "--role", "orchestrator", "--json"], capsys)
    # a heartbeat past the stale window flags the banner even before
    # the daemon's next sweep marks the row dead
    _stale_heartbeat(env.db, "orc1")
    lines = _status_human(capsys)
    assert len(lines) < 25
    assert (
        "ORCHESTRATOR DEAD: reopen session, bridge register --role orchestrator [--force]"
    ) in lines
    assert any(line.startswith("orchestrator: orc1") for line in lines)
    # a live heartbeat clears the banner
    db.heartbeat("orc1", path=env.db)
    lines = _status_human(capsys)
    assert not any(line.startswith("ORCHESTRATOR DEAD") for line in lines)


def test_item13e2_dead_session_compact_marker(env, capsys):
    _run(["register", "w1", "--json"], capsys)
    assert db.set_session_status("w1", SessionStatus.DEAD, path=env.db)
    lines = _status_human(capsys)
    row = next(line for line in lines if line.startswith("w1"))
    # the dead marker is the state column, not a dropped row
    assert "dead" in row.split()


def test_item13e2_digest_counts_24h_window(env, capsys):
    tid = _task(env)
    owner = _claim_acp(env, tid)
    assert db.finish_task(tid, owner, ok=True, path=env.db)
    old = _task(env, sid="s2")
    o2 = _claim_acp(env, old, owner="acp-2-2")
    assert db.finish_task(old, o2, ok=True, path=env.db)
    _age_row(env.db, "UPDATE tasks SET updated_at=? WHERE id=?", (old,))
    db.audit(AuditEventKind.DELIBERATION, session_id="w1", question_text="q1", path=env.db)
    stale_delib = db.audit(
        AuditEventKind.DELIBERATION, session_id="w1", question_text="q2", path=env.db
    )
    _age_row(env.db, "UPDATE audit_events SET created_at=? WHERE id=?", (stale_delib,))
    _escalation(env)
    lines = _status_human(capsys)
    assert lines[-1] == "24h: done 1 | auto-decisions 1 | escalations 1"


def test_item13e2_line_cap_under_fleet_and_banner_overflow(env, capsys):
    for i in range(40):
        _run(["register", f"w{i:02d}", "--json"], capsys)
    for _ in range(8):
        _escalation(env)
    lines = _status_human(capsys)
    assert len(lines) <= bridge.STATUS_MAX_LINES
    assert len(lines) < 25
    text = "\n".join(lines)
    # the banner folds past its own cap, the fleet folds into "+N more"
    assert "+3 more open escalation(s) - bridge escalate show" in text
    assert "more session(s) - see --json" in text
    # vitals, table header and the digest footer still render
    assert any(line.startswith("daemon:") for line in lines)
    assert any(line.startswith("orchestrator:") for line in lines)
    assert "STATE" in text
    assert lines[-1].startswith("24h: done ")


def test_item13e2_status_json_carries_fleet_detail(env, capsys):
    _run(["register", "orc1", "--role", "orchestrator", "--json"], capsys)
    _run(["register", "w1", "--workspace", "/tmp/alpha", "--json"], capsys)
    tid = _task(env, sid="w1")
    assert db.ask("w1", tid, "q?", path=env.db).accepted
    _escalation(env)
    rc, out = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)
    data = resp.data
    assert data["vitals"]["daemon"]["running"] is False
    assert data["vitals"]["orchestrator"]["session_id"] == "orc1"
    fleet = {r["session_id"]: r for r in data["fleet"]}
    assert fleet["orc1"]["role"] == "orchestrator"
    assert fleet["orc1"]["state"] == "idle"
    assert fleet["w1"]["registered"] is True
    assert fleet["w1"]["task_id"] == tid
    # pump dispatched the queued task to an acp worker (w1 is a known,
    # unmanaged, unlocked session)
    assert fleet["w1"]["task_status"] == TaskStatus.RUNNING.value
    assert fleet["w1"]["state"] == "running"
    assert fleet["w1"]["questions"] == 1
    assert fleet["w1"]["attempt"] == 0
    assert fleet["w1"]["heartbeat_age_seconds"] is not None
    assert data["digest_24h"] == {"done": 0, "auto_decisions": 0, "escalations": 1}

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

import pytest

from session_bridge.acp.contract import AcpError, PromptResult
from session_bridge.cli import bridge
from session_bridge.cli.contract import (
    EventKind,
    ExitCode,
    JsonResponse,
)
from session_bridge.state import db
from session_bridge.state.contract import TaskStatus, Transport

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
    monkeypatch.setattr(
        db, "is_locked", lambda sid: locks.get(sid, (False, None))
    )
    devin = {}
    monkeypatch.setattr(
        db, "sessions_db_lookup", lambda sid: devin.get(sid)
    )
    sent = []
    monkeypatch.setattr(
        bridge, "_notify_send", lambda title, body: sent.append((title, body))
    )
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

    prompts = []

    def __init__(self, *a, **kw):
        pass

    def load_session(self, sid, cwd):
        pass

    def prompt(self, sid, text, timeout=None):
        type(self).prompts.append((sid, text))
        return PromptResult(
            text="assistant tail text", stop_reason="end_turn", usage={}
        )

    def close_session(self, sid):
        pass

    def terminate(self):
        pass


class _LockedClient(_OkClient):
    """AcpClient stand-in: session/load answers session_locked."""

    def load_session(self, sid, cwd):
        raise AcpError(
            "session is locked", kind="session_locked",
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
    rc, out = _run(
        ["push", SID, "--file", "/specs/plan.md", "--json"], capsys
    )
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


def test_item07_gwt3_work_session_locked_falls_back_to_hook(
    env, capsys, monkeypatch
):
    env.devin[SID] = {"working_directory": "/repo"}
    env.locks[SID] = (True, 777)
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(bridge, "AcpClient", _LockedClient)
    rc, out = _run(
        ["_work", str(tid), "--owner", owner, "--json"], capsys
    )
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


def test_item07_gwt3_work_session_locked_from_dispatch_decided(
    env, capsys, monkeypatch
):
    # the fallback also wins while the task is still dispatch_decided
    env.devin[SID] = {"working_directory": "/repo"}
    env.locks[SID] = (True, 777)
    tid = _task(env)
    owner = "acp-9-7"
    assert db.claim_queued(tid, owner, Transport.ACP_PUSH, path=env.db)
    monkeypatch.setattr(bridge, "AcpClient", _LockedClient)
    rc, out = _run(
        ["_work", str(tid), "--owner", owner, "--json"], capsys
    )
    assert rc == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.QUEUED.value
    assert row["transport"] == Transport.HOOK_QUEUE.value
    assert _trigger(env).exists()


# --- GWT-4: worker exit pumps the next queued task (semaphore) ----------------


def test_item07_gwt4_finished_worker_dispatches_next_queued(
    env, capsys, monkeypatch
):
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
    assert db.set_awaiting_user(t1, "hook-1", note="park budget exhausted",
                               path=env.db)
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
    header = table.splitlines()[0]
    for col in ("SESSION", "REPO", "TRANSPORT", "STATE", "QUEUE", "ACTION"):
        assert col in header


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


def test_item07_gwt6_work_missing_session_fails_task(
    env, capsys, monkeypatch
):
    # _work cannot resolve a cwd: controlled not_found failure
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(bridge, "AcpClient", _OkClient)
    rc, out = _run(
        ["_work", str(tid), "--owner", owner, "--json"], capsys
    )
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
    rc, out = _run(["clear", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert db.get_task(tid, path=env.db)["status"] == \
        TaskStatus.RUNNING.value


def test_item07_gwt7_unpark_resumes_queue_and_rewrites_trigger(
    env, capsys
):
    env.locks[SID] = (True, 555)
    tid = _task(env)
    owner = "hook-1"
    assert db.claim_queued(tid, owner, Transport.HOOK_QUEUE, path=env.db)
    assert db.mark_running(tid, owner, path=env.db)
    assert db.set_awaiting_user(tid, owner, path=env.db)
    db.session_pause(SID, True, path=env.db)
    rc, out = _run(["unpark", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    # queue pause cleared and the awaiting task is back on the hook queue
    assert db.get_session(SID, path=env.db)["queue_paused"] == 0
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.QUEUED.value
    assert row["transport"] == Transport.HOOK_QUEUE.value
    assert _trigger(env).exists()
    assert env.spawned == []


def test_item07_gwt7_retry_failed_requeues_with_attempt_reset(
    env, capsys
):
    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    owner = _claim_acp(env, tid)
    assert db.finish_task(
        tid, owner, ok=False, note="boom", path=env.db
    )
    rc, out = _run(["retry", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    assert row["attempt"] == 0
    # pump ran: the unlocked session got an acp worker
    assert row["status"] == TaskStatus.RUNNING.value
    assert (tid, row["lease_owner"]) in env.spawned


def test_item07_gwt7_retry_non_failed_conflicts(env, capsys):
    tid = _task(env)
    rc, out = _run(["retry", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert db.get_task(tid, path=env.db)["status"] == \
        TaskStatus.QUEUED.value


# --- worker failure paths --------------------------------------------------------


def test_item07_work_failed_turn_finishes_failed_and_notifies(
    env, capsys, monkeypatch
):
    class RefusalClient(_OkClient):
        def prompt(self, sid, text, timeout=None):
            return PromptResult(
                text="", stop_reason="refusal", usage={}
            )

    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(bridge, "AcpClient", RefusalClient)
    rc, out = _run(
        ["_work", str(tid), "--owner", owner, "--json"], capsys
    )
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)
    assert resp.status == TaskStatus.FAILED.value
    assert any(e.kind == EventKind.FAILED for e in resp.events)
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.FAILED.value
    assert "refusal" in (row["note"] or "")
    assert env.sent


def test_item07_work_transient_acp_error_waits_for_lease_recovery(
    env, capsys, monkeypatch
):
    class FlakyClient(_OkClient):
        def load_session(self, sid, cwd):
            raise AcpError("silence", kind="timeout", retryable=True)

    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(bridge, "AcpClient", FlakyClient)
    rc, out = _run(
        ["_work", str(tid), "--owner", owner, "--json"], capsys
    )
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
    rc, out = _run(
        ["_work", str(tid), "--owner", owner, "--json"], capsys
    )
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
    rc, out = _run(["inspect", "nope", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND


# --- recovery inside pump ----------------------------------------------------------


def test_item07_pump_recovers_stale_running(env, capsys):
    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    owner = _claim_acp(env, tid)
    con = db.connect(env.db)
    try:
        con.execute(
            "UPDATE tasks SET lease_expires_at=? WHERE id=?",
            (int(time.time()) - 1, tid),
        )
    finally:
        con.close()
    rc, out = _run(["status", "--json"], capsys)
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


def test_item10_gwt1_queue_depth_true_count_pump_processes_all(
    env, capsys
):
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
    tasks = db.list_tasks(
        status=TaskStatus.QUEUED, path=env.db, limit=100
    )
    assert len(tasks) == 60
    assert all(
        t["transport"] == Transport.HOOK_QUEUE.value for t in tasks
    )
    assert len(list(Path(env.qdir).glob("*.json"))) == 60
    assert env.spawned == []


def test_item10_gwt2_concurrent_pumps_never_exceed_semaphore(env):
    for i in range(8):
        sid = f"s{i}"
        env.devin[sid] = {"working_directory": f"/r{i}"}
        _task(env, sid=sid)
    args = SimpleNamespace(
        db=env.db, queue_dir=str(env.qdir), timeout=None
    )
    max_seen = [0]

    threads = [
        threading.Thread(target=lambda: bridge.pump(args))
        for _ in range(3)
    ]
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
    assert db.count_tasks(
        status=TaskStatus.DISPATCH_DECIDED, path=env.db
    ) == 0

    # and status surfaces the saturation to the operator
    _rc, out = _run_status_json(args_db=env.db)
    assert out["data"]["saturation_reason"] == "MAX_CONCURRENT_REACHED"
    assert out["data"]["concurrency"]["active"] == bridge.SEMAPHOR


def test_item10_mark_running_lease_refusal_is_not_saturation(
    env, capsys, monkeypatch
):
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
    wrapper.write_text(
        '#!/bin/sh\necho "worker stderr marker" >&2\n'
        f'exec "{real_python}" "$@"\n'
    )
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

    logs = list(
        (tmp_path / "logs").glob(f"worker_{tid}_*_*.log")
    )
    assert len(logs) == 1
    content = logs[0].read_text()
    assert "worker stderr marker" in content
    assert logs[0].stat().st_size <= bridge.WORKER_LOG_MAX_BYTES

    # the crashed/failed task surfaces log_path + stderr_tail
    rc, out = _run_status_json(args_db=db_path)
    assert rc == ExitCode.OK
    view = next(
        v for v in out["data"]["sessions"] if v["session"] == SID
    )
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
    big.write_bytes(
        b"HEAD\n" + b"y" * (bridge.WORKER_LOG_MAX_BYTES + 64) + b"TAIL\n"
    )
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

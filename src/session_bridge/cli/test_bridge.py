"""Tests for the bridge CLI + dispatcher (item_07).

Boundaries are mocked at their contract edges: ``AcpClient`` (acp
transport), ``db.is_locked`` + ``db.sessions_db_lookup`` (devin's own
storage), ``dispatch._spawn_worker`` (subprocess boundary) and
``common._notify_send`` (notify-send). state.db itself stays real in a
tmp_path via ``SESSIONS_BRIDGE_DB``.
"""

import io
import json
import os
import re
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest

from session_bridge.acp.contract import AcpError, PromptResult
from session_bridge.cli import bridge, common, dispatch
from session_bridge.cli import status as status_mod
from session_bridge.cli.commands import ops, sessions
from session_bridge.cli.contract import (
    EventKind,
    ExitCode,
    JsonResponse,
)
from session_bridge.daemon import core as daemon
from session_bridge.mailbox import mailbox_duty_text
from session_bridge.state import db
from session_bridge.state.contract import (
    AuditEventKind,
    NotificationKind,
    SessionMode,
    SessionStatus,
    TaskStatus,
    Transport,
)
from session_bridge.state.verdict import UnregisterNotFound
from session_bridge.wake import WakeError

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
        dispatch,
        "_spawn_worker",
        lambda tid, owner, args: spawned.append((tid, owner)),
    )
    locks = {}
    monkeypatch.setattr(db, "is_locked", lambda sid: locks.get(sid, (False, None)))
    devin = {}
    monkeypatch.setattr(db, "sessions_db_lookup", lambda sid: devin.get(sid))
    sent = []
    monkeypatch.setattr(common, "_notify_send", lambda title, body: sent.append((title, body)))
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
    monkeypatch.setattr(dispatch, "AcpClient", _LockedClient)
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
    monkeypatch.setattr(dispatch, "AcpClient", _LockedClient)
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
    monkeypatch.setattr(dispatch, "AcpClient", _OkClient)
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
    for col in ("SID", "REPO", "TASK", "TSTAT", "STATE", "Q", "FAIL", "SEEN", "WAKE", "MAIL"):
        assert col in header
    # the pushed-but-never-registered session shows as a fleet row
    assert any(line.startswith("*s1") for line in table.splitlines())


# --- item_21: status surfaces wake outcome + managed activity age --------------


def test_item21_gwt5_status_shows_wake_and_activity(env, capsys):
    """[ITEM-21.GWT-5] managed row: SEEN is activity recency, WAKE shows
    the last outcome + streak; --json carries the raw fields."""
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    # stale heartbeat - the metric the OLD cell would show - but a
    # fresh task update: the session is demonstrably turning
    db.heartbeat(SID, path=env.db)
    stale = int(time.time()) - db.HEARTBEAT_STALE_SECONDS - 50
    con = db.connect(env.db)
    try:
        con.execute(
            "UPDATE worker_heartbeats SET last_seen_at=? WHERE session_id=?",
            (stale, SID),
        )
        con.commit()
    finally:
        con.close()
    tid = _task(env)
    db.record_wake_outcome(SID, "sent_unconfirmed", path=env.db)
    db.bump_wake_streak(SID, path=env.db)
    db.bump_wake_streak(SID, path=env.db)

    rc, out = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    row = next(r for r in out["data"]["fleet"] if r["session_id"] == SID)
    assert row["wake_outcome"] == "sent_unconfirmed"
    assert row["wake_streak"] == 2
    assert row["heartbeat_age_seconds"] > db.HEARTBEAT_STALE_SECONDS
    # task.updated_at is fresh, so the managed activity age is small
    assert row["activity_age_seconds"] < row["heartbeat_age_seconds"]

    rc = bridge.main(["status"])
    assert rc == ExitCode.OK
    line = next(ln for ln in capsys.readouterr().out.splitlines() if ln.startswith(SID))
    assert "uncx2" in line
    assert db.get_task(tid, path=env.db) is not None


def test_item23_gwt1_tstat_done_lifecycle_not_idle(env, capsys):
    """[ITEM-23.GWT-1] the live misread: latest task done, STATE=idle
    (session reading) but TSTAT shows done - the row disambiguates
    without inspect."""
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    tid = _task(env)
    assert _run(["claim", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    assert _run(["finish", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK

    rc = bridge.main(["status"])
    assert rc == ExitCode.OK
    line = next(ln for ln in capsys.readouterr().out.splitlines() if ln.startswith(SID))
    assert " done " in line  # TSTAT
    assert " idle " in line  # STATE still reports the idle session


def test_item23_gwt2_tstat_queued_vs_done_disambiguation(env, capsys):
    """[ITEM-23.GWT-2] TSTAT makes the lifecycle explicit on every row:
    queued stays queued in both cells, done no longer hides behind
    the STATE idle alias."""
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted
    res2 = db.register("s2", "/repo/ws2", path=env.db)
    assert res2.accepted
    _task(env)  # queued, unclaimed for SID
    # keep it queued through the status call - pump would otherwise
    # dispatch the queued task for the unmanaged session mid-view
    db.session_pause(SID, True, path=env.db)
    # s2's task lands done: enqueue keeps the insert invariants, the
    # update just jumps it to the terminal state - CLI claim/finish
    # would dispatch mid-view for the unmanaged session
    tid2 = _task(env, sid="s2")
    con = db.connect(env.db)
    try:
        con.execute("UPDATE tasks SET status='done' WHERE id=?", (tid2,))
        con.commit()
    finally:
        con.close()
    rc = bridge.main(["status"])
    assert rc == ExitCode.OK
    lines = {
        ln.split()[0]: ln
        for ln in capsys.readouterr().out.splitlines()
        if ln.startswith((SID, "s2"))
    }
    assert " queued " in lines[SID]
    assert " done " in lines["s2"]
    assert " idle " in lines["s2"]  # done still reads idle in STATE


# --- item_139: MAIL column - oldest undrained actionable mail age -------------


def test_item139_gwt7_mail_column_shows_oldest_undrained_age(env, capsys):
    """[ITEM-139.GWT-7] the visibility half of the stall pack: MAIL
    renders the age of the oldest relayed-but-unacked actionable
    notification; a drained mailbox falls back to '-'."""
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted
    nid = db.notify(SID, NotificationKind.DIRECTIVE, "steer", path=env.db)
    db.mark_relayed(nid, path=env.db)
    con = db.connect(env.db)
    try:
        con.execute(
            "UPDATE notifications SET created_at=? WHERE id=?",
            (int(time.time()) - 300, nid),
        )
        con.commit()
    finally:
        con.close()

    rc = bridge.main(["status"])
    assert rc == ExitCode.OK
    line = next(ln for ln in capsys.readouterr().out.splitlines() if ln.startswith(SID))
    assert re.search(r"\b30\ds\b", line), line

    db.ack_notifications(SID, path=env.db)
    rc = bridge.main(["status"])
    assert rc == ExitCode.OK
    line = next(ln for ln in capsys.readouterr().out.splitlines() if ln.startswith(SID))
    assert line.rstrip().endswith("-"), line


def test_item102a_gwt1_version_reports_checkout_version(env, capsys):
    """[ITEM-102A.GWT-1] `bridge version` reports the pyproject version
    of the source checkout (stale dist-info is bypassed) plus the
    source path; --version flag and --json agree."""
    rc = bridge.main(["version"])
    assert rc == ExitCode.OK
    out = capsys.readouterr().out
    assert "session-bridge" in out
    assert "(editable, src: " in out
    # the version must match the repo's pyproject, not dist-info
    pyproject = Path(bridge.__file__).resolve().parents[3] / "pyproject.toml"
    ver = next(
        re.fullmatch(r'version\s*=\s*"([^"]+)"', ln.strip()).group(1)
        for ln in pyproject.read_text().splitlines()
        if re.fullmatch(r'version\s*=\s*"([^"]+)"', ln.strip())
    )
    assert ver in out
    rc, outj = _run(["version", "--json"], capsys)
    assert rc == ExitCode.OK
    assert outj["data"]["version"].startswith(ver)


def test_item102a_gwt2_version_flag(capsys):
    """[ITEM-102A.GWT-2] `bridge --version` exits 0 without needing a
    subcommand or a state db."""
    try:
        bridge.main(["--version"])
    except SystemExit as e:
        assert e.code == 0
    out = capsys.readouterr().out
    assert "session-bridge" in out
    assert "editable" in out or "src:" in out


def test_item21_wake_cell_failed_reason_prefix():
    """failed:<reason> outcomes render compactly, not as 'faile'."""
    assert bridge._wake_cell({"wake_outcome": "failed:no_session"}) == "f:no_se"
    assert bridge._wake_cell({"wake_outcome": "sent"}) == "sent"


def test_item21_managed_orchestrator_vitals_uses_seen(env, capsys):
    """A managed orchestrator's vitals line reports activity recency,
    not the by-design-stale heartbeat."""
    res = db.register("orc1", "/repo/ctl", role="orchestrator", path=env.db)
    assert res.accepted
    db.set_session_mode("orc1", SessionMode.MANAGED, path=env.db)
    rc = bridge.main(["status"])
    assert rc == ExitCode.OK
    line = next(ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("orchestrator:"))
    assert "orc1 active (seen " in line


def test_item21_unmanaged_keeps_heartbeat_age(env, capsys):
    """Unmanaged rows still report bare heartbeat age in SEEN."""
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted  # unmanaged by default
    rc, out = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    row = next(r for r in out["data"]["fleet"] if r["session_id"] == SID)
    assert row["mode"] == SessionMode.UNMANAGED.value


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
    monkeypatch.setattr(dispatch, "AcpClient", _OkClient)
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
    monkeypatch.setattr(dispatch, "AcpClient", RefusalClient)
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
    monkeypatch.setattr(dispatch, "AcpClient", FlakyClient)
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
    monkeypatch.setattr(dispatch, "AcpClient", SlowClient)
    rc, _out = _run(["_work", str(tid), "--owner", owner, "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.FAILED.value
    assert "timeout" in row["note"]


def test_item230_work_rate_limit_parks_not_fails(env, capsys, monkeypatch):
    """GWT-230-5: a prompt AcpError carrying a provider rate-limit
    marker parks the task as waiting_quota instead of hard-failing it
    (live incident 2026-11-09: rpc_error swallowed the marker)."""

    class LimitedClient(_OkClient):
        def prompt(self, sid, text, timeout=None):
            raise AcpError(
                "session/prompt: Reached free model rate limit. "
                "Upgrade to Max for higher limits, or switch to a "
                "different model. Your limit will reset in 10 minutes.",
                kind="rpc_error",
            )

    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(dispatch, "AcpClient", LimitedClient)
    rc, out = _run(["_work", str(tid), "--owner", owner, "--json"], capsys)
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)
    assert resp.status == TaskStatus.RUNNING.value
    assert resp.data["quota"]["parked"] is True
    assert resp.data["quota"]["default_window"] is False
    row = db.get_task(tid, path=env.db)
    # parked, not failed: lifecycle + lease stay with the claimant
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["step_state"] == "waiting_quota"
    assert row["quota_source"] == "acp-prompt"
    assert row["quota_reset_at"] is not None
    assert row["lease_owner"] == owner


def test_item230_work_marker_without_reset_parks_default_window(env, capsys, monkeypatch):
    """GWT-230-6: a marker without a reset parks for the bounded
    default window - the pool is dry either way."""

    class BareLimitClient(_OkClient):
        def prompt(self, sid, text, timeout=None):
            raise AcpError(
                "session/prompt: Reached free model rate limit.",
                kind="rpc_error",
            )

    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(dispatch, "AcpClient", BareLimitClient)
    before = int(time.time())
    rc, out = _run(["_work", str(tid), "--owner", owner, "--json"], capsys)
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)
    assert resp.data["quota"]["default_window"] is True
    row = db.get_task(tid, path=env.db)
    assert row["step_state"] == "waiting_quota"
    assert before + 900 - 2 <= row["quota_reset_at"] <= before + 900 + 2


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
    proc = dispatch._spawn_worker(tid, owner, args)
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
    # no daemon runs in tests: the command relays the DISARM row
    # synchronously (issue #119) - the mailbox file tells the live
    # watcher to exit
    assert db.unrelayed_notifications(SID, path=env.db) == []
    files = list((Path(env.qdir) / SID).glob("msg_*.txt"))
    assert len(files) == 1
    assert "kind: disarm" in files[0].read_text()


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
        # daemon-down disarm relays synchronously (issue #119)
        assert db.unrelayed_notifications(sid, path=env.db) == []
        files = list((Path(env.qdir) / sid).glob("msg_*.txt"))
        assert len(files) == 1
        assert "kind: disarm" in files[0].read_text()
    # the unmanaged session got no DISARM notice
    assert db.unrelayed_notifications("w1", path=env.db) == []
    assert not (Path(env.qdir) / "w1").exists()


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
    # dispatch_decided + lease is the durable delivery record
    # (item_18): the task left the queued scan so pump passes cannot
    # re-notify it; the lease is the claim window for recover_stale
    assert row["status"] == TaskStatus.DISPATCH_DECIDED.value
    assert row["transport"] is None
    assert row["lease_owner"] == f"bridge-{SID}-{tid}"
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
    assert row["status"] == TaskStatus.DISPATCH_DECIDED.value
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
    # item_20: ask needs an active orchestrator to route to
    db.register("orc", "/repo/ctl", role="orchestrator", path=env.db)
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
    db.register("orc", "/repo/ctl", role="orchestrator", path=env.db)
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
    db.register("orc", "/repo/ctl", role="orchestrator", path=env.db)
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
    for col in ("SID", "REPO", "TASK", "TSTAT", "STATE", "Q", "FAIL", "SEEN", "WAKE", "MAIL"):
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
        "ORCHESTRATOR DEAD: reopen session, bridge register <new-sid> --role orchestrator [--force]"
    ) in lines
    assert any(line.startswith("orchestrator: orc1") for line in lines)
    # a live heartbeat clears the banner
    db.heartbeat("orc1", path=env.db)
    lines = _status_human(capsys)
    assert not any(line.startswith("ORCHESTRATOR DEAD") for line in lines)


def test_item17_managed_orchestrator_stale_hb_no_banner(env, capsys):
    """Managed orchestrator idle between turns is stale-hb by design -
    the banner mirrors the sweep's managed exemption (item_17)."""
    _run(["register", "orc1", "--role", "orchestrator", "--json"], capsys)
    _run(["arm", "orc1", "--json"], capsys)
    _stale_heartbeat(env.db, "orc1")
    lines = _status_human(capsys)
    assert not any(line.startswith("ORCHESTRATOR DEAD") for line in lines)
    # a real dead flip still shows
    assert db.set_session_status("orc1", SessionStatus.DEAD, path=env.db)
    lines = _status_human(capsys)
    assert any(line.startswith("ORCHESTRATOR DEAD") for line in lines)


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
    assert lines[-1] == "24h: done 1 | auto-decisions 1 | escalations 1 | lease-expired 0"


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
    assert data["digest_24h"] == {
        "done": 0,
        "auto_decisions": 0,
        "escalations": 1,
        "lease_expired": 0,
        "waiting_external": 0,
    }


# --- item_13g: protocol wiring gaps ------------------------------------------


def _heartbeat_seen(path, sid):
    """The raw worker_heartbeats.last_seen_at for a session."""
    con = db.connect(path)
    try:
        row = con.execute(
            "SELECT last_seen_at FROM worker_heartbeats WHERE session_id=?",
            (sid,),
        ).fetchone()
        return row["last_seen_at"] if row is not None else None
    finally:
        con.close()


def test_item13g_gwt1_heartbeat_refreshes_last_seen(env, capsys):
    _run(["register", "w1", "--json"], capsys)
    _stale_heartbeat(env.db, "w1")
    rc, out = _run(["heartbeat", "w1", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["session"] == "w1"
    # register stamped generation 1; the heartbeat bumps it, not resets it
    assert out["data"]["generation"] == 2
    assert abs(_heartbeat_seen(env.db, "w1") - int(time.time())) < 5


def test_item13g_gwt1_heartbeat_is_not_a_takeover(env, capsys):
    _run(["register", "orc1", "--role", "orchestrator", "--json"], capsys)
    rc, out = _run(["heartbeat", "w9", "--json"], capsys)
    assert rc == ExitCode.OK
    # heartbeat touches only worker_heartbeats: no sessions row, and the
    # orchestrator keeps its role
    assert db.get_session("w9", path=env.db) is None
    assert db.get_session("orc1", path=env.db)["role"] == "orchestrator"
    # the register CAS still guards the singleton: a fresh holder
    # refuses a challenger, --force takes over
    rc, out = _run(["register", "orc2", "--role", "orchestrator", "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert out["data"]["holder"] == "orc1"
    rc, out = _run(["register", "orc2", "--role", "orchestrator", "--force", "--json"], capsys)
    assert rc == ExitCode.OK


def test_item13g_gwt2_questions_claim_and_answer_binding(env, capsys):
    db.register("orc1", "/repo/ctl", role="orchestrator", path=env.db)
    tid = _task(env)
    qid = db.ask(SID, tid, "which branch?", path=env.db).question_id
    # claim binds the claimant
    rc, out = _run(["questions", "claim", str(qid), "--claimant", "orc1", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["status"] == "claimed"
    assert out["data"]["claimed_by"] == "orc1"
    # a second claim loses the CAS
    rc, out = _run(["questions", "claim", str(qid), "--claimant", "orc2", "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert "orc1" in out["error"]
    # an answer bound to the wrong claimant is refused (13e1 binding)
    rc, out = _run(
        ["questions", "answer", str(qid), "--claimant", "orc2", "use dev", "--json"],
        capsys,
    )
    assert rc == ExitCode.CONFLICT
    q = db.get_question(qid, path=env.db)
    assert q["state"] == "claimed"
    assert q["claimed_by"] == "orc1"
    assert q["answer"] is None
    # the right claimant answers; the asker gets a QUESTION_ANSWER
    # outbox row for the daemon relay -> fs mailbox path
    rc, out = _run(
        ["questions", "answer", str(qid), "--claimant", "orc1", "use main", "--json"],
        capsys,
    )
    assert rc == ExitCode.OK
    assert out["status"] == "answered"
    assert out["data"]["notified"] is True
    q = db.get_question(qid, path=env.db)
    assert q["state"] == "answered"
    assert q["answer"] == "use main"
    rows = db.unrelayed_notifications(SID, path=env.db)
    assert len(rows) == 1
    assert rows[0]["kind"] == NotificationKind.QUESTION_ANSWER.value
    assert rows[0]["question_id"] == qid
    assert rows[0]["task_id"] == tid


def test_item13g_questions_list_filters_and_pending_only(env, capsys):
    db.register("orc1", "/repo/ctl", role="orchestrator", path=env.db)
    t1 = _task(env, sid="w1")
    t2 = _task(env, sid="w2")
    q1 = db.ask("w1", t1, "q one", path=env.db).question_id
    q2 = db.ask("w2", t2, "q two", path=env.db).question_id
    rc, out = _run(["questions", "list", "--json"], capsys)
    assert rc == ExitCode.OK
    assert [i["id"] for i in out["data"]["questions"]] == [q1, q2]
    rc, out = _run(["questions", "list", "--session", "w1", "--json"], capsys)
    assert [i["id"] for i in out["data"]["questions"]] == [q1]
    rc, out = _run(["questions", "list", "--task", str(t2), "--json"], capsys)
    assert [i["id"] for i in out["data"]["questions"]] == [q2]
    # a claimed question leaves the pending queue the list reads
    assert db.claim_question(q1, "orc1", path=env.db)
    rc, out = _run(["questions", "list", "--json"], capsys)
    assert [i["id"] for i in out["data"]["questions"]] == [q2]


def test_item13g_questions_answer_pending_and_unknown(env, capsys):
    db.register("orc1", "/repo/ctl", role="orchestrator", path=env.db)
    tid = _task(env)
    qid = db.ask(SID, tid, "q?", path=env.db).question_id
    # a pending question cannot be answered - claim comes first
    rc, _out = _run(
        ["questions", "answer", str(qid), "--claimant", "orc1", "a", "--json"],
        capsys,
    )
    assert rc == ExitCode.CONFLICT
    rc, _out = _run(["questions", "claim", "999", "--claimant", "orc1", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND
    rc, _out = _run(
        ["questions", "answer", "999", "--claimant", "orc1", "a", "--json"],
        capsys,
    )
    assert rc == ExitCode.NOT_FOUND


def test_item13g_gwt3_duty_text_ack_command_runs(env, capsys):
    # the duty text instructs the exact argparse surface
    text = mailbox_duty_text(SID, queue_dir=str(env.qdir))
    assert f"bridge ack --session {SID}" in text
    assert f"bridge ack {SID}" not in text
    # and the documented command runs end-to-end
    db.notify(SID, NotificationKind.DIRECTIVE, "steer", path=env.db)
    rc, out = _run(["ack", "--session", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["acked"] == 1


def test_item13g_orchestrator_dead_banner_sid_placeholder(env, capsys):
    _run(["register", "orc1", "--role", "orchestrator", "--json"], capsys)
    _stale_heartbeat(env.db, "orc1")
    lines = _status_human(capsys)
    assert (
        "ORCHESTRATOR DEAD: reopen session, bridge register <new-sid> --role orchestrator [--force]"
    ) in lines
    _rc, out = _run(["status", "--json"], capsys)
    dead = [a for a in out["data"]["attention"] if a["kind"] == "orchestrator_dead"]
    assert dead[0]["session_id"] == "orc1"
    assert "<new-sid>" in dead[0]["hint"]
    # the hinted command is complete and runnable: the dead holder's
    # own re-register wins the CAS without --force
    rc, out = _run(["register", "orc1", "--role", "orchestrator", "--json"], capsys)
    assert rc == ExitCode.OK


def test_item13g_stale_boundary_matches_daemon(env, capsys):
    # age == HEARTBEAT_STALE_SECONDS is dead for the daemon sweep
    # (last_seen_at <= cutoff); status must agree on the same boundary
    _run(["register", "orc1", "--role", "orchestrator", "--json"], capsys)
    con = db.connect(env.db)
    try:
        con.execute(
            "UPDATE worker_heartbeats SET last_seen_at=? WHERE session_id=?",
            (int(time.time()) - db.HEARTBEAT_STALE_SECONDS, "orc1"),
        )
    finally:
        con.close()
    lines = _status_human(capsys)
    assert any(line.startswith("ORCHESTRATOR DEAD") for line in lines)


# --- item_14: bridge schedule ----------------------------------------------------


def _schedules(env):
    return db.list_schedules(path=env.db)


def test_item14_schedule_add_every(env, capsys):
    before = int(time.time())
    rc, out = _run(
        ["schedule", SID, "--task", "nightly sweep", "--every", "60", "--json"],
        capsys,
    )
    assert rc == ExitCode.OK
    assert out["status"] == "scheduled"
    assert out["data"]["every_seconds"] == 60
    assert out["data"]["catchup"] is False
    assert before + 60 <= out["data"]["next_run_at"] <= int(time.time()) + 60
    rows = _schedules(env)
    assert len(rows) == 1
    assert rows[0]["id"] == out["data"]["schedule_id"]
    assert rows[0]["session_id"] == SID
    assert rows[0]["task_text"] == "nightly sweep"
    assert rows[0]["enabled"] == 1
    assert rows[0]["catchup"] == 0


def test_item14_schedule_add_every_unit_suffixes(env, capsys):
    cases = {"300": 300, "30m": 1800, "1h": 3600, "2d": 172800, "45s": 45}
    for spec, seconds in cases.items():
        rc, out = _run(
            ["schedule", SID, "--task", "t", "--every", spec, "--json"],
            capsys,
        )
        assert rc == ExitCode.OK, spec
        assert out["data"]["every_seconds"] == seconds, spec


def test_item14_schedule_add_every_rejects_bad_specs(env, capsys):
    for spec in ("abc", "1x", "0", "-5", "1.5h", "h"):
        rc, _out = _run(
            ["schedule", SID, "--task", "t", "--every", spec, "--json"],
            capsys,
        )
        assert rc == ExitCode.USAGE, spec
    assert _schedules(env) == []


def test_item14_gwt5_schedule_add_at_sets_daily_grid(env, capsys):
    # a time comfortably ahead today: the grid anchors on it
    ahead = time.localtime(time.time() + 2 * 3600)
    spec = f"{ahead.tm_hour:02d}:{ahead.tm_min:02d}"
    before = int(time.time())
    rc, out = _run(
        ["schedule", SID, "--task", "standup", "--at", spec, "--json"],
        capsys,
    )
    assert rc == ExitCode.OK
    assert out["data"]["every_seconds"] == 86400
    nxt = out["data"]["next_run_at"]
    assert before < nxt <= before + 86400
    tm = time.localtime(nxt)
    assert (tm.tm_hour, tm.tm_min) == (ahead.tm_hour, ahead.tm_min)


def test_item14_gwt5_schedule_add_at_past_time_wraps_to_tomorrow(env, capsys):
    # a time already behind today: the nearest occurrence is tomorrow
    behind = time.localtime(time.time() - 2 * 3600)
    spec = f"{behind.tm_hour:02d}:{behind.tm_min:02d}"
    before = int(time.time())
    rc, out = _run(
        ["schedule", SID, "--task", "standup", "--at", spec, "--json"],
        capsys,
    )
    assert rc == ExitCode.OK
    nxt = out["data"]["next_run_at"]
    assert out["data"]["every_seconds"] == 86400
    assert nxt > before
    tm = time.localtime(nxt)
    assert (tm.tm_hour, tm.tm_min) == (behind.tm_hour, behind.tm_min)
    # today's slot (~2h ago) was skipped: the next occurrence is ~22h
    # out, never the missed one (tolerance for minute truncation)
    assert nxt - before > 86400 - 2 * 3600 - 120


def test_item14_schedule_add_at_rejects_bad_specs(env, capsys):
    for spec in ("25:00", "12:60", "noon", "9", "9:5x"):
        rc, _out = _run(
            ["schedule", SID, "--task", "t", "--at", spec, "--json"],
            capsys,
        )
        assert rc == ExitCode.USAGE, spec
    assert _schedules(env) == []


def test_item14_schedule_add_requires_task_and_interval(env, capsys):
    rc, _out = _run(["schedule", SID, "--every", "60", "--json"], capsys)
    assert rc == ExitCode.USAGE
    rc, _out = _run(["schedule", SID, "--task", "t", "--json"], capsys)
    assert rc == ExitCode.USAGE
    assert _schedules(env) == []


def test_item14_schedule_add_catchup_flag(env, capsys):
    rc, out = _run(
        ["schedule", SID, "--task", "t", "--every", "60", "--catchup", "--json"],
        capsys,
    )
    assert rc == ExitCode.OK
    assert out["data"]["catchup"] is True
    assert _schedules(env)[0]["catchup"] == 1


def test_item14_schedule_list_and_session_filter(env, capsys):
    _run(["schedule", "s-a", "--task", "ta", "--every", "60", "--json"], capsys)
    _run(["schedule", "s-b", "--task", "tb", "--every", "120", "--json"], capsys)
    _run(["schedule", "s-a", "--task", "tc", "--at", "09:30", "--json"], capsys)
    rc, out = _run(["schedule", "list", "--json"], capsys)
    assert rc == ExitCode.OK
    items = out["data"]["schedules"]
    assert [i["session_id"] for i in items] == ["s-a", "s-b", "s-a"]
    assert items[2]["every_seconds"] == 86400
    rc, out = _run(["schedule", "list", "--session", "s-a", "--json"], capsys)
    assert [i["task_text"] for i in out["data"]["schedules"]] == ["ta", "tc"]
    # human render smoke: one line per schedule
    assert bridge.main(["schedule", "list"]) == ExitCode.OK
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 3
    assert all(line.startswith("schedule ") for line in lines)
    rc, out = _run(["schedule", "list", "--session", "ghost", "--json"], capsys)
    assert out["data"]["schedules"] == []


def test_item14_schedule_clear_disables_and_keeps_row(env, capsys):
    rc, out = _run(
        ["schedule", SID, "--task", "t", "--every", "60", "--json"],
        capsys,
    )
    sched_id = out["data"]["schedule_id"]
    rc, out = _run(["schedule", "clear", str(sched_id), "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["status"] == "disabled"
    row = _schedules(env)[0]
    assert row["enabled"] == 0
    # the row is kept (history), the schedule never fires again
    assert len(_schedules(env)) == 1
    # a repeated clear conflicts; an unknown id is not_found
    rc, out = _run(["schedule", "clear", str(sched_id), "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    rc, out = _run(["schedule", "clear", "9999", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND
    # clear without an id is a usage error
    rc, _out = _run(["schedule", "clear", "--json"], capsys)
    assert rc == ExitCode.USAGE


def test_item14_schedule_cli_created_schedule_fires(env, capsys):
    # a schedule created through the CLI materializes on a daemon pass
    rc, _out = _run(
        ["schedule", SID, "--task", "cli tick", "--every", "1", "--json"],
        capsys,
    )
    assert rc == ExitCode.OK
    time.sleep(1.1)
    assert daemon.run(env.db, str(env.qdir), once=True) == daemon.RUN_OK
    task = db.get_live_task(SID, path=env.db)
    assert task is not None
    assert task["task_text"] == "cli tick"


# --- item_18: managed task lifecycle verbs -------------------------------------


def test_item18_gwt1_claim_queued_managed_task(env, capsys):
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    """[ITEM-18.GWT-1] queued managed task -> running + lease."""
    tid = _task(env)
    rc, out = _run(["claim", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["status"] == "running"
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["lease_owner"] == f"bridge-{SID}-{tid}"
    assert row["lease_expires_at"] is not None


def test_item18_gwt2_second_claim_refused(env, capsys):
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    """[ITEM-18.GWT-2] a queued task claimed elsewhere refuses."""
    tid = _task(env)
    rc, _ = _run(["claim", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    # claim from another owner path -> running, not queued -> conflict
    con = db.connect(env.db)
    try:
        con.execute(
            "UPDATE tasks SET status=?, lease_owner=? WHERE id=?",
            (TaskStatus.RUNNING.value, "acp-other-1", tid),
        )
        con.commit()
    finally:
        con.close()
    rc, out = _run(["claim", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert out["status"] == TaskStatus.RUNNING.value


def test_item18_claim_renews_own_lease(env, capsys):
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    """Repeat claim on own running task = lease renew, not a conflict."""
    tid = _task(env)
    assert _run(["claim", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    con = db.connect(env.db)
    try:
        con.execute("UPDATE tasks SET lease_expires_at=? WHERE id=?", (1, tid))  # about to expire
        con.commit()
    finally:
        con.close()
    rc, out = _run(["claim", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["renewed"] is True
    assert db.get_task(tid, path=env.db)["lease_expires_at"] > 1


def test_item18_gwt3_finish_ok_and_fail(env, capsys):
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    """[ITEM-18.GWT-3] finish closes running -> done|failed."""
    t1 = _task(env)
    assert _run(["claim", str(t1), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    rc, out = _run(["finish", str(t1), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["status"] == "done"
    assert db.get_task(t1, path=env.db)["status"] == TaskStatus.DONE.value
    t2 = _task(env)
    assert _run(["claim", str(t2), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    rc, out = _run(
        ["finish", str(t2), "--session", SID, "--fail", "--note", "boom", "--json"],
        capsys,
    )
    assert rc == ExitCode.OK
    row = db.get_task(t2, path=env.db)
    assert row["status"] == TaskStatus.FAILED.value
    assert row["note"] == "boom"


def test_item18_gwt4_claim_terminal_refused(env, capsys):
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    """[ITEM-18.GWT-4] done/failed tasks are not claimable."""
    tid = _task(env)
    assert _run(["claim", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    assert _run(["finish", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    rc, out = _run(["claim", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert out["status"] == TaskStatus.DONE.value


def test_item18_wrong_session_and_missing(env, capsys):
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    tid = _task(env)
    rc, _ = _run(["claim", str(tid), "--session", "other-sid", "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    rc, _ = _run(["claim", "9999", "--session", SID, "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND
    rc, _ = _run(["finish", "9999", "--session", SID, "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND


def test_item18_finish_wrong_owner_refused(env, capsys):
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    """finish needs the claiming session's owner - foreign owner loses."""
    tid = _task(env)
    _claim_acp(env, tid, owner="acp-9-1")
    rc, _ = _run(["finish", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert db.get_task(tid, path=env.db)["status"] == TaskStatus.RUNNING.value


def test_item18_queue_paused_claim_refused(env, capsys):
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    tid = _task(env)
    db.session_pause(SID, True, path=env.db)
    rc, _ = _run(["claim", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT


def test_item18_gwt5_duty_text_instructs_claim_finish():
    """[ITEM-18.GWT-5] the managed mailbox duty teaches the lifecycle."""
    from session_bridge.mailbox.core import mailbox_duty_text

    text = mailbox_duty_text("s1")
    assert "bridge claim <task_id> --session" in text
    assert "bridge finish <task_id> --session" in text


# --- item_22: bridge wait (external-wait lease signal) ------------------------


def test_item22_gwt1_wait_renews_own_lease_with_reason(env, capsys):
    """[ITEM-22.GWT-1] wait on own running task extends the lease and
    records the reason in the task note."""
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    tid = _task(env)
    assert _run(["claim", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    con = db.connect(env.db)
    try:
        con.execute("UPDATE tasks SET lease_expires_at=? WHERE id=?", (1, tid))
        con.commit()
    finally:
        con.close()
    rc, out = _run(
        ["wait", str(tid), "--session", SID, "--reason", "PR review", "--json"],
        capsys,
    )
    assert rc == ExitCode.OK
    assert out["status"] == "running"
    row = db.get_task(tid, path=env.db)
    assert row["lease_expires_at"] > 1
    assert row["note"] == "PR review"
    assert row["wait_reason"] == "PR review"
    assert row["lease_renewed_at"] is not None


def test_item22_gwt2_wait_protects_task_from_recover_stale(env, capsys):
    """[ITEM-22.GWT-2] an about-to-expire task whose claimer waits
    survives recover_stale untouched (the live casualty shape:
    long review rounds burning the lease)."""
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    tid = _task(env)
    assert _run(["claim", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    con = db.connect(env.db)
    try:
        con.execute("UPDATE tasks SET lease_expires_at=? WHERE id=?", (1, tid))
        con.commit()
    finally:
        con.close()
    assert _run(["wait", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    assert db.recover_stale(path=env.db) == []
    assert db.get_task(tid, path=env.db)["status"] == TaskStatus.RUNNING.value


def test_item22_gwt3_wait_refusals(env, capsys):
    """[ITEM-22.GWT-3] wait refuses: foreign session, unmanaged claimer,
    foreign lease owner, terminal task."""
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    tid = _task(env)
    rc, _ = _run(["wait", str(tid), "--session", "other-sid", "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    rc, _ = _run(["wait", "9999", "--session", SID, "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND
    # own task but the session is not managed -> the mode gate fires
    db.set_session_mode(SID, SessionMode.UNMANAGED, path=env.db)
    rc, out = _run(["wait", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert "not managed" in out["error"]
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)

    assert _run(["claim", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    # foreign owner holds the lease now
    con = db.connect(env.db)
    try:
        con.execute("UPDATE tasks SET lease_owner=? WHERE id=?", ("acp-9-1", tid))
        con.commit()
    finally:
        con.close()
    rc, out = _run(["wait", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert out["status"] == TaskStatus.RUNNING.value
    # terminal task refuses
    con = db.connect(env.db)
    try:
        con.execute(
            "UPDATE tasks SET status=?, lease_owner=? WHERE id=?",
            (TaskStatus.RUNNING.value, f"bridge-{SID}-{tid}", tid),
        )
        con.commit()
    finally:
        con.close()
    assert _run(["finish", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    rc, _ = _run(["wait", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT


def test_item22_gwt4_duty_text_mandates_wait():
    """[ITEM-22.GWT-4] the managed duty teaches the external-wait call."""
    from session_bridge.mailbox.core import mailbox_duty_text

    text = mailbox_duty_text("s1")
    assert "bridge wait <task_id> --session" in text
    assert "EXTERNAL WAITS" in text


# --- issue #151: external-wait visibility + status observability MVP ----------


def _wait_stamped_running(env, capsys, reason="PR review"):
    """A managed session claiming a task, then wait-stamping it."""
    _managed(env)
    tid = _task(env)
    assert _run(["claim", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    argv = ["wait", str(tid), "--session", SID, "--json"]
    if reason is not None:
        argv = ["wait", str(tid), "--session", SID, "--reason", reason, "--json"]
    assert _run(argv, capsys)[0] == ExitCode.OK
    return tid


def test_issue151_gwt1_wait_stamp_renders_parked_phase(env, capsys):
    """[ISSUE-151.GWT-1] a running task with a fresh wait stamp reads
    parked with its declared reason - per-session view, fleet cells
    and --json fields all expose the phase."""
    tid = _wait_stamped_running(env, capsys)
    # the declared reason lives in the dedicated column (issue #151)
    assert db.get_task(tid, path=env.db)["wait_reason"] == "PR review"

    rc, out = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    view = next(v for v in out["data"]["sessions"] if v["session"] == SID)
    assert view["state"] == status_mod.STATE_PARKED
    assert view["action"] == "external wait: PR review"
    assert view["wait_until"] is not None
    assert view["wait_reason"] == "PR review"
    row = next(r for r in out["data"]["fleet"] if r["session_id"] == SID)
    assert row["task_id"] == tid
    assert row["state"] == "waiting"
    assert status_mod._task_state_cell(row) == "ewait"
    assert row["wait_until"] is not None
    assert row["wait_reason"] == "PR review"

    rc = bridge.main(["status"])
    assert rc == ExitCode.OK
    line = next(ln for ln in capsys.readouterr().out.splitlines() if ln.startswith(SID))
    assert " ewait " in line
    assert " waiting " in line


def test_issue151_gwt2_wait_phase_decay_and_empty_reason(env, capsys):
    """[ISSUE-151.GWT-2] no reason falls back to the bare 'external
    wait' action, and a stamp already past now decays back to plain
    running with no clearing write."""
    tid = _wait_stamped_running(env, capsys, reason=None)
    assert db.get_task(tid, path=env.db)["wait_reason"] is None
    rc, out = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    view = next(v for v in out["data"]["sessions"] if v["session"] == SID)
    assert view["action"] == "external wait"

    # let the stamp lapse - the phase decays on read, nothing clears it
    con = db.connect(env.db)
    try:
        con.execute("UPDATE tasks SET wait_until=? WHERE id=?", (1, tid))
        con.commit()
    finally:
        con.close()
    rc, out = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    view = next(v for v in out["data"]["sessions"] if v["session"] == SID)
    assert view["state"] == status_mod.STATE_RUNNING
    assert view["action"] == "-"
    row = next(r for r in out["data"]["fleet"] if r["session_id"] == SID)
    assert row["state"] == "running"
    assert status_mod._task_state_cell(row) == "run"
    # the stale stamp is still carried honestly for --json consumers
    assert row["wait_until"] == 1
    assert row["wait_reason"] is None  # this wait was declared without a reason


def test_issue151_gwt2b_reasonless_wait_never_renders_stale_note(env, capsys):
    """[ISSUE-151.GWT-2b] the reported bleed: a lease-expired fold
    leaves note='lease expired' on the task; reclaim + a reason-less
    `bridge wait` must render the bare 'external wait' action - the
    stale note can never pose as the declared reason."""
    _managed(env)
    tid = _task(env)
    assert _run(["claim", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    con = db.connect(env.db)
    try:
        con.execute("UPDATE tasks SET lease_expires_at=? WHERE id=?", (1, tid))
        con.commit()
    finally:
        con.close()
    assert db.requeue_or_fail(tid, note="lease expired", path=env.db) == TaskStatus.QUEUED
    assert db.get_task(tid, path=env.db)["note"] == "lease expired"
    # reclaim through the managed path, then wait without a reason
    assert _run(["claim", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    assert _run(["wait", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    # lifecycle history keeps the note; the declared reason is cleared
    # (a reason-less wait declares no reason - it does not inherit one)
    assert row["note"] == "lease expired"
    assert row["wait_reason"] is None

    rc, out = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    view = next(v for v in out["data"]["sessions"] if v["session"] == SID)
    assert view["state"] == status_mod.STATE_PARKED
    assert view["action"] == "external wait"  # NOT 'external wait: lease expired'
    assert view["wait_reason"] is None


def test_issue151_gwt3_lease_expired_digest_counts_window(env, capsys):
    """[ISSUE-151.GWT-3] lease_expired counts requeue_or_fail folds
    inside the 24h window via the dedicated lease_expired_at stamp -
    a sticky 'lease expired' note on a fresh row (the old overcount
    case) and a stamp aged past the window both stay out, while a
    note overwrite after the fold can no longer lose the count."""
    t1 = _task(env)
    o1 = _claim_acp(env, t1)
    con = db.connect(env.db)
    try:
        con.execute("UPDATE tasks SET lease_expires_at=? WHERE id=?", (1, t1))
        con.commit()
    finally:
        con.close()
    assert db.requeue_or_fail(t1, note="lease expired", path=env.db) is not None
    assert db.get_task(t1, path=env.db)["lease_expired_at"] is not None
    # noise: the old proxy shape - a sticky 'lease expired' note on a
    # freshly-written row must NOT count, and neither does a real fold
    # stamp aged past the window; the fold itself survives a later
    # note overwrite (the old undercount case)
    t2 = _task(env, sid="s2")
    t3 = _task(env, sid="s3")
    stale = int(time.time()) - 25 * 3600
    con = db.connect(env.db)
    try:
        con.execute("UPDATE tasks SET note=? WHERE id=?", ("lease expired", t2))
        con.execute(
            "UPDATE tasks SET lease_expired_at=?, note=? WHERE id=?",
            (stale, "lease expired", t3),
        )
        con.execute("UPDATE tasks SET note=? WHERE id=?", ("requeued elsewhere", t1))
        con.commit()
    finally:
        con.close()

    rc, out = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["digest_24h"]["lease_expired"] == 1
    assert o1  # lease owner was real, the fold was a genuine CAS


def test_issue151_gwt4_status_json_carries_wait_fields(env, capsys):
    """[ISSUE-151.GWT-4] --json exposes the parked-waiter gauge at top
    level and inside digest_24h; the human footer shows it only
    when >0."""
    _wait_stamped_running(env, capsys)
    rc, out = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["digest_24h"]["waiting_external"] == 1
    assert out["data"]["waiting_external"] == 1

    rc = bridge.main(["status"])
    assert rc == ExitCode.OK
    table = capsys.readouterr().out
    assert "waiting-external 1" in table
    assert "lease-expired 0" in table


def test_item18_no_redispatch_after_ack(env, capsys):
    """[ITEM-18.regression] the reported storm: acked dispatch +
    unclaimed task must NOT re-notify - the task left queued."""
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    _, out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    tid = out["data"]["task_id"]
    db.ack_notifications(SID, path=env.db)
    for _ in range(3):
        _run(["status", "--json"], capsys)
    rows = db.unrelayed_notifications(SID, path=env.db)
    assert len(rows) == 1  # the original dispatch, no storm
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.DISPATCH_DECIDED.value


def test_item18_claim_after_pump_dispatch(env, capsys):
    """[ITEM-18.e2e] pump dispatch -> drain -> claim -> finish."""
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    _, out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    tid = out["data"]["task_id"]
    assert db.get_task(tid, path=env.db)["status"] == TaskStatus.DISPATCH_DECIDED.value
    rc, _ = _run(["claim", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["lease_owner"] == f"bridge-{SID}-{tid}"
    rc, _ = _run(["finish", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert db.get_task(tid, path=env.db)["status"] == TaskStatus.DONE.value


def test_item18_unclaimed_dispatch_recovers_to_queued(env, capsys):
    """acked-but-never-claimed dispatch lapses -> recover_stale
    requeues -> the next pump pass re-notifies exactly once."""
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    _, out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    tid = out["data"]["task_id"]
    db.ack_notifications(SID, path=env.db)
    con = db.connect(env.db)
    try:
        con.execute("UPDATE tasks SET lease_expires_at=1 WHERE id=?", (tid,))
        con.commit()
    finally:
        con.close()
    # the same pump pass recovers the lapsed dispatch lease back to
    # queued, then re-dispatches it - bounded to one new notification
    for _ in range(3):
        _run(["status", "--json"], capsys)
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.DISPATCH_DECIDED.value
    assert row["attempt"] == 1
    rows = db.unrelayed_notifications(SID, path=env.db)
    assert len(rows) == 2  # original + exactly one re-dispatch


def test_item18_unmanaged_session_claim_refused(env, capsys):
    """managed verbs refuse unmanaged sessions - transports own them."""
    db.set_session_mode(SID, SessionMode.UNMANAGED, path=env.db)
    tid = _task(env)
    rc, out = _run(["claim", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert "not managed" in out["error"]
    rc, out = _run(["finish", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert "not managed" in out["error"]
    assert db.get_task(tid, path=env.db)["status"] == TaskStatus.QUEUED.value


def test_item18_paused_session_dispatch_skipped(env, capsys):
    """a queue_paused managed session gets no dispatch mark nor mail
    (review: a paused task must not burn attempts re-dispatching)."""
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    db.session_pause(SID, True, path=env.db)
    tid = _task(env)
    _run(["status", "--json"], capsys)
    row = db.get_task(tid, path=env.db)
    assert row["status"] == TaskStatus.QUEUED.value
    assert row["lease_owner"] is None
    assert db.unrelayed_notifications(SID, path=env.db) == []


def test_item18_renew_cas_lost_reports_conflict(env, capsys, monkeypatch):
    """a running task whose renew CAS loses is never reported as
    renewed - the worker must not trust a lease it does not own."""
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    tid = _task(env)
    assert _run(["claim", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
    monkeypatch.setattr(db, "renew_lease", lambda *a, **k: False)
    rc, out = _run(["claim", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert "renew" in out["error"]


def test_item19_hook_owner_names_session(env, capsys):
    """[ITEM-19.GWT-1] hook-fold dispatch folds to queued cleanly;
    the transient pump owner names the session (asserted via the
    db-level CAS since fallback clears the lease)."""
    env.locks[SID] = (True, 1234)
    db.set_session_mode(SID, SessionMode.UNMANAGED, path=env.db)
    _, out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    tid = out["data"]["task_id"]
    row = db.get_task(tid, path=env.db)
    # folded back: transport decided, lease released - the hook
    # worker re-claims as hook-<sid>-<pid> (worker-side test)
    assert row["status"] == TaskStatus.QUEUED.value
    assert row["transport"] == Transport.HOOK_QUEUE.value


def test_item19_acp_owner_names_session(env, capsys):
    """[ITEM-19.GWT-2] acp-push owner names the session, not just pid."""
    env.devin[SID] = {"working_directory": "/repo"}
    db.set_session_mode(SID, SessionMode.UNMANAGED, path=env.db)
    _, out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    tid = out["data"]["task_id"]
    assert env.spawned
    _tid, owner = env.spawned[0]
    assert _tid == tid
    assert owner.startswith(f"acp-{SID}-")
    assert owner.endswith(f"-{tid}")
    row = db.get_task(tid, path=env.db)
    assert row["lease_owner"] == owner


# --- item_152: push guard for unmanaged/dead sids ------------------------------


def test_item152_gwt1_push_dead_session_refused(env, capsys):
    """[ITEM-152.GWT-1] push to a dead sid is refused BEFORE enqueue:
    a dead row is undrainable by every transport, queueing would drop
    the task into a void (devin-telegram v2.2.6 incident)."""
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted
    db.set_session_status(SID, "dead", path=env.db)

    rc, out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND
    resp = JsonResponse.model_validate(out)
    assert "dead" in (resp.error or "")
    # no task row was created at all
    assert db.get_live_task(SID, path=env.db) is None


def test_item152_gwt2_push_unmanaged_warns_and_flags(env, capsys):
    """[ITEM-152.GWT-2] push to a known-but-unmanaged sid still goes
    through its transport (hook queue here), but the response warns
    loudly and the queue row is flagged for inspect."""
    env.locks[SID] = (True, 4242)
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted  # unmanaged by default

    rc, out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)
    # warning lives in data/agent_instruction - error stays the
    # failure channel (error!=null means failure by convention)
    assert resp.error is None
    assert resp.data["warning"] is not None
    assert "not a managed" in resp.data["warning"]
    assert resp.agent_instruction is not None and "warning" in resp.agent_instruction
    task = db.get_live_task(SID, path=env.db)
    assert task is not None
    assert task["note"] is not None and "not a managed" in task["note"]

    # inspect re-derives the warning live - durable even if the note
    # flag is overwritten by a later lifecycle writer
    _rc2, out2 = _run(["inspect", SID, "--json"], capsys)
    resp2 = JsonResponse.model_validate(out2)
    assert resp2.data["unmanaged_target"] is True

    # flag survives note loss: overwrite the note, inspect still warns
    db.set_task_note(task["id"], "lease expired", path=env.db)
    _rc3, out3 = _run(["inspect", SID, "--json"], capsys)
    assert JsonResponse.model_validate(out3).data["unmanaged_target"] is True


def test_item152_gwt3_push_managed_no_warning(env, capsys):
    """[ITEM-152.GWT-3] a managed+active target is the mailbox contract:
    no warning, no flag."""
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)

    _rc, out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    resp = JsonResponse.model_validate(out)
    assert resp.data["warning"] is None
    task = db.get_live_task(SID, path=env.db)
    assert task is None or task["note"] is None


def test_item152_gwt4_push_dead_unmanaged_also_refused(env, capsys):
    """[ITEM-152.GWT-4] a dead UNMANAGED row is refused too - the dead
    check applies before the mode check."""
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted
    db.set_session_status(SID, "dead", path=env.db)

    rc, _out = _run(["push", SID, "--task", "do it", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND
    assert db.get_live_task(SID, path=env.db) is None


# --- item_180: fleet halt / resume ----------------------------------------------


def _one(path, sql, args=()):
    con = db.connect(path)
    try:
        return con.execute(sql, args).fetchone()
    finally:
        con.close()


def _managed(env, sid=SID):
    res = db.register(sid, f"/repo/{sid}", path=env.db)
    assert res.accepted
    db.set_session_mode(sid, SessionMode.MANAGED, path=env.db)


def _stub_wake(env, monkeypatch, outcomes=None):
    calls = []
    monkeypatch.setattr(
        sessions,
        "wake_session",
        lambda sid, *a, **k: calls.append(sid) or (outcomes or {}).get(sid, "sent"),
    )
    return calls


def _sess(env, sid):
    return db.get_session(sid, path=env.db)


def test_item180_gwt1_halt_single_flags_notifies_wakes(env, monkeypatch, capsys):
    """[ITEM-180.GWT-1] `bridge halt <sid>` on managed+active: halted_at
    set, a registered HALT notification queued, a wake attempted and
    the row reports the post state."""
    _managed(env)
    calls = _stub_wake(env, monkeypatch)

    rc, out = _run(["halt", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    row = out["data"]["sessions"][0]
    assert row["status"] == "halted" and row["wake"] == "sent"
    assert row["halted"] is True
    assert _sess(env, SID)["halted_at"] is not None
    assert calls == [SID]
    n = _one(env.db, "SELECT kind, body FROM notifications WHERE session_id=?", (SID,))
    assert n["kind"] == "halt"
    assert "HALT" in n["body"]


def test_item180_gwt2_claim_refused_while_halted(env, monkeypatch, capsys):
    """[ITEM-180.GWT-2] the enforced half of HALT: a halted session's
    `bridge claim` on a queued task is refused as 'halted' - while a
    lease renewal on its own running task still succeeds (finishing
    the current atomic action is what HALT asks for)."""
    _managed(env)
    _stub_wake(env, monkeypatch)
    tid = _task(env)

    _run(["halt", SID, "--json"], capsys)
    rc, out = _run(["claim", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert out["status"] == "halted"
    assert db.get_task(tid, path=env.db)["status"] == TaskStatus.QUEUED.value


def test_item180_gwt2b_running_task_renews_while_halted(env, monkeypatch, capsys):
    """[ITEM-180.GWT-2b] halt mid-task: the running task's lease renew
    still passes - HALT parks new claims, not current work."""
    _managed(env)
    _stub_wake(env, monkeypatch)
    tid = _task(env)
    assert _run(["claim", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK

    _run(["halt", SID, "--json"], capsys)
    rc, out = _run(["claim", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["renewed"] is True


def test_item180_gwt3_resume_clears_and_notifies(env, monkeypatch, capsys):
    """[ITEM-180.GWT-3] `bridge resume <sid>` clears halted_at, queues
    the RESUME semantic, wakes - and claims flow again."""
    _managed(env)
    calls = _stub_wake(env, monkeypatch)
    tid = _task(env)
    _run(["halt", SID, "--json"], capsys)
    capsys.readouterr()

    rc, out = _run(["resume", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    row = out["data"]["sessions"][0]
    assert row["status"] == "resumed" and row["wake"] == "sent"
    assert _sess(env, SID)["halted_at"] is None
    assert calls == [SID, SID]
    kinds = [
        r["kind"]
        for r in db.connect(env.db)
        .execute("SELECT kind FROM notifications WHERE session_id=? ORDER BY id", (SID,))
        .fetchall()
    ]
    assert kinds == ["halt", "resume"]

    rc, out = _run(["claim", str(tid), "--session", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["status"] == "running"


def test_item180_gwt4_halt_all_broadcasts_and_snapshots(env, monkeypatch, capsys):
    """[ITEM-180.GWT-4] `bridge halt --all` hits every managed+active
    session, skips unmanaged and dead rows, and the snapshot carries
    mailbox depth + live task per halted target."""
    _managed(env)
    _managed(env, sid="s2")
    res = db.register("unman", "/repo/u", path=env.db)
    assert res.accepted  # stays unmanaged
    _managed(env, sid="deads")
    db.set_session_status("deads", "dead", path=env.db)
    calls = _stub_wake(env, monkeypatch)
    tid = _task(env)
    os.makedirs(env.qdir / SID, exist_ok=True)
    (env.qdir / SID / "msg_9.txt").write_text("notification_id: 9\nkind: directive\n\nx")

    rc, out = _run(["halt", "--all", "--json"], capsys)
    assert rc == ExitCode.OK
    rows = {r["session_id"]: r for r in out["data"]["sessions"]}
    assert set(rows) == {SID, "s2"}
    assert rows[SID]["status"] == "halted"
    assert rows[SID]["mailbox_files"] == 1
    assert rows[SID]["live_task"] == tid
    assert sorted(calls) == [SID, "s2"]
    # the excluded rows got nothing
    assert _sess(env, "unman")["halted_at"] is None
    assert _sess(env, "deads")["halted_at"] is None
    con = db.connect(env.db)
    try:
        n = con.execute(
            "SELECT COUNT(*) c FROM notifications WHERE session_id IN ('unman','deads')"
        ).fetchone()
    finally:
        con.close()
    assert n["c"] == 0


def test_item180_gwt5_halt_all_stop_daemon(env, monkeypatch, capsys):
    """[ITEM-180.GWT-5] `--stop-daemon` SIGTERMs the relay loop after
    the broadcast and reports the outcome."""
    _managed(env)
    _stub_wake(env, monkeypatch)
    seen = []
    monkeypatch.setattr(ops, "_sigterm_daemon", lambda sdir: seen.append(sdir) or "stopped")

    rc, out = _run(["halt", "--all", "--stop-daemon", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["daemon"] == "stopped"
    assert seen  # the helper ran with the state dir


def test_item180_gwt6_skip_cases(env, monkeypatch, capsys):
    """[ITEM-180.GWT-6] honest skips: unknown sid, unmanaged target,
    already-halted and not-halted resume - each reports skipped with
    a reason and writes no notification."""
    _managed(env)
    _stub_wake(env, monkeypatch)
    res = db.register("unman", "/repo/u", path=env.db)
    assert res.accepted

    _rc, out = _run(["halt", "ghost", "--json"], capsys)
    row = out["data"]["sessions"][0]
    assert row["status"] == "skipped" and "not registered" in row["reason"]

    _rc, out = _run(["halt", "unman", "--json"], capsys)
    row = out["data"]["sessions"][0]
    assert row["status"] == "skipped" and "not managed" in row["reason"]

    # resume without halt is a no-op skip
    _rc, out = _run(["resume", SID, "--json"], capsys)
    row = out["data"]["sessions"][0]
    assert row["status"] == "skipped" and "not halted" in row["reason"]

    _run(["halt", SID, "--json"], capsys)
    _rc, out = _run(["halt", SID, "--json"], capsys)
    row = out["data"]["sessions"][0]
    assert row["status"] == "skipped" and "already halted" in row["reason"]

    con = db.connect(env.db)
    try:
        n = con.execute(
            "SELECT COUNT(*) c FROM notifications WHERE session_id=?", (SID,)
        ).fetchone()
    finally:
        con.close()
    assert n["c"] == 1  # only the single successful halt


def test_item180_gwt9_dispatch_skips_halted(env, monkeypatch, capsys):
    """[ITEM-180.GWT-9] review fix (critical): a halted session's queued
    task is NOT staged dispatch_decided - staging it would burn a
    lease attempt per redispatch until MAX_ATTEMPTS failed it
    terminally; the task persists queued until resume."""
    _managed(env)
    _stub_wake(env, monkeypatch)
    tid = _task(env)
    _run(["halt", SID, "--json"], capsys)

    assert not db.dispatch_managed(tid, "bridge-pump", path=env.db)
    assert db.get_task(tid, path=env.db)["status"] == TaskStatus.QUEUED.value

    _run(["resume", SID, "--json"], capsys)
    assert db.dispatch_managed(tid, "bridge-pump", path=env.db)


def test_item180_gwt10_stop_daemon_relays_halt_files(env, monkeypatch, capsys):
    """[ITEM-180.GWT-10] review fix: --stop-daemon must not strand the
    HALT outbox rows - after the SIGTERM a synchronous relay pass
    materializes them as mailbox files."""
    _managed(env)
    _stub_wake(env, monkeypatch)
    monkeypatch.setattr(ops, "_sigterm_daemon", lambda sdir: "stopped")

    rc, out = _run(["halt", "--all", "--stop-daemon", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["relayed"] is True
    nid = out["data"]["sessions"][0]["notification_id"]
    assert (env.qdir / SID / f"msg_{nid}.txt").exists()
    # the relayed row is marked - no double-delivery on daemon start
    assert (
        _one(env.db, "SELECT relayed_at FROM notifications WHERE id=?", (nid,))["relayed_at"]
        is not None
    )


def test_item180_gwt11_target_fault_isolation(env, monkeypatch, capsys):
    """[ITEM-180.GWT-11] review fix: one failing broadcast member
    reports status=error per row and does not abort the rest of the
    fleet."""
    _managed(env)
    _managed(env, sid="s2")
    _stub_wake(env, monkeypatch)
    calls = []
    orig = db.set_session_halted

    def _flaky(sid, halted, *, path=None):
        if sid == "s2":
            raise RuntimeError("db gone")
        calls.append(sid)
        return orig(sid, halted, path=path)

    monkeypatch.setattr(db, "set_session_halted", _flaky)
    rc, out = _run(["halt", "--all", "--json"], capsys)
    assert rc == ExitCode.OK
    rows = {r["session_id"]: r for r in out["data"]["sessions"]}
    assert rows[SID]["status"] == "halted"
    assert rows["s2"]["status"] == "error"
    assert "db gone" in rows["s2"]["reason"]


def test_item180_gwt12_disarm_drops_halt_flag(env, monkeypatch, capsys):
    """[ITEM-180.GWT-12] review fix: disarm clears halted_at - leaving
    managed mode drops the managed-only halt flag, so a stray flag
    cannot refuse claims after a re-arm."""
    _managed(env)
    _stub_wake(env, monkeypatch)
    _run(["halt", SID, "--json"], capsys)
    assert _sess(env, SID)["halted_at"] is not None

    _run(["disarm", SID, "--json"], capsys)
    assert _sess(env, SID)["halted_at"] is None


# --- item_198: bridge unregister - dead/ghost registration removal ----------------


def _ghost(env, sid=SID):
    res = db.register(sid, f"/repo/{sid}", path=env.db)
    assert res.accepted
    # register stamps a heartbeat - a ghost's is stale by definition
    _one(
        env.db,
        "UPDATE worker_heartbeats SET last_seen_at=? WHERE session_id=?",
        (int(time.time()) - db.HEARTBEAT_STALE_SECONDS - 60, sid),
    )


def _mailbox(env, sid=SID):
    mdir = Path(env.qdir) / sid
    mdir.mkdir(parents=True, exist_ok=True)
    (mdir / "msg_1.txt").write_text("notification_id: 1\nkind: disarm\n\nbye")
    return mdir


def _decisions(env):
    path = Path(env.db).parent / daemon.DECISION_LOG_FILENAME
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_item198_gwt1_ghost_removed_everywhere(env, capsys):
    """[ITEM-198.GWT-1] a ghost registration - no heartbeat row, no
    tasks, stale outbox + mailbox files - is fully removed:
    sessions row, notifications and the mailbox dir all go, the
    removal is decision-logged, and the sid leaves status views."""
    _ghost(env)
    db.notify(SID, NotificationKind.DISARM, "bye", path=env.db)
    mdir = _mailbox(env)

    rc, out = _run(["unregister", SID, "--json"], capsys)

    assert rc == ExitCode.OK and out["status"] == "unregistered"
    assert out["data"]["counts"]["sessions"] == 1
    assert out["data"]["counts"]["notifications"] == 1
    assert out["data"]["mailbox_files_removed"] == 1
    assert db.get_session(SID, path=env.db) is None
    assert not mdir.exists()
    assert any(d["event"] == "session_unregistered" and d["sid"] == SID for d in _decisions(env))
    # the status fold must not resurrect the ghost
    tasks = db.list_tasks(path=env.db)
    assert all(t["session_id"] != SID for t in tasks)


def test_item198_gwt2_unknown_sid_not_found(env, capsys):
    """[ITEM-198.GWT-2] unregister on a never-registered sid is a
    clean not_found, not a silent no-op."""
    rc, out = _run(["unregister", "never-seen", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND and out["status"] == "not_found"


def test_item198_gwt3_live_task_refused(env, capsys):
    """[ITEM-198.GWT-3] a live task still owns the lifecycle -
    removal is refused, and even --force cannot kill a live task."""
    _ghost(env)
    _task(env)
    rc, out = _run(["unregister", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT and out["status"] == "conflict"
    assert db.get_session(SID, path=env.db) is not None

    rc, _ = _run(["unregister", SID, "--force", "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert db.get_session(SID, path=env.db) is not None


def test_item198_gwt4_fresh_heartbeat_refused(env, capsys):
    """[ITEM-198.GWT-4] a fresh heartbeat is a live client on the bus
    right now - refused; the refusal carries the age evidence."""
    _ghost(env)
    db.heartbeat(SID, path=env.db)
    rc, out = _run(["unregister", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT and out["status"] == "conflict"
    assert "heartbeat" in out["error"]
    assert db.get_session(SID, path=env.db) is not None


def test_item198_gwt5_host_present_refused(env, monkeypatch, capsys):
    """[ITEM-198.GWT-5] the acp-host still lists the session - a
    resumable Devin session is alive evidence: refused. A statusless
    entry (None) is listed too - 'present but unknown state' can
    never read as 'absent' (review: statusless must refuse)."""
    _ghost(env)
    monkeypatch.setattr(sessions, "find_lockfile", lambda *a, **k: {"_p": 1})
    monkeypatch.setattr(sessions, "verify_sessions", lambda sids, **k: {SID: "idle"})
    rc, out = _run(["unregister", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT and out["status"] == "conflict"
    assert db.get_session(SID, path=env.db) is not None

    monkeypatch.setattr(sessions, "verify_sessions", lambda sids, **k: {SID: None})
    rc, out = _run(["unregister", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT and out["status"] == "conflict"
    assert db.get_session(SID, path=env.db) is not None


def test_item198_gwt6_force_skips_liveness(env, monkeypatch, capsys):
    """[ITEM-198.GWT-6] --force skips the heartbeat and host legs -
    the operator override for a session the liveness signals lie
    about; the live-task refusal still applies (GWT-3)."""
    _ghost(env)
    db.heartbeat(SID, path=env.db)  # fresh - would refuse without force
    monkeypatch.setattr(sessions, "verify_sessions", lambda sids, **k: {SID: "working"})
    rc, out = _run(["unregister", SID, "--force", "--json"], capsys)
    assert rc == ExitCode.OK and out["data"]["host_check"] == "force"
    assert db.get_session(SID, path=env.db) is None


def test_item198_gwt7_verify_failure_proceeds(env, monkeypatch, capsys):
    """[ITEM-198.GWT-7] the host check is best-effort: an attach
    failure proves nothing either way, so removal proceeds and the
    output records which leg ran."""
    _ghost(env)
    monkeypatch.setattr(sessions, "find_lockfile", lambda *a, **k: {"_p": 1})

    def boom(sids, **k):
        raise bridge.WakeError("attach_failed", "nope")

    monkeypatch.setattr(sessions, "verify_sessions", boom)
    rc, out = _run(["unregister", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["host_check"] == "verify_failed:attach_failed"
    assert db.get_session(SID, path=env.db) is None


# --- item_203: unregister mailbox tombstone (audit F1) -------------------------


def test_item203_gwt1_refusal_restores_tombstoned_mailbox(env, capsys):
    """[ITEM-203.GWT-1] a refused unregister must leave the mailbox
    untouched: the dir is tombstone-renamed before the db call and
    restored on refusal - the msg files are back under <sid>."""
    _ghost(env)
    db.notify(SID, NotificationKind.DISARM, "bye", path=env.db)
    _mailbox(env)
    _task(env)  # live task refuses

    rc, _out = _run(["unregister", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    mdir = Path(env.qdir) / SID
    assert mdir.is_dir() and (mdir / "msg_1.txt").exists()
    # no tombstone left behind
    assert not list(env.qdir.glob(".unregistered-*"))


def test_item203_gwt2_recreated_mailbox_dir_survives(env, monkeypatch, capsys):
    """[ITEM-203.GWT-2] the F1 race: a same-sid writer (re-register,
    foreign relay) creates a FRESH <sid> dir during the delete. The
    pre-delete mailbox went to the tombstone and is removed; the new
    dir belongs to the post-delete world and must survive - the old
    code's late rmtree would have wiped it."""
    _ghost(env)
    db.notify(SID, NotificationKind.DISARM, "bye", path=env.db)
    _mailbox(env)

    orig = db.unregister_session

    def racing_unregister(sid, *, force=False, path=None):
        verdict = orig(sid, force=force, path=path)
        # a writer materializes a fresh mailbox mid-delete
        fresh = Path(env.qdir) / sid
        fresh.mkdir(parents=True, exist_ok=True)
        (fresh / "msg_99.txt").write_text("notification_id: 99\nkind: notice\n\nnew")
        return verdict

    monkeypatch.setattr(bridge.db, "unregister_session", racing_unregister)

    rc, out = _run(["unregister", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["mailbox_tombstoned"] is True
    assert out["data"]["mailbox_dir_recreated"] is True
    assert out["data"]["mailbox_files_removed"] == 1  # only the pre-delete mail
    mdir = Path(env.qdir) / SID
    assert (mdir / "msg_99.txt").exists()  # post-delete mail survives
    assert not (mdir / "msg_1.txt").exists()  # pre-delete mail is gone
    assert not list(env.qdir.glob(".unregistered-*"))


def test_item203_gwt3_delete_exception_restores_mailbox(env, monkeypatch, capsys):
    """[ITEM-203.GWT-3] a db.unregister_session crash must not strand
    the parked mailbox: the registration is intact (delete never
    committed), so the tombstone is restored before the error
    surfaces - review W1: restore was only wired to verdict paths."""
    _ghost(env)
    _mailbox(env)

    def boom(sid, *, force=False, path=None):
        raise RuntimeError("db exploded mid-delete")

    monkeypatch.setattr(bridge.db, "unregister_session", boom)
    rc, out = _run(["unregister", SID, "--json"], capsys)
    assert rc == ExitCode.INTERNAL and out["status"] == "internal"
    mdir = Path(env.qdir) / SID
    assert (mdir / "msg_1.txt").exists()  # mail back in place
    assert not list(env.qdir.glob(".unregistered-*"))
    assert db.get_session(SID, path=env.db) is not None  # nothing deleted


def test_item203_gwt4_concurrent_not_found_drops_tombstone(env, monkeypatch, capsys):
    """[ITEM-203.GWT-4] an UnregisterNotFound verdict means a
    concurrent unregister already deleted the row: the parked
    mailbox is the dead session's last mail - remove it, do NOT
    resurrect an orphan <sid> dir nothing drains (review W4)."""
    _ghost(env)
    _mailbox(env)
    orig = db.unregister_session

    def already_gone(sid, *, force=False, path=None):
        orig(sid, force=force, path=path)  # the concurrent winner deletes
        return UnregisterNotFound()

    monkeypatch.setattr(bridge.db, "unregister_session", already_gone)
    rc, out = _run(["unregister", SID, "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND
    assert out["data"]["mailbox_restored"] == "removed"
    assert not (Path(env.qdir) / SID).exists()  # no orphan dir
    assert not list(env.qdir.glob(".unregistered-*"))


def test_item203_gwt5_refusal_merge_never_clobbers(env, monkeypatch, capsys):
    """[ITEM-203.GWT-5] a refused unregister that finds a recreated
    <sid> dir merges the parked mail back WITHOUT clobbering live
    files: a post-refusal write owning the same name wins (review
    W3 - os.replace would silently destroy the newer file)."""
    _ghost(env)
    _mailbox(env)  # msg_1.txt parked by the tombstone
    _task(env)  # live task refuses

    orig = db.unregister_session

    def racing(sid, *, force=False, path=None):
        verdict = orig(sid, force=force, path=path)
        fresh = Path(env.qdir) / sid
        fresh.mkdir(parents=True, exist_ok=True)
        (fresh / "msg_1.txt").write_text("notification_id: 1\nkind: notice\n\nnewer")
        (fresh / "msg_7.txt").write_text("notification_id: 7\nkind: notice\n\nfresh")
        return verdict

    monkeypatch.setattr(bridge.db, "unregister_session", racing)
    rc, out = _run(["unregister", SID, "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert out["data"]["mailbox_restored"] == "merged"
    mdir = Path(env.qdir) / SID
    # the live same-name file keeps its post-refusal content
    assert (mdir / "msg_1.txt").read_text().endswith("newer")
    assert (mdir / "msg_7.txt").exists()  # post-refusal file untouched
    assert not list(env.qdir.glob(".unregistered-*"))


def test_item203_gwt6_register_reaps_orphaned_tombstone(env, capsys):
    """[ITEM-203.GWT-6] a crash between the rename and the db commit
    leaves .unregistered-<sid>-* stranded while the registration
    lives. The reaper in `bridge register` merges the parked mail
    back into the live mailbox (review W5)."""
    _ghost(env)
    mdir = _mailbox(env)
    park = Path(env.qdir) / f".unregistered-{SID}-1-1"
    os.rename(mdir, park)
    assert not mdir.exists() and (park / "msg_1.txt").exists()

    rc, _out = _run(["register", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert (mdir / "msg_1.txt").exists()  # parked mail back in the live dir
    assert not park.exists()


# --- item_207: lazy GC of dead registrations + durable event log -----------------


def _ghost_no_hb(env, sid="g1", age=None):
    """Unmanaged sessions row with NO heartbeat row - the
    register-then-vanish ghost class (impossible-vibraphone)."""
    res = db.register(sid, f"/repo/{sid}", path=env.db)
    assert res.accepted
    _one(env.db, "DELETE FROM worker_heartbeats WHERE session_id=?", (sid,))
    old = int(time.time()) - (age if age is not None else db.GHOST_TTL_SECONDS + 60)
    _one(env.db, "UPDATE sessions SET updated_at=? WHERE session_id=?", (old, sid))


def test_item207_gwt7_status_marks_ghost_dead(env, capsys):
    """[ITEM-207.GWT-7] lazy GC inside `bridge status`: an unmanaged
    ghost (no hb row, updated_at past GHOST_TTL) is marked dead by
    the status call itself - the impossible-vibraphone class dies on
    the next operator call, daemon or not."""
    _ghost_no_hb(env)
    rc, _ = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    assert db.get_session("g1", path=env.db)["status"] == SessionStatus.DEAD.value


def test_item207_gwt8_fresh_ghost_and_managed_survive(env, capsys):
    """[ITEM-207.GWT-8] the grace window protects live rows: a
    recently-touched unmanaged ghost and any MANAGED session - even
    a stale-heartbeat one (drain-mode by design) - survive the GC."""
    _ghost_no_hb(env, "g2", age=60)  # recent -> inside grace
    _ghost(env, "m1")
    _one(env.db, "UPDATE sessions SET mode='managed' WHERE session_id='m1'")
    rc, _ = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    assert db.get_session("g2", path=env.db)["status"] == SessionStatus.ACTIVE.value
    assert db.get_session("m1", path=env.db)["status"] == SessionStatus.ACTIVE.value


def test_item207_gwt9_gc_requeues_live_task_and_decides(env, capsys):
    """[ITEM-207.GWT-9] GC fires the full dead-worker effect on the
    heartbeat leg: the silent worker's live task folds back to
    queued inside the attempt budget and a worker_dead decision
    lands in the journal."""
    _ghost(env, "g1")  # stamped heartbeat gone silent, not a no-row ghost
    tid = _task(env, "g1")
    assert db.claim_queued(tid, "hook-1", Transport.HOOK_QUEUE, path=env.db)
    assert db.mark_running(tid, "hook-1", path=env.db)
    rc, _ = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    assert db.get_session("g1", path=env.db)["status"] == SessionStatus.DEAD.value
    # the requeue CAS fired (attempt+1, back to queued) - the decision
    # journal carries it; the pump's dispatch leg may re-pick the
    # queued task afterwards, which is pre-existing dispatch
    # behavior outside the GC contract
    details = " ".join(d["detail"] or "" for d in _decisions(env) if d["event"] == "worker_dead")
    assert f"task={tid} -> queued" in details
    assert db.get_task(tid, path=env.db)["attempt"] == 1


def test_item207_gwt9b_ghost_with_live_task_is_deferred(env, capsys):
    """[ITEM-207.GWT-9b] a no-heartbeat-row ghost holding a LIVE task
    is NOT demoted on the ghost leg: sessions.updated_at does not
    track task liveness, so a silently-working unmanaged worker
    past GHOST_TTL would otherwise be killed mid-task -> duplicate
    execution. Lease recovery owns the task; once nothing is live
    the next call removes the registration."""
    _ghost_no_hb(env)
    tid = _task(env, "g1")
    assert db.claim_queued(tid, "hook-1", Transport.HOOK_QUEUE, path=env.db)
    assert db.mark_running(tid, "hook-1", path=env.db)
    rc, _ = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    # deferred: still active, task untouched
    assert db.get_session("g1", path=env.db)["status"] == SessionStatus.ACTIVE.value
    assert db.get_task(tid, path=env.db)["status"] == TaskStatus.RUNNING.value
    # once the task is no longer live the same call GCs the ghost
    _one(env.db, "UPDATE tasks SET status='done' WHERE id=?", (tid,))
    rc, _ = _run(["status", "--json"], capsys)
    assert rc == ExitCode.OK
    assert db.get_session("g1", path=env.db)["status"] == SessionStatus.DEAD.value


def test_item207_gwt10_event_roundtrip_and_kind_gate(env, capsys):
    """[ITEM-207.GWT-10] `bridge event` appends a durable decision
    row; `bridge events` reads it back with session/kind filters; a
    bad kind slug is refused with usage."""
    rc, out = _run(
        ["event", "orc", "contract", "task 207 signed", "--task", "207", "--json"],
        capsys,
    )
    assert rc == ExitCode.OK and out["data"]["kind"] == "contract"
    _run(["event", "-", "plan_delta", "fleet halt lifted", "--json"], capsys)

    rc, out = _run(["events", "--session", "orc", "--json"], capsys)
    assert rc == ExitCode.OK
    items = out["data"]["events"]
    assert len(items) == 1 and items[0]["task_id"] == 207
    rc, out = _run(["events", "--kind", "plan_delta", "--json"], capsys)
    assert out["data"]["events"][0]["session"] is None
    # `--session -` filters back exactly the bus-level rows
    rc, out = _run(["events", "--session", "-", "--json"], capsys)
    assert rc == ExitCode.OK
    items = out["data"]["events"]
    assert len(items) == 1 and items[0]["kind"] == "plan_delta"
    # `--last` is floored at 1 - a negative SQLite LIMIT reads unbounded
    rc, out = _run(["events", "--last", "0", "--json"], capsys)
    assert rc == ExitCode.USAGE

    rc, out = _run(["event", "orc", "BAD KIND!", "x", "--json"], capsys)
    assert rc == ExitCode.USAGE


# --- task #208 (directive 426): host-level resource allocations ------------------


def test_item208_gwt11_reserve_conflict_names_incumbent(env, capsys):
    """[ITEM-208.GWT-11] `bridge reserve` grants a free key and the
    conflicted caller gets CONFLICT with the incumbent owner in the
    wire payload - the hard-fail signal deploy scripts gate on."""
    rc, out = _run(
        [
            "reserve",
            "tcp_port",
            "5678",
            "--project",
            "trade-ai",
            "--purpose",
            "n8n forward",
            "--json",
        ],
        capsys,
    )
    assert rc == ExitCode.OK and out["status"] == "ok"
    assert out["data"]["renewed"] is False

    rc, out = _run(
        ["reserve", "tcp_port", "5678", "--project", "rival", "--json"],
        capsys,
    )
    assert rc == ExitCode.CONFLICT and out["status"] == "conflict"
    assert out["data"]["owner"] == "trade-ai"
    assert out["data"]["purpose"] == "n8n forward"


def test_item208_gwt12_release_owner_gate_and_list(env, capsys):
    """[ITEM-208.GWT-12] `bridge release` refuses a non-owner
    (CONFLICT naming the incumbent), frees for the owner, and
    `bridge allocations` lists with type/project filters."""
    _run(["reserve", "tcp_port", "8443", "--project", "trade-ai", "--json"], capsys)
    _run(
        ["reserve", "netns_id", "trade-ai-inner", "--project", "trade-ai", "--json"],
        capsys,
    )

    rc, out = _run(["release", "tcp_port", "8443", "--project", "rival", "--json"], capsys)
    assert rc == ExitCode.CONFLICT and out["data"]["owner"] == "trade-ai"

    rc, out = _run(["allocations", "--type", "tcp_port", "--json"], capsys)
    assert rc == ExitCode.OK
    items = out["data"]["allocations"]
    assert [i["resource_id"] for i in items] == ["8443"]

    rc, out = _run(
        ["release", "tcp_port", "8443", "--project", "orc", "--force", "--json"],
        capsys,
    )
    assert rc == ExitCode.OK and out["data"]["owner"] == "trade-ai"

    rc, out = _run(["allocations", "--project", "trade-ai", "--json"], capsys)
    assert [i["resource_type"] for i in out["data"]["allocations"]] == ["netns_id"]


def test_item208_gwt13_reserve_usage_gates(env, capsys):
    """[ITEM-208.GWT-13] bad resource_type, empty project and
    non-positive --ttl are USAGE refusals, never silent writes."""
    rc, _ = _run(["reserve", "bogus", "1", "--project", "p", "--json"], capsys)
    assert rc == ExitCode.USAGE
    rc, _ = _run(
        ["reserve", "tcp_port", "1", "--project", "p", "--ttl", "0", "--json"],
        capsys,
    )
    assert rc == ExitCode.USAGE
    rc, out = _run(["allocations", "--json"], capsys)
    assert out["data"]["allocations"] == []


def test_item208_gwt14_wildcard_enforced_release_usage_and_session(env, capsys):
    """[ITEM-208.GWT-14] the {project}-* wildcard lease conflicts a
    foreign exact request at the CLI boundary; `release` refuses a
    typo'd type as USAGE; `--session` lands in the allocations row."""
    _run(
        [
            "reserve",
            "systemd_unit_prefix",
            "trade-ai-*",
            "--project",
            "trade-ai",
            "--session",
            "wonderful-aster",
            "--json",
        ],
        capsys,
    )
    rc, out = _run(
        ["reserve", "systemd_unit_prefix", "trade-ai-web", "--project", "rival", "--json"],
        capsys,
    )
    assert rc == ExitCode.CONFLICT and out["data"]["owner"] == "trade-ai"

    rc, out = _run(["allocations", "--json"], capsys)
    assert out["data"]["allocations"][0]["session"] == "wonderful-aster"

    rc, _ = _run(["release", "bogus", "x", "--project", "p", "--json"], capsys)
    assert rc == ExitCode.USAGE


def test_item210_gwt16_kill_cli_and_event(env, capsys):
    """[ITEM-210.GWT-16] `bridge kill <sid>` detaches a session's live
    state: dead row, requeued task, heartbeat gone, durable
    session_killed event readable back through `bridge events`."""
    db.register("w1", "/repo/w", path=env.db)
    db.heartbeat("w1", path=env.db)
    tid = db.enqueue("w1", "work", path=env.db)
    _claim_acp(env, tid)

    rc, out = _run(["kill", "w1", "--json"], capsys)
    assert rc == ExitCode.OK
    d = out["data"]
    assert d["killed"] and d["tasks_requeued"] == 1 and d["heartbeat_removed"]
    assert d["status_before"] == "active"
    assert db.get_session("w1", path=env.db)["status"] == "dead"
    assert db.get_task(tid, path=env.db)["status"] == "queued"

    rc, out = _run(["kill", "ghost", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND and out["status"] == "not_found"

    rc, out = _run(["events", "--session", "w1", "--kind", "session_killed", "--json"], capsys)
    assert rc == ExitCode.OK and out["data"]["events"][0]["kind"] == "session_killed"


def test_item113_gwt4_kill_task_cli(env, capsys):
    """[ITEM-113.GWT-4] `bridge kill <task_id>` (numeric positional)
    force-fails a live task: failed row, lease released, task_killed
    event, and a NOTICE outbox row for the owning session. A sid
    argument keeps the session path; non-numeric ghosts stay
    not_found; a queued task is refused."""
    db.register("s1", "/repo/s", path=env.db)
    db.heartbeat("s1", path=env.db)
    tid = _task(env)
    _claim_acp(env, tid)

    rc, out = _run(["kill", str(tid), "--json"], capsys)
    assert rc == ExitCode.OK
    d = out["data"]
    assert d["killed"] and d["task_id"] == tid and d["status_before"] == "running"
    assert d["notification_id"] is not None
    assert db.get_task(tid, path=env.db)["status"] == "failed"
    rows = db.unrelayed_notifications("s1", path=env.db)
    assert rows and rows[0]["kind"] == "notice" and str(tid) in rows[0]["body"]

    # session path untouched: a word sid still kills the session
    rc, out = _run(["kill", "ghost", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND and out["status"] == "not_found"

    # a queued task is refused with its status, nothing written
    tid2 = _task(env)
    rc, out = _run(["kill", str(tid2), "--json"], capsys)
    assert rc == ExitCode.CONFLICT and out["status"] == "refused"
    assert db.get_task(tid2, path=env.db)["status"] == "queued"

    # an already-failed task stays refused (no duplicate event)
    rc, out = _run(["kill", str(tid), "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    rc, out = _run(["events", "--session", "s1", "--kind", "task_killed", "--json"], capsys)
    assert rc == ExitCode.OK and len(out["data"]["events"]) == 1


def test_issue142_gwt10_withdraw_cli(env, capsys, monkeypatch):
    """[ISSUE-142.GWT-10] `bridge withdraw <task_id>`: the audited
    scope-pull - cancelled row, actor+reason event, owner NOTICE,
    refused on terminal, and --stop-turn routes through the acp-host
    cancel transport with an honest outcome word."""
    db.register("s1", "/repo/s", path=env.db)
    db.heartbeat("s1", path=env.db)
    tid = _task(env)

    rc, out = _run(
        [
            "withdraw",
            str(tid),
            "--actor",
            "buttered-tilapia",
            "--reason",
            "scope pulled",
            "--json",
        ],
        capsys,
    )
    assert rc == ExitCode.OK
    d = out["data"]
    assert d["withdrawn"] and d["task_id"] == tid and d["status_before"] == "queued"
    assert d["notification_id"] is not None
    assert "stop_turn" not in d  # not requested, nothing claimed
    assert db.get_task(tid, path=env.db)["status"] == "cancelled"
    ev = db.list_events(session_id="s1", kind="task_withdrawn", path=env.db)
    assert len(ev) == 1 and "actor=buttered-tilapia" in ev[0]["body"]

    # repeat is refused, not double-journaled; missing task not_found
    rc, out = _run(["withdraw", str(tid), "--json"], capsys)
    assert rc == ExitCode.CONFLICT and out["status"] == "refused"
    rc, out = _run(["withdraw", "9999", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND and out["status"] == "not_found"
    assert len(db.list_events(session_id="s1", kind="task_withdrawn", path=env.db)) == 1

    # --stop-turn on a running task: the cancel transport reports its
    # honest outcome word and the task is cancelled regardless
    tid2 = _task(env)
    _claim_acp(env, tid2)
    monkeypatch.setattr("session_bridge.cli.commands.sessions.cancel_turn", lambda sid: "confirmed")
    rc, out = _run(["withdraw", str(tid2), "--stop-turn", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["stop_turn"] == "confirmed"
    assert out["data"]["status_before"] == "running"
    assert db.get_task(tid2, path=env.db)["status"] == "cancelled"

    # a turn-stop transport failure surfaces in the report but never
    # fails the withdraw itself - the CAS already committed
    tid3 = _task(env)
    _claim_acp(env, tid3)

    def _boom(sid):
        raise WakeError("wake_host_down", "no lockfile")

    monkeypatch.setattr("session_bridge.cli.commands.sessions.cancel_turn", _boom)
    rc, out = _run(["withdraw", str(tid3), "--stop-turn", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["stop_turn"] == "failed:wake_host_down"
    assert db.get_task(tid3, path=env.db)["status"] == "cancelled"


# --- item_98 / directive 446: fleet-metrics read-only report -------


def _cycle_repo(tmp_path, rows):
    """Scratch repo dir carrying a .devin/cycle_log.jsonl."""
    repo = tmp_path / "repoA"
    (repo / ".devin").mkdir(parents=True)
    lines = "\n".join(json.dumps(r) for r in rows)
    (repo / ".devin" / "cycle_log.jsonl").write_text(lines + "\n")
    return repo


def test_item98_gwt1_fleet_metrics_report(env, capsys, tmp_path):
    """[ITEM-98.GWT-1] fleet-metrics aggregates per-session task
    counters, decision_events, questions and the repo's cycle_log
    (path from session cwd) into a markdown report - read-only."""
    repo = _cycle_repo(
        tmp_path,
        [
            {
                "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
                "item": "i_001",
                "action": "commit",
                "verdict": "ok",
            },
            {
                "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
                "item": "i_001",
                "action": "push",
                "verdict": "ok",
            },
            {
                "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
                "item": "i_002",
                "action": "verify_constraints",
                "verdict": "failure",
                "failure_class": "gate_blocked",
                "status": "closed",
                "result": "1 failure",
            },
        ],
    )
    db.register("w1", str(repo), path=env.db)
    tid = _task(env, "w1")
    _claim_acp(env, tid)
    assert db.finish_task(tid, "acp-9-1", "done", path=env.db)
    con = db.connect(env.db)
    try:
        con.execute(
            "INSERT INTO questions(task_id,session_id,text,state,answer,"
            " asked_at,answered_at) VALUES(?,?,?,?,?,?,?)",
            (tid, "w1", "q?", "answered", "a", int(time.time()) - 120, int(time.time()) - 60),
        )
        con.commit()
    finally:
        con.close()
    db.record_event("w1", "plan_delta", "delta", path=env.db)
    db.record_event("w1", "mail_stall", "reason=mail_stall", path=env.db)

    rc, out = _run(["fleet-metrics", "--window", "7d", "--json"], capsys)
    assert rc == ExitCode.OK
    d = out["data"]
    s = d["sessions"]["w1"]
    assert s["done"] == 1 and s["plan_delta"] == 1 and s["stall_episodes"] == 1
    assert s["questions"] == 1
    repo_m = d["repos"]["repoA"]
    assert repo_m["verify_failures"] == 1
    assert repo_m["rounds_max"] == 2  # i_001 got commit+push
    assert d["orchestrator"]["ask_answer_mean_s"] == 60
    assert "| w1 |" in d["report"] and "## Incident log" in d["report"]
    assert "repoA" in d["report"] and "gate_blocked" in d["report"]


def test_item98_gwt2_window_filter_and_no_cycle_log(env, capsys, tmp_path):
    """[ITEM-98.GWT-2] rows older than the window drop out; a repo
    without cycle_log yields empty aggregates, not an error."""
    repo = tmp_path / "repoB"
    repo.mkdir()
    db.register("w2", str(repo), path=env.db)
    tid = _task(env, "w2")
    # age the task beyond the 24h window
    con = db.connect(env.db)
    try:
        con.execute(
            "UPDATE tasks SET created_at=?, updated_at=? WHERE id=?",
            (int(time.time()) - 90000, int(time.time()) - 89000, tid),
        )
        con.commit()
    finally:
        con.close()
    rc, out = _run(["fleet-metrics", "--window", "24h", "--json"], capsys)
    assert rc == ExitCode.OK
    d = out["data"]
    assert d["sessions"]["w2"]["done"] == 0 or d["sessions"]["w2"]["inflight"] == 1
    assert d["repos"]["repoB"]["incidents_by_class"] == {}


def test_item98_gwt3_stall_events_collapse_to_episodes(env, capsys):
    """[ITEM-98.GWT-3] consecutive mail_stall decision events inside
    one re-alert cadence (plus poll jitter) count as ONE episode; a
    gap beyond the episode bound opens a new episode - the daemon
    re-journals every cadence inside a stall, so the raw count would
    inflate the metric for the worst stalls."""
    db.register("w1", "/tmp/none", path=env.db)
    base = int(time.time()) - 20000
    con = db.connect(env.db)
    try:
        # 0 -> +3720 -> +7320: realert cadence with poll jitter stays
        # ONE episode; +20000 (gap ~12700s) opens a second.
        for off in (0, 3720, 7320, 20000):
            con.execute(
                "INSERT INTO decision_events(session_id,kind,body,created_at) VALUES(?,?,?,?)",
                ("w1", "mail_stall", "x", base + off),
            )
        con.commit()
    finally:
        con.close()
    rc, out = _run(["fleet-metrics", "--window", "7d", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["sessions"]["w1"]["stall_episodes"] == 2


def test_item98_gwt4_cycle_ts_utc_window_boundary(env, capsys, tmp_path):
    """[ITEM-98.GWT-4] a cycle_log row whose UTC timestamp sits just
    inside the window must be counted - mktime-style host-local
    parsing would shift it out of the window by the TZ offset."""
    edge = datetime.now(timezone.utc) - timedelta(seconds=86400 - 60)
    repo = _cycle_repo(
        tmp_path,
        [
            {
                "ts": edge.isoformat(),
                "item": "i_edge",
                "action": "commit",
                "verdict": "ok",
            }
        ],
    )
    db.register("w1", str(repo), path=env.db)
    rc, out = _run(["fleet-metrics", "--window", "24h", "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["repos"][repo.name]["rounds_max"] == 1


def test_item98_gwt5_repo_basename_collision(env, capsys, tmp_path):
    """[ITEM-98.GWT-5] two repos sharing a directory basename never
    silently merge - the repo table falls back to full paths."""
    a = tmp_path / "a" / "proj"
    b = tmp_path / "b" / "proj"
    (a / ".devin").mkdir(parents=True)
    (b / ".devin").mkdir(parents=True)
    now_iso = datetime.now(timezone.utc).isoformat()
    (a / ".devin" / "cycle_log.jsonl").write_text(
        json.dumps({"ts": now_iso, "item": "i_1", "action": "commit", "verdict": "ok"}) + "\n"
    )
    db.register("w1", str(a), path=env.db)
    db.register("w2", str(b), path=env.db)
    rc, out = _run(["fleet-metrics", "--window", "7d", "--json"], capsys)
    assert rc == ExitCode.OK
    repos = out["data"]["repos"]
    assert str(a) in repos and str(b) in repos
    assert repos[str(a)]["rounds_max"] == 1 and repos[str(b)]["rounds_max"] == 0


def test_item98_gwt6_finished_at_stable_under_post_writes(env, capsys):
    """[ITEM-98.GWT-6] finish_task stamps finished_at; post-completion
    bumps of updated_at (note/notify) must not inflate cycle time nor
    re-enter old rows into the window."""
    db.register("w1", "/tmp/none", path=env.db)
    tid = _task(env, "w1")
    _claim_acp(env, tid)
    assert db.finish_task(tid, "acp-9-1", "done", path=env.db)
    con = db.connect(env.db)
    try:
        fin = con.execute("SELECT finished_at FROM tasks WHERE id=?", (tid,)).fetchone()[
            "finished_at"
        ]
        assert fin is not None
        # simulate a late post-completion write on updated_at
        con.execute("UPDATE tasks SET updated_at=? WHERE id=?", (fin + 3600, tid))
        con.commit()
    finally:
        con.close()
    rc, out = _run(["fleet-metrics", "--window", "7d", "--json"], capsys)
    assert rc == ExitCode.OK
    s = out["data"]["sessions"]["w1"]
    assert s["done"] == 1
    assert s["cycle_median_s"] is not None and s["cycle_median_s"] < 120


def test_item98_gwt7_escalated_rows_count_as_incidents(env, capsys, tmp_path):
    """[ITEM-98.GWT-7] escalated verdicts show in the incident log AND
    in the class histogram - not silently dropped from counts."""
    now_iso = datetime.now(timezone.utc).isoformat()
    repo = _cycle_repo(
        tmp_path,
        [
            {
                "ts": now_iso,
                "item": "i_esc",
                "action": "review",
                "verdict": "escalated",
                "status": "open",
            }
        ],
    )
    db.register("w1", str(repo), path=env.db)
    rc, out = _run(["fleet-metrics", "--window", "7d", "--json"], capsys)
    assert rc == ExitCode.OK
    m = out["data"]["repos"][repo.name]
    assert m["escalations"] == 1 and m["incidents_by_class"] == {"escalated": 1}


def test_item98_gwt8_missing_db_errors_without_creating(tmp_path, capsys, monkeypatch):
    """[ITEM-98.GWT-8] the report is kernel-enforced read-only: a
    missing state.db is an error, not an empty file created on disk."""
    missing = tmp_path / "no.db"
    monkeypatch.setenv("SESSIONS_BRIDGE_DB", str(missing))
    rc, out = _run(["fleet-metrics", "--json"], capsys)
    assert rc == ExitCode.INTERNAL
    assert out["status"] == "error" and not missing.exists()


def test_item98_gwt9_pre_finished_at_db_falls_back(env, capsys, tmp_path):
    """[ITEM-98.GWT-9] a state.db created before the finished_at
    column still reports (degraded to updated_at) instead of erroring
    on the missing column."""
    repo = tmp_path / "repoC"
    repo.mkdir()
    db.register("w1", str(repo), path=env.db)
    con = db.connect(env.db)
    try:
        con.execute("ALTER TABLE tasks DROP COLUMN finished_at")
        con.commit()
    finally:
        con.close()
    rc, out = _run(["fleet-metrics", "--window", "7d", "--json"], capsys)
    assert rc == ExitCode.OK
    assert "w1" in out["data"]["sessions"]


# --- task #216: quota-park / quota-resume / wake suppression / auto-park -------


def _parked(env, tid, sid=SID, reset_in=3600):
    assert db.quota_park(tid, sid, int(time.time()) + reset_in, "rate limit", "manual", path=env.db)


def test_quota_park_command_parks_live_task(env, capsys):
    """GWT-216-cli-1: bridge quota-park stores the park with an
    explicit reset; the lifecycle status stays running."""
    _managed(env)
    tid = _task(env)
    _claim_acp(env, tid)
    reset_iso = "2999-01-01T00:00:00Z"
    rc, out = _run(
        [
            "quota-park",
            SID,
            "--task",
            str(tid),
            "--reset-at",
            reset_iso,
            "--note",
            "claude rate limit",
            "--model",
            "opus",
            "--json",
        ],
        capsys,
    )
    assert rc == ExitCode.OK
    assert out["status"] == "waiting_quota"
    row = db.get_task(tid, path=env.db)
    assert row["status"] == "running"
    assert row["step_state"] == "waiting_quota"
    assert row["quota_source"] == "manual"
    assert row["quota_model"] == "opus"


def test_quota_park_refusals(env, capsys):
    """GWT-216-cli-2: wrong session, missing task, bad reset string
    all refuse cleanly."""
    _managed(env)
    tid = _task(env)
    _claim_acp(env, tid)
    rc, _out = _run(
        [
            "quota-park",
            "other",
            "--task",
            str(tid),
            "--reset-at",
            "2999-01-01T00:00:00Z",
            "--json",
        ],
        capsys,
    )
    assert rc == ExitCode.NOT_FOUND
    rc, _out = _run(
        ["quota-park", SID, "--task", str(tid), "--reset-at", "banana", "--json"],
        capsys,
    )
    assert rc == ExitCode.USAGE


def test_quota_resume_command_releases_park(env, capsys):
    """GWT-216-cli-3: bridge quota-resume clears the park early."""
    _managed(env)
    tid = _task(env)
    _claim_acp(env, tid)
    _parked(env, tid)
    rc, _out = _run(["quota-resume", SID, "--task", str(tid), "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    assert row["step_state"] is None
    # a second resume refuses - nothing parked any more
    rc, _out = _run(["quota-resume", SID, "--task", str(tid), "--json"], capsys)
    assert rc == ExitCode.CONFLICT


def test_wake_suppressed_before_quota_reset(env, monkeypatch, capsys):
    """GWT-216-cli-4: bridge wake on a parked session answers
    'suppressed: waiting_quota until <reset>' and never attaches."""
    _managed(env)
    tid = _task(env)
    _claim_acp(env, tid)
    _parked(env, tid)
    calls = _stub_wake(env, monkeypatch)
    rc, out = _run(["wake", SID, "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["status"] == "suppressed"
    assert calls == []
    # after the window closes the suppression lifts
    _parked(env, tid, reset_in=-10)
    rc, out = _run(["wake", SID, "--json"], capsys)
    assert calls == [SID]


def test_event_quota_text_autoparks(env, capsys):
    """GWT-216-cli-5: an event carrying rate-limit text + reset parks
    the reporter's live task - the self-report channel."""
    _managed(env)
    tid = _task(env)
    _claim_acp(env, tid)
    rc, out = _run(
        [
            "event",
            SID,
            "worker_note",
            "model rate limit reached, reset at 23:59 UTC",
            "--json",
        ],
        capsys,
    )
    assert rc == ExitCode.OK
    assert out["data"]["quota"]["parked"] is True
    row = db.get_task(tid, path=env.db)
    assert row["step_state"] == "waiting_quota"
    assert row["quota_source"] == "self-report"


def test_notice_quota_text_autoparks(env, capsys):
    """GWT-216-cli-6: same self-report contract on the notice channel."""
    _managed(env)
    tid = _task(env)
    _claim_acp(env, tid)
    rc, out = _run(
        ["notice", SID, "rate limit on the pool, retry after 1800s", "--json"],
        capsys,
    )
    assert rc == ExitCode.OK
    assert out["data"]["quota"]["parked"] is True
    row = db.get_task(tid, path=env.db)
    assert row["step_state"] == "waiting_quota"
    assert row["quota_reset_at"] > int(time.time()) + 1700


def test_non_quota_text_never_autoparks(env, capsys):
    """GWT-216-cli-7: rpc_error/busy/timeout text is never a park."""
    _managed(env)
    tid = _task(env)
    _claim_acp(env, tid)
    for text in (
        "rpc_error: host closed the channel",
        "session busy mid-turn",
        "timeout on prompt ack",
        "lease expired",
    ):
        rc, out = _run(["event", SID, "worker_note", text, "--json"], capsys)
        assert rc == ExitCode.OK
        assert "quota" not in out["data"]
    assert db.get_task(tid, path=env.db)["step_state"] is None


def test_inspect_and_status_show_quota_fields(env, capsys):
    """GWT-216-cli-8: inspect exposes step_state + reset; the status
    fleet table marks the session quota."""
    _managed(env)
    tid = _task(env)
    _claim_acp(env, tid)
    _parked(env, tid)
    rc, out = _run(["inspect", str(tid), "--json"], capsys)
    assert rc == ExitCode.OK
    assert out["data"]["step_state"] == "waiting_quota"
    assert out["data"]["quota_reset_at"] is not None
    rc, snap = _run_status_json(env.db)
    row = next(r for r in snap["data"]["fleet"] if r["session_id"] == SID)
    assert row["step_state"] == "waiting_quota"
    assert row["quota_reset_at"] is not None
    assert row["state"] == "quota"


def test_item230_work_local_quota_error_does_not_park(env, capsys, monkeypatch):
    """GWT-230-7 (review r1): a local failure whose text happens to
    contain the word 'quota' is not a provider signal - only rpc_error
    bodies are sniffed (OSError: Disk quota exceeded must not park)."""

    class DiskClient(_OkClient):
        def prompt(self, sid, text, timeout=None):
            raise OSError("Disk quota exceeded")

    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(dispatch, "AcpClient", DiskClient)
    rc, _out = _run(["_work", str(tid), "--owner", owner, "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    assert row["step_state"] is None
    assert row["status"] == TaskStatus.FAILED.value


def test_item230_work_load_phase_limit_also_parks(env, capsys, monkeypatch):
    """GWT-230-8: a rate-limit surfacing at session/load parks the
    same way (source acp-load) - the pool is dry before the prompt."""

    class LoadLimitedClient(_OkClient):
        def load_session(self, sid, cwd):
            raise AcpError(
                "session/load: Reached free model rate limit. Your limit will reset in 10 minutes.",
                kind="rpc_error",
            )

    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(dispatch, "AcpClient", LoadLimitedClient)
    rc, out = _run(["_work", str(tid), "--owner", owner, "--json"], capsys)
    assert rc == ExitCode.OK
    resp = JsonResponse.model_validate(out)
    assert resp.data["quota"]["parked"] is True
    row = db.get_task(tid, path=env.db)
    assert row["step_state"] == "waiting_quota"
    assert row["quota_source"] == "acp-load"


def test_item230_work_park_cas_lost_falls_through(env, capsys, monkeypatch):
    """GWT-230-9 (review r1): a park CAS lost to a task that moved on
    (not to a fresher park) falls through to by-cause handling - the
    failure is not masked by the quota path."""

    class LimitedClient(_OkClient):
        def prompt(self, sid, text, timeout=None):
            raise AcpError(
                "session/prompt: Reached free model rate limit. "
                "Your limit will reset in 10 minutes.",
                kind="rpc_error",
            )

    env.devin[SID] = {"working_directory": "/repo"}
    tid = _task(env)
    owner = _claim_acp(env, tid)
    monkeypatch.setattr(dispatch, "AcpClient", LimitedClient)
    monkeypatch.setattr(dispatch.db, "quota_park", lambda *a, **k: False)
    rc, _out = _run(["_work", str(tid), "--owner", owner, "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    assert row["step_state"] is None
    assert row["status"] == TaskStatus.FAILED.value
    events = [e["kind"] for e in db.list_events(session_id=SID, path=env.db)]
    assert "quota_park_lost" in events


def test_issue119_gwt9_resume_daemon_down_relays_and_prunes(env, capsys):
    """GWT-119-9 (review r1): with no daemon running, `bridge resume`
    materializes the RESUME file synchronously AND prunes the stale
    HALT file it supersedes - a drain during the outage must not meet
    a dead epoch (the daemon-resident janitor cannot run)."""
    _managed(env)
    db.set_session_halted(SID, True, path=env.db)
    halt_nid = db.notify(SID, NotificationKind.HALT, "stop", path=env.db)
    row = dict(_one(env.db, "SELECT * FROM notifications WHERE id=?", (halt_nid,)))
    db.mark_relayed(halt_nid, path=env.db)
    mdir = Path(env.qdir) / SID
    mdir.mkdir(parents=True)
    stale = mdir / f"msg_{halt_nid}.txt"
    stale.write_text(daemon.relay_payload(row))

    rc, out = _run(["resume", SID, "--json"], capsys)

    assert rc == ExitCode.OK
    assert out["data"].get("relayed") is True
    assert not stale.exists()
    files = [f.name for f in mdir.glob("msg_*.txt")]
    assert len(files) == 1
    body = (mdir / files[0]).read_text()
    assert "kind: resume" in body


def test_issue119_gwt10_resume_daemon_up_skips_sync(env, capsys, monkeypatch):
    """GWT-119-10 (review r1): with the daemon alive the command must
    NOT run the synchronous relay - the loop owns delivery timing."""
    _managed(env)
    db.set_session_halted(SID, True, path=env.db)
    monkeypatch.setattr(daemon, "daemon_running", lambda sdir: True)
    calls = []
    monkeypatch.setattr(daemon, "_relay_pass", lambda *a: calls.append(a) or {})

    rc, out = _run(["resume", SID, "--json"], capsys)

    assert rc == ExitCode.OK
    assert "relayed" not in out["data"]
    assert calls == []


def test_item121_gwt1_session_slot_cap_blocks_new_sid(env, monkeypatch):
    """[ITEM-121.GWT-1] cap=1: a queued task for an inactive session
    waits while every slot is held, and dispatches once a slot frees.
    (The already-active-sid exemption is unreachable by construction -
    ux_tasks_live_session allows one live task per session - but the
    guard stays so the check is safe under any future schema change.)"""
    # issue #142: pump pre-filter and CAS layer both read the durable
    # effective cap - the db module constant is the test hook
    monkeypatch.setattr(db, "MAX_ACTIVE_SESSIONS", 1)
    env.devin["s1"] = {"working_directory": "/r1"}
    env.devin["s2"] = {"working_directory": "/r2"}
    t1 = _task(env, sid="s1")
    owner = _claim_acp(env, t1)  # s1 holds the only slot
    t2 = _task(env, sid="s2")  # would need a new slot

    args = SimpleNamespace(db=env.db, queue_dir=str(env.qdir), timeout=None)
    result = bridge.pump(args)

    assert db.get_task(t2, path=env.db)["status"] == TaskStatus.QUEUED.value
    assert result["slot_wait"] == [t2]
    assert env.spawned == []
    # freeing the slot lets the next pass dispatch it
    assert db.finish_task(t1, owner, ok=True, path=env.db)
    result = bridge.pump(args)
    assert db.get_task(t2, path=env.db)["status"] == TaskStatus.RUNNING.value
    assert result["slot_wait"] == []


def test_item121_gwt2_zero_cap_unlimited(env):
    """[ITEM-121.GWT-2] the default (0) leaves dispatch unbounded -
    every queued task dispatches regardless of sid count."""
    for i in range(bridge.SEMAPHOR):
        sid = f"s{i}"
        env.devin[sid] = {"working_directory": f"/r{i}"}
        _task(env, sid=sid)
    args = SimpleNamespace(db=env.db, queue_dir=str(env.qdir), timeout=None)
    result = bridge.pump(args)
    assert result["slot_wait"] == []
    assert len(env.spawned) == bridge.SEMAPHOR


def test_item121_gwt3_status_surfaces_slots(env, monkeypatch):
    """[ITEM-121.GWT-3] status --json reports the session-slot
    occupancy, the configured cap, and the slot-wait reason."""
    # issue #142: the pump and the report read the durable effective
    # cap - monkeypatching the db-level fallback covers both readers
    monkeypatch.setattr(db, "MAX_ACTIVE_SESSIONS", 1)
    env.devin["s1"] = {"working_directory": "/r1"}
    env.devin["s2"] = {"working_directory": "/r2"}
    t1 = _task(env, sid="s1")
    _claim_acp(env, t1)
    _task(env, sid="s2")

    _rc, out = _run_status_json(env.db)

    assert out["data"]["session_slots"] == {"active": 1, "max": 1}
    assert out["data"]["slot_wait"] >= 1
    assert out["data"]["slot_wait_reason"] == "MAX_ACTIVE_SESSIONS_REACHED"


# --- issue #153: thrash circuit breaker ---------------------------------------


def _thrash_escalations(db_path):
    return [
        r for r in db.list_open_escalations(path=db_path) if r["doa_trigger"] == "thrash_breaker"
    ]


def test_issue153_gwt1_finish_fail_trips_breaker(env, capsys):
    """[ISSUE-153.GWT-1] the Nth consecutive `finish --fail` trips the
    breaker: decision event + open escalation + orchestrator NOTICE,
    and the finish response reports the trip."""
    db.register("orc1", "/repo/ctl", role="orchestrator", path=env.db)
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    tid = _task(env)

    out = None
    for i in range(db.THRASH_LIMIT):
        assert _run(["claim", str(tid), "--session", SID, "--json"], capsys)[0] == ExitCode.OK
        rc, out = _run(
            [
                "finish",
                str(tid),
                "--session",
                SID,
                "--fail",
                "--note",
                f"gate red {i}",
                "--json",
            ],
            capsys,
        )
        assert rc == ExitCode.OK
        if i < db.THRASH_LIMIT - 1:
            # a plain retry below the limit requeues as today
            assert _run(["retry", str(tid), "--json"], capsys)[0] == ExitCode.OK

    # the landing failure reported the trip in its response
    trip = out["data"]["thrash_breaker"]
    assert trip["fail_streak"] == db.THRASH_LIMIT
    assert trip["first"] is True
    assert trip["notified"] is True

    escs = _thrash_escalations(env.db)
    assert len(escs) == 1
    assert escs[0]["id"] == trip["escalation_id"]
    assert str(tid) in escs[0]["question_text"]

    events = db.list_events(kind="thrash_breaker", path=env.db)
    assert len(events) == 1
    assert json.loads(events[0]["body"])["task_id"] == tid

    notices = db.unrelayed_notifications("orc1", path=env.db)
    assert any(
        n["kind"] == NotificationKind.NOTICE.value and "thrash_breaker" in n["body"]
        for n in notices
    )


def test_issue153_gwt2_retry_refused_then_force(env, capsys):
    """[ISSUE-153.GWT-2] `bridge retry` at the limit conflicts with the
    breaker explanation + escalation id; a repeated refuse journals a
    second decision event but no duplicate escalation; `--force`
    unparks with the streak reset."""
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    tid = _task(env)
    con = db.connect(env.db)
    try:
        con.execute(
            "UPDATE tasks SET status='failed', fail_streak=? WHERE id=?",
            (db.THRASH_LIMIT, tid),
        )
    finally:
        con.close()

    # WHEN a plain retry is attempted at the limit, it conflicts
    rc, out = _run(["retry", str(tid), "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert out["status"] == "thrash_breaker"
    assert "thrash breaker" in out["error"]
    escs = _thrash_escalations(env.db)
    assert len(escs) == 1
    assert out["data"]["escalation_id"] == escs[0]["id"]
    assert db.get_task(tid, path=env.db)["status"] == TaskStatus.FAILED.value

    # a second refused retry journals the observation but no new
    # escalation
    rc, out = _run(["retry", str(tid), "--json"], capsys)
    assert rc == ExitCode.CONFLICT
    assert out["data"]["escalation_id"] == escs[0]["id"]
    assert len(_thrash_escalations(env.db)) == 1
    assert len(db.list_events(kind="thrash_breaker", path=env.db)) == 2

    # WHEN the operator forces it, the task unparks cleared
    rc, out = _run(["retry", str(tid), "--force", "--json"], capsys)
    assert rc == ExitCode.OK
    row = db.get_task(tid, path=env.db)
    assert row["fail_streak"] == 0
    assert row["attempt"] == 0
    assert row["status"] != TaskStatus.FAILED.value


def test_issue153_gwt3_disabled_threshold(env, capsys, monkeypatch):
    """[ISSUE-153.GWT-3] SB_THRASH_LIMIT<=0 disables the breaker: the
    plain retry passes and nothing is journaled."""
    monkeypatch.setattr(db, "THRASH_LIMIT", 0)
    tid = _task(env)
    con = db.connect(env.db)
    try:
        con.execute(
            "UPDATE tasks SET status='failed', fail_streak=? WHERE id=?",
            (99, tid),
        )
    finally:
        con.close()

    rc, _out = _run(["retry", str(tid), "--json"], capsys)
    assert rc == ExitCode.OK
    assert db.list_events(kind="thrash_breaker", path=env.db) == []
    assert _thrash_escalations(env.db) == []


# --- issue #164: bridge stop / kill --hard-stop -------------------------------


def test_issue164_gwt1_stop_not_found(env, capsys):
    """[ISSUE-164.CLI-1] `bridge stop <sid>` on an unregistered sid is
    NOT_FOUND, mirroring kill - no attach is attempted."""
    rc, out = _run(["stop", "ghost", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND and out["status"] == "not_found"


def test_issue164_gwt2_stop_reports_layer_outcomes(env, capsys, monkeypatch):
    """[ISSUE-164.CLI-2] `bridge stop <sid>` runs cancel then forced
    close and reports both outcomes verbatim; a 'requested' layer is
    honest, not a claimed stop. The session_stopped decision event
    carries the same outcomes."""
    db.register("w1", "/repo/w", path=env.db)
    seen = []
    monkeypatch.setattr(
        sessions, "cancel_turn", lambda sid, **kw: seen.append("cancel") or "requested"
    )
    monkeypatch.setattr(
        sessions, "close_session", lambda sid, **kw: seen.append("close") or "requested"
    )

    rc, out = _run(["stop", "w1", "--json"], capsys)

    assert rc == ExitCode.OK
    assert seen == ["cancel", "close"]  # layer order is the contract
    d = out["data"]
    assert d["cancel_outcome"] == "requested" and d["close_outcome"] == "requested"
    assert d["status"] == "requested"
    ev = db.list_events(session_id="w1", kind="session_stopped", path=env.db)
    assert len(ev) == 1
    assert "cancel=requested" in ev[0]["body"] and "close=requested" in ev[0]["body"]
    # stop is session-level only: the bus row is untouched
    assert db.get_session("w1", path=env.db)["status"] == "active"


def test_issue164_gwt3_stop_confirmed_reports_stopped(env, capsys, monkeypatch):
    """[ISSUE-164.CLI-3] an observed-stopped layer upgrades the overall
    verdict to 'stopped' - claimed only on observed idle/absent."""
    db.register("w1", "/repo/w", path=env.db)
    monkeypatch.setattr(sessions, "cancel_turn", lambda sid, **kw: "confirmed")
    monkeypatch.setattr(sessions, "close_session", lambda sid, **kw: "confirmed")

    rc, out = _run(["stop", "w1", "--json"], capsys)

    assert rc == ExitCode.OK and out["data"]["status"] == "stopped"


def test_issue164_gwt4_stop_dry_run_suppresses_layers(env, capsys, monkeypatch):
    """[ISSUE-164.CLI-4] --dry-run sends nothing: layers 0-1 report
    'dry_run' and never call the transport - the only safe way to
    preview a real stop on a live host."""
    db.register("w1", "/repo/w", path=env.db)

    def _boom(*a, **kw):
        raise AssertionError("dry_run must not attach")

    monkeypatch.setattr(sessions, "cancel_turn", _boom)
    monkeypatch.setattr(sessions, "close_session", _boom)

    rc, out = _run(["stop", "w1", "--dry-run", "--json"], capsys)

    assert rc == ExitCode.OK
    assert out["data"]["status"] == "dry_run"
    assert out["data"]["cancel_outcome"] == "dry_run"
    assert out["data"]["close_outcome"] == "dry_run"
    ev = db.list_events(session_id="w1", kind="session_stopped", path=env.db)
    assert len(ev) == 1 and "dry_run=True" in ev[0]["body"]


def test_issue164_gwt5_stop_hard_dry_run_enumerates(env, capsys, monkeypatch, tmp_path):
    """[ISSUE-164.CLI-5] `bridge stop <sid> --hard --dry-run` runs the
    attribution enumeration over the (faked) backend tree and lists
    candidates + exclusions - nothing is signalled, nothing sent."""
    from session_bridge.stop import hard_stop
    from session_bridge.stop.hard_stop import ProcInfo

    db.register("w1", "/repo/w", path=env.db)
    db.register("w2", "/repo/w2", path=env.db)

    def _boom(*a, **kw):
        raise AssertionError("dry_run must not attach")

    monkeypatch.setattr(sessions, "cancel_turn", _boom)
    monkeypatch.setattr(sessions, "close_session", _boom)
    monkeypatch.setattr(sessions, "find_lockfile", lambda *a, **kw: {"pid": 100})

    def P(pid, ppid, argv, cwd=None, start=2000.0):
        return ProcInfo(pid=pid, ppid=ppid, argv=tuple(argv), cwd=cwd, start_seconds=start)

    fake_procs = [
        P(100, 1, ["/opt/windsurf-cli", "agent", "acp-host"], "/home/u", 900.0),
        P(101, 100, ["/opt/bin/devin", "acp"], "/home/u", 1000.0),
        P(102, 101, ["/usr/bin/python3", "/prj/mcp_partner.py"], "/home/u", 1001.0),
        P(103, 101, ["bash", "-c", "make"], "/repo/w/build", 2000.0),
        P(104, 101, ["python3", "w.py", "queue/w1"], "/home/u", 2000.0),
        P(105, 101, ["bash", "-c", "build"], "/repo/w2", 2000.0),
    ]
    monkeypatch.setattr(hard_stop, "list_processes", lambda *a, **kw: list(fake_procs))

    rc, out = _run(["stop", "w1", "--hard", "--dry-run", "--json"], capsys)

    assert rc == ExitCode.OK
    d = out["data"]
    assert d["status"] == "dry_run"
    assert d["sweep_status"] == "dry_run"
    assert {c["pid"] for c in d["candidates"]} == {103, 104}
    assert d["swept"] == []
    excluded = {e["pid"]: e["exclusion"] for e in d["sweep_excluded"]}
    assert excluded[102] == "mcp_server" and excluded[105].startswith("other_session:")
    assert d["backend_pid"] == 101


def test_issue164_gwt6_kill_hard_stop_chains_layers_then_teardown(env, capsys, monkeypatch):
    """[ISSUE-164.CLI-6] `bridge kill <sid> --hard-stop` runs the stop
    ladder FIRST and db.kill_session LAST; the session_killed event
    body carries the per-layer outcomes."""
    db.register("w1", "/repo/w", path=env.db)
    db.heartbeat("w1", path=env.db)
    tid = _task(env, sid="w1")
    _claim_acp(env, tid)
    monkeypatch.setattr(sessions, "cancel_turn", lambda sid, **kw: "requested")
    monkeypatch.setattr(sessions, "close_session", lambda sid, **kw: "requested")
    canned = {
        "sweep_status": "ok",
        "swept": [
            {"pid": 424242, "reason": "cwd_under:/repo/w", "signal": "SIGTERM", "outcome": "gone"}
        ],
        "candidates": [{"pid": 424242, "reason": "cwd_under:/repo/w", "cmdline": "bash"}],
        "sweep_excluded": [],
        "acp_host_pid": 100,
        "backend_pid": 101,
    }
    monkeypatch.setattr(sessions, "_run_sweep", lambda args, sid, **kw: dict(canned))

    rc, out = _run(["kill", "w1", "--hard-stop", "--json"], capsys)

    assert rc == ExitCode.OK
    d = out["data"]
    assert d["killed"] and d["tasks_requeued"] == 1
    assert d["stop"]["cancel_outcome"] == "requested"
    assert d["stop"]["close_outcome"] == "requested"
    assert d["stop"]["swept"][0]["pid"] == 424242
    assert db.get_session("w1", path=env.db)["status"] == "dead"
    ev = db.list_events(session_id="w1", kind="session_killed", path=env.db)
    assert len(ev) == 1
    body = ev[0]["body"]
    assert "stop_cancel=requested" in body
    assert "stop_close=requested" in body
    assert "swept_pids=[424242]" in body


def test_issue164_gwt7_kill_dry_run_no_teardown(env, capsys, monkeypatch):
    """[ISSUE-164.CLI-7] `bridge kill <sid> --hard-stop --dry-run`
    reports the layers + candidates and leaves the bus row alone."""
    db.register("w1", "/repo/w", path=env.db)
    monkeypatch.setattr(
        sessions,
        "_run_stop_layers",
        lambda args, sid, **kw: {
            "hard": True,
            "dry_run": True,
            "cancel_outcome": "dry_run",
            "close_outcome": "dry_run",
            "sweep_status": "dry_run",
            "swept": [],
            "candidates": [{"pid": 1, "reason": "x", "cmdline": "c"}],
            "sweep_excluded": [],
            "acp_host_pid": 100,
            "backend_pid": 101,
        },
    )

    rc, out = _run(["kill", "w1", "--hard-stop", "--dry-run", "--json"], capsys)

    assert rc == ExitCode.OK and out["data"]["status"] == "dry_run"
    assert db.get_session("w1", path=env.db)["status"] == "active"
    assert db.list_events(session_id="w1", kind="session_killed", path=env.db) == []


def test_issue164_gwt8_kill_hard_stop_unregistered_is_not_found(env, capsys):
    """[ISSUE-164.CLI-8] kill --hard-stop on a ghost refuses before any
    attach - same not_found contract as plain kill."""
    rc, out = _run(["kill", "ghost", "--hard-stop", "--json"], capsys)
    assert rc == ExitCode.NOT_FOUND and out["status"] == "not_found"


def test_issue164_gwt9_kill_task_ignores_hard_flags(env, capsys):
    """[ISSUE-164.CLI-9] `bridge kill <task_id>` keeps its contract:
    the session-level --hard-stop/--dry-run flags do not reach the
    per-task force-fail path."""
    db.register("s1", "/repo/s", path=env.db)
    tid = _task(env)
    _claim_acp(env, tid)

    rc, _out = _run(["kill", str(tid), "--hard-stop", "--json"], capsys)

    assert rc == ExitCode.OK
    assert db.get_task(tid, path=env.db)["status"] == "failed"
    assert db.get_session("s1", path=env.db)["status"] == "active"


def test_issue164_gwt10_stop_all_layers_failed_is_internal(env, capsys, monkeypatch):
    """[ISSUE-164.CLI-10] review MINOR: every layer failing before
    delivery reports status=error AND exits non-zero - scripts read
    the exit code before the JSON, so a refused stop cannot masquerade
    as an ok-with-caveats. Under the issue #167 gate a failed cancel
    also skips the close (the same transport would refuse it), so
    nothing was delivered on either layer."""
    db.register("w1", "/repo/w", path=env.db)

    def _down(*a, **kw):
        raise WakeError("wake_host_down", "no acp-host lockfile")

    monkeypatch.setattr(sessions, "cancel_turn", _down)
    monkeypatch.setattr(sessions, "close_session", _down)

    rc, out = _run(["stop", "w1", "--json"], capsys)

    assert rc == ExitCode.INTERNAL
    assert out["status"] == "error"
    assert out["data"]["cancel_outcome"] == "failed:wake_host_down"
    assert out["data"]["close_outcome"] == "skipped:failed:wake_host_down"
    ev = db.list_events(session_id="w1", kind="session_stopped", path=env.db)
    assert len(ev) == 1 and "status=error" in ev[0]["body"]


def test_issue164_gwt11_kill_hard_stop_teardown_survives_layer_crash(env, capsys, monkeypatch):
    """[ISSUE-164.CLI-11] review WARNING-3: an unexpected exception in
    the stop ladder must NOT strand the registry alive over a
    half-killed tree - db.kill_session still runs LAST, and the
    exception surfaces in the JSON, the human line and the
    session_killed event body."""
    db.register("w1", "/repo/w", path=env.db)
    db.heartbeat("w1", path=env.db)
    tid = _task(env, sid="w1")
    _claim_acp(env, tid)

    def _boom(*a, **kw):
        raise RuntimeError("ladder exploded")

    monkeypatch.setattr(sessions, "_run_stop_layers", _boom)

    rc, out = _run(["kill", "w1", "--hard-stop", "--json"], capsys)

    # teardown still ran to completion
    assert rc == ExitCode.OK
    assert out["data"]["killed"] and out["data"]["tasks_requeued"] == 1
    assert db.get_session("w1", path=env.db)["status"] == "dead"
    # the failure is surfaced, not swallowed
    assert "RuntimeError" in out["data"]["stop"]["stop_exception"]
    assert "ladder exploded" in out["data"]["stop"]["stop_exception"]
    ev = db.list_events(session_id="w1", kind="session_killed", path=env.db)
    assert len(ev) == 1
    assert "stop_exception=RuntimeError" in ev[0]["body"]


def test_issue164_gwt12_stop_layer_crash_still_writes_audit(env, capsys, monkeypatch):
    """[ISSUE-164.CLI-12] review round-2: an unexpected exception
    inside the stop ladder must NOT lose the session_stopped audit
    event - layers 0-1 may already have signalled the live session,
    so the run still journals status=error with stop_exception and
    exits INTERNAL."""
    db.register("w1", "/repo/w", path=env.db)

    def _boom(*a, **kw):
        raise RuntimeError("sweep enumerator exploded")

    monkeypatch.setattr(sessions, "_run_stop_layers", _boom)

    rc, out = _run(["stop", "w1", "--hard", "--json"], capsys)

    assert rc == ExitCode.INTERNAL
    assert out["status"] == "error"
    assert "RuntimeError" in out["data"]["stop_exception"]
    assert "sweep enumerator exploded" in out["data"]["stop_exception"]
    ev = db.list_events(session_id="w1", kind="session_stopped", path=env.db)
    assert len(ev) == 1
    assert "status=error" in ev[0]["body"]
    assert "stop_exception=RuntimeError" in ev[0]["body"]


# --- issue #167: layer-1 close is a conditional escalation -----------------


def test_issue167_gwt1_stop_idle_cancel_skips_close(env, capsys, monkeypatch):
    """[ISSUE-167.CLI-1] cancel=idle means the goal state is already
    observed: the forced session/close is NEVER sent - close_session
    is not called - and close_outcome reports skipped:idle in the
    JSON, the session_stopped event and the human line. The verdict
    is 'stopped' (observed idle), a skip is not a stop claim."""
    db.register("w1", "/repo/w", path=env.db)
    sent = []
    monkeypatch.setattr(sessions, "cancel_turn", lambda sid, **kw: "idle")
    monkeypatch.setattr(
        sessions,
        "close_session",
        lambda sid, **kw: sent.append(sid) or "confirmed",
    )

    rc, out = _run(["stop", "w1", "--json"], capsys)

    assert rc == ExitCode.OK
    assert sent == []  # layer 1 must not fire on an idle session
    d = out["data"]
    assert d["cancel_outcome"] == "idle"
    assert d["close_outcome"] == "skipped:idle"
    assert d["status"] == "stopped"
    ev = db.list_events(session_id="w1", kind="session_stopped", path=env.db)
    assert len(ev) == 1
    assert "cancel=idle" in ev[0]["body"]
    assert "close=skipped:idle" in ev[0]["body"]
    assert "status=stopped" in ev[0]["body"]

    # the human line carries the same honest skip reason
    rc = bridge.main(["stop", "w1"])
    human = capsys.readouterr().out.strip()
    assert rc == ExitCode.OK
    assert "cancel=idle" in human and "close=skipped:idle" in human


def test_issue167_gwt2_stop_confirmed_cancel_skips_close(env, capsys, monkeypatch):
    """[ISSUE-167.CLI-2] cancel=confirmed observed the turn stopped:
    no escalation is warranted - close_session is not called and
    close_outcome reports skipped:confirmed under status=stopped."""
    db.register("w1", "/repo/w", path=env.db)
    sent = []
    monkeypatch.setattr(sessions, "cancel_turn", lambda sid, **kw: "confirmed")
    monkeypatch.setattr(
        sessions,
        "close_session",
        lambda sid, **kw: sent.append(sid) or "confirmed",
    )

    rc, out = _run(["stop", "w1", "--json"], capsys)

    assert rc == ExitCode.OK
    assert sent == []
    d = out["data"]
    assert d["cancel_outcome"] == "confirmed"
    assert d["close_outcome"] == "skipped:confirmed"
    assert d["status"] == "stopped"
    ev = db.list_events(session_id="w1", kind="session_stopped", path=env.db)
    assert len(ev) == 1 and "close=skipped:confirmed" in ev[0]["body"]


def test_issue167_gwt3_stop_requested_cancel_runs_close(env, capsys, monkeypatch):
    """[ISSUE-167.CLI-3] cancel=requested is the ONE outcome that
    escalates: the session still reports busy after the layer-0
    grace, so the forced close fires and reports its real outcome."""
    db.register("w1", "/repo/w", path=env.db)
    seen = []
    monkeypatch.setattr(
        sessions, "cancel_turn", lambda sid, **kw: seen.append("cancel") or "requested"
    )
    monkeypatch.setattr(
        sessions,
        "close_session",
        lambda sid, **kw: seen.append("close") or "confirmed",
    )

    rc, out = _run(["stop", "w1", "--json"], capsys)

    assert rc == ExitCode.OK
    assert seen == ["cancel", "close"]  # escalation order preserved
    d = out["data"]
    assert d["cancel_outcome"] == "requested"
    assert d["close_outcome"] == "confirmed"
    assert d["status"] == "stopped"


def test_issue167_gwt4_stop_failed_cancel_skips_close(env, capsys, monkeypatch):
    """[ISSUE-167.CLI-4] cancel=failed:<reason> means the transport
    refused - a close attempt would hit the same dead transport, so
    it is skipped with the honest skipped:failed:<reason> verdict
    (the skip is recorded, not masked). No delivery happened, so the
    verdict stays error and exits non-zero."""
    db.register("w1", "/repo/w", path=env.db)
    sent = []

    def _down(*a, **kw):
        raise WakeError("wake_host_down", "no acp-host lockfile")

    monkeypatch.setattr(sessions, "cancel_turn", _down)
    monkeypatch.setattr(
        sessions,
        "close_session",
        lambda sid, **kw: sent.append(sid) or "confirmed",
    )

    rc, out = _run(["stop", "w1", "--json"], capsys)

    assert rc == ExitCode.INTERNAL
    assert sent == []
    assert out["status"] == "error"
    d = out["data"]
    assert d["cancel_outcome"] == "failed:wake_host_down"
    assert d["close_outcome"] == "skipped:failed:wake_host_down"
    ev = db.list_events(session_id="w1", kind="session_stopped", path=env.db)
    assert len(ev) == 1
    assert "close=skipped:failed:wake_host_down" in ev[0]["body"]
    assert "status=error" in ev[0]["body"]


def test_issue167_gwt5_real_close_rpc_error_stays_honest(env, capsys, monkeypatch):
    """[ISSUE-167.CLI-5] only the UNCONDITIONAL invocation was the
    bug: when the escalation gate does fire (cancel=requested), a
    WakeError on the real close attempt is still reported verbatim
    as failed:<reason>, never converted into a skip."""
    db.register("w1", "/repo/w", path=env.db)
    seen = []
    monkeypatch.setattr(sessions, "cancel_turn", lambda sid, **kw: "requested")

    def _rpc_err(*a, **kw):
        seen.append("close")
        raise WakeError("rpc_error", "close rejected")

    monkeypatch.setattr(sessions, "close_session", _rpc_err)

    rc, out = _run(["stop", "w1", "--json"], capsys)

    assert rc == ExitCode.OK
    assert seen == ["close"]
    d = out["data"]
    assert d["cancel_outcome"] == "requested"
    assert d["close_outcome"] == "failed:rpc_error"
    assert d["status"] == "requested"


def test_issue167_gwt6_kill_hard_stop_inherits_close_skip(env, capsys, monkeypatch):
    """[ISSUE-167.CLI-6] `bridge kill <sid> --hard-stop` shares
    _run_stop_layers, so the same gate applies: cancel=idle skips
    the close (close_session never called), the skipped outcome
    folds into the JSON stop block and the session_killed event,
    and the bus teardown still runs last."""
    db.register("w1", "/repo/w", path=env.db)
    db.heartbeat("w1", path=env.db)
    tid = _task(env, sid="w1")
    _claim_acp(env, tid)
    sent = []
    monkeypatch.setattr(sessions, "cancel_turn", lambda sid, **kw: "idle")
    monkeypatch.setattr(
        sessions,
        "close_session",
        lambda sid, **kw: sent.append(sid) or "confirmed",
    )

    rc, out = _run(["kill", "w1", "--hard-stop", "--json"], capsys)

    assert rc == ExitCode.OK
    assert sent == []
    d = out["data"]
    assert d["killed"] and d["tasks_requeued"] == 1
    assert d["stop"]["cancel_outcome"] == "idle"
    assert d["stop"]["close_outcome"] == "skipped:idle"
    assert db.get_session("w1", path=env.db)["status"] == "dead"
    ev = db.list_events(session_id="w1", kind="session_killed", path=env.db)
    assert len(ev) == 1
    assert "stop_cancel=idle" in ev[0]["body"]
    assert "stop_close=skipped:idle" in ev[0]["body"]

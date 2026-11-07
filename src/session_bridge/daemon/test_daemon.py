"""Tests for bridge-daemon (item_13d): relay, staleness sweep, flock.

state.db and the fs queue are real in tmp_path; notify-send is mocked
through the injectable ``notify_send`` callable of ``daemon.run`` and
the ``SESSIONS_BRIDGE_NOTIFY_SEND`` env override. The start/stop
lifecycle test spawns the real detached child via ``bridge daemon``.
"""

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from session_bridge.cli import bridge
from session_bridge.daemon import core as daemon
from session_bridge.state import db
from session_bridge.state.contract import (
    CLAIM_TIMEOUT_SECONDS,
    PENDING_TIMEOUT_SECONDS,
    NotificationKind,
    SessionMode,
    SessionStatus,
    TaskStatus,
    Transport,
)

SID = "s1"


@pytest.fixture()
def env(tmp_path):
    """Isolated state.db + queue dir + decision/heartbeat paths."""
    db_path = str(tmp_path / "state.db")
    db.init(db_path)
    qdir = str(tmp_path / "queue")
    sent = []
    return SimpleNamespace(
        db=db_path,
        qdir=qdir,
        sdir=str(tmp_path),
        sent=sent,
        notifier=lambda title, body: sent.append((title, body)),
    )


def _exec(path, sql, args=()):
    con = db.connect(path)
    try:
        con.execute(sql, args)
    finally:
        con.close()


def _one(path, sql, args=()):
    con = db.connect(path)
    try:
        return con.execute(sql, args).fetchone()
    finally:
        con.close()


def _stale_heartbeat(path, sid):
    _exec(
        path,
        "UPDATE worker_heartbeats SET last_seen_at=? WHERE session_id=?",
        (int(time.time()) - db.HEARTBEAT_STALE_SECONDS - 5, sid),
    )


def _msg_files(mdir):
    try:
        return [n for n in os.listdir(mdir) if n.startswith("msg_")]
    except OSError:
        return []


def _decisions(env):
    path = Path(env.sdir) / daemon.DECISION_LOG_FILENAME
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


# --- GWT-1: outbox row -> fs mailbox -> relayed_at -------------------------------


def test_item13d_gwt1_relay_roundtrip(env):
    nid = db.notify(
        SID, NotificationKind.TASK_DISPATCH, "write the tests first", task_id=42, path=env.db
    )
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    mdir = Path(env.qdir) / SID
    files = _msg_files(mdir)
    assert files == [f"msg_{nid}.txt"]
    text = (mdir / files[0]).read_text()
    # kind + routing refs are embedded in the payload (duty text routes by kind)
    assert f"notification_id: {nid}" in text
    assert "kind: task_dispatch" in text
    assert "task_id: 42" in text
    assert text.rstrip().endswith("write the tests first")
    # the outbox row is marked relayed
    assert db.unrelayed_notifications(path=env.db) == []


def test_item13d_relay_payload_format(env):
    nid = db.notify(
        SID,
        NotificationKind.QUESTION_ANSWER,
        "use the CAS writer",
        task_id=7,
        question_id=3,
        path=env.db,
    )
    row = db.unrelayed_notifications(path=env.db)[0]
    payload = daemon.relay_payload(row)
    head, _, body = payload.partition("\n\n")
    assert head.splitlines() == [
        f"notification_id: {nid}",
        f"session_id: {SID}",
        "kind: question_answer",
        "task_id: 7",
        "question_id: 3",
    ]
    assert body == "use the CAS writer"


def test_item13d_relay_payload_omits_absent_refs(env):
    db.notify(SID, NotificationKind.DISARM, "disarm now", path=env.db)
    row = db.unrelayed_notifications(path=env.db)[0]
    payload = daemon.relay_payload(row)
    assert "task_id:" not in payload
    assert "question_id:" not in payload
    assert payload.startswith("notification_id: ")
    assert "kind: disarm" in payload


def test_item13d_relay_redelivers_unrelayed_row(env):
    # a relay killed between the fs write and mark_relayed leaves the
    # row unrelayed; the next pass re-delivers it (at-least-once)
    nid = db.notify(SID, NotificationKind.HEARTBEAT_TICK, "tick", path=env.db)
    mdir = Path(env.qdir) / SID
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    # unmark the row to simulate the crash window, keep the msg file
    _exec(env.db, "UPDATE notifications SET relayed_at=NULL WHERE id=?", (nid,))
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    assert db.unrelayed_notifications(path=env.db) == []
    assert _msg_files(mdir) == [f"msg_{nid}.txt"]  # still exactly one file


# --- GWT-2: heartbeat staleness sweep ------------------------------------------


def test_item13d_gwt2_dead_worker_requeued_with_reset_instruction(env):
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted
    tid = db.enqueue(SID, "do the thing", path=env.db)
    assert tid is not None
    assert db.claim_queued(tid, "acp-1", Transport.ACP_PUSH, path=env.db)
    assert db.mark_running(tid, "acp-1", path=env.db)
    _stale_heartbeat(env.db, SID)

    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK

    session = db.get_session(SID, path=env.db)
    assert session["status"] == SessionStatus.DEAD.value
    task = db.get_task(tid, path=env.db)
    # running -> queued, lease cleared, reset instruction stamped
    assert task["status"] == TaskStatus.QUEUED.value
    assert task["lease_owner"] is None
    assert task["lease_expires_at"] is None
    assert task["attempt"] == 1
    assert "requeued-dead-worker" in task["note"]
    assert "base ref" in task["note"]
    assert "stash" in task["note"]
    # decided once: a second pass does not re-transition or re-requeue
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    assert db.get_task(tid, path=env.db)["attempt"] == 1


def test_item13d_gwt2_dead_worker_fails_after_attempt_budget(env):
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted
    tid = db.enqueue(SID, "do the thing", path=env.db)
    assert tid is not None
    _exec(env.db, "UPDATE tasks SET attempt=? WHERE id=?", (1, tid))
    _stale_heartbeat(env.db, SID)
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    task = db.get_task(tid, path=env.db)
    assert task["status"] == TaskStatus.FAILED.value  # poison pill, not another requeue


def test_item13d_gwt2_orchestrator_silence_notifies_once(env):
    res = db.register("orc", "/repo/ctl", role="orchestrator", path=env.db)
    assert res.accepted
    _stale_heartbeat(env.db, "orc")

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert len(env.sent) == 1
    title, body = env.sent[0]
    assert "ORCHESTRATOR DEAD" in title
    assert "orc" in body
    assert db.get_session("orc", path=env.db)["status"] == SessionStatus.DEAD.value

    # dedup: later passes while still dead do not re-notify
    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert len(env.sent) == 1


def test_item13d_fresh_heartbeat_untouched(env):
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted
    tid = db.enqueue(SID, "do the thing", path=env.db)
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    assert db.get_session(SID, path=env.db)["status"] == SessionStatus.ACTIVE.value
    assert db.get_task(tid, path=env.db)["status"] == TaskStatus.QUEUED.value
    assert env.sent == []


def test_item13d_revived_session_can_die_again(env):
    res = db.register("orc", "/repo/ctl", role="orchestrator", path=env.db)
    assert res.accepted
    _stale_heartbeat(env.db, "orc")
    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert len(env.sent) == 1
    # re-register revives to active; a second silence notifies again
    res2 = db.register("orc", "/repo/ctl", role="orchestrator", force=True, path=env.db)
    assert res2.accepted
    assert db.get_session("orc", path=env.db)["status"] == SessionStatus.ACTIVE.value
    _stale_heartbeat(env.db, "orc")
    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert len(env.sent) == 2


# --- GWT-3: flock singleton -----------------------------------------------------


def test_item13d_gwt3_second_instance_refused(env):
    fd = daemon._acquire_lock(env.sdir)
    assert fd is not None
    try:
        assert daemon.daemon_running(env.sdir)
        assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_LOCK_HELD
        events = [d["event"] for d in _decisions(env)]
        assert "lock_refused" in events
    finally:
        os.close(fd)
    assert not daemon.daemon_running(env.sdir)


def test_item13d_gwt3_daemon_start_refuses_held_lock(env, capsys):
    fd = daemon._acquire_lock(env.sdir)
    try:
        rc = bridge.main(["--db", env.db, "daemon", "start", "--json"])
        out = capsys.readouterr().out.strip()
        resp = json.loads(out)
        assert rc == 4  # ExitCode.CONFLICT
        assert resp["status"] == "running"
    finally:
        os.close(fd)


# --- once mode, heartbeat file, decision log ------------------------------------


def test_item13d_once_writes_heartbeat_and_decision_log(env):
    db.notify(SID, NotificationKind.DIRECTIVE, "steer left", path=env.db)
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK

    hb = Path(env.sdir) / daemon.HEARTBEAT_FILENAME
    assert hb.exists()
    # line 1 is the timestamp contract; line 2 carries the item_139
    # mail-stall metric - readers take the first line only
    ts = int(hb.read_text().splitlines()[0])
    assert abs(ts - int(time.time())) < 5
    assert daemon.heartbeat_age(env.sdir) is not None
    assert daemon.heartbeat_age(env.sdir) < 5

    events = {d["event"] for d in _decisions(env)}
    assert {"daemon_start", "relayed", "daemon_stop"} <= events
    for d in _decisions(env):
        assert set(d) == {"ts", "event", "sid", "detail"}
        assert isinstance(d["ts"], int)


def test_item13d_decision_log_records_worker_death(env):
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted
    tid = db.enqueue(SID, "do the thing", path=env.db)
    _stale_heartbeat(env.db, SID)
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    dead = [d for d in _decisions(env) if d["event"] == "worker_dead"]
    assert len(dead) == 1
    assert dead[0]["sid"] == SID
    assert f"task={tid}" in dead[0]["detail"]
    assert "queued" in dead[0]["detail"]


def test_item13d_requeue_note_is_reset_instruction():
    note = daemon.requeue_note(9)
    assert note.startswith("requeued-dead-worker")
    assert "stash" in note
    assert "base ref" in note


# --- item_13g: question claim janitor inside the pass ---------------------------


def test_item13g_daemon_pass_janitors_stale_claims(env):
    db.register("orc1", "/repo/ctl", role="orchestrator", path=env.db)
    tid = db.enqueue(SID, "do the thing", path=env.db)
    qid = db.ask(SID, tid, "which branch?", path=env.db).question_id
    assert db.claim_question(qid, "orc1", path=env.db)
    _exec(
        env.db,
        "UPDATE questions SET claimed_at=? WHERE id=?",
        (int(time.time()) - CLAIM_TIMEOUT_SECONDS - 5, qid),
    )
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    q = db.get_question(qid, path=env.db)
    # the stale claim is released back to pending, same question id
    assert q["state"] == "pending"
    assert q["claimed_by"] is None
    assert q["claimed_at"] is None
    released = [d for d in _decisions(env) if d["event"] == "question_claims_released"]
    assert len(released) == 1
    assert "released=1" in released[0]["detail"]
    assert str(qid) in released[0]["detail"]
    # a quiet pass releases nothing and logs nothing
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    assert len([d for d in _decisions(env) if d["event"] == "question_claims_released"]) == 1


def test_item20_daemon_pass_expires_pending_and_notifies_worker(env):
    """[ITEM-20.GWT-3] a pending question older than the TTL expires
    in the janitor pass and the worker is notified through the outbox;
    the freed slot lets the worker re-ask."""
    db.register("orc1", "/repo/ctl", role="orchestrator", path=env.db)
    tid = db.enqueue(SID, "do the thing", path=env.db)
    qid = db.ask(SID, tid, "which branch?", path=env.db).question_id
    _exec(
        env.db,
        "UPDATE questions SET asked_at=? WHERE id=?",
        (int(time.time()) - PENDING_TIMEOUT_SECONDS - 5, qid),
    )
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    q = db.get_question(qid, path=env.db)
    assert q["state"] == "expired"
    # the expiry notice is an outbox row for the worker: it relays on
    # the next pass (the janitor runs after the relay in the loop)
    pending = db.unrelayed_notifications(SID, path=env.db)
    assert len(pending) == 1
    assert pending[0]["kind"] == NotificationKind.QUESTION_ANSWER.value
    assert pending[0]["question_id"] == qid
    assert pending[0]["task_id"] == tid
    assert "expired" in pending[0]["body"]
    decisions = _decisions(env)
    assert any(d["event"] == "question_pending_expired" for d in decisions)
    # second pass: the notice relays to the fs mailbox; expired is
    # terminal so nothing further expires
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    msgs = _msg_files(Path(env.qdir) / SID)
    expiry = [
        m
        for m in msgs
        if "expired" in (Path(env.qdir) / SID / m).read_text()
        and f"question_id: {qid}" in (Path(env.qdir) / SID / m).read_text()
    ]
    assert len(expiry) == 1
    assert len([d for d in _decisions(env) if d["event"] == "question_pending_expired"]) == 1
    # the freed slot: a fresh ask on the same task is accepted again
    assert db.ask(SID, tid, "retry?", path=env.db).accepted


def test_item13g_fresh_claim_survives_the_pass(env):
    db.register("orc1", "/repo/ctl", role="orchestrator", path=env.db)
    tid = db.enqueue(SID, "do the thing", path=env.db)
    qid = db.ask(SID, tid, "which branch?", path=env.db).question_id
    assert db.claim_question(qid, "orc1", path=env.db)
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    q = db.get_question(qid, path=env.db)
    assert q["state"] == "claimed"
    assert q["claimed_by"] == "orc1"
    assert _decisions(env)  # daemon_start/stop lines exist
    assert not any(d["event"] == "question_claims_released" for d in _decisions(env))


def test_item13d_once_via_cli(env, capsys):
    nid = db.notify(SID, NotificationKind.DIRECTIVE, "steer left", path=env.db)
    rc = bridge.main(["--db", env.db, "--queue-dir", env.qdir, "daemon", "once", "--json"])
    out = capsys.readouterr().out.strip()
    assert rc == 0
    resp = json.loads(out)
    assert resp["status"] == "ok"
    assert _msg_files(Path(env.qdir) / SID) == [f"msg_{nid}.txt"]
    assert db.unrelayed_notifications(path=env.db) == []


# --- start / status / stop lifecycle ---------------------------------------------


def _cli(args):
    return bridge.main(args)


def test_item13d_daemon_start_status_stop_lifecycle(env, capsys):
    argv_base = ["--db", env.db, "--queue-dir", env.qdir, "daemon"]
    try:
        assert _cli([*argv_base, "status"]) == 0
        out = capsys.readouterr().out.strip()
        assert "stopped" in out

        assert _cli([*argv_base, "start", "--interval", "60"]) == 0
        out = capsys.readouterr().out.strip()
        assert "daemon started" in out

        # wait for the detached child to take daemon.lock
        deadline = time.time() + 10
        while time.time() < deadline and not daemon.daemon_running(env.sdir):
            time.sleep(0.1)
        assert daemon.daemon_running(env.sdir)

        # a second start refuses (flock held by the child)
        rc = _cli([*argv_base, "start", "--json"])
        resp = json.loads(capsys.readouterr().out.strip())
        assert rc == 4
        assert resp["status"] == "running"

        assert _cli([*argv_base, "status", "--json"]) == 0
        resp = json.loads(capsys.readouterr().out.strip())
        assert resp["status"] == "running"

        assert _cli([*argv_base, "stop"]) == 0
        out = capsys.readouterr().out.strip()
        assert "daemon stopped" in out
        assert not daemon.daemon_running(env.sdir)
    finally:
        # never leak a daemon out of a failed test
        if daemon.daemon_running(env.sdir):
            pid = daemon.daemon_pid(env.sdir)
            if pid is not None:
                try:
                    os.kill(pid, 15)
                except OSError:
                    pass
            deadline = time.time() + 5
            while time.time() < deadline and daemon.daemon_running(env.sdir):
                time.sleep(0.1)


def test_item13d_daemon_stop_when_not_running(env, capsys):
    rc = bridge.main(["--db", env.db, "daemon", "stop"])
    out = capsys.readouterr().out.strip()
    assert rc == 0
    assert "not running" in out


def test_item13d_notify_send_env_override(env, monkeypatch, tmp_path):
    # SESSIONS_BRIDGE_NOTIFY_SEND points at a recording script
    rec = tmp_path / "notify.log"
    script = tmp_path / "fake-notify.sh"
    script.write_text(f'#!/bin/sh\necho "$@" >> "{rec}"\n')
    script.chmod(0o755)
    monkeypatch.setenv("SESSIONS_BRIDGE_NOTIFY_SEND", str(script))

    res = db.register("orc", "/repo/ctl", role="orchestrator", path=env.db)
    assert res.accepted
    _stale_heartbeat(env.db, "orc")
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    assert rec.exists()
    line = rec.read_text()
    assert "ORCHESTRATOR DEAD" in line
    assert "orc" in line


# --- item_14: schedule materialization inside the pass -------------------------


def test_item14_daemon_materializes_due_schedule(env):
    now = int(time.time())
    sid = db.add_schedule(SID, "nightly sweep", 60, now - 1, path=env.db)
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK

    task = db.get_live_task(SID, path=env.db)
    assert task is not None
    assert task["status"] == TaskStatus.QUEUED.value
    assert task["task_text"] == "nightly sweep"
    # the dispatch notification was relayed to the fs mailbox in the
    # same pass (the schedule pass runs before the relay)
    nid = db.unrelayed_notifications(path=env.db)
    assert nid == []
    mdir = Path(env.qdir) / SID
    files = _msg_files(mdir)
    assert len(files) == 1
    text = (mdir / files[0]).read_text()
    assert "kind: task_dispatch" in text
    assert f"task_id: {task['id']}" in text
    assert text.rstrip().endswith("nightly sweep")
    # the grid advanced and the fire is in the decision log
    row = _one(
        env.db,
        "SELECT * FROM schedules WHERE id=?",
        (sid,),
    )
    assert row["last_run_at"] is not None
    assert row["next_run_at"] > now
    fired = [d for d in _decisions(env) if d["event"] == "schedule_fired"]
    assert len(fired) == 1
    assert fired[0]["sid"] == SID
    assert f"schedule={sid}" in fired[0]["detail"]
    assert f"task_id={task['id']}" in fired[0]["detail"]
    # a quiet pass fires nothing
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    assert len([d for d in _decisions(env) if d["event"] == "schedule_fired"]) == 1
    assert len(db.list_tasks(path=env.db)) == 1


def test_item14_not_due_and_disabled_schedules_skipped(env):
    now = int(time.time())
    future = db.add_schedule(SID, "later", 60, now + 3600, path=env.db)
    off = db.add_schedule(SID, "never", 60, now - 1, path=env.db)
    assert db.disable_schedule(off, path=env.db)
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    assert db.list_tasks(path=env.db) == []
    assert not any(d["event"] == "schedule_fired" for d in _decisions(env))
    # grid anchors untouched
    for sid_, anchor in ((future, now + 3600), (off, now - 1)):
        row = _one(env.db, "SELECT * FROM schedules WHERE id=?", (sid_,))
        assert row["next_run_at"] == anchor


def test_item14_busy_session_defers_the_fire(env):
    now = int(time.time())
    db.add_schedule(SID, "scheduled work", 60, now - 1, path=env.db)
    # the session already holds a live task - the materialize rolls
    # back and the pass logs the deferral instead of dropping the fire
    tid = db.enqueue(SID, "busy", path=env.db)
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    assert len(db.list_tasks(path=env.db)) == 1
    deferred = [d for d in _decisions(env) if d["event"] == "schedule_deferred"]
    assert len(deferred) == 1
    # once the slot frees the next pass fires it
    _exec(env.db, "UPDATE tasks SET status='done' WHERE id=?", (tid,))
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    task = db.get_live_task(SID, path=env.db)
    assert task["task_text"] == "scheduled work"


def test_item14_one_bad_schedule_does_not_sink_the_pass(env, monkeypatch):
    now = int(time.time())
    bad = db.add_schedule("s-bad", "boom", 60, now - 1, path=env.db)
    good = db.add_schedule(SID, "fine", 60, now - 1, path=env.db)
    real = db.materialize_schedule

    def flaky(schedule_id, now_arg=None, *, path=None):
        if schedule_id == bad:
            raise RuntimeError("simulated materialize failure")
        return real(schedule_id, now_arg, path=path)

    monkeypatch.setattr(db, "materialize_schedule", flaky)
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    errors = [d for d in _decisions(env) if d["event"] == "schedule_error"]
    assert len(errors) == 1
    assert errors[0]["sid"] == "s-bad"
    assert f"schedule={bad}" in errors[0]["detail"]
    # the sibling schedule still fired and its task is queued
    task = db.get_live_task(SID, path=env.db)
    assert task is not None and task["task_text"] == "fine"
    fired = [d for d in _decisions(env) if d["event"] == "schedule_fired"]
    assert any(f"schedule={good}" in d["detail"] for d in fired)


# --- item_17: managed liveness coherence ---------------------------------------


def test_item17_gwt1_managed_worker_stale_hb_not_swept(env):
    """[ITEM-17.GWT-1] managed worker + stale hb -> idle, not dead.

    Between turns a managed drain-mode session is stale-hb by design;
    the sweep must leave it (and its live task) alone.
    """
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    tid = db.enqueue(SID, "do the thing", path=env.db)
    assert db.claim_queued(tid, "acp-1", Transport.ACP_PUSH, path=env.db)
    assert db.mark_running(tid, "acp-1", path=env.db)
    _stale_heartbeat(env.db, SID)

    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK

    session = db.get_session(SID, path=env.db)
    assert session["status"] == SessionStatus.ACTIVE.value
    assert db.get_task(tid, path=env.db)["status"] == TaskStatus.RUNNING.value
    assert not any(d["event"] == "worker_dead" for d in _decisions(env))


def test_item17_gwt2_unmanaged_stale_hb_still_dead(env):
    """[ITEM-17.GWT-2] regression guard: unmanaged sweep unchanged."""
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted  # default mode: unmanaged
    _stale_heartbeat(env.db, SID)
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    assert db.get_session(SID, path=env.db)["status"] == SessionStatus.DEAD.value


def test_item17_gwt3_managed_orchestrator_stale_hb_no_notify(env):
    """[ITEM-17.GWT-3] managed orchestrator idle -> no DEAD flip."""
    res = db.register("orc", "/repo/ctl", role="orchestrator", path=env.db)
    assert res.accepted
    db.set_session_mode("orc", SessionMode.MANAGED, path=env.db)
    _stale_heartbeat(env.db, "orc")

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert env.sent == []
    assert db.get_session("orc", path=env.db)["status"] == SessionStatus.ACTIVE.value


# --- item_19: dispatch attribution ---------------------------------------------


def test_item19_msg_headers_name_executing_session(env):
    """[ITEM-19.GWT-3] the delivered note names the executing session."""
    nid = db.notify(SID, NotificationKind.TASK_DISPATCH, "do the thing", task_id=5, path=env.db)
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    text = (Path(env.qdir) / SID / f"msg_{nid}.txt").read_text()
    head = text.split("\n\n", 1)[0]
    assert f"session_id: {SID}" in head.splitlines()
    assert "task_id: 5" in head.splitlines()


def test_item19_question_answer_names_answerer(env):
    """[ITEM-19.GWT-4] question_answer deliveries carry from: <answerer>."""
    db.register("orch-1", "/repo/ctl", role="orchestrator", path=env.db)
    res = db.ask("worker-1", 7, "which file?", path=env.db)
    assert res.accepted
    db.claim_question(res.question_id, "orch-1", path=env.db)
    db.answer_question(res.question_id, "db.py", claimant="orch-1", path=env.db)
    # the CLI answer path writes the QUESTION_ANSWER outbox row
    db.notify(
        "worker-1",
        NotificationKind.QUESTION_ANSWER,
        "db.py",
        task_id=7,
        question_id=res.question_id,
        path=env.db,
    )
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    rows = [p for p in (Path(env.qdir) / "worker-1").iterdir() if p.name.startswith("msg_")]
    assert rows
    head = rows[0].read_text().split("\n\n", 1)[0]
    assert "from: orch-1" in head.splitlines()


def test_item19_ask_directive_names_asker(env):
    """[ITEM-19.GWT-5] worker-ask directives carry from: <asking session>."""
    db.register("orch-1", "/repo/ctl", role="orchestrator", path=env.db)
    res = db.ask("worker-1", 9, "status?", path=env.db)
    assert res.accepted
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    rows = [p for p in (Path(env.qdir) / "orch-1").iterdir() if p.name.startswith("msg_")]
    assert rows
    head = rows[0].read_text().split("\n\n", 1)[0]
    assert "from: worker-1" in head.splitlines()


def test_item19_source_lookup_failure_still_delivers(env, monkeypatch):
    """a broken question lookup must not break the relay - the
    delivery lands without a from: header instead of aborting."""
    nid = db.notify(
        SID,
        NotificationKind.QUESTION_ANSWER,
        "answer text",
        task_id=7,
        question_id=3,
        path=env.db,
    )
    monkeypatch.setattr(
        daemon.db,
        "get_question",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("db gone")),
    )
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    text = (Path(env.qdir) / SID / f"msg_{nid}.txt").read_text()
    assert "answer text" in text
    assert "from:" not in text.split("\n\n", 1)[0]


def test_item19_header_newlines_sanitized(env):
    """a newline in a header field must not forge extra headers."""
    db.notify(
        "evil\nfrom: forged",
        NotificationKind.DISARM,
        "body",
        path=env.db,
    )
    row = db.unrelayed_notifications(path=env.db)[0]
    head = daemon.relay_payload(row).split("\n\n", 1)[0]
    assert len([ln for ln in head.splitlines() if ln.startswith("from:")]) == 0
    assert "session_id: evil from: forged" in head.splitlines()


# --- item_21: post-inject wake verification + durable streak --------------------


def _managed_with_mail(env, sid=SID):
    """A managed session, stale hb, mail in the fs queue = wake candidate."""
    res = db.register(sid, "/repo/ws", path=env.db)
    assert res.accepted
    db.set_session_mode(sid, SessionMode.MANAGED, path=env.db)
    _stale_heartbeat(env.db, sid)
    _undrained_mail(env, sid, NotificationKind.DIRECTIVE)


def _fake_unconfirmed(env, monkeypatch, statuses):
    """wake_session -> sent_unconfirmed; verify -> scripted statusEnum."""
    monkeypatch.setattr(daemon, "wake_session", lambda *a, **k: "sent_unconfirmed")
    monkeypatch.setattr(
        daemon,
        "verify_sessions",
        lambda sids, **k: {s: statuses.get(s) for s in sids},
    )


def test_item21_gwt1_unconfirmed_then_busy_verifies(env, monkeypatch):
    """[ITEM-21.GWT-1] unconfirmed + verify busy -> confirmed, streak reset."""
    _managed_with_mail(env)
    _fake_unconfirmed(env, monkeypatch, {SID: "working"})

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK

    ws = db.get_wake_state(SID, path=env.db)
    assert ws["last_outcome"] == "verified"
    assert ws["unconfirmed_streak"] == 0
    assert env.sent == []
    assert any(d["event"] == "wake_verified" for d in _decisions(env))


def test_item21_gwt2_unconfirmed_then_idle_grows_streak(env, monkeypatch):
    """[ITEM-21.GWT-2] unconfirmed + verify still idle -> durable streak 1."""
    _managed_with_mail(env)
    _fake_unconfirmed(env, monkeypatch, {SID: "finished"})

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK

    ws = db.get_wake_state(SID, path=env.db)
    assert ws["last_outcome"] == "sent_unconfirmed"
    assert ws["unconfirmed_streak"] == 1
    assert env.sent == []
    assert any(
        d["event"] == "wake_unconfirmed" and "streak=1" in (d.get("detail") or "")
        for d in _decisions(env)
    )


def test_item21_gwt3_streak_threshold_escalates_once(env, monkeypatch):
    """[ITEM-21.GWT-3] streak hits N -> one notify-send, then silent."""
    _managed_with_mail(env)
    _fake_unconfirmed(env, monkeypatch, {SID: "finished"})
    db.bump_wake_streak(SID, path=env.db)  # episode already at 2
    db.bump_wake_streak(SID, path=env.db)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK

    ws = db.get_wake_state(SID, path=env.db)
    assert ws["unconfirmed_streak"] == 3
    assert ws["escalated"] == 1
    assert len(env.sent) == 1
    assert "WAKE UNCONFIRMED" in env.sent[0][0]
    assert SID in env.sent[0][1]

    # the streak keeps growing but the episode stays escalated-once
    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert len(env.sent) == 1
    assert db.get_wake_state(SID, path=env.db)["unconfirmed_streak"] == 4


def test_item21_gwt4_streak_survives_daemon_restart(env, monkeypatch):
    """[ITEM-21.GWT-4] two run() calls = restart boundary; durable row keeps count."""
    _managed_with_mail(env)
    _fake_unconfirmed(env, monkeypatch, {SID: "finished"})

    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK
    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK

    assert db.get_wake_state(SID, path=env.db)["unconfirmed_streak"] == 2


def test_item21_verify_attach_failure_leaves_streak(env, monkeypatch):
    """An unverifiable batch is not unconfirmed: streaks untouched."""
    _managed_with_mail(env)
    monkeypatch.setattr(daemon, "wake_session", lambda *a, **k: "sent_unconfirmed")

    def boom(sids, **k):
        raise daemon.WakeError("wake_host_down", "host gone")

    monkeypatch.setattr(daemon, "verify_sessions", boom)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK

    ws = db.get_wake_state(SID, path=env.db)
    assert ws["last_outcome"] == "sent_unconfirmed"
    assert ws["unconfirmed_streak"] == 0
    assert env.sent == []
    assert any(d["event"] == "wake_verify_failed" for d in _decisions(env))


def test_item21_confirmed_wake_skips_verification(env, monkeypatch):
    """A fully sent wake needs no post-inject verify at all."""
    _managed_with_mail(env)
    monkeypatch.setattr(daemon, "wake_session", lambda *a, **k: "sent")
    calls = []
    monkeypatch.setattr(daemon, "verify_sessions", lambda *a, **k: calls.append(a))

    assert daemon.run(env.db, env.qdir, once=True) == daemon.RUN_OK

    assert calls == []
    ws = db.get_wake_state(SID, path=env.db)
    assert ws["last_outcome"] == "sent"
    assert ws["unconfirmed_streak"] == 0


def test_item21_fresh_ack_proves_turn_despite_idle_status(env, monkeypatch):
    """Review fix: a fast drain turn closes before verify - the ack
    watermark is the turn evidence statusEnum can no longer show."""
    _managed_with_mail(env)
    db.ack_notifications(SID, path=env.db)  # watermark just moved
    # a fresh unacked mail keeps the session wake-worthy
    nid2 = db.notify(SID, NotificationKind.DIRECTIVE, "new mail", path=env.db)
    db.mark_relayed(nid2, path=env.db)
    _fake_unconfirmed(env, monkeypatch, {SID: "finished"})

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK

    ws = db.get_wake_state(SID, path=env.db)
    assert ws["last_outcome"] == "verified"
    assert ws["unconfirmed_streak"] == 0
    assert env.sent == []


def test_item21_absent_on_verify_feeds_no_session_streak(env, monkeypatch):
    """Review fix: absent on the re-check counts toward dead-marking,
    same as a failed session/list lookup."""
    _managed_with_mail(env)
    monkeypatch.setattr(
        daemon,
        "verify_sessions",
        lambda sids, **k: dict.fromkeys(sids, daemon.SESSION_ABSENT),
    )
    policy = daemon.WakePolicy()
    for _ in range(daemon._NO_SESSION_DEAD_STREAK):
        daemon._verify_unconfirmed_wakes(env.db, env.sdir, [SID], policy, None)
    assert db.get_session(SID, path=env.db)["status"] == SessionStatus.DEAD.value


def test_item21_absent_with_fresh_ack_is_live_not_dead(env, monkeypatch):
    """A fresh ack proves the turn ran even when session/list raced and
    reported the session absent - no_session must not accumulate
    against a provably-live session (item_21 r3)."""
    _managed_with_mail(env)
    db.ack_notifications(SID, path=env.db)
    monkeypatch.setattr(
        daemon,
        "verify_sessions",
        lambda sids, **k: dict.fromkeys(sids, daemon.SESSION_ABSENT),
    )
    policy = daemon.WakePolicy()
    for _ in range(daemon._NO_SESSION_DEAD_STREAK + 1):
        daemon._verify_unconfirmed_wakes(env.db, env.sdir, [SID], policy, None)
    ws = db.get_wake_state(SID, path=env.db)
    assert ws["last_outcome"] == "verified"
    assert db.get_session(SID, path=env.db)["status"] == SessionStatus.ACTIVE.value


def test_item21_statusless_listing_is_unconfirmed_not_absent(env, monkeypatch):
    """A session listed without statusEnum meta grows the unconfirmed
    streak but must never feed no_session dead-marking (item_21 r2)."""
    _managed_with_mail(env)
    monkeypatch.setattr(daemon, "verify_sessions", lambda sids, **k: dict.fromkeys(sids))
    policy = daemon.WakePolicy()
    daemon._verify_unconfirmed_wakes(env.db, env.sdir, [SID], policy, None)
    assert db.get_wake_state(SID, path=env.db)["unconfirmed_streak"] == 1
    assert db.get_session(SID, path=env.db)["status"] == SessionStatus.ACTIVE.value


def test_item21_alive_outcome_resets_escalated_episode(env, monkeypatch):
    """A verified wake after an escalated streak opens a fresh episode."""
    _managed_with_mail(env)
    db.record_wake_outcome(SID, "sent_unconfirmed", path=env.db)
    for _ in range(3):
        db.bump_wake_streak(SID, path=env.db)
    db.set_wake_escalated(SID, path=env.db)
    _fake_unconfirmed(env, monkeypatch, {SID: "working"})

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK

    ws = db.get_wake_state(SID, path=env.db)
    assert ws["last_outcome"] == "verified"
    assert ws["unconfirmed_streak"] == 0
    assert ws["escalated"] == 0


# --- item_102c: actionable mail wakes fresh-heartbeat DRAIN sessions --------------


def _wake_calls(env, monkeypatch):
    """Record wake_session call sids; verify is a no-op (outcome 'sent').

    find_lockfile is stubbed too: probe-only candidates (item_162) need
    a lockfile to reach verify_sessions, and relying on whatever
    acp-host lock the dev machine happens to hold made tests
    environment-dependent (CI red on PR #69). The fake host_key keeps
    the cooldown/breaker accounting exercised without a real host.
    """
    calls = []
    monkeypatch.setattr(daemon, "wake_session", lambda sid, *a, **k: calls.append(sid) or "sent")
    monkeypatch.setattr(daemon, "verify_sessions", lambda sids, **k: {})
    monkeypatch.setattr(daemon, "find_lockfile", lambda *a, **k: {"_path": "fake-host"})
    return calls


def _managed_fresh(env, sid=SID):
    """Managed session with a FRESH heartbeat - the DRAIN-idle-at-prompt
    case: the session is alive at the UI but opens no turn on its own."""
    res = db.register(sid, "/repo/ws", path=env.db)
    assert res.accepted
    db.set_session_mode(sid, SessionMode.MANAGED, path=env.db)
    db.heartbeat(sid, path=env.db)


def _unacked(env, sid, kind):
    """Unacked relayed mail in its true on-disk shape (task #184): a
    relayed row without its msg file is consumed-but-unacked - ack
    lag, not mail - so every unacked fixture writes the file."""
    return _undrained_mail(env, sid, kind)


def _undrained_mail(env, sid, kind, age_seconds=None):
    """Registered unacked mail whose relayed file really sits in the
    mailbox - the post-#143 undrained shape: the row alone is not a
    stall, consumed-but-unacked is not undrained.  The file carries
    real relay_payload headers so tick files are recognisable as
    ticks.  ``age_seconds`` backdates the file mtime - on-disk mail
    ages by mtime, not by created_at."""
    nid = db.notify(sid, kind, "body", path=env.db)
    db.mark_relayed(nid, path=env.db)
    row = _one(env.db, "SELECT * FROM notifications WHERE id=?", (nid,))
    path = os.path.join(env.qdir, sid, f"msg_{nid}.txt")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        fh.write(daemon.relay_payload(dict(row)))
    if age_seconds is not None:
        old = int(time.time()) - age_seconds
        os.utime(path, (old, old))
    return nid


def test_item102c_gwt1_directive_wakes_fresh_hb_drain(env, monkeypatch):
    """[ITEM-102C.GWT-1] fresh-hb managed session + unacked directive ->
    wake candidate (the live DRAIN defect)."""
    _managed_fresh(env)
    _unacked(env, SID, NotificationKind.DIRECTIVE)
    calls = _wake_calls(env, monkeypatch)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == [SID]


def test_item102c_gwt2_tick_alone_does_not_wake_fresh_hb(env, monkeypatch):
    """[ITEM-102C.GWT-2] fresh-hb session + only a heartbeat_tick ->
    not a candidate: the tick is a liveness poke, not actionable mail."""
    _managed_fresh(env)
    _unacked(env, SID, NotificationKind.HEARTBEAT_TICK)
    calls = _wake_calls(env, monkeypatch)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == []


def test_item102c_gwt3_live_task_alone_keeps_fresh_hb_calm(env, monkeypatch):
    """[ITEM-102C.GWT-3] fresh-hb session + a live task but no mail ->
    not a candidate: the task path is a stale-hb liveness probe only."""
    _managed_fresh(env)
    db.enqueue(SID, "do the thing", path=env.db)
    calls = _wake_calls(env, monkeypatch)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == []


def test_item102c_gwt4_tick_file_alone_does_not_wake_stale_hb(env, monkeypatch):
    """[ITEM-102C.GWT-4, retargeted by task #184] stale-hb session +
    only a heartbeat_tick -> not a candidate: the tick is a liveness
    poke, and a stale session that cannot act on it must not be
    woken just to drain a poke file."""
    res = db.register(SID, "/repo/ws", path=env.db)
    assert res.accepted
    db.set_session_mode(SID, SessionMode.MANAGED, path=env.db)
    _stale_heartbeat(env.db, SID)
    _unacked(env, SID, NotificationKind.HEARTBEAT_TICK)
    calls = _wake_calls(env, monkeypatch)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == []


def test_item102c_gwt5_consumed_actionable_mail_does_not_rewake(env, monkeypatch):
    """[ITEM-102C.GWT-5, retargeted by task #184] fresh-hb session whose
    actionable mail is consumed AND acked -> not a candidate: the
    re-wake loop boundary. (Ack without consumption still wakes -
    the file on disk is undrained mail.)"""
    _managed_fresh(env)
    nid = _unacked(env, SID, NotificationKind.DIRECTIVE)
    db.ack_notifications(SID, path=env.db)
    os.remove(os.path.join(env.qdir, SID, f"msg_{nid}.txt"))
    calls = _wake_calls(env, monkeypatch)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == []


# --- item_122: incident/stall hardening ------------------------------------


def test_item122_gwt6_rpc_error_post_list_resets_no_session_streak(env, monkeypatch):
    """[ITEM-122.GWT-6] rpc_error reaching _wake_pass is post-list by
    construction (wake_session remaps pre-list RPC failures to
    attach_failed), so it proves the session answered to
    session/list: the no_session streak must reset, not accumulate
    toward dead-marking."""
    _managed_with_mail(env)
    monkeypatch.setattr(daemon, "find_lockfile", lambda: None)

    def no_session(*a, **k):
        raise daemon.WakeError("no_session", "not in session/list")

    monkeypatch.setattr(daemon, "wake_session", no_session)
    now = int(time.time())
    policy = daemon.WakePolicy(cooldown_seconds=0)
    daemon._wake_pass(env.db, env.sdir, now, policy)
    daemon._wake_pass(env.db, env.sdir, now, policy)  # streak 2 of 3
    assert policy._no_session_streak[SID] == 2

    def rpc_fail(*a, **k):
        raise daemon.WakeError("rpc_error", "session/prompt rejected mid-turn")

    monkeypatch.setattr(daemon, "wake_session", rpc_fail)
    daemon._wake_pass(env.db, env.sdir, now, policy)
    assert SID not in policy._no_session_streak

    # back to no_session: the dead streak rebuilds from zero
    monkeypatch.setattr(daemon, "wake_session", no_session)
    daemon._wake_pass(env.db, env.sdir, now, policy)
    daemon._wake_pass(env.db, env.sdir, now, policy)
    assert db.get_session(SID, path=env.db)["status"] == SessionStatus.ACTIVE.value
    daemon._wake_pass(env.db, env.sdir, now, policy)
    assert db.get_session(SID, path=env.db)["status"] == SessionStatus.DEAD.value


def test_item122_gwt7_restart_mid_streak_starts_fresh(env, monkeypatch):
    """[ITEM-122.GWT-7] WakePolicy is process-local: a daemon restart
    mid no_session streak drops the accumulated count - the session
    is dead-marked only after a full fresh streak, never on
    carried-over misses (the PR #34 missing test)."""
    _managed_with_mail(env)
    monkeypatch.setattr(daemon, "find_lockfile", lambda: None)

    def gone(*a, **k):
        raise daemon.WakeError("no_session", "not in session/list")

    monkeypatch.setattr(daemon, "wake_session", gone)
    now = int(time.time())

    p1 = daemon.WakePolicy(cooldown_seconds=0)
    daemon._wake_pass(env.db, env.sdir, now, p1)
    daemon._wake_pass(env.db, env.sdir, now, p1)  # streak 2 of 3
    assert db.get_session(SID, path=env.db)["status"] == SessionStatus.ACTIVE.value

    # restart boundary: a fresh policy = a clean streak ledger
    p2 = daemon.WakePolicy(cooldown_seconds=0)
    daemon._wake_pass(env.db, env.sdir, now, p2)
    daemon._wake_pass(env.db, env.sdir, now, p2)  # fresh streak 2 - alive
    assert db.get_session(SID, path=env.db)["status"] == SessionStatus.ACTIVE.value
    daemon._wake_pass(env.db, env.sdir, now, p2)  # fresh streak 3 - dead
    assert db.get_session(SID, path=env.db)["status"] == SessionStatus.DEAD.value


# --- item_139: parked tasks are not wake triggers; mail-stall escalation -----


def _running_task(env, sid=SID):
    tid = db.enqueue(sid, "work", path=env.db)
    assert db.claim_managed(tid, sid, sid, path=env.db)
    return tid


def _managed_stale(env, sid=SID):
    res = db.register(sid, "/repo/ws", path=env.db)
    assert res.accepted
    db.set_session_mode(sid, SessionMode.MANAGED, path=env.db)
    _stale_heartbeat(env.db, sid)


def test_item139_gwt1_awaiting_user_task_not_wake_candidate(env, monkeypatch):
    """[ITEM-139.GWT-1] stale-hb managed session whose only live task is
    awaiting_user -> no task leg wake: the task is parked on an
    out-of-band answer that arrives as actionable mail with its own
    leg (the orchestrator task 128 defect)."""
    _managed_stale(env)
    tid = _running_task(env)
    assert db.set_awaiting_user(tid, SID, "waiting on review", path=env.db)
    calls = _wake_calls(env, monkeypatch)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == []


def test_item139_gwt2_fresh_wait_until_suppresses_task_leg(env, monkeypatch):
    """[ITEM-139.GWT-2] `bridge wait` stamps wait_until = lease expiry:
    a running task mid external-wait is not probe-worthy until the
    stamp decays, then it is again (the dead-worker boundary). Since
    item_162 the task leg produces a liveness probe, never a drain
    prompt - wake_session must not be called in any phase."""
    _managed_stale(env)
    tid = _running_task(env)
    calls = _wake_calls(env, monkeypatch)

    # running without a wait stamp: probe-worthy, but never woken
    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == []
    assert any(d["event"] == "wake_probe_absent" for d in _decisions(env))

    calls.clear()
    assert db.wait_task(tid, SID, "waiting on CI", path=env.db)
    task = db.get_task(tid, path=env.db)
    assert task["wait_until"] == task["lease_expires_at"]
    before = len(_decisions(env))
    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == []
    assert not any(d["event"].startswith("wake_probe") for d in _decisions(env)[before:])

    # stamp expired -> probe-worthy again on its own, no clear needed
    _exec(env.db, "UPDATE tasks SET wait_until=? WHERE id=?", (int(time.time()) - 1, tid))
    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == []
    assert any(d["event"] == "wake_probe_absent" for d in _decisions(env))


def test_item139_gwt3_mail_stall_escalates_once_per_episode(env, monkeypatch):
    """[ITEM-139.GWT-3] actionable mail older than the stall threshold
    -> exactly one directive into the orchestrator mailbox naming the
    session and age; draining opens a fresh episode."""
    _managed_stale(env)
    res = db.register("orc", "/repo/orc", role="orchestrator", path=env.db)
    assert res.accepted
    db.set_session_mode("orc", SessionMode.MANAGED, path=env.db)
    nid = _undrained_mail(env, SID, NotificationKind.DIRECTIVE, age_seconds=7200)
    _wake_calls(env, monkeypatch)  # keep the wake path quiet

    now = int(time.time())
    age = daemon._mail_stall_pass(env.db, env.sdir, now, env.notifier)
    assert age >= 7199
    orch_mail = _one(
        env.db,
        "SELECT body FROM notifications WHERE session_id='orc' AND kind='directive'",
    )
    assert orch_mail is not None and SID in orch_mail["body"] and "120m" in orch_mail["body"]
    assert (
        _one(env.db, "SELECT mail_escalated_at FROM wake_state WHERE session_id=?", (SID,))[
            "mail_escalated_at"
        ]
        is not None
    )

    # second pass same episode: dedup, no second directive
    daemon._mail_stall_pass(env.db, env.sdir, now + 60, env.notifier)
    assert _one(env.db, "SELECT COUNT(*) c FROM notifications WHERE session_id='orc'")["c"] == 1

    # drain (rm + ack, the duty order) -> episode stamp cleared ->
    # next stall re-escalates
    os.remove(os.path.join(env.qdir, SID, f"msg_{nid}.txt"))
    db.ack_notifications(SID, path=env.db)
    assert daemon._mail_stall_pass(env.db, env.sdir, now + 120, env.notifier) == 0
    assert (
        _one(env.db, "SELECT mail_escalated_at FROM wake_state WHERE session_id=?", (SID,))[
            "mail_escalated_at"
        ]
        is None
    )
    _undrained_mail(env, SID, NotificationKind.DIRECTIVE, age_seconds=7200)
    daemon._mail_stall_pass(env.db, env.sdir, now + 180, env.notifier)
    assert _one(env.db, "SELECT COUNT(*) c FROM notifications WHERE session_id='orc'")["c"] == 2


def test_item139_gwt4_tick_only_mail_never_stalls(env, monkeypatch):
    """[ITEM-139.GWT-4] an ancient unacked heartbeat_tick is not
    actionable mail: no escalation, metric stays 0."""
    _managed_stale(env)
    _undrained_mail(env, SID, NotificationKind.HEARTBEAT_TICK, age_seconds=7200)
    _wake_calls(env, monkeypatch)

    assert daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier) == 0
    assert env.sent == []
    assert _one(env.db, "SELECT COUNT(*) c FROM notifications WHERE kind='directive'")["c"] == 0


def test_item139_gwt5_orchestrator_self_stall_uses_notify_send(env, monkeypatch):
    """[ITEM-139.GWT-5] the active orchestrator's own undrained mail
    cannot go to its own mailbox (it would stall there too) -> the
    escalation lands on notify_send instead."""
    res = db.register("orc", "/repo/orc", role="orchestrator", path=env.db)
    assert res.accepted
    db.set_session_mode("orc", SessionMode.MANAGED, path=env.db)
    _undrained_mail(env, "orc", NotificationKind.DIRECTIVE, age_seconds=7200)
    _wake_calls(env, monkeypatch)

    daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier)

    assert env.sent and "orc" in env.sent[0][1]
    assert _one(env.db, "SELECT COUNT(*) c FROM notifications WHERE kind='directive'")["c"] == 1


def test_item139_gwt6_no_orchestrator_falls_back_to_notify_send(env, monkeypatch):
    """[ITEM-139.GWT-6] no active orchestrator holder -> the stall
    notice has nowhere to route but the OS notification."""
    _managed_stale(env)
    _undrained_mail(env, SID, NotificationKind.DIRECTIVE, age_seconds=7200)
    _wake_calls(env, monkeypatch)

    daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier)

    assert env.sent and SID in env.sent[0][1]


def test_item139_gwt8_episode_rearms_on_new_mail_after_partial_drain(env, monkeypatch):
    """[ITEM-139.GWT-8] review fix: a session whose escalated mail
    drained but NEWER mail keeps stalling must re-escalate - re-arming
    only on a fully-empty mailbox would give a chronically stalled
    session one escalation ever."""
    _managed_stale(env)
    res = db.register("orc", "/repo/orc", role="orchestrator", path=env.db)
    assert res.accepted
    nid = _undrained_mail(env, SID, NotificationKind.DIRECTIVE, age_seconds=7200)
    _wake_calls(env, monkeypatch)

    now = int(time.time())
    daemon._mail_stall_pass(env.db, env.sdir, now, env.notifier)
    assert _one(env.db, "SELECT COUNT(*) c FROM notifications WHERE session_id='orc'")["c"] == 1

    # the escalated mail drains (rm + ack), a NEW directive arrives
    # (delivered after the stamp) and itself stalls past the threshold
    os.remove(os.path.join(env.qdir, SID, f"msg_{nid}.txt"))
    db.ack_notifications(SID, path=env.db)
    _exec(
        env.db,
        "UPDATE wake_state SET mail_escalated_at=? WHERE session_id=?",
        (int(time.time()) - 3600, SID),
    )
    # file mtime is newer than the (backdated) stamp but older than
    # the stall threshold -> a fresh episode, not the same stall
    _undrained_mail(env, SID, NotificationKind.DIRECTIVE, age_seconds=2000)
    daemon._mail_stall_pass(env.db, env.sdir, now + 30, env.notifier)
    assert _one(env.db, "SELECT COUNT(*) c FROM notifications WHERE session_id='orc'")["c"] == 2


def test_item139_gwt9_no_target_defers_without_consuming_episode(env, monkeypatch):
    """[ITEM-139.GWT-9] review fix: no orchestrator and no notify_send
    -> mail_stall_deferred, the stamp is NOT consumed - the escalation
    still fires the first pass a target exists."""
    _managed_stale(env)
    _undrained_mail(env, SID, NotificationKind.DIRECTIVE, age_seconds=7200)
    _wake_calls(env, monkeypatch)

    daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), None)
    assert (
        _one(env.db, "SELECT COUNT(*) c FROM wake_state WHERE mail_escalated_at IS NOT NULL")["c"]
        == 0
    )
    assert [d["event"] for d in _decisions(env)] == ["mail_stall_deferred"]

    # notify_send appears -> the episode escalates on the next pass
    daemon._mail_stall_pass(env.db, env.sdir, int(time.time()) + 5, env.notifier)
    assert env.sent and SID in env.sent[0][1]


# --- item_139 amendment: file-level pending mail + `bridge directive` ---------


def _foreign_msg(env, sid, nid=99, body="foreign directive"):
    """Hand-written mailbox file - the msg_003 shape: a writer that
    bypassed the notifications registry entirely."""
    mdir = os.path.join(env.qdir, sid)
    os.makedirs(mdir, exist_ok=True)
    path = os.path.join(mdir, f"msg_{nid}.txt")
    with open(path, "w") as fh:
        fh.write(body)
    return path


def test_item139_gwt10_foreign_msg_file_wakes_fresh_hb(env, monkeypatch):
    """[ITEM-139.GWT-10] fresh-hb managed session + an unconsumed
    msg_*.txt and NO notification rows -> wake candidate: file
    presence is mail, whoever wrote it (the msg_003 incident). Runs
    with queue_dir=None - the default resolution path the review
    caught disabled: env.qdir IS <dir of env.db>/queue."""
    _managed_fresh(env)
    _foreign_msg(env, SID)
    calls = _wake_calls(env, monkeypatch)

    assert daemon.run(env.db, None, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == [SID]


def test_item139_gwt11_bridge_directive_relays_and_wakes(env, monkeypatch, capsys):
    """[ITEM-139.GWT-11] `bridge directive` writes a registered
    notification: the next daemon pass relays it into the fs mailbox
    and the actionable-mail leg wakes the recipient."""
    _managed_fresh(env)
    calls = _wake_calls(env, monkeypatch)

    assert bridge.main(["--db", env.db, "directive", SID, "steer now"]) == 0
    nid = _one(
        env.db,
        "SELECT id FROM notifications WHERE session_id=? AND kind='directive'",
        (SID,),
    )
    assert nid is not None

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == [SID]
    assert os.path.exists(os.path.join(env.qdir, SID, f"msg_{nid['id']}.txt"))

    # an unknown sid is refused, not queued into the void
    capsys.readouterr()
    assert bridge.main(["--db", env.db, "directive", "ghost-sid", "steer"]) != 0
    assert (
        _one(env.db, "SELECT COUNT(*) c FROM notifications WHERE session_id='ghost-sid'")["c"] == 0
    )

    # a dead session is refused too - undrainable mail would only
    # grow a phantom stall episode
    db.set_session_status(SID, "dead", path=env.db)
    assert bridge.main(["--db", env.db, "directive", SID, "steer"]) != 0
    assert (
        _one(
            env.db,
            "SELECT COUNT(*) c FROM notifications WHERE session_id=? AND kind='directive'",
            (SID,),
        )["c"]
        == 1
    )  # only the earlier successful directive


def test_item172_gwt1_bridge_notice_relays_and_wakes(env, monkeypatch):
    """[ITEM-172.GWT-1] `bridge notice` writes a registered NOTICE
    notification: the next daemon pass relays it into the fs mailbox
    and the actionable-mail leg wakes the recipient - same delivery
    machinery as directive, distinct kind."""
    _managed_fresh(env)
    calls = _wake_calls(env, monkeypatch)

    assert bridge.main(["--db", env.db, "notice", SID, "report ready"]) == 0
    nid = _one(
        env.db,
        "SELECT id FROM notifications WHERE session_id=? AND kind='notice'",
        (SID,),
    )
    assert nid is not None
    # no directive row is created by the notice path
    assert (
        _one(
            env.db,
            "SELECT COUNT(*) c FROM notifications WHERE session_id=? AND kind='directive'",
            (SID,),
        )["c"]
        == 0
    )

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == [SID]
    assert os.path.exists(os.path.join(env.qdir, SID, f"msg_{nid['id']}.txt"))

    # empty text and non-active targets are refused like directive
    assert bridge.main(["--db", env.db, "notice", SID, "   "]) != 0
    assert bridge.main(["--db", env.db, "notice", "ghost-sid", "hi"]) != 0
    db.set_session_status(SID, "dead", path=env.db)
    assert bridge.main(["--db", env.db, "notice", SID, "hi"]) != 0
    assert (
        _one(
            env.db,
            "SELECT COUNT(*) c FROM notifications WHERE session_id=? AND kind='notice'",
            (SID,),
        )["c"]
        == 1
    )


def test_item139_gwt12_fs_only_pending_escalates_stall(env, monkeypatch):
    """[ITEM-139.GWT-12] a mailbox file from a foreign writer older
    than the stall threshold escalates by its mtime even though the
    notifications table knows nothing about it."""
    _managed_stale(env)
    path = _foreign_msg(env, SID)
    old = int(time.time()) - 7200
    os.utime(path, (old, old))
    _wake_calls(env, monkeypatch)

    daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier, env.qdir)

    assert env.sent and SID in env.sent[0][1] and "unregistered mailbox file" in env.sent[0][1]


def test_item139_gwt13_fs_pending_respects_managed_active_gate(env, monkeypatch):
    """[ITEM-139.GWT-13] the file leg honours the same gate as every
    other: an unmanaged session's mailbox dir and a dead managed
    session's dir are not wake candidates."""
    res = db.register("unmanaged", "/repo/ws", path=env.db)
    assert res.accepted
    _foreign_msg(env, "unmanaged")
    _managed_stale(env)
    _foreign_msg(env, SID)
    db.set_session_status(SID, "dead", path=env.db)
    calls = _wake_calls(env, monkeypatch)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == []


def test_item139_gwt14_real_tick_file_does_not_wake(env, monkeypatch):
    """[ITEM-139.GWT-14] a genuine relayed heartbeat_tick file in the
    mailbox is a poke, not pending mail: file leg stays silent even
    though a msg_*.txt exists on disk."""
    _managed_fresh(env)
    db.notify(SID, NotificationKind.HEARTBEAT_TICK, "tick", path=env.db)
    calls = _wake_calls(env, monkeypatch)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == []
    # the relayed tick file is really on disk and still must not wake
    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == []


def test_item139_gwt15_foreign_file_colliding_with_tick_nid_wakes(env, monkeypatch):
    """[ITEM-139.GWT-15] a foreign msg_<N>.txt whose N collides with a
    heartbeat_tick row id is still actionable: provenance comes from
    the relay headers, not the bare id."""
    _managed_fresh(env)
    tick_nid = db.notify(SID, NotificationKind.HEARTBEAT_TICK, "tick", path=env.db)
    db.mark_relayed(tick_nid, path=env.db)
    _foreign_msg(env, SID, nid=tick_nid)  # no relay headers inside
    calls = _wake_calls(env, monkeypatch)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == [SID]


def test_item139_gwt16_consumed_but_unacked_is_not_a_stall(env, monkeypatch):
    """[ITEM-139.GWT-16] task #143 false positive: a session mid-drain
    deletes the msg file and only acks at the turn boundary - the
    unacked row alone must NOT escalate. Undrained means still on
    disk."""
    _managed_stale(env)
    res = db.register("orc", "/repo/orc", role="orchestrator", path=env.db)
    assert res.accepted
    _unacked(env, SID, NotificationKind.DIRECTIVE)  # row, NO file
    _exec(env.db, "UPDATE notifications SET created_at=?", (int(time.time()) - 7200,))
    _wake_calls(env, monkeypatch)

    assert daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier) == 0
    assert _one(env.db, "SELECT COUNT(*) c FROM notifications WHERE session_id='orc'")["c"] == 0

    # same, but the file is still on disk -> genuine stall escalates
    _undrained_mail(env, SID, NotificationKind.DIRECTIVE, age_seconds=7200)
    assert daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier) >= 7199
    assert _one(env.db, "SELECT COUNT(*) c FROM notifications WHERE session_id='orc'")["c"] == 1


def test_item139_gwt17_dead_session_still_escalates_on_disk_mail(env, monkeypatch):
    """[ITEM-139.GWT-17] review fix: a dead session's mailbox is not in
    the managed+active pending scan, but its still-on-disk mail is
    undrainable - the ad-hoc _dir_pending path keeps it escalating."""
    _managed_stale(env)
    res = db.register("orc", "/repo/orc", role="orchestrator", path=env.db)
    assert res.accepted
    _undrained_mail(env, SID, NotificationKind.DIRECTIVE, age_seconds=7200)
    db.set_session_status(SID, "dead", path=env.db)
    _wake_calls(env, monkeypatch)

    assert daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier) >= 7199
    orch_mail = _one(
        env.db,
        "SELECT body FROM notifications WHERE session_id='orc' AND kind='directive'",
    )
    assert orch_mail is not None and SID in orch_mail["body"]

    # but a dead session whose mail was consumed before it died is NOT
    # a stall - the file-presence rule holds on the ad-hoc path too
    _managed_stale(env, sid="gone")
    _unacked(env, "gone", NotificationKind.DIRECTIVE)  # row, no file
    db.set_session_status("gone", "dead", path=env.db)
    daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier)
    con = db.connect(env.db)
    try:
        bodies = [
            r["body"]
            for r in con.execute(
                "SELECT body FROM notifications WHERE session_id='orc' AND kind='directive'"
            ).fetchall()
        ]
    finally:
        con.close()
    assert not any("gone" in b for b in bodies)


# --- item_174: busy-grace - a verifiably mid-turn session is never escalated ---


def _stall_env(env):
    """Managed session + orchestrator recipient for stall directives."""
    _managed_stale(env)
    res = db.register("orc", "/repo/orc", role="orchestrator", path=env.db)
    assert res.accepted
    db.set_session_mode("orc", SessionMode.MANAGED, path=env.db)


def _stall_verify(env, monkeypatch, statuses):
    """Stub the session/list the busy-grace check performs."""
    monkeypatch.setattr(daemon, "find_lockfile", lambda *a, **k: {"_path": "fake-host"})
    monkeypatch.setattr(daemon, "verify_sessions", lambda sids, **k: dict(statuses))


def _orch_stall_count(env):
    return _one(
        env.db,
        "SELECT COUNT(*) c FROM notifications WHERE session_id='orc' AND kind='directive'",
    )["c"]


def test_item174_gwt1_busy_session_defers_stall_escalation(env, monkeypatch):
    """[ITEM-174.GWT-1] undrained mail past the stall threshold on a
    session verifiably mid-turn (statusEnum 'working') -> NO
    escalation, mail_stall_deferred_busy decision, episode stamp not
    consumed - the mahogany-dryosaurus false positive."""
    _stall_env(env)
    _undrained_mail(env, SID, NotificationKind.DIRECTIVE, age_seconds=3600)
    _stall_verify(env, monkeypatch, {SID: "working"})

    assert daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier) >= 3599
    assert _orch_stall_count(env) == 0
    assert [d["event"] for d in _decisions(env)] == ["mail_stall_deferred_busy"]
    ws = _one(env.db, "SELECT mail_escalated_at FROM wake_state WHERE session_id=?", (SID,))
    assert ws is None or ws["mail_escalated_at"] is None


def test_item174_gwt2_idle_after_busy_escalates(env, monkeypatch):
    """[ITEM-174.GWT-2] busy buys grace, not amnesty: the same undrained
    mail escalates once the session goes idle - the deferral consumed
    no stamp."""
    _stall_env(env)
    _undrained_mail(env, SID, NotificationKind.DIRECTIVE, age_seconds=3600)
    monkeypatch.setattr(daemon, "find_lockfile", lambda *a, **k: {"_path": "fake-host"})

    monkeypatch.setattr(daemon, "verify_sessions", lambda sids, **k: {SID: "working"})
    assert daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier) >= 3599
    assert _orch_stall_count(env) == 0

    monkeypatch.setattr(daemon, "verify_sessions", lambda sids, **k: {SID: "idle"})
    assert daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier) >= 3599
    assert _orch_stall_count(env) == 1
    orch_mail = _one(
        env.db,
        "SELECT body FROM notifications WHERE session_id='orc' AND kind='directive'",
    )
    assert SID in orch_mail["body"]


def test_item174_gwt3_statusless_and_absent_still_escalate(env, monkeypatch):
    """[ITEM-174.GWT-3] busy must be VERIFIED: a statusless entry or
    SESSION_ABSENT is not busy evidence - escalation stays
    conservative for dead/absent/unverifiable sessions."""
    _stall_env(env)
    _managed_stale(env, sid="gone")
    _undrained_mail(env, SID, NotificationKind.DIRECTIVE, age_seconds=7200)
    _undrained_mail(env, "gone", NotificationKind.DIRECTIVE, age_seconds=7200)
    _stall_verify(
        env,
        monkeypatch,
        {SID: None, "gone": daemon.SESSION_ABSENT},
    )

    assert daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier) >= 7199
    assert _orch_stall_count(env) == 2


def test_item174_gwt4_verify_failure_still_escalates(env, monkeypatch):
    """[ITEM-174.GWT-4] a failed session/list attach cannot prove busy -
    the stall escalates exactly as before (unverifiable is not busy)."""
    _stall_env(env)
    _undrained_mail(env, SID, NotificationKind.DIRECTIVE, age_seconds=7200)
    monkeypatch.setattr(daemon, "find_lockfile", lambda *a, **k: {"_path": "fake-host"})

    def _down(sids, **k):
        raise daemon.WakeError("host_down", "attach failed")

    monkeypatch.setattr(daemon, "verify_sessions", _down)
    assert daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier) >= 7199
    assert _orch_stall_count(env) == 1
    assert any(d["event"] == "mail_stall_verify_failed" for d in _decisions(env))


def test_item174_gwt5_no_lockfile_still_escalates(env, monkeypatch):
    """[ITEM-174.GWT-5] no acp-host lockfile -> busy cannot be verified -
    escalation is not suppressed."""
    _stall_env(env)
    _undrained_mail(env, SID, NotificationKind.DIRECTIVE, age_seconds=7200)
    monkeypatch.setattr(daemon, "find_lockfile", lambda *a, **k: None)

    assert daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier) >= 7199
    assert _orch_stall_count(env) == 1


def test_item174_gwt6_busy_check_covers_orch_itself(env, monkeypatch):
    """[ITEM-174.GWT-6] a busy orchestrator's own stall defers too -
    notify_send is not fired while it is verifiably mid-turn."""
    _stall_env(env)
    _undrained_mail(env, "orc", NotificationKind.DIRECTIVE, age_seconds=3600)
    _stall_verify(env, monkeypatch, {"orc": "working"})

    daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier)
    assert env.sent == []
    assert [d["event"] for d in _decisions(env)] == ["mail_stall_deferred_busy"]


def test_item174_gwt7_busy_grace_has_a_ceiling(env, monkeypatch):
    """[ITEM-174.GWT-7] review fix: a session still 'working' with mail
    older than _BUSY_GRACE_SECONDS is wedged, not lagging - the stall
    escalates; the grace ceiling is the last operator signal for a
    shape the heartbeat sweep never reaches."""
    _stall_env(env)
    _undrained_mail(
        env, SID, NotificationKind.DIRECTIVE, age_seconds=daemon._BUSY_GRACE_SECONDS + 600
    )
    _stall_verify(env, monkeypatch, {SID: "working"})

    assert daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier) >= 7199
    assert _orch_stall_count(env) == 1


def test_item180_gwt8_halted_session_mail_is_parked(env, monkeypatch):
    """[ITEM-180.GWT-8] a halted session's undrained mail is parked by
    design - mail_stall_deferred_halted, no escalation, no stamp,
    unbounded in time (unlike the busy grace ceiling)."""
    _stall_env(env)
    _undrained_mail(
        env, SID, NotificationKind.DIRECTIVE, age_seconds=daemon._BUSY_GRACE_SECONDS + 600
    )
    db.set_session_halted(SID, True, path=env.db)
    _wake_calls(env, monkeypatch)

    assert daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier) >= 7199
    assert _orch_stall_count(env) == 0
    assert [d["event"] for d in _decisions(env)] == ["mail_stall_deferred_halted"]

    # resume unparks it - the same mail escalates on the next pass
    db.set_session_halted(SID, False, path=env.db)
    assert daemon._mail_stall_pass(env.db, env.sdir, int(time.time()), env.notifier) >= 7199
    assert _orch_stall_count(env) == 1


# --- item_184: file-only wake trigger (consumed-but-unacked noise) ------------


def test_item184_gwt1_consumed_but_unacked_never_wakes(env, monkeypatch):
    """[ITEM-184.GWT-1] the mahogany-dryosaurus loop: a relayed
    notification whose file was drained but never acked kept
    qualifying the DB mail legs forever (~5min wake cadence on a
    physically empty mailbox). File truth is the only wake leg:
    no file, no wake - the row is ack lag, not mail."""
    _managed_fresh(env)
    nid = _unacked(env, SID, NotificationKind.TASK_DISPATCH)
    os.remove(os.path.join(env.qdir, SID, f"msg_{nid}.txt"))  # drained, not acked
    calls = _wake_calls(env, monkeypatch)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == []


def test_item184_gwt2_unacked_row_with_file_still_wakes(env, monkeypatch):
    """[ITEM-184.GWT-2] regression pair: the same unacked row DOES wake
    while its file is still on disk - the leg dropped was the
    row-only path, not real undrained mail."""
    _managed_fresh(env)
    _unacked(env, SID, NotificationKind.DIRECTIVE)
    calls = _wake_calls(env, monkeypatch)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == [SID]


def test_item184_gwt3_stale_hb_mail_on_disk_still_wakes(env, monkeypatch):
    """[ITEM-184.GWT-3] the item_16/17 stale-heartbeat contract survives
    the leg drop: undrained mail on a sleeping session still wakes -
    only the heartbeat-freshness gate is gone, not the mail check."""
    _managed_with_mail(env)
    calls = _wake_calls(env, monkeypatch)

    assert daemon.run(env.db, env.qdir, once=True, notify_send=env.notifier) == daemon.RUN_OK
    assert calls == [SID]


# --- item_184: systemd --user unit installer ----------------------------------

INSTALL_UNIT_SH = Path(__file__).resolve().parents[3] / "install_daemon_unit.sh"


def _install_unit(tmp_path, *args):
    """Run the installer with a fake systemctl + isolated unit dir; no
    real user manager is touched."""
    import subprocess

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    bridge_bin = bin_dir / "bridge"
    bridge_bin.write_text("#!/bin/sh\n")
    bridge_bin.chmod(0o755)
    log = tmp_path / "systemctl.log"
    systemctl = bin_dir / "systemctl"
    systemctl.write_text(f'#!/bin/sh\necho "$@" >> "{log}"\n')
    systemctl.chmod(0o755)
    loginctl = bin_dir / "loginctl"
    loginctl.write_text("#!/bin/sh\nexit 0\n")
    loginctl.chmod(0o755)
    env = dict(
        os.environ,
        SESSIONS_BRIDGE_BIN=str(bridge_bin),
        SESSIONS_BRIDGE_UNIT_DIR=str(tmp_path / "units"),
        SESSIONS_BRIDGE_SYSTEMCTL=str(systemctl),
        SESSIONS_BRIDGE_LOGINCTL=str(loginctl),
    )
    res = subprocess.run(
        ["bash", str(INSTALL_UNIT_SH), *args],
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )
    return res, tmp_path / "units" / "sessions-bridge.service", log


def test_item184_gwt6_unit_print_renders_paths(tmp_path):
    """[ITEM-184.GWT-6] --print renders the template with the resolved
    bridge binary and daemon flags - the file a user manager would
    get, without touching systemd."""
    res, unit_file, _ = _install_unit(tmp_path, "--print", "--db", "/data/state.db")
    assert res.returncode == 0, res.stderr
    bridge_bin = str(tmp_path / "bin" / "bridge")
    # --db is a TOP-LEVEL flag: it precedes the daemon subcommand
    assert f'ExecStart={bridge_bin} --db "/data/state.db" daemon run' in res.stdout
    assert "Restart=on-failure" in res.stdout
    assert "WantedBy=default.target" in res.stdout
    assert not unit_file.exists()  # print writes nothing


def test_item184_gwt7_unit_install_enable_start(tmp_path):
    """[ITEM-184.GWT-7] a full install writes the unit under
    ~/.config/systemd/user, reloads the manager, enables and starts
    the service - the auto-start-on-boot path (linger is attempted
    and only warns on failure)."""
    res, unit_file, log = _install_unit(tmp_path)
    assert res.returncode == 0, res.stderr
    text = unit_file.read_text()
    bridge_bin = str(tmp_path / "bin" / "bridge")
    assert f"ExecStart={bridge_bin} daemon run" in text
    calls = log.read_text().splitlines()
    assert calls[0] == "--user daemon-reload"
    assert "--user enable sessions-bridge.service" in calls
    assert "--user start sessions-bridge.service" in calls


def test_item184_gwt8_unit_no_start_no_enable(tmp_path):
    """[ITEM-184.GWT-8] --no-start/--no-enable install the unit file
    only - the operator's staging path."""
    res, unit_file, log = _install_unit(tmp_path, "--no-start", "--no-enable")
    assert res.returncode == 0, res.stderr
    assert unit_file.exists()
    calls = log.read_text().splitlines()
    assert calls == ["--user daemon-reload"]


def test_item184_gwt9_unit_missing_bridge_binary_refused(tmp_path):
    """[ITEM-184.GWT-9] no bridge binary -> refuse before touching
    systemd (no half-installed unit)."""
    import subprocess

    env = dict(
        os.environ,
        SESSIONS_BRIDGE_BIN=str(tmp_path / "no-such-bridge"),
        SESSIONS_BRIDGE_UNIT_DIR=str(tmp_path / "units"),
        SESSIONS_BRIDGE_SYSTEMCTL="/nonexistent/systemctl",
    )
    res = subprocess.run(
        ["bash", str(INSTALL_UNIT_SH)],
        capture_output=True,
        text=True,
        timeout=30,
        env=env,
    )
    assert res.returncode != 0
    assert "bridge binary not" in res.stderr
    assert not (tmp_path / "units").exists()

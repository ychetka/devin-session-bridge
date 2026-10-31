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
    NotificationKind,
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
    ts = int(hb.read_text().strip())
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

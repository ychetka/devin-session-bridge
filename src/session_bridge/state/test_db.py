"""Persistence and concurrency tests for the state.db layer (item_04)."""

import json
import multiprocessing
import os
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from session_bridge.state import db
from session_bridge.state.contract import (
    BUSY_TIMEOUT_MS,
    CLAIM_TIMEOUT_SECONDS,
    MAX_ATTEMPTS,
    MAX_QUESTIONS_PER_TASK,
    PENDING_TIMEOUT_SECONDS,
    AuditEventKind,
    NotificationKind,
    SessionMode,
    SessionRole,
    SessionStatus,
    TaskStatus,
    Transport,
)
from session_bridge.state.verdict import (
    Killed,
    KillNotFound,
    ReleaseNotFound,
    ReleaseRefusedOwner,
    ReleaseRemoved,
    ReserveConflict,
    ReserveGranted,
    UnregisterDeleted,
    UnregisterNotFound,
    UnregisterRefusedFreshHeartbeat,
    UnregisterRefusedLiveTask,
)


@pytest.fixture()
def db_path(tmp_path):
    p = str(tmp_path / "state.db")
    db.init(p)
    return p


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


def _expire(path, task_id):
    """Force the lease of a running task into the past."""
    _exec(
        path,
        "UPDATE tasks SET lease_expires_at=? WHERE id=?",
        (int(time.time()) - 1, task_id),
    )


def _enqueue_running(path, session_id, owner="w1"):
    tid = db.enqueue(session_id, "do the thing", path=path)
    assert tid is not None
    assert db.claim_queued(tid, owner, Transport.ACP_PUSH, path=path)
    assert db.mark_running(tid, owner, path=path)
    return tid


def test_item04_gwt1_duplicate_enqueue_rejected_on_live_task(db_path):
    tid = db.enqueue("s1", "first", path=db_path)
    assert tid is not None
    # queued blocks
    assert db.enqueue("s1", "second", path=db_path) is None
    # dispatch_decided blocks
    assert db.claim_queued(tid, "w", "acp-push", path=db_path)
    assert db.enqueue("s1", "second", path=db_path) is None
    # running blocks
    assert db.mark_running(tid, "w", path=db_path)
    assert db.enqueue("s1", "second", path=db_path) is None
    # awaiting_user is live (blocks) but carries no lease
    _exec(
        db_path,
        "UPDATE tasks SET status='awaiting_user', lease_owner=NULL,"
        " lease_expires_at=NULL WHERE id=?",
        (tid,),
    )
    assert db.enqueue("s1", "second", path=db_path) is None
    # a terminal status releases the session slot
    _exec(db_path, "UPDATE tasks SET status='done' WHERE id=?", (tid,))
    tid2 = db.enqueue("s1", "second", path=db_path)
    assert tid2 is not None
    _exec(db_path, "UPDATE tasks SET status='failed' WHERE id=?", (tid2,))
    assert db.enqueue("s1", "third", path=db_path) is not None
    # an unrelated session was never affected
    assert db.get_live_task("s2", path=db_path) is None
    assert db.enqueue("s2", "other", path=db_path) is not None


def test_item04_gwt2_concurrent_claim_single_winner(db_path):
    tid = db.enqueue("s1", "work", path=db_path)
    owners = [f"w{i}" for i in range(8)]
    with ThreadPoolExecutor(max_workers=len(owners)) as ex:
        results = list(
            ex.map(
                lambda o: db.claim_queued(tid, o, "acp-push", path=db_path),
                owners,
            )
        )
    assert results.count(True) == 1
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "dispatch_decided"
    assert row["lease_owner"] == owners[results.index(True)]
    assert row["transport"] == "acp-push"
    # a late claim on an already-claimed task always loses
    assert db.claim_queued(tid, "late", "hook-queue", path=db_path) is False


def test_item04_gwt3_expired_lease_requeues_then_fails(db_path):
    tid = _enqueue_running(db_path, "s1", "w1")
    # lease still valid -> untouched by recovery
    assert db.recover_stale(path=db_path) == []

    _expire(db_path, tid)
    assert db.recover_stale(path=db_path) == [tid]
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "queued"
    assert row["attempt"] == 1
    assert row["lease_owner"] is None
    assert row["lease_expires_at"] is None
    # recovery is idempotent on a second pass
    assert db.recover_stale(path=db_path) == []

    # reclaim, run and expire again at attempt == MAX-1 -> terminal failed
    assert db.claim_queued(tid, "w2", "hook-queue", path=db_path)
    assert db.mark_running(tid, "w2", path=db_path)
    _expire(db_path, tid)
    assert db.recover_stale(path=db_path) == [tid]
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "failed"
    assert row["attempt"] == MAX_ATTEMPTS
    # the failure is flaggable for notify exactly once in the dedup window
    assert db.mark_notified(tid, path=db_path) is True
    assert db.mark_notified(tid, path=db_path) is False


def test_item04_gwt4_recover_vs_claim_race_consistent(db_path):
    for i in range(20):
        tid = _enqueue_running(db_path, f"s{i}", "w_old")
        _expire(db_path, tid)
        outcome = {}

        def do_recover(outcome=outcome):
            outcome["recovered"] = db.recover_stale(path=db_path)

        def do_claim(tid=tid, outcome=outcome):
            # claim only wins once the requeue has landed
            for _ in range(1000):
                if db.claim_queued(tid, "w_new", "acp-push", path=db_path):
                    outcome["claimed"] = True
                    return
                time.sleep(0.001)
            outcome["claimed"] = False

        with ThreadPoolExecutor(2) as ex:
            f_recover = ex.submit(do_recover)
            f_claim = ex.submit(do_claim)
            f_recover.result()
            f_claim.result()

        row = db.get_task(tid, path=db_path)
        # invariant: the expired owner can never be running again
        assert not (row["status"] == "running" and row["lease_owner"] == "w_old")
        assert row["attempt"] == 1
        if outcome["claimed"]:
            assert row["status"] == "dispatch_decided"
            assert row["lease_owner"] == "w_new"
        else:
            assert row["status"] == "queued"
            assert row["lease_owner"] is None
        # the stale worker loses every owner-CAS operation
        assert db.mark_running(tid, "w_old", path=db_path) is False
        assert db.renew_lease(tid, "w_old", path=db_path) is False
        assert db.finish_task(tid, "w_old", True, path=db_path) is False


def _writer_proc(path, prefix, count):
    for i in range(count):
        tid = db.enqueue(f"{prefix}{i}", "payload", path=path)
        if tid is None:
            raise RuntimeError(f"enqueue lost: {prefix}{i}")
        if i % 5 == 0:
            db.upsert_session(f"{prefix}{i}", cwd="/repo", path=path)


def test_item04_gwt5_parallel_writers_lose_nothing(tmp_path):
    path = str(tmp_path / "state.db")
    db.init(path)
    per_writer = 25
    ctx = multiprocessing.get_context("fork")
    procs = [ctx.Process(target=_writer_proc, args=(path, f"w{i}-s", per_writer)) for i in range(2)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(30)
        assert p.exitcode == 0
    count = _one(path, "SELECT COUNT(*) AS c FROM tasks")["c"]
    assert count == 2 * per_writer
    sessions = _one(path, "SELECT COUNT(*) AS c FROM sessions")["c"]
    assert sessions == 2 * (per_writer // 5)


def test_item04_gwt6_missing_db_initialized_corrupt_rejected(tmp_path):
    # missing file (and missing parent dir) -> init creates the schema
    path = str(tmp_path / "sub" / "state.db")
    db.init(path)
    version = _one(path, "PRAGMA user_version")[0]
    assert version == 3
    journal = _one(path, "PRAGMA journal_mode")[0]
    assert journal == "wal"
    busy = _one(path, "PRAGMA busy_timeout")[0]
    assert busy == BUSY_TIMEOUT_MS
    assert db.enqueue("s1", "x", path=path) is not None

    # a corrupt file -> controlled BridgeDBError, never a bare traceback
    corrupt = tmp_path / "corrupt.db"
    corrupt.write_bytes(b"definitely not a sqlite database")
    with pytest.raises(db.BridgeDBError):
        db.connect(str(corrupt))
    with pytest.raises(db.BridgeDBError):
        db.init(str(corrupt))


def test_item04_gwt7_stale_finish_does_not_overwrite(db_path):
    tid = _enqueue_running(db_path, "s1", "w_old")
    _expire(db_path, tid)
    assert db.recover_stale(path=db_path) == [tid]
    assert db.claim_queued(tid, "w_new", "hook-queue", path=db_path)
    assert db.mark_running(tid, "w_new", path=db_path)

    # the zombie worker's report must not clobber the live run
    assert db.finish_task(tid, "w_old", True, last_assistant_message="stale", path=db_path) is False
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "running"
    assert row["lease_owner"] == "w_new"
    assert row["last_assistant_message"] is None

    # the current owner finishes for real
    assert db.finish_task(
        tid,
        "w_new",
        False,
        last_assistant_message="boom",
        note="crash",
        path=db_path,
    )
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "failed"
    assert row["last_assistant_message"] == "boom"
    assert row["note"] == "crash"
    assert row["lease_owner"] is None
    assert row["lease_expires_at"] is None


def test_item04_gwt8_queue_paused_session_skipped(db_path):
    db.upsert_session("paused", queue_paused=True, path=db_path)
    t_paused = db.enqueue("paused", "x", path=db_path)
    t_free = db.enqueue("free", "x", path=db_path)

    # paused session's task is skipped; the unpaused sibling is claimable
    assert db.claim_queued(t_paused, "w", "acp-push", path=db_path) is False
    assert db.claim_queued(t_free, "w", "acp-push", path=db_path) is True

    # a session without a sessions row is claimable too
    t_norow = db.enqueue("norow", "x", path=db_path)
    assert db.claim_queued(t_norow, "w", "acp-push", path=db_path) is True

    # unpausing makes the skipped task claimable again
    db.upsert_session("paused", queue_paused=False, path=db_path)
    assert db.claim_queued(t_paused, "w", "acp-push", path=db_path) is True


def test_item04_lease_lifecycle(db_path):
    tid = db.enqueue("s1", "x", path=db_path)
    # mark_running requires the dispatch_decided claim first
    assert db.mark_running(tid, "w1", path=db_path) is False
    assert db.claim_queued(tid, "w1", "acp-push", path=db_path)
    # a different owner cannot take over the claim
    assert db.mark_running(tid, "w2", path=db_path) is False
    assert db.mark_running(tid, "w1", path=db_path)

    row = db.get_task(tid, path=db_path)
    assert row["status"] == "running"
    assert row["lease_expires_at"] is not None
    assert abs(row["lease_expires_at"] - (time.time() + db.LEASE_SECONDS)) < 5
    expires_before = row["lease_expires_at"]

    # renew_lease is CAS-guarded on the owner
    assert db.renew_lease(tid, "w2", path=db_path) is False
    assert db.renew_lease(tid, "w1", path=db_path) is True
    row = db.get_task(tid, path=db_path)
    assert row["lease_expires_at"] >= expires_before
    assert row["lease_renewed_at"] is not None

    # release: wrong owner loses the CAS, right owner clears the lease
    assert db.release_lease(tid, "w2", path=db_path) is False
    assert db.release_lease(tid, "w1", path=db_path) is True
    row = db.get_task(tid, path=db_path)
    assert row["lease_owner"] is None
    assert row["lease_expires_at"] is None


def test_item04_finish_done_clears_lease_and_frees_slot(db_path):
    tid = _enqueue_running(db_path, "s1", "w1")
    assert db.finish_task(tid, "w1", True, last_assistant_message="all done", path=db_path)
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "done"
    assert row["lease_owner"] is None
    assert row["lease_expires_at"] is None
    assert row["last_assistant_message"] == "all done"
    # terminal status frees the session for a new task
    assert db.enqueue("s1", "next", path=db_path) is not None


def test_item04_retry_failed_returns_to_queued(db_path):
    tid = _enqueue_running(db_path, "s1", "w1")
    assert db.finish_task(tid, "w1", False, note="boom", path=db_path)
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "failed"
    _exec(db_path, "UPDATE tasks SET attempt=? WHERE id=?", (MAX_ATTEMPTS, tid))

    # manual retry is the only exit from failed: queued, attempt reset
    assert db.retry(tid, note="operator retry", path=db_path) is True
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "queued"
    assert row["attempt"] == 0
    assert row["lease_owner"] is None
    assert row["lease_expires_at"] is None
    assert row["lease_renewed_at"] is None
    assert row["note"] == "operator retry"

    # retry is defined only for failed tasks
    assert db.retry(tid, path=db_path) is False
    # the retried task re-enters the lifecycle
    assert db.claim_queued(tid, "w2", "acp-push", path=db_path) is True


def test_item04_enqueue_requires_payload(db_path):
    with pytest.raises(ValueError):
        db.enqueue("s1", path=db_path)
    tid = db.enqueue("s1", task_file="/tmp/task.md", path=db_path)
    assert tid is not None


def test_item04_upsert_get_session_round_trip(db_path):
    assert db.get_session("s1", path=db_path) is None
    db.upsert_session("s1", cwd="/repo", title="t", path=db_path)
    row = db.get_session("s1", path=db_path)
    assert row["cwd"] == "/repo"
    assert row["title"] == "t"
    assert row["queue_paused"] == 0
    assert row["park_cycles_left"] == 5

    # upsert patches only the given fields
    db.upsert_session("s1", queue_paused=True, note="n", path=db_path)
    row = db.get_session("s1", path=db_path)
    assert row["queue_paused"] == 1
    assert row["note"] == "n"
    assert row["cwd"] == "/repo"

    with pytest.raises(ValueError):
        db.upsert_session("s1", bogus_field=1, path=db_path)


def test_item04_sessions_db_lookup(tmp_path, monkeypatch):
    # missing file -> graceful None
    missing = tmp_path / "no-such.db"
    monkeypatch.setattr(db, "DEVIN_SESSIONS_DB", str(missing))
    assert db.sessions_db_lookup("s1") is None

    # a real sessions.db -> row dict
    real = tmp_path / "sessions.db"
    con = sqlite3.connect(str(real))
    con.execute("CREATE TABLE sessions(id TEXT PRIMARY KEY, working_directory TEXT, title TEXT)")
    con.execute(
        "INSERT INTO sessions VALUES(?,?,?)",
        ("s1", "/repo", "my session"),
    )
    con.commit()
    con.close()
    monkeypatch.setattr(db, "DEVIN_SESSIONS_DB", str(real))
    res = db.sessions_db_lookup("s1")
    assert res is not None
    assert res["working_directory"] == "/repo"
    assert res["title"] == "my session"
    assert db.sessions_db_lookup("absent") is None

    # an unreadable/corrupt file -> graceful None, never writes
    bad = tmp_path / "bad.db"
    bad.write_bytes(b"junk")
    monkeypatch.setattr(db, "DEVIN_SESSIONS_DB", str(bad))
    assert db.sessions_db_lookup("s1") is None
    assert bad.read_bytes() == b"junk"


def _named_exe(tmp_path, name):
    """A long-running executable reachable under ``name`` via symlink,
    so the spawned process's argv[0] basename is exactly ``name``.

    Links to the Python interpreter rather than ``sleep``: multicall
    coreutils builds (rust coreutils on ubuntu-26.04 runners) refuse to
    exec under a foreign argv[0] name. Python does not care.
    """
    exe = tmp_path / name
    exe.symlink_to(sys.executable)
    return str(exe)


_PY_SLEEP = ["-c", "import time; time.sleep(30)"]


def _spawn_cmdline(argv, marker):
    """Spawn a process and wait until /proc/<pid>/cmdline shows the
    final exec'd argv (fork/exec race: cmdline is the parent's first)."""
    proc = subprocess.Popen(argv)
    for _ in range(400):
        try:
            with open(f"/proc/{proc.pid}/cmdline", "rb") as f:
                if marker in f.read():
                    return proc
        except OSError:
            break
        if proc.poll() is not None:
            break
        time.sleep(0.005)
    proc.kill()
    proc.wait()
    raise RuntimeError(f"process never showed cmdline marker {marker!r}")


def test_item04_is_locked(tmp_path, monkeypatch):
    locks = tmp_path / "locks"
    locks.mkdir()
    monkeypatch.setattr(db, "SESSION_LOCKS_DIR", str(locks))

    # no lock file
    assert db.is_locked("nolock") == (False, None)

    # lock file with a dead pid -> free, but pid is reported
    dead_pid = 2**22 - 1  # almost certainly not a live pid
    (locks / "dead.lock").write_text(str(dead_pid))
    assert db.is_locked("dead") == (False, dead_pid)

    # live process whose argv[0] basename is 'devin' -> locked
    proc = _spawn_cmdline([_named_exe(tmp_path, "devin"), *_PY_SLEEP], b"devin")
    (locks / "live.lock").write_text(str(proc.pid))
    try:
        assert db.is_locked("live") == (True, proc.pid)
    finally:
        proc.terminate()
        proc.wait()
    # holder died -> lock released even though the file remains
    assert db.is_locked("live") == (False, proc.pid)

    # 'devin' anywhere else in the cmdline is not enough: a basename
    # that merely mentions devin must not lock (PID-reuse guard)
    lookalike = _named_exe(tmp_path, "devin-checkout")
    proc2 = _spawn_cmdline([lookalike, *_PY_SLEEP], b"devin-checkout")
    (locks / "lookalike.lock").write_text(str(proc2.pid))
    try:
        assert db.is_locked("lookalike") == (False, proc2.pid)
    finally:
        proc2.terminate()
        proc2.wait()

    # live pid reused by a non-devin process -> PID-reuse guard
    proc3 = _spawn_cmdline(["sleep", "30"], b"sleep")
    (locks / "alien.lock").write_text(str(proc3.pid))
    try:
        assert db.is_locked("alien") == (False, proc3.pid)
    finally:
        proc3.terminate()
        proc3.wait()


def test_item04_mark_notified_dedup_window(db_path):
    tid = db.enqueue("s1", "x", path=db_path)
    assert db.mark_notified(tid, path=db_path) is True
    # inside the ~300s window a second flag is refused
    assert db.mark_notified(tid, path=db_path) is False
    # outside the window the flag can be set again
    _exec(
        db_path,
        "UPDATE tasks SET last_notified_at=? WHERE id=?",
        (int(time.time()) - 400, tid),
    )
    assert db.mark_notified(tid, path=db_path) is True


def test_item04_gwt9_set_awaiting_user_clears_lease_stays_live(db_path):
    tid = _enqueue_running(db_path, "s1", "w1")
    # a stale owner cannot park somebody else's run
    assert db.set_awaiting_user(tid, "w2", path=db_path) is False
    assert db.set_awaiting_user(tid, "w1", note="parked", path=db_path)

    row = db.get_task(tid, path=db_path)
    assert row["status"] == "awaiting_user"
    assert row["note"] == "parked"
    # awaiting_user is live but carries no lease
    assert row["lease_owner"] is None
    assert row["lease_expires_at"] is None
    assert db.enqueue("s1", "blocked", path=db_path) is None
    # lease recovery ignores it - there is no lease to expire
    _exec(db_path, "UPDATE tasks SET lease_expires_at=1 WHERE id=?", (tid,))
    assert db.recover_stale(path=db_path) == []
    _exec(db_path, "UPDATE tasks SET lease_expires_at=NULL WHERE id=?", (tid,))

    # manual unpark is the only way back into the lifecycle
    assert db.unpark(tid, path=db_path) is True
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "queued"
    assert db.claim_queued(tid, "w3", "hook-queue", path=db_path) is True


def test_item04_gwt10_fallback_to_hook_atomic_requeue(db_path):
    tid = db.enqueue("s1", "x", path=db_path)
    assert db.claim_queued(tid, "w1", "acp-push", path=db_path)
    # acp-push refused (session locked): a different owner cannot do it
    assert db.fallback_to_hook(tid, "w2", path=db_path) is False
    assert db.fallback_to_hook(tid, "w1", path=db_path) is True

    row = db.get_task(tid, path=db_path)
    # one statement: queued + hook transport, no half-state
    assert row["status"] == "queued"
    assert row["transport"] == "hook-queue"
    assert row["lease_owner"] is None
    # the task is claimable again through the hook path
    assert db.claim_queued(tid, "w2", "hook-queue", path=db_path) is True
    # fallback only applies to dispatch_decided
    assert db.mark_running(tid, "w2", path=db_path)
    assert db.fallback_to_hook(tid, "w2", path=db_path) is False


def test_item04_gwt11_pause_unpark_cycle(db_path):
    tid = db.enqueue("s1", "x", path=db_path)
    assert db.pause(tid, path=db_path) is True
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "paused"
    # paused is not live: claim skips it and enqueue is not blocked
    assert db.claim_queued(tid, "w", "acp-push", path=db_path) is False
    live = db.enqueue("s1", "other live task", path=db_path)
    assert live is not None

    # unpark cannot resurrect while another live task holds the slot
    assert db.unpark(tid, path=db_path) is False
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "paused"  # untouched, not crashed

    # once the slot frees, unpark lands the task back in queued
    assert db.clear(live, path=db_path) is True
    assert db.unpark(tid, path=db_path) is True
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "queued"

    # pause/unpark only apply to their declared manual edges
    assert db.claim_queued(tid, "w", "acp-push", path=db_path)
    assert db.pause(tid, path=db_path) is False  # dispatch_decided, not queued
    assert db.unpark(tid, path=db_path) is False  # not parked/awaiting


def test_item04_gwt12_clear_queued_only(db_path):
    tid = db.enqueue("s1", "x", path=db_path)
    assert db.clear(tid, path=db_path) is True
    assert db.get_task(tid, path=db_path) is None

    # clearing is refused beyond queued
    tid2 = _enqueue_running(db_path, "s1", "w1")
    assert db.clear(tid2, path=db_path) is False
    row = db.get_task(tid2, path=db_path)
    assert row["status"] == "running"


def test_item04_gwt13_session_pause_helper(db_path):
    # pausing a session that has no row yet creates it
    db.session_pause("s1", True, path=db_path)
    row = db.get_session("s1", path=db_path)
    assert row["queue_paused"] == 1

    tid = db.enqueue("s1", "x", path=db_path)
    assert db.claim_queued(tid, "w", "acp-push", path=db_path) is False

    db.session_pause("s1", False, path=db_path)
    row = db.get_session("s1", path=db_path)
    assert row["queue_paused"] == 0
    assert db.claim_queued(tid, "w", "acp-push", path=db_path) is True


def test_item04_release_lease_expected_status_guard(db_path):
    tid = db.enqueue("s1", "x", path=db_path)
    assert db.claim_queued(tid, "w1", "acp-push", path=db_path)
    assert db.mark_running(tid, "w1", path=db_path)
    # a stale cleanup guarded on dispatch_decided must not strip the
    # lease of a task that is already running
    assert db.release_lease(tid, "w1", expected_status="dispatch_decided", path=db_path) is False
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "running"
    assert row["lease_owner"] == "w1"
    assert row["lease_expires_at"] is not None

    # the release still lands while the task is in the expected status
    tid2 = db.enqueue("s2", "x", path=db_path)
    assert db.claim_queued(tid2, "w1", "acp-push", path=db_path)
    assert db.release_lease(tid2, "w1", expected_status="dispatch_decided", path=db_path) is True
    assert db.get_task(tid2, path=db_path)["lease_owner"] is None


def test_item04_decrement_park_cycles_atomic(db_path):
    # a missing row is created with fresh_budget - 1
    assert db.decrement_park_cycles("s1", 5, path=db_path) == 4
    assert db.decrement_park_cycles("s1", 5, path=db_path) == 3
    # clamps at zero and never resurrects the budget
    _exec(
        db_path,
        "UPDATE sessions SET park_cycles_left=1 WHERE session_id='s1'",
    )
    assert db.decrement_park_cycles("s1", 5, path=db_path) == 0
    assert db.decrement_park_cycles("s1", 5, path=db_path) == 0
    # a persisted value above the configured budget is clamped down
    _exec(
        db_path,
        "UPDATE sessions SET park_cycles_left=9 WHERE session_id='s1'",
    )
    assert db.decrement_park_cycles("s1", 5, path=db_path) == 4

    # concurrent decrements each consume a distinct cycle - the
    # read-modify-write version could lose updates and double-spend
    db.upsert_session("s2", park_cycles_left=3, path=db_path)
    with ThreadPoolExecutor(max_workers=4) as ex:
        results = sorted(
            ex.map(
                lambda _: db.decrement_park_cycles("s2", 5, path=db_path),
                range(4),
            )
        )
    assert results == [0, 0, 1, 2]
    assert db.get_session("s2", path=db_path)["park_cycles_left"] == 0


def test_item04_list_tasks_limit_applies_with_status(db_path):
    for i in range(5):
        assert db.enqueue(f"s{i}", "x", path=db_path) is not None
    rows = db.list_tasks(status="queued", limit=3, path=db_path)
    assert len(rows) == 3
    # FIFO order is preserved for the dispatch pass
    ids = [r["id"] for r in rows]
    assert ids == sorted(ids)
    # the unfiltered listing stays newest-first
    rows = db.list_tasks(limit=2, path=db_path)
    assert len(rows) == 2
    assert rows[0]["id"] > rows[1]["id"]


def test_item10_gwt3_dispatch_decided_wedge_recovers(db_path):
    # the claim itself carries a lease, so a claimant that dies before
    # the transport is visible to recovery instead of wedging silently
    tid = db.enqueue("s1", "x", path=db_path)
    assert db.claim_queued(tid, "w_dead", "acp-push", path=db_path)
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "dispatch_decided"
    assert row["lease_expires_at"] is not None

    _expire(db_path, tid)
    # the dead owner cannot promote the task anymore: its re-confirm
    # CAS is guarded on the unexpired lease and sees 0 rows
    assert db.mark_running(tid, "w_dead", path=db_path) is False
    assert db.recover_stale(path=db_path) == [tid]
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "queued"
    assert row["attempt"] == 1
    assert row["lease_owner"] is None
    assert row["lease_expires_at"] is None

    # a repeated wedge on the same task exhausts the attempt budget:
    # poison pill to terminal failed, never another requeue
    assert db.claim_queued(tid, "w_dead2", "acp-push", path=db_path)
    _expire(db_path, tid)
    assert db.recover_stale(path=db_path) == [tid]
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "failed"
    assert row["attempt"] == MAX_ATTEMPTS


def test_item10_gwt3_owner_confirm_vs_recover_single_winner(db_path):
    # expired claim lease: recovery CAS wins, the late owner backs off
    tid = db.enqueue("s1", "x", path=db_path)
    assert db.claim_queued(tid, "w1", "acp-push", path=db_path)
    _expire(db_path, tid)
    outcome = {}

    def do_confirm():
        outcome["confirmed"] = db.mark_running(tid, "w1", path=db_path)

    def do_recover():
        outcome["recovered"] = db.recover_stale(path=db_path)

    with ThreadPoolExecutor(2) as ex:
        f_confirm = ex.submit(do_confirm)
        f_recover = ex.submit(do_recover)
        f_confirm.result()
        f_recover.result()
    # exactly one side transitioned the task: the owner lost (expired
    # lease) and recovery requeued it - no double transport possible
    assert outcome["confirmed"] is False
    assert outcome["recovered"] == [tid]
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "queued"
    assert row["lease_owner"] is None

    # unexpired claim lease: the owner CAS wins and recovery sees the
    # fresh lease expiry - it must not touch the task
    assert db.claim_queued(tid, "w2", "acp-push", path=db_path)
    outcome.clear()

    def do_confirm2():
        outcome["confirmed"] = db.mark_running(tid, "w2", path=db_path)

    with ThreadPoolExecutor(2) as ex:
        f_confirm = ex.submit(do_confirm2)
        f_recover = ex.submit(do_recover)
        f_confirm.result()
        f_recover.result()
    assert outcome["confirmed"] is True
    assert outcome["recovered"] == []
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "running"
    assert row["lease_owner"] == "w2"


def test_item10_gwt5_demote_running_to_hook(db_path):
    tid = _enqueue_running(db_path, "s1", "w1")
    # a foreign owner cannot demote somebody else's running task
    assert db.demote_running_to_hook(tid, "w2", path=db_path) is False
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "running"
    assert row["transport"] == "acp-push"

    assert db.demote_running_to_hook(tid, "w1", path=db_path) is True
    row = db.get_task(tid, path=db_path)
    # one statement: queued + hook transport, lease fully cleared
    assert row["status"] == "queued"
    assert row["transport"] == "hook-queue"
    assert row["lease_owner"] is None
    assert row["lease_expires_at"] is None
    assert row["lease_renewed_at"] is None
    # the demoted task is claimable again through the hook path
    assert db.claim_queued(tid, "w2", "hook-queue", path=db_path) is True

    # the CAS applies to running only - dispatch_decided is handled by
    # fallback_to_hook
    tid2 = db.enqueue("s2", "x", path=db_path)
    assert db.claim_queued(tid2, "w1", "acp-push", path=db_path)
    assert db.demote_running_to_hook(tid2, "w1", path=db_path) is False
    assert db.get_task(tid2, path=db_path)["status"] == "dispatch_decided"


def test_item04_trigger_filename_sanitized():
    assert db.trigger_filename("abc-123_OK.9") == "abc-123_OK.9.json"
    # traversal segments and slashes cannot escape the queue dir
    for sid in ("../x", "..", "a/b", "a\\b", "...", ".env"):
        name = db.trigger_filename(sid)
        assert "/" not in name and "\\" not in name
        assert not name.startswith(".")
    assert db.trigger_filename("") == "sid.json"


def test_item13b1_gwt1_orchestrator_cas_singleton(db_path):
    res = db.register("orc1", "/repo", role="orchestrator", path=db_path)
    assert res.accepted is True
    assert res.role == "orchestrator"
    assert res.generation == 1
    assert res.holder is None

    # a second orchestrator is refused while orc1's heartbeat is fresh:
    # no state is written for the rejected aspirant
    res = db.register("orc2", "/repo2", role="orchestrator", path=db_path)
    assert res.accepted is False
    assert res.holder == "orc1"
    assert res.heartbeat_age is not None
    assert 0 <= res.heartbeat_age < db.HEARTBEAT_STALE_SECONDS
    assert res.generation is None
    assert db.get_session("orc2", path=db_path) is None

    # the holder re-registering itself never blocks on its own heartbeat
    res = db.register("orc1", "/repo", role="orchestrator", path=db_path)
    assert res.accepted is True

    # stale heartbeat -> takeover without force; the old holder is demoted
    _exec(
        db_path,
        "UPDATE worker_heartbeats SET last_seen_at=? WHERE session_id=?",
        (int(time.time()) - db.HEARTBEAT_STALE_SECONDS - 1, "orc1"),
    )
    res = db.register("orc2", "/repo2", role="orchestrator", path=db_path)
    assert res.accepted is True
    assert db.get_session("orc2", path=db_path)["role"] == "orchestrator"
    assert db.get_session("orc1", path=db_path)["role"] == "worker"

    # force overrides a live holder
    res = db.register("orc3", "/repo3", role="orchestrator", force=True, path=db_path)
    assert res.accepted is True
    assert db.get_session("orc3", path=db_path)["role"] == "orchestrator"
    assert db.get_session("orc2", path=db_path)["role"] == "worker"

    # an orchestrator row with no heartbeat at all never blocks
    _exec(db_path, "UPDATE sessions SET role='worker' WHERE session_id='orc3'")
    _exec(
        db_path,
        "INSERT INTO sessions(session_id,role,updated_at) VALUES('ghost','orchestrator',1)",
    )
    res = db.register("orc4", "/repo4", role="orchestrator", path=db_path)
    assert res.accepted is True
    assert db.get_session("ghost", path=db_path)["role"] == "worker"

    # worker registration is independent of the orchestrator slot
    res = db.register("w1", "/repo", path=db_path)
    assert res.accepted is True and res.role == "worker"
    assert db.get_session("orc4", path=db_path)["role"] == "orchestrator"


def test_item13b1_gwt2_reregister_bumps_generation(db_path):
    r1 = db.register("w1", "/repo", path=db_path)
    assert r1.generation == 1
    r2 = db.register("w1", "/repo", path=db_path)
    assert r2.generation == 2
    # a caller still holding generation 1 detects it has been superseded
    current = _one(
        db_path,
        "SELECT generation FROM worker_heartbeats WHERE session_id='w1'",
    )["generation"]
    assert current == r2.generation
    assert r1.generation < current


def test_item13b1_worker_register_upsert(db_path):
    res = db.register("w1", "/repo", path=db_path)
    assert res.accepted is True
    assert res.role == "worker"
    row = db.get_session("w1", path=db_path)
    assert row["role"] == "worker"
    assert row["mode"] == "unmanaged"
    assert row["status"] == "active"
    # the contract workspace maps onto the cwd column
    assert row["cwd"] == "/repo"
    hb = _one(db_path, "SELECT * FROM worker_heartbeats WHERE session_id='w1'")
    assert hb["generation"] == 1
    assert abs(hb["last_seen_at"] - time.time()) < 5

    # re-register updates cwd and preserves fields written by other paths
    db.upsert_session("w1", title="t", path=db_path)
    res = db.register("w1", "/repo2", path=db_path)
    assert res.generation == 2
    row = db.get_session("w1", path=db_path)
    assert row["cwd"] == "/repo2"
    assert row["title"] == "t"

    # contract enums are accepted alongside plain strings
    res = db.register("w2", "/repo", role=SessionRole.ORCHESTRATOR, path=db_path)
    assert res.accepted and res.role == "orchestrator"
    with pytest.raises(ValueError):
        db.register("w3", "/repo", role="bogus", path=db_path)


def test_item13b1_heartbeat_refresh(db_path):
    # heartbeat on an unknown session creates the row at generation 1
    assert db.heartbeat("w1", path=db_path) == 1
    hb = _one(db_path, "SELECT * FROM worker_heartbeats WHERE session_id='w1'")
    assert abs(hb["last_seen_at"] - time.time()) < 5

    # a later heartbeat refreshes last_seen_at and bumps the generation
    stale = int(time.time()) - db.HEARTBEAT_STALE_SECONDS - 10
    _exec(
        db_path,
        "UPDATE worker_heartbeats SET last_seen_at=? WHERE session_id='w1'",
        (stale,),
    )
    assert db.heartbeat("w1", path=db_path) == 2
    hb = _one(db_path, "SELECT * FROM worker_heartbeats WHERE session_id='w1'")
    assert hb["last_seen_at"] > stale


def test_item13b1_migration_old_schema_db(tmp_path):
    # a populated pre-v2 state.db: sessions without role/mode/status,
    # no worker_heartbeats table
    path = str(tmp_path / "state.db")
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE sessions(
          session_id TEXT PRIMARY KEY,
          cwd TEXT,
          title TEXT,
          queue_paused INTEGER NOT NULL DEFAULT 0,
          park_cycles_left INTEGER NOT NULL DEFAULT 5,
          note TEXT,
          updated_at INTEGER NOT NULL
        );
        INSERT INTO sessions(session_id,cwd,title,updated_at)
          VALUES('s1','/old','t',1);
        """
    )
    con.commit()
    con.close()

    db.init(path)
    row = db.get_session("s1", path=path)
    # existing rows survive; new columns carry the contract defaults
    assert row["cwd"] == "/old"
    assert row["title"] == "t"
    assert row["role"] == "worker"
    assert row["mode"] == "unmanaged"
    assert row["status"] == "active"
    assert _one(path, "SELECT COUNT(*) AS c FROM worker_heartbeats")["c"] == 0

    # the migration is idempotent
    db.init(path)

    # the migrated file serves the full bus flow
    res = db.register("s1", "/new", path=path)
    assert res.accepted is True and res.generation == 1
    assert db.get_session("s1", path=path)["cwd"] == "/new"
    res = db.register("orc", "/repo", role="orchestrator", path=path)
    assert res.accepted is True


def test_item13b1_migration_concurrent_init(tmp_path):
    # a pre-v2 state.db raced by several init() calls: the
    # check-then-ALTER migration runs inside BEGIN IMMEDIATE, so the
    # losers wait on the writer lock instead of dying on a
    # "duplicate column name" OperationalError
    path = str(tmp_path / "state.db")
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE sessions(
          session_id TEXT PRIMARY KEY,
          cwd TEXT,
          title TEXT,
          queue_paused INTEGER NOT NULL DEFAULT 0,
          park_cycles_left INTEGER NOT NULL DEFAULT 5,
          note TEXT,
          updated_at INTEGER NOT NULL
        );
        INSERT INTO sessions(session_id,cwd,title,updated_at)
          VALUES('s1','/old','t',1);
        """
    )
    con.commit()
    con.close()

    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(lambda _: db.init(path), range(8)))

    row = db.get_session("s1", path=path)
    assert row["cwd"] == "/old"
    assert row["role"] == "worker"
    assert row["mode"] == "unmanaged"
    assert row["status"] == "active"
    # user_version commits with the ALTERs, so 3 certifies v2+v3 columns
    assert _one(path, "PRAGMA user_version")[0] == 3

    # an already-migrated file: further init() stays a no-op
    db.init(path)
    assert db.get_session("s1", path=path)["cwd"] == "/old"


def _ask(db_path, session_id="w1", task_id=1, text="which branch?"):
    # item_20: ask() refuses without an active orchestrator - seed one
    db.register("orch-seed", "/repo/ctl", role=SessionRole.ORCHESTRATOR, path=db_path)
    res = db.ask(session_id, task_id, text, path=db_path)
    assert res.accepted is True
    assert res.question_id is not None
    assert res.reason is None
    return res.question_id


def test_item13b2_gwt1_concurrent_claims_single_winner(db_path):
    qid = _ask(db_path)
    claimants = [f"orc{i}" for i in range(8)]
    with ThreadPoolExecutor(max_workers=len(claimants)) as ex:
        results = list(
            ex.map(
                lambda c: db.claim_question(qid, c, path=db_path),
                claimants,
            )
        )
    assert results.count(True) == 1
    row = db.get_question(qid, path=db_path)
    assert row["state"] == "claimed"
    assert row["claimed_by"] == claimants[results.index(True)]
    assert row["claimed_at"] is not None
    # a late claim on an already-claimed question always loses
    assert db.claim_question(qid, "late", path=db_path) is False


def test_item13b2_gwt2_second_ask_on_task_with_pending_rejected(db_path):
    qid = _ask(db_path, task_id=7)
    # a second ask while the first is pending is refused, structured
    res = db.ask("w1", 7, "and another?", path=db_path)
    assert res.accepted is False
    assert res.reason == "pending_exists"
    assert res.question_id is None
    # the unique pending index backs the refusal at the storage level
    with pytest.raises(sqlite3.IntegrityError):
        _exec(
            db_path,
            "INSERT INTO questions(task_id,session_id,text,state,asked_at)"
            " VALUES(7,'w1','x','pending',1)",
        )
    # a different task is unaffected
    other = _ask(db_path, task_id=8)
    assert other != qid
    # once the first question leaves pending the slot frees up
    assert db.claim_question(qid, "orc", path=db_path) is True
    res = db.ask("w1", 7, "now it fits", path=db_path)
    assert res.accepted is True


def test_item13b2_gwt3_stale_claim_janitor_returns_pending_same_id(db_path):
    qid = _ask(db_path, task_id=3)
    assert db.claim_question(qid, "orc1", path=db_path) is True
    # a fresh claim is not the janitor's business
    assert db.janitor_questions(path=db_path) == []
    row = db.get_question(qid, path=db_path)
    assert row["state"] == "claimed"

    # age the claim past CLAIM_TIMEOUT_SECONDS
    stale = int(time.time()) - CLAIM_TIMEOUT_SECONDS - 1
    _exec(
        db_path,
        "UPDATE questions SET claimed_at=? WHERE id=?",
        (stale, qid),
    )
    assert db.janitor_questions(path=db_path) == [qid]
    row = db.get_question(qid, path=db_path)
    assert row["id"] == qid
    assert row["state"] == "pending"
    assert row["claimed_by"] is None
    assert row["claimed_at"] is None
    # second pass is a no-op
    assert db.janitor_questions(path=db_path) == []
    # the released question is claimable again under the same id
    assert db.claim_question(qid, "orc2", path=db_path) is True


def test_item13b2_janitor_skips_task_with_newer_pending(db_path):
    # stale claim on q1 while q2 already holds the task's pending slot:
    # releasing q1 would trip the unique pending index, so the janitor
    # leaves it claimed for the next pass instead of crashing
    q1 = _ask(db_path, task_id=5)
    assert db.claim_question(q1, "orc1", path=db_path) is True
    _ask(db_path, task_id=5, text="second")
    stale = int(time.time()) - CLAIM_TIMEOUT_SECONDS - 1
    _exec(
        db_path,
        "UPDATE questions SET claimed_at=? WHERE id=?",
        (stale, q1),
    )
    assert db.janitor_questions(path=db_path) == []
    assert db.get_question(q1, path=db_path)["state"] == "claimed"


def test_item13b2_janitor_one_stale_claim_per_task_per_pass(db_path):
    # two stale claims on one task used to abort the whole UPDATE on
    # the unique pending index (both rows pass the pending guard
    # against the same pre-update snapshot), wedging the janitor for
    # every task forever. Now a pass releases at most the min stale
    # id per task; the rest catch up once the pending slot frees.
    q1 = _ask(db_path, task_id=5)
    assert db.claim_question(q1, "orc1", path=db_path) is True
    q2 = _ask(db_path, task_id=5, text="second")
    assert db.claim_question(q2, "orc2", path=db_path) is True
    # a stale claim on another task must not be collateral damage
    q3 = _ask(db_path, task_id=6, text="other task")
    assert db.claim_question(q3, "orc3", path=db_path) is True

    stale = int(time.time()) - CLAIM_TIMEOUT_SECONDS - 1
    _exec(
        db_path,
        "UPDATE questions SET claimed_at=? WHERE state='claimed'",
        (stale,),
    )

    # first pass: the min stale id on the wedged task plus the other
    # task's stale claim - the sweep completes instead of raising
    assert db.janitor_questions(path=db_path) == [q1, q3]
    assert db.get_question(q1, path=db_path)["state"] == "pending"
    assert db.get_question(q2, path=db_path)["state"] == "claimed"
    assert db.get_question(q3, path=db_path)["state"] == "pending"

    # q1 now holds the task's pending slot, so q2 stays claimed
    assert db.janitor_questions(path=db_path) == []
    # once the slot frees the next pass releases q2 under the same id
    assert db.claim_question(q1, "orc1b", path=db_path) is True
    assert db.janitor_questions(path=db_path) == [q2]
    assert db.get_question(q2, path=db_path)["state"] == "pending"
    assert db.get_question(q2, path=db_path)["claimed_by"] is None


def test_item13b2_claim_question_rejects_blank_claimant(db_path):
    qid = _ask(db_path)
    with pytest.raises(ValueError):
        db.claim_question(qid, "", path=db_path)
    with pytest.raises(ValueError):
        db.claim_question(qid, "   ", path=db_path)
    assert db.get_question(qid, path=db_path)["state"] == "pending"


def test_item13b2_answer_question_claimed_to_answered(db_path):
    qid = _ask(db_path)
    # pending cannot be answered - the claim must land first
    assert db.answer_question(qid, "too early", path=db_path) is False
    row = db.get_question(qid, path=db_path)
    assert row["state"] == "pending"
    assert row["answer"] is None

    assert db.claim_question(qid, "orc", path=db_path) is True
    assert db.answer_question(qid, "use main", path=db_path) is True
    row = db.get_question(qid, path=db_path)
    assert row["state"] == "answered"
    assert row["answer"] == "use main"
    assert row["answered_at"] is not None
    # claim fields survive - the row passed through claimed
    assert row["claimed_by"] == "orc"
    assert row["claimed_at"] is not None
    # answered is terminal
    assert db.answer_question(qid, "again", path=db_path) is False
    assert db.claim_question(qid, "orc2", path=db_path) is False
    assert db.janitor_questions(path=db_path) == []

    with pytest.raises(ValueError):
        db.answer_question(qid, None, path=db_path)


def test_item13b2_max_questions_per_task_refusal(db_path):
    # MAX_QUESTIONS_PER_TASK counts every state, so fill the budget by
    # cycling questions through the whole lifecycle
    for i in range(MAX_QUESTIONS_PER_TASK):
        qid = _ask(db_path, task_id=9, text=f"q{i}")
        assert db.claim_question(qid, "orc", path=db_path) is True
        assert db.answer_question(qid, f"a{i}", path=db_path) is True
    res = db.ask("w1", 9, "one too many", path=db_path)
    assert res.accepted is False
    assert res.reason == "question_limit"
    assert res.question_id is None
    # the limit is per task
    assert db.ask("w1", 10, "fine here", path=db_path).accepted is True


def test_item13b2_pending_helpers_and_get(db_path):
    q1 = _ask(db_path, session_id="w1", task_id=1, text="first")
    q2 = _ask(db_path, session_id="w2", task_id=2, text="second")
    assert db.pending_questions_count(1, path=db_path) == 1
    assert db.pending_questions_count(42, path=db_path) == 0

    rows = db.list_pending_questions(path=db_path)
    assert [r["id"] for r in rows] == [q1, q2]
    rows = db.list_pending_questions(task_id=2, path=db_path)
    assert [r["id"] for r in rows] == [q2]

    row = db.get_question(q1, path=db_path)
    assert row["task_id"] == 1
    assert row["session_id"] == "w1"
    assert row["text"] == "first"
    assert row["state"] == "pending"
    assert row["asked_at"] is not None
    assert db.get_question(999, path=db_path) is None

    # claimed questions leave the pending scan
    assert db.claim_question(q1, "orc", path=db_path) is True
    assert [r["id"] for r in db.list_pending_questions(path=db_path)] == [q2]
    assert db.pending_questions_count(1, path=db_path) == 0

    with pytest.raises(ValueError):
        db.ask("w1", 3, "", path=db_path)


def test_item13b2_audit_insert_append_only(db_path):
    qid = _ask(db_path, task_id=11)
    assert db.claim_question(qid, "orc", path=db_path) is True
    assert db.answer_question(qid, "ans", path=db_path) is True
    q = db.get_question(qid, path=db_path)

    outputs = {"gh-copilot": "verdict A", "gemini": "verdict B"}
    eid = db.audit(
        AuditEventKind.DELIBERATION,
        session_id="w1",
        task_id=11,
        question_id=qid,
        question_text="which branch?",
        partner_outputs=outputs,
        synthesis="merged view",
        directive="use main",
        asked_at=q["asked_at"],
        claimed_at=q["claimed_at"],
        answered_at=q["answered_at"],
        path=db_path,
    )
    assert eid is not None
    row = _one(db_path, "SELECT * FROM audit_events WHERE id=?", (eid,))
    assert row["kind"] == "deliberation"
    assert row["session_id"] == "w1"
    assert row["task_id"] == 11
    assert row["question_id"] == qid
    assert row["question_text"] == "which branch?"
    # partner outputs stored verbatim as JSON text
    assert json.loads(row["partner_outputs"]) == outputs
    assert row["synthesis"] == "merged view"
    assert row["directive"] == "use main"
    assert row["doa_trigger"] is None
    assert row["asked_at"] == q["asked_at"]
    assert row["claimed_at"] == q["claimed_at"]
    assert row["answered_at"] == q["answered_at"]
    assert abs(row["created_at"] - time.time()) < 5

    # string kinds and minimal events are accepted; ids only grow
    eid2 = db.audit("escalation", doa_trigger="budget_exceeded", path=db_path)
    assert eid2 > eid
    row = _one(db_path, "SELECT * FROM audit_events WHERE id=?", (eid2,))
    assert row["kind"] == "escalation"
    assert row["doa_trigger"] == "budget_exceeded"
    assert json.loads(row["partner_outputs"]) == {}

    with pytest.raises(ValueError):
        db.audit("bogus-kind", path=db_path)


def test_item13b3_gwt1_unrelayed_row_survives_relay_crash(db_path):
    # notify() commits the row with relayed_at/ack_at NULL - a relay
    # killed between the commit and the fs-write/mark must not lose it
    nid = db.notify("w1", NotificationKind.DIRECTIVE, "steer left", task_id=7, path=db_path)
    assert nid is not None
    row = _one(db_path, "SELECT * FROM notifications WHERE id=?", (nid,))
    assert row["session_id"] == "w1"
    assert row["kind"] == "directive"
    assert row["body"] == "steer left"
    assert row["task_id"] == 7
    assert row["question_id"] is None
    assert row["relayed_at"] is None
    assert row["ack_at"] is None
    assert abs(row["created_at"] - time.time()) < 5

    # a relay pass that dies after the scan leaves the row unrelayed;
    # the retry pass picks the same row up - nothing is lost
    assert [r["id"] for r in db.unrelayed_notifications(path=db_path)] == [nid]
    assert [r["id"] for r in db.unrelayed_notifications(path=db_path)] == [nid]
    assert db.mark_relayed(nid, path=db_path) is True
    assert db.unrelayed_notifications(path=db_path) == []


def test_item13b3_gwt2_mark_relayed_idempotent_no_duplicate_signal(db_path):
    nid = db.notify("w1", "heartbeat_tick", "ping", path=db_path)
    assert db.mark_relayed(nid, path=db_path) is True
    relayed = _one(db_path, "SELECT relayed_at FROM notifications WHERE id=?", (nid,))["relayed_at"]
    assert relayed is not None

    # a replayed mark loses the CAS: False and relayed_at untouched, so
    # the caller cannot take it as a fresh write-the-mailbox signal
    assert db.mark_relayed(nid, path=db_path) is False
    row = _one(db_path, "SELECT relayed_at FROM notifications WHERE id=?", (nid,))
    assert row["relayed_at"] == relayed

    # two racing relays: exactly one wins the mark
    nid2 = db.notify("w1", "heartbeat_tick", "ping2", path=db_path)
    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(lambda _: db.mark_relayed(nid2, path=db_path), range(8)))
    assert results.count(True) == 1


def test_item13b3_gwt3_restart_redelivers_unacked(db_path):
    n1 = db.notify("w1", "task_dispatch", "run task", task_id=1, path=db_path)
    n2 = db.notify("w1", "directive", "steer", path=db_path)
    n3 = db.notify("w2", "disarm", "teardown", path=db_path)

    # w1 consumes n1 and advances its ack watermark past it
    assert db.ack_notifications("w1", up_to_id=n1, path=db_path) == 1
    row = _one(db_path, "SELECT ack_at FROM notifications WHERE id=?", (n1,))
    assert row["ack_at"] is not None

    # restart: level-triggered - w1 sees exactly the rows it has not
    # acked, ordered for reprocessing; w2's rows are not its business
    assert [r["id"] for r in db.unacked_notifications("w1", path=db_path)] == [n2]
    assert [r["id"] for r in db.unacked_notifications("w2", path=db_path)] == [n3]

    # re-acking the same watermark is a no-op on already-acked rows
    assert db.ack_notifications("w1", up_to_id=n1, path=db_path) == 0
    # a full ack clears the restart view; other recipients untouched
    assert db.ack_notifications("w1", path=db_path) == 1
    assert db.unacked_notifications("w1", path=db_path) == []
    assert [r["id"] for r in db.unacked_notifications("w2", path=db_path)] == [n3]


def test_item13b3_notify_validates_kind_and_session(db_path):
    # enum and plain string kinds are both accepted
    nid = db.notify("w1", NotificationKind.QUESTION_ANSWER, "use main", question_id=3, path=db_path)
    row = _one(db_path, "SELECT kind, question_id FROM notifications WHERE id=?", (nid,))
    assert row["kind"] == "question_answer"
    assert row["question_id"] == 3
    nid2 = db.notify("w1", "disarm", "x", path=db_path)
    assert nid2 == nid + 1

    with pytest.raises(ValueError):
        db.notify("w1", "bogus-kind", "x", path=db_path)
    with pytest.raises(ValueError):
        db.notify("", "directive", "x", path=db_path)
    with pytest.raises(ValueError):
        db.notify("   ", "directive", "x", path=db_path)
    with pytest.raises(ValueError):
        db.notify("w1", "directive", "", path=db_path)
    # nothing was written by the refused calls
    assert _one(db_path, "SELECT COUNT(*) AS c FROM notifications")["c"] == 2


def test_item13b3_unrelayed_scan_uses_partial_index(db_path):
    # the index the relay scan relies on exists with the declared shape
    idx = _one(
        db_path,
        "SELECT sql FROM sqlite_master WHERE name='ix_notifications_unrelayed'",
    )
    assert idx is not None
    assert "WHERE relayed_at IS NULL" in idx["sql"]
    # and the planner actually picks it for the relay scan
    plan = _one(
        db_path,
        "EXPLAIN QUERY PLAN SELECT * FROM notifications WHERE relayed_at IS NULL",
    )
    assert "ix_notifications_unrelayed" in plan["detail"]


def test_item13b3_unrelayed_per_session_filtering(db_path):
    n1 = db.notify("w1", "directive", "for w1", path=db_path)
    n2 = db.notify("w2", "directive", "for w2", path=db_path)
    n3 = db.notify("w1", "disarm", "w1 again", path=db_path)

    # the full scan is oldest-first; the session filter narrows it
    assert [r["id"] for r in db.unrelayed_notifications(path=db_path)] == [n1, n2, n3]
    assert [r["id"] for r in db.unrelayed_notifications("w1", path=db_path)] == [n1, n3]
    assert [r["id"] for r in db.unrelayed_notifications("w2", path=db_path)] == [n2]
    assert db.unrelayed_notifications("nobody", path=db_path) == []

    # a marked row leaves every unrelayed view
    assert db.mark_relayed(n1, path=db_path) is True
    assert [r["id"] for r in db.unrelayed_notifications("w1", path=db_path)] == [n3]
    assert [r["id"] for r in db.unrelayed_notifications(path=db_path)] == [n2, n3]


# --- item_13c: set_session_mode (managed arm/disarm writer) --------------------


def test_item13c_set_session_mode_cycle(db_path):
    sid = "s-mode"
    db.set_session_mode(sid, SessionMode.MANAGED, path=db_path)
    row = db.get_session(sid, path=db_path)
    assert row is not None
    assert row["mode"] == "managed"  # arm creates the row when absent
    db.set_session_mode(sid, SessionMode.UNMANAGED, path=db_path)
    assert db.get_session(sid, path=db_path)["mode"] == "unmanaged"


def test_item13c_set_session_mode_accepts_strings_and_enums(db_path):
    db.set_session_mode("s-a", "managed", path=db_path)
    assert db.get_session("s-a", path=db_path)["mode"] == "managed"
    db.set_session_mode("s-a", SessionMode.UNMANAGED, path=db_path)
    assert db.get_session("s-a", path=db_path)["mode"] == "unmanaged"


def test_item13c_set_session_mode_preserves_other_fields(db_path):
    sid = "s-keep"
    db.upsert_session(sid, cwd="/repo", queue_paused=True, path=db_path)
    db.set_session_mode(sid, SessionMode.MANAGED, path=db_path)
    row = db.get_session(sid, path=db_path)
    # mode moves; arming also clears queue_paused (item_17); the rest stays put
    assert row["mode"] == "managed"
    assert row["cwd"] == "/repo"
    assert row["queue_paused"] == 0


def test_item13c_set_session_mode_validates(db_path):
    with pytest.raises(ValueError):
        db.set_session_mode("s-x", "bogus", path=db_path)
    with pytest.raises(ValueError):
        db.set_session_mode("", SessionMode.MANAGED, path=db_path)
    assert db.get_session("s-x", path=db_path) is None
    # and mode stays unreachable through the plain upsert whitelist
    with pytest.raises(ValueError):
        db.upsert_session("s-x", mode="managed", path=db_path)


# --- item_13d: supervisor writers (set_session_status + requeue_dead_worker) -----


def test_item13d_set_session_status_transition_dedup(db_path):
    sid = "s-st"
    db.upsert_session(sid, cwd="/repo", path=db_path)
    # first flip wins, the repeat loses - this is what dedups the
    # daemon's per-cycle dead-marking
    assert db.set_session_status(sid, "dead", path=db_path) is True
    assert db.set_session_status(sid, "dead", path=db_path) is False
    assert db.get_session(sid, path=db_path)["status"] == "dead"
    # revival flip works the same way
    assert db.set_session_status(sid, SessionStatus.ACTIVE, path=db_path) is True
    assert db.get_session(sid, path=db_path)["status"] == "active"


def test_item13d_set_session_status_validates(db_path):
    with pytest.raises(ValueError):
        db.set_session_status("s-x", "bogus", path=db_path)
    with pytest.raises(ValueError):
        db.set_session_status(" ", "dead", path=db_path)
    # a missing sessions row is not created (no upsert)
    assert db.set_session_status("ghost", "dead", path=db_path) is False
    assert db.get_session("ghost", path=db_path) is None


def test_item13d_requeue_dead_worker_running_to_queued(db_path):
    tid = _enqueue_running(db_path, "s-dead")
    note = "requeued-dead-worker: reset"
    assert db.requeue_dead_worker(tid, note=note, path=db_path) is TaskStatus.QUEUED
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "queued"
    assert row["attempt"] == 1
    assert row["lease_owner"] is None
    assert row["lease_expires_at"] is None
    assert row["note"] == note


def test_item13d_requeue_dead_worker_covers_all_live_statuses(db_path):
    # queued
    t1 = db.enqueue("s-a", "t1", path=db_path)
    assert db.requeue_dead_worker(t1, path=db_path) is TaskStatus.QUEUED
    # awaiting_user
    t2 = _enqueue_running(db_path, "s-b")
    assert db.set_awaiting_user(t2, "w1", path=db_path)
    assert db.requeue_dead_worker(t2, path=db_path) is TaskStatus.QUEUED
    # dispatch_decided (claimed, transport not yet run)
    t3 = db.enqueue("s-c", "t3", path=db_path)
    assert db.claim_queued(t3, "w3", Transport.ACP_PUSH, path=db_path)
    assert db.requeue_dead_worker(t3, path=db_path) is TaskStatus.QUEUED


def test_item13d_requeue_dead_worker_fails_at_attempt_budget(db_path):
    tid = _enqueue_running(db_path, "s-poison")
    _exec(db_path, "UPDATE tasks SET attempt=? WHERE id=?", (MAX_ATTEMPTS - 1, tid))
    assert db.requeue_dead_worker(tid, path=db_path) is TaskStatus.FAILED
    # terminal row is out of the live set - the CAS loses on replay
    assert db.requeue_dead_worker(tid, path=db_path) is None


# --- item_13e1: claimant binding + escalation writers -------------------------


def test_item13e1_answer_question_binds_to_claimant(db_path):
    qid = _ask(db_path)
    assert db.claim_question(qid, "orc1", path=db_path) is True
    # a zombie orchestrator answering somebody else's claim loses the CAS
    assert db.answer_question(qid, "wrong", claimant="zombie", path=db_path) is False
    row = _one(db_path, "SELECT * FROM questions WHERE id=?", (qid,))
    assert row["state"] == "claimed"
    assert row["answer"] is None
    # the actual claimant answers; claimant=None keeps the pre-binding behavior
    assert db.answer_question(qid, "use main", claimant="orc1", path=db_path) is True
    row = _one(db_path, "SELECT * FROM questions WHERE id=?", (qid,))
    assert row["state"] == "answered"
    assert row["answer"] == "use main"


def test_item13e1_resolve_escalation_cas_and_open_scan(db_path):
    eid = db.audit(
        AuditEventKind.ESCALATION,
        session_id="w1",
        doa_trigger="question_limit",
        question_text="which branch?",
        path=db_path,
    )
    other = db.audit(AuditEventKind.DELIBERATION, directive="use main", path=db_path)
    assert [r["id"] for r in db.list_open_escalations(path=db_path)] == [eid]
    # non-escalation rows cannot be resolved
    assert db.resolve_escalation(other, "A", path=db_path) is False
    assert db.resolve_escalation(eid, "defer", path=db_path) is True
    assert db.list_open_escalations(path=db_path) == []
    # a repeated fill loses the CAS - "already resolved"
    assert db.resolve_escalation(eid, "B", path=db_path) is False
    row = _one(db_path, "SELECT directive FROM audit_events WHERE id=?", (eid,))
    assert row["directive"] == "defer"


def test_item13e1_defer_abort_task_block_writers(db_path):
    # defer parks a queued task; paused leaves the live set
    t1 = db.enqueue("s-a", "t1", path=db_path)
    assert db.defer_task(t1, note="blocked: deferred by human", path=db_path) is True
    row = db.get_task(t1, path=db_path)
    assert row["status"] == "paused"
    assert row["note"] == "blocked: deferred by human"
    # already parked - the live guard loses the CAS on replay
    assert db.defer_task(t1, path=db_path) is False

    # abort drops a running task to terminal failed, lease cleared
    t2 = _enqueue_running(db_path, "s-b", owner="w-zombie")
    assert db.abort_task(t2, note="aborted by human: cleanup branch", path=db_path) is True
    row = db.get_task(t2, path=db_path)
    assert row["status"] == "failed"
    assert row["note"] == "aborted by human: cleanup branch"
    assert row["lease_owner"] is None
    # the zombie owner cannot overwrite the human's verdict
    assert db.finish_task(t2, "w-zombie", ok=True, path=db_path) is False
    # terminal rows are out of the live set
    assert db.abort_task(t2, path=db_path) is False
    assert db.defer_task(t2, path=db_path) is False


# --- item_14: recurring schedules ----------------------------------------------


def test_item14_schema_v3_tables_and_version(db_path):
    assert _one(db_path, "PRAGMA user_version")[0] == 3
    con = db.connect(db_path)
    try:
        tables = {
            r["name"] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        cols = {r["name"] for r in con.execute("PRAGMA table_info(tasks)")}
    finally:
        con.close()
    assert "schedules" in tables
    assert "run_after" in cols


def test_item14_init_migrates_pre_v3_file(tmp_path):
    # a minimal pre-v3 file: old tasks/sessions layouts, no schedules
    p = str(tmp_path / "state.db")
    con = sqlite3.connect(p, isolation_level=None)
    try:
        con.execute("CREATE TABLE tasks(id INTEGER PRIMARY KEY, session_id TEXT, status TEXT)")
        con.execute("CREATE TABLE sessions(session_id TEXT PRIMARY KEY, updated_at INTEGER)")
    finally:
        con.close()
    db.init(p)
    con = db.connect(p)
    try:
        assert con.execute("PRAGMA user_version").fetchone()[0] == 3
        tasks_cols = {r["name"] for r in con.execute("PRAGMA table_info(tasks)")}
        sessions_cols = {r["name"] for r in con.execute("PRAGMA table_info(sessions)")}
        names = {
            r["name"] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    finally:
        con.close()
    assert "run_after" in tasks_cols
    assert {"role", "mode", "status"} <= sessions_cols
    assert "schedules" in names


def test_item14_add_schedule_validates(db_path):
    with pytest.raises(ValueError):
        db.add_schedule("", "t", 60, 100, path=db_path)
    with pytest.raises(ValueError):
        db.add_schedule(" ", "t", 60, 100, path=db_path)
    with pytest.raises(ValueError):
        db.add_schedule("s1", "", 60, 100, path=db_path)
    with pytest.raises(ValueError):
        db.add_schedule("s1", "t", 0, 100, path=db_path)
    with pytest.raises(ValueError):
        db.add_schedule("s1", "t", -60, 100, path=db_path)
    assert db.list_schedules(path=db_path) == []


def test_item14_gwt1_schedule_persists_and_materializes(db_path):
    now = int(time.time())
    sid = db.add_schedule("s1", "nightly sweep", 60, now - 1, path=db_path)

    # a "restart" (fresh connection) still sees the schedule
    con = db.connect(db_path)
    try:
        row = con.execute("SELECT * FROM schedules WHERE id=?", (sid,)).fetchone()
    finally:
        con.close()
    assert row["session_id"] == "s1"
    assert row["task_text"] == "nightly sweep"
    assert row["every_seconds"] == 60
    assert row["enabled"] == 1
    assert row["catchup"] == 0
    assert row["last_run_at"] is None

    due = db.due_schedules(now, path=db_path)
    assert [r["id"] for r in due] == [sid]

    task_id = db.materialize_schedule(sid, now, path=db_path)
    assert task_id is not None
    task = db.get_task(task_id, path=db_path)
    assert task["session_id"] == "s1"
    assert task["task_text"] == "nightly sweep"
    assert task["status"] == TaskStatus.QUEUED.value
    assert task["run_after"] is None  # dispatchable immediately
    # the outbox row carries the dispatch routing refs
    outbox = db.unrelayed_notifications("s1", path=db_path)
    assert len(outbox) == 1
    assert outbox[0]["kind"] == NotificationKind.TASK_DISPATCH.value
    assert outbox[0]["task_id"] == task_id
    assert outbox[0]["body"] == "nightly sweep"
    # the grid advanced: fired at now, next on now+every
    row = _one(db_path, "SELECT * FROM schedules WHERE id=?", (sid,))
    assert row["last_run_at"] == now
    assert row["next_run_at"] == now + 60


def test_item14_gwt2_double_materialize_yields_one_task(db_path):
    now = int(time.time())
    sid = db.add_schedule("s1", "t", 60, now - 1, path=db_path)
    assert db.materialize_schedule(sid, now, path=db_path) is not None
    # a second due-call (or a second daemon pass) loses the CAS: the
    # advance guard on next_run_at<=now no longer matches
    assert db.materialize_schedule(sid, now, path=db_path) is None
    tasks = db.list_tasks(status=TaskStatus.QUEUED, path=db_path)
    assert len(tasks) == 1
    assert len(db.unrelayed_notifications(path=db_path)) == 1


def test_item14_gwt2_concurrent_materialize_single_winner(db_path):
    now = int(time.time())
    sid = db.add_schedule("s1", "t", 60, now - 1, path=db_path)
    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(
            ex.map(
                lambda _i: db.materialize_schedule(sid, now, path=db_path),
                range(8),
            )
        )
    assert sum(r is not None for r in results) == 1
    assert len(db.list_tasks(status=TaskStatus.QUEUED, path=db_path)) == 1
    assert len(db.unrelayed_notifications(path=db_path)) == 1


def test_item14_gwt3_catchup0_skips_missed_grid(db_path):
    # three missed periods: catchup=0 fires once and resets from now
    now = int(time.time())
    sid = db.add_schedule("s1", "t", 60, now - 3 * 60, path=db_path)
    assert db.materialize_schedule(sid, now, path=db_path) is not None
    row = _one(db_path, "SELECT * FROM schedules WHERE id=?", (sid,))
    assert row["next_run_at"] == now + 60
    assert len(db.list_tasks(status=TaskStatus.QUEUED, path=db_path)) == 1


def test_item14_gwt3_catchup1_recatches_the_grid(db_path):
    now = int(time.time())
    anchor = now - 3 * 60 - 17  # missed grid points at -197, -137, -77, -17
    sid = db.add_schedule("s1", "t", 60, anchor, catchup=True, path=db_path)
    assert db.materialize_schedule(sid, now, path=db_path) is not None
    row = _one(db_path, "SELECT * FROM schedules WHERE id=?", (sid,))
    # one fire, then the original grid resumes: smallest anchor+k*60 > now
    assert row["next_run_at"] == anchor + 4 * 60
    assert row["next_run_at"] > now
    assert len(db.list_tasks(status=TaskStatus.QUEUED, path=db_path)) == 1


def test_item14_catchup1_far_missed_grid_realigns_in_o1(db_path):
    # regression: a linear grid walk over hundreds of millions of
    # missed every=1 points would hang inside the write txn - the
    # catchup realign must be closed-form
    now = int(time.time())
    anchor = 946684800  # 2000-01-01T00:00:00Z, decades of missed fires
    sid = db.add_schedule("s1", "t", 1, anchor, catchup=True, path=db_path)
    started = time.monotonic()
    task_id = db.materialize_schedule(sid, now, path=db_path)
    assert task_id is not None
    assert time.monotonic() - started < 5
    row = _one(db_path, "SELECT * FROM schedules WHERE id=?", (sid,))
    # still exactly one task, and the next fire sits on the anchor
    # grid strictly ahead of now (anchor + k*1 > now)
    assert len(db.list_tasks(status=TaskStatus.QUEUED, path=db_path)) == 1
    assert row["next_run_at"] == now + 1
    assert row["next_run_at"] > now
    assert (row["next_run_at"] - anchor) % 1 == 0


def test_item14_gwt4_disable_stops_the_schedule(db_path):
    now = int(time.time())
    sid = db.add_schedule("s1", "t", 60, now - 1, path=db_path)
    assert db.disable_schedule(sid, path=db_path) is True
    # a repeated clear loses the CAS
    assert db.disable_schedule(sid, path=db_path) is False
    # disabled rows stay listed but are never due
    assert [r["id"] for r in db.list_schedules(path=db_path)] == [sid]
    assert db.due_schedules(now + 10_000, path=db_path) == []
    assert db.materialize_schedule(sid, now + 10_000, path=db_path) is None
    assert db.list_tasks(status=TaskStatus.QUEUED, path=db_path) == []
    # the row survives with its history (enabled=0, never fired)
    row = _one(db_path, "SELECT * FROM schedules WHERE id=?", (sid,))
    assert row["enabled"] == 0
    assert row["last_run_at"] is None


def test_item14_not_due_and_missing_materialize_noop(db_path):
    now = int(time.time())
    sid = db.add_schedule("s1", "t", 60, now + 3600, path=db_path)
    assert db.due_schedules(now, path=db_path) == []
    assert db.materialize_schedule(sid, now, path=db_path) is None
    assert db.materialize_schedule(9999, now, path=db_path) is None
    assert db.list_tasks(path=db_path) == []


def test_item14_live_task_slot_defers_the_fire(db_path):
    now = int(time.time())
    sid = db.add_schedule("s1", "scheduled work", 60, now - 1, path=db_path)
    # the session already carries a live task - the unique live index
    # refuses the insert, the whole materialize rolls back atomically
    live = db.enqueue("s1", "busy", path=db_path)
    assert live is not None
    assert db.materialize_schedule(sid, now, path=db_path) is None
    # no half-state: no task beyond the live one, no outbox row, and
    # the schedule is still due (not advanced) for the next pass
    assert len(db.list_tasks(path=db_path)) == 1
    assert db.unrelayed_notifications(path=db_path) == []
    row = _one(db_path, "SELECT * FROM schedules WHERE id=?", (sid,))
    assert row["last_run_at"] is None
    assert row["next_run_at"] == now - 1
    # once the slot frees the deferred fire lands
    _exec(db_path, "UPDATE tasks SET status='done' WHERE id=?", (live,))
    assert db.materialize_schedule(sid, now, path=db_path) is not None


def test_item14_list_schedules_session_filter(db_path):
    now = int(time.time())
    a = db.add_schedule("s-a", "ta", 60, now, path=db_path)
    b = db.add_schedule("s-b", "tb", 120, now, path=db_path)
    c = db.add_schedule("s-a", "tc", 180, now, path=db_path)
    assert [r["id"] for r in db.list_schedules(path=db_path)] == [a, b, c]
    assert [r["id"] for r in db.list_schedules("s-a", path=db_path)] == [a, c]
    assert db.list_schedules("ghost", path=db_path) == []


# --- item_17: arm clears queue_paused; ask surfaces to orchestrator -------------


def test_item17_gwt5_arm_clears_queue_paused(db_path):
    """[ITEM-17.GWT-5] set_session_mode(managed) clears queue_paused -
    tasks become claimable again in the same CAS."""
    db.register("s1", "/repo", path=db_path)
    db.session_pause("s1", True, path=db_path)
    assert db.get_session("s1", path=db_path)["queue_paused"] == 1
    db.set_session_mode("s1", SessionMode.MANAGED, path=db_path)
    assert db.get_session("s1", path=db_path)["queue_paused"] == 0
    # disarm/rearm stays consistent
    db.session_pause("s1", True, path=db_path)
    db.set_session_mode("s1", SessionMode.UNMANAGED, path=db_path)
    assert db.get_session("s1", path=db_path)["queue_paused"] == 1
    db.set_session_mode("s1", SessionMode.MANAGED, path=db_path)
    assert db.get_session("s1", path=db_path)["queue_paused"] == 0


def _notifications(db_path, sid):
    con = db.connect(db_path)
    try:
        return con.execute(
            "SELECT * FROM notifications WHERE session_id=? ORDER BY id", (sid,)
        ).fetchall()
    finally:
        con.close()


def test_item17_gwt6_ask_notifies_active_orchestrator(db_path):
    """[ITEM-17.GWT-6] worker ask -> DIRECTIVE notification lands in the
    orchestrator's outbox with task/question correlation refs."""
    db.register("orc", "/repo/ctl", role=SessionRole.ORCHESTRATOR, path=db_path)
    tid = db.enqueue("w1", "work", path=db_path)
    r = db.ask("w1", tid, "which base ref?", path=db_path)
    assert r.accepted
    rows = _notifications(db_path, "orc")
    assert len(rows) == 1
    row = rows[0]
    assert row["kind"] == "directive"
    assert row["task_id"] == tid
    assert row["question_id"] == r.question_id
    assert row["relayed_at"] is None  # outbox pending, daemon relays it
    assert "w1" in row["body"] and str(tid) in row["body"]


def test_item20_gwt1_ask_without_orchestrator_refused(db_path):
    """[ITEM-20.GWT-1] no active orchestrator -> the ask is refused,
    no question row and no notification exist - never accept-and-drop."""
    tid = db.enqueue("w1", "work", path=db_path)
    r = db.ask("w1", tid, "q", path=db_path)
    assert r.accepted is False
    assert r.reason == "no_orchestrator"
    assert r.question_id is None
    assert db.pending_questions_count(tid, path=db_path) == 0
    assert _notifications(db_path, "w1") == []
    # a dead orchestrator does not count as a route either
    db.register("orc", "/repo/ctl", role=SessionRole.ORCHESTRATOR, path=db_path)
    db.set_session_status("orc", SessionStatus.DEAD, path=db_path)
    r = db.ask("w1", tid, "q", path=db_path)
    assert r.accepted is False
    assert r.reason == "no_orchestrator"
    # the asker itself holding the role is excluded from its own route
    db.register("orc", "/repo/ctl", role=SessionRole.ORCHESTRATOR, force=True, path=db_path)
    r = db.ask("orc", tid, "q to self", path=db_path)
    assert r.accepted is False
    assert r.reason == "no_orchestrator"


# --- item_20: question lifecycle hardening -----------------------------------


def test_item20_gwt3_expire_pending_questions_frees_slot(db_path):
    """[ITEM-20.GWT-3] pending older than PENDING_TIMEOUT_SECONDS ->
    terminal expired, the task's pending slot frees for a re-ask."""
    qid = _ask(db_path, task_id=5)
    # a fresh pending question survives the sweep
    assert db.expire_pending_questions(path=db_path) == []
    _exec(
        db_path,
        "UPDATE questions SET asked_at=? WHERE id=?",
        (int(time.time()) - PENDING_TIMEOUT_SECONDS - 5, qid),
    )
    rows = db.expire_pending_questions(path=db_path)
    assert [r["id"] for r in rows] == [qid]
    assert rows[0]["task_id"] == 5
    assert rows[0]["session_id"] == "w1"
    row = db.get_question(qid, path=db_path)
    assert row["state"] == "expired"
    # expired is terminal: a second pass is a no-op
    assert db.expire_pending_questions(path=db_path) == []
    # the freed slot lets the worker re-ask within its budget
    assert db.ask("w1", 5, "again?", path=db_path).accepted


def test_item20_gwt4_pending_question_protects_task_lease(db_path):
    """[ITEM-20.GWT-4] a running task blocked on a pending question
    gets its expired lease renewed by recover_stale, not requeued -
    and recovery resumes once the question resolves."""
    tid = _enqueue_running(db_path, "w1", "w1")
    qid = _ask(db_path, session_id="w1", task_id=tid)
    _expire(db_path, tid)
    assert db.recover_stale(path=db_path) == []
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "running"
    assert row["attempt"] == 0
    assert row["lease_expires_at"] > int(time.time())
    assert row["lease_renewed_at"] is not None
    # once the question resolves the protection lifts: the next
    # expired lease recovers normally
    assert db.claim_question(qid, "orch-seed", path=db_path)
    assert db.answer_question(qid, "main", claimant="orch-seed", path=db_path)
    _expire(db_path, tid)
    assert db.recover_stale(path=db_path) == [tid]
    assert db.get_task(tid, path=db_path)["status"] == "queued"


def test_item20_gwt5_claimed_question_protects_task_lease(db_path):
    """[ITEM-20.GWT-5] a claimed (in-deliberation) question protects
    the task lease the same way as a pending one."""
    tid = _enqueue_running(db_path, "w1", "w1")
    qid = _ask(db_path, session_id="w1", task_id=tid)
    assert db.claim_question(qid, "orch-seed", path=db_path)
    _expire(db_path, tid)
    assert db.recover_stale(path=db_path) == []
    assert db.get_task(tid, path=db_path)["status"] == "running"


def test_item20_answered_expired_questions_do_not_protect(db_path):
    """Only unresolved questions protect: answered and expired
    questions leave the lease recoverable."""
    tid = _enqueue_running(db_path, "w1", "w1")
    qid = _ask(db_path, session_id="w1", task_id=tid)
    _exec(
        db_path,
        "UPDATE questions SET state='expired' WHERE id=?",
        (qid,),
    )
    _expire(db_path, tid)
    assert db.recover_stale(path=db_path) == [tid]


# --- item_21: durable wake_state ------------------------------------------


def test_item21_wake_state_outcome_lifecycle(db_path):
    """record + bump + escalate + reset in one coherent episode."""
    db.record_wake_outcome("s1", "sent_unconfirmed", path=db_path)
    ws = db.get_wake_state("s1", path=db_path)
    assert ws["last_outcome"] == "sent_unconfirmed"
    assert ws["unconfirmed_streak"] == 0

    assert db.bump_wake_streak("s1", path=db_path) == (1, 0)
    assert db.bump_wake_streak("s1", path=db_path) == (2, 0)
    db.set_wake_escalated("s1", path=db_path)
    ws = db.get_wake_state("s1", path=db_path)
    assert ws["escalated"] == 1

    # a liveness-proving outcome closes the episode
    db.record_wake_outcome("s1", "verified", path=db_path)
    ws = db.get_wake_state("s1", path=db_path)
    assert ws["last_outcome"] == "verified"
    assert ws["unconfirmed_streak"] == 0
    assert ws["escalated"] == 0


def test_item21_wake_outcome_preserves_streak(db_path):
    """Non-alive outcomes journal the outcome without touching the streak."""
    db.bump_wake_streak("s1", path=db_path)
    db.bump_wake_streak("s1", path=db_path)
    db.record_wake_outcome("s1", "failed:timeout", path=db_path)
    ws = db.get_wake_state("s1", path=db_path)
    assert ws["last_outcome"] == "failed:timeout"
    assert ws["unconfirmed_streak"] == 2


def test_item21_list_wake_state(db_path):
    db.record_wake_outcome("s1", "sent", path=db_path)
    db.record_wake_outcome("s2", "sent_unconfirmed", path=db_path)
    rows = db.list_wake_state(path=db_path)
    assert set(rows) == {"s1", "s2"}
    assert rows["s2"]["last_outcome"] == "sent_unconfirmed"


# --- item_122: incident/stall hardening ------------------------------------


def test_item122_gwt1_managed_orchestrator_refuses_stale_takeover(db_path):
    """[ITEM-122.GWT-1] a managed, still-active orchestrator blocks CAS
    takeover at ANY heartbeat age - DRAIN sessions are stale-hb
    between turns by design; only --force or a dead verdict clears."""
    res = db.register("orc1", "/repo", role="orchestrator", path=db_path)
    assert res.accepted
    db.set_session_mode("orc1", "managed", path=db_path)

    # heartbeat ages past stale - an unmanaged holder would demote here
    _exec(
        db_path,
        "UPDATE worker_heartbeats SET last_seen_at=? WHERE session_id=?",
        (int(time.time()) - db.HEARTBEAT_STALE_SECONDS - 1, "orc1"),
    )
    res = db.register("orc2", "/repo2", role="orchestrator", path=db_path)
    assert res.accepted is False
    assert res.holder == "orc1"
    assert res.heartbeat_age > db.HEARTBEAT_STALE_SECONDS
    assert db.get_session("orc2", path=db_path) is None
    assert db.get_session("orc1", path=db_path)["role"] == "orchestrator"

    # --force still takes the slot unconditionally
    res = db.register("orc2", "/repo2", role="orchestrator", force=True, path=db_path)
    assert res.accepted is True
    assert db.get_session("orc1", path=db_path)["role"] == "worker"


def test_item122_gwt2_dead_managed_orchestrator_does_not_block(db_path):
    """[ITEM-122.GWT-2] the protection ends at the supervisor's dead
    verdict: a managed orchestrator marked dead is demotable without
    force - it is provably gone, not merely between turns."""
    res = db.register("orc1", "/repo", role="orchestrator", path=db_path)
    assert res.accepted
    db.set_session_mode("orc1", "managed", path=db_path)
    assert db.set_session_status("orc1", "dead", path=db_path)

    res = db.register("orc2", "/repo2", role="orchestrator", path=db_path)
    assert res.accepted is True
    assert db.get_session("orc1", path=db_path)["role"] == "worker"


def test_item122_gwt3_heartbeat_revives_dead_latch(db_path):
    """[ITEM-122.GWT-3] a heartbeat is authoritative liveness: a
    dead-flagged session that heartbeats clears its own latch -
    no manual re-register needed (orchestrator_dead auto-recovery)."""
    res = db.register("orc1", "/repo", role="orchestrator", path=db_path)
    assert res.accepted
    assert db.set_session_status("orc1", "dead", path=db_path)
    assert db.get_session("orc1", path=db_path)["status"] == "dead"

    db.heartbeat("orc1", path=db_path)
    assert db.get_session("orc1", path=db_path)["status"] == "active"


def test_item122_gwt4_worker_heartbeat_also_revives(db_path):
    """[ITEM-122.GWT-4] revival is not orchestrator-only: any unmanaged
    session whose process provably heartbeats was never really gone -
    the row mirrors register()'s revive-on-sighting semantics."""
    res = db.register("w1", "/repo", path=db_path)
    assert res.accepted
    assert db.set_session_status("w1", "dead", path=db_path)
    db.heartbeat("w1", path=db_path)
    assert db.get_session("w1", path=db_path)["status"] == "active"


def test_item122_gwt5_managed_heartbeat_does_not_revive(db_path):
    """[ITEM-122.GWT-5] a MANAGED dead row stays dead on heartbeat:
    its verdict comes from the no_session streak, which a heartbeat
    cannot refute - and an observer-shell heartbeat loop outliving
    the session would otherwise drive a revive/re-dead flap that
    requeues the live task to terminal failure (review finding).
    Recovery for managed sessions goes through register()."""
    res = db.register("m1", "/repo", path=db_path)
    assert res.accepted
    db.set_session_mode("m1", "managed", path=db_path)
    assert db.set_session_status("m1", "dead", path=db_path)
    db.heartbeat("m1", path=db_path)
    assert db.get_session("m1", path=db_path)["status"] == "dead"

    # ...but an explicit register still revives it - deliberate
    # operator/session intent, never emitted by an observer shell
    res = db.register("m1", "/repo", path=db_path)
    assert res.accepted
    assert db.get_session("m1", path=db_path)["status"] == "active"


# --- item_139: wait_until stamp + undrained-mail stall bookkeeping -----------


def test_item139_gwt1_wait_task_stamps_wait_until(db_path):
    """[ITEM-139.GWT-1] `bridge wait` mirrors the renewed lease into
    wait_until: the daemon wake query reads the stamp without a second
    signal channel, and it decays with the lease on its own."""
    db.register("w1", "/repo", path=db_path)
    tid = _enqueue_running(db_path, "w1", owner="w1")

    before = int(time.time())
    assert db.wait_task(tid, "w1", "waiting on CI", path=db_path)
    task = db.get_task(tid, path=db_path)
    assert task["wait_until"] == task["lease_expires_at"]
    assert task["wait_until"] >= before + db.LEASE_SECONDS - 2


def test_item139_gwt2_sessions_with_undrained_mail(db_path):
    """[ITEM-139.GWT-2] the stall query sees only relayed+unacked
    actionable mail: pending, acked and heartbeat_tick rows are all
    excluded; oldest is the MIN over what remains."""
    db.register("s1", "/repo", path=db_path)
    old = db.notify("s1", NotificationKind.DIRECTIVE, "old", path=db_path)
    tick = db.notify("s1", NotificationKind.HEARTBEAT_TICK, "tick", path=db_path)
    new = db.notify("s1", NotificationKind.TASK_DISPATCH, "new", path=db_path)
    db.notify("s1", NotificationKind.DIRECTIVE, "not relayed", path=db_path)
    for nid in (old, tick, new):
        db.mark_relayed(nid, path=db_path)
    ts = int(time.time())
    _exec(
        db_path,
        "UPDATE notifications SET created_at=? WHERE id=?",
        (ts - 3600, old),
    )
    _exec(
        db_path,
        "UPDATE notifications SET created_at=? WHERE id=?",
        (ts - 9000, tick),
    )

    res = db.sessions_with_undrained_mail(path=db_path)
    assert res["s1"]["count"] == 2  # directive + task_dispatch, not the tick
    assert res["s1"]["oldest"] == ts - 3600  # oldest ACTIONABLE, not the tick's

    db.ack_notifications("s1", path=db_path)
    assert db.sessions_with_undrained_mail(path=db_path) == {}


def test_item139_gwt3_mail_escalated_stamp_dedups_until_cleared(db_path):
    """[ITEM-139.GWT-3] one escalation per stall episode: the second
    mark returns False; clearing after drain arms the next episode."""
    assert db.mark_mail_escalated("s1", path=db_path)
    assert not db.mark_mail_escalated("s1", path=db_path)
    assert db.list_wake_state(path=db_path)["s1"]["mail_escalated_at"] is not None
    assert db.clear_mail_escalated("s1", path=db_path)
    assert db.mark_mail_escalated("s1", path=db_path)


def test_item189_gwt8_auto_renew_cas_and_ceiling(db_path):
    """[ITEM-189.GWT-8] auto_renew_lease CAS shape: renews only a
    running/dispatch_decided row under the given owner whose last
    explicit lease touch is younger than the ceiling; preserves
    lease_renewed_at so the ceiling baseline never moves."""
    tid = db.enqueue("s1", "x", path=db_path)
    assert db.claim_managed(tid, "s1", "w1", path=db_path)
    row = db.get_task(tid, path=db_path)
    touched = row["lease_renewed_at"]

    # wrong owner -> refused
    assert db.auto_renew_lease(tid, "w2", ceiling_seconds=3600, path=db_path) is False
    # right owner, fresh touch -> renewed a full lease ahead
    assert db.auto_renew_lease(tid, "w1", ceiling_seconds=3600, path=db_path) is True
    row = db.get_task(tid, path=db_path)
    assert row["lease_expires_at"] >= int(time.time()) + db.LEASE_SECONDS - 5
    assert row["lease_renewed_at"] == touched  # baseline preserved

    # explicit touch aged past the ceiling -> refused (zombie bound)
    _exec(
        db_path,
        "UPDATE tasks SET lease_renewed_at=? WHERE id=?",
        (int(time.time()) - 7200, tid),
    )
    assert db.auto_renew_lease(tid, "w1", ceiling_seconds=3600, path=db_path) is False

    # non-lease-live statuses never qualify
    assert db.finish_task(tid, "w1", ok=True, note="done", path=db_path) is True
    assert db.auto_renew_lease(tid, "w1", ceiling_seconds=3600, path=db_path) is False


def test_item198_gwt8_unregister_session_counts_and_spared_audit(db_path):
    """[ITEM-198.GWT-8] unregister_session deletes every
    session-scoped row (sessions, heartbeats, tasks, notifications,
    questions, schedules, wake_state), re-pends questions OTHER
    askers lost their claimer to, spares audit_events, and returns
    None for an unknown sid."""
    res = db.register("ghost", "/r", path=db_path)
    assert res.accepted
    tid = db.enqueue("ghost", "x", path=db_path)
    db.notify("ghost", NotificationKind.DISARM, "bye", path=db_path)
    db.heartbeat("ghost", path=db_path)
    # live tasks refuse the whole op - fold the fixture to terminal
    # so the delete path itself is what the counts prove; the
    # heartbeat row is aged too (a fresh one also refuses)
    _exec(db_path, "UPDATE tasks SET status='done' WHERE id=?", (tid,))
    _exec(
        db_path,
        "UPDATE worker_heartbeats SET last_seen_at=? WHERE session_id='ghost'",
        (int(time.time()) - db.HEARTBEAT_STALE_SECONDS - 60,),
    )
    # another asker's question the ghost had claimed must re-pend,
    # not dangle on a dead claimer
    res2 = db.register("asker", "/r2", path=db_path)
    assert res2.accepted
    res3 = db.register("orc", "/r3", role="orchestrator", path=db_path)
    assert res3.accepted
    tid2 = db.enqueue("asker", "y", path=db_path)
    qid = db.ask("asker", tid2, "q?", path=db_path).question_id
    assert db.claim_question(qid, "ghost", path=db_path)

    verdict = db.unregister_session("ghost", path=db_path)

    assert isinstance(verdict, UnregisterDeleted)
    counts = verdict.counts
    assert counts["sessions"] == 1 and counts["tasks"] == 1
    assert counts["notifications"] == 1
    assert counts["worker_heartbeats"] == 1
    assert counts["questions_unclaimed"] == 1
    assert db.get_session("ghost", path=db_path) is None
    assert db.get_task(tid, path=db_path) is None
    row = db.get_question(qid, path=db_path)
    assert row["state"] == "pending" and row["claimed_by"] is None
    # the asker is untouched
    assert db.get_session("asker", path=db_path) is not None
    assert db.get_task(tid2, path=db_path) is not None
    # unknown sid -> UnregisterNotFound, nothing deleted
    assert isinstance(db.unregister_session("nope", path=db_path), UnregisterNotFound)


def test_item198_gwt9_unregister_refusals_inside_txn(db_path):
    """[ITEM-198.GWT-9] the db-side invariants are re-verified inside
    the deletion transaction (TOCTOU fix): a live task refuses even
    with force=True; a fresh heartbeat refuses unless force=True."""
    res = db.register("s9", "/r9", path=db_path)
    assert res.accepted
    tid = db.enqueue("s9", "x", path=db_path)
    out = db.unregister_session("s9", path=db_path)
    assert isinstance(out, UnregisterRefusedLiveTask) and out.task_id == tid
    out = db.unregister_session("s9", force=True, path=db_path)
    assert isinstance(out, UnregisterRefusedLiveTask)  # force cannot kill a live task

    # fold the queued task out of live states so the heartbeat leg is next
    _exec(db_path, "UPDATE tasks SET status='failed' WHERE id=?", (tid,))
    db.heartbeat("s9", path=db_path)
    out = db.unregister_session("s9", path=db_path)
    assert isinstance(out, UnregisterRefusedFreshHeartbeat) and out.age_seconds <= 2
    # force skips the heartbeat leg
    out = db.unregister_session("s9", force=True, path=db_path)
    assert isinstance(out, UnregisterDeleted) and out.counts["sessions"] == 1
    assert db.get_session("s9", path=db_path) is None


# --- task #207: lazy GC scan + durable decision_events --------------------------


def _session_row(path, sid, *, mode="unmanaged", updated_at=None, heartbeat=True):
    """A sessions row via register; heartbeat row dropped unless asked.

    ``db.register`` stamps the first heartbeat - the ghost class is
    a sessions row WITHOUT one (hook upsert / crashed before the
    first stamp), so the helper deletes the row for the no-hb legs.
    """
    res = db.register(sid, f"/repo/{sid}", path=path)
    assert res.accepted
    if mode != "unmanaged":
        _exec(path, "UPDATE sessions SET mode=? WHERE session_id=?", (mode, sid))
    if updated_at is not None:
        _exec(path, "UPDATE sessions SET updated_at=? WHERE session_id=?", (updated_at, sid))
    if not heartbeat:
        _exec(path, "DELETE FROM worker_heartbeats WHERE session_id=?", (sid,))


def test_item207_gwt1_stale_scan_sees_ghosts(db_path):
    """[ITEM-207.GWT-1] the shared stale scan carries BOTH unmanaged
    dead signals: a stamped heartbeat gone silent past
    HEARTBEAT_STALE_SECONDS, and NO heartbeat row with updated_at
    past GHOST_TTL_SECONDS - the register-then-vanish ghost class
    the old JOIN dropped forever (impossible-vibraphone blind spot)."""
    now = int(time.time())
    ghost = now - db.GHOST_TTL_SECONDS - 60
    stale_hb = now - db.HEARTBEAT_STALE_SECONDS - 60

    # ghost: no hb row, updated_at past the ghost TTL -> stale
    _session_row(db_path, "g_old", updated_at=ghost, heartbeat=False)
    # fresh ghost: no hb row but recent -> alive (the TTL is a grace
    # window for live unmanaged sessions that never heartbeat)
    _session_row(db_path, "g_fresh", updated_at=now - 60, heartbeat=False)
    # stamped-then-silent: the classic sweep signal
    _session_row(db_path, "h_stale", updated_at=stale_hb)
    _exec(
        db_path,
        "UPDATE worker_heartbeats SET last_seen_at=? WHERE session_id=?",
        (stale_hb, "h_stale"),
    )
    # stamped and fresh -> alive
    _session_row(db_path, "h_fresh", updated_at=now - 60)
    # managed session excluded even with a stale heart
    _session_row(db_path, "m_stale", mode="managed", updated_at=stale_hb)
    _exec(
        db_path,
        "UPDATE worker_heartbeats SET last_seen_at=? WHERE session_id=?",
        (stale_hb, "m_stale"),
    )

    sids = {r["sid"]: r for r in db.stale_unmanaged_sessions(now, path=db_path)}
    assert set(sids) == {"g_old", "h_stale"}
    assert sids["g_old"]["hb"] is None  # the ghost leg is distinguishable
    assert sids["h_stale"]["hb"] == stale_hb


def test_item207_gwt2_decision_events_append_and_read(db_path):
    """[ITEM-207.GWT-2] record_event appends durable rows; list_events
    returns newest-first with session/kind filters; bad kinds are
    refused at the gate (typo namespaces would splinter the log)."""
    eid = db.record_event("orc", "contract", "task 207 signed", task_id=207, path=db_path)
    db.record_event("orc", "review_verdict", "PR 87 approved", path=db_path)
    db.record_event(None, "plan_delta", "fleet halt lifted", path=db_path)
    db.record_event("w1", "contract", "task 210 signed", path=db_path)

    all_rows = db.list_events(path=db_path)
    assert [r["id"] for r in all_rows] == sorted((r["id"] for r in all_rows), reverse=True)
    assert all_rows[0]["kind"] == "contract" and all_rows[0]["session_id"] == "w1"

    orc = db.list_events(session_id="orc", path=db_path)
    assert len(orc) == 2 and orc[-1]["task_id"] == 207
    contracts = db.list_events(kind="contract", path=db_path)
    assert {r["session_id"] for r in contracts} == {"orc", "w1"}
    bus = db.list_events(session_id=None, path=db_path)
    assert len(bus) == len(all_rows)  # no filter reads everything
    assert db.list_events(kind="plan_delta", path=db_path)[0]["session_id"] is None
    assert isinstance(eid, int) and eid > 0

    with pytest.raises(ValueError):
        db.record_event("orc", "BAD KIND!", "x", path=db_path)
    with pytest.raises(ValueError):
        db.record_event("orc", "contract", "   ", path=db_path)


# --- task #208 (directive 426): host-level resource allocations ------------------


def test_item208_gwt1_reserve_grants_and_conflicts_atomic(db_path):
    """[ITEM-208.GWT-1] reserve grants a free (type,id) key; a second
    project on the same key gets ReserveConflict echoing the
    incumbent owner - the PRIMARY KEY is the mutex, no
    check-then-write window."""
    v = db.reserve("tcp_port", "5678", "trade-ai", purpose="n8n", path=db_path)
    assert isinstance(v, ReserveGranted) and v.renewed is False and v.expires_at is None

    v = db.reserve("tcp_port", "5678", "rival", path=db_path)
    assert isinstance(v, ReserveConflict)
    assert v.owner == "trade-ai" and v.purpose == "n8n"

    # a different resource_id or type is an independent key
    assert isinstance(db.reserve("tcp_port", "5679", "rival", path=db_path), ReserveGranted)
    assert isinstance(db.reserve("netns_id", "5678", "rival", path=db_path), ReserveGranted)


def test_item208_gwt2_same_owner_renew_not_conflict(db_path):
    """[ITEM-208.GWT-2] a re-reserve by the SAME project renews the
    row (updated_at/expires_at/purpose touched) - a deploy
    pre-flight calling reserve() every run is idempotent, not a
    false conflict."""
    db.reserve("tcp_port", "5678", "trade-ai", purpose="n8n", path=db_path)
    v = db.reserve("tcp_port", "5678", "trade-ai", ttl=600, path=db_path)
    assert isinstance(v, ReserveGranted) and v.renewed is True
    assert v.expires_at is not None and v.expires_at > int(time.time())
    rows = db.list_allocations(path=db_path)
    assert len(rows) == 1 and rows[0]["expires_at"] == v.expires_at


def test_item208_gwt3_release_owner_only_force_override(db_path):
    """[ITEM-208.GWT-3] release is owner-bound: a foreign project is
    refused (naming the incumbent), the owner frees the key, and
    --force - the orchestrator's arbitration override - evicts a
    foreign holder."""
    db.reserve("tcp_port", "8443", "trade-ai", path=db_path)
    v = db.release("tcp_port", "8443", "rival", path=db_path)
    assert isinstance(v, ReleaseRefusedOwner) and v.owner == "trade-ai"
    assert db.list_allocations(path=db_path) != []  # still held

    v = db.release("tcp_port", "8443", "trade-ai", path=db_path)
    assert isinstance(v, ReleaseRemoved) and v.owner == "trade-ai"
    assert isinstance(db.release("tcp_port", "8443", "trade-ai", path=db_path), ReleaseNotFound)

    db.reserve("tcp_port", "8080", "rival", path=db_path)
    v = db.release("tcp_port", "8080", "orc", force=True, path=db_path)
    assert isinstance(v, ReleaseRemoved) and v.owner == "rival"


def test_item208_gwt4_ttl_lazy_gc_frees_key(db_path):
    """[ITEM-208.GWT-4] a TTL'd allocation sweeps itself on the next
    registry call once expired - no background watcher; the freed
    key is reservable inside the same atomic call."""
    v = db.reserve("tcp_port", "9223", "tmp", ttl=1, path=db_path)
    assert isinstance(v, ReserveGranted)
    _exec(db_path, "UPDATE allocations SET expires_at=expires_at-10")
    # the expired row is invisible to the list and free to re-reserve
    assert db.list_allocations(path=db_path) == []
    db.reserve("tcp_port", "9223", "tmp", ttl=1, path=db_path)
    _exec(db_path, "UPDATE allocations SET expires_at=expires_at-10")
    v = db.reserve("tcp_port", "9223", "other", path=db_path)
    assert isinstance(v, ReserveGranted)


def test_item208_gwt5_reserve_gates_bad_input(db_path):
    """[ITEM-208.GWT-5] unknown resource_type, empty ids and a
    non-positive TTL are refused at the gate before any write."""
    with pytest.raises(ValueError):
        db.reserve("bogus_type", "1", "p", path=db_path)
    with pytest.raises(ValueError):
        db.reserve("tcp_port", "  ", "p", path=db_path)
    with pytest.raises(ValueError):
        db.reserve("tcp_port", "1", "  ", path=db_path)
    with pytest.raises(ValueError):
        db.reserve("tcp_port", "1", "p", ttl=0, path=db_path)


def test_item208_gwt6_reserve_is_atomic_under_threads(db_path):
    """[ITEM-208.GWT-6] two threads racing one key split the verdict:
    exactly one ReserveGranted, one ReserveConflict - the PK INSERT
    serializes them, no lost update."""
    results = []

    def claim(proj):
        results.append(db.reserve("tcp_port", "7777", proj, path=db_path))

    t1 = threading.Thread(target=claim, args=("alpha",))
    t2 = threading.Thread(target=claim, args=("beta",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    kinds = sorted(type(v).__name__ for v in results)
    assert kinds == ["ReserveConflict", "ReserveGranted"]
    assert len(db.list_allocations(path=db_path)) == 1


def test_item208_gwt7_wildcard_prefix_conflicts_both_directions(db_path):
    """[ITEM-208.GWT-7] systemd_unit_prefix wildcards are enforced
    leases, not advisory rows: a foreign exact `P-foo` request
    conflicts with the `P-*` holder, a `P-*` request collides with
    a foreign row it would cover, and nested foreign wildcards
    conflict. Same-project requests under an owned wildcard grant
    normally."""
    db.reserve("systemd_unit_prefix", "trade-ai-*", "trade-ai", path=db_path)
    # foreign exact key covered by the wildcard -> conflict
    v = db.reserve("systemd_unit_prefix", "trade-ai-webhook", "rival", path=db_path)
    assert isinstance(v, ReserveConflict) and v.owner == "trade-ai"
    # same project under its own wildcard -> granted
    v = db.reserve("systemd_unit_prefix", "trade-ai-webhook", "trade-ai", path=db_path)
    assert isinstance(v, ReserveGranted)
    # a foreign wildcard covering an existing foreign-held exact row
    db.reserve("systemd_unit_prefix", "other-svc", "rival", path=db_path)
    v = db.reserve("systemd_unit_prefix", "other-*", "trade-ai", path=db_path)
    assert isinstance(v, ReserveConflict) and v.owner == "rival"
    # nested foreign wildcards conflict
    db.reserve("systemd_unit_prefix", "nest-*", "rival", path=db_path)
    v = db.reserve("systemd_unit_prefix", "nest-deep-*", "trade-ai", path=db_path)
    assert isinstance(v, ReserveConflict)


def test_item208_gwt8_renew_keeps_purpose_release_gates(db_path):
    """[ITEM-208.GWT-8] same-owner renew without --purpose keeps the
    stored purpose (a re-run deploy pre-flight must not wipe the
    declaration it repeats); release applies reserve's input gates."""
    db.reserve("tcp_port", "9000", "p", purpose="api", ttl=600, path=db_path)
    v = db.reserve("tcp_port", "9000", "p", path=db_path)
    assert isinstance(v, ReserveGranted) and v.renewed
    row = db.list_allocations(path=db_path)[0]
    assert row["purpose"] == "api"
    # bare re-reserve keeps the incumbent expiry - it must not
    # silently turn a TTL'd row permanent
    assert row["expires_at"] is not None and v.expires_at == row["expires_at"]

    with pytest.raises(ValueError):
        db.release("bogus", "1", "p", path=db_path)
    with pytest.raises(ValueError):
        db.release("tcp_port", "9000", "", path=db_path)


def test_item210_gwt15_kill_detaches_live_state(db_path):
    """[ITEM-210.GWT-15] kill_session: the session flips to dead, a
    running task requeues with its lease released (queued is the only
    claimable re-entry state - leaseless dispatch_decided is
    unclaimable limbo), staged dispatch_decided folds back to queued,
    the heartbeat row is removed and a session_killed event is
    journaled - all in one transaction. Unknown sids answer
    KillNotFound."""
    assert isinstance(db.kill_session("ghost", path=db_path), KillNotFound)

    res = db.register("w1", "/repo/w", path=db_path)
    assert res.accepted
    db.heartbeat("w1", path=db_path)
    running = db.enqueue("w1", "live work", path=db_path)
    assert db.claim_queued(running, "bridge-1", Transport.ACP_PUSH, path=db_path)
    assert db.mark_running(running, "bridge-1", path=db_path)

    v = db.kill_session("w1", path=db_path)
    assert isinstance(v, Killed)
    assert v.tasks_requeued == 1 and v.heartbeat_removed and v.status_before == "active"

    t = db.get_task(running, path=db_path)
    assert t["status"] == TaskStatus.QUEUED.value
    assert t["lease_owner"] is None and t["lease_expires_at"] is None
    s = db.get_session("w1", path=db_path)
    assert s["status"] == SessionStatus.DEAD.value
    assert _one(db_path, "SELECT COUNT(*) c FROM worker_heartbeats WHERE session_id='w1'")["c"] == 0
    ev = db.list_events(session_id="w1", kind="session_killed", path=db_path)
    assert len(ev) == 1 and "tasks_requeued=1" in ev[0]["body"]

    # no limbo: a resurrected session claims the requeued task through
    # the normal managed path (review finding regression check)
    db.register("w1", "/repo/w", path=db_path)
    assert db.claim_managed(running, "w1", "w1-owner", path=db_path)
    assert db.get_task(running, path=db_path)["status"] == TaskStatus.RUNNING.value

    # a task staged as dispatch_decided folds back to queued with its
    # claim-window lease released (one live task per session -> own
    # session for this leg)
    db.register("w2", "/repo/w2", path=db_path)
    staged = db.enqueue("w2", "staged", path=db_path)
    _exec(
        db_path,
        "UPDATE tasks SET status=?, lease_owner=?, lease_expires_at=? WHERE id=?",
        (
            TaskStatus.DISPATCH_DECIDED.value,
            "bridge-2",
            int(time.time()) + 60,
            staged,
        ),
    )
    v2 = db.kill_session("w2", path=db_path)
    assert isinstance(v2, Killed) and v2.tasks_requeued == 0 and v2.staged_folded == 1
    t2 = db.get_task(staged, path=db_path)
    assert t2["status"] == TaskStatus.QUEUED.value
    assert t2["lease_owner"] is None

    # a queued task never claimed a lease and stays queued
    db.register("w3", "/repo/w3", path=db_path)
    queued = db.enqueue("w3", "next", path=db_path)
    db.kill_session("w3", path=db_path)
    assert db.get_task(queued, path=db_path)["status"] == TaskStatus.QUEUED.value

    # a leaseless dispatch_decided row is corruption (unclaimable
    # limbo) - the fold repairs it to queued too, not just leased
    # staged rows
    db.register("w5", "/repo/w5", path=db_path)
    corrupt = db.enqueue("w5", "corrupt", path=db_path)
    _exec(
        db_path,
        "UPDATE tasks SET status=? WHERE id=?",
        (TaskStatus.DISPATCH_DECIDED.value, corrupt),
    )
    v5 = db.kill_session("w5", path=db_path)
    assert isinstance(v5, Killed) and v5.staged_folded == 1
    assert db.get_task(corrupt, path=db_path)["status"] == TaskStatus.QUEUED.value

    # a repeat kill still performs its effects: w1 resurrected above
    # and re-claimed the task, so the second kill requeues it again
    v4 = db.kill_session("w1", path=db_path)
    assert isinstance(v4, Killed) and v4.status_before == "active" and v4.tasks_requeued == 1

    # idempotent on an already-dead row with nothing left to change:
    # a no-op repeat kill writes no duplicate audit event
    db.register("w4", "/repo/w4", path=db_path)
    assert isinstance(db.kill_session("w4", path=db_path), Killed)
    assert isinstance(db.kill_session("w4", path=db_path), Killed)
    assert len(db.list_events(session_id="w4", kind="session_killed", path=db_path)) == 1


# --- task #216: waiting_quota step state ------------------------------------


def _park(path, tid, sid, reset_in=3600, **kw):
    assert db.quota_park(
        tid,
        sid,
        int(time.time()) + reset_in,
        kw.pop("note", "rate limit hit"),
        kw.pop("source", "manual"),
        path=path,
        **kw,
    )


def test_quota_park_keeps_live_status_and_metadata(db_path):
    """GWT-216-1: parking a live task stores step_state orthogonally -
    the lifecycle status and the claimant's lease stay as they were."""
    db.register("s1", "/repo/s1", path=db_path)
    tid = _enqueue_running(db_path, "s1", "w1")
    _park(db_path, tid, "s1", model="claude-opus", checkpoint="step 3/5")
    row = db.get_task(tid, path=db_path)
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["step_state"] == "waiting_quota"
    assert row["lease_owner"] == "w1"
    assert row["quota_source"] == "manual"
    assert row["quota_model"] == "claude-opus"
    assert row["quota_checkpoint"] == "step 3/5"
    assert row["quota_reset_at"] > int(time.time())
    # parking a non-live task refuses - the CAS carries the live guard
    tid_done = db.enqueue("s2", "t", path=db_path)
    _exec(db_path, "UPDATE tasks SET status='done' WHERE id=?", (tid_done,))
    assert not db.quota_park(tid_done, "s2", 1, "n", "manual", path=db_path)


def test_quota_parked_task_survives_lease_expiry(db_path):
    """GWT-216-2: the park, not the claimant, owns the wait - an expired
    lease on a waiting_quota row is not folded by recover_stale."""
    db.register("s1", "/repo/s1", path=db_path)
    tid = _enqueue_running(db_path, "s1", "w1")
    _park(db_path, tid, "s1")
    _expire(db_path, tid)
    assert db.recover_stale(path=db_path) == []
    row = db.get_task(tid, path=db_path)
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["step_state"] == "waiting_quota"
    assert row["attempt"] == 0


def test_quota_parked_task_resists_second_claimant(db_path):
    """GWT-216-3: a parked task keeps its owner lease - no second
    claimant can be issued one, and a dead-worker fold skips it."""
    db.register("s1", "/repo/s1", path=db_path)
    tid = _enqueue_running(db_path, "s1", "w1")
    _park(db_path, tid, "s1")
    _expire(db_path, tid)
    # direct claim paths refuse: the task is not queued/dispatch_decided
    assert not db.claim_queued(tid, "w2", "acp-push", path=db_path)
    # dead-worker fold treats it as not-live for reclaim purposes
    assert db.requeue_dead_worker(tid, "owner died", path=db_path) is None
    row = db.get_task(tid, path=db_path)
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["lease_owner"] == "w1"
    assert row["step_state"] == "waiting_quota"


def test_unpark_quota_expired_releases_and_rearms(db_path):
    """GWT-216-4: the sweep clears only closed windows and re-arms the
    lease for a full window - the claimant resumes the step."""
    db.register("s1", "/repo/s1", path=db_path)
    tid = _enqueue_running(db_path, "s1", "w1")
    _park(db_path, tid, "s1", reset_in=3600)
    now = int(time.time())
    # future reset: nothing to sweep
    assert db.unpark_quota_expired(now, path=db_path) == []
    # close the window
    _exec(db_path, "UPDATE tasks SET quota_reset_at=? WHERE id=?", (now - 1, tid))
    _expire(db_path, tid)
    rows = db.unpark_quota_expired(now, path=db_path)
    assert [r["id"] for r in rows] == [tid]
    row = db.get_task(tid, path=db_path)
    assert row["step_state"] is None
    assert row["quota_reset_at"] is None
    assert row["quota_note"] is None
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["lease_expires_at"] >= now + db.LEASE_SECONDS - 5


def test_quota_repark_extends_reset(db_path):
    """GWT-216-5: a second quota error on the same task overwrites the
    window - repeated failures extend the park, not the attempt count."""
    db.register("s1", "/repo/s1", path=db_path)
    tid = _enqueue_running(db_path, "s1", "w1")
    now = int(time.time())
    _park(db_path, tid, "s1", reset_in=600)
    _park(db_path, tid, "s1", reset_in=7200, note="still limited")
    row = db.get_task(tid, path=db_path)
    assert row["step_state"] == "waiting_quota"
    assert row["quota_reset_at"] >= now + 7100
    assert row["quota_note"] == "still limited"
    assert row["attempt"] == 0


def test_quota_unpark_skips_halted(db_path):
    """GWT-216-6: HALT is senior to a quota reset - a halted session's
    park is not swept until resume."""
    db.register("s1", "/repo/s1", path=db_path)
    tid = _enqueue_running(db_path, "s1", "w1")
    now = int(time.time())
    _park(db_path, tid, "s1", reset_in=-10)
    db.set_session_halted("s1", True, path=db_path)
    assert db.unpark_quota_expired(now, path=db_path) == []
    assert db.get_task(tid, path=db_path)["step_state"] == "waiting_quota"
    # lifting the halt makes the expired park sweepable again
    db.set_session_halted("s1", False, path=db_path)
    assert [r["id"] for r in db.unpark_quota_expired(now, path=db_path)] == [tid]


def test_quota_resume_releases_early_and_refuses(db_path):
    """GWT-216-7: quota-resume releases a park before its reset;
    refuses on non-parked tasks and under HALT."""
    db.register("s1", "/repo/s1", path=db_path)
    tid = _enqueue_running(db_path, "s1", "w1")
    # not parked -> refuse
    assert not db.quota_resume(tid, "s1", path=db_path)
    _park(db_path, tid, "s1")
    # wrong session -> refuse
    assert not db.quota_resume(tid, "s2", path=db_path)
    # halted -> refuse
    db.set_session_halted("s1", True, path=db_path)
    assert not db.quota_resume(tid, "s1", path=db_path)
    db.set_session_halted("s1", False, path=db_path)
    # happy path
    assert db.quota_resume(tid, "s1", path=db_path)
    row = db.get_task(tid, path=db_path)
    assert row["step_state"] is None
    assert row["quota_reset_at"] is None
    assert row["status"] == TaskStatus.RUNNING.value
    assert row["lease_expires_at"] >= int(time.time()) + db.LEASE_SECONDS - 5


def test_waiting_quota_task_lookup_boundary(db_path):
    """GWT-216-8: the suppression lookup sees only open windows - an
    expired-but-unswept park does not suppress (the sweep owns it)."""
    db.register("s1", "/repo/s1", path=db_path)
    tid = _enqueue_running(db_path, "s1", "w1")
    now = int(time.time())
    assert db.waiting_quota_task("s1", now, path=db_path) is None
    _park(db_path, tid, "s1", reset_in=600)
    row = db.waiting_quota_task("s1", now, path=db_path)
    assert row["id"] == tid
    assert db.waiting_quota_task("s1", now + 601, path=db_path) is None


def test_quota_terminal_writers_clear_step_fields(db_path):
    """GWT-216-9: terminal/parked lifecycle writers reset the step
    slot - no quota metadata outlives its transition."""
    db.register("s1", "/repo/s1", path=db_path)
    tid = _enqueue_running(db_path, "s1", "w1")
    _park(db_path, tid, "s1")
    assert db.finish_task(tid, "w1", True, path=db_path)
    row = db.get_task(tid, path=db_path)
    assert row["status"] == TaskStatus.DONE.value
    assert row["step_state"] is None and row["quota_reset_at"] is None
    # escalation defer parks the task and marks the step itself
    db.register("s2", "/repo/s2", path=db_path)
    tid2 = _enqueue_running(db_path, "s2", "w2")
    _park(db_path, tid2, "s2")
    assert db.defer_task(tid2, "human parked it", path=db_path)
    row2 = db.get_task(tid2, path=db_path)
    assert row2["status"] == TaskStatus.PAUSED.value
    assert row2["step_state"] == "paused"
    assert row2["quota_reset_at"] is None
    # unpark clears the paused step
    assert db.unpark(tid2, path=db_path)
    assert db.get_task(tid2, path=db_path)["step_state"] is None


def test_lease_ttl_env_override():
    """issue #105 (directive): ``SB_LEASE_TTL_S`` resolves once at db
    import; a malformed value falls back to the contract default,
    and sub-floor values clamp at 2x the default renew margin."""
    code = "from session_bridge.state import db; print(db.LEASE_SECONDS)"
    src = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

    def resolved(env_override):
        env = {**os.environ, "PYTHONPATH": src}
        env.pop("SB_LEASE_TTL_S", None)
        env.update(env_override)
        out = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
        assert out.returncode == 0
        return out.stdout.strip()

    assert resolved({"SB_LEASE_TTL_S": "7200"}) == "7200"
    assert resolved({"SB_LEASE_TTL_S": "banana"}) == "3600"
    # below the 600s floor (2x renew margin) the resolved value clamps
    assert resolved({"SB_LEASE_TTL_S": "36"}) == "600"
    # and the no-env default is the contract's 1h regardless of the
    # ambient environment this suite was launched under
    assert resolved({}) == "3600"

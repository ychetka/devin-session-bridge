"""Persistence and concurrency tests for the state.db layer (item_04)."""

import json
import multiprocessing
import shutil
import sqlite3
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from session_bridge.state import db
from session_bridge.state.contract import (
    BUSY_TIMEOUT_MS,
    CLAIM_TIMEOUT_SECONDS,
    LEASE_SECONDS,
    MAX_ATTEMPTS,
    MAX_QUESTIONS_PER_TASK,
    AuditEventKind,
    SessionRole,
    Transport,
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
    assert version == 2
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
    assert abs(row["lease_expires_at"] - (time.time() + LEASE_SECONDS)) < 5
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
    """A ``sleep`` binary reachable under ``name`` via symlink, so the
    spawned process's argv[0] basename is exactly ``name``."""
    exe = tmp_path / name
    exe.symlink_to(shutil.which("sleep"))
    return str(exe)


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
    proc = _spawn_cmdline([_named_exe(tmp_path, "devin"), "30"], b"devin")
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
    proc2 = _spawn_cmdline([lookalike, "30"], b"devin-checkout")
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
    # user_version=2 commits with the ALTERs, so 2 certifies v2 columns
    assert _one(path, "PRAGMA user_version")[0] == 2

    # an already-migrated file: further init() stays a no-op
    db.init(path)
    assert db.get_session("s1", path=path)["cwd"] == "/old"


def _ask(db_path, session_id="w1", task_id=1, text="which branch?"):
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

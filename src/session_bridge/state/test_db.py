"""Persistence and concurrency tests for the state.db layer (item_04)."""

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
    LEASE_SECONDS,
    MAX_ATTEMPTS,
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
                lambda o: db.claim_queued(
                    tid, o, "acp-push", path=db_path
                ),
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

        def do_recover():
            outcome["recovered"] = db.recover_stale(path=db_path)

        def do_claim():
            # claim only wins once the requeue has landed
            for _ in range(1000):
                if db.claim_queued(
                    tid, "w_new", "acp-push", path=db_path
                ):
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
        assert not (
            row["status"] == "running" and row["lease_owner"] == "w_old"
        )
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
    procs = [
        ctx.Process(
            target=_writer_proc, args=(path, f"w{i}-s", per_writer)
        )
        for i in range(2)
    ]
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
    assert version == 1
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
    assert (
        db.finish_task(
            tid, "w_old", True, last_assistant_message="stale", path=db_path
        )
        is False
    )
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
    assert db.finish_task(
        tid, "w1", True, last_assistant_message="all done", path=db_path
    )
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
    con.execute(
        "CREATE TABLE sessions(id TEXT PRIMARY KEY,"
        " working_directory TEXT, title TEXT)"
    )
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
    assert db.release_lease(
        tid, "w1", expected_status="dispatch_decided", path=db_path
    ) is False
    row = db.get_task(tid, path=db_path)
    assert row["status"] == "running"
    assert row["lease_owner"] == "w1"
    assert row["lease_expires_at"] is not None

    # the release still lands while the task is in the expected status
    tid2 = db.enqueue("s2", "x", path=db_path)
    assert db.claim_queued(tid2, "w1", "acp-push", path=db_path)
    assert db.release_lease(
        tid2, "w1", expected_status="dispatch_decided", path=db_path
    ) is True
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


def test_item04_trigger_filename_sanitized():
    assert db.trigger_filename("abc-123_OK.9") == "abc-123_OK.9.json"
    # traversal segments and slashes cannot escape the queue dir
    for sid in ("../x", "..", "a/b", "a\\b", "...", ".env"):
        name = db.trigger_filename(sid)
        assert "/" not in name and "\\" not in name
        assert not name.startswith(".")
    assert db.trigger_filename("") == "sid.json"

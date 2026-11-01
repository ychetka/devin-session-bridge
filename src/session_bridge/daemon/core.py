"""bridge-daemon: external supervisor + outbox relay (item_13d).

The daemon is a supervisor only - it never injects anything into a
session. A single instance per state dir is enforced by
``flock(daemon.lock)``; each loop pass:

1. materializes due recurring schedules: every enabled
   ``schedules`` row with ``next_run_at <= now`` becomes a regular
   ``queued`` task plus a ``task_dispatch`` outbox row in one
   transaction (``db.materialize_schedule``; the CAS advance makes a
   double fire impossible, a busy live-task slot defers the fire to
   the next pass); runs first so the relay below ships the fresh
   outbox rows in the same pass;
2. relays the notifications outbox into fs mailboxes
   (``unrelayed_notifications`` -> ``mailbox.write_message`` ->
   ``mark_relayed``; at-least-once - a crash between the fs write and
   the mark re-delivers on the next pass, consumers dedup by id);
3. sweeps heartbeat staleness: a session whose ``last_seen_at`` is
   older than ``HEARTBEAT_STALE_SECONDS`` flips to ``status='dead'``;
   a dead worker's live task is requeued carrying the reset-to-base
   instruction for the next worker, a silent orchestrator raises one
   deduped ``notify-send`` (both deduped by the ``active > dead``
   transition-CAS in ``set_session_status``);
4. janitors stale question claims: ``db.janitor_questions`` returns
   every claim older than ``CLAIM_TIMEOUT_SECONDS`` to ``pending``,
   so a claim whose orchestrator died mid-deliberation never wedges
   the question (one released ids line per pass in the decision log);
5. stamps ``daemon_heartbeat.txt`` so an external watchdog
   (cron/systemd timer) can detect a dead or hung daemon.

Every decision is appended to ``daemon_decisions.jsonl`` under the
state dir (one JSON object per line: ``{ts, event, sid, detail}``) and
echoed to stderr, which ``bridge daemon start`` captures in the logs
dir.
"""

import errno
import fcntl
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

from session_bridge.mailbox import write_message
from session_bridge.state import db
from session_bridge.state.contract import SessionMode, SessionRole, SessionStatus
from session_bridge.wake import WakeError, WakePolicy, find_lockfile, wake_session

__all__ = [
    "DECISION_LOG_FILENAME",
    "HEARTBEAT_FILENAME",
    "LOCK_FILENAME",
    "POLL_INTERVAL_SECONDS",
    "RUN_LOCK_HELD",
    "RUN_OK",
    "STARTUP_JITTER_SECONDS",
    "daemon_pid",
    "daemon_running",
    "heartbeat_age",
    "heartbeat_path",
    "lock_path",
    "relay_payload",
    "requeue_note",
    "run",
    "state_dir",
]

#: seconds between relay+sweep passes in the long-running loop.
POLL_INTERVAL_SECONDS = 5

#: random delay before the first pass, so a fleet restart does not
#: stampede state.db in lockstep.
STARTUP_JITTER_SECONDS = 1.0

#: files the daemon owns inside the state dir (next to state.db).
LOCK_FILENAME = "daemon.lock"
HEARTBEAT_FILENAME = "daemon_heartbeat.txt"
DECISION_LOG_FILENAME = "daemon_decisions.jsonl"

#: run() return codes.
RUN_OK = 0
RUN_LOCK_HELD = 4

#: env override for the notify-send binary (tests, non-desktop hosts).
_NOTIFY_SEND_ENV = "SESSIONS_BRIDGE_NOTIFY_SEND"
#: env override for the loop interval.
_INTERVAL_ENV = "SESSIONS_BRIDGE_DAEMON_INTERVAL"

_LOCK_ERRNOS = (errno.EACCES, errno.EAGAIN)


def state_dir(db_path=None) -> str:
    """The daemon's working directory: the dir holding state.db."""
    base = db_path if db_path is not None else db.DB_PATH
    return os.path.dirname(os.path.abspath(base))


def lock_path(state_directory: str) -> str:
    return os.path.join(state_directory, LOCK_FILENAME)


def heartbeat_path(state_directory: str) -> str:
    return os.path.join(state_directory, HEARTBEAT_FILENAME)


def _decision_log_path(state_directory: str) -> str:
    return os.path.join(state_directory, DECISION_LOG_FILENAME)


# --- relay payload ------------------------------------------------------------


def relay_payload(notification) -> str:
    """fs mailbox body for one outbox row.

    Header lines route the delivery: ``notification_id`` and ``kind``
    always, ``task_id``/``question_id`` when the refs are set; a blank
    line, then the body verbatim. ``kind`` is required in the payload -
    the managed session's duty text routes deliveries by it (the file
    name ``msg_<id>.txt`` alone cannot carry it).
    """
    lines = [
        f"notification_id: {notification['id']}",
        f"kind: {notification['kind']}",
    ]
    if notification["task_id"] is not None:
        lines.append(f"task_id: {notification['task_id']}")
    if notification["question_id"] is not None:
        lines.append(f"question_id: {notification['question_id']}")
    return "\n".join(lines) + "\n\n" + notification["body"]


def requeue_note(task_id: int) -> str:
    """Reset instruction stamped on a dead worker's requeued task.

    Text instruction for the next worker: the dead worker may have
    left a dirty worktree, so the replacement stashes it and resets
    to the task's base ref before starting.
    """
    return (
        "requeued-dead-worker: stash dirty state "
        f"(git stash -u -m dead-worker-backup-{task_id}) and reset "
        "the worktree to the task's base ref before starting"
    )


# --- flock singleton ------------------------------------------------------------


def _acquire_lock(state_directory: str):
    """flock ``daemon.lock`` LOCK_EX|LOCK_NB; returns the held fd.

    The fd doubles as the pid file: the daemon writes its pid into it
    after the lock is won, so ``daemon stop``/``status`` can find the
    holder. A second daemon (or a probe while one runs) gets ``None``.
    Releasing/closing the fd - or process death - drops the lock.
    """
    path = lock_path(state_directory)
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    except OSError:
        return None
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as e:
        os.close(fd)
        if e.errno in _LOCK_ERRNOS:
            return None
        raise
    try:
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())
    except OSError:
        pass  # the lock is held either way; the pid is best-effort
    return fd


def daemon_running(state_directory: str) -> bool:
    """True while some process holds ``daemon.lock``."""
    path = lock_path(state_directory)
    if not os.path.exists(path):
        return False
    try:
        fd = os.open(path, os.O_RDWR)
    except OSError:
        return False
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            if e.errno in _LOCK_ERRNOS:
                return True
            raise
        return False
    finally:
        os.close(fd)


def daemon_pid(state_directory: str):
    """The pid recorded inside ``daemon.lock``, or None."""
    try:
        with open(lock_path(state_directory)) as fh:
            return int(fh.read().strip().splitlines()[0])
    except (OSError, ValueError, IndexError):
        return None


# --- daemon heartbeat ------------------------------------------------------------


def _write_heartbeat(state_directory: str) -> None:
    """Stamp ``daemon_heartbeat.txt`` with the current unix time."""
    target = heartbeat_path(state_directory)
    fd, tmp = tempfile.mkstemp(prefix=".hb-", dir=state_directory)
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(f"{int(time.time())}\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, target)
        tmp = None
    finally:
        if tmp is not None:
            try:
                os.unlink(tmp)
            except OSError:
                pass


def heartbeat_age(state_directory: str):
    """Seconds since the daemon's last heartbeat stamp.

    Falls back to the file mtime when the content is unparseable;
    ``None`` when no heartbeat file exists at all.
    """
    path = heartbeat_path(state_directory)
    try:
        with open(path) as fh:
            ts = int(fh.read().strip().splitlines()[0])
    except (OSError, ValueError, IndexError):
        try:
            ts = int(os.path.getmtime(path))
        except OSError:
            return None
    return max(0, int(time.time()) - ts)


# --- decision log ------------------------------------------------------------------


def _decision(state_directory: str, event: str, sid=None, detail=None) -> None:
    """Append one ``{ts, event, sid, detail}`` jsonl line + stderr echo."""
    rec = {"ts": int(time.time()), "event": event, "sid": sid, "detail": detail}
    line = json.dumps(rec, separators=(",", ":"))
    try:
        sys.stderr.write(f"sessions-bridge-daemon: {line}\n")
        sys.stderr.flush()
    except OSError:
        pass
    try:
        with open(_decision_log_path(state_directory), "a") as fh:
            fh.write(line + "\n")
    except OSError:
        pass


# --- notify-send ---------------------------------------------------------------------


def _notify_send(title: str, body: str) -> None:
    """``notify-send -u critical``; fail-safe when the binary is absent.

    ``SESSIONS_BRIDGE_NOTIFY_SEND`` overrides the binary path (tests
    point it at a recording script); ``run`` also accepts an injected
    callable.
    """
    exe = os.environ.get(_NOTIFY_SEND_ENV) or shutil.which("notify-send")
    if not exe:
        return
    try:
        subprocess.run(
            [exe, "-u", "critical", title, body],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        pass


# --- loop passes ----------------------------------------------------------------------


def _schedule_pass(db_path, state_directory, now: int) -> None:
    """Materialize due recurring schedules into queued tasks.

    Each due row becomes a regular ``queued`` task plus a
    ``task_dispatch`` outbox notification inside
    ``db.materialize_schedule``'s single transaction - the CAS advance
    makes a double fire impossible even under a second daemon, and a
    session already holding a live task keeps the schedule due for the
    next pass instead of dropping the fire (never queue-until-online:
    the queued task plus level-triggered mailbox cover a dead or busy
    session; there is no drop policy). One bad schedule never sinks
    the pass - the error is logged and the sweep continues.
    """
    for s in db.due_schedules(now, path=db_path):
        sid = s["session_id"]
        try:
            task_id = db.materialize_schedule(s["id"], now, path=db_path)
        except Exception as e:
            _decision(
                state_directory,
                "schedule_error",
                sid,
                f"schedule={s['id']}: {type(e).__name__}: {e}",
            )
            continue
        if task_id is None:
            _decision(
                state_directory,
                "schedule_deferred",
                sid,
                f"schedule={s['id']} not materialized (CAS lost or live task slot busy)",
            )
            continue
        _decision(
            state_directory,
            "schedule_fired",
            sid,
            f"schedule={s['id']} task_id={task_id}",
        )


def _relay_pass(db_path, queue_dir, state_directory) -> None:
    """Drain the outbox: unrelayed -> fs msg file -> mark_relayed."""
    for row in db.unrelayed_notifications(path=db_path):
        try:
            path = write_message(
                row["session_id"],
                row["id"],
                relay_payload(row),
                db_path=db_path,
                queue_dir=queue_dir,
            )
        except OSError as e:
            _decision(
                state_directory,
                "relay_error",
                row["session_id"],
                f"notification={row['id']}: {e}",
            )
            continue
        marked = db.mark_relayed(row["id"], path=db_path)
        _decision(
            state_directory,
            "relayed",
            row["session_id"],
            f"notification={row['id']} kind={row['kind']} file={path}"
            f" mark={'ok' if marked else 'superseded'}",
        )


def _stale_sessions(db_path, now: int):
    """Sessions whose heartbeat is older than HEARTBEAT_STALE_SECONDS."""
    cutoff = now - db.HEARTBEAT_STALE_SECONDS
    con = db.connect(db_path)
    try:
        return con.execute(
            "SELECT s.session_id AS sid, s.role AS role, s.status AS status,"
            " wh.last_seen_at AS seen"
            " FROM sessions s"
            " JOIN worker_heartbeats wh ON wh.session_id=s.session_id"
            " WHERE wh.last_seen_at<=? ORDER BY s.session_id",
            (cutoff,),
        ).fetchall()
    finally:
        con.close()


def _sweep_pass(db_path, state_directory, notify_send, now: int) -> None:
    """Heartbeat staleness sweep.

    A silent orchestrator raises one deduped OS notification. A dead
    worker's live task is requeued with the reset-to-base instruction.
    Both fire only on the ``active > dead`` transition (the
    ``set_session_status`` CAS), so a session that stays dead is never
    re-notified or re-requeued on later passes.
    """
    for row in _stale_sessions(db_path, now):
        sid = row["sid"]
        age = now - int(row["seen"])
        if row["role"] == SessionRole.ORCHESTRATOR.value:
            if db.set_session_status(sid, SessionStatus.DEAD, path=db_path):
                notify_send(
                    "sessions-bridge ORCHESTRATOR DEAD",
                    f"{sid}: heartbeat silent {age}s (limit {db.HEARTBEAT_STALE_SECONDS}s)",
                )
                _decision(
                    state_directory,
                    "orchestrator_dead",
                    sid,
                    f"heartbeat_age={age}s notify-send sent",
                )
            continue
        if not db.set_session_status(sid, SessionStatus.DEAD, path=db_path):
            continue  # already dead or raced - handled before
        task = db.get_live_task(sid, path=db_path)
        if task is None:
            _decision(
                state_directory,
                "worker_dead",
                sid,
                f"heartbeat_age={age}s no live task",
            )
            continue
        result = db.requeue_dead_worker(task["id"], note=requeue_note(task["id"]), path=db_path)
        outcome = result.value if result is not None else "unchanged"
        _decision(
            state_directory,
            "worker_dead",
            sid,
            f"heartbeat_age={age}s task={task['id']} -> {outcome}",
        )


# Hard ceiling for one _wake_pass: the poll loop must never stall on
# wake work - leftovers are retried by the next pass.
_WAKE_PASS_DEADLINE_SECONDS = 120


def _wake_candidates(db_path, now: int):
    """Managed sessions with stale heartbeat and unacked relayed mail.

    These are the sessions the mailbox alone cannot reach: no turn is
    open (heartbeat stale, or never heartbeated) while mail waits in
    the fs queue, so the daemon gives them a nudge prompt through the
    acp-host (item_16).
    """
    cutoff = now - db.HEARTBEAT_STALE_SECONDS
    con = db.connect(db_path)
    try:
        return con.execute(
            "SELECT DISTINCT s.session_id AS sid, s.cwd AS cwd"
            " FROM sessions s"
            " LEFT JOIN worker_heartbeats wh ON wh.session_id=s.session_id"
            " WHERE s.mode=?"
            " AND (wh.last_seen_at IS NULL OR wh.last_seen_at<=?)"
            " AND EXISTS (SELECT 1 FROM notifications n"
            "             WHERE n.session_id=s.session_id"
            "               AND n.relayed_at IS NOT NULL"
            "               AND n.ack_at IS NULL)",
            (SessionMode.MANAGED.value, cutoff),
        ).fetchall()
    finally:
        con.close()


def _wake_pass(db_path, state_directory, now: int, policy: WakePolicy) -> None:
    """Nudge idle managed sessions that have mail waiting (item_16).

    Runs after the relay pass so the same pass can already wake a
    recipient of fresh mail. The wake carries no payload - it is a
    "drain your mailbox" prompt over the acp-host ACP channel; the fs
    mailbox stays the only data channel, so a lost or double wake is
    harmless. Pacing (per-session cooldown, per-host circuit breaker)
    lives in ``policy``; outcome events land in the decision log.
    """
    deadline = time.monotonic() + _WAKE_PASS_DEADLINE_SECONDS
    for row in _wake_candidates(db_path, now):
        if time.monotonic() >= deadline:
            _decision(
                state_directory,
                "wake_pass_deadline",
                detail="remaining candidates deferred to next pass",
            )
            break
        sid = row["sid"]
        # Re-read the lockfile per attempt: the host can rotate
        # (restart -> new port/token) between candidates of one pass,
        # and a stale key must not poison the breaker.
        lock_data = find_lockfile()
        host_key = lock_data["_path"] if lock_data else "none"
        if policy.check(sid, host_key) is not None:
            continue  # cooldown or open breaker - normal pacing
        policy.record_attempt(sid)
        try:
            outcome = wake_session(sid, row["cwd"], lock_data=lock_data)
        except WakeError as e:
            opened = policy.record_result(host_key, False)
            _decision(state_directory, "wake_failed", sid, f"reason={e.reason} {e}")
            if opened:
                _decision(
                    state_directory,
                    "wake_breaker_open",
                    detail=f"host={host_key} suspended",
                )
            continue
        policy.record_result(host_key, True)
        if outcome == "busy":
            _decision(state_directory, "wake_skipped_busy", sid)
            continue
        if outcome == "sent_unconfirmed":
            # prompt written but unconfirmed - retry sooner than the
            # full cooldown so a silently missed nudge self-heals
            policy.record_retry_soon(sid)
        else:
            policy.record_confirmed(sid)
        _decision(state_directory, "wake_sent", sid, f"outcome={outcome}")


def _question_janitor_pass(db_path, state_directory) -> None:
    """Release question claims that outlived CLAIM_TIMEOUT_SECONDS.

    Runs every pass - it is a single guarded UPDATE, cheap when idle.
    A question claimed by an orchestrator that died mid-deliberation
    returns to ``pending`` (same id, no re-ask) instead of hanging
    forever; released ids land in the decision log.
    """
    released = db.janitor_questions(path=db_path)
    if released:
        _decision(
            state_directory,
            "question_claims_released",
            detail=f"released={len(released)} ids={released}",
        )


# --- main loop -------------------------------------------------------------------------


def _env_interval() -> float:
    try:
        return float(os.environ.get(_INTERVAL_ENV, "") or POLL_INTERVAL_SECONDS)
    except ValueError:
        return POLL_INTERVAL_SECONDS


def run(
    db_path=None,
    queue_dir=None,
    *,
    once=False,
    interval=None,
    notify_send=None,
    jitter=None,
) -> int:
    """Supervisor loop; returns ``RUN_OK`` or ``RUN_LOCK_HELD``.

    ``once=True`` performs a single relay+sweep pass and returns
    (tests, manual debug). ``interval`` defaults to
    ``SESSIONS_BRIDGE_DAEMON_INTERVAL`` then ``POLL_INTERVAL_SECONDS``.
    ``notify_send`` is an injectable callable ``(title, body)`` for
    tests; ``jitter`` (default ``STARTUP_JITTER_SECONDS``) is the
    random pre-loop delay that spreads fleet restarts - skipped in
    once mode.
    """
    state_directory = state_dir(db_path)
    os.makedirs(state_directory, exist_ok=True)
    lock_fd = _acquire_lock(state_directory)
    if lock_fd is None:
        _decision(
            state_directory,
            "lock_refused",
            detail="daemon.lock is held by another daemon",
        )
        return RUN_LOCK_HELD
    if interval is None:
        interval = _env_interval()
    if jitter is None:
        jitter = STARTUP_JITTER_SECONDS
    notify = notify_send or _notify_send
    wake_policy = WakePolicy()
    stop = threading.Event()

    def _on_term(_signum, _frame):
        stop.set()

    try:
        signal.signal(signal.SIGTERM, _on_term)
    except (ValueError, OSError):
        pass  # not running on the main thread (embedded/test use)

    _decision(
        state_directory,
        "daemon_start",
        detail=f"pid={os.getpid()} once={once} interval={interval}",
    )
    try:
        if not once and jitter > 0:
            stop.wait(random.uniform(0, jitter))
        while not stop.is_set():
            # the heartbeat is stamped before the pass so a hung pass
            # still ages the stamp - the external watchdog reads
            # staleness as the hang signal
            _write_heartbeat(state_directory)
            try:
                now = int(time.time())
                _schedule_pass(db_path, state_directory, now)
                _relay_pass(db_path, queue_dir, state_directory)
                _wake_pass(db_path, state_directory, now, wake_policy)
                _sweep_pass(db_path, state_directory, notify, now)
                _question_janitor_pass(db_path, state_directory)
            except Exception as e:
                if once:
                    raise
                # a failed pass must not kill the supervisor: log it
                # and retry on the next tick
                _decision(
                    state_directory,
                    "pass_error",
                    detail=f"{type(e).__name__}: {e}",
                )
            if once:
                break
            stop.wait(interval)
    finally:
        _decision(state_directory, "daemon_stop", detail=f"pid={os.getpid()}")
        os.close(lock_fd)
    return RUN_OK

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
from session_bridge.state.contract import (
    WAKE_UNCONFIRMED_ESCALATE_STREAK,
    NotificationKind,
    SessionMode,
    SessionRole,
    SessionStatus,
)
from session_bridge.wake import (
    SESSION_ABSENT,
    WakeError,
    WakePolicy,
    find_lockfile,
    status_is_idle,
    verify_sessions,
    wake_session,
)

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


def relay_payload(notification, source: str | None = None) -> str:
    """fs mailbox body for one outbox row.

    Header lines route the delivery: ``notification_id``, ``kind`` and
    ``session_id`` always - the recipient is the executing session, so
    the delivery names the side that owns the work (item_19; before,
    the only name in a dispatch note was the dispatcher's, embedded
    in body prose). ``task_id``/``question_id`` when the refs are set,
    ``from`` when the source is derivable; a blank line, then the body
    verbatim. ``kind`` is required in the payload - the managed
    session's duty text routes deliveries by it (the file name
    ``msg_<id>.txt`` alone cannot carry it).
    """

    def _hdr(v) -> str:
        # header values are line-structured: a stray newline in a
        # free-form field (e.g. --claimant) must not forge headers
        return str(v).replace("\r", " ").replace("\n", " ")

    lines = [
        f"notification_id: {notification['id']}",
        f"session_id: {_hdr(notification['session_id'])}",
        f"kind: {notification['kind']}",
    ]
    if source is not None:
        lines.append(f"from: {_hdr(source)}")
    if notification["task_id"] is not None:
        lines.append(f"task_id: {notification['task_id']}")
    if notification["question_id"] is not None:
        lines.append(f"question_id: {notification['question_id']}")
    return "\n".join(lines) + "\n\n" + notification["body"]


def _notification_source(notification, db_path) -> str | None:
    """Best-effort sender attribution for the ``from`` header (item_19).

    Only question-linked rows carry a resolvable source today: for a
    ``question_answer`` the answerer is ``questions.claimed_by``, for a
    worker-ask ``directive`` it is ``questions.session_id``. Task
    dispatches record no pusher, so they stay unattributed. Any lookup
    failure degrades to no header - attribution must never break the
    delivery it annotates.
    """
    qid = notification["question_id"]
    if qid is None:
        return None
    try:
        q = db.get_question(qid, path=db_path)
    except Exception:
        return None
    if q is None:
        return None
    if notification["kind"] == NotificationKind.QUESTION_ANSWER.value:
        return q["claimed_by"]
    return q["session_id"]


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
                relay_payload(row, source=_notification_source(row, db_path)),
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
    """Unmanaged sessions whose heartbeat is older than the limit.

    Managed sessions are excluded deliberately (item_17): between
    turns a managed drain-mode session is stale-hb by design - that
    very signal is what makes it a WAKE candidate. Treating it as
    dead would fail every dispatched task 120s after dispatch while
    the wake nudge is in flight. Managed liveness is ACP-reported:
    repeated wake ``no_session`` outcomes mark the session dead in
    ``_wake_pass`` instead.
    """
    cutoff = now - db.HEARTBEAT_STALE_SECONDS
    con = db.connect(db_path)
    try:
        return con.execute(
            "SELECT s.session_id AS sid, s.role AS role, s.status AS status,"
            " wh.last_seen_at AS seen"
            " FROM sessions s"
            " JOIN worker_heartbeats wh ON wh.session_id=s.session_id"
            " WHERE wh.last_seen_at<=?"
            " AND COALESCE(s.mode,'unmanaged')<>?"
            " ORDER BY s.session_id",
            (cutoff, SessionMode.MANAGED.value),
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

# WakeError reasons that blame the SESSION, not the acp-host: a
# successful attach+list happened before the failure, so they must
# not count against the host circuit breaker (a fleet of dead tabs
# would otherwise suspend every wake).
_SESSION_LEVEL_REASONS = {"no_session", "no_cwd", "rpc_error"}

# Consecutive no_session outcomes before a managed session is marked
# dead - the managed counterpart of the heartbeat sweep (item_17).
# At the default 300s cooldown this is ~15 minutes of absence.
_NO_SESSION_DEAD_STREAK = 3


def _wake_candidates(db_path, now: int):
    """Active managed sessions, heartbeat stale, with mail or live work.

    These are the sessions the mailbox alone cannot reach: no turn is
    open (heartbeat stale, or never heartbeated) while mail waits in
    the fs queue, so the daemon gives them a nudge prompt through the
    acp-host (item_16). A live task also qualifies (item_17): it keeps
    the liveness probe - and thus no_session death detection - running
    even when the outbox is empty. Dead sessions are excluded so a
    no_session-marked session is not probed forever.
    """
    cutoff = now - db.HEARTBEAT_STALE_SECONDS
    con = db.connect(db_path)
    try:
        return con.execute(
            "SELECT DISTINCT s.session_id AS sid, s.cwd AS cwd"
            " FROM sessions s"
            " LEFT JOIN worker_heartbeats wh ON wh.session_id=s.session_id"
            " WHERE s.mode=? AND s.status='active'"
            " AND (wh.last_seen_at IS NULL OR wh.last_seen_at<=?)"
            " AND (EXISTS (SELECT 1 FROM notifications n"
            "              WHERE n.session_id=s.session_id"
            "                AND n.relayed_at IS NOT NULL"
            "                AND n.ack_at IS NULL)"
            # a live task is also wake-worthy mail: it keeps liveness
            # probing (and thus no_session death detection) alive even
            # when the outbox is empty (item_17 review)
            "      OR EXISTS (SELECT 1 FROM tasks t"
            "                 WHERE t.session_id=s.session_id"
            "                   AND t.status IN ('queued','dispatch_decided',"
            "                                    'running','awaiting_user')))",
            (SessionMode.MANAGED.value, cutoff),
        ).fetchall()
    finally:
        con.close()


def _wake_pass(db_path, state_directory, now: int, policy: WakePolicy, notify=None) -> None:
    """Nudge idle managed sessions that have mail waiting (item_16).

    Runs after the relay pass so the same pass can already wake a
    recipient of fresh mail. The wake carries no payload - it is a
    "drain your mailbox" prompt over the acp-host ACP channel; the fs
    mailbox stays the only data channel, so a lost or double wake is
    harmless. Pacing (per-session cooldown, per-host circuit breaker)
    lives in ``policy``; outcome events land in the decision log.

    Post-inject verification (item_21): sessions whose wake returned
    ``sent_unconfirmed`` are re-checked on one shared session/list
    attach after the candidate loop. A session that turned busy proves
    the nudge landed a turn; a still-idle one bumps the durable
    wake_state streak, and a streak crossing
    WAKE_UNCONFIRMED_ESCALATE_STREAK escalates to the human once per
    episode via ``notify``.
    """
    deadline = time.monotonic() + _WAKE_PASS_DEADLINE_SECONDS
    unverified: list[str] = []
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
            if e.reason in _SESSION_LEVEL_REASONS:
                # attach+list worked - the host is healthy even
                # though this session could not be woken
                policy.record_result(host_key, True)
                opened = False
            else:
                opened = policy.record_result(host_key, False)
            if e.reason == "no_cwd":
                # session was in session/list (present, unwakeable) -
                # counts as seen, never toward the dead-streak
                policy.record_session_seen(sid)
            if e.reason == "no_session":
                streak = policy.record_no_session(sid)
                if streak >= _NO_SESSION_DEAD_STREAK:
                    _mark_dead_no_session(db_path, state_directory, sid, streak)
            _decision(state_directory, "wake_failed", sid, f"reason={e.reason} {e}")
            # a failed wake proves nothing either way - journal it
            # without touching the unconfirmed streak
            db.record_wake_outcome(sid, f"failed:{e.reason}", path=db_path)
            if opened:
                _decision(
                    state_directory,
                    "wake_breaker_open",
                    detail=f"host={host_key} suspended",
                )
            continue
        policy.record_result(host_key, True)
        policy.record_session_seen(sid)
        if outcome == "busy":
            _decision(state_directory, "wake_skipped_busy", sid)
            # a busy session is demonstrably turning - proof of life,
            # resets any unconfirmed episode in flight
            db.record_wake_outcome(sid, "busy", path=db_path)
            policy.record_confirmed(sid)
            continue
        db.record_wake_outcome(sid, outcome, path=db_path)
        if outcome == "sent_unconfirmed":
            # prompt written but unconfirmed - retry sooner than the
            # full cooldown so a silently missed nudge self-heals;
            # the post-pass verification decides the durable streak
            unverified.append(sid)
            policy.record_retry_soon(sid)
        else:
            policy.record_confirmed(sid)
        _decision(state_directory, "wake_sent", sid, f"outcome={outcome}")

    if unverified:
        _verify_unconfirmed_wakes(db_path, state_directory, unverified, policy, notify)


def _mark_dead_no_session(db_path, state_directory, sid, streak: int) -> None:
    """no_session streak reached the dead threshold.

    Absent from session/list over enough attempts = really gone
    (closed tab) - the managed dead path; kills its live task like
    the unmanaged sweep does.
    """
    if db.set_session_status(sid, SessionStatus.DEAD, path=db_path):
        task = db.get_live_task(sid, path=db_path)
        outcome = "no live task"
        if task is not None:
            result = db.requeue_dead_worker(task["id"], note=requeue_note(task["id"]), path=db_path)
            outcome = f"task={task['id']} -> {result.value if result else 'unchanged'}"
        _decision(
            state_directory,
            "worker_dead",
            sid,
            f"reason=no_session_streak streak={streak} {outcome}",
        )


# A fresh-enough ack watermark proves the drain turn ran even when the
# re-check finds the session idle again - acks only move inside live
# turns. Covers the "nudge landed, turn opened and closed before
# verify" window a point-in-time statusEnum cannot see (item_21
# review). Generous on purpose: a session that acked within the last
# two minutes is demonstrably reachable.
_VERIFY_ACK_GRACE_SECONDS = 120


def _verify_unconfirmed_wakes(db_path, state_directory, sids, policy, notify) -> None:
    """Re-check ``sent_unconfirmed`` sessions on one shared attach.

    One session/list answers for the whole batch (item_21): a session
    that went busy since its nudge proves the prompt landed a turn -
    durable outcome ``verified``, streak and escalation reset. A
    still-idle or absent session grows its durable wake_state streak;
    crossing WAKE_UNCONFIRMED_ESCALATE_STREAK notifies the human once
    per episode (the ``escalated`` flag survives a daemon restart, so
    it cannot re-notify after one). A failed verification attach
    leaves every streak untouched - unverified is not unconfirmed.
    """
    lock_data = find_lockfile()
    try:
        statuses = verify_sessions(sids, lock_data=lock_data)
    except WakeError as e:
        _decision(
            state_directory,
            "wake_verify_failed",
            detail=f"sessions={len(sids)} reason={e.reason} {e}",
        )
        return
    now = int(time.time())
    for sid in sids:
        status = statuses.get(sid, SESSION_ABSENT)
        if status == SESSION_ABSENT:
            # absent on the re-check is the same evidence as a failed
            # session/list lookup: feed the no_session streak so a
            # dead tab counts toward dead-marking (item_21 review)
            ns_streak = policy.record_no_session(sid)
            if ns_streak >= _NO_SESSION_DEAD_STREAK:
                _mark_dead_no_session(db_path, state_directory, sid, ns_streak)
                continue  # dead-marked - the unconfirmed bump is moot
        else:
            # listed but possibly statusless (None) - present evidence,
            # never dead-mark input
            policy.record_session_seen(sid)
        alive = status is not None and status != SESSION_ABSENT and not status_is_idle(status)
        if not alive:
            # idle on the list, but a fresh ack watermark is equally
            # strong turn evidence: the drain turn opened and closed
            # before this re-check
            ack = db.last_ack_at(sid, path=db_path)
            if ack is not None and now - int(ack) <= _VERIFY_ACK_GRACE_SECONDS:
                alive = True
        if alive:
            db.record_wake_outcome(sid, "verified", path=db_path)
            policy.record_confirmed(sid)
            _decision(
                state_directory,
                "wake_verified",
                sid,
                f"status={status if status is not None else 'ack'}",
            )
            continue
        streak, escalated = db.bump_wake_streak(sid, path=db_path)
        if streak >= WAKE_UNCONFIRMED_ESCALATE_STREAK and not escalated and notify is not None:
            notify(
                "sessions-bridge WAKE UNCONFIRMED",
                f"{sid}: {streak} consecutive wakes left the session idle"
                " - it may be unreachable (closed tab or dead agent)",
            )
            db.set_wake_escalated(sid, path=db_path)
            _decision(
                state_directory,
                "wake_unconfirmed_escalated",
                sid,
                f"streak={streak} status={status}",
            )
        else:
            _decision(
                state_directory,
                "wake_unconfirmed",
                sid,
                f"streak={streak} status={status}",
            )


def _question_janitor_pass(db_path, state_directory) -> None:
    """Janitor question claims and pending TTL (CLAIM/PENDING timeouts).

    Runs every pass - two guarded UPDATEs, cheap when idle. A question
    claimed by an orchestrator that died mid-deliberation returns to
    ``pending`` (same id, no re-ask) instead of hanging forever;
    released ids land in the decision log. A pending question never
    claimed within PENDING_TIMEOUT_SECONDS expires (item_20) - the
    orchestrator it was routed to is unreachable - and the asking
    worker is notified through the outbox so its pending slot frees
    and the wait ends visibly instead of wedging the task.
    """
    released = db.janitor_questions(path=db_path)
    if released:
        _decision(
            state_directory,
            "question_claims_released",
            detail=f"released={len(released)} ids={released}",
        )
    for row in db.expire_pending_questions(path=db_path):
        # the expiry notice committed inside the same transaction -
        # the daemon only journals the event here
        _decision(
            state_directory,
            "question_pending_expired",
            row["session_id"],
            f"question={row['id']} task={row['task_id']} worker notified",
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
                _wake_pass(db_path, state_directory, now, wake_policy, notify)
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

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

from session_bridge.mailbox import mailbox_dir, queue_root, write_message
from session_bridge.quota import DEFAULT_PARK_SECONDS, parse_quota_signal
from session_bridge.state import db
from session_bridge.state.contract import (
    WAKE_UNCONFIRMED_ESCALATE_STREAK,
    NotificationKind,
    QuestionState,
    SessionMode,
    SessionRole,
    SessionStatus,
    TaskStatus,
)
from session_bridge.state.verdict import SessionRevived
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
    if notification["kind"] in db.CONTROL_KINDS:
        # control deliveries carry their epoch (issue #119): the
        # janitor drops a file whose issued_at is past the control
        # TTL without needing the notification row, and the drain
        # can see when the command was actually issued
        lines.append(f"issued_at: {notification['created_at']}")
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


def _write_heartbeat(state_directory: str, metric: str | None = None) -> None:
    """Stamp ``daemon_heartbeat.txt`` with the current unix time.

    ``metric`` is an optional ``key=value`` line appended after the
    timestamp - consumers that read the file take line 1 only, so the
    second line is pure observability (item_139 mail-stall gauge).
    """
    target = heartbeat_path(state_directory)
    fd, tmp = tempfile.mkstemp(prefix=".hb-", dir=state_directory)
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(f"{int(time.time())}\n")
            if metric:
                fh.write(f"{metric}\n")
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


def _control_stale_reason(sid, nid, issued_at, now: int, *, db_path=None) -> str | None:
    """Why a control delivery must be dropped, or ``None``.

    Two staleness signals (issue #119): supersession - a newer
    control notification exists for the session (the control epoch
    moved past this command) - and age - the delivery is older than
    ``_CONTROL_TTL_SECONDS``. ``issued_at`` is the notification's
    ``created_at`` when the row is known, otherwise the file's own
    timestamp.
    """
    latest = db.latest_control_notification_id(sid, path=db_path)
    if latest and nid is not None and nid < latest:
        return f"superseded_by={latest}"
    if issued_at is not None and now - issued_at > _CONTROL_TTL_SECONDS:
        return f"stale age={now - issued_at}s ttl={_CONTROL_TTL_SECONDS}"
    return None


def _relay_pass(db_path, queue_dir, state_directory, now: int) -> None:
    """Drain the outbox: unrelayed -> fs msg file -> mark_relayed.

    Control-class rows that went stale while queued (daemon down,
    superseded by a newer halt/resume/disarm) are consumed without
    delivery: a control command must never execute out of epoch
    (issue #119). The row is still marked relayed so the drop does
    not spin the outbox, and the journal names the reason.
    """
    for row in db.unrelayed_notifications(path=db_path):
        if row["kind"] in db.CONTROL_KINDS:
            reason = _control_stale_reason(
                row["session_id"], row["id"], row["created_at"], now, db_path=db_path
            )
            if reason is not None:
                db.mark_relayed(row["id"], path=db_path)
                detail = f"notification={row['id']} kind={row['kind']} {reason}"
                _decision(state_directory, "stale_control_dropped", row["session_id"], detail)
                db.record_event(row["session_id"], "stale_control_dropped", detail, path=db_path)
                continue
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


def _stale_control_pass(db_path, queue_dir, state_directory, now: int) -> None:
    """Unlink stale control files already sitting in mailboxes.

    The relay-side drop only covers rows still in the outbox; a
    control file delivered while fresh can still sit through a long
    turn and execute out of epoch at drain (issue #119's incident
    shape: a HALT consumed 24h late). Same staleness rule as the
    relay leg - superseded by a newer control notification or older
    than ``_CONTROL_TTL_SECONDS`` - evaluated per file. Genuine relay
    artifacts resolve their real sid + issue time through the
    notification row; unverifiable files fall back to ``issued_at``
    header or file mtime for the age leg only.
    """
    root = queue_root(db_path, queue_dir)
    try:
        dirnames = os.listdir(root)
    except OSError:
        return
    con = db.connect(db_path)
    try:
        for dirname in dirnames:
            mdir = os.path.join(root, dirname)
            if not os.path.isdir(mdir):
                continue
            try:
                names = [n for n in os.listdir(mdir) if n.startswith("msg_") and n.endswith(".txt")]
            except OSError:
                continue
            for name in names:
                path = os.path.join(mdir, name)
                headers = _msg_headers(path)
                if headers.get("kind") not in db.CONTROL_KINDS:
                    continue
                nid = _msg_nid(name)
                sid = dirname  # mailbox dirs are named by sanitized sid
                issued_at = None
                if nid is not None and headers.get("notification_id") == str(nid):
                    row = con.execute(
                        "SELECT session_id, created_at FROM notifications WHERE id=?",
                        (nid,),
                    ).fetchone()
                    if row is not None:
                        sid = row["session_id"]
                        issued_at = row["created_at"]
                if issued_at is None:
                    iat = headers.get("issued_at", "")
                    if iat.isdigit():
                        issued_at = int(iat)
                if issued_at is None:
                    try:
                        issued_at = int(os.path.getmtime(path))
                    except OSError:
                        continue  # vanished mid-scan - already consumed
                reason = _control_stale_reason(sid, nid, issued_at, now, db_path=db_path)
                if reason is None:
                    continue
                try:
                    os.unlink(path)
                except OSError:
                    continue  # raced with the consumer - either outcome is a drain
                detail = f"file={name} kind={headers['kind']} {reason}"
                _decision(state_directory, "stale_control_dropped", sid, detail)
                db.record_event(sid, "stale_control_dropped", detail, path=db_path)
    finally:
        con.close()


def _stale_sessions(db_path, now: int):
    """Unmanaged sessions past their liveness TTL.

    Delegates to the shared ``db.stale_unmanaged_sessions`` scan
    (task #207 item f): the stamped-heartbeat leg is the classic
    sweep signal, the no-heartbeat-row leg catches the
    register-then-vanish ghost class the old JOIN could not see.

    Managed sessions are excluded deliberately (item_17): between
    turns a managed drain-mode session is stale-hb by design - that
    very signal is what makes it a WAKE candidate. Treating it as
    dead would fail every dispatched task 120s after dispatch while
    the wake nudge is in flight. Managed liveness is ACP-reported:
    repeated wake ``no_session`` outcomes mark the session dead in
    ``_wake_pass`` instead.
    """
    return db.stale_unmanaged_sessions(now, path=db_path)


def mark_dead(row, db_path, state_directory, notify_send, now: int, *, tag: str = "") -> None:
    """Apply the dead verdict to one ``stale_unmanaged_sessions`` row.

    Shared by the daemon sweep pass and the CLI lazy GC (task #207
    item f) so the two paths cannot drift on the effect block:
    CAS demotion to ``dead``, live-task requeue, orchestrator
    notify-send, decision journal. ``tag`` distinguishes the caller
    in the detail line (the CLI passes ``"gc "``).

    Ghost-leg guard (hb=None): ``sessions.updated_at`` is the only
    liveness baseline a no-heartbeat-row registration has, and no
    task lifecycle write bumps it - a silently-working unmanaged
    worker past GHOST_TTL would be killed mid-task and its running
    task requeued into duplicate execution. So a ghost holding a
    live task is NOT demoted here: lease recovery owns the task
    first, and once nothing is live the next pass removes the
    registration. The heartbeat leg keeps the classic verdict
    (silent 120s = dead, task requeued immediately).
    """
    sid = row["sid"]
    age = now - int(row["seen"])
    # the scan carries two legs: hb=None means the no-heartbeat-row
    # ghost class (GHOST_TTL), a stamped-but-silent heartbeat is the
    # classic signal - say which one fired in the detail
    ghost = row["hb"] is None
    marker = " ghost" if ghost else ""
    limit = db.GHOST_TTL_SECONDS if ghost else db.HEARTBEAT_STALE_SECONDS
    if ghost and db.get_live_task(sid, path=db_path) is not None:
        # not journaled: a live task past GHOST_TTL is steady state
        # until its lease folds, and a per-pass event would spam the
        # decisions file - lease recovery's task_recovered already
        # documents the fold that unblocks this registration
        return
    if row["role"] == SessionRole.ORCHESTRATOR.value:
        body = db.mark_dead(
            sid,
            "orchestrator_dead",
            f"{tag}heartbeat_age={age}s{marker} notify-send sent",
            path=db_path,
        )
        if body is None:
            return  # already dead or raced - handled before
        notify_send(
            "sessions-bridge ORCHESTRATOR DEAD",
            f"{sid}: heartbeat silent {age}s (limit {limit}s)",
        )
        _decision(state_directory, "orchestrator_dead", sid, body)
        return
    task = db.get_live_task(sid, path=db_path)
    head = f"{tag}heartbeat_age={age}s{marker}"
    body = db.mark_dead(
        sid,
        "worker_dead",
        head if task is not None else f"{head} no live task",
        task_id=task["id"] if task is not None else None,
        task_note=requeue_note(task["id"]) if task is not None else None,
        path=db_path,
    )
    if body is None:
        return  # already dead or raced - handled before
    _decision(state_directory, "worker_dead", sid, body)


def _sweep_pass(db_path, state_directory, notify_send, now: int) -> None:
    """Heartbeat staleness sweep.

    A silent orchestrator raises one deduped OS notification. A dead
    worker's live task is requeued with the reset-to-base instruction.
    Both fire only on the ``active > dead`` transition (the
    ``set_session_status`` CAS inside ``mark_dead``), so a session
    that stays dead is never re-notified or re-requeued on later
    passes.
    """
    for row in _stale_sessions(db_path, now):
        mark_dead(row, db_path, state_directory, notify_send, now)


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


def _pending_mailbox_dirs(db_path, queue_dir=None) -> dict:
    """Managed+active sessions with unconsumed ``msg_*.txt`` files.

    The notifications table only knows mail the daemon itself relayed;
    a foreign writer (an orchestrator hand-writing msg_*.txt, a
    hook) leaves files no row covers - file presence is the ground
    truth of "mail waiting" (item_139 amendment: shore-felidae's
    msg_003 sat ~2h with zero wake events because the wake query
    looked at rows only). Returns
    {sid: {"oldest": mtime, "names": [msg basenames]}}.
    """
    con = db.connect(db_path)
    try:
        sids = [
            r["session_id"]
            for r in con.execute(
                "SELECT session_id FROM sessions WHERE mode=? AND status='active'",
                (SessionMode.MANAGED.value,),
            ).fetchall()
        ]
        pending = {}
        for sid in sids:
            r = _dir_pending(db_path, queue_dir, sid)
            if r is not None:
                pending[sid] = r
        return pending
    finally:
        con.close()


def _dir_pending(db_path, queue_dir, sid):
    """Actionable mail on disk for ONE session, or None.

    Same shape as a ``_pending_mailbox_dirs`` entry:
    {"oldest": mtime, "names": [msg basenames]}.  A heartbeat_tick
    file is a liveness poke, never actionable mail - without the
    header check the file leg would re-break the "tick alone never
    wakes" contract every time the relay delivers a poke.  Provenance
    comes from the file's own relay headers (_msg_is_tick), not a
    bare id lookup: a foreign writer's msg_<N>.txt may collide with
    an unrelated row id.
    """
    mdir = mailbox_dir(sid, db_path=db_path, queue_dir=queue_dir)
    try:
        names = [n for n in os.listdir(mdir) if n.startswith("msg_") and n.endswith(".txt")]
    except OSError:
        return None
    # stat each file individually: the consumer may delete it
    # between listdir and getmtime (TOCTOU) - a vanished file
    # is just already-drained mail, not a pass-killing error
    stats = []
    for n in names:
        try:
            stats.append((n, int(os.path.getmtime(os.path.join(mdir, n)))))
        except OSError:
            continue
    actionable = [
        (n, mt) for n, mt in stats if not _msg_is_tick(os.path.join(mdir, n), _msg_nid(n))
    ]
    if not actionable:
        return None
    return {"oldest": min(mt for _, mt in actionable), "names": [n for n, _ in actionable]}


def _wake_candidates(db_path, now: int, queue_dir=None, pending=None):
    """Sessions the daemon should poke, in two disjoint classes.

    Wake rows (``probe_only=False``) get the fixed drain prompt. A
    session qualifies through one leg only - actionable mail on disk:

    File-pending leg (item_139 amendment, sole trigger since task
    #184): any unconsumed non-tick msg_*.txt in the session's mailbox
    dir qualifies whatever the heartbeat or notification rows say -
    the fs mailbox is the authoritative delivery channel, and foreign
    writers bypass the outbox entirely. The older DB legs (unacked
    relayed rows with a stale or fresh heartbeat) were dropped: they
    kept qualifying consumed-but-unacked mail long after the file was
    drained, producing the very wake-on-empty-mailbox loop this
    function exists to prevent (mahogany-dryosaurus ran a permanent
    ~5-minute wake cadence on a physically empty mailbox - task #184).
    Rows whose file is gone are ack lag, not mail; rows whose file
    exists are already covered by this leg. heartbeat_tick files are
    excluded by ``_dir_pending`` - a liveness poke is never mail.

    Probe rows (``probe_only=True``) get a session/list presence check
    only - never a prompt (item_162). A live task with a stale
    heartbeat is the liveness/death-detection input: it keeps the
    no_session streak running when the outbox is empty, but the drain
    prompt it used to trigger produced a permanent wake loop - a
    managed DRAIN session's heartbeat is structurally stale between
    turns, so the task leg re-qualified every retry window while the
    mailbox stayed physically empty (the buttered-tilapia incident:
    three drain prompts, zero files, ack zero). A task alone can no
    longer wake a session; an empty mailbox never wakes.

    Dead sessions are excluded so a no_session-marked session is not
    probed forever.
    """
    cutoff = now - db.HEARTBEAT_STALE_SECONDS
    con = db.connect(db_path)
    try:
        # file-pending leg: unconsumed msg_*.txt is mail even when no
        # notification row exists for it (foreign writers bypass the
        # outbox - the msg_003 sleeping-DRAIN evidence, item_139). No
        # queue_dir guard: mailbox_dir resolves the default queue root
        # from db_path, so the leg is on in every configuration.
        if pending is None:
            pending = _pending_mailbox_dirs(db_path, queue_dir)
        result = []
        if pending:
            marks = ",".join("?" * len(pending))
            # waiting_quota suppression (task #216): a parked session's
            # mail keeps until the provider's reset - a nudge before
            # then would spend the very quota that is exhausted. The
            # quota pass probes once at reset; the mail drains then.
            # Expired-but-unswept rows do not suppress (quota_reset_at
            # > now only) - the pass owns their single probe.
            rows = con.execute(
                "SELECT session_id AS sid, cwd AS cwd FROM sessions"
                f" WHERE session_id IN ({marks})"
                " AND mode=? AND status='active'"
                " AND NOT EXISTS (SELECT 1 FROM tasks t"
                "                 WHERE t.session_id=sessions.session_id"
                "                   AND t.step_state='waiting_quota'"
                "                   AND t.quota_reset_at>?)",
                (*pending, SessionMode.MANAGED.value, now),
            ).fetchall()
            result = [{"sid": r["sid"], "cwd": r["cwd"], "probe_only": False} for r in rows]
        # liveness probe rows (item_162): stale-heartbeat sessions with
        # a live task get a session/list presence check so no_session
        # dead-marking still runs on an empty outbox - but the drain
        # prompt is for mail only, so these rows carry probe_only and
        # are never sent a wake. Sessions already qualified through a
        # mail leg stay wake rows.
        wake_sids = {r["sid"] for r in result}
        # Two statuses are excluded from the task probe deliberately
        # (item_139): awaiting_user is a parked task whose blocker is
        # out-of-band (its answer arrives as actionable mail, which has
        # its own wake leg), and a running task with a fresh wait_until
        # stamp declared an external wait via `bridge wait` - probing a
        # session that by definition cannot act yet is noise. The stamp
        # decays with the lease it mirrors, so an abandoned wait becomes
        # probe-worthy again on its own.
        probe = con.execute(
            "SELECT DISTINCT s.session_id AS sid, s.cwd AS cwd"
            " FROM sessions s"
            " LEFT JOIN worker_heartbeats wh ON wh.session_id=s.session_id"
            " WHERE s.mode=? AND s.status='active'"
            " AND (wh.last_seen_at IS NULL OR wh.last_seen_at<=?)"
            " AND EXISTS (SELECT 1 FROM tasks t"
            "             WHERE t.session_id=s.session_id"
            "               AND COALESCE(t.step_state,'')<>'waiting_quota'"
            "               AND (t.status IN ('queued','dispatch_decided')"
            "                    OR (t.status='running'"
            "                        AND (t.wait_until IS NULL"
            "                             OR t.wait_until<=?))))",
            (SessionMode.MANAGED.value, cutoff, now),
        ).fetchall()
        for r in probe:
            if r["sid"] not in wake_sids:
                result.append({"sid": r["sid"], "cwd": r["cwd"], "probe_only": True})
        return result
    finally:
        con.close()


def _wake_quota_park(db_path, state_directory, sid, exc, now: int) -> bool:
    """Park ``sid``'s live task when a wake's rpc_error text carries a
    provider quota signal (task #230).

    The drain prompt hits the same exhausted pool the worker's prompt
    did - the marker arrives as rpc_error free text ("Reached free
    model rate limit ... reset in 58 minutes (at 22:01 UTC)"), so the
    wake pass is the first place that can turn it into a park instead
    of burning the retry streak on a dead pool. A marker without a
    reset parks for the bounded ``DEFAULT_PARK_SECONDS`` window - the
    quota pass re-probes at its end. Returns True when the task is
    parked after the call - by this park's CAS or by a fresher one
    that won the race (``quota_park_yield``); a signal with no live
    task journals ``quota_signal_seen``, and a CAS lost to a moved
    task journals ``quota_park_lost`` - both then take the ordinary
    rpc_error bookkeeping.
    """
    signal = parse_quota_signal(str(exc))
    if signal is None:
        return False
    task = db.get_live_task(sid, path=db_path)
    if task is None:
        _decision(
            state_directory,
            "quota_signal_seen",
            sid,
            f"no live task - {str(exc)[:200]}",
        )
        return False
    reset_at = signal["reset_at"]
    note = str(exc)[:500]
    if reset_at is None:
        reset_at = now + DEFAULT_PARK_SECONDS
        note = f"marker without reset - default window: {note[:460]}"
    if db.quota_park(
        task["id"], sid, reset_at, note, source="wake", require_unparked=True, path=db_path
    ):
        _decision(
            state_directory,
            "quota_park",
            sid,
            f"task={task['id']} reset_at={reset_at} source=wake",
        )
        return True
    # CAS lost: a fresher park may have landed mid-wake - that window
    # is authoritative, so the net effect is still "parked". Anything
    # else (finished/requeued) journals the lost park and falls back
    # to ordinary rpc_error bookkeeping.
    row = db.get_task(task["id"], path=db_path)
    if row is not None and row["step_state"] == "waiting_quota":
        _decision(
            state_directory,
            "quota_park_yield",
            sid,
            f"task={task['id']} fresher park already in place",
        )
        return True
    _decision(
        state_directory,
        "quota_park_lost",
        sid,
        f"task={task['id']} moved on before the park CAS landed",
    )
    db.record_event(
        sid,
        "quota_park_lost",
        f"task={task['id']} wake-surfaced park lost its CAS",
        task_id=task["id"],
        path=db_path,
    )
    return False


def _dead_pending_sids(db_path, queue_dir) -> list:
    """Dead managed sessions that still have actionable mail on disk.

    The file-pending leg scans active sessions only - a dead sid is
    deliberately never a wake candidate. Pending mail for a dead sid
    is the demand signal that tells the resurrect probe the bus wants
    that session back (issue #115).
    """
    con = db.connect(db_path)
    try:
        sids = [
            r["session_id"]
            for r in con.execute(
                "SELECT session_id FROM sessions WHERE COALESCE(mode,?)=? AND status=?",
                (
                    SessionMode.UNMANAGED.value,
                    SessionMode.MANAGED.value,
                    SessionStatus.DEAD.value,
                ),
            ).fetchall()
        ]
    finally:
        con.close()
    return [s for s in sids if _dir_pending(db_path, queue_dir, s) is not None]


def _resurrect_pass(db_path, state_directory, queue_dir, dead_pending, policy, notify=None) -> list:
    """Presence check -> revive for dead managed sessions with mail.

    The ``orchestrator_dead`` auto-recovery gap (issue #115): a
    dead-flagged managed session that comes back (tab reopened, agent
    restarted) used to stay dead until a manual re-register - dead
    sids are excluded from every wake leg, so nothing ever looked.
    While files sit in its mailbox the session gets one batched
    session/list presence check on the same ``verify_sessions``
    infrastructure the probe leg uses; a sid reported present is
    CAS-revived with a durable ``session_revived`` row and re-enters
    ordinary wake candidacy in the same pass (``clear_cooldown`` -
    the probe attempt must not push its drain prompt a window out).

    Operator intent wins: ``db.dead_revivable_managed`` pre-filters
    to sids whose latest dead-causing marker is a liveness verdict,
    and ``revive_dead_managed`` re-checks that marker inside its own
    transaction - ``bridge kill <sid>`` never undoes itself. Absent
    sids keep the armed cooldown, which paces the next probe instead
    of spending an RPC per pass on a gone session's undrainable mail.
    """
    if not dead_pending:
        return []
    revivable = sorted(db.dead_revivable_managed(dead_pending, path=db_path))
    if not revivable:
        return []
    lock_data = find_lockfile()
    if lock_data is None:
        return []
    host_key = lock_data["_path"]
    due = [sid for sid in revivable if policy.check(sid, host_key) is None]
    if not due:
        return []
    for sid in due:
        policy.record_attempt(sid)
    try:
        statuses = verify_sessions(due, lock_data=lock_data)
    except WakeError as e:
        opened = policy.record_result(host_key, False)
        _decision(
            state_directory,
            "resurrect_probe_failed",
            detail=f"sessions={len(due)} reason={e.reason} {e}",
        )
        if opened:
            _decision(
                state_directory,
                "wake_breaker_open",
                detail=f"host={host_key} suspended",
            )
        return []
    policy.record_result(host_key, True)
    revived = []
    for sid in due:
        if statuses.get(sid, SESSION_ABSENT) == SESSION_ABSENT:
            _decision(state_directory, "resurrect_absent", sid)
            continue
        policy.record_session_seen(sid)
        verdict = db.revive_dead_managed(sid, path=db_path)
        if isinstance(verdict, SessionRevived):
            policy.clear_cooldown(sid)
            revived.append(sid)
            _decision(state_directory, "session_revived", sid, f"marker={verdict.marker}")
            if notify is not None:
                row = db.get_session(sid, path=db_path)
                if row is not None and row["role"] == SessionRole.ORCHESTRATOR.value:
                    notify(
                        "sessions-bridge ORCHESTRATOR REVIVED",
                        f"{sid}: present in session/list again - latch cleared",
                    )
        else:
            _decision(
                state_directory,
                "resurrect_refused",
                sid,
                f"reason={getattr(verdict, 'reason', verdict.kind)}",
            )
    return revived


def _wake_pass(
    db_path,
    state_directory,
    now: int,
    policy: WakePolicy,
    notify=None,
    queue_dir=None,
    pending=None,
) -> None:
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

    Probe-only candidates (item_162): stale-heartbeat sessions whose
    sole qualification is a live task never reach the prompt path -
    ``_probe_pass`` checks their presence on one batched session/list
    after the candidate loop so no_session dead-marking still runs
    while an empty mailbox provably never wakes.
    """
    deadline = time.monotonic() + _WAKE_PASS_DEADLINE_SECONDS
    # Resurrect leg (issue #115) runs BEFORE candidate selection so a
    # revived sid qualifies through the ordinary file-pending leg in
    # the same pass. The caller's ``pending`` scan is active-only, so
    # a revived sid's mailbox entry is patched in here - otherwise the
    # file leg cannot see it until the next pass.
    revived = _resurrect_pass(
        db_path,
        state_directory,
        queue_dir,
        _dead_pending_sids(db_path, queue_dir),
        policy,
        notify,
    )
    if revived and pending is not None:
        for sid in revived:
            if sid not in pending:
                r = _dir_pending(db_path, queue_dir, sid)
                if r is not None:
                    pending[sid] = r
    unverified: list[str] = []
    probes: list[str] = []
    for row in _wake_candidates(db_path, now, queue_dir, pending):
        if row.get("probe_only"):
            # liveness-only candidate (item_162): presence is checked
            # in one batched session/list below - the drain prompt is
            # for pending mail only, so an empty mailbox never wakes.
            probes.append(row["sid"])
            continue
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
            if e.reason == "rpc_error" and _wake_quota_park(db_path, state_directory, sid, e, now):
                # provider quota surfaced through the drain prompt
                # (task #230): the live task is parked until reset -
                # the host is fine and the session provably answered
                # post-list, so both bookkeeping lines mark success
                policy.record_result(host_key, True)
                policy.record_session_seen(sid)
                db.record_wake_outcome(sid, "quota_park", path=db_path)
                continue
            if e.reason in _SESSION_LEVEL_REASONS:
                # attach+list worked - the host is healthy even
                # though this session could not be woken
                policy.record_result(host_key, True)
                opened = False
            else:
                opened = policy.record_result(host_key, False)
            if e.reason in ("no_cwd", "rpc_error"):
                # both prove the session answered to session/list:
                # no_cwd means present-but-unwakeable, and rpc_error
                # can only surface post-list (wake_session remaps
                # pre-list RPC failures to attach_failed) - either way
                # it counts as seen, never toward the dead-streak
                policy.record_session_seen(sid)
            if e.reason == "no_session":
                streak = policy.record_no_session(sid)
                if streak >= _NO_SESSION_DEAD_STREAK:
                    _mark_dead_no_session(db_path, state_directory, sid, streak, notify)
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

    if probes:
        _probe_pass(db_path, state_directory, probes, policy, notify)

    if unverified:
        _verify_unconfirmed_wakes(db_path, state_directory, unverified, policy, notify)


def _probe_pass(db_path, state_directory, sids, policy, notify=None) -> None:
    """Batched session/list presence check for probe-only candidates.

    The liveness probe that the old task wake leg provided by side
    effect (item_162): a stale-heartbeat managed session with a live
    task still needs no_session dead-marking to run while its mailbox
    is empty, but the drain prompt is for mail only - so presence is
    checked with a single shared session/list and no prompt is ever
    injected. Absent sids feed the durable no_session streak (and the
    dead-mark at the threshold); listed sids reset it. The shared
    cooldown paces probes like wakes; a failed attach is a host
    problem, not session evidence, so it leaves streaks untouched and
    counts toward the breaker like any wake failure.
    """
    lock_data = find_lockfile()
    host_key = lock_data["_path"] if lock_data else "none"
    due = [sid for sid in sids if policy.check(sid, host_key) is None]
    if not due or lock_data is None:
        return
    for sid in due:
        policy.record_attempt(sid)
    try:
        statuses = verify_sessions(due, lock_data=lock_data)
    except WakeError as e:
        opened = policy.record_result(host_key, False)
        _decision(
            state_directory,
            "wake_probe_failed",
            detail=f"sessions={len(due)} reason={e.reason} {e}",
        )
        if opened:
            _decision(
                state_directory,
                "wake_breaker_open",
                detail=f"host={host_key} suspended",
            )
        return
    policy.record_result(host_key, True)
    now = int(time.time())
    for sid in due:
        # Parity with _verify_unconfirmed_wakes: a fresh ack proves a
        # live drain turn even when session/list raced and reported
        # the session absent - absent-with-fresh-ack is not no_session
        # evidence.
        ack = db.last_ack_at(sid, path=db_path)
        absent = statuses.get(sid, SESSION_ABSENT) == SESSION_ABSENT
        if absent and not (ack is not None and now - int(ack) <= _VERIFY_ACK_GRACE_SECONDS):
            streak = policy.record_no_session(sid)
            _decision(
                state_directory,
                "wake_probe_absent",
                sid,
                f"streak={streak}",
            )
            if streak >= _NO_SESSION_DEAD_STREAK:
                _mark_dead_no_session(db_path, state_directory, sid, streak, notify)
        else:
            policy.record_session_seen(sid)
            _decision(
                state_directory,
                "wake_probe",
                sid,
                "present" if not absent else "absent_but_acked",
            )


def _env_int(name: str, default: int) -> int:
    """Env-derived int with a safe default: a malformed override must
    not crash the module import (bridge.py loads this module at CLI
    startup - a typoed env var would brick the whole CLI)."""
    try:
        return int(os.environ.get(name, ""))
    except ValueError:
        return default


#: Undrained actionable mail older than this triggers one escalation
#: per stall episode (item_139) - a session whose relayed mail sits
#: unacked this long is unreachable by every other signal we have.
#: ``SB_STALL_THRESHOLD_S`` (directive 445) is the canonical env
#: override; ``SESSIONS_BRIDGE_MAIL_STALL_SECONDS`` stays as the
#: legacy alias so existing deployments keep working.
_MAIL_STALL_SECONDS = _env_int(
    "SB_STALL_THRESHOLD_S",
    _env_int("SESSIONS_BRIDGE_MAIL_STALL_SECONDS", 1800),
)

#: Re-alert cadence inside one stall episode (directive 445): a
#: session whose stall never resolves re-escalates every hour
#: instead of being silenced by its own episode stamp - the alert
#: is the last human-visible signal for a wedged drain contract.
_MAIL_STALL_REALERT_SECONDS = _env_int("SB_STALL_REALERT_S", 3600)

#: Busy grace has a ceiling (task #174 review): a session whose
#: statusEnum reports live work defers escalation only while the mail
#: is younger than this. Past it the session may be wedged mid-turn -
#: managed sessions are exempt from the heartbeat sweep, so this pass
#: is the last operator signal for that shape. Default = 4 stall
#: lengths; clamped at the stall threshold so a misconfigured
#: override cannot silently disable the grace.
_BUSY_GRACE_SECONDS = max(
    _MAIL_STALL_SECONDS,
    _env_int("SESSIONS_BRIDGE_BUSY_GRACE_SECONDS", 4 * _MAIL_STALL_SECONDS),
)

#: Auto-renewal engages once the remaining lease falls under this
#: margin (task #189): renew-before-expiry rather than
#: recover-after-expiry keeps ``recover_stale`` from ever seeing a
#: live busy task as stale. Far wider than one daemon interval, so
#: consecutive passes amortize to one write per task per lease.
_LEASE_RENEW_MARGIN = _env_int("SESSIONS_BRIDGE_LEASE_RENEW_MARGIN", 300)

#: Upper bound on the auto-renewed window (task #189): the wall-clock
#: age of the last EXPLICIT lease touch (claim / mark_running / renew
#: / wait) beyond which auto-renewal stops and expiry recovery takes
#: over - a wedged turn that keeps reporting busy cannot hold the
#: task forever. Default = 6 lease lengths; clamped at one
#: lease length so a misconfigured override cannot silently disable
#: the mechanism.
_LEASE_AUTO_RENEW_LIMIT = max(
    db.LEASE_SECONDS,
    _env_int("SESSIONS_BRIDGE_LEASE_AUTO_RENEW_SECONDS", 6 * db.LEASE_SECONDS),
)

#: Control-delivery TTL (issue #119): a halt/resume/disarm delivery
#: this old is dropped, not executed - the durable flag it accompanied
#: already did the enforcing (``halted_at`` blocks claims regardless),
#: so the file is only a courtesy signal whose semantics a stale
#: read would invert (a 24h-late HALT self-halting a resumed fleet).
#: ``SB_CONTROL_TTL_S`` override; default 6h.
_CONTROL_TTL_SECONDS = _env_int("SB_CONTROL_TTL_S", 21600)


def _msg_nid(name: str):
    """Numeric notification id from ``msg_<nid>.txt``; None when the
    name carries no digits (a foreign file that matches no row)."""
    stem = name[4:-4] if name.startswith("msg_") and name.endswith(".txt") else ""
    return int(stem) if stem.isdigit() else None


def _msg_headers(path: str) -> dict:
    """Parsed relay headers of a msg file (first 8 lines, up to blank).

    ``{}`` on unreadable files. Provenance stays the caller's job:
    the headers are what the file claims, only a ``notification_id``
    matching the basename id makes them trustworthy (the msg_003
    lesson - a foreign writer can collide with an unrelated row id).
    """
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            head = [fh.readline() for _ in range(8)]
    except OSError:
        return {}
    headers = {}
    for line in head:
        line = line.strip()
        if not line:
            break
        key, sep, val = line.partition(":")
        if sep:
            headers[key.strip()] = val.strip()
    return headers


def _msg_is_tick(path: str, nid) -> bool:
    """True only for a genuine relayed heartbeat_tick artifact.

    The filename id alone cannot prove identity - a foreign writer
    may collide with an unrelated row's id - so the relay's own
    headers must agree: ``notification_id`` matching the basename id
    AND ``kind: heartbeat_tick`` verbatim (relay_payload always
    writes both).  A file without parseable headers is not a tick;
    erring on the actionable side re-breaks nothing - the msg_003
    incident was undetected foreign mail, not a noisy wake.
    """
    headers = _msg_headers(path)
    return (
        headers.get("kind") == NotificationKind.HEARTBEAT_TICK.value
        and nid is not None
        and headers.get("notification_id") == str(nid)
    )


def _busy_sessions(sids, state_directory=None, verify_failed_event=None) -> set:
    """Sids verifiably mid-turn per session/list, best-effort.

    One shared attach answers for the whole stall-candidate batch -
    the same verify channel the wake paths use. A session whose
    statusEnum reports live turn work is busy; absent, statusless and
    idle entries are not busy evidence, and a failed attach yields an
    empty set (unverifiable is not busy - escalation stays
    conservative). The busy check is the escalation-grace signal for
    task #174: a mid-turn session cannot drain by definition, so its
    undrained mail is expected lag, not a stall.
    ``verify_failed_event`` names the caller in the decision log -
    the same attach failure must not read as a stall-pass event when
    the lease pass asked.
    """
    if not sids:
        return set()
    lock_data = find_lockfile()
    if lock_data is None:
        return set()
    try:
        statuses = verify_sessions(list(sids), lock_data=lock_data)
    except WakeError as e:
        _decision(
            state_directory,
            verify_failed_event or "mail_stall_verify_failed",
            None,
            f"{e.reason}: {e}",
        )
        return set()
    return {
        sid
        for sid, st in statuses.items()
        if st is not None and st != SESSION_ABSENT and not status_is_idle(st)
    }


def _lease_renew_pass(db_path, state_directory, now: int) -> None:
    """Renew leases about to lapse on verifiably-busy sessions (task #189).

    A DRAIN-mode session cannot renew its own lease inside a long ACP
    turn, so without this pass a >lease-length turn dies on schedule:
    ``recover_stale`` burns an attempt - or fails the task at the
    attempt budget - while the worker is verifiably mid-turn doing
    the work. Session/list busy status IS the liveness signal the
    worker cannot send itself, so the pass renews on its behalf.
    Renew-before-expiry is required, not renew-after: ``recover_stale``
    fires lazily inside every bridge ``_work`` pump, so a lease that
    has already lapsed is gone regardless of what the daemon does
    next.

    Eligibility is deliberately narrow - a missed renewal must fail
    closed into the pre-existing recovery path, never resurrect work
    the owner abandoned: managed+active sessions only (the churn the
    task addresses), lease within the margin, owning session
    verifiably busy per the shared session/list batch, and the last
    explicit lease touch younger than ``_LEASE_AUTO_RENEW_LIMIT`` so
    a wedged turn cannot hold the task forever. Idle, absent,
    statusless, halted-and-idle or unverifiable sessions are simply
    not renewed - their leases lapse into ``recover_stale`` exactly
    as before.

    The shared ``_busy_sessions`` polarity (any non-idle statusEnum
    counts as busy) is deliberately liberal for a liveness grant: a
    status that merely LOOKS mid-turn still renews, but only inside
    the ceiling window - the bound absorbs the false-positive risk.
    """
    con = db.connect(db_path)
    try:
        rows = con.execute(
            "SELECT t.id, t.lease_owner, t.session_id"
            " FROM tasks t JOIN sessions s ON s.session_id=t.session_id"
            " WHERE t.status IN (?,?)"
            " AND s.mode=? AND s.status=?"
            " AND COALESCE(t.step_state,'')<>'waiting_quota'"
            " AND t.lease_expires_at IS NOT NULL AND t.lease_expires_at<=?"
            " AND t.lease_renewed_at IS NOT NULL"
            " AND t.lease_renewed_at+?>=?",
            (
                TaskStatus.RUNNING.value,
                TaskStatus.DISPATCH_DECIDED.value,
                SessionMode.MANAGED.value,
                SessionStatus.ACTIVE.value,
                now + _LEASE_RENEW_MARGIN,
                _LEASE_AUTO_RENEW_LIMIT,
                now,
            ),
        ).fetchall()
    finally:
        con.close()
    if not rows:
        return
    busy = _busy_sessions(
        [r["session_id"] for r in rows],
        state_directory,
        verify_failed_event="lease_renew_verify_failed",
    )
    for r in rows:
        if r["session_id"] not in busy:
            continue
        if db.auto_renew_lease(
            r["id"],
            r["lease_owner"],
            ceiling_seconds=_LEASE_AUTO_RENEW_LIMIT,
            path=db_path,
        ):
            _decision(
                state_directory,
                "lease_auto_renewed",
                r["session_id"],
                f"task={r['id']}",
            )


#: how long a released park re-parks when its single control probe
#: could not be delivered (paced, no lockfile, WakeError) - spending
#: the episode without delivering the nudge would let the re-armed
#: lease lapse into recover_stale and burn an attempt, which is the
#: exact failure the park exists to prevent (task #216 review).
_QUOTA_PROBE_DEFER_SECONDS = 120

#: bound on consecutive probe defers per task episode (task #216
#: review r2): a session whose probe can never land (dead, host gone)
#: would otherwise re-park forever - after the limit the park is
#: released for good and the ordinary lease/recovery lifecycle owns
#: the outcome, with a durable give-up event for the operator.
_QUOTA_PROBE_DEFER_LIMIT = 10


def _defer_probe(db_path, state_directory, sid, task_id, now: int, why: str, row=None) -> None:
    """Re-park a released task when its control probe could not run.

    A short fresh window keeps ``waiting_quota`` semantics alive (no
    attempt spent, no wake noise) until the next sweep retries the
    probe - the alternative, leaving the re-armed lease to lapse,
    would fold the task into ``recover_stale`` for a delivery problem,
    not a worker problem. ``row`` carries the swept park's metadata so
    the defer does not NULL the operator's model/checkpoint fields.
    Bounded per episode: ``quota_defer_count`` increments on each
    defer and resets on every real park, so past
    ``_QUOTA_PROBE_DEFER_LIMIT`` consecutive defers the park is not
    renewed and a ``quota_probe_gave_up`` event marks the hand-off
    back to the ordinary lease lifecycle. The re-park CASes on
    ``step_state IS NULL`` - a fresher self-report park landing
    mid-sweep always wins over a stale defer.
    """
    task = db.get_task(task_id, path=db_path)
    defers = (task["quota_defer_count"] or 0) if task is not None else 0
    if defers >= _QUOTA_PROBE_DEFER_LIMIT:
        _decision(
            state_directory,
            "quota_probe_gave_up",
            sid,
            f"task={task_id} why={why} after {defers} defers",
        )
        db.record_event(
            sid,
            "quota_probe_gave_up",
            f"task={task_id} probe undeliverable after {defers} defers ({why})"
            " - released to ordinary lease lifecycle",
            task_id=task_id,
            path=db_path,
        )
        return
    if db.quota_park(
        task_id,
        sid,
        now + _QUOTA_PROBE_DEFER_SECONDS,
        f"control probe deferred ({why})",
        "probe-defer",
        model=row["quota_model"] if row is not None else None,
        checkpoint=row["quota_checkpoint"] if row is not None else None,
        defer=True,
        require_unparked=True,
        path=db_path,
    ):
        _decision(
            state_directory,
            "quota_probe_deferred",
            sid,
            f"task={task_id} why={why}",
        )
        db.record_event(
            sid,
            "quota_probe_deferred",
            f"task={task_id} {why}",
            task_id=task_id,
            path=db_path,
        )
    else:
        # the task moved on mid-pass - finished, requeued, or a fresher
        # park landed first; journal durably, do not resurrect or
        # overwrite
        _decision(
            state_directory,
            "quota_probe_defer_lost",
            sid,
            f"task={task_id} why={why}",
        )
        db.record_event(
            sid,
            "quota_probe_defer_lost",
            f"task={task_id} defer lost CAS ({why})",
            task_id=task_id,
            path=db_path,
        )


def _quota_pass(db_path, state_directory, now: int, policy: WakePolicy) -> None:
    """Lift expired ``waiting_quota`` parks and fire the control probe.

    ``db.unpark_quota_expired`` clears the step state and re-arms the
    lease for every park whose ``quota_reset_at`` has passed (halted
    sessions stay parked - HALT is senior to a quota reset). Each
    released task then gets exactly one nudge - the control probe that
    tells the claimant to resume the blocked step. A provider that is
    still limited answers with another rate-limit error; the worker's
    self-report parks the task again with the new reset, so repeated
    failures extend the park instead of burning attempts or spamming
    wakes. The ordinary wake cooldown applies so a reset storm cannot
    burst probes past the pacing policy.

    A probe that never lands (paced, no lockfile, WakeError) is not
    spent silently: ``_defer_probe`` re-parks the task for a short
    defer window and the next sweep retries - otherwise the re-armed
    lease would lapse into ``recover_stale`` and burn the attempt
    budget the park protects.
    """
    rows = db.unpark_quota_expired(now, path=db_path)
    if not rows:
        return
    for r in rows:
        sid = r["session_id"]
        _decision(state_directory, "quota_unparked", sid, f"task={r['id']}")
        db.record_event(
            sid,
            "quota_unparked",
            f"task={r['id']} reset reached - control probe",
            task_id=r["id"],
            path=db_path,
        )
        # Re-read the lockfile per attempt like _wake_pass: the host
        # can rotate (restart -> new port/token) between candidates of
        # one pass, and a stale key must not poison the breaker.
        lock_data = find_lockfile()
        if lock_data is None:
            _defer_probe(db_path, state_directory, sid, r["id"], now, "no_lockfile", r)
            continue
        host_key = lock_data["_path"]
        if policy.check(sid, host_key) is not None:
            _defer_probe(db_path, state_directory, sid, r["id"], now, "paced", r)
            continue
        policy.record_attempt(sid)
        try:
            outcome = wake_session(
                sid,
                None,
                lock_data=lock_data,
                nudge=(
                    f"quota reset reached - resume task {r['id']} "
                    "(the step was parked as waiting_quota; saved work stands)"
                ),
            )
        except WakeError as e:
            policy.record_result(host_key, e.reason in _SESSION_LEVEL_REASONS)
            _decision(
                state_directory,
                "quota_probe_failed",
                sid,
                f"task={r['id']} reason={e.reason}",
            )
            _defer_probe(db_path, state_directory, sid, r["id"], now, f"probe_{e.reason}", r)
            continue
        policy.record_result(host_key, True)
        _decision(state_directory, "quota_probe_sent", sid, f"task={r['id']} outcome={outcome}")


def _recovery_pass(db_path, state_directory, notify_send) -> None:
    """Lease-expiry recovery - the daemon owns the liveness leg (task #207 b).

    ``recover_stale`` used to fire only lazily inside the CLI
    ``_work`` pump and a session-start hook, so a fleet with zero
    CLI traffic left expired leases frozen forever - the split-brain
    recovery-ownership gap the audit flagged. The pass runs right
    after ``_lease_renew_pass`` so verifiably-busy tasks get their
    renewal first and only genuinely lapsed leases fold here.

    Effects mirror the CLI pump's recover leg: a requeued task is a
    routine journal line, a terminal FAILED task additionally pings
    the operator via notify-send once (``mark_notified`` dedups
    across the daemon and any racing CLI pump - the CLI leg stays as
    a fallback for daemon-down operation and is idempotent by CAS).
    """
    for tid in db.recover_stale(path=db_path):
        row = db.get_task(tid, path=db_path)
        if row is None:
            continue
        if row["status"] != TaskStatus.FAILED.value:
            _decision(
                state_directory,
                "task_recovered",
                row["session_id"],
                f"task={tid} requeued (lease expired)",
            )
            continue
        detail = "lease expired; attempts exhausted"
        if db.mark_notified(tid, path=db_path):
            notify_send(
                "sessions-bridge FAILED",
                f"{row['session_id']}: {detail}",
            )
        _decision(
            state_directory,
            "task_recovered",
            row["session_id"],
            f"task={tid} -> failed ({detail})",
        )


def _mail_stall_pass(
    db_path,
    state_directory,
    now: int,
    notify_send=None,
    queue_dir=None,
    pending=None,
) -> int:
    """Escalate undrained mail to the orchestrator, once per episode.

    For every session whose actionable mail (relayed, unacked,
    non-heartbeat_tick, still on disk) sits past
    ``_MAIL_STALL_SECONDS`` the daemon pushes a ``notice``
    notification carrying the ``[MAIL_STALL]`` marker into the active
    orchestrator's mailbox - naming the stalled sid, project, unacked
    count, oldest mail age, pending questions and last-seen age
    (directive 445) - so the orchestrator can manual-wake, forward to
    ``tg_escalate`` or escalate to the developer. The orchestrator's
    own stall goes to ``notify_send`` instead (a self-addressed
    notice would sit undrained too). Each escalation also appends a
    durable ``decision_events`` row (``kind=mail_stall``) with the
    same payload fields (task #210 item 3).

    "Still on disk" is load-bearing (task #143): consumed-but-unacked
    mail - file deleted, ack pending at a turn boundary - is not a
    stall, so an active session mid-drain is never escalated.
    Unconsumed ``msg_*.txt`` files count as undrained too (item_139
    amendment): foreign writers bypass the notifications table, so a
    hand-written directive that never reaches its session must still
    escalate - the file's mtime is its age.

    Dedup is the ``wake_state.mail_escalated_at`` stamp: one
    escalation per stall episode, cleared when the mailbox drains or
    when the oldest remaining mail is newer than the stamp (the
    escalated items drained - a newer stall is a fresh episode). A
    stamp older than ``_MAIL_STALL_REALERT_SECONDS`` (default 3600s)
    re-arms itself: a stall that never resolves re-alerts on the
    cadence inside the same episode instead of going silent forever
    (directive 445).
    Returns the maximum undrained age seen this pass - the caller
    writes it into ``daemon_heartbeat.txt`` as the stall metric.
    """
    undrained = db.sessions_with_undrained_mail(path=db_path)
    if pending is None:
        pending = _pending_mailbox_dirs(db_path, queue_dir)
    # Undrained means STILL ON DISK: the drain duty is cat+rm THEN
    # ack, so a notification row whose msg_<nid>.txt the session
    # already deleted is consumed-but-unacked - ack lag at a turn
    # boundary, not a stall. Counting it escalated for a session
    # mid-drain (the mahogany-dryosaurus false positive, task #143).
    # Registered nids not in pending were consumed; files in pending
    # without a matching row are genuinely unregistered foreign mail
    # (the msg_003 case) - only they join the count.
    #
    # A session verifiably mid-turn per session/list gets grace
    # instead of escalation (task #174): it cannot drain mid-turn, so
    # its undrained mail is expected lag. The deferral consumes no
    # episode stamp - once it goes idle with the mail still on disk,
    # the same stall escalates. Grace is bounded by
    # ``_BUSY_GRACE_SECONDS`` so a wedged turn cannot silence the
    # signal forever. Dead, absent, unmanaged and unverifiable
    # sessions are never graced.
    #
    # The shared ``pending`` scan is only a candidate hint: it is
    # taken before _wake_pass, and a wake can itself trigger a drain
    # that removes files before this merge - every candidate dir is
    # re-scanned fresh via _dir_pending so a mid-pass rm can never be
    # counted (the same false positive in a narrower window).
    for sid in list(set(undrained) | set(pending)):
        r = _dir_pending(db_path, queue_dir, sid)
        if r is None:
            undrained.pop(sid, None)
            continue
        u = undrained.get(sid)
        nids = u["nids"] if u is not None else set()
        kept = sum(1 for n in r["names"] if _msg_nid(n) in nids)
        undrained[sid] = {
            "oldest": r["oldest"],
            "count": kept,
            "foreign": len(r["names"]) - kept,
        }
    max_age = 0
    for r in undrained.values():
        max_age = max(max_age, now - r["oldest"])

    # episode re-arm: a fully-drained mailbox obviously opens a fresh
    # episode, but so does one whose OLDEST remaining mail is newer
    # than the stamp - the previously escalated items were drained
    # and what sits there now is a new stall, not the same one
    # (review: without this, a session that never fully drains gets
    # exactly one escalation ever).
    for sid, ws in db.list_wake_state(path=db_path).items():
        stamp = ws.get("mail_escalated_at")
        if stamp is None:
            continue
        if sid not in undrained or undrained[sid]["oldest"] > stamp:
            db.clear_mail_escalated(sid, path=db_path)

    con = db.connect(db_path)
    try:
        orch = con.execute(
            "SELECT session_id FROM sessions WHERE role=? AND status=?"
            " ORDER BY updated_at DESC LIMIT 1",
            (SessionRole.ORCHESTRATOR.value, SessionStatus.ACTIVE.value),
        ).fetchone()
    finally:
        con.close()
    orch_sid = orch["session_id"] if orch else None

    # Busy grace (task #174): a session verifiably mid-turn cannot
    # drain by definition - its undrained mail is expected ack lag,
    # not a stall, so the escalation is deferred instead of fired.
    # The episode stamp is NOT consumed: when the session goes idle
    # with the mail still undrained it escalates on a later pass, so
    # busy buys grace, not amnesty. The grace is also bounded by
    # _BUSY_GRACE_SECONDS: mail older than that on a still-busy
    # session may mean a wedged turn, so it escalates anyway. Dead,
    # absent, unmanaged and unverifiable sessions escalate exactly
    # as before.
    busy = _busy_sessions(
        [sid for sid, r in undrained.items() if now - r["oldest"] >= _MAIL_STALL_SECONDS],
        state_directory,
    )

    # Halt parking (task #180): a halted session's undrained mail is
    # parked until `bridge resume` by design - it is the strongest
    # "expected lag" signal we have and needs no grace ceiling.
    halted = {
        sid
        for sid, r in undrained.items()
        if now - r["oldest"] >= _MAIL_STALL_SECONDS
        and (sess := db.get_session(sid, path=db_path)) is not None
        and sess["halted_at"] is not None
    }

    for sid, r in undrained.items():
        age = now - r["oldest"]
        if age < _MAIL_STALL_SECONDS:
            continue
        if sid in halted:
            _decision(
                state_directory,
                "mail_stall_deferred_halted",
                sid,
                f"age={age}s count={r['count']} session halted",
            )
            continue
        if sid in busy and age < _BUSY_GRACE_SECONDS:
            _decision(
                state_directory,
                "mail_stall_deferred_busy",
                sid,
                f"age={age}s count={r['count']} session mid-turn",
            )
            continue
        # pick the delivery target FIRST: the dedup stamp is consumed
        # only when an escalation can actually leave the daemon
        # (review: stamping before delivery meant a notify-send-less
        # pass logged the episode's only escalation into the void).
        if orch_sid is not None and sid != orch_sid:
            target = "orchestrator"
        elif notify_send is not None:
            target = "notify-send"
        else:
            _decision(
                state_directory,
                "mail_stall_deferred",
                sid,
                f"age={age}s count={r['count']} no escalation target",
            )
            continue
        # task #210 item 3 / directive 445: the escalation payload
        # names project, unacked count, oldest mail age, pending
        # questions and last-seen age so the orchestrator's
        # tg_escalate forward carries it verbatim; delivery rides
        # NOTICE + the [MAIL_STALL] marker (a parseable alert class,
        # not an obey-directive), and the durable decision_events row
        # keeps the same fields restart-proof for audit.
        sess = db.get_session(sid, path=db_path)
        project = None
        if sess is not None:
            cwd = sess["cwd"]
            project = (os.path.basename(cwd.rstrip("/")) or cwd) if cwd else sess["title"]
        con = db.connect(db_path)
        try:
            pq = con.execute(
                "SELECT COUNT(*) AS c FROM questions WHERE session_id=? AND state=?",
                (sid, QuestionState.PENDING.value),
            ).fetchone()["c"]
            hb = con.execute(
                "SELECT last_seen_at FROM worker_heartbeats WHERE session_id=?",
                (sid,),
            ).fetchone()
        finally:
            con.close()
        last_seen_s = max(0, now - hb["last_seen_at"]) if hb is not None else -1
        fields = (
            f"unacked={r['count']} oldest_mail_age_s={age}"
            f" pending_questions={pq} last_seen_s={last_seen_s}"
        )
        body = (
            f"[MAIL_STALL] mail undrained {age // 60}m: {sid}"
            + (f" ({project})" if project else "")
            + f" has {r['count']} unacked"
            f" notification(s) + {r.get('foreign', 0)} unregistered"
            f" mailbox file(s) {fields}"
            " - drain is not reaching it"
            " (manual `bridge wake` or escalate to developer)"
        )
        # stamp + journal atomically: the episode dedup guard and the
        # restart-proof decision_events payload commit together, so a
        # crash can neither lose the journal nor re-alert an episode
        # whose event already landed
        if not db.mark_mail_escalated(
            sid,
            rearm_after=_MAIL_STALL_REALERT_SECONDS,
            now=now,
            event_body=(
                f"reason=mail_stall project={project or '-'} {fields}"
                f" pending_foreign={r.get('foreign', 0)}"
                f" target={target}"
            ),
            path=db_path,
        ):
            continue  # already escalated this episode, cadence not due
        if target == "orchestrator":
            db.notify(orch_sid, NotificationKind.NOTICE, body, path=db_path)
        else:
            notify_send("sessions-bridge MAIL STALL", body)
        _decision(
            state_directory,
            "mail_stall_escalated",
            sid,
            f"age={age}s count={r['count']} -> {target}"
            + (f" {orch_sid}" if target == "orchestrator" else ""),
        )
    return max_age


def _mark_dead_no_session(db_path, state_directory, sid, streak: int, notify=None) -> None:
    """no_session streak reached the dead threshold.

    Absent from session/list over enough attempts = really gone
    (closed tab) - the managed dead path; kills its live task like
    the unmanaged sweep does. An orchestrator gets the sweep path's
    parity instead (issue #115): ``orchestrator_dead`` + notify-send,
    no task requeue - its live row belongs to the orchestrator role,
    not a reclaimable worker slot. Both branches dual-journal the
    durable ``decision_events`` row the resurrect leg's marker check
    reads (the jsonl decision log is not queryable). The CAS, the
    task fold and the marker all commit inside ``db.mark_dead``'s
    single ``BEGIN IMMEDIATE`` - a ``bridge kill`` racing the edge
    can no longer wedge ``session_killed`` between the flip and the
    marker and lose to the late liveness verdict (issue #115 review).
    """
    row = db.get_session(sid, path=db_path)
    if row is not None and row["role"] == SessionRole.ORCHESTRATOR.value:
        detail = f"reason=no_session_streak streak={streak}"
        body = db.mark_dead(
            sid,
            "orchestrator_dead",
            detail + (" notify-send sent" if notify is not None else ""),
            path=db_path,
        )
        if body is None:
            return  # already dead or raced - handled before
        if notify is not None:
            notify(
                "sessions-bridge ORCHESTRATOR DEAD",
                f"{sid}: absent from session/list in {streak} consecutive probes",
            )
        _decision(state_directory, "orchestrator_dead", sid, body)
        return
    task = db.get_live_task(sid, path=db_path)
    head = f"reason=no_session_streak streak={streak}"
    body = db.mark_dead(
        sid,
        "worker_dead",
        head if task is not None else f"{head} no live task",
        task_id=task["id"] if task is not None else None,
        task_note=requeue_note(task["id"]) if task is not None else None,
        path=db_path,
    )
    if body is None:
        return  # already dead or raced - handled before
    _decision(state_directory, "worker_dead", sid, body)


# A fresh-enough ack watermark proves the drain turn ran even when the
# re-check finds the session idle again - acks only move inside live
# turns. Covers the "nudge landed, turn opened and closed before
# verify" window a point-in-time statusEnum cannot see (item_21
# review). Generous on purpose: a session that acked within the last
# two minutes is demonstrably reachable.
_VERIFY_ACK_GRACE_SECONDS = 120


def _verify_status_label(status) -> str:
    """Human-readable status for decision-log details - the raw
    SESSION_ABSENT sentinel and bare None must not leak verbatim."""
    if status == SESSION_ABSENT:
        return "absent"
    return status if status is not None else "none"


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
        absent = status == SESSION_ABSENT
        # the ack watermark is read before the no_session accounting:
        # a fresh ack proves a live drain turn even when session/list
        # raced and reported the session absent (item_21 r3)
        ack = db.last_ack_at(sid, path=db_path)
        ack_fresh = ack is not None and now - int(ack) <= _VERIFY_ACK_GRACE_SECONDS
        if absent and not ack_fresh:
            # absent on the re-check is the same evidence as a failed
            # session/list lookup: feed the no_session streak so a
            # dead tab counts toward dead-marking (item_21 review)
            ns_streak = policy.record_no_session(sid)
            if ns_streak >= _NO_SESSION_DEAD_STREAK:
                _mark_dead_no_session(db_path, state_directory, sid, ns_streak, notify)
                continue  # dead-marked - the unconfirmed bump is moot
        else:
            # listed (possibly statusless), or absent but provably
            # live via a fresh ack - either way not no_session evidence
            policy.record_session_seen(sid)
        alive = ack_fresh or (not absent and status is not None and not status_is_idle(status))
        if alive:
            db.record_wake_outcome(sid, "verified", path=db_path)
            policy.record_confirmed(sid)
            _decision(
                state_directory,
                "wake_verified",
                sid,
                f"status={_verify_status_label(status)}",
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
                f"streak={streak} status={_verify_status_label(status)}",
            )
        else:
            _decision(
                state_directory,
                "wake_unconfirmed",
                sid,
                f"streak={streak} status={_verify_status_label(status)}",
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
            # per-pass timing instrumentation (task #203, audit
            # finding): the loop is a sequence of full-scan passes;
            # without durations a pass quietly outgrowing the
            # interval is invisible until the fleet looks stalled.
            # One consolidated decision event per iteration keeps
            # the journal grep-able without per-pass line spam. The
            # accumulator is bound before try so the finally-emit can
            # never see an unbound name.
            timings: dict[str, float] = {}

            def _timed(name, fn, *a, _acc=timings):
                start = time.monotonic()
                try:
                    return fn(*a)
                finally:
                    _acc[name] = round((time.monotonic() - start) * 1000, 1)

            try:
                now = int(time.time())
                _timed("schedule", _schedule_pass, db_path, state_directory, now)
                _timed("relay", _relay_pass, db_path, queue_dir, state_directory, now)
                # stale-control janitor (issue #119) runs after relay
                # so a fresh superseding command is already on disk
                # before older control files are judged against it
                _timed(
                    "stale_control",
                    _stale_control_pass,
                    db_path,
                    queue_dir,
                    state_directory,
                    now,
                )
                # one mailbox-dir scan per pass shared by the wake and
                # stall legs (review: computing it twice per pass was a
                # wasted second connection + listdir round)
                pending = _timed("pending_scan", _pending_mailbox_dirs, db_path, queue_dir)
                _timed(
                    "wake",
                    _wake_pass,
                    db_path,
                    state_directory,
                    now,
                    wake_policy,
                    notify,
                    queue_dir,
                    pending,
                )
                _timed("lease_renew", _lease_renew_pass, db_path, state_directory, now)
                # quota sweep (task #216): expired parks release here
                # and get their single control probe; still-open
                # windows suppress the wake leg by reset_at > now, so
                # the pass order vs wake is not load-bearing
                _timed("quota", _quota_pass, db_path, state_directory, now, wake_policy)
                # recovery runs AFTER lease_renew: verifiably-busy
                # tasks renew first, only genuinely lapsed leases
                # fold into recover_stale here (task #207 item b)
                _timed("recovery", _recovery_pass, db_path, state_directory, notify)
                _timed("sweep", _sweep_pass, db_path, state_directory, notify, now)
                stall_age = _timed(
                    "mail_stall",
                    _mail_stall_pass,
                    db_path,
                    state_directory,
                    now,
                    notify,
                    queue_dir,
                    pending,
                )
                _timed("question_janitor", _question_janitor_pass, db_path, state_directory)
                # metric line: the oldest undrained-mail age rides along
                # in the heartbeat file (external watchdogs read line 1
                # only - line 2 is observability, not a contract)
                _write_heartbeat(state_directory, f"mail_undrained_max_age={stall_age}")
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
            finally:
                # the journal line rides in finally: a pass that throws
                # mid-iteration is exactly where a stalled pass would
                # hide - record whatever durations got measured
                # (review W6: keeping this inside try meant the timing
                # event silently vanished on every pass failure)
                if timings:
                    _decision(
                        state_directory,
                        "pass_timing",
                        detail=" ".join(f"{name}={ms}ms" for name, ms in timings.items()),
                    )
            if once:
                break
            stop.wait(interval)
    finally:
        _decision(state_directory, "daemon_stop", detail=f"pid={os.getpid()}")
        os.close(lock_fd)
    return RUN_OK

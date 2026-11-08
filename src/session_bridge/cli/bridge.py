"""Bridge CLI + dispatcher (item_07).

``bridge`` is the operator and AI-orchestrator surface of
sessions-bridge. There is no daemon: every mutating command (push,
clear, unpark, retry), ``status`` and each exiting ``_work`` process
runs a ``pump`` pass - lazy lease recovery first, then dispatch of
queued tasks.

Dispatch policy per queued task:

- ``db.is_locked`` true (live devin PID holds the session) - transport
  ``hook-queue``: the task is claimed and atomically folded back to
  ``queued`` with the hook transport, then ``queue/<sid>.json`` is
  written (tmp + fsync + rename, after the db commit) to wake the
  parked Stop hook. The db stays the source of truth; the trigger is
  only a signal.
- unlocked - transport ``acp-push``: the task is claimed, marked
  ``running`` with a lease, and a detached ``bridge _work <task_id>
  --owner <owner>`` child is spawned. At most ``SEMAPHOR`` concurrent
  acp-push tasks; the rest stay queued for the next pump.
- unknown session (not locked, absent from devin's sessions.db and the
  bridge sessions table) - left queued; ``push`` reports ``not_found``
  while the task is preserved.

``_work`` runs one acp push cycle: resolve the session cwd, ``session/
load`` (a ``session_locked`` answer falls back to the hook queue
atomically - no half-state), ``session/prompt`` with a lease-renewing
thread, ``session/close`` and ``finish_task`` keyed on ``stopReason``.
Retryable acp failures leave the task running so lease expiry can
requeue-or-fail it with attempt accounting; permanent failures finish
it ``failed`` and notify. The worker ends with a pump pass so a freed
slot dispatches the next queued task.

Notifications: transitions to NEEDS_KICK/FAILED call
``notify-send -u critical`` through ``db.mark_notified``'s 300s dedup;
a missing notify-send is ignored.

``bridge schedule`` manages persistent recurring task sources: a
session id with ``--task`` plus ``--every N[s|m|h|d]`` or ``--at
HH:MM`` adds one (``--catchup`` pins the misfire policy), ``list``
shows them and ``clear <id>`` disables one. The daemon materializes
due rows into regular queued tasks.
"""

import argparse
import importlib.metadata
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from session_bridge.acp.client import AcpClient
from session_bridge.acp.contract import AcpError
from session_bridge.cli.contract import (
    Command,
    Event,
    EventKind,
    ExitCode,
    JsonResponse,
)
from session_bridge.daemon import core as daemon
from session_bridge.ingress import spec as ingress_spec
from session_bridge.ingress import sync as ingress_sync
from session_bridge.mailbox import mailbox_dir
from session_bridge.state import db
from session_bridge.state.contract import (
    LEASE_SECONDS,
    MAX_ATTEMPTS,
    MAX_QUESTIONS_PER_TASK,
    AuditEventKind,
    NotificationKind,
    QuestionState,
    SessionMode,
    SessionRole,
    SessionStatus,
    TaskStatus,
    Transport,
)
from session_bridge.state.verdict import (
    ReleaseNotFound,
    ReleaseRefusedOwner,
    ReleaseRemoved,
    ReserveConflict,
    ReserveGranted,
    UnregisterNotFound,
    UnregisterRefusedFreshHeartbeat,
    UnregisterRefusedLiveTask,
)
from session_bridge.wake import (
    SESSION_ABSENT,
    WakeError,
    find_lockfile,
    verify_sessions,
    wake_session,
)

__all__ = ["main", "pump"]

#: concurrent acp-push sessions cap; queued tasks beyond it wait.
SEMAPHOR = 3

#: default end_turn wait for ``session/prompt`` in the worker.
WORK_TIMEOUT_DEFAULT = 3600

#: lease renewal cadence inside a running prompt wait.
LEASE_RENEW_INTERVAL = max(60, LEASE_SECONDS // 3)

#: lease-owner prefixes: ``acp-`` for _work children, ``bridge-`` for
#: the transient claim used when deciding the hook transport.
ACP_OWNER_PREFIX = "acp-"
DISPATCH_OWNER_PREFIX = "bridge-"

#: per-session STATE names shown by ``status``.
STATE_RUNNING = "RUNNING"
STATE_DISPATCHING = "DISPATCHING"
STATE_PARKED = "PARKED"
STATE_HEADLESS_IDLE = "HEADLESS-IDLE"
STATE_NEEDS_KICK = "NEEDS-KICK"
STATE_FAILED = "FAILED"

_END_TURN = "end_turn"
_ASSISTANT_TAIL = 4000
_NOTE_TAIL = 2000

_LIVE_VALUES = frozenset(
    s.value
    for s in (
        TaskStatus.QUEUED,
        TaskStatus.DISPATCH_DECIDED,
        TaskStatus.RUNNING,
        TaskStatus.AWAITING_USER,
    )
)


def _log(msg: str) -> None:
    sys.stderr.write(f"sessions-bridge: {msg}\n")
    sys.stderr.flush()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


def _work_timeout(args) -> int:
    return getattr(args, "timeout", None) or _env_int(
        "SESSIONS_BRIDGE_WORK_TIMEOUT", WORK_TIMEOUT_DEFAULT
    )


def _queue_dir(args) -> str:
    if args.queue_dir:
        return args.queue_dir
    base = args.db if args.db else db.DB_PATH
    return os.path.join(os.path.dirname(os.path.abspath(base)), "queue")


def _queue_depth(args) -> int:
    """True queued count - a COUNT(*), never the capped task list."""
    return db.count_tasks(status=TaskStatus.QUEUED, path=args.db)


# --- worker stderr logs --------------------------------------------------------

#: per-invocation worker log: <state_dir>/logs/worker_<tid>_<attempt>_<ts>.log
WORKER_LOG_MAX_BYTES = 1_000_000  # oversized logs keep their tail
WORKER_LOG_KEEP = 10  # retention: newest files survive the pump sweep
#: retention never unlinks a log touched inside one lease window: a
#: file that fresh can still belong to a live, writing worker
WORKER_LOG_MIN_AGE_SECONDS = LEASE_SECONDS
_STDERR_TAIL_LINES = 3


def _logs_dir(args) -> str:
    base = args.db if args.db else db.DB_PATH
    return os.path.join(os.path.dirname(os.path.abspath(base)), "logs")


def _worker_log_path(task_id: int, args) -> str:
    task = db.get_task(task_id, path=args.db)
    attempt = task["attempt"] if task is not None else 0
    return os.path.join(
        _logs_dir(args),
        f"worker_{task_id}_{attempt}_{int(time.time())}.log",
    )


def _sweep_worker_logs(args) -> None:
    """Bound the worker log dir: tail-cap oversized files, keep newest."""
    log_dir = _logs_dir(args)
    try:
        paths = [
            os.path.join(log_dir, n)
            for n in os.listdir(log_dir)
            if n.startswith("worker_") and n.endswith(".log")
        ]
    except OSError:
        return
    for p in paths:
        try:
            if os.path.getsize(p) <= WORKER_LOG_MAX_BYTES:
                continue
            mtime = os.path.getmtime(p)
            with open(p, "rb+") as fh:
                fh.seek(-WORKER_LOG_MAX_BYTES, os.SEEK_END)
                tail = fh.read()
                fh.seek(0)
                fh.write(tail)
                fh.truncate()
            # restore mtime: the tail-cap is our own maintenance
            # write, not worker activity - it must not make an old
            # log look fresh to the retention pass below
            os.utime(p, (mtime, mtime))
        except OSError:
            continue
    try:
        now = time.time()
        paths = [
            p
            for p in paths
            if os.path.exists(p) and now - os.path.getmtime(p) > WORKER_LOG_MIN_AGE_SECONDS
        ]
        paths.sort(key=lambda p: os.path.getmtime(p))
        for p in paths[: max(0, len(paths) - WORKER_LOG_KEEP)]:
            os.unlink(p)
    except OSError:
        pass


def _worker_log_tail(args, task_id: int):
    """(path, tail) of the newest log a worker wrote for ``task_id``."""
    log_dir = _logs_dir(args)
    try:
        paths = [
            os.path.join(log_dir, n)
            for n in os.listdir(log_dir)
            if n.startswith(f"worker_{task_id}_") and n.endswith(".log")
        ]
    except OSError:
        return None, None
    if not paths:
        return None, None
    try:
        path = max(paths, key=lambda p: os.path.getmtime(p))
        with open(path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            size = fh.tell()
            fh.seek(max(0, size - 8192))
            raw = fh.read()
    except OSError:
        return None, None
    lines = [line for line in raw.decode("utf-8", "replace").splitlines() if line.strip()]
    return path, "\n".join(lines[-_STDERR_TAIL_LINES:]) or None


# --- queue trigger ------------------------------------------------------------


def _write_trigger(queue_dir: str, session_id: str) -> None:
    """Write ``queue/<sid>.json`` atomically (tmp + fsync + rename).

    Written only after the state.db commit; it is a wakeup signal for
    the parked Stop hook, never a store, so a best-effort write is
    correct.
    """
    tmp = None
    try:
        os.makedirs(queue_dir, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".trigger-", dir=queue_dir)
        with os.fdopen(fd, "w") as fh:
            json.dump({"session_id": session_id, "ts": int(time.time())}, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(
            tmp,
            os.path.join(queue_dir, db.trigger_filename(session_id)),
        )
        tmp = None
    except OSError as e:
        _log(f"cannot write queue trigger for {session_id}: {e}")
    finally:
        if tmp is not None:
            try:
                os.unlink(tmp)
            except OSError:
                pass


# --- notifications -------------------------------------------------------------


def _notify_send(title: str, body: str) -> None:
    """``notify-send -u critical``; fail-safe when the binary is absent."""
    exe = shutil.which("notify-send")
    if exe is None:
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


def _notify(args, task, kind: str, detail: str) -> None:
    """Deduped operator notification for a task transition."""
    try:
        if not db.mark_notified(task["id"], path=args.db):
            return
    except Exception:
        return
    _notify_send(
        f"sessions-bridge {kind}",
        f"{task['session_id']}: {detail or 'needs attention'}",
    )


# --- session resolution --------------------------------------------------------


def _session_cwd(session_id: str, args) -> str | None:
    """cwd for an acp load: devin's sessions.db first, then the bridge
    sessions row recorded by SessionStart hooks."""
    info = db.sessions_db_lookup(session_id)
    if info is not None:
        cwd = info.get("working_directory") or info.get("cwd")
        if cwd:
            return cwd
    row = db.get_session(session_id, path=args.db)
    if row is not None and row["cwd"]:
        return row["cwd"]
    return None


def _session_known(session_id: str, args) -> bool:
    """True when any evidence of the session exists."""
    locked, _pid = db.is_locked(session_id)
    if locked:
        return True
    if db.sessions_db_lookup(session_id) is not None:
        return True
    return db.get_session(session_id, path=args.db) is not None


def _session_mode(session_id: str, args) -> str:
    """sessions.mode for a session (unmanaged when the row is absent)."""
    row = db.get_session(session_id, path=args.db)
    if row is None:
        return SessionMode.UNMANAGED.value
    return row["mode"]


def _has_open_dispatch(task_id: int, args) -> bool:
    """True when the task already has an unacked TASK_DISPATCH row.

    The dedup guard for managed dispatch: one queued task maps to at
    most one open outbox notification, so pump passes do not spam the
    mailbox while the task waits for the daemon relay (item_13d) and
    the recipient's ack watermark (``ack_at``) marks delivery closed.
    """
    con = db.connect(args.db)
    try:
        row = con.execute(
            "SELECT 1 FROM notifications WHERE task_id=? AND kind=? AND ack_at IS NULL LIMIT 1",
            (task_id, NotificationKind.TASK_DISPATCH.value),
        ).fetchone()
        return row is not None
    finally:
        con.close()


# --- dispatch ------------------------------------------------------------------


def _spawn_worker(task_id: int, owner: str, args) -> subprocess.Popen:
    """Detach a ``bridge _work`` child for one acp-push task."""
    argv = [sys.executable, "-m", "session_bridge.cli.bridge"]
    if args.db:
        argv += ["--db", args.db]
    if args.queue_dir:
        argv += ["--queue-dir", args.queue_dir]
    argv += [
        Command.WORK.value,
        str(task_id),
        "--owner",
        owner,
        "--timeout",
        str(_work_timeout(args)),
    ]
    env = dict(os.environ)
    # make the package importable even when the parent ran from a
    # source checkout without an install
    src = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    prev = env.get("PYTHONPATH")
    env["PYTHONPATH"] = f"{src}{os.pathsep}{prev}" if prev else src
    # stderr goes to a per-invocation log (not DEVNULL) so a crashed
    # worker stays diagnosable; the pump sweep bounds size and count
    stderr: object = subprocess.DEVNULL
    log_fh = None
    try:
        os.makedirs(_logs_dir(args), exist_ok=True)
        log_fh = open(_worker_log_path(task_id, args), "ab")
        stderr = log_fh
    except OSError as e:
        _log(f"cannot open worker log for task {task_id}: {e}")
    try:
        proc = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=stderr,
            start_new_session=True,
            close_fds=True,
            env=env,
        )
    finally:
        if log_fh is not None:
            log_fh.close()  # the child keeps its own dup of the fd
    return proc


def _task_to_hook_queue(task_id: int, owner: str, args) -> bool:
    """Atomically requeue our task for the hook transport.

    The task can be in either of two states when this runs: still
    ``dispatch_decided`` (``fallback_to_hook`` wins) or already
    ``running`` under our lease (pump admitted it before the worker
    answered - ``demote_running_to_hook`` wins). Both are single
    guarded UPDATEs in db.py - no half-state is ever committed.
    """
    if db.fallback_to_hook(task_id, owner, path=args.db):
        return True
    return db.demote_running_to_hook(task_id, owner, path=args.db)


def _gc_dead_sessions(args) -> None:
    """Lazy GC of dead registrations (task #207 item f).

    Every bus-touching call (``bridge status``, ``wait``, the
    ``pump`` pass behind ``push``/``_work``) marks unmanaged
    sessions dead when their liveness TTL passed - the same verdict
    the daemon sweep gives, so a ghost dies on the next operator
    call even with the daemon down. Two legs ride the shared
    ``db.stale_unmanaged_sessions`` scan: silent stamped heartbeats
    (``HEARTBEAT_STALE_SECONDS``) and no-heartbeat-row ghosts
    (``GHOST_TTL_SECONDS``). The ``set_session_status`` CAS dedups a
    racing daemon sweep: only the active->dead edge fires effects,
    whoever flips it.
    """
    now = int(time.time())
    for row in db.stale_unmanaged_sessions(now, path=args.db):
        daemon.mark_dead(row, args.db, _state_dir(args), _notify_send, now, tag="gc ")


def pump(args) -> dict:
    """Lazy recovery + one dispatch pass over queued tasks.

    Returns ``{"events", "hooked", "spawned", "unknown", "managed",
    "saturated"}`` for the calling command's response. The queue is
    scanned in capped batches paginated by task id, so every queued
    task is visited even beyond the ``list_tasks`` limit; hook-folded
    tasks land back in ``queued`` keeping the same id - already below
    ``last_id``, so they are never revisited in the same pass.
    """
    events: list[Event] = []
    hooked: list[int] = []
    spawned: list[int] = []
    unknown: list[str] = []
    managed: list[int] = []
    saturated = False

    _sweep_worker_logs(args)

    _gc_dead_sessions(args)

    for tid in db.recover_stale(path=args.db):
        row = db.get_task(tid, path=args.db)
        if row is not None and row["status"] == TaskStatus.FAILED.value:
            detail = "lease expired; attempts exhausted"
            events.append(
                Event(
                    kind=EventKind.FAILED,
                    session_id=row["session_id"],
                    task_id=tid,
                    detail=detail,
                    ts=int(time.time()),
                )
            )
            _notify(args, row, "FAILED", detail)

    running_acp = db.count_tasks(
        status=TaskStatus.RUNNING,
        transport=Transport.ACP_PUSH,
        path=args.db,
    )

    last_id = 0
    while True:
        batch = db.list_tasks(
            status=TaskStatus.QUEUED,
            path=args.db,
            after_id=last_id,
        )
        if not batch:
            break
        for task in batch:
            last_id = task["id"]
            tid = task["id"]
            sid = task["session_id"]
            if _session_mode(sid, args) == SessionMode.MANAGED.value:
                # managed sessions take deliveries only through the
                # notifications outbox -> fs mailbox (the daemon relay
                # writes the msg file in item_13d). dispatch_managed
                # marks the task dispatch_decided with a lease the
                # moment the notification is written (item_18): the
                # task leaves the queued scan so it cannot be re-
                # notified every pass, and a never-claimed delivery
                # folds back via recover_stale when the lease lapses.
                if not _has_open_dispatch(tid, args):
                    owner = _managed_owner(sid, tid)
                    if db.dispatch_managed(tid, owner, path=args.db):
                        db.notify(
                            sid,
                            NotificationKind.TASK_DISPATCH,
                            task["task_text"] or f"execute spec file {task['task_file']}",
                            task_id=tid,
                            path=args.db,
                        )
                managed.append(tid)
                continue
            locked, _pid = db.is_locked(sid)
            if locked:
                # transport decision: the session is open in devin, so
                # the parked Stop hook delivers it - claim + fold back
                # to queued with the hook transport, then wake the hook
                owner = f"{DISPATCH_OWNER_PREFIX}{sid}-{os.getpid()}-{tid}"
                if db.claim_queued(
                    tid, owner, Transport.HOOK_QUEUE, path=args.db
                ) and db.fallback_to_hook(tid, owner, path=args.db):
                    _write_trigger(_queue_dir(args), sid)
                    hooked.append(tid)
                continue
            if running_acp >= SEMAPHOR:
                saturated = True
                continue  # stays queued until a slot frees
            if not _session_known(sid, args):
                if sid not in unknown:
                    unknown.append(sid)
                continue
            owner = f"{ACP_OWNER_PREFIX}{sid}-{os.getpid()}-{tid}"
            if not db.claim_queued(tid, owner, Transport.ACP_PUSH, path=args.db):
                continue
            # atomic admission: the running count is checked inside the
            # same CAS statement, so racing pumps can never exceed
            # SEMAPHOR; running + lease is set before the child exists
            # so a spawn failure cannot strand a dispatch_decided task
            if not db.mark_running(tid, owner, max_running=SEMAPHOR, path=args.db):
                row = db.get_task(tid, path=args.db)
                if (
                    row is not None
                    and row["status"] == TaskStatus.DISPATCH_DECIDED.value
                    and row["lease_owner"] == owner
                ):
                    # the claim is still ours: fold it back so the
                    # task waits for the next pass instead of wedging
                    # on a dying lease
                    db.release_claim(tid, owner, path=args.db)
                    # report saturation only when the semaphore was
                    # the refusing guard: a claim lease still alive
                    # now was alive at CAS time, and a live running
                    # count at the cap leaves the count predicate as
                    # the only one that could have failed - an
                    # expired claim lease or another CAS loss is not
                    # semaphore saturation
                    if (
                        row["lease_expires_at"] is not None
                        and row["lease_expires_at"] > int(time.time())
                        and db.count_tasks(
                            status=TaskStatus.RUNNING,
                            transport=Transport.ACP_PUSH,
                            path=args.db,
                        )
                        >= SEMAPHOR
                    ):
                        saturated = True
                continue
            try:
                _spawn_worker(tid, owner, args)
            except OSError as e:
                _log(f"worker spawn failed for task {tid}: {e}; requeueing for the hook path")
                if _task_to_hook_queue(tid, owner, args):
                    _write_trigger(_queue_dir(args), sid)
                    hooked.append(tid)
                continue
            spawned.append(tid)
            running_acp += 1
    return {
        "events": events,
        "hooked": hooked,
        "spawned": spawned,
        "unknown": unknown,
        "managed": managed,
        "saturated": saturated,
    }


# --- _work: the acp-push worker -------------------------------------------------


def _terminate(client) -> None:
    if client is None:
        return
    try:
        client.terminate()
    except Exception:
        pass


def _renew_loop(task_id: int, owner: str, args, done: threading.Event) -> None:
    """Keep the lease fresh while a (possibly long) prompt runs."""
    while not done.wait(LEASE_RENEW_INTERVAL):
        try:
            db.renew_lease(task_id, owner, path=args.db)
        except Exception:
            pass  # a lost CAS just means we no longer own the lease


def _acp_failure(
    task_id: int, owner: str, sid: str, exc: Exception, args, phase: str
) -> JsonResponse:
    """By-cause failure handling for the worker.

    Retryable causes leave the task ``running`` with its lease: expiry
    requeues-or-fails it with attempt accounting. Everything else -
    including a prompt timeout - finishes the task ``failed`` and
    notifies.
    """
    kind = getattr(exc, "kind", None) or type(exc).__name__
    note = f"acp {phase} failed ({kind}): {exc}"
    if bool(getattr(exc, "retryable", False)) and not (phase == "prompt" and kind == "timeout"):
        # the lease is deliberately kept: its expiry is the requeue
        # trigger in recover_stale
        return JsonResponse(
            session=sid,
            status=TaskStatus.RUNNING.value,
            error=note[:_NOTE_TAIL],
            agent_instruction=(
                "transient acp failure; lease expiry will requeue the task - do not retry manually"
            ),
        )
    finished = db.finish_task(task_id, owner, ok=False, note=note[:_NOTE_TAIL], path=args.db)
    db.release_lease(task_id, owner, path=args.db)
    events = []
    if finished:
        events.append(
            Event(
                kind=EventKind.FAILED,
                session_id=sid,
                task_id=task_id,
                detail=note[:200],
                ts=int(time.time()),
            )
        )
        row = db.get_task(task_id, path=args.db)
        _notify(args, row or {"id": task_id, "session_id": sid}, "FAILED", note[:200])
    return JsonResponse(
        session=sid,
        status=TaskStatus.FAILED.value,
        error=note[:_NOTE_TAIL],
        events=events,
    )


def _work_task(task_id: int, owner: str | None, args) -> JsonResponse:
    """One acp push cycle for a claimed task."""
    task = db.get_task(task_id, path=args.db)
    if task is None:
        return JsonResponse(status="not_found", error=f"task {task_id} not found")
    sid = task["session_id"]
    if owner is None:
        # standalone `bridge work` (no pump-spawned owner): attribute
        # the lease to the executing session like the pump paths do
        owner = f"{ACP_OWNER_PREFIX}{sid}-{os.getpid()}-{task_id}"
    if task["status"] == TaskStatus.DISPATCH_DECIDED.value:
        db.mark_running(task_id, owner, path=args.db)  # standalone run
    elif task["status"] != TaskStatus.RUNNING.value:
        return JsonResponse(
            session=sid,
            status=task["status"],
            agent_instruction=("task is not owned by this worker; do not retry"),
        )
    # ownership re-confirm immediately before any transport side
    # effect: a single CAS statement proves status='running' AND
    # lease_owner=me and pushes the expiry out. A task reclaimed by
    # recover_stale (or moved by any other transition) loses the CAS
    # and the worker backs off instead of double-prompting.
    task = db.get_task(task_id, path=args.db)
    if task is None or not db.renew_lease(task_id, owner, path=args.db):
        return JsonResponse(
            session=sid,
            status=(task["status"] if task is not None else "not_found"),
            agent_instruction=("task is not owned by this worker; do not retry"),
        )

    cwd = _session_cwd(sid, args)
    if cwd is None:
        note = "session not found in devin sessions.db (no cwd)"
        finished = db.finish_task(task_id, owner, ok=False, note=note, path=args.db)
        db.release_lease(task_id, owner, path=args.db)
        events = []
        if finished:
            events.append(
                Event(
                    kind=EventKind.FAILED,
                    session_id=sid,
                    task_id=task_id,
                    detail=note,
                    ts=int(time.time()),
                )
            )
            _notify(
                args,
                db.get_task(task_id, path=args.db) or {"id": task_id, "session_id": sid},
                "FAILED",
                note,
            )
        return JsonResponse(
            session=sid,
            status=TaskStatus.FAILED.value,
            error=f"not_found: {note}",
            events=events,
        )

    client = None
    try:
        client = AcpClient()
        client.load_session(sid, cwd)
    except AcpError as e:
        _terminate(client)
        if e.is_session_locked:
            # the session is open in devin after all: fold back to the
            # hook queue atomically and wake the parked Stop hook
            if _task_to_hook_queue(task_id, owner, args):
                _write_trigger(_queue_dir(args), sid)
            return JsonResponse(
                session=sid,
                status=TaskStatus.QUEUED.value,
                agent_instruction=(
                    "do not retry: session is open in devin; task queued for hook delivery"
                ),
                data={"transport": Transport.HOOK_QUEUE.value},
            )
        return _acp_failure(task_id, owner, sid, e, args, "load")
    except Exception as e:  # credentials, spawn OSError, ...
        _terminate(client)
        return _acp_failure(task_id, owner, sid, e, args, "start")

    text = task["task_text"] or f"execute spec file {task['task_file']}"
    done = threading.Event()
    renewer = threading.Thread(
        target=_renew_loop,
        args=(task_id, owner, args, done),
        daemon=True,
    )
    renewer.start()
    try:
        result = client.prompt(sid, text, timeout=_work_timeout(args))
    except AcpError as e:
        return _acp_failure(task_id, owner, sid, e, args, "prompt")
    except Exception as e:
        return _acp_failure(task_id, owner, sid, e, args, "prompt")
    finally:
        done.set()
        try:
            client.close_session(sid)
        except Exception:
            pass
        _terminate(client)

    ok = result.stop_reason == _END_TURN
    tail = result.text[-_ASSISTANT_TAIL:] if result.text else None
    note = None if ok else f"stopReason={result.stop_reason}"
    finished = db.finish_task(
        task_id,
        owner,
        ok=ok,
        last_assistant_message=tail,
        note=note,
        path=args.db,
    )
    db.release_lease(task_id, owner, path=args.db)
    events = []
    if finished and not ok:
        detail = f"agent turn ended with stopReason={result.stop_reason}"
        events.append(
            Event(
                kind=EventKind.FAILED,
                session_id=sid,
                task_id=task_id,
                detail=detail,
                ts=int(time.time()),
            )
        )
        _notify(
            args,
            db.get_task(task_id, path=args.db) or {"id": task_id, "session_id": sid},
            "FAILED",
            detail,
        )
    return JsonResponse(
        session=sid,
        status=TaskStatus.DONE.value if ok else TaskStatus.FAILED.value,
        events=events,
    )


def _cmd_work(args) -> int:
    resp = _work_task(args.task_id, args.owner, args)
    try:
        # a worker exit frees an acp slot: dispatch whatever is queued
        result = pump(args)
        resp.events.extend(result["events"])
    except Exception as e:
        _log(f"post-work pump failed: {e!r}")
    return _emit(args, resp, ExitCode.OK)


# --- operator commands ----------------------------------------------------------


def _emit(args, resp: JsonResponse, code: ExitCode, human: str | None = None) -> int:
    if getattr(args, "json", False):
        sys.stdout.write(resp.model_dump_json() + "\n")
        sys.stdout.flush()
    elif human is not None:
        sys.stdout.write(human + "\n")
    elif resp.error:
        sys.stderr.write(f"error: {resp.error}\n")
    return int(code)


def _cmd_push(args) -> int:
    target = db.get_session(args.session_id, path=args.db)
    if target is not None and target["status"] == SessionStatus.DEAD.value:
        # a dead sid is undrainable by every transport - queueing
        # would drop the task into a void (the devin-telegram v2.2.6
        # incident: task sat 2200+s invisible as "idle")
        resp = JsonResponse(
            session=args.session_id,
            status="dead",
            queue_depth=_queue_depth(args),
            error=f"session {args.session_id} is dead - queued work would never drain",
            agent_instruction="do not retry: re-register the session first",
        )
        return _emit(
            args,
            resp,
            ExitCode.NOT_FOUND,
            f"refused: session {args.session_id} is dead",
        )
    unmanaged = target is None or target["mode"] != SessionMode.MANAGED.value
    tid = db.enqueue(
        args.session_id,
        task_text=args.task,
        task_file=args.task_file,
        path=args.db,
    )
    if tid is None:
        live = db.get_live_task(args.session_id, path=args.db)
        resp = JsonResponse(
            session=args.session_id,
            status=live["status"] if live is not None else "conflict",
            queue_depth=_queue_depth(args),
            error="session already has a live task",
            agent_instruction=("do not retry: a live task already exists for this session"),
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: session {args.session_id} already has a live task",
        )
    if unmanaged:
        # warn + flag (item_152): unmanaged sids have no drain
        # contract - hook/acp transports may still deliver, but the
        # caller must see that nothing guarantees it. Flag the queue
        # row so `bridge inspect` surfaces the risk for as long as
        # the task sits.
        db.set_task_note(
            tid,
            "push target is not a managed session - delivery not guaranteed",
            path=args.db,
        )
    result = pump(args)
    task = db.get_task(tid, path=args.db)
    if task is None:
        # a concurrent `bridge clear` deleted the fresh task mid-pump
        resp = JsonResponse(
            session=args.session_id,
            status="not_found",
            queue_depth=_queue_depth(args),
            error=f"task {tid} was cleared during dispatch",
        )
        return _emit(
            args,
            resp,
            ExitCode.NOT_FOUND,
            f"not found: task {tid} was cleared during dispatch",
        )
    events = [e for e in result["events"] if e.session_id == args.session_id]
    instruction = None
    error = None
    code = ExitCode.OK
    if args.session_id in result["unknown"]:
        code = ExitCode.NOT_FOUND
        error = (
            f"not_found: session {args.session_id} is unknown (not in"
            " devin sessions.db, not locked, not seen by hooks)"
        )
        instruction = (
            "task is preserved in the queue; verify the session id or open the session once"
        )
    elif tid in result["managed"]:
        instruction = (
            "do not retry: managed session - task dispatched via the"
            " notifications outbox (mailbox delivery)"
        )
    elif task["transport"] == Transport.HOOK_QUEUE.value:
        instruction = "do not retry: task queued for hook delivery inside the open session"
    elif task["status"] == TaskStatus.RUNNING.value:
        instruction = "do not retry: acp worker dispatched"
    warning = None
    if unmanaged:
        warning = (
            f"session {args.session_id} is not a managed session - no drain"
            " contract; the task may sit undelivered"
        )
        instruction = warning if instruction is None else f"{instruction}; warning: {warning}"
    resp = JsonResponse(
        session=args.session_id,
        status=task["status"],
        queue_depth=_queue_depth(args),
        events=events,
        agent_instruction=instruction,
        error=error,
        data={"task_id": tid, "transport": task["transport"], "warning": warning},
    )
    human = f"task {tid} {task['status']} for {args.session_id}"
    if task["transport"]:
        human += f" via {task['transport']}"
    if error:
        human += f" ({error})"
    if warning:
        human += " [UNMANAGED TARGET]"
    return _emit(args, resp, code, human)


def _all_session_rows(args):
    con = db.connect(args.db)
    try:
        return con.execute("SELECT * FROM sessions ORDER BY session_id").fetchall()
    finally:
        con.close()


def _session_views(args) -> tuple[list[dict], list[Event], int]:
    """Fold state.db sessions + tasks into per-session status views.

    Returns (views, events, queue_shown) - queue_shown is how many
    queued tasks the capped listing actually folded into views, so the
    caller can flag truncation against the true COUNT(*).
    """
    tasks = db.list_tasks(limit=1000, path=args.db)
    queue_shown = sum(1 for t in tasks if t["status"] == TaskStatus.QUEUED.value)
    by_session: dict[str, list] = {}
    for t in tasks:  # newest first
        by_session.setdefault(t["session_id"], []).append(t)
    sids = {r["session_id"] for r in _all_session_rows(args)} | set(by_session)
    views = []
    events: list[Event] = []
    for sid in sorted(sids):
        session_tasks = by_session.get(sid, [])
        live = next(
            (t for t in session_tasks if t["status"] in _LIVE_VALUES),
            None,
        )
        last_terminal = next(
            (
                t
                for t in session_tasks
                if t["status"] in (TaskStatus.DONE.value, TaskStatus.FAILED.value)
            ),
            None,
        )
        srow = db.get_session(sid, path=args.db)
        paused = bool(srow["queue_paused"]) if srow is not None else False

        notify_task = None
        log_path = stderr_tail = None
        if live is not None and live["status"] == TaskStatus.RUNNING.value:
            state, action = STATE_RUNNING, "-"
        elif live is not None and live["status"] == TaskStatus.DISPATCH_DECIDED.value:
            # a claimed task mid-dispatch is visible, not a silent wedge
            state = STATE_DISPATCHING
            action = "dispatch in progress"
        elif live is not None and live["status"] == TaskStatus.AWAITING_USER.value:
            state = STATE_NEEDS_KICK
            action = "send a short prompt (<=3 chars) to kick"
            notify_task = (live, EventKind.NEEDS_KICK)
        elif live is not None and paused and live["status"] == TaskStatus.QUEUED.value:
            state = STATE_NEEDS_KICK
            action = "queue paused: kick or `bridge unpark`"
            notify_task = (live, EventKind.NEEDS_KICK)
        elif live is not None:
            state, action = STATE_PARKED, "waiting for dispatch"
        elif last_terminal is not None and last_terminal["status"] == TaskStatus.FAILED.value:
            state = STATE_FAILED
            action = f"bridge retry {sid}"
            notify_task = (last_terminal, EventKind.FAILED)
            log_path, stderr_tail = _worker_log_tail(args, last_terminal["id"])
        else:
            state, action = STATE_HEADLESS_IDLE, "push a task"

        if notify_task is not None:
            task_row, kind = notify_task
            events.append(
                Event(
                    kind=kind,
                    session_id=sid,
                    task_id=task_row["id"],
                    detail=task_row["note"],
                    ts=int(time.time()),
                )
            )
            _notify(
                args,
                task_row,
                kind.value,
                task_row["note"] or "needs attention",
            )

        repo = "-"
        if srow is not None:
            repo = srow["cwd"] or srow["title"] or "-"
        views.append(
            {
                "session": sid,
                "repo": repo,
                "transport": live["transport"] if live is not None else None,
                "state": state,
                "queue": 1 if live is not None and live["status"] == TaskStatus.QUEUED.value else 0,
                "action": action,
                "log_path": log_path,
                "stderr_tail": stderr_tail,
            }
        )
    return views, events, queue_shown


# --- fleet status view (item_13e2) ---------------------------------------------

#: hard ceiling on human ``status`` lines - the contract is <25.
STATUS_MAX_LINES = 24

#: open-escalation banner rows before the banner collapses to "+N more".
STATUS_BANNER_MAX = 5

#: rolling window of the footer digest counts.
STATUS_DIGEST_SECONDS = 24 * 3600

#: task statuses that never make a session row "idle" - the fleet row
#: resolves its task cell on the latest task regardless of status, but
#: the state column only inherits non-terminal ones (done -> idle is
#: misleading: the worker finished, it is idle carrying a done task).
_IDLE_TASK_STATUSES = frozenset({TaskStatus.DONE.value})

#: display aliases for statuses longer than the STATE column.
_FLEET_STATE_ALIAS = {
    TaskStatus.DISPATCH_DECIDED.value: "dispatch",
    TaskStatus.AWAITING_USER.value: "awaiting",
}

#: compact lifecycle cells for the TSTAT column (item_23): STATE
#: alone was ambiguous - a done task and a never-claimed queued one
#: both read "idle", so the orchestrator could not tell "worker idle
#: after finishing" from "task not picked up" without inspect.
_TASK_STATE_CELL = {
    TaskStatus.QUEUED.value: "queued",
    TaskStatus.DISPATCH_DECIDED.value: "disp",
    TaskStatus.RUNNING.value: "run",
    TaskStatus.AWAITING_USER.value: "wait",
    TaskStatus.DONE.value: "done",
    TaskStatus.FAILED.value: "fail",
    TaskStatus.PAUSED.value: "paus",
}


def _age(now: int, ts):
    """Whole seconds between ``ts`` and ``now``; None when unknown."""
    if ts is None:
        return None
    return max(0, now - int(ts))


def _fmt_age(age) -> str:
    return "-" if age is None else f"{age}s"


def _cut(text, width: int) -> str:
    """Fit ``text`` into ``width`` chars, "~" marking the truncation."""
    s = "-" if text is None else str(text)
    if len(s) <= width:
        return s
    return s[: max(1, width - 1)] + "~"


def _repo_name(row) -> str:
    """Compact repo cell: basename of the session cwd, else its title."""
    cwd = row["cwd"]
    if cwd:
        return os.path.basename(cwd.rstrip("/")) or cwd
    return row["title"] or "-"


def _fleet_state(session_status, task) -> str:
    """The one-word state cell for a fleet row.

    Dead sessions show ``dead`` (the compact marker - a dead row stays
    visible so the operator sees which sid to reopen); otherwise the
    row inherits its latest task status unless that task is done - a
    finished task means the session is simply idle.
    """
    if session_status == SessionStatus.DEAD.value:
        return "dead"
    if task is None or task["status"] in _IDLE_TASK_STATUSES:
        return "idle"
    return _FLEET_STATE_ALIAS.get(task["status"], task["status"])


def _status_snapshot(args) -> dict:
    """One read-only pass over state.db + the daemon state dir.

    Everything here is a read (pump already ran the recovery and
    dispatch side effects). The fleet row joins sessions to their
    latest task and the authoritative ``worker_heartbeats`` row;
    sessions that exist only through a pushed task (never registered)
    are folded in too - an unregistered sid is still fleet activity
    the operator must see.
    """
    now = int(time.time())
    cutoff = now - STATUS_DIGEST_SECONDS
    con = db.connect(args.db)
    try:
        srows = con.execute(
            "SELECT s.session_id AS sid, s.cwd AS cwd, s.title AS title,"
            " s.role AS role, s.mode AS mode, s.status AS status,"
            " wh.last_seen_at AS seen,"
            " ws.last_outcome AS wake_outcome,"
            " ws.unconfirmed_streak AS wake_streak,"
            " ws.escalated AS wake_escalated,"
            " ws.last_outcome_at AS wake_at,"
            " (SELECT MAX(n.ack_at) FROM notifications n"
            "   WHERE n.session_id=s.session_id) AS last_ack,"
            " (SELECT MIN(n.created_at) FROM notifications n"
            "   WHERE n.session_id=s.session_id"
            "     AND n.relayed_at IS NOT NULL AND n.ack_at IS NULL"
            "     AND n.kind<>?) AS mail_oldest"
            " FROM sessions s"
            " LEFT JOIN worker_heartbeats wh"
            "   ON wh.session_id=s.session_id"
            " LEFT JOIN wake_state ws"
            "   ON ws.session_id=s.session_id"
            " ORDER BY s.session_id",
            (NotificationKind.HEARTBEAT_TICK.value,),
        ).fetchall()
        latest = {
            r["session_id"]: r
            for r in con.execute(
                "SELECT t.* FROM tasks t"
                " JOIN (SELECT MAX(id) AS id FROM tasks GROUP BY session_id) m"
                "  ON m.id=t.id"
            ).fetchall()
        }
        qcounts = {
            r["task_id"]: int(r["c"])
            for r in con.execute(
                "SELECT task_id, COUNT(*) AS c FROM questions GROUP BY task_id"
            ).fetchall()
        }
        escalations = con.execute(
            "SELECT id, session_id, task_id, doa_trigger, question_text"
            " FROM audit_events WHERE kind=? AND directive IS NULL ORDER BY id",
            (AuditEventKind.ESCALATION.value,),
        ).fetchall()
        digest = {
            "done": int(
                con.execute(
                    "SELECT COUNT(*) AS c FROM tasks WHERE status=? AND updated_at>=?",
                    (TaskStatus.DONE.value, cutoff),
                ).fetchone()["c"]
            ),
            "auto_decisions": int(
                con.execute(
                    "SELECT COUNT(*) AS c FROM audit_events WHERE kind=? AND created_at>=?",
                    (AuditEventKind.DELIBERATION.value, cutoff),
                ).fetchone()["c"]
            ),
            "escalations": int(
                con.execute(
                    "SELECT COUNT(*) AS c FROM audit_events WHERE kind=? AND created_at>=?",
                    (AuditEventKind.ESCALATION.value, cutoff),
                ).fetchone()["c"]
            ),
        }
    finally:
        con.close()

    fleet = []
    orchestrator = None
    for r in srows:
        t = latest.pop(r["sid"], None)
        # managed liveness (item_21): turn-activity recency - the
        # freshest of heartbeat, last ack and task update. Stale hb
        # between drain-mode turns is by design, so heartbeat alone
        # would report a working managed session dead. sessions.updated_at
        # is deliberately excluded: daemon bookkeeping (arm, park,
        # dead-mark) writes it too and would fake a fresh SEEN right
        # after dead-marking (item_21 r2).
        act_sources = [r["seen"], r["last_ack"]]
        if t is not None:
            act_sources.append(t["updated_at"])
        activity = max((v for v in act_sources if v is not None), default=None)
        row = {
            "session_id": r["sid"],
            "registered": True,
            "role": r["role"],
            "mode": r["mode"],
            "repo": _repo_name(r),
            "session_status": r["status"],
            "task_id": t["id"] if t is not None else None,
            "task_label": (t["task_text"] or t["task_file"] or "") if t is not None else None,
            "task_status": t["status"] if t is not None else None,
            "attempt": t["attempt"] if t is not None else None,
            "questions": qcounts.get(t["id"], 0) if t is not None else None,
            "heartbeat_age_seconds": _age(now, r["seen"]),
            "activity_age_seconds": _age(now, activity),
            "wake_outcome": r["wake_outcome"],
            "wake_age_seconds": _age(now, r["wake_at"]),
            "wake_streak": int(r["wake_streak"] or 0),
            "wake_escalated": bool(r["wake_escalated"]),
            "mail_age_seconds": _age(now, r["mail_oldest"]),
        }
        row["state"] = _fleet_state(r["status"], t)
        fleet.append(row)
        if r["role"] == SessionRole.ORCHESTRATOR.value:
            # the register CAS keeps at most one orchestrator holder
            orchestrator = row
    for sid, t in sorted(latest.items()):
        row = {
            "session_id": sid,
            "registered": False,
            "role": None,
            "mode": None,
            "repo": "-",
            "session_status": None,
            "task_id": t["id"],
            "task_label": t["task_text"] or t["task_file"] or "",
            "task_status": t["status"],
            "attempt": t["attempt"],
            "questions": qcounts.get(t["id"], 0),
            "heartbeat_age_seconds": None,
            "activity_age_seconds": None,
            "wake_outcome": None,
            "wake_age_seconds": None,
            "wake_streak": 0,
            "wake_escalated": False,
            "mail_age_seconds": None,
            "state": _fleet_state(None, t),
        }
        fleet.append(row)
    fleet.sort(
        key=lambda r: (
            0 if r["role"] == SessionRole.ORCHESTRATOR.value else 1 if r["registered"] else 2,
            r["session_id"],
        )
    )

    attention = [
        {
            "kind": "escalation",
            "audit_id": r["id"],
            "session_id": r["session_id"],
            "task_id": r["task_id"],
            "doa_trigger": r["doa_trigger"],
            "reply_command": (f"bridge escalate reply {r['id']} A|B|defer|abort|override:<text>"),
        }
        for r in escalations
    ]
    # the orchestrator counts as dead when the supervisor marked it or
    # (unmanaged only) its heartbeat already slipped past the bounded
    # stale window - the banner must not wait for the next daemon pass
    # to fire; a MANAGED orchestrator idle between turns is stale-hb by
    # design (the wake trigger), so only a real dead flip counts - same
    # rule the sweep applies since item_17
    if orchestrator is not None and (
        orchestrator["session_status"] == SessionStatus.DEAD.value
        or (
            orchestrator["mode"] != SessionMode.MANAGED.value
            and (
                orchestrator["heartbeat_age_seconds"] is None
                or orchestrator["heartbeat_age_seconds"] >= db.HEARTBEAT_STALE_SECONDS
            )
        )
    ):
        attention.append(
            {
                "kind": "orchestrator_dead",
                "session_id": orchestrator["session_id"],
                "hint": ("reopen session, bridge register <new-sid> --role orchestrator [--force]"),
            }
        )

    sdir = _state_dir(args)
    return {
        "attention": attention,
        "vitals": {
            "daemon": {
                "running": daemon.daemon_running(sdir),
                "pid": daemon.daemon_pid(sdir),
                "heartbeat_age_seconds": daemon.heartbeat_age(sdir),
                "state_dir": sdir,
            },
            "orchestrator": {
                "session_id": orchestrator["session_id"] if orchestrator else None,
                "status": orchestrator["session_status"] if orchestrator else None,
                "mode": orchestrator["mode"] if orchestrator else None,
                "heartbeat_age_seconds": (
                    orchestrator["heartbeat_age_seconds"] if orchestrator else None
                ),
                "activity_age_seconds": (
                    orchestrator["activity_age_seconds"] if orchestrator else None
                ),
            },
        },
        "fleet": fleet,
        "digest": digest,
    }


def _attention_lines(snap) -> list[str]:
    lines = []
    esc = [a for a in snap["attention"] if a["kind"] == "escalation"]
    for a in esc[:STATUS_BANNER_MAX]:
        lines.append(
            f"ESCALATED #{a['audit_id']}: {a['doa_trigger'] or 'escalation'}"
            f" - reply: {a['reply_command']}"
        )
    if len(esc) > STATUS_BANNER_MAX:
        hidden = len(esc) - STATUS_BANNER_MAX
        lines.append(f"+{hidden} more open escalation(s) - bridge escalate show")
    if any(a["kind"] == "orchestrator_dead" for a in snap["attention"]):
        lines.append(
            "ORCHESTRATOR DEAD: reopen session, bridge register <new-sid>"
            " --role orchestrator [--force]"
        )
    return lines


def _fleet_seen_age(r) -> int | None:
    """The liveness cell: activity recency for managed, hb for unmanaged.

    A managed drain-mode session is stale-hb between turns by design -
    its turn-activity recency is the honest liveness metric (item_21).
    """
    if r["mode"] == SessionMode.MANAGED.value:
        return r["activity_age_seconds"]
    return r["heartbeat_age_seconds"]


def _wake_cell(r) -> str:
    """Compact last-wake cell: outcome, streak count, escalation mark."""
    outcome = r.get("wake_outcome")
    if not outcome:
        return "-"
    if outcome.startswith("failed:"):
        cell = "f:" + outcome[7:12]
    else:
        cell = {
            "sent": "sent",
            "busy": "busy",
            "verified": "verif",
            "sent_unconfirmed": "unc",
        }.get(outcome, outcome[:5])
    if r.get("wake_streak"):
        cell += f"x{r['wake_streak']}"
    if r.get("wake_escalated"):
        cell += "!"
    return _cut(cell, 7)


def _task_state_cell(r) -> str:
    """Compact lifecycle of the latest task for TSTAT (item_23)."""
    st = r.get("task_status")
    if not st:
        return "-"
    return _TASK_STATE_CELL.get(st, _cut(st, 6))


def _fleet_row_line(r) -> str:
    # a leading "*" marks a session that never registered on the bus
    # (seen only through a pushed task)
    sid = r["session_id"] if r["registered"] else f"*{r['session_id']}"
    if r["task_id"] is not None:
        task = f"#{r['task_id']} {r['task_label'] or ''}".strip()
    else:
        task = "-"
    q = "-" if r["questions"] is None else f"{r['questions']}/{MAX_QUESTIONS_PER_TASK}"
    fail = "-" if r["attempt"] is None else f"{r['attempt']}/{MAX_ATTEMPTS}"
    return (
        f"{_cut(sid, 16):<16} {_cut(r['repo'], 16):<16} {_cut(task, 20):<20}"
        f" {_task_state_cell(r):<6} {_cut(r['state'], 8):<8} {q:>4} {fail:>4}"
        f" {_fmt_age(_fleet_seen_age(r)):>5} {_wake_cell(r):<7}"
        f" {_fmt_age(r['mail_age_seconds']):>5}"
    )


def _render_status(snap) -> str:
    """Human status view, strictly below 25 lines.

    Layout: attention banner (open escalations with their exact reply
    command, then a dead orchestrator), daemon + orchestrator vitals,
    one-row-per-session fleet table, and the 24h digest footer. The
    line budget is enforced by construction - capped banner, two vital
    lines, header plus rows bounded by what is left, a "+N more" fold
    for the overflow - and by a final defensive slice.
    """
    lines = _attention_lines(snap)
    d = snap["vitals"]["daemon"]
    if d["running"]:
        pid = d["pid"] if d["pid"] is not None else "-"
        lines.append(f"daemon: running (pid {pid}, hb {_fmt_age(d['heartbeat_age_seconds'])})")
    else:
        lines.append("daemon: stopped")
    o = snap["vitals"]["orchestrator"]
    if o["session_id"] is None:
        lines.append("orchestrator: none")
    else:
        # managed orchestrators idle between drain turns are stale-hb
        # by design - report turn-activity recency like the fleet's
        # SEEN column does (item_21 review)
        if o.get("mode") == SessionMode.MANAGED.value:
            seen, label = o["activity_age_seconds"], "seen"
        else:
            seen, label = o["heartbeat_age_seconds"], "hb"
        lines.append(f"orchestrator: {o['session_id']} {o['status']} ({label} {_fmt_age(seen)})")
    fleet = snap["fleet"]
    if not fleet:
        lines.append("(no active sessions)")
    else:
        lines.append(
            f"{'SID':<16} {'REPO':<16} {'TASK':<20} {'TSTAT':<6} {'STATE':<8}"
            f" {'Q':>4} {'FAIL':>4} {'SEEN':>5} {'WAKE':<7} {'MAIL':>5}"
        )
        # rows get what the budget leaves after banner + vitals +
        # header + the digest footer
        avail = STATUS_MAX_LINES - len(lines) - 1
        shown = fleet[:avail]
        if len(fleet) > avail:
            shown = fleet[: max(1, avail - 1)]
            hidden = len(fleet) - len(shown)
            lines.extend(_fleet_row_line(r) for r in shown)
            lines.append(f"+{hidden} more session(s) - see --json")
        else:
            lines.extend(_fleet_row_line(r) for r in shown)
    g = snap["digest"]
    lines.append(
        f"24h: done {g['done']} | auto-decisions {g['auto_decisions']}"
        f" | escalations {g['escalations']}"
    )
    return "\n".join(lines[:STATUS_MAX_LINES])


def _cmd_status(args) -> int:
    result = pump(args)
    views, events, queue_shown = _session_views(args)
    seen = {(e.kind, e.task_id) for e in result["events"]}
    events = result["events"] + [e for e in events if (e.kind, e.task_id) not in seen]
    queue_depth = _queue_depth(args)
    snap = _status_snapshot(args)
    data = {
        "sessions": views,
        # queue_depth is the true COUNT(*); queue_shown is what the
        # capped session listing folded in - truncation is explicit
        "queue_shown": queue_shown,
        "queue_truncated": queue_depth > queue_shown,
        "in_flight_dispatch": db.count_tasks(status=TaskStatus.DISPATCH_DECIDED, path=args.db),
        "concurrency": {
            "active": db.count_tasks(
                status=TaskStatus.RUNNING,
                transport=Transport.ACP_PUSH,
                path=args.db,
            ),
            "max": SEMAPHOR,
        },
        # item_13e2 fleet view: the human render below reads only
        # these keys; --json carries the uncapped structures
        "attention": snap["attention"],
        "vitals": snap["vitals"],
        "fleet": snap["fleet"],
        "digest_24h": snap["digest"],
    }
    if result["saturated"]:
        data["saturation_reason"] = "MAX_CONCURRENT_REACHED"
    resp = JsonResponse(
        status="ok",
        queue_depth=queue_depth,
        events=events,
        data=data,
    )
    return _emit(args, resp, ExitCode.OK, _render_status(snap))


def _resolve_ref(ref: str, args):
    """Resolve a task id or session id to a task row.

    A session ref yields its live task first, else the most recent
    task recorded for the session.
    """
    if ref.isdigit():
        task = db.get_task(int(ref), path=args.db)
        if task is not None:
            return task
    live = db.get_live_task(ref, path=args.db)
    if live is not None:
        return live
    for t in db.list_tasks(limit=1000, path=args.db):
        if t["session_id"] == ref:
            return t
    return None


def _next_action(task) -> str:
    st = task["status"]
    if st == TaskStatus.QUEUED.value:
        return "waiting for dispatch (semaphore or hook trigger)"
    if st == TaskStatus.DISPATCH_DECIDED.value:
        return "dispatch in progress"
    if st == TaskStatus.RUNNING.value:
        return f"in flight via {task['transport'] or 'unknown'}"
    if st == TaskStatus.AWAITING_USER.value:
        return "kick the session with a short prompt (<=3 chars)"
    if st == TaskStatus.PAUSED.value:
        return f"bridge unpark {task['session_id']}"
    if st == TaskStatus.FAILED.value:
        return f"bridge retry {task['session_id']}"
    return "completed"


def _cmd_inspect(args) -> int:
    task = _resolve_ref(args.ref, args)
    if task is None:
        resp = JsonResponse(
            session=args.ref,
            status="not_found",
            error=f"no task found for {args.ref}",
        )
        return _emit(args, resp, ExitCode.NOT_FOUND, f"not found: {args.ref}")
    now = int(time.time())
    if task["status"] in _LIVE_VALUES:
        wait = now - task["created_at"]
    else:
        wait = task["updated_at"] - task["created_at"]
    next_action = _next_action(task)
    # derived guard surface (item_152): the push-time note flag can be
    # overwritten by later lifecycle writers, so inspect re-derives the
    # unmanaged warning live from the session row instead of trusting
    # the stored note
    target = db.get_session(task["session_id"], path=args.db)
    unmanaged = target is None or target["mode"] != SessionMode.MANAGED.value
    data = {
        "task_id": task["id"],
        "session_id": task["session_id"],
        "status": task["status"],
        "transport": task["transport"],
        "attempt": task["attempt"],
        "wait_seconds": wait,
        "task_text": task["task_text"],
        "task_file": task["task_file"],
        "note": task["note"],
        "last_assistant_message": task["last_assistant_message"],
        "next_action": next_action,
        "unmanaged_target": unmanaged,
    }
    resp = JsonResponse(
        session=task["session_id"],
        status=task["status"],
        queue_depth=_queue_depth(args),
        data=data,
    )
    human = (
        f"task {task['id']} (session {task['session_id']})\n"
        f"  status:    {task['status']}\n"
        f"  transport: {task['transport'] or '-'}\n"
        f"  attempt:   {task['attempt']}\n"
        f"  waited:    {wait}s\n"
        f"  task:      {task['task_text'] or task['task_file']}\n"
        f"  reason:    {task['note'] or '-'}\n"
        + (
            "  warning:   target is not a managed session - delivery not guaranteed\n"
            if unmanaged
            else ""
        )
        + f"  next:      {next_action}"
    )
    return _emit(args, resp, ExitCode.OK, human)


def _cmd_clear(args) -> int:
    task = db.get_live_task(args.session, path=args.db)
    if task is None:
        resp = JsonResponse(
            session=args.session,
            status="not_found",
            error=f"no live task for session {args.session}",
        )
        return _emit(
            args,
            resp,
            ExitCode.NOT_FOUND,
            f"not found: no live task for {args.session}",
        )
    if task["status"] != TaskStatus.QUEUED.value:
        resp = JsonResponse(
            session=args.session,
            status=task["status"],
            error=(f"task {task['id']} is {task['status']}; only queued tasks can be cleared"),
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: task {task['id']} is {task['status']}, not queued",
        )
    if not db.clear(task["id"], path=args.db):
        # the task left `queued` between our check and the delete -
        # e.g. claim_queued already dispatched it
        row = db.get_task(task["id"], path=args.db)
        status = row["status"] if row is not None else "gone"
        resp = JsonResponse(
            session=args.session,
            status=status,
            queue_depth=_queue_depth(args),
            error=f"task {task['id']} is {status}; not cleared",
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: task {task['id']} is {status}, not cleared",
        )
    pump(args)
    resp = JsonResponse(
        session=args.session,
        status="cleared",
        queue_depth=_queue_depth(args),
    )
    return _emit(args, resp, ExitCode.OK, f"cleared queued task {task['id']}")


def _cmd_unpark(args) -> int:
    sid = args.session
    db.session_pause(sid, False, path=args.db)
    candidates = []
    live = db.get_live_task(sid, path=args.db)
    if live is not None and live["status"] == TaskStatus.AWAITING_USER.value:
        candidates.append(live)
    for t in db.list_tasks(status=TaskStatus.PAUSED, path=args.db):
        if t["session_id"] == sid:
            candidates.append(t)
    for t in candidates:
        if db.unpark(t["id"], path=args.db):
            row = db.get_task(t["id"], path=args.db)
            if row["transport"] == Transport.HOOK_QUEUE.value:
                _write_trigger(_queue_dir(args), sid)
    pump(args)
    task = db.get_live_task(sid, path=args.db)
    status = task["status"] if task is not None else "idle"
    resp = JsonResponse(
        session=sid,
        status=status,
        queue_depth=_queue_depth(args),
        data={"unparked": len(candidates)},
    )
    return _emit(args, resp, ExitCode.OK, f"session {sid}: {status}")


def _cmd_retry(args) -> int:
    task = _resolve_ref(args.ref, args)
    if task is None:
        resp = JsonResponse(
            session=args.ref,
            status="not_found",
            error=f"no task found for {args.ref}",
        )
        return _emit(args, resp, ExitCode.NOT_FOUND, f"not found: {args.ref}")
    if task["status"] != TaskStatus.FAILED.value:
        resp = JsonResponse(
            session=task["session_id"],
            status=task["status"],
            error=(f"task {task['id']} is {task['status']}; retry only applies to failed tasks"),
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: task {task['id']} is {task['status']}, not failed",
        )
    if not db.retry(task["id"], path=args.db):
        resp = JsonResponse(
            session=task["session_id"],
            status=task["status"],
            error=f"retry of task {task['id']} lost a race",
        )
        return _emit(args, resp, ExitCode.CONFLICT, "conflict: retry lost a race")
    pump(args)
    row = db.get_task(task["id"], path=args.db)
    resp = JsonResponse(
        session=task["session_id"],
        status=row["status"],
        queue_depth=_queue_depth(args),
        agent_instruction="do not retry: task requeued",
        data={"task_id": task["id"]},
    )
    return _emit(args, resp, ExitCode.OK, f"task {task['id']} requeued")


# --- managed mode: arm / disarm ----------------------------------------------------

_DISARM_BODY = "managed mode disarmed: stop re-arming the mailbox watcher"


def _cmd_arm(args) -> int:
    """``bridge arm <sid>``: enroll a session into managed mode."""
    db.set_session_mode(args.session_id, SessionMode.MANAGED, path=args.db)
    resp = JsonResponse(
        session=args.session_id,
        status=SessionMode.MANAGED.value,
        queue_depth=_queue_depth(args),
        agent_instruction=(
            "session armed: deliveries go through the mailbox watcher"
            " (SessionStart/kick injects the mailbox duty)"
        ),
    )
    return _emit(args, resp, ExitCode.OK, f"session {args.session_id} armed (managed)")


def _cmd_disarm(args) -> int:
    """``bridge disarm <sid>|--all``: leave managed mode + DISARM notify.

    The durable outbox DISARM row is what a live watcher receives once
    the daemon relays it - mode is cleared first so a restarted or
    re-arming session sees unmanaged immediately.
    """
    if args.all:
        sids = [
            r["session_id"]
            for r in _all_session_rows(args)
            if r["mode"] == SessionMode.MANAGED.value
        ]
    elif args.session_id:
        sids = [args.session_id]
    else:
        resp = JsonResponse(
            status="usage",
            error="disarm requires a session id or --all",
        )
        return _emit(
            args,
            resp,
            ExitCode.USAGE,
            "usage: bridge disarm <session_id> | bridge disarm --all",
        )
    for sid in sids:
        db.set_session_mode(sid, SessionMode.UNMANAGED, path=args.db)
        # leaving managed mode drops the managed-only halt flag too -
        # a disarmed session no longer owes drain duty, and a stray
        # halted_at would still refuse its claims after a re-arm
        db.set_session_halted(sid, False, path=args.db)
        db.notify(sid, NotificationKind.DISARM, _DISARM_BODY, path=args.db)
    resp = JsonResponse(
        session=None if args.all else args.session_id,
        status=SessionMode.UNMANAGED.value,
        queue_depth=_queue_depth(args),
        data={"disarmed": sids},
    )
    if args.all:
        human = f"disarmed {len(sids)} managed session(s)"
        if sids:
            human += f": {', '.join(sids)}"
    else:
        human = f"session {args.session_id} disarmed (unmanaged)"
    return _emit(args, resp, ExitCode.OK, human)


_HALT_BODY = (
    "HALT: finish your current atomic action only, save state"
    " (commit/push WIP, notes in the task file), then end your turn."
    " Do not claim new queued tasks - they persist and resume later."
)
_RESUME_BODY = "RESUME: the halt is lifted - normal mailbox duty resumes"


def _tombstone_path(sid: str, mdir: str) -> str:
    """Park name for a mailbox being unregistered - unique per caller."""
    return os.path.join(
        os.path.dirname(mdir), f".unregistered-{sid}-{int(time.time())}-{os.getpid()}"
    )


def _merge_dir_no_clobber(src: str, dst: str) -> None:
    """Move src's files into dst; a live same-name file always wins.

    Mailbox names are msg_<notification_id>.txt, so a same-name file
    in a recreated dir is almost always the same notification (the
    consumer dedups by id anyway) - the post-delete write owns the
    slot and the parked duplicate is dropped with the park.
    """
    for name in os.listdir(src):
        if os.path.exists(os.path.join(dst, name)):
            continue
        os.replace(os.path.join(src, name), os.path.join(dst, name))


def _reap_unregister_tombstones(sid: str, mdir: str) -> None:
    """Merge leftover ``.unregistered-<sid>-*`` parks back into ``<sid>``.

    A crash between the tombstone rename and the db commit (or a
    failed restore) strands the mailbox under a name no writer
    targets - the registration lives but its mail is parked forever.
    The reaper runs where a live registration meets its mailbox
    again: top of ``bridge unregister`` and after a successful
    ``bridge register``. Best-effort: a sick fs must not block the
    command itself.
    """
    root = os.path.dirname(mdir)
    try:
        parked = [
            name
            for name in os.listdir(root)
            if name.startswith(f".unregistered-{sid}-") and os.path.isdir(os.path.join(root, name))
        ]
    except OSError:
        return
    for name in parked:
        park = os.path.join(root, name)
        try:
            os.makedirs(mdir, exist_ok=True)
            _merge_dir_no_clobber(park, mdir)
            shutil.rmtree(park, ignore_errors=True)
        except OSError:
            continue


def _cmd_unregister(args) -> int:
    """``bridge unregister <sid> [--force]``: remove a dead registration (task #198).

    Ghost registrations - a sid that registered once and vanished -
    keep an active sessions row, stale outbox rows and an undrained
    mailbox that haunts every fleet status. Removal is guarded by
    three refusals:

    - a live task still owns the lifecycle: fail/requeue it first;
      deleting it mid-flight is a kill, not a cleanup. Enforced
      inside the deletion transaction - no --force override.
    - a fresh heartbeat means a live client is on the bus right now
      (authoritative for unmanaged sessions); also enforced inside
      the transaction so a heartbeat stamped between the legs and
      the delete cannot be lost to a TOCTOU gap.
    - the acp-host still lists the session: ANY listed entry -
      including a statusless one - is a resumable Devin session and
      therefore alive evidence no stale row can refute. The host
      check is best-effort and cannot live in the write txn
      (network I/O): a missing lockfile or failed attach proves
      nothing either way, so it does not refuse (``host_check`` in
      the output records which leg ran); ``--force`` skips this leg
      and the heartbeat leg for the case the operator knows better.

    Deletion covers every session-scoped row plus the mailbox
    directory (delivery artifacts, not history); ``audit_events``
    and the decision journal keep the audit trail.
    """
    sid = args.session_id
    srow = db.get_session(sid, path=args.db)
    if srow is None:
        resp = JsonResponse(
            session=sid,
            status="not_found",
            error=f"no registration for {sid}",
        )
        return _emit(args, resp, ExitCode.NOT_FOUND, f"not registered: {sid}")

    host_check = "force"
    if not args.force:
        lock_data = find_lockfile()
        if lock_data is None:
            host_check = "no_lockfile"
        else:
            try:
                st = verify_sessions([sid], lock_data=lock_data).get(sid)
            except WakeError as e:
                host_check = f"verify_failed:{e.reason}"
            else:
                # listed at all - even statusless (None) - is alive
                # evidence: the host can still resume it
                if st != SESSION_ABSENT:
                    resp = JsonResponse(
                        session=sid,
                        status="conflict",
                        error=f"verifiably alive in acp-host (status={st}) (--force to override)",
                    )
                    return _emit(
                        args,
                        resp,
                        ExitCode.CONFLICT,
                        f"conflict: {sid} alive in acp-host status={st} (--force to override)",
                    )
                host_check = "absent"

    # tombstone-rename the mailbox BEFORE the db delete (audit F1,
    # task #203): between the delete commit and a direct rmtree of
    # <sid>, a relay could materialize new mail or a same-sid
    # re-register could create a fresh mailbox that the late rmtree
    # would wipe. An atomic rename parks the pre-delete mailbox
    # under a name no writer targets; a refusal restores it, a
    # success deletes the tombstone. Mail landing in a recreated
    # <sid> dir during the delete belongs to the post-delete world
    # and is left alone.
    mdir = mailbox_dir(sid, db_path=args.db, queue_dir=args.queue_dir)
    _reap_unregister_tombstones(sid, mdir)
    tombstone = None
    rename_failed = False
    if os.path.isdir(mdir):
        tombstone = _tombstone_path(sid, mdir)
        try:
            os.rename(mdir, tombstone)
        except OSError:
            tombstone = None
            rename_failed = True

    def _restore_tombstone() -> str:
        """Put the parked mailbox back - the registration survived.

        Returns 'restored' | 'merged' | 'none' | 'failed:<err>' so a
        restore failure lands in the response instead of silently
        stranding the mail.
        """
        nonlocal tombstone
        if tombstone is None:
            return "none"
        try:
            if not os.path.isdir(mdir):
                os.rename(tombstone, mdir)
                result = "restored"
            else:
                # a writer recreated <sid> in the window: merge the
                # parked files into the live dir without clobbering
                # post-delete writes, then drop the park
                _merge_dir_no_clobber(tombstone, mdir)
                shutil.rmtree(tombstone)
                result = "merged"
        except OSError as e:
            result = f"failed:{e}"
        tombstone = None
        return result

    try:
        verdict = db.unregister_session(sid, force=args.force, path=args.db)
    except Exception:
        # the delete never committed: the registration is intact, so
        # the parked mailbox must come back before the error surfaces
        _restore_tombstone()
        raise

    if isinstance(verdict, UnregisterNotFound):
        # raced with a concurrent unregister: the row is already gone,
        # which is the outcome the caller wanted. The parked mailbox
        # is restored ONLY when the sid re-registered in the window
        # (it belongs to the new registration); otherwise it is the
        # deleted session's last mailbox - remove it rather than
        # resurrecting an orphan dir nothing drains.
        if db.get_session(sid, path=args.db) is not None:
            restored = _restore_tombstone()
        else:
            if tombstone is not None:
                shutil.rmtree(tombstone, ignore_errors=True)
                tombstone = None
            # the row is gone: any <sid> dir left (e.g. the tombstone
            # rename failed earlier, or a writer slipped files into a
            # recreated dir) is orphaned dead-session mail - remove it
            # instead of leaving an undrainable artifact behind
            if os.path.isdir(mdir):
                shutil.rmtree(mdir, ignore_errors=True)
            restored = "removed"
        resp = JsonResponse(
            session=sid,
            status="not_found",
            error=f"no registration for {sid} (removed concurrently)",
            data={"mailbox_restored": restored},
        )
        return _emit(
            args,
            resp,
            ExitCode.NOT_FOUND,
            f"not registered: {sid} (removed concurrently)",
        )
    if isinstance(verdict, UnregisterRefusedLiveTask):
        restored = _restore_tombstone()
        resp = JsonResponse(
            session=sid,
            status="conflict",
            error=(
                f"task {verdict.task_id} is {verdict.task_status} - close it before unregistering"
            ),
            data={"mailbox_restored": restored},
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: task {verdict.task_id} still {verdict.task_status} (mailbox {restored})",
        )
    if isinstance(verdict, UnregisterRefusedFreshHeartbeat):
        restored = _restore_tombstone()
        resp = JsonResponse(
            session=sid,
            status="conflict",
            error=(
                f"fresh heartbeat {verdict.age_seconds}s ago"
                " - session is alive (--force to override)"
            ),
            data={"mailbox_restored": restored},
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: {sid} heartbeat {verdict.age_seconds}s ago (--force to override)"
            f" (mailbox {restored})",
        )
    counts = verdict.counts
    mail_files = 0
    if tombstone is not None:
        try:
            mail_files = len(os.listdir(tombstone))
        except OSError:
            mail_files = None
        shutil.rmtree(tombstone, ignore_errors=True)
        mailbox_removed = not os.path.isdir(tombstone)
    elif os.path.isdir(mdir):
        # the tombstone rename failed earlier - degraded to the old
        # delete-in-place path, which keeps the F1 race window
        try:
            mail_files = len(os.listdir(mdir))
        except OSError:
            mail_files = None
        shutil.rmtree(mdir, ignore_errors=True)
        mailbox_removed = not os.path.isdir(mdir)
    else:
        mailbox_removed = True
    recreated = os.path.isdir(mdir)
    daemon._decision(
        _state_dir(args),
        "session_unregistered",
        sid,
        f"counts={counts} mail_files={mail_files} host={host_check}",
    )
    resp = JsonResponse(
        session=sid,
        status="unregistered",
        queue_depth=_queue_depth(args),
        data={
            "counts": counts,
            "mailbox_files_removed": mail_files,
            "mailbox_removed": mailbox_removed,
            "mailbox_tombstoned": tombstone is not None,
            "mailbox_rename_failed": rename_failed,
            "mailbox_dir_recreated": recreated,
            "host_check": host_check,
        },
    )
    tail = "" if mailbox_removed else " (mailbox dir survived removal!)"
    files_text = "unknown" if mail_files is None else str(mail_files)
    return _emit(
        args,
        resp,
        ExitCode.OK,
        f"unregistered {sid} ({files_text} mailbox file(s), host={host_check}){tail}",
    )


def _halt_targets(args) -> tuple[list[str], int | None]:
    """Session ids the halt/resume applies to; empty + usage rc.

    ``--all`` broadcasts to every managed+active session - the same
    fleet set ``bridge disarm --all`` addresses.
    """
    if args.all:
        return [
            r["session_id"]
            for r in _all_session_rows(args)
            if r["mode"] == SessionMode.MANAGED.value and r["status"] == SessionStatus.ACTIVE.value
        ], None
    if args.session_id:
        return [args.session_id], None
    resp = JsonResponse(
        status="usage",
        error=f"{args.command} requires a session id or --all",
    )
    rc = _emit(
        args,
        resp,
        ExitCode.USAGE,
        f"usage: bridge {args.command} <session_id> | bridge {args.command} --all",
    )
    return [], rc


def _mailbox_file_count(args, sid: str) -> int:
    """Unconsumed ``msg_*.txt`` files in the sid's mailbox - the
    "whose mail is undrained" column of the halt snapshot."""
    try:
        return sum(
            1
            for name in os.listdir(os.path.join(_queue_dir(args), sid))
            if name.startswith("msg_") and name.endswith(".txt")
        )
    except OSError:
        return 0


def _halt_apply(args, sid: str, halt: bool) -> dict:
    """Flag + notify + wake one session; fold the result into a row.

    HALT is a drain-on-boundary contract (task #180): the flag and
    notification persist regardless of wake outcome, so a mid-turn
    session picks it up at its own boundary - the wake is a
    best-effort speedup, not the delivery path.
    """
    sess = db.get_session(sid, path=args.db)
    row = {"session_id": sid}
    if sess is None or sess["status"] != SessionStatus.ACTIVE.value:
        row["status"] = "skipped"
        row["reason"] = "not registered" if sess is None else f"is {sess['status']}"
        return row
    if halt and sess["mode"] != SessionMode.MANAGED.value:
        row["status"] = "skipped"
        row["reason"] = "not managed - halt duty is managed-only"
        return row
    if halt and sess["halted_at"] is not None:
        row["status"] = "skipped"
        row["reason"] = "already halted"
        return row
    if not halt and sess["halted_at"] is None:
        row["status"] = "skipped"
        row["reason"] = "not halted"
        return row
    # per-target fault isolation (PR #79 review): a failure on one
    # broadcast member must not abort the rest of the fleet
    try:
        db.set_session_halted(sid, halt, path=args.db)
        kind = NotificationKind.HALT if halt else NotificationKind.RESUME
        body = _HALT_BODY if halt else _RESUME_BODY
        if args.reason:
            body += f" ({args.reason})"
        row["notification_id"] = db.notify(sid, kind, body, path=args.db)
        try:
            row["wake"] = wake_session(sid, _session_cwd(sid, args))
        except WakeError as e:
            row["wake"] = f"failed:{e.reason}"
        row["mailbox_files"] = _mailbox_file_count(args, sid)
        task = db.get_live_task(sid, path=args.db)
        row["live_task"] = task["id"] if task is not None else None
        row["halted"] = halt
        row["status"] = "halted" if halt else "resumed"
    except Exception as e:
        row["status"] = "error"
        row["reason"] = f"{type(e).__name__}: {e}"
        try:
            # the flag may have been applied before the failing step
            # (e.g. notify raised after set_session_halted) - report
            # the true durable state, not just the exception
            row["halted"] = db.get_session(sid, path=args.db)["halted_at"] is not None
        except Exception:
            pass
    return row


def _cmd_halt(args) -> int:
    """``bridge halt <sid>|--all [--stop-daemon]``: graceful fleet stop.

    Sets the durable halted flag, enqueues the HALT semantic
    notification and best-effort wakes each target; ``--all`` adds a
    fleet snapshot (who is mid-turn, whose mail is undrained, which
    tasks are in flight and will resume) and ``--stop-daemon`` SIGTERMs
    the relay loop afterwards.
    """
    sids, rc = _halt_targets(args)
    if rc is not None:
        return rc
    rows = [_halt_apply(args, sid, True) for sid in sids]
    data: dict = {"sessions": rows}
    daemon_state = None
    if getattr(args, "stop_daemon", False):
        sdir = _state_dir(args)
        daemon_state = _sigterm_daemon(sdir)
        data["daemon"] = daemon_state
        if daemon_state in ("stopped", "not_running"):
            # review fix (PR #79): a SIGTERM right after the outbox
            # write would kill the relay before the HALT rows ever
            # materialize as mailbox files - the woken sessions would
            # drain nothing and skip the save-state step. Relay them
            # synchronously now that the lock is free.
            try:
                daemon._relay_pass(args.db, _queue_dir(args), sdir)
                data["relayed"] = True
            except Exception as e:
                # state is already safe (flag set + outbox rows exist);
                # report instead of tracebacking past the JSON emit
                data["relay_error"] = f"{type(e).__name__}: {e}"
    resp = JsonResponse(status="ok", data=data)
    lines = [
        f"halt {r['session_id']}: {r['status']}"
        + (f" ({r['reason']})" if r.get("reason") else "")
        + (f" wake={r['wake']}" if r.get("wake") else "")
        + (
            f" mail={r['mailbox_files']} task=#{r['live_task']}"
            if r.get("status") == "halted"
            else ""
        )
        for r in rows
    ]
    if daemon_state is not None:
        lines.append(f"daemon {daemon_state}")
    return _emit(args, resp, ExitCode.OK, "\n".join(lines))


def _cmd_resume(args) -> int:
    """``bridge resume <sid>|--all``: lift the fleet halt.

    Clears halted_at, enqueues the RESUME semantic and wakes the
    session so queued work flows again.
    """
    sids, rc = _halt_targets(args)
    if rc is not None:
        return rc
    rows = [_halt_apply(args, sid, False) for sid in sids]
    resp = JsonResponse(status="ok", data={"sessions": rows})
    lines = [
        f"resume {r['session_id']}: {r['status']}"
        + (f" ({r['reason']})" if r.get("reason") else "")
        + (f" wake={r['wake']}" if r.get("wake") else "")
        for r in rows
    ]
    return _emit(args, resp, ExitCode.OK, "\n".join(lines))


# --- orchestration bus: register / ask / ack (item_13e1) -------------------------


def _cmd_register(args) -> int:
    """``bridge register <sid>``: join the bus as worker/orchestrator.

    Orchestrator registrations go through the CAS singleton in
    ``db.register`` - a refused takeover (fresh-heartbeat holder or
    managed-active holder) maps to CONFLICT with a ``--force`` hint
    instead of a bare error.
    """
    result = db.register(
        args.session_id,
        args.workspace or os.getcwd(),
        role=args.role,
        force=args.force,
        path=args.db,
    )
    if not result.accepted:
        fresh = (
            result.heartbeat_age is not None and result.heartbeat_age < db.HEARTBEAT_STALE_SECONDS
        )
        if fresh:
            why = (
                f"orchestrator {result.holder} holds a fresh heartbeat"
                f" ({result.heartbeat_age}s ago)"
            )
        else:
            why = (
                f"orchestrator {result.holder} is a managed-active holder"
                " - heartbeat age cannot demote it"
            )
        resp = JsonResponse(
            session=args.session_id,
            status="refused",
            error=why,
            agent_instruction=("retry with --force to take over the orchestrator role"),
            data={
                "holder": result.holder,
                "heartbeat_age": result.heartbeat_age,
            },
        )
        human = f"refused: {why}; use --force to take over"
        return _emit(args, resp, ExitCode.CONFLICT, human)
    # a crash inside a previous unregister can leave the mailbox
    # parked as .unregistered-<sid>-* - merge it back so the new
    # registration drains its pre-crash mail (task #203 reaper)
    _reap_unregister_tombstones(
        result.session_id,
        mailbox_dir(result.session_id, db_path=args.db, queue_dir=args.queue_dir),
    )
    resp = JsonResponse(
        session=result.session_id,
        status="registered",
        data={"role": result.role, "generation": result.generation},
    )
    human = f"{result.session_id} registered as {result.role} (generation {result.generation})"
    return _emit(args, resp, ExitCode.OK, human)


def _cmd_ask(args) -> int:
    """``bridge ask``: worker question for the orchestrator."""
    if not args.text.strip():
        resp = JsonResponse(
            session=args.session,
            status="usage",
            error="ask requires non-empty question text",
        )
        return _emit(args, resp, ExitCode.USAGE, "usage: bridge ask --session SID --task ID <text>")
    result = db.ask(args.session, args.task, args.text, path=args.db)
    if not result.accepted:
        if result.reason == "pending_exists":
            error = f"task {args.task} already has a pending question"
            instruction = "do not retry: wait for the pending question to be answered"
        elif result.reason == "no_orchestrator":
            error = "no active orchestrator on the bus - the question has nowhere to go"
            instruction = (
                "do not retry: escalate to the human or wait for an"
                " orchestrator to register, then re-ask"
            )
        else:
            error = (
                f"task {args.task} exhausted the question budget"
                f" ({MAX_QUESTIONS_PER_TASK} questions)"
            )
            instruction = "do not retry: question budget exhausted - escalate to the human"
        resp = JsonResponse(
            session=args.session,
            status="refused",
            error=error,
            agent_instruction=instruction,
            data={"reason": result.reason, "task_id": args.task},
        )
        return _emit(args, resp, ExitCode.CONFLICT, f"ask refused: {error}")
    resp = JsonResponse(
        session=args.session,
        status=QuestionState.PENDING.value,
        data={"question_id": result.question_id, "task_id": args.task},
    )
    return _emit(
        args,
        resp,
        ExitCode.OK,
        f"question {result.question_id} pending for task {args.task}",
    )


def _push_registered(args, kind: NotificationKind) -> int:
    """Shared body of ``bridge directive`` / ``bridge notice``: write a
    notification row so the daemon relays it into the fs mailbox AND
    the actionable-mail wake leg reaches the session. A hand-written
    msg_*.txt gets woken too (the file leg) but lacks registry
    evidence - no ack tracking, no dedup, no stall attribution."""
    verb = kind.value
    if not args.text.strip():
        resp = JsonResponse(
            session=args.session_id,
            status="usage",
            error=f"{verb} requires non-empty text",
        )
        return _emit(
            args,
            resp,
            ExitCode.USAGE,
            f"usage: bridge {verb} <session_id> <text>",
        )
    target = db.get_session(args.session_id, path=args.db)
    if target is None or target["status"] != "active":
        # a mistyped or dead sid would sit undrained forever and
        # eventually fire a phantom mail-stall escalation at the
        # orchestrator
        resp = JsonResponse(
            session=args.session_id,
            status="not_found",
            error=(
                f"session {args.session_id} is not registered"
                if target is None
                else f"session {args.session_id} is {target['status']}, not active"
            ),
            agent_instruction="do not retry: check the session id and re-send",
        )
        return _emit(
            args,
            resp,
            ExitCode.NOT_FOUND,
            f"not deliverable: session {args.session_id}"
            + (" is not registered" if target is None else f" is {target['status']}"),
        )
    nid = db.notify(
        args.session_id,
        kind,
        args.text,
        path=args.db,
    )
    resp = JsonResponse(
        session=args.session_id,
        status="queued",
        data={"notification_id": nid},
    )
    return _emit(
        args,
        resp,
        ExitCode.OK,
        f"{verb} notification {nid} queued for {args.session_id}",
    )


def _cmd_directive(args) -> int:
    """``bridge directive``: the registered directive-push path
    (item_139 amendment). DIRECTIVE is a steering instruction the
    recipient must obey - reports and status notes use
    ``bridge notice`` instead."""
    return _push_registered(args, NotificationKind.DIRECTIVE)


def _cmd_notice(args) -> int:
    """``bridge notice``: the registered informational-report path
    (question_answer vs directive semantics backlog item). NOTICE is
    read-and-incorporate, not an order - the worker-to-orchestrator
    report flow rides it so the recipient's duty table can tell
    "obey" apart from "read". Same delivery mechanics as
    ``bridge directive``: outbox row, fs relay, actionable wake."""
    return _push_registered(args, NotificationKind.NOTICE)


def _cmd_event(args) -> int:
    """``bridge event <sid> <kind> <text>``: append a durable decision.

    The append-only ``decision_events`` log (task #207 item g) is
    the restart-proof home for decisions that used to live in
    session memory only - contracts signed, PLAN_DELTAs, review
    verdicts, milestones. ``<sid>`` names the deciding session;
    ``-`` records a bus-level decision no single session owns.
    """
    sid = None if args.session_id == "-" else args.session_id
    try:
        eid = db.record_event(sid, args.event_kind, args.text, task_id=args.task, path=args.db)
    except ValueError as e:
        resp = JsonResponse(session=sid, status="usage", error=str(e))
        return _emit(args, resp, ExitCode.USAGE, f"usage: {e}")
    resp = JsonResponse(
        session=sid,
        status="ok",
        data={"event_id": eid, "kind": args.event_kind.strip().lower()},
    )
    return _emit(
        args,
        resp,
        ExitCode.OK,
        f"event {eid} logged ({args.event_kind.strip().lower()})",
    )


def _cmd_events(args) -> int:
    """``bridge events [--session SID] [--kind K] [--last N]``: read the log."""
    if args.last < 1:
        return _emit(
            args,
            JsonResponse(status="error", error="--last must be >= 1"),
            ExitCode.USAGE,
            "events: --last must be >= 1",
        )
    bus_only = args.session == "-"
    rows = db.list_events(
        session_id=None if bus_only else args.session,
        bus_only=bus_only,
        kind=args.kind,
        limit=args.last,
        path=args.db,
    )
    items = [
        {
            "id": r["id"],
            "session": r["session_id"],
            "kind": r["kind"],
            "body": r["body"],
            "task_id": r["task_id"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]
    resp = JsonResponse(session=args.session, status="ok", data={"events": items})
    if not items:
        return _emit(args, resp, ExitCode.OK, "no events")
    lines = [f"#{r['id']} [{r['kind']}] {r['session'] or '-'}: {r['body'][:80]}" for r in items]
    return _emit(args, resp, ExitCode.OK, "\n".join(lines))


def _cmd_reserve(args) -> int:
    """``bridge reserve <type> <id> --project P``: atomic allocation.

    The host-level resource registry (directive 426): a project
    declares each host resource it needs BEFORE deploying - the
    PRIMARY KEY on (type, id) is the mutex, so the verdict is
    atomic: granted (new or same-owner renew) or conflict echoing
    the incumbent owner. Deploy scripts treat any non-granted exit
    as a hard fail and escalate to the orchestrator - never retry
    or force their way past a conflict.

    ``--fwd`` (tcp_port only) embeds a machine-readable forward
    declaration into the stored purpose - the neutral ingress layer
    materializes a host listener from it (docs/ALLOCATIONS.md).
    """
    purpose = args.purpose
    if args.fwd is not None:
        try:
            fwd = ingress_spec.parse_fwd_spec(args.fwd, bind=args.bind or ingress_spec.DEFAULT_BIND)
        except ValueError as e:
            resp = JsonResponse(status="usage", error=str(e))
            return _emit(args, resp, ExitCode.USAGE, f"usage: {e}")
        if args.resource_type != "tcp_port":
            resp = JsonResponse(status="usage", error="--fwd is only valid for tcp_port")
            return _emit(args, resp, ExitCode.USAGE, "usage: --fwd is only valid for tcp_port")
        purpose = ingress_spec.format_purpose(
            fwd.spec_text,
            args.bind,
            args.purpose,
        )
    elif args.bind is not None:
        resp = JsonResponse(status="usage", error="--bind requires --fwd")
        return _emit(args, resp, ExitCode.USAGE, "usage: --bind requires --fwd")
    try:
        verdict = db.reserve(
            args.resource_type,
            args.resource_id,
            args.project,
            purpose=purpose,
            session_id=args.session,
            ttl=args.ttl,
            path=args.db,
        )
    except ValueError as e:
        resp = JsonResponse(status="usage", error=str(e))
        return _emit(args, resp, ExitCode.USAGE, f"usage: {e}")
    if isinstance(verdict, ReserveConflict):
        resp = JsonResponse(
            status="conflict",
            error=f"{args.resource_type}/{verdict.resource_id} held by {verdict.owner}",
            data={
                "owner": verdict.owner,
                "incumbent_resource_id": verdict.resource_id,
                "purpose": verdict.purpose,
                "created_at": verdict.created_at,
                "expires_at": verdict.expires_at,
            },
        )
        # the incumbent key differs from the request only on a
        # wildcard (systemd_unit_prefix `P-*`) conflict - name it so
        # release --force arbitration needs no second query
        key_note = (
            f" via wildcard {verdict.resource_id}"
            if verdict.resource_id != args.resource_id
            else ""
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: {args.resource_type}/{args.resource_id} held by {verdict.owner}"
            f"{key_note}" + (f" ({verdict.purpose})" if verdict.purpose else ""),
        )
    assert isinstance(verdict, ReserveGranted)
    resp = JsonResponse(
        status="ok",
        data={
            "resource_type": args.resource_type,
            "resource_id": args.resource_id,
            "project": args.project,
            "renewed": verdict.renewed,
            "expires_at": verdict.expires_at,
        },
    )
    verb = "renewed" if verdict.renewed else "reserved"
    return _emit(
        args,
        resp,
        ExitCode.OK,
        f"{verb}: {args.resource_type}/{args.resource_id} -> {args.project}",
    )


def _cmd_release(args) -> int:
    """``bridge release <type> <id> --project P [--force]``: drop the
    allocation. Owner-only; ``--force`` is the orchestrator's
    arbitration override for genuinely contested resources - deploy
    scripts must never pass it."""
    try:
        verdict = db.release(
            args.resource_type,
            args.resource_id,
            args.project,
            force=args.force,
            path=args.db,
        )
    except ValueError as e:
        resp = JsonResponse(status="usage", error=str(e))
        return _emit(args, resp, ExitCode.USAGE, f"usage: {e}")
    if isinstance(verdict, ReleaseNotFound):
        resp = JsonResponse(status="ok", data={"released": False})
        return _emit(args, resp, ExitCode.OK, "no allocation at that key")
    if isinstance(verdict, ReleaseRefusedOwner):
        resp = JsonResponse(
            status="conflict",
            error=f"held by {verdict.owner} (use --force only for orchestrator arbitration)",
            data={"owner": verdict.owner},
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: held by {verdict.owner} - orchestrator arbitration only (--force)",
        )
    assert isinstance(verdict, ReleaseRemoved)
    resp = JsonResponse(status="ok", data={"released": True, "owner": verdict.owner})
    return _emit(
        args,
        resp,
        ExitCode.OK,
        f"released: {args.resource_type}/{args.resource_id} (was {verdict.owner})",
    )


def _cmd_allocations(args) -> int:
    """``bridge allocations [--type T] [--project P]``: live registry."""
    rows = db.list_allocations(resource_type=args.type, project_id=args.project, path=args.db)
    items = [
        {
            "resource_type": r["resource_type"],
            "resource_id": r["resource_id"],
            "project": r["project_id"],
            "purpose": r["purpose"],
            "session": r["session_id"],
            "created_at": r["created_at"],
            "expires_at": r["expires_at"],
        }
        for r in rows
    ]
    resp = JsonResponse(status="ok", data={"allocations": items})
    if not items:
        return _emit(args, resp, ExitCode.OK, "no allocations")
    lines = [
        f"{r['resource_type']}/{r['resource_id']} -> {r['project']}"
        + (f" ({r['purpose']})" if r["purpose"] else "")
        + (f" [expires {r['expires_at']}]" if r["expires_at"] else "")
        for r in items
    ]
    return _emit(args, resp, ExitCode.OK, "\n".join(lines))


def _cmd_ingress(args) -> int:
    """``bridge ingress sync|status``: the neutral host ingress layer.

    sync reconciles running forwarder processes with tcp_port rows
    carrying a fwd= declaration; status shows desired vs running.
    """
    rows = db.list_allocations(resource_type="tcp_port", path=args.db)
    sdir = _state_dir(args)
    if args.ingress_action == "sync":
        summary = ingress_sync.sync(sdir, rows)
        ok = summary.get("status") == "ok"
        resp = JsonResponse(
            status="ok" if ok else "skipped",
            data=summary,
            error=None if ok else summary.get("reason"),
        )
        lines = [
            f"started={summary.get('started', [])} "
            f"stopped={summary.get('stopped', [])} "
            f"kept={summary.get('kept', [])} "
            f"errors={len(summary.get('errors', []))}"
        ]
        for err in summary.get("errors", []):
            lines.append(f"  port {err['port']}: {err['error']} ({err.get('spec')})")
        return _emit(args, resp, ExitCode.OK, "\n".join(lines))
    view = ingress_sync.status(sdir, rows)
    resp = JsonResponse(status="ok", data=view)
    if not view["forwards"]:
        return _emit(args, resp, ExitCode.OK, "no ingress forwards declared")
    lines = [
        f"{f['port']} {'UP' if f['alive'] else 'DOWN'} "
        f"{f['bind']}:{f['port']} -> {f['spec']} pid={f['forwarder_pid']}"
        for f in view["forwards"]
    ]
    for port in view["orphaned"]:
        lines.append(f"{port} ORPHAN (tracked but row gone)")
    return _emit(args, resp, ExitCode.OK, "\n".join(lines))


def _cmd_ack(args) -> int:
    """``bridge ack``: advance the restart watermark after mailbox consume."""
    acked = db.ack_notifications(args.session, args.up_to, path=args.db)
    resp = JsonResponse(
        session=args.session,
        status="ok",
        data={"acked": acked, "up_to_id": args.up_to},
    )
    human = f"{args.session}: acked {acked} notification(s)"
    if args.up_to is not None:
        human += f" up to {args.up_to}"
    return _emit(args, resp, ExitCode.OK, human)


def _managed_owner(sid: str, tid: int) -> str:
    """Deterministic lease owner for managed claim/finish (item_18).

    Derived from (session, task) so `bridge finish` recomputes the
    same owner without any state - the finish CAS binds on it.
    """
    return f"{DISPATCH_OWNER_PREFIX}{sid}-{tid}"


def _cmd_claim(args) -> int:
    """``bridge claim <task_id> --session <sid>``: managed task claim.

    Managed delivery happens via the mailbox; nothing else marks the
    task, so the worker claims it straight to ``running`` (item_18).
    Idempotent: claiming a task already running under this session's
    owner renews the lease instead of failing - long tasks keep
    calling it as a heartbeat. Refusals are structured: the task
    belongs to another session, or it is no longer queued.
    """
    tid = args.task_id
    sid = args.session
    task = db.get_task(tid, path=args.db)
    if task is None:
        resp = JsonResponse(session=sid, status="not_found", error=f"task {tid} not found")
        return _emit(args, resp, ExitCode.NOT_FOUND, f"not found: task {tid}")
    if task["session_id"] != sid:
        resp = JsonResponse(
            session=sid,
            status="conflict",
            error=f"task {tid} belongs to {task['session_id']}, not {sid}",
        )
        return _emit(args, resp, ExitCode.CONFLICT, f"conflict: task {tid} is not yours")
    if _session_mode(sid, args) != SessionMode.MANAGED.value:
        resp = JsonResponse(
            session=sid,
            status="conflict",
            error=(
                f"{sid} is not managed; unmanaged tasks move through"
                " the hook/acp transports, not claim"
            ),
        )
        return _emit(args, resp, ExitCode.CONFLICT, f"conflict: {sid} is not managed")
    sess = db.get_session(sid, path=args.db)
    if sess is not None and sess["halted_at"] is not None:
        # the enforced half of the HALT contract (task #180): while
        # halted a session finishes its CURRENT atomic action only -
        # the lease renewal path below stays open (finishing is what
        # HALT asks for), but no new task may be claimed
        if task["status"] != TaskStatus.RUNNING.value or task["lease_owner"] != _managed_owner(
            sid, tid
        ):
            resp = JsonResponse(
                session=sid,
                status="halted",
                error=f"{sid} is halted - awaiting `bridge resume`; task {tid} persists queued",
                agent_instruction=(
                    "do not claim: the session is halted; the task persists until resume"
                ),
            )
            return _emit(args, resp, ExitCode.CONFLICT, f"halted: claim refused for task {tid}")
    owner = _managed_owner(sid, tid)
    if task["status"] == TaskStatus.RUNNING.value and task["lease_owner"] == owner:
        if db.renew_lease(tid, owner, path=args.db):
            resp = JsonResponse(session=sid, status="running", data={"renewed": True})
            return _emit(args, resp, ExitCode.OK, f"task {tid}: lease renewed")
        # the row moved under us (requeue/dead-worker race): do not
        # tell the worker it still owns the lease
        resp = JsonResponse(
            session=sid,
            status="conflict",
            error=f"lease renew CAS lost for task {tid}; re-inspect before continuing",
        )
        return _emit(args, resp, ExitCode.CONFLICT, f"conflict: lease renew lost for task {tid}")
    claimable = task["status"] == TaskStatus.QUEUED.value or (
        task["status"] == TaskStatus.DISPATCH_DECIDED.value and task["lease_owner"] == owner
    )
    if not claimable:
        resp = JsonResponse(
            session=sid,
            status=task["status"],
            error=(
                f"task {tid} is {task['status']}"
                f" (lease_owner={task['lease_owner']}); only your queued or"
                " mailbox-dispatched tasks are claimable"
            ),
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: task {tid} is {task['status']}",
        )
    if not db.claim_managed(tid, sid, owner, path=args.db):
        resp = JsonResponse(
            session=sid,
            status="conflict",
            error=f"claim of task {tid} refused (queue_paused or raced)",
        )
        return _emit(args, resp, ExitCode.CONFLICT, f"conflict: claim refused for task {tid}")
    row = db.get_task(tid, path=args.db)
    resp = JsonResponse(
        session=sid,
        status="running",
        data={
            "task_id": tid,
            "task_text": row["task_text"],
            "task_file": row["task_file"],
            "lease_expires_at": row["lease_expires_at"],
        },
    )
    return _emit(args, resp, ExitCode.OK, f"task {tid}: claimed, running")


def _cmd_finish(args) -> int:
    """``bridge finish <task_id> --session <sid> [--fail] [--note T]``.

    Lease-owner CAS ``running > done|failed`` for a managed worker -
    the closing half of the claim lifecycle (item_18).
    """
    tid = args.task_id
    sid = args.session
    task = db.get_task(tid, path=args.db)
    if task is None:
        resp = JsonResponse(session=sid, status="not_found", error=f"task {tid} not found")
        return _emit(args, resp, ExitCode.NOT_FOUND, f"not found: task {tid}")
    if task["session_id"] != sid:
        resp = JsonResponse(
            session=sid,
            status="conflict",
            error=f"task {tid} belongs to {task['session_id']}, not {sid}",
        )
        return _emit(args, resp, ExitCode.CONFLICT, f"conflict: task {tid} is not yours")
    if _session_mode(sid, args) != SessionMode.MANAGED.value:
        resp = JsonResponse(
            session=sid,
            status="conflict",
            error=(
                f"{sid} is not managed; unmanaged tasks are finished by"
                " their transport worker, not finish"
            ),
        )
        return _emit(args, resp, ExitCode.CONFLICT, f"conflict: {sid} is not managed")
    owner = _managed_owner(sid, tid)
    ok = not args.fail
    if not db.finish_task(tid, owner, ok, note=args.note, path=args.db):
        resp = JsonResponse(
            session=sid,
            status=task["status"],
            error=(
                f"finish refused: task {tid} is {task['status']}"
                f" (lease_owner={task['lease_owner']}); only the claiming"
                " session can finish a running task"
            ),
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: finish refused for task {tid}",
        )
    resp = JsonResponse(session=sid, status="done" if ok else "failed")
    return _emit(args, resp, ExitCode.OK, f"task {tid}: {'done' if ok else 'failed'}")


def _cmd_wait(args) -> int:
    """``bridge wait <task_id> --session <sid> [--reason T]`` (item_22).

    External-wait signal: the claiming managed session declares "still
    alive, blocked on something out-of-band" - CI, a PR review, a
    partner consult. Lease-owner CAS extends the lease by one
    LEASE_SECONDS and lands the reason in the task note. Without it a
    long wait reads exactly like a dead worker: task 87 failed on
    ``lease expired`` mid review-cycle. Repeat the call while waiting;
    a dead claimer simply stops renewing and the task requeues.
    """
    tid = args.task_id
    sid = args.session
    task = db.get_task(tid, path=args.db)
    if task is None:
        resp = JsonResponse(session=sid, status="not_found", error=f"task {tid} not found")
        return _emit(args, resp, ExitCode.NOT_FOUND, f"not found: task {tid}")
    if task["session_id"] != sid:
        resp = JsonResponse(
            session=sid,
            status="conflict",
            error=f"task {tid} belongs to {task['session_id']}, not {sid}",
        )
        return _emit(args, resp, ExitCode.CONFLICT, f"conflict: task {tid} is not yours")
    # lazy GC on this bus-touching path too (task #207 item f) - a
    # dead registration expires on the next call, daemon or not
    _gc_dead_sessions(args)
    if _session_mode(sid, args) != SessionMode.MANAGED.value:
        resp = JsonResponse(
            session=sid,
            status="conflict",
            error=(
                f"{sid} is not managed; unmanaged leases renew inside"
                " their transport worker, not via wait"
            ),
        )
        return _emit(args, resp, ExitCode.CONFLICT, f"conflict: {sid} is not managed")
    owner = _managed_owner(sid, tid)
    if not db.wait_task(tid, owner, args.reason, path=args.db):
        resp = JsonResponse(
            session=sid,
            status=task["status"],
            error=(
                f"wait refused: task {tid} is {task['status']}"
                f" (lease_owner={task['lease_owner']}); only the claiming"
                " session can wait on a running task"
            ),
        )
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"conflict: wait refused for task {tid}",
        )
    row = db.get_task(tid, path=args.db)
    resp = JsonResponse(
        session=sid,
        status="running",
        data={"task_id": tid, "lease_expires_at": row["lease_expires_at"]},
    )
    return _emit(
        args,
        resp,
        ExitCode.OK,
        f"task {tid}: lease renewed (waiting)",
    )


def _cmd_heartbeat(args) -> int:
    """``bridge heartbeat <sid>``: the liveness metronome (item_13g).

    Deliberately lighter than ``register``: no role change and no
    orchestrator CAS - a watcher cycle can keep its own session alive
    without ever taking the orchestrator role over. For unmanaged
    sessions it also revives a dead-flagged row (item_122): heartbeat
    is their dead-marking signal, so fresh heartbeat activity is
    authoritative recovery. Managed rows are not revived here - their
    dead verdict is session/list-derived, which a heartbeat cannot
    refute; they recover through ``register``. The bumped generation
    is returned so a holder detects supersession.
    """
    generation = db.heartbeat(args.session_id, path=args.db)
    resp = JsonResponse(
        session=args.session_id,
        status="ok",
        data={"generation": generation},
    )
    human = f"{args.session_id} heartbeat ok (generation {generation})"
    return _emit(args, resp, ExitCode.OK, human)


def _cmd_wake(args) -> int:
    """``bridge wake <sid>``: manual acp-host wake nudge (item_16).

    Attaches to the local acp-host and injects the fixed drain nudge
    into the session - the same path the daemon's ``_wake_pass`` uses.
    Bypasses the daemon cooldown but keeps the busy check: a session
    mid-turn is never prompted. ``--prompt`` overrides the nudge text.
    """
    sid = args.session_id
    try:
        outcome = wake_session(
            sid,
            _session_cwd(sid, args),
            nudge=args.prompt or None,
        )
    except WakeError as e:
        resp = JsonResponse(
            session=sid,
            status="error",
            error=f"wake failed ({e.reason}): {e}",
        )
        return _emit(args, resp, ExitCode.CONFLICT, f"wake {sid}: {e.reason} - {e}")
    resp = JsonResponse(session=sid, status=outcome)
    if outcome == "busy":
        return _emit(
            args,
            resp,
            ExitCode.CONFLICT,
            f"wake {sid}: session is mid-turn - not prompted",
        )
    human = f"wake {sid}: prompt {outcome}"
    return _emit(args, resp, ExitCode.OK, human)


# --- orchestrator question queue: list / claim / answer (item_13g) ---------------


def _question_view(q) -> dict:
    """One question row folded into the CLI payload."""
    return {
        "id": q["id"],
        "task_id": q["task_id"],
        "session_id": q["session_id"],
        "state": q["state"],
        "text": q["text"],
        "claimed_by": q["claimed_by"],
        "answer": q["answer"],
    }


def _questions_list(args) -> int:
    rows = db.list_pending_questions(task_id=args.task, path=args.db)
    if args.session:
        rows = [q for q in rows if q["session_id"] == args.session]
    items = [_question_view(q) for q in rows]
    resp = JsonResponse(status="ok", data={"questions": items})
    if not items:
        return _emit(args, resp, ExitCode.OK, "(no pending questions)")
    lines = [
        f"question {i['id']} (task {i['task_id']}, session {i['session_id']}): {i['text']}"
        for i in items
    ]
    return _emit(args, resp, ExitCode.OK, "\n".join(lines))


def _claim_refusal(args, qid: int, action: str) -> int:
    """Shared CONFLICT/NOT_FOUND response for a lost claim/answer CAS."""
    q = db.get_question(qid, path=args.db)
    if q is None:
        resp = JsonResponse(status="not_found", error=f"question {qid} not found")
        return _emit(args, resp, ExitCode.NOT_FOUND, f"not found: question {qid}")
    detail = f"question {qid} is {q['state']}"
    if q["claimed_by"]:
        detail += f" (claimed by {q['claimed_by']})"
    resp = JsonResponse(
        status=q["state"],
        error=f"cannot {action}: {detail}",
        agent_instruction=(
            "do not retry: the claim was lost - a stale claim returns to"
            " pending via the daemon janitor, then it can be claimed again"
        ),
        data=_question_view(q),
    )
    return _emit(args, resp, ExitCode.CONFLICT, f"conflict: {detail}")


def _questions_claim(args) -> int:
    qid = args.question_id
    if not db.claim_question(qid, args.claimant, path=args.db):
        return _claim_refusal(args, qid, "claim")
    resp = JsonResponse(
        status=QuestionState.CLAIMED.value,
        data={"question_id": qid, "claimed_by": args.claimant},
    )
    return _emit(args, resp, ExitCode.OK, f"question {qid} claimed by {args.claimant}")


def _questions_answer(args) -> int:
    """Answer a claimed question; the claimant binding is the CAS guard.

    On success the asking session gets a QUESTION_ANSWER outbox row so
    the daemon relay delivers the answer into its mailbox - the same
    path the orchestrator playbook documents for the Python API.
    """
    qid = args.question_id
    if not args.text.strip():
        resp = JsonResponse(status="usage", error="answer requires non-empty text")
        return _emit(
            args,
            resp,
            ExitCode.USAGE,
            "usage: bridge questions answer <qid> --claimant SID <text>",
        )
    # notify_worker commits the QUESTION_ANSWER outbox row inside the
    # answer's transaction - no crash window between the two (item_20)
    if not db.answer_question(
        qid, args.text, claimant=args.claimant, notify_worker=True, path=args.db
    ):
        return _claim_refusal(args, qid, "answer")
    q = db.get_question(qid, path=args.db)
    resp = JsonResponse(
        session=q["session_id"],
        status=QuestionState.ANSWERED.value,
        data={
            "question_id": qid,
            "task_id": q["task_id"],
            "notified": True,
        },
    )
    return _emit(args, resp, ExitCode.OK, f"question {qid} answered")


def _cmd_questions(args) -> int:
    action = args.questions_action
    if action == "list":
        return _questions_list(args)
    if action == "claim":
        return _questions_claim(args)
    if action == "answer":
        return _questions_answer(args)
    resp = JsonResponse(status="usage", error=f"unknown questions action: {action}")
    return _emit(args, resp, ExitCode.USAGE, f"usage: unknown questions action {action}")


# --- escalations: show / reply ----------------------------------------------------

#: directives ``escalate reply`` accepts verbatim; ``override:<text>``
#: is the fourth form
_DIRECTIVES = ("A", "B", "defer", "abort")

#: claimant name the human-reply path uses when it must claim a
#: still-pending question bound to an escalation before answering it
_ESCALATE_CLAIMANT = "bridge-escalate"


def _parse_directive(arg: str) -> str | None:
    """Validate a reply directive; returns it verbatim or None."""
    if arg in _DIRECTIVES:
        return arg
    if arg.startswith("override:") and arg[len("override:") :].strip():
        return arg
    return None


def _escalation_view(row) -> dict:
    """One open escalation folded into the ``show`` payload."""
    try:
        partner_outputs = json.loads(row["partner_outputs"] or "{}")
    except (TypeError, ValueError):
        partner_outputs = {}
    reply_command = f"bridge escalate reply {row['id']} A|B|defer|abort|override:<text>"
    return {
        "id": row["id"],
        "session_id": row["session_id"],
        "task_id": row["task_id"],
        "question_id": row["question_id"],
        "doa_trigger": row["doa_trigger"],
        "question_text": row["question_text"],
        "partner_outputs": partner_outputs,
        "synthesis": row["synthesis"],
        "created_at": row["created_at"],
        "reply_command": reply_command,
    }


def _escalate_show(args) -> int:
    rows = db.list_open_escalations(path=args.db)
    items = [_escalation_view(r) for r in rows]
    resp = JsonResponse(status="ok", data={"escalations": items})
    if not items:
        return _emit(args, resp, ExitCode.OK, "(no open escalations)")
    lines = []
    for i in items:
        lines.append(
            f"escalation {i['id']} (session {i['session_id'] or '-'}, task {i['task_id'] or '-'})"
        )
        lines.append(f"  trigger:   {i['doa_trigger'] or '-'}")
        lines.append(f"  question:  {i['question_text'] or '-'}")
        lines.append(f"  synthesis: {i['synthesis'] or '-'}")
        lines.append(f"  reply:     {i['reply_command']}")
    return _emit(args, resp, ExitCode.OK, "\n".join(lines))


def _already_resolved(args, row) -> int:
    existing = row["directive"]
    detail = f" (directive={existing!r})" if existing is not None else ""
    resp = JsonResponse(
        session=row["session_id"],
        status="resolved",
        error=f"escalation {args.audit_id} already resolved{detail}",
        agent_instruction="do not retry: the escalation is already resolved",
    )
    return _emit(
        args,
        resp,
        ExitCode.CONFLICT,
        f"conflict: escalation {args.audit_id} already resolved{detail}",
    )


def _answer_bound_question(question_id: int, detail: str, args) -> bool:
    """Answer the question bound to a resolved escalation.

    A claimed question is answered with the claimant binding (the
    13b2 hardening: a claim that changed hands between the read and
    the write loses the CAS); a still-pending one is claimed by the
    reply path first so the bound worker is never left hanging.
    """
    q = db.get_question(question_id, path=args.db)
    if q is None:
        return False
    if q["state"] == QuestionState.PENDING.value:
        db.claim_question(question_id, _ESCALATE_CLAIMANT, path=args.db)
        q = db.get_question(question_id, path=args.db)
    if q["state"] != QuestionState.CLAIMED.value:
        return False
    return db.answer_question(
        question_id,
        detail,
        claimant=q["claimed_by"],
        path=args.db,
    )


def _escalate_reply(args) -> int:
    directive = _parse_directive(args.directive)
    if directive is None:
        resp = JsonResponse(
            status="usage",
            error=f"invalid directive {args.directive!r}",
        )
        return _emit(
            args,
            resp,
            ExitCode.USAGE,
            "usage: bridge escalate reply <id> A|B|defer|abort|override:<text>",
        )
    row = db.get_audit_event(args.audit_id, path=args.db)
    if row is None or row["kind"] != AuditEventKind.ESCALATION.value:
        resp = JsonResponse(
            status="not_found",
            error=f"no escalation with audit id {args.audit_id}",
        )
        return _emit(
            args,
            resp,
            ExitCode.NOT_FOUND,
            f"not found: escalation {args.audit_id}",
        )
    if row["directive"] is not None:
        return _already_resolved(args, row)
    # the CAS is the idempotency gate: exactly one reply wins the fill
    if not db.resolve_escalation(args.audit_id, directive, path=args.db):
        return _already_resolved(args, db.get_audit_event(args.audit_id, path=args.db) or row)

    # fixed mapping, no new TaskStatus: defer parks the task on paused,
    # abort lands it on terminal failed - both carrying the human note
    tid = row["task_id"]
    task_status = None
    detail = directive
    if directive == "defer":
        detail = "blocked: deferred by human"
        if tid is not None and db.defer_task(tid, note=detail, path=args.db):
            task_status = TaskStatus.PAUSED.value
    elif directive == "abort":
        detail = "aborted by human: cleanup branch"
        if tid is not None and db.abort_task(tid, note=detail, path=args.db):
            task_status = TaskStatus.FAILED.value
    elif directive.startswith("override:"):
        detail = directive.split(":", 1)[1]

    qid = row["question_id"]
    question_answered = False
    if qid is not None:
        question_answered = _answer_bound_question(qid, detail, args)

    notified = False
    if row["session_id"]:
        db.notify(
            row["session_id"],
            NotificationKind.DIRECTIVE,
            f"escalation {args.audit_id} resolved: {detail}",
            task_id=tid,
            question_id=qid,
            path=args.db,
        )
        notified = True

    resp = JsonResponse(
        session=row["session_id"],
        status="resolved",
        data={
            "audit_id": args.audit_id,
            "directive": directive,
            "task_id": tid,
            "task_status": task_status,
            "question_id": qid,
            "question_answered": question_answered,
            "notified": notified,
        },
    )
    human = f"escalation {args.audit_id} resolved: {directive}"
    if task_status is not None:
        human += f"; task {tid} {task_status}"
    return _emit(args, resp, ExitCode.OK, human)


def _cmd_escalate(args) -> int:
    action = args.escalate_action
    if action == "show":
        return _escalate_show(args)
    if action == "reply":
        return _escalate_reply(args)
    resp = JsonResponse(status="usage", error=f"unknown escalate action: {action}")
    return _emit(args, resp, ExitCode.USAGE, f"usage: unknown escalate action {action}")


# --- recurring schedules (item_14) ----------------------------------------------

#: ``--every`` grammar: a number plus an optional unit suffix
#: (s/m/h/d); a bare number means seconds.
_EVERY_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _parse_every(spec: str):
    """Parse ``--every`` (e.g. ``300``, ``30m``, ``1h``, ``1d``).

    Returns the interval in seconds, or ``None`` on a malformed spec.
    """
    m = re.fullmatch(r"(\d+)([smhd]?)", (spec or "").strip())
    if m is None:
        return None
    seconds = int(m.group(1)) * _EVERY_UNITS[m.group(2) or "s"]
    return seconds or None


def _parse_at(spec: str, now: int):
    """Parse ``--at HH:MM`` into the next local occurrence's unix ts.

    Daily schedules normalize to ``every_seconds=86400`` anchored on
    the nearest HH:MM (today when still ahead, else tomorrow) - a full
    cron parser is deliberately out of scope. Returns ``None`` on a
    malformed spec.
    """
    m = re.fullmatch(r"(\d{1,2}):(\d{2})", (spec or "").strip())
    if m is None:
        return None
    hour, minute = int(m.group(1)), int(m.group(2))
    if hour > 23 or minute > 59:
        return None
    tm = time.localtime(now)
    candidate = time.mktime((*tm[:3], hour, minute, 0, *tm[6:]))
    if candidate <= now:
        candidate += 86400
    return int(candidate)


def _schedule_add(args) -> int:
    sid = args.target
    if not (args.schedule_task and args.schedule_task.strip()):
        resp = JsonResponse(
            session=sid,
            status="usage",
            error='schedule requires --task "text"',
        )
        return _emit(
            args,
            resp,
            ExitCode.USAGE,
            'usage: bridge schedule <sid> --task "text" --every N[s|m|h|d] | --at HH:MM',
        )
    if args.every is None and args.at is None:
        resp = JsonResponse(
            session=sid,
            status="usage",
            error="schedule requires --every or --at",
        )
        return _emit(
            args,
            resp,
            ExitCode.USAGE,
            'usage: bridge schedule <sid> --task "text" --every N[s|m|h|d] | --at HH:MM',
        )
    now = int(time.time())
    if args.every is not None:
        every = _parse_every(args.every)
        if every is None:
            resp = JsonResponse(
                session=sid,
                status="usage",
                error=f"invalid --every interval: {args.every!r}",
            )
            return _emit(
                args,
                resp,
                ExitCode.USAGE,
                f"usage: invalid --every {args.every!r} (expect N[s|m|h|d])",
            )
        next_run_at = now + every
    else:
        next_run_at = _parse_at(args.at, now)
        if next_run_at is None:
            resp = JsonResponse(
                session=sid,
                status="usage",
                error=f"invalid --at time: {args.at!r}",
            )
            return _emit(
                args,
                resp,
                ExitCode.USAGE,
                f"usage: invalid --at {args.at!r} (expect HH:MM local)",
            )
        every = 86400
    schedule_id = db.add_schedule(
        sid,
        args.schedule_task,
        every,
        next_run_at,
        catchup=args.catchup,
        path=args.db,
    )
    resp = JsonResponse(
        session=sid,
        status="scheduled",
        data={
            "schedule_id": schedule_id,
            "every_seconds": every,
            "next_run_at": next_run_at,
            "catchup": bool(args.catchup),
        },
    )
    human = (
        f"schedule {schedule_id} for {sid}: every {every}s,"
        f" next run {time.strftime('%Y-%m-%d %H:%M', time.localtime(next_run_at))}"
    )
    if args.catchup:
        human += " (catchup)"
    return _emit(args, resp, ExitCode.OK, human)


def _schedule_view(row) -> dict:
    """One schedules row folded into the CLI payload."""
    return {
        "id": row["id"],
        "session_id": row["session_id"],
        "task_text": row["task_text"],
        "every_seconds": row["every_seconds"],
        "next_run_at": row["next_run_at"],
        "last_run_at": row["last_run_at"],
        "catchup": bool(row["catchup"]),
        "enabled": bool(row["enabled"]),
        "created_at": row["created_at"],
    }


def _schedule_list(args) -> int:
    rows = db.list_schedules(session_id=args.session, path=args.db)
    items = [_schedule_view(r) for r in rows]
    resp = JsonResponse(status="ok", data={"schedules": items})
    if not items:
        return _emit(args, resp, ExitCode.OK, "(no schedules)")
    lines = []
    for i in items:
        nxt = time.strftime("%Y-%m-%d %H:%M", time.localtime(i["next_run_at"]))
        last = (
            time.strftime("%Y-%m-%d %H:%M", time.localtime(i["last_run_at"]))
            if i["last_run_at"] is not None
            else "-"
        )
        lines.append(
            f"schedule {i['id']} (session {i['session_id']}): every {i['every_seconds']}s"
            f" | next {nxt} | last {last}"
            f" | catchup {'on' if i['catchup'] else 'off'}"
            f" | {'enabled' if i['enabled'] else 'disabled'}"
            f" | {i['task_text']}"
        )
    return _emit(args, resp, ExitCode.OK, "\n".join(lines))


def _schedule_clear(args) -> int:
    """Disable a schedule (the row stays - fire history is kept)."""
    if args.schedule_id is None:
        resp = JsonResponse(status="usage", error="schedule clear requires an id")
        return _emit(args, resp, ExitCode.USAGE, "usage: bridge schedule clear <id>")
    if db.disable_schedule(args.schedule_id, path=args.db):
        resp = JsonResponse(
            status="disabled",
            data={"schedule_id": args.schedule_id},
        )
        return _emit(args, resp, ExitCode.OK, f"schedule {args.schedule_id} disabled")
    row = next(
        (r for r in db.list_schedules(path=args.db) if r["id"] == args.schedule_id),
        None,
    )
    if row is None:
        resp = JsonResponse(
            status="not_found",
            error=f"schedule {args.schedule_id} not found",
        )
        return _emit(
            args,
            resp,
            ExitCode.NOT_FOUND,
            f"not found: schedule {args.schedule_id}",
        )
    resp = JsonResponse(
        status="disabled",
        error=f"schedule {args.schedule_id} is already disabled",
        agent_instruction="do not retry: the schedule is already disabled",
    )
    return _emit(
        args,
        resp,
        ExitCode.CONFLICT,
        f"conflict: schedule {args.schedule_id} already disabled",
    )


def _cmd_schedule(args) -> int:
    if args.target == "list":
        return _schedule_list(args)
    if args.target == "clear":
        return _schedule_clear(args)
    return _schedule_add(args)


# --- daemon: external supervisor (item_13d) --------------------------------------

#: grace period for SIGTERM before ``daemon stop`` reports a wedged daemon
DAEMON_STOP_TIMEOUT_SECONDS = 5.0


def _state_dir(args) -> str:
    base = args.db if args.db else db.DB_PATH
    return os.path.dirname(os.path.abspath(base))


def _spawn_daemon(args) -> subprocess.Popen:
    """Detach the supervisor child (``bridge daemon run``).

    Mirrors ``_spawn_worker``: PYTHONPATH carries a source checkout,
    stderr lands in ``<state_dir>/logs/daemon.log`` so the detached
    process stays diagnosable. The child itself takes daemon.lock.
    """
    argv = [sys.executable, "-m", "session_bridge.cli.bridge"]
    if args.db:
        argv += ["--db", args.db]
    if args.queue_dir:
        argv += ["--queue-dir", args.queue_dir]
    argv += ["daemon", "run"]
    if getattr(args, "interval", None):
        argv += ["--interval", str(args.interval)]
    env = dict(os.environ)
    src = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    prev = env.get("PYTHONPATH")
    env["PYTHONPATH"] = f"{src}{os.pathsep}{prev}" if prev else src
    stderr: object = subprocess.DEVNULL
    log_fh = None
    try:
        os.makedirs(_logs_dir(args), exist_ok=True)
        log_fh = open(os.path.join(_logs_dir(args), "daemon.log"), "ab")
        stderr = log_fh
    except OSError as e:
        _log(f"cannot open daemon log: {e}")
    try:
        proc = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=stderr,
            start_new_session=True,
            close_fds=True,
            env=env,
        )
    finally:
        if log_fh is not None:
            log_fh.close()  # the child keeps its own dup of the fd
    return proc


def _daemon_start(args) -> int:
    sdir = _state_dir(args)
    if daemon.daemon_running(sdir):
        resp = JsonResponse(
            status="running",
            error="daemon already running (daemon.lock held)",
            agent_instruction="do not retry: a daemon already holds daemon.lock",
            data={"pid": daemon.daemon_pid(sdir), "state_dir": sdir},
        )
        return _emit(args, resp, ExitCode.CONFLICT, "daemon already running")
    try:
        proc = _spawn_daemon(args)
    except OSError as e:
        resp = JsonResponse(status="internal", error=f"daemon spawn failed: {e}")
        return _emit(args, resp, ExitCode.INTERNAL, f"error: daemon spawn failed: {e}")
    resp = JsonResponse(
        status="started",
        queue_depth=_queue_depth(args),
        data={"pid": proc.pid, "state_dir": sdir},
    )
    return _emit(args, resp, ExitCode.OK, f"daemon started (pid {proc.pid})")


def _sigterm_daemon(sdir) -> str:
    """SIGTERM the running daemon, wait for the lock to release.

    Result word for reports - shared by ``bridge daemon stop`` and
    ``bridge halt --all --stop-daemon`` (task #180): "stopped",
    "not_running", "pid_unreadable", "sigterm_failed:<err>" or
    "timeout".
    """
    if not daemon.daemon_running(sdir):
        return "not_running"
    pid = daemon.daemon_pid(sdir)
    if pid is None:
        return "pid_unreadable"
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass  # already gone; the lock release check below reports it
    except OSError as e:
        return f"sigterm_failed:{e}"
    deadline = time.time() + DAEMON_STOP_TIMEOUT_SECONDS
    while time.time() < deadline:
        if not daemon.daemon_running(sdir):
            return "stopped"
        time.sleep(0.1)
    return "timeout"


def _daemon_stop(args) -> int:
    sdir = _state_dir(args)
    pid = daemon.daemon_pid(sdir)
    result = _sigterm_daemon(sdir)
    if result == "not_running":
        resp = JsonResponse(status="not_running", data={"state_dir": sdir})
        return _emit(args, resp, ExitCode.OK, "daemon not running")
    if result == "pid_unreadable":
        resp = JsonResponse(
            status="running",
            error="daemon running but its pid is unreadable in daemon.lock",
        )
        return _emit(args, resp, ExitCode.INTERNAL, "error: daemon running but pid unreadable")
    if result == "stopped":
        resp = JsonResponse(status="stopped", data={"pid": pid, "state_dir": sdir})
        return _emit(args, resp, ExitCode.OK, f"daemon stopped (pid {pid})")
    if result.startswith("sigterm_failed:"):
        resp = JsonResponse(status="running", error=f"SIGTERM failed: {result[15:]}")
        return _emit(args, resp, ExitCode.INTERNAL, f"error: SIGTERM failed: {result[15:]}")
    resp = JsonResponse(
        status="running",
        error=f"daemon pid did not exit within {DAEMON_STOP_TIMEOUT_SECONDS}s",
    )
    return _emit(args, resp, ExitCode.INTERNAL, "error: daemon did not stop")


def _daemon_status(args) -> int:
    sdir = _state_dir(args)
    running = daemon.daemon_running(sdir)
    pid = daemon.daemon_pid(sdir)
    age = daemon.heartbeat_age(sdir)
    resp = JsonResponse(
        status="running" if running else "stopped",
        data={
            "pid": pid,
            "heartbeat_age_seconds": age,
            "state_dir": sdir,
        },
    )
    human = (
        f"daemon {'running' if running else 'stopped'}"
        f" (pid {pid if pid is not None else '-'},"
        f" heartbeat age {age if age is not None else '-'}s)"
    )
    return _emit(args, resp, ExitCode.OK, human)


def _daemon_once(args) -> int:
    rc = daemon.run(args.db, args.queue_dir, once=True)
    if rc == daemon.RUN_LOCK_HELD:
        resp = JsonResponse(
            status="conflict",
            error="daemon.lock is held; a daemon is already running",
        )
        return _emit(args, resp, ExitCode.CONFLICT, "conflict: daemon already running")
    resp = JsonResponse(status="ok", queue_depth=_queue_depth(args))
    return _emit(args, resp, ExitCode.OK, "daemon pass complete")


def _daemon_run(args) -> int:
    rc = daemon.run(args.db, args.queue_dir, once=False, interval=args.interval)
    if rc == daemon.RUN_LOCK_HELD:
        resp = JsonResponse(
            status="conflict",
            error="daemon.lock is held; a daemon is already running",
        )
        return _emit(args, resp, ExitCode.CONFLICT, "conflict: daemon already running")
    return _emit(args, JsonResponse(status="stopped"), ExitCode.OK, "daemon stopped")


def _cmd_daemon(args) -> int:
    action = args.daemon_action
    if action == "start":
        return _daemon_start(args)
    if action == "stop":
        return _daemon_stop(args)
    if action == "status":
        return _daemon_status(args)
    if action == "once":
        return _daemon_once(args)
    if action == "run":
        return _daemon_run(args)
    resp = JsonResponse(status="usage", error=f"unknown daemon action: {action}")
    return _emit(args, resp, ExitCode.USAGE, f"usage: unknown daemon action {action}")


# --- entry point -----------------------------------------------------------------


def _add_json(sp: argparse.ArgumentParser) -> None:
    sp.add_argument("--json", action="store_true", help="emit the JsonResponse envelope")


def _version_string() -> str:
    """True deployed version (item_102a).

    Editable installs carry stale dist-info (`pip show` said 0.2.7
    while the checkout sat at v0.2.8), so when running out of a
    ``src/`` tree the version is read from the repo's pyproject.toml;
    packaged installs fall back to importlib.metadata. The source
    path is always appended so the report is self-describing.
    """
    here = Path(__file__).resolve()
    # src/session_bridge/cli/bridge.py -> repo root; guard the walk so
    # an odd layout can never break parser construction for every command
    pyproject = here.parents[3] / "pyproject.toml" if len(here.parents) > 3 else None
    if pyproject is not None and "src" in here.parts and pyproject.is_file():
        try:
            in_project = False
            for line in pyproject.read_text().splitlines():
                line = line.strip()
                if line.startswith("["):
                    in_project = line == "[project]"
                    continue
                if in_project:
                    m = re.fullmatch(r'version\s*=\s*"([^"]+)"', line)
                    if m:
                        return f"{m.group(1)} (editable, src: {pyproject.parent})"
        except OSError:
            pass
    try:
        ver = importlib.metadata.version("session-bridge")
    except importlib.metadata.PackageNotFoundError:
        ver = "unknown"
    src_hint = here.parents[2] if len(here.parents) > 2 else here
    return f"{ver} (src: {src_hint})"


def _cmd_version(args) -> int:
    ver = _version_string()
    return _emit(
        args,
        JsonResponse(status="ok", data={"version": ver}),
        ExitCode.OK,
        human=f"session-bridge {ver}",
    )


def _parse_args(argv) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="bridge",
        description="sessions-bridge task dispatcher",
    )
    p.add_argument(
        "--db",
        default=os.environ.get("SESSIONS_BRIDGE_DB"),
        help="path to state.db (default: SESSIONS_BRIDGE_DB or XDG path)",
    )
    p.add_argument(
        "--queue-dir",
        dest="queue_dir",
        default=os.environ.get("SESSIONS_BRIDGE_QUEUE_DIR"),
        help="trigger directory (default: <state.db dir>/queue)",
    )
    p.add_argument(
        "--version",
        action="version",
        version=f"session-bridge {_version_string()}",
        help="show the true deployed version and exit",
    )
    _add_json(p)
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("version", help="show the true deployed version")
    _add_json(sp)

    sp = sub.add_parser(Command.PUSH.value, help="enqueue a task")
    sp.add_argument("session_id")
    group = sp.add_mutually_exclusive_group(required=True)
    group.add_argument("--task", help="task text")
    group.add_argument("--file", dest="task_file", help="spec file path")
    _add_json(sp)

    sp = sub.add_parser(Command.STATUS.value, help="session overview")
    _add_json(sp)

    sp = sub.add_parser(Command.INSPECT.value, help="full task detail by session or id")
    sp.add_argument("ref", help="session id or task id")
    _add_json(sp)

    sp = sub.add_parser(Command.CLEAR.value, help="drop a queued task")
    sp.add_argument("session")
    _add_json(sp)

    sp = sub.add_parser(Command.UNPARK.value, help="resume a paused/awaiting session")
    sp.add_argument("session")
    _add_json(sp)

    sp = sub.add_parser(Command.RETRY.value, help="requeue a failed task")
    sp.add_argument("ref", help="session id or task id")
    _add_json(sp)

    # managed mode (item_13c); the Command enum in cli/contract.py is
    # frozen, so the subcommand names are literals here
    sp = sub.add_parser("arm", help="enroll a session into managed mode")
    sp.add_argument("session_id")
    _add_json(sp)

    sp = sub.add_parser(
        "wake", help="acp-host wake nudge: prompt an idle session to drain its mailbox"
    )
    sp.add_argument("session_id")
    sp.add_argument("--prompt", help="override the default drain-nudge text", default=None)
    _add_json(sp)

    sp = sub.add_parser("disarm", help="leave managed mode (--all for the fleet)")
    sp.add_argument("session_id", nargs="?", default=None)
    sp.add_argument(
        "--all",
        action="store_true",
        help="disarm every managed session (big red button)",
    )
    _add_json(sp)

    # fleet stop (task #180): HALT parks managed sessions until RESUME
    sp = sub.add_parser(
        "halt",
        help="graceful fleet stop: park managed sessions until resume",
    )
    sp.add_argument("session_id", nargs="?", default=None)
    sp.add_argument(
        "--all",
        action="store_true",
        help="halt every managed+active session (fleet stop)",
    )
    sp.add_argument(
        "--stop-daemon",
        action="store_true",
        help="SIGTERM the supervisor daemon after the broadcast",
    )
    sp.add_argument("--reason", default=None, help="optional note appended to the HALT body")
    _add_json(sp)

    sp = sub.add_parser("resume", help="lift a fleet halt (--all for the fleet)")
    sp.add_argument("session_id", nargs="?", default=None)
    sp.add_argument(
        "--all",
        action="store_true",
        help="resume every managed+active session",
    )
    sp.add_argument("--reason", default=None, help="optional note appended to the RESUME body")
    _add_json(sp)

    sp = sub.add_parser(
        "unregister",
        help="remove a dead/ghost registration and all its state",
    )
    sp.add_argument("session_id")
    sp.add_argument(
        "--force",
        action="store_true",
        help="skip the heartbeat and acp-host liveness refusals",
    )
    _add_json(sp)

    # supervisor daemon (item_13d); literal command name like
    # arm/disarm - the Command enum in cli/contract.py is frozen
    sp = sub.add_parser(
        "daemon",
        help="supervisor daemon: outbox relay + liveness sweep",
    )
    dsub = sp.add_subparsers(dest="daemon_action", required=True)
    sp_start = dsub.add_parser("start", help="spawn the detached daemon")
    sp_start.add_argument(
        "--interval",
        type=float,
        default=None,
        help="seconds between passes (default: env or 5)",
    )
    _add_json(sp_start)
    sp_stop = dsub.add_parser("stop", help="SIGTERM the running daemon")
    _add_json(sp_stop)
    sp_status = dsub.add_parser("status", help="daemon liveness + heartbeat age")
    _add_json(sp_status)
    sp_once = dsub.add_parser("once", help="one relay+sweep pass, then exit")
    _add_json(sp_once)
    sp_run = dsub.add_parser(
        "run",
        help="internal: foreground supervisor loop (used by 'daemon start')",
    )
    sp_run.add_argument("--interval", type=float, default=None)
    _add_json(sp_run)
    _add_json(sp)

    # orchestration bus commands (item_13e1); literal names like
    # arm/disarm - the Command enum in cli/contract.py is frozen
    sp = sub.add_parser("register", help="register a session on the bus")
    sp.add_argument("session_id")
    sp.add_argument(
        "--role",
        choices=("worker", "orchestrator"),
        default="worker",
        help="bus role (orchestrator is a CAS singleton)",
    )
    sp.add_argument(
        "--workspace",
        default=None,
        help="session working directory (default: current directory)",
    )
    sp.add_argument(
        "--force",
        action="store_true",
        help="take over the orchestrator role regardless of the holder's heartbeat",
    )
    _add_json(sp)

    sp = sub.add_parser("ask", help="post a worker question to the orchestrator")
    sp.add_argument("--session", required=True, help="asking session id")
    sp.add_argument("--task", type=int, required=True, help="task id the question is about")
    sp.add_argument("text", help="question text")
    _add_json(sp)

    sp = sub.add_parser(
        "ack",
        help="ack mailbox notifications (restart watermark)",
    )
    sp.add_argument("--session", required=True, help="consuming session id")
    sp.add_argument(
        "--up-to",
        dest="up_to",
        type=int,
        default=None,
        help="ack only notifications up to this id (default: all unacked)",
    )
    _add_json(sp)

    # liveness metronome + orchestrator question queue (item_13g);
    # literal names like arm/disarm - the Command enum is frozen
    sp = sub.add_parser(
        "heartbeat",
        help="refresh the session heartbeat (light metronome, no register CAS)",
    )
    sp.add_argument("session_id")
    _add_json(sp)

    # managed task lifecycle (item_18): claim/finish verbs - the
    # mailbox had no way to transition a delivered task before
    sp = sub.add_parser(
        "claim",
        help="claim your queued managed task (queued > running, idempotent renew)",
    )
    sp.add_argument("task_id", type=int)
    sp.add_argument("--session", required=True, help="claiming session id")
    _add_json(sp)

    sp = sub.add_parser(
        "finish",
        help="finish your running managed task (running > done|failed)",
    )
    sp.add_argument("task_id", type=int)
    sp.add_argument("--session", required=True, help="finishing session id")
    sp.add_argument("--fail", action="store_true", help="mark failed instead of done")
    sp.add_argument("--note", default=None, help="optional completion note")
    _add_json(sp)

    # item_22: declare an external wait - one LEASE_SECONDS renewal per
    # call, CAS-bound to the claimer's lease_owner
    sp = sub.add_parser(
        "wait",
        help="renew your running task's lease while blocked externally",
    )
    sp.add_argument("task_id", type=int)
    sp.add_argument("--session", required=True, help="waiting session id")
    sp.add_argument(
        "--reason",
        default=None,
        help="what you are waiting on (lands in the task note)",
    )
    _add_json(sp)

    # item_139 amendment: the registered directive-push path - writes a
    # notification row so the daemon relays it to the fs mailbox AND
    # wakes the session (hand-written msg_*.txt files bypass the
    # registry and used to never wake anyone)
    sp = sub.add_parser(
        "directive",
        help="send a registered directive notification to a session",
    )
    sp.add_argument("session_id")
    sp.add_argument("text", help="directive text")
    _add_json(sp)

    # question_answer vs directive semantics backlog item: worker
    # reports rode DIRECTIVE (obey) - NOTICE carries read-and-
    # incorporate reports through the same delivery machinery
    sp = sub.add_parser(
        "notice",
        help="send a registered informational notice to a session",
    )
    sp.add_argument("session_id")
    sp.add_argument("text", help="notice text")
    _add_json(sp)

    # task #207 item g: durable append-only decision log - decisions
    # that used to live in session memory only (contracts,
    # PLAN_DELTAs, review verdicts) survive a session restart here
    sp = sub.add_parser(
        "event",
        help="append a durable decision event ('-' sid = bus-level)",
    )
    sp.add_argument("session_id", help="deciding session id, or '-' for bus-level")
    sp.add_argument("event_kind", help="kind slug: contract, plan_delta, review_verdict, ...")
    sp.add_argument("text", help="decision body")
    sp.add_argument("--task", type=int, default=None, help="optional task id ref")
    _add_json(sp)

    sp = sub.add_parser(
        "events",
        help="read the durable decision log (latest first)",
    )
    sp.add_argument("--session", default=None, help="filter to one session")
    sp.add_argument("--kind", default=None, help="filter to one kind slug")
    sp.add_argument("--last", type=int, default=50, help="page size (default 50)")
    _add_json(sp)

    # task #208 (directive 426): host-level resource registry - the
    # machine-readable reservation layer self-sufficient projects
    # call as a deploy pre-flight; the (type, id) PRIMARY KEY is the
    # mutex, conflicts name the incumbent owner, and genuinely
    # contested resources escalate to orchestrator arbitration only
    sp = sub.add_parser(
        "reserve",
        help="atomically reserve a host resource for a project",
    )
    sp.add_argument("resource_type", help=f"one of: {', '.join(sorted(db.ALLOCATION_TYPES))}")
    sp.add_argument("resource_id", help="port, unit prefix, CIDR, netns handle, ...")
    sp.add_argument("--project", required=True, help="owning project id")
    sp.add_argument("--purpose", default=None, help="free-text purpose")
    sp.add_argument(
        "--fwd",
        default=None,
        metavar="SPEC",
        help="tcp_port only: forward spec the ingress layer materializes "
        "(tcp:<host>:<port> | ns-pid:<pid>:<port> | ns-pidfile:<path>:<port>)",
    )
    sp.add_argument(
        "--bind",
        default=None,
        help="with --fwd: host listen address (default 0.0.0.0)",
    )
    sp.add_argument(
        "--session",
        default=None,
        help="reserving session id (audit trail; optional)",
    )
    sp.add_argument(
        "--ttl",
        type=int,
        default=None,
        help="seconds until lazy-expiry (default: held until released)",
    )
    _add_json(sp)

    sp = sub.add_parser(
        "release",
        help="release a reserved host resource (owner-only unless --force)",
    )
    sp.add_argument("resource_type")
    sp.add_argument("resource_id")
    sp.add_argument("--project", required=True, help="owning project id")
    sp.add_argument(
        "--force",
        action="store_true",
        help="override ownership - orchestrator arbitration only",
    )
    _add_json(sp)

    sp = sub.add_parser(
        "allocations",
        help="list live host-resource allocations",
    )
    sp.add_argument("--type", default=None, help="filter to one resource_type")
    sp.add_argument("--project", default=None, help="filter to one project")
    _add_json(sp)

    # task #209 (issue #92): neutral host ingress - materializes
    # port forwarders from tcp_port rows carrying a fwd= spec so
    # `bridge reserve` alone publishes a listener and `release`
    # removes it. `sync` is the reconcile pass (host-ingress.timer
    # runs it periodically, deploy scripts may call it directly for
    # zero-wait publication); `status` is the read-only view.
    sp = sub.add_parser(
        "ingress",
        help="host ingress layer: sync | status",
    )
    isub = sp.add_subparsers(dest="ingress_action", required=True)
    sp_sync = isub.add_parser("sync", help="reconcile forwarders with the registry")
    _add_json(sp_sync)
    sp_status = isub.add_parser("status", help="show desired vs running forwards")
    _add_json(sp_status)

    sp = sub.add_parser(
        "questions",
        help="orchestrator question queue: list / claim / answer",
    )
    qsub = sp.add_subparsers(dest="questions_action", required=True)
    sp_list = qsub.add_parser("list", help="list pending questions")
    sp_list.add_argument("--session", default=None, help="filter to one asking session")
    sp_list.add_argument("--task", type=int, default=None, help="filter to one task id")
    _add_json(sp_list)
    sp_claim = qsub.add_parser("claim", help="claim a pending question")
    sp_claim.add_argument("question_id", type=int)
    sp_claim.add_argument("--claimant", required=True, help="claiming session id")
    _add_json(sp_claim)
    sp_answer = qsub.add_parser(
        "answer",
        help="answer a claimed question (claimant-bound CAS)",
    )
    sp_answer.add_argument("question_id", type=int)
    sp_answer.add_argument("--claimant", required=True, help="claimant binding for the answer CAS")
    sp_answer.add_argument("text", help="answer text")
    _add_json(sp_answer)
    _add_json(sp)

    sp = sub.add_parser(
        "escalate",
        help="human escalation queue: show / reply",
    )
    esub = sp.add_subparsers(dest="escalate_action", required=True)
    sp_show = esub.add_parser("show", help="list unresolved escalations")
    _add_json(sp_show)
    sp_reply = esub.add_parser("reply", help="resolve an escalation")
    sp_reply.add_argument("audit_id", type=int, help="audit_events id of the escalation")
    sp_reply.add_argument(
        "directive",
        help="A | B | defer | abort | override:<text>",
    )
    _add_json(sp_reply)
    _add_json(sp)

    # recurring schedules (item_14); literal name like arm/disarm -
    # the Command enum in cli/contract.py is frozen. The positional
    # doubles as the subcommand: a session id adds a schedule, the
    # reserved words 'list' and 'clear' run those actions.
    sp = sub.add_parser(
        "schedule",
        help="recurring schedules: <sid> --task ... | list | clear <id>",
    )
    sp.add_argument(
        "target",
        help="session id for a new schedule, or 'list' / 'clear'",
    )
    sp.add_argument(
        "schedule_id",
        nargs="?",
        type=int,
        default=None,
        help="schedule id (clear only)",
    )
    sp.add_argument(
        "--task",
        dest="schedule_task",
        default=None,
        help="task text the schedule dispatches",
    )
    group = sp.add_mutually_exclusive_group()
    group.add_argument(
        "--every",
        default=None,
        metavar="N[s|m|h|d]",
        help="repeat interval (bare number = seconds)",
    )
    group.add_argument(
        "--at",
        default=None,
        metavar="HH:MM",
        help="daily local time (every=86400, next occurrence anchors)",
    )
    sp.add_argument(
        "--catchup",
        action="store_true",
        help="after downtime: fire once and re-catch the grid (default: skip missed)",
    )
    sp.add_argument(
        "--session",
        default=None,
        help="list filter: only this session's schedules",
    )
    _add_json(sp)

    sp = sub.add_parser(
        Command.WORK.value,
        help=argparse.SUPPRESS,  # internal worker
    )
    sp.add_argument("task_id", type=int)
    sp.add_argument("--owner", default=None)
    sp.add_argument("--timeout", type=int, default=None)
    _add_json(sp)

    return p.parse_args(argv)


_HANDLERS = {
    Command.PUSH.value: _cmd_push,
    Command.STATUS.value: _cmd_status,
    Command.INSPECT.value: _cmd_inspect,
    Command.CLEAR.value: _cmd_clear,
    Command.UNPARK.value: _cmd_unpark,
    Command.RETRY.value: _cmd_retry,
    Command.WORK.value: _cmd_work,
    "arm": _cmd_arm,
    "wake": _cmd_wake,
    "wait": _cmd_wait,
    "disarm": _cmd_disarm,
    "halt": _cmd_halt,
    "resume": _cmd_resume,
    "unregister": _cmd_unregister,
    "daemon": _cmd_daemon,
    "register": _cmd_register,
    "ask": _cmd_ask,
    "ack": _cmd_ack,
    "directive": _cmd_directive,
    "notice": _cmd_notice,
    "allocations": _cmd_allocations,
    "event": _cmd_event,
    "release": _cmd_release,
    "reserve": _cmd_reserve,
    "ingress": _cmd_ingress,
    "events": _cmd_events,
    "claim": _cmd_claim,
    "finish": _cmd_finish,
    "heartbeat": _cmd_heartbeat,
    "questions": _cmd_questions,
    "escalate": _cmd_escalate,
    "schedule": _cmd_schedule,
    "version": _cmd_version,
}


def main(argv=None) -> int:
    args = _parse_args(argv)
    if args.command == "version":
        # pure diagnostic: no state db init - a broken db must never
        # block the deployed-version report (PR54 r1 warning)
        return _cmd_version(args)
    try:
        db.init(args.db)
    except Exception as e:
        return _emit(
            args,
            JsonResponse(status="internal", error=f"state.db init failed: {e}"),
            ExitCode.INTERNAL,
        )
    try:
        return _HANDLERS[args.command](args)
    except Exception as e:
        return _emit(
            args,
            JsonResponse(status="internal", error=f"{type(e).__name__}: {e}"),
            ExitCode.INTERNAL,
        )


if __name__ == "__main__":
    sys.exit(main())

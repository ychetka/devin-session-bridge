"""Dispatch machinery for the bridge CLI (task #210 split).

``pump`` is the lazy dispatcher every mutating command runs: lease
recovery first, then dispatch of queued tasks by transport policy
(hook-queue for locked sessions, acp-push worker children for free
ones). Also owns the ``_work`` worker cycle, worker log retention and
the concurrency semaphore.
"""

import os
import subprocess
import sys
import threading
import time

from session_bridge.acp.client import AcpClient
from session_bridge.acp.contract import AcpError
from session_bridge.cli.common import (
    ACP_OWNER_PREFIX,
    DISPATCH_OWNER_PREFIX,
    _emit,
    _env_int,
    _gc_dead_sessions,
    _has_open_dispatch,
    _log,
    _managed_owner,
    _notify,
    _queue_dir,
    _session_cwd,
    _session_known,
    _session_mode,
    _write_trigger,
)
from session_bridge.cli.contract import (
    Command,
    Event,
    EventKind,
    ExitCode,
    JsonResponse,
)
from session_bridge.state import db
from session_bridge.state.contract import (
    NotificationKind,
    SessionMode,
    TaskStatus,
    Transport,
)

#: concurrent acp-push sessions cap; queued tasks beyond it wait.
SEMAPHOR = 3

#: default end_turn wait for ``session/prompt`` in the worker.
WORK_TIMEOUT_DEFAULT = 3600

#: lease renewal cadence inside a running prompt wait.
LEASE_RENEW_INTERVAL = max(60, db.LEASE_SECONDS // 3)

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


def _work_timeout(args) -> int:
    return getattr(args, "timeout", None) or _env_int(
        "SESSIONS_BRIDGE_WORK_TIMEOUT", WORK_TIMEOUT_DEFAULT
    )


#: per-invocation worker log: <state_dir>/logs/worker_<tid>_<attempt>_<ts>.log
WORKER_LOG_MAX_BYTES = 1_000_000  # oversized logs keep their tail

WORKER_LOG_KEEP = 10  # retention: newest files survive the pump sweep

#: retention never unlinks a log touched inside one lease window: a
#: file that fresh can still belong to a live, writing worker
WORKER_LOG_MIN_AGE_SECONDS = db.LEASE_SECONDS

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

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
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

from session_bridge.acp.client import AcpClient
from session_bridge.acp.contract import AcpError
from session_bridge.cli.contract import (
    Command,
    Event,
    EventKind,
    ExitCode,
    JsonResponse,
)
from session_bridge.state import db
from session_bridge.state.contract import (
    LEASE_SECONDS,
    TaskStatus,
    Transport,
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


def pump(args) -> dict:
    """Lazy recovery + one dispatch pass over queued tasks.

    Returns ``{"events", "hooked", "spawned", "unknown", "saturated"}``
    for the calling command's response. The queue is scanned in capped
    batches paginated by task id, so every queued task is visited even
    beyond the ``list_tasks`` limit; hook-folded tasks land back in
    ``queued`` keeping the same id - already below ``last_id``, so
    they are never revisited in the same pass.
    """
    events: list[Event] = []
    hooked: list[int] = []
    spawned: list[int] = []
    unknown: list[str] = []
    saturated = False

    _sweep_worker_logs(args)

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
            locked, _pid = db.is_locked(sid)
            if locked:
                # transport decision: the session is open in devin, so
                # the parked Stop hook delivers it - claim + fold back
                # to queued with the hook transport, then wake the hook
                owner = f"{DISPATCH_OWNER_PREFIX}{os.getpid()}-{tid}"
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
            owner = f"{ACP_OWNER_PREFIX}{os.getpid()}-{tid}"
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


def _work_task(task_id: int, owner: str, args) -> JsonResponse:
    """One acp push cycle for a claimed task."""
    task = db.get_task(task_id, path=args.db)
    if task is None:
        return JsonResponse(status="not_found", error=f"task {task_id} not found")
    sid = task["session_id"]
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
    elif task["transport"] == Transport.HOOK_QUEUE.value:
        instruction = "do not retry: task queued for hook delivery inside the open session"
    elif task["status"] == TaskStatus.RUNNING.value:
        instruction = "do not retry: acp worker dispatched"
    resp = JsonResponse(
        session=args.session_id,
        status=task["status"],
        queue_depth=_queue_depth(args),
        events=events,
        agent_instruction=instruction,
        error=error,
        data={"task_id": tid, "transport": task["transport"]},
    )
    human = f"task {tid} {task['status']} for {args.session_id}"
    if task["transport"]:
        human += f" via {task['transport']}"
    if error:
        human += f" ({error})"
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


def _cmd_status(args) -> int:
    result = pump(args)
    views, events, queue_shown = _session_views(args)
    seen = {(e.kind, e.task_id) for e in result["events"]}
    events = result["events"] + [e for e in events if (e.kind, e.task_id) not in seen]
    queue_depth = _queue_depth(args)
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
    }
    if result["saturated"]:
        data["saturation_reason"] = "MAX_CONCURRENT_REACHED"
    resp = JsonResponse(
        status="ok",
        queue_depth=queue_depth,
        events=events,
        data=data,
    )
    lines = ["SESSION | REPO | TRANSPORT | STATE | QUEUE | ACTION"]
    for v in views:
        lines.append(
            f"{v['session']} | {v['repo']} | {v['transport'] or '-'}"
            f" | {v['state']} | {v['queue']} | {v['action']}"
        )
        if v["stderr_tail"]:
            lines.append(f"    log: {v['log_path']}")
            for tail_line in v["stderr_tail"].splitlines():
                lines.append(f"    stderr: {tail_line}")
    if len(lines) == 1:
        lines.append("(no sessions)")
    return _emit(args, resp, ExitCode.OK, "\n".join(lines))


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
        f"  next:      {next_action}"
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


# --- entry point -----------------------------------------------------------------


def _add_json(sp: argparse.ArgumentParser) -> None:
    sp.add_argument("--json", action="store_true", help="emit the JsonResponse envelope")


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
    _add_json(p)
    sub = p.add_subparsers(dest="command", required=True)

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
}


def main(argv=None) -> int:
    args = _parse_args(argv)
    if args.command == Command.WORK.value and not args.owner:
        args.owner = f"{ACP_OWNER_PREFIX}{os.getpid()}"
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

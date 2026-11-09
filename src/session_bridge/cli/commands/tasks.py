"""Task domain commands (task #210 split).

push / inspect / clear / unpark / retry / claim / finish / wait.
"""

import time

from session_bridge.cli.common import (
    _emit,
    _gc_dead_sessions,
    _managed_owner,
    _queue_depth,
    _queue_dir,
    _session_mode,
    _write_trigger,
)
from session_bridge.cli.contract import (
    ExitCode,
    JsonResponse,
)
from session_bridge.cli.dispatch import _LIVE_VALUES, pump
from session_bridge.cli.status import _next_action
from session_bridge.state import db
from session_bridge.state.contract import (
    SessionMode,
    SessionStatus,
    TaskStatus,
    Transport,
)


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

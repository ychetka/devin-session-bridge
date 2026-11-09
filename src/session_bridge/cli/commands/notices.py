"""Notification commands (task #210 split).

directive / notice / event / events.
"""

from session_bridge.cli.common import _emit
from session_bridge.cli.contract import (
    ExitCode,
    JsonResponse,
)
from session_bridge.state import db
from session_bridge.state.contract import (
    NotificationKind,
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

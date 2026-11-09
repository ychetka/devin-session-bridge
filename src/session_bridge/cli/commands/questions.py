"""Question + escalation commands (task #210 split).

ask / questions list|claim|answer|reply / escalate.
"""

import json

from session_bridge.cli.common import _emit
from session_bridge.cli.contract import (
    ExitCode,
    JsonResponse,
)
from session_bridge.state import db
from session_bridge.state.contract import (
    MAX_QUESTIONS_PER_TASK,
    AuditEventKind,
    NotificationKind,
    QuestionState,
    TaskStatus,
)


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

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
import os
import sys

from session_bridge.cli.commands.allocations import (
    _cmd_allocations,
    _cmd_ingress,
    _cmd_release,
    _cmd_reserve,
)
from session_bridge.cli.commands.metrics import _cmd_fleet_metrics
from session_bridge.cli.commands.notices import (
    _cmd_directive,
    _cmd_event,
    _cmd_events,
    _cmd_notice,
)
from session_bridge.cli.commands.ops import (
    _cmd_daemon,
    _cmd_schedule,
    _cmd_version,
    _version_string,
)

# Compatibility re-exports (task #210 split): read-only seams tests
# and callers address through the bridge module. Patch seams
# (monkeypatch targets) live in their home modules - patch there.
from session_bridge.cli.commands.questions import (  # noqa: F401
    _ESCALATE_CLAIMANT,
    _cmd_ask,
    _cmd_escalate,
    _cmd_questions,
)
from session_bridge.cli.commands.sessions import (
    _cmd_ack,
    _cmd_arm,
    _cmd_disarm,
    _cmd_halt,
    _cmd_heartbeat,
    _cmd_kill,
    _cmd_register,
    _cmd_resume,
    _cmd_stop,
    _cmd_unregister,
    _cmd_wake,
    _cmd_withdraw,
)
from session_bridge.cli.commands.tasks import (
    _cmd_claim,
    _cmd_clear,
    _cmd_finish,
    _cmd_inspect,
    _cmd_push,
    _cmd_quota_park,
    _cmd_quota_resume,
    _cmd_retry,
    _cmd_unpark,
    _cmd_wait,
)
from session_bridge.cli.common import (
    ACP_OWNER_PREFIX,  # noqa: F401
    _add_json,
    _emit,
)
from session_bridge.cli.contract import (
    Command,
    ExitCode,
    JsonResponse,
)
from session_bridge.cli.dispatch import (  # noqa: F401
    SEMAPHOR,
    STATE_FAILED,
    STATE_NEEDS_KICK,
    WORKER_LOG_KEEP,
    WORKER_LOG_MAX_BYTES,
    WORKER_LOG_MIN_AGE_SECONDS,
    _cmd_work,
    _logs_dir,
    _sweep_worker_logs,
    pump,
)
from session_bridge.cli.status import (  # noqa: F401
    STATUS_MAX_LINES,
    _cmd_status,
    _wake_cell,
)
from session_bridge.state import db
from session_bridge.wake import WakeError  # noqa: F401

__all__ = ["main", "pump"]


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
    sp.add_argument(
        "--force",
        action="store_true",
        help="override thrash breaker",
    )
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

    # task #210: detach a session's live bridge state without
    # unregister's liveness refusals - demote live tasks, release
    # leases, stop the heartbeat, mark the session dead.
    # issue #113: a numeric positional addresses a task instead -
    # operator force-fail of one live task
    sp = sub.add_parser(
        "kill",
        help=(
            "force-terminate a session's live state, or force-fail one"
            " live task when the argument is a numeric task id"
        ),
    )
    sp.add_argument("session_id")
    sp.add_argument(
        "--hard-stop",
        action="store_true",
        help=(
            "session kill only: run the stop layers (cancel, forced"
            " session/close, attributed sweep) BEFORE the bus teardown"
        ),
    )
    sp.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "enumerate the stop layers and sweep victims without"
            " signalling and without the bus teardown"
        ),
    )
    _add_json(sp)

    # issue #164: layered hard-stop - cancel -> forced close ->
    # (--hard) attributed orphan sweep. Session/process level only;
    # the bus row is untouched (kill owns the teardown). --dry-run
    # enumerates victims without sending or signalling anything.
    sp = sub.add_parser(
        "stop",
        help=(
            "layered session stop: session/cancel, forced session/close,"
            " and with --hard the attributed orphan sweep"
        ),
    )
    sp.add_argument("session_id")
    sp.add_argument(
        "--hard",
        action="store_true",
        help=(
            "also run the attributed orphan sweep (SIGTERM -> grace ->"
            " SIGKILL) against backend-tree processes of this session"
        ),
    )
    sp.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "enumerate layers and would-be sweep victims with reasons; sends and signals nothing"
        ),
    )
    _add_json(sp)

    # issue #142: audited orchestrator scope-pull - the benign
    # counterpart of `kill <task_id>` (which force-FAILS only
    # running/dispatch_decided); withdraw covers every
    # still-actionable state, journals actor+reason, and never
    # claims the worker process died.
    sp = sub.add_parser(
        "withdraw",
        help="withdraw a task's scope: terminal cancelled, audited",
    )
    sp.add_argument("task_id", type=int)
    sp.add_argument(
        "--actor",
        default="operator",
        help="who is pulling the scope (journaled verbatim)",
    )
    sp.add_argument(
        "--reason",
        default="",
        help="why the scope is pulled (journaled verbatim)",
    )
    sp.add_argument(
        "--stop-turn",
        action="store_true",
        help=(
            "also send session/cancel to the owning session -"
            " best-effort turn interrupt, never a process-death claim"
        ),
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

    # task #216: manual quota park + early release - the operator-side
    # of the notice/event self-report auto-park
    sp = sub.add_parser(
        "quota-park",
        help="park a live task into waiting_quota until the provider resets",
    )
    sp.add_argument("session_id")
    sp.add_argument("--task", type=int, required=True, help="task id")
    sp.add_argument(
        "--reset-at",
        required=True,
        help="quota reset time (ISO-8601 or 'HH:MM UTC')",
    )
    sp.add_argument("--note", default=None, help="error text that caused the park")
    sp.add_argument("--model", default=None, help="model/pool that is limited")
    sp.add_argument("--checkpoint", default=None, help="where to resume the step")
    _add_json(sp)

    sp = sub.add_parser(
        "quota-resume",
        help="release a waiting_quota park before its reset time",
    )
    sp.add_argument("session_id")
    sp.add_argument("--task", type=int, required=True, help="task id")
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

    # directive 446: read-only fleet report for the orchestrator /
    # operator - bridge state + each repo's cycle_log.jsonl, rendered
    # as markdown; sessions never consume it
    sp = sub.add_parser(
        "fleet-metrics",
        help="read-only markdown report: per-session/repo/orchestrator metrics",
    )
    sp.add_argument(
        "--window",
        choices=("24h", "7d"),
        default="7d",
        help="report window (default 7d)",
    )
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
    "kill": _cmd_kill,
    "stop": _cmd_stop,
    "withdraw": _cmd_withdraw,
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
    "quota-park": _cmd_quota_park,
    "quota-resume": _cmd_quota_resume,
    "heartbeat": _cmd_heartbeat,
    "questions": _cmd_questions,
    "escalate": _cmd_escalate,
    "schedule": _cmd_schedule,
    "fleet-metrics": _cmd_fleet_metrics,
    "version": _cmd_version,
}


def main(argv=None) -> int:
    args = _parse_args(argv)
    if args.command == "version":
        # pure diagnostic: no state db init - a broken db must never
        # block the deployed-version report (PR54 r1 warning)
        return _cmd_version(args)
    try:
        # fleet-metrics is a pure reader (directive 446): init() would
        # CREATE a missing state.db and run column migrations - both
        # are writes, so the report opens the file mode=ro instead and
        # serves pre-migration files via the updated_at fallback.
        if args.command != "fleet-metrics":
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

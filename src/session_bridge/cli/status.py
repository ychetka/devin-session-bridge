"""Fleet status view for the bridge CLI (task #210 split).

``bridge status`` snapshot assembly and rendering: per-session task
state, wake outcomes, attention lines and the compact fleet table.
"""

import os
import time

from session_bridge.cli.common import _emit, _notify, _queue_depth, _state_dir
from session_bridge.cli.contract import (
    Event,
    EventKind,
    ExitCode,
    JsonResponse,
)
from session_bridge.cli.dispatch import (
    _LIVE_VALUES,
    SEMAPHOR,
    STATE_DISPATCHING,
    STATE_FAILED,
    STATE_HEADLESS_IDLE,
    STATE_NEEDS_KICK,
    STATE_PARKED,
    STATE_RUNNING,
    _worker_log_tail,
    pump,
)
from session_bridge.daemon import core as daemon
from session_bridge.state import db
from session_bridge.state.contract import (
    MAX_ATTEMPTS,
    MAX_QUESTIONS_PER_TASK,
    AuditEventKind,
    NotificationKind,
    SessionMode,
    SessionRole,
    SessionStatus,
    TaskStatus,
    Transport,
)


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
    now = int(time.time())
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
                if t["status"]
                in (
                    TaskStatus.DONE.value,
                    TaskStatus.FAILED.value,
                    TaskStatus.CANCELLED.value,
                )
            ),
            None,
        )
        srow = db.get_session(sid, path=args.db)
        paused = bool(srow["queue_paused"]) if srow is not None else False

        notify_task = None
        log_path = stderr_tail = None
        if live is not None and live["step_state"] == "waiting_quota":
            # the park overrides the lifecycle word on every live
            # status - quota_park accepts any live task, not just
            # running (task #216 review)
            state = STATE_PARKED
            action = f"waiting_quota until {_quota_reset_label(live)}"
        elif live is not None and live["status"] == TaskStatus.RUNNING.value:
            if _external_wait_fresh(live["wait_until"], now):
                # `bridge wait` parked the claimer on an out-of-band
                # answer - observability only, the lifecycle stays
                # running (issue #151). The reason reads from the
                # dedicated wait_reason column - a stale lifecycle
                # note ('lease expired') can never pose as the cause.
                state = STATE_PARKED
                reason = (live["wait_reason"] or "").strip()
                action = _cut(f"external wait: {reason}" if reason else "external wait", 48)
            else:
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
                    ts=now,
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
        wait_until = live["wait_until"] if live is not None else None
        views.append(
            {
                "session": sid,
                "repo": repo,
                "transport": live["transport"] if live is not None else None,
                "state": state,
                "queue": 1 if live is not None and live["status"] == TaskStatus.QUEUED.value else 0,
                "action": action,
                "wait_until": wait_until,
                "wait_reason": (
                    live["wait_reason"]
                    if live is not None and live["wait_until"] is not None
                    else None
                ),
                "log_path": log_path,
                "stderr_tail": stderr_tail,
            }
        )
    return views, events, queue_shown


#: hard ceiling on human ``status`` lines - the contract is <25.
STATUS_MAX_LINES = 24

#: open-escalation banner rows before the banner collapses to "+N more".
STATUS_BANNER_MAX = 5

#: rolling window of the footer digest counts.
STATUS_DIGEST_SECONDS = 24 * 3600

#: task statuses that never make a session row "idle" - the fleet row
#: resolves its task cell on the latest task regardless of status, but
#: the state column only inherits non-terminal ones (done -> idle is
#: misleading: the worker finished, it is idle carrying a done task;
#: cancelled -> idle likewise, issue #142).
_IDLE_TASK_STATUSES = frozenset({TaskStatus.DONE.value, TaskStatus.CANCELLED.value})

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
    TaskStatus.CANCELLED.value: "cncl",
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


def _quota_reset_label(task) -> str:
    """Compact UTC reset stamp for a ``waiting_quota`` row, or ``?``."""
    reset = task["quota_reset_at"]
    if reset is None:
        return "?"
    return time.strftime("%H:%MZ", time.gmtime(reset))


def _external_wait_fresh(wait_until, now=None) -> bool:
    """True while a ``bridge wait`` stamp is inside the lease window.

    ``wait_until`` mirrors the lease expiry it was stamped with, so a
    stamp at or past ``now`` decays on its own - an abandoned wait
    simply stops reading as parked (issue #151).
    """
    if wait_until is None:
        return False
    if now is None:
        now = int(time.time())
    return int(wait_until) > now


def _fleet_state(session_status, task, now=None) -> str:
    """The one-word state cell for a fleet row.

    Dead sessions show ``dead`` (the compact marker - a dead row stays
    visible so the operator sees which sid to reopen); otherwise the
    row inherits its latest task status unless that task is done - a
    finished task means the session is simply idle. A task parked in
    ``waiting_quota`` overrides the lifecycle word: the session is
    deliberately asleep until the provider's reset, which is neither
    running nor idle. A fresh ``wait_until`` stamp reads ``waiting``:
    the claimer is parked on an out-of-band answer, distinct from a
    session that is actually running (issue #151). ``now`` is the
    snapshot clock the caller resolved - passing it keeps every cell
    of one render on the same freshness boundary.
    """
    if session_status == SessionStatus.DEAD.value:
        return "dead"
    if task is None or task["status"] in _IDLE_TASK_STATUSES:
        return "idle"
    if task["step_state"] == "waiting_quota":
        return "quota"
    if task["status"] == TaskStatus.RUNNING.value and _external_wait_fresh(task["wait_until"], now):
        return "waiting"
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
            # item_2/issue #151: lease-expiry pressure - counted on the
            # dedicated lease_expired_at stamp requeue_or_fail writes on
            # every fold (the old 'lease expired' note proxy overcounted
            # any post-fold write and undercounted on note overwrite)
            "lease_expired": int(
                con.execute(
                    "SELECT COUNT(*) AS c FROM tasks"
                    " WHERE lease_expired_at IS NOT NULL AND lease_expired_at>=?",
                    (cutoff,),
                ).fetchone()["c"]
            ),
            # a current-snapshot gauge, not a windowed count: running
            # tasks parked on a still-fresh `bridge wait` stamp
            "waiting_external": int(
                con.execute(
                    "SELECT COUNT(*) AS c FROM tasks WHERE status=?"
                    " AND wait_until IS NOT NULL AND wait_until>?",
                    (TaskStatus.RUNNING.value, now),
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
            "step_state": t["step_state"] if t is not None else None,
            "quota_reset_at": t["quota_reset_at"] if t is not None else None,
            "wait_until": t["wait_until"] if t is not None else None,
            "wait_reason": (
                t["wait_reason"] if t is not None and t["wait_until"] is not None else None
            ),
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
        row["state"] = _fleet_state(r["status"], t, now)
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
            "step_state": t["step_state"],
            "quota_reset_at": t["quota_reset_at"],
            "wait_until": t["wait_until"],
            "wait_reason": t["wait_reason"] if t["wait_until"] is not None else None,
            "attempt": t["attempt"],
            "questions": qcounts.get(t["id"], 0),
            "heartbeat_age_seconds": None,
            "activity_age_seconds": None,
            "wake_outcome": None,
            "wake_age_seconds": None,
            "wake_streak": 0,
            "wake_escalated": False,
            "mail_age_seconds": None,
            "state": _fleet_state(None, t, now),
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
        # one render reads one clock: _fleet_row_line/_task_state_cell
        # resolve wait freshness against this same `now`, never a
        # second time.time() taken mid-render
        "now": now,
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


def _task_state_cell(r, now=None) -> str:
    """Compact lifecycle of the latest task for TSTAT (item_23).

    A ``waiting_quota`` park overrides the lifecycle word - "the step
    is asleep until reset" is the answer the column must give, and the
    raw status stays one ``inspect`` away. A fresh ``wait_until`` stamp
    overrides a bare ``run`` likewise: ``ewait`` marks the claimer
    parked on an out-of-band answer (issue #151). ``now`` is the
    snapshot clock the caller resolved - passing it keeps one render
    from splitting cells across the freshness boundary.
    """
    st = r.get("task_status")
    if not st:
        return "-"
    if r.get("step_state") == "waiting_quota":
        return "quota"
    if st == TaskStatus.RUNNING.value and _external_wait_fresh(r.get("wait_until"), now):
        return "ewait"
    return _TASK_STATE_CELL.get(st, _cut(st, 6))


def _fleet_row_line(r, now=None) -> str:
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
        f" {_task_state_cell(r, now):<6} {_cut(r['state'], 8):<8} {q:>4} {fail:>4}"
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
    now = snap.get("now")
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
            lines.extend(_fleet_row_line(r, now) for r in shown)
            lines.append(f"+{hidden} more session(s) - see --json")
        else:
            lines.extend(_fleet_row_line(r, now) for r in shown)
    g = snap["digest"]
    footer = (
        f"24h: done {g['done']} | auto-decisions {g['auto_decisions']}"
        f" | escalations {g['escalations']} | lease-expired {g['lease_expired']}"
    )
    if g["waiting_external"]:
        footer += f" | waiting-external {g['waiting_external']}"
    lines.append(footer)
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
        # issue #121: optional session-slot cap; max 0 = unlimited.
        # slot_wait is the backlog this pass left queued only because
        # a fresh session slot was unavailable. issue #142: report the
        # durable effective value, not this process's env read.
        "session_slots": {
            "active": len(db.active_session_ids(path=args.db)),
            "max": db.effective_max_active_sessions(path=args.db),
        },
        "slot_wait": len(result["slot_wait"]),
        # issue #151: live gauge of `bridge wait` parked claimers -
        # the digest below carries the same count plus lease_expired
        "waiting_external": snap["digest"]["waiting_external"],
        # item_13e2 fleet view: the human render below reads only
        # these keys; --json carries the uncapped structures
        "attention": snap["attention"],
        "vitals": snap["vitals"],
        "fleet": snap["fleet"],
        "digest_24h": snap["digest"],
    }
    if result["saturated"]:
        data["saturation_reason"] = "MAX_CONCURRENT_REACHED"
    if result["slot_wait"]:
        data["slot_wait_reason"] = "MAX_ACTIVE_SESSIONS_REACHED"
    resp = JsonResponse(
        status="ok",
        queue_depth=queue_depth,
        events=events,
        data=data,
    )
    return _emit(args, resp, ExitCode.OK, _render_status(snap))


def _next_action(task) -> str:
    st = task["status"]
    if st == TaskStatus.QUEUED.value:
        return "waiting for dispatch (semaphore, session-slot cap, or hook trigger)"
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

"""Fleet metrics command (directive 446, issue #98).

``bridge fleet-metrics [--window 24h|7d]`` renders a read-only
markdown report: per-session task/stall/question counters from the
bridge state db plus per-repo incident/review-round aggregates from
each repo's ``.devin/cycle_log.jsonl`` (repo paths come from the
registered sessions' ``cwd`` - the allocation of a session to a repo
is the registry). Facts only - no quality scoring, no automation.
"""

import json
import statistics
import time
from pathlib import Path

from session_bridge.cli.common import _emit
from session_bridge.cli.contract import (
    ExitCode,
    JsonResponse,
)
from session_bridge.state import db
from session_bridge.state.contract import LIVE_STATUSES, TaskStatus

_WINDOW_SECONDS = {"24h": 86400, "7d": 7 * 86400}

# Actions in repo cycle_log.jsonl that represent a fix round on an
# item's branch - each lands after a review pass, so their per-item
# count is the review-rounds proxy the directive asks for.
_FIX_ROUND_ACTIONS = ("commit", "push")


def _project(cwd: str | None) -> str:
    if not cwd:
        return "-"
    return Path(cwd.rstrip("/")).name or cwd


def _read_cycle_log(repo_dir: Path, since: float) -> list[dict]:
    """Rows of ``<repo>/.devin/cycle_log.jsonl`` inside the window.

    Tolerant reader: malformed lines are skipped, missing file means
    "no recorded ops", not an error.
    """
    path = repo_dir / ".devin" / "cycle_log.jsonl"
    rows = []
    try:
        for ln in path.read_text(encoding="utf-8", errors="replace").splitlines():
            try:
                r = json.loads(ln)
            except json.JSONDecodeError:
                continue
            ts = r.get("ts")
            try:
                epoch = (
                    time.mktime(time.strptime(str(ts)[:19], "%Y-%m-%dT%H:%M:%S"))
                    if isinstance(ts, str)
                    else float(ts)
                )
            except (ValueError, TypeError, OverflowError):
                continue
            r["_epoch"] = epoch
            if epoch >= since:
                rows.append(r)
    except OSError:
        return []
    return rows


def _repo_metrics(rows: list[dict]) -> dict:
    """Aggregates over one repo's in-window cycle_log rows."""
    fails = [r for r in rows if r.get("verdict") == "failure"]
    escal = [r for r in rows if r.get("verdict") == "escalated"]
    by_class: dict[str, int] = {}
    for r in fails:
        by_class[str(r.get("failure_class") or "unknown")] = (
            by_class.get(str(r.get("failure_class") or "unknown"), 0) + 1
        )
    verify_fails = [r for r in fails if "verify" in str(r.get("action", ""))]
    # review-rounds proxy: fix-round actions per item
    per_item: dict[str, int] = {}
    for r in rows:
        action = str(r.get("action") or "")
        if any(a in action for a in _FIX_ROUND_ACTIONS):
            item = str(r.get("item") or "?")
            per_item[item] = per_item.get(item, 0) + 1
    rounds = sorted(per_item.values())
    return {
        "incidents_by_class": by_class,
        "verify_failures": len(verify_fails),
        "escalations": len(escal),
        "rounds_avg": round(statistics.mean(rounds), 1) if rounds else 0,
        "rounds_max": max(rounds) if rounds else 0,
        "incident_rows": fails + escal,
    }


def _session_metrics(con, sid: str, since: int, now: int) -> dict:
    """Bridge-db counters for one session inside the window."""
    tasks = con.execute(
        "SELECT status, created_at, updated_at FROM tasks WHERE session_id=? AND created_at>=?",
        (sid, since),
    ).fetchall()
    done = [t for t in tasks if t["status"] == TaskStatus.DONE.value]
    failed = [t for t in tasks if t["status"] == TaskStatus.FAILED.value]
    live_sql = ", ".join(f"'{s.value}'" for s in sorted(LIVE_STATUSES, key=str))
    inflight = con.execute(
        f"SELECT COUNT(*) c FROM tasks WHERE session_id=? AND status IN ({live_sql})",
        (sid,),
    ).fetchone()["c"]
    cycles = [t["updated_at"] - t["created_at"] for t in done]
    plan_delta = con.execute(
        "SELECT COUNT(*) c FROM decision_events WHERE session_id=?"
        " AND kind LIKE '%plan_delta%' AND created_at>=?",
        (sid, since),
    ).fetchone()["c"]
    stalls = con.execute(
        "SELECT COUNT(*) c FROM decision_events WHERE session_id=?"
        " AND kind='mail_stall' AND created_at>=?",
        (sid, since),
    ).fetchone()["c"]
    questions = con.execute(
        "SELECT COUNT(*) c FROM questions WHERE session_id=? AND asked_at>=?",
        (sid, since),
    ).fetchone()["c"]
    return {
        "done": len(done),
        "failed": len(failed),
        "inflight": inflight,
        "cycle_median_s": int(statistics.median(cycles)) if cycles else None,
        "plan_delta": plan_delta,
        "stall_episodes": stalls,
        "questions": questions,
    }


def _fmt_dur(seconds: int | None) -> str:
    if seconds is None:
        return "-"
    if seconds < 120:
        return f"{seconds}s"
    if seconds < 7200:
        return f"{seconds // 60}m"
    return f"{seconds / 3600:.1f}h"


def _cmd_fleet_metrics(args) -> int:
    """``bridge fleet-metrics``: read-only markdown fleet report
    (directive 446). Sessions never see this - it is written for the
    orchestrator/operator on stdout."""
    window = getattr(args, "window", None) or "7d"
    seconds = _WINDOW_SECONDS.get(window)
    if seconds is None:
        return _emit(
            args,
            JsonResponse(status="error", error=f"unknown window {window!r} (use 24h|7d)"),
            ExitCode.USAGE,
            f"unknown window {window!r}",
        )
    now = int(time.time())
    since = now - seconds

    con = db.connect(args.db)
    try:
        sessions = con.execute(
            "SELECT session_id, cwd, role, mode, status FROM sessions ORDER BY session_id"
        ).fetchall()
        per_session = {
            r["session_id"]: _session_metrics(con, r["session_id"], since, now) for r in sessions
        }
        orch_rows = con.execute(
            "SELECT asked_at, answered_at FROM questions"
            " WHERE answered_at IS NOT NULL AND answered_at>=?",
            (since,),
        ).fetchall()
        task_lat = con.execute(
            "SELECT updated_at - created_at AS d FROM tasks"
            " WHERE status=? AND updated_at>=? AND updated_at>created_at",
            (TaskStatus.DONE.value, since),
        ).fetchall()
    finally:
        con.close()

    # repo metrics: union of session cwds (a repo may host several
    # sessions - metrics aggregate per repo path once)
    repos: dict[str, dict] = {}
    for r in sessions:
        cwd = r["cwd"]
        if not cwd or cwd in repos:
            continue
        rows = _read_cycle_log(Path(cwd), since)
        repos[cwd] = _repo_metrics(rows) if rows is not None else {}

    hungry_now = sum(
        1
        for r in sessions
        if r["mode"] == "managed"
        and r["status"] == "active"
        and r["role"] != "orchestrator"
        and per_session.get(r["session_id"], {}).get("inflight", 0) == 0
    )
    lat = [r["d"] for r in task_lat]
    answer_s = [r["answered_at"] - r["asked_at"] for r in orch_rows]

    cwd_of = {s["session_id"]: s["cwd"] for s in sessions}
    data = {
        "window": window,
        "generated_at": now,
        "sessions": {
            sid: {**m, "project": _project(cwd_of.get(sid))} for sid, m in per_session.items()
        },
        "repos": {
            _project(k): {kk: vv for kk, vv in v.items() if kk != "incident_rows"}
            for k, v in repos.items()
        },
        "incidents": [
            {
                "repo": _project(cwd),
                "ts": r.get("ts"),
                "item": r.get("item"),
                "action": r.get("action"),
                "failure_class": r.get("failure_class"),
                "status": r.get("status"),
                "result": str(r.get("result"))[:140],
            }
            for cwd, m in repos.items()
            for r in m.get("incident_rows", [])
        ],
        "orchestrator": {
            "dispatch_to_done_median_s": int(statistics.median(lat)) if lat else None,
            "hungry_workers_now": hungry_now,
            "escalations": sum(m["escalations"] for m in repos.values()),
            "ask_answer_mean_s": int(statistics.mean(answer_s)) if answer_s else None,
        },
    }

    # markdown rendering (the human output IS the report)
    lines = [f"# Fleet metrics ({window})", ""]
    lines.append(
        "| session | project | done | failed | in-flight | PLAN_DELTA"
        " | stall>ep | asks | cycle~med |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for sid, m in sorted(data["sessions"].items()):
        lines.append(
            f"| {sid} | {m['project']} | {m['done']} | {m['failed']}"
            f" | {m['inflight']} | {m['plan_delta']} | {m['stall_episodes']}"
            f" | {m['questions']} | {_fmt_dur(m['cycle_median_s'])} |"
        )
    lines += [
        "",
        "## Repo cycles",
        "",
        "| repo | fix-rounds/item avg | max | verify fails | escalations | incidents |",
        "|---|---|---|---|---|---|",
    ]
    for repo, m in sorted(data["repos"].items()):
        inc = ", ".join(f"{k}x{v}" for k, v in sorted(m["incidents_by_class"].items())) or "-"
        lines.append(
            f"| {repo} | {m['rounds_avg']} | {m['rounds_max']}"
            f" | {m['verify_failures']} | {m['escalations']} | {inc} |"
        )
    o = data["orchestrator"]
    lines += [
        "",
        "## Orchestrator",
        "",
        f"- dispatch_decided→done median: {_fmt_dur(o['dispatch_to_done_median_s'])}",
        f"- hungry managed workers (snapshot): {o['hungry_workers_now']}",
        f"- escalations (cycle_log 'escalated' verdicts): {o['escalations']}",
        f"- mean answer time on bridge ask: {_fmt_dur(o['ask_answer_mean_s'])}",
        "",
        "## Incident log",
        "",
        "| date | repo | item | action | failure_class | status | result |",
        "|---|---|---|---|---|---|---|",
    ]
    for i in sorted(data["incidents"], key=lambda r: str(r["ts"])):
        lines.append(
            f"| {str(i['ts'])[:10]} | {i['repo']} | {i['item'] or '-'} | {i['action']}"
            f" | {i['failure_class'] or '-'} | {i['status'] or '-'} | {i['result']} |"
        )
    report = "\n".join(lines)

    data["report"] = report
    return _emit(args, JsonResponse(status="ok", data=data), ExitCode.OK, report)

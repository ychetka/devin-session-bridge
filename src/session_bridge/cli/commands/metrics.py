"""Fleet metrics command (directive 446, issue #98).

``bridge fleet-metrics [--window 24h|7d]`` renders a read-only
markdown report: per-session task/stall/question counters from the
bridge state db plus per-repo incident/review-round aggregates from
each repo's ``.devin/cycle_log.jsonl`` (repo paths come from the
registered sessions' ``cwd`` - the allocation of a session to a repo
is the registry). The db is opened ``mode=ro`` so the no-write claim
is kernel-enforced. Facts only - no quality scoring, no automation.
"""

import json
import os
import sqlite3
import statistics
import time
from collections import Counter
from datetime import datetime, timezone
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
# count is the review-rounds proxy the directive asks for. Exact
# match: a substring check would fold ``skip_commit``/``recommit``
# into rounds they are not.
_FIX_ROUND_ACTIONS = frozenset({"commit", "push", "commit_push"})

# Re-alert cadence of the daemon's mail-stall pass (same env contract
# as daemon/core.py): consecutive ``mail_stall`` decision events
# spaced at most this far apart are one episode, not N incidents -
# the daemon re-arms and re-journals every cadence inside a stall.
_STALL_REALERT_S = int(os.environ.get("SB_STALL_REALERT_S", "3600"))


def _parse_cycle_ts(ts) -> float | None:
    """Epoch seconds for a cycle_log ``ts`` (ISO-8601 or numeric).

    cycle_log stamps are UTC ISO strings. A naive string is read as
    UTC, never host-local time - mktime-based parsing would shift
    window boundaries by the host offset.
    """
    if isinstance(ts, bool):
        return None
    if isinstance(ts, (int, float)):
        return float(ts)
    if not isinstance(ts, str):
        return None
    try:
        dt = datetime.fromisoformat(ts.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    try:
        return dt.timestamp()
    except (OverflowError, OSError):
        return None


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
            epoch = _parse_cycle_ts(r.get("ts"))
            if epoch is None:
                continue
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
    # Escalated rows are incidents too - they show in the incident log
    # and must not silently vanish from the class histogram.
    for r in fails + escal:
        cls = str(r.get("failure_class") or ("escalated" if r in escal else "unknown"))
        by_class[cls] = by_class.get(cls, 0) + 1
    verify_fails = [r for r in fails if "verify" in str(r.get("action", ""))]
    # review-rounds proxy: fix-round actions per item
    per_item: dict[str, int] = {}
    for r in rows:
        if str(r.get("action") or "").strip() in _FIX_ROUND_ACTIONS:
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


def _stall_episodes(con, sid: str, since: int) -> int:
    """``mail_stall`` episodes: consecutive events within one re-alert
    cadence collapse into a single episode - the daemon re-journals
    every ``SB_STALL_REALERT_S`` inside a stall, so raw event count
    would inflate the metric for the worst stalls."""
    rows = con.execute(
        "SELECT created_at FROM decision_events WHERE session_id=?"
        " AND kind='mail_stall' AND created_at>=? ORDER BY created_at",
        (sid, since),
    ).fetchall()
    episodes = 0
    prev = None
    for r in rows:
        if prev is None or r["created_at"] - prev > _STALL_REALERT_S:
            episodes += 1
        prev = r["created_at"]
    return episodes


def _session_metrics(con, sid: str, since: int, fin_col: str) -> dict:
    """Bridge-db counters for one session inside the window.

    ``fin_col`` is ``finished_at`` on current schemas, ``updated_at``
    as a read-only fallback for pre-migration files (see init()).
    """
    done_rows = con.execute(
        f"SELECT created_at, {fin_col} AS fin FROM tasks WHERE session_id=?"
        f" AND status=? AND {fin_col}>=?",
        (sid, TaskStatus.DONE.value, since),
    ).fetchall()
    failed = con.execute(
        f"SELECT COUNT(*) c FROM tasks WHERE session_id=? AND status=? AND {fin_col}>=?",
        (sid, TaskStatus.FAILED.value, since),
    ).fetchone()["c"]
    live_sql = ", ".join(f"'{s.value}'" for s in sorted(LIVE_STATUSES, key=str))
    inflight = con.execute(
        f"SELECT COUNT(*) c FROM tasks WHERE session_id=? AND status IN ({live_sql})",
        (sid,),
    ).fetchone()["c"]
    cycles = [r["fin"] - r["created_at"] for r in done_rows]
    plan_delta = con.execute(
        "SELECT COUNT(*) c FROM decision_events WHERE session_id=?"
        " AND kind='plan_delta' AND created_at>=?",
        (sid, since),
    ).fetchone()["c"]
    questions = con.execute(
        "SELECT COUNT(*) c FROM questions WHERE session_id=? AND asked_at>=?",
        (sid, since),
    ).fetchone()["c"]
    return {
        "done": len(done_rows),
        "failed": failed,
        "inflight": inflight,
        "cycle_median_s": int(statistics.median(cycles)) if cycles else None,
        "plan_delta": plan_delta,
        "stall_episodes": _stall_episodes(con, sid, since),
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


def _md(v) -> str:
    """Escape a value for a markdown table cell."""
    if v is None:
        return "-"
    return str(v).replace("|", "\\|").replace("\r", " ").replace("\n", " ")


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

    try:
        con = db.connect_ro(args.db)
    except sqlite3.OperationalError as e:
        return _emit(
            args,
            JsonResponse(status="error", error=f"state.db not readable: {e}"),
            ExitCode.INTERNAL,
            f"state.db not readable: {e}",
        )
    try:
        sessions = con.execute(
            "SELECT session_id, cwd, role, mode, status FROM sessions ORDER BY session_id"
        ).fetchall()
        # pre-finished_at files (created before v0.6 and never re-init'ed)
        # degrade to updated_at - approximate but the report still runs.
        task_cols = {r["name"] for r in con.execute("PRAGMA table_info(tasks)")}
        fin_col = "finished_at" if "finished_at" in task_cols else "updated_at"
        per_session = {
            r["session_id"]: _session_metrics(con, r["session_id"], since, fin_col)
            for r in sessions
        }
        orch_rows = con.execute(
            "SELECT asked_at, answered_at FROM questions"
            " WHERE answered_at IS NOT NULL AND answered_at>=?",
            (since,),
        ).fetchall()
        task_lat = con.execute(
            f"SELECT {fin_col} - created_at AS d FROM tasks"
            f" WHERE status=? AND {fin_col}>=? AND {fin_col}>created_at",
            (TaskStatus.DONE.value, since),
        ).fetchall()
    finally:
        con.close()

    # repo metrics: union of normalized session cwds (a repo may host
    # several sessions - metrics aggregate per repo path once).
    # Repo keys stay full paths so two repos sharing a directory
    # basename can never silently merge; the display name below is a
    # basename when unique, the full path on collision.
    repos: dict[str, dict] = {}
    for r in sessions:
        cwd = r["cwd"]
        if not cwd:
            continue
        norm = os.path.normpath(cwd)
        if norm in repos:
            continue
        repos[norm] = _repo_metrics(_read_cycle_log(Path(norm), since))

    base_counts = Counter(Path(c).name or c for c in repos)

    def _repo_name(cwd: str) -> str:
        base = Path(cwd).name or cwd
        return base if base_counts[base] == 1 else cwd

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
            sid: {
                **m,
                "project": _repo_name(os.path.normpath(cwd_of[sid])) if cwd_of.get(sid) else "-",
            }
            for sid, m in per_session.items()
        },
        "repos": {
            _repo_name(k): {kk: vv for kk, vv in v.items() if kk != "incident_rows"}
            for k, v in repos.items()
        },
        "incidents": [
            {
                "repo": _repo_name(cwd),
                "ts": r.get("ts"),
                "item": r.get("item"),
                "action": r.get("action"),
                "failure_class": r.get("failure_class"),
                "status": r.get("status"),
                "result": str(r.get("result"))[:140] if r.get("result") is not None else None,
            }
            for cwd, m in repos.items()
            for r in m.get("incident_rows", [])
        ],
        "orchestrator": {
            "cycle_median_s": int(statistics.median(lat)) if lat else None,
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
            f"| {_md(sid)} | {_md(m['project'])} | {m['done']} | {m['failed']}"
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
            f"| {_md(repo)} | {m['rounds_avg']} | {m['rounds_max']}"
            f" | {m['verify_failures']} | {m['escalations']} | {_md(inc)} |"
        )
    o = data["orchestrator"]
    lines += [
        "",
        "## Orchestrator",
        "",
        f"- task cycle (enqueue→done) median: {_fmt_dur(o['cycle_median_s'])}",
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
            f"| {str(i['ts'])[:10]} | {_md(i['repo'])} | {_md(i['item'])}"
            f" | {_md(i['action'])} | {_md(i['failure_class'])}"
            f" | {_md(i['status'])} | {_md(i['result'])} |"
        )
    report = "\n".join(lines)

    data["report"] = report
    return _emit(args, JsonResponse(status="ok", data=data), ExitCode.OK, report)

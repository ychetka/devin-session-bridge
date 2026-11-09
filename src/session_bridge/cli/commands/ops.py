"""Operator commands (task #210 split).

schedule / daemon / version.
"""

import importlib.metadata
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

from session_bridge.cli.common import _emit, _log, _queue_depth, _state_dir
from session_bridge.cli.contract import (
    ExitCode,
    JsonResponse,
)
from session_bridge.cli.dispatch import _logs_dir
from session_bridge.daemon import core as daemon
from session_bridge.state import db

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


#: grace period for SIGTERM before ``daemon stop`` reports a wedged daemon
DAEMON_STOP_TIMEOUT_SECONDS = 5.0


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
    src = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    )
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


def _version_string() -> str:
    """True deployed version (item_102a).

    Editable installs carry stale dist-info (`pip show` said 0.2.7
    while the checkout sat at v0.2.8), so when running out of a
    ``src/`` tree the version is read from the repo's pyproject.toml;
    packaged installs fall back to importlib.metadata. The source
    path is always appended so the report is self-describing.
    """
    here = Path(__file__).resolve()
    # src/session_bridge/cli/commands/ops.py -> repo root; guard the
    # walk so an odd layout can never break parser construction for
    # every command
    pyproject = here.parents[4] / "pyproject.toml" if len(here.parents) > 4 else None
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
    src_hint = here.parents[3] if len(here.parents) > 3 else here
    return f"{ver} (src: {src_hint})"


def _cmd_version(args) -> int:
    ver = _version_string()
    return _emit(
        args,
        JsonResponse(status="ok", data={"version": ver}),
        ExitCode.OK,
        human=f"session-bridge {ver}",
    )

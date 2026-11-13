"""Layer-2 attributed orphan sweep (issue #164, docs/SESSION_HARD_STOP.md).

One shared ``devin acp`` backend process owns every local session -
there is no per-session OS process to signal, so a plain ``kill`` of
the session lockfile pid is a fleet kill, never a session kill. This
module enumerates the backend's descendant process tree and signals
only the processes attributable to the target session, after layers
0-1 (``session/cancel``, forced ``session/close``) already ran.

Safety invariants (design doc section 8):

- the backend and the acp-host supervisor are never signalled - a
  fleet kill dressed as a session kill is worse than no kill;
- MCP/tool server children are never signalled: they match a
  conservative ``mcp`` cmdline whitelist, or they spawned inside the
  backend's own startup window (verified live 2026-11-13: every MCP
  child - ``mcp_partner.py``, ``hands-agent/mcp_server.py``,
  ``devin-telegram mcp``, ``mcp-server-mikrotik`` - starts within
  ~0.5s of ``devin acp``). The design's "start-time older than the
  cancel moment" is operationalized as that startup window: a
  literal reading would exempt every candidate, since all victims
  predate the cancel by definition;
- a process whose markers name a DIFFERENT registered session
  (``queue/<other-sid>``, ``--session <other-sid>``, a foreign
  mailbox dir, or a cwd under a foreign session's workspace) is
  never signalled;
- attribution is positive: a candidate must carry the session's
  workspace cwd or an explicit sid marker (``queue/<sid>``,
  ``--session <sid>`` / ``--session=<sid>``, the mailbox dir path).
  Marker attribution is strong and sweeps regardless of age; cwd
  attribution is weak and yields to the startup-window exclusion.

Signal order: SIGTERM, grace (default 2s), SIGKILL to survivors -
deepest tree members first so a dying parent cannot respawn or
re-parent a child mid-sweep. Every action lands in the result with
its attribution reason; nothing is claimed that was not observed.

Stdlib only (Linux ``/proc``). The process lister is injectable:
tests pass a fabricated ``procs`` list plus fake signal/alive/sleep
functions - no real PID is ever enumerated or signalled under test.
"""

import os
import re
import signal
import time
from typing import NamedTuple

__all__ = [
    "BACKEND_SPAWN_GRACE_SECONDS",
    "SWEEP_TERM_GRACE_SECONDS",
    "ProcInfo",
    "SweepContext",
    "classify",
    "descendants",
    "is_devin_acp",
    "list_processes",
    "plan_and_sweep",
    "process_gone",
    "resolve_backend",
    "sweep",
]

#: default pause between the SIGTERM volley and the SIGKILL follow-up.
SWEEP_TERM_GRACE_SECONDS = 2.0

#: children started within this many seconds of the backend's own
#: start are infrastructure (MCP/tool servers spawn with the host),
#: never session work - the cwd-only exclusion window.
BACKEND_SPAWN_GRACE_SECONDS = 30.0

#: conservative server whitelist: any cmdline containing this token
#: is treated as a shared MCP/tool server and excluded, even when it
#: also carries session markers (deliverable's literal rule).
_MCP_TOKEN = "mcp"

#: matches ``devin acp`` as an argv pair or inside a wrapper cmdline.
_DEVIN_ACP_RE = re.compile(r"(?:^|[/\s])devin acp(?:\s|$)")

#: cwd attribution is refused for trivially-broad session workspaces:
#: a registration at the filesystem root (or an empty/relative cwd)
#: would attribute half the host to one session.
_MIN_CWD_DEPTH = 2


class ProcInfo(NamedTuple):
    """One enumerated process - the injected-fake seam of the sweep."""

    pid: int
    ppid: int
    argv: tuple
    cwd: str | None
    #: seconds since boot (``/proc/<pid>/stat`` starttime / CLK_TCK);
    #: only relative comparisons against the backend's start matter.
    start_seconds: float

    @property
    def cmdline(self) -> str:
        return " ".join(self.argv)


def _read_proc(proc_root: str, pid: int, hz: float) -> ProcInfo | None:
    base = os.path.join(proc_root, str(pid))
    try:
        with open(os.path.join(base, "stat"), "rb") as fh:
            raw = fh.read()
    except OSError:
        return None
    # comm may contain spaces/parens: fields after the last ')' are
    # state ppid pgrp ... starttime(field 22 overall, index 19 here).
    rparen = raw.rfind(b")")
    if rparen < 0:
        return None
    fields = raw[rparen + 2 :].split()
    try:
        ppid = int(fields[1])
        start_ticks = int(fields[19])
    except (IndexError, ValueError):
        return None
    try:
        with open(os.path.join(base, "cmdline"), "rb") as fh:
            argv = tuple(tok.decode("utf-8", "replace") for tok in fh.read().split(b"\0") if tok)
    except OSError:
        argv = ()
    try:
        cwd = os.readlink(os.path.join(base, "cwd"))
    except OSError:
        cwd = None
    return ProcInfo(
        pid=pid,
        ppid=ppid,
        argv=argv,
        cwd=cwd,
        start_seconds=start_ticks / hz,
    )


def list_processes(proc_root: str = "/proc") -> list[ProcInfo]:
    """Enumerate the host process table (the real, un-faked lister)."""
    try:
        hz = float(os.sysconf("SC_CLK_TCK"))
    except (ValueError, OSError, AttributeError):
        hz = 100.0
    out = []
    try:
        names = os.listdir(proc_root)
    except OSError:
        return out
    for name in names:
        if not name.isdigit():
            continue
        info = _read_proc(proc_root, int(name), hz)
        if info is not None:
            out.append(info)
    return out


def is_devin_acp(proc: ProcInfo) -> bool:
    """True when the process is the shared ``devin acp`` backend."""
    argv = proc.argv
    if argv and os.path.basename(argv[0]) == "devin" and len(argv) > 1 and argv[1] == "acp":
        return True
    return bool(_DEVIN_ACP_RE.search(proc.cmdline))


def resolve_backend(lock_pid, procs) -> list[ProcInfo]:
    """The shared ``devin acp`` backend(s) belonging to the acp-host.

    Verified live (issue #164): the lockfile pid is the
    ``windsurf-cli ... agent acp-host`` supervisor and the backend
    owning all sessions is its direct child. Fallback for installs
    without the supervisor split: the lockfile pid itself IS
    ``devin acp``.
    """
    for p in procs:
        if p.pid == lock_pid:
            if is_devin_acp(p):
                return [p]
            break
    return [p for p in procs if p.ppid == lock_pid and is_devin_acp(p)]


def descendants(root_pids, procs) -> list[ProcInfo]:
    """Every descendant of ``root_pids`` found in ``procs`` (any depth)."""
    children = {}
    for p in procs:
        children.setdefault(p.ppid, []).append(p)
    out = []
    stack = list(root_pids)
    seen = set(root_pids)
    while stack:
        cur = stack.pop()
        for child in children.get(cur, ()):
            if child.pid in seen:
                continue
            seen.add(child.pid)
            out.append(child)
            stack.append(child.pid)
    return out


def _under(path: str | None, root: str | None) -> bool:
    """``path`` == ``root`` or sits inside it (both already normalized)."""
    if not path or not root:
        return False
    root = root.rstrip("/")
    return path == root or path.startswith(root + "/")


def _segment_hit(token: str, marker: str) -> bool:
    """``marker`` occurs in ``token`` as a /-bounded path segment."""
    idx = token.find(marker)
    while idx >= 0:
        start_ok = idx == 0 or token[idx - 1] == "/"
        end = idx + len(marker)
        end_ok = end == len(token) or token[end] == "/"
        if start_ok and end_ok:
            return True
        idx = token.find(marker, idx + 1)
    return False


def _markers_for(sid: str, mailbox_dir: str | None) -> list[str]:
    markers = [f"queue/{sid}"]
    if mailbox_dir:
        markers.append(mailbox_dir.rstrip("/"))
    return markers


def marker_hits(proc: ProcInfo, sid: str, mailbox_dir: str | None) -> list[str]:
    """Explicit sid markers in the process argv (strong attribution)."""
    hits = []
    argv = proc.argv
    for i, tok in enumerate(argv):
        if tok == "--session" and i + 1 < len(argv) and argv[i + 1] == sid:
            hits.append(f"--session {sid}")
        elif tok == f"--session={sid}":
            hits.append(f"--session={sid}")
        else:
            for marker in _markers_for(sid, mailbox_dir):
                if _segment_hit(tok, marker):
                    hits.append(marker)
                    break
    return hits


def _norm(path: str | None) -> str | None:
    if not path:
        return None
    try:
        norm = os.path.normpath(path)
    except (TypeError, ValueError):
        return None
    return norm if os.path.isabs(norm) else None


def _cwd_usable(session_cwd: str | None) -> bool:
    """Refuse trivially-broad workspaces (root, home) for attribution."""
    if session_cwd is None:
        return False
    return len([seg for seg in session_cwd.split("/") if seg]) >= _MIN_CWD_DEPTH


class SweepContext(NamedTuple):
    """Attribution context for one ``bridge stop --hard`` target."""

    sid: str
    session_cwd: str | None
    mailbox_dir: str | None
    #: pids that must never be signalled: backend(s) + acp-host lock pid.
    protected_pids: frozenset
    backend_pids: frozenset
    backend_start: float | None
    #: other registered sessions: (sid, cwd) pairs - the foreign check.
    foreign_sessions: tuple
    spawn_grace_seconds: float = BACKEND_SPAWN_GRACE_SECONDS


def _foreign_hit(proc: ProcInfo, ctx: SweepContext) -> str | None:
    """The OTHER registered session this process belongs to, if any."""
    for sid, cwd in ctx.foreign_sessions:
        if marker_hits(proc, sid, None):
            return sid
        if _under(proc.cwd, cwd):
            return sid
    return None


def classify(proc: ProcInfo, ctx: SweepContext) -> tuple[str, str]:
    """Attribute one process: ("candidate"|"excluded"|"unrelated", reason).

    Exclusions are evaluated before attribution and in order of
    blast radius: infrastructure pids first, foreign-session markers
    next (a misattributed kill is a second incident), the MCP
    whitelist third, then the positive-attribution legs.
    """
    if proc.pid in ctx.protected_pids or proc.pid in ctx.backend_pids:
        return "excluded", "protected_infrastructure"
    foreign = _foreign_hit(proc, ctx)
    if foreign is not None:
        return "excluded", f"other_session:{foreign}"
    if _MCP_TOKEN in proc.cmdline.lower():
        return "excluded", "mcp_server"
    hits = marker_hits(proc, ctx.sid, ctx.mailbox_dir)
    if hits:
        # explicit sid markers are strong attribution: they sweep
        # regardless of process age
        return "candidate", f"cmdline:{hits[0]}"
    if _under(proc.cwd, ctx.session_cwd) and _cwd_usable(ctx.session_cwd):
        if (
            ctx.backend_start is not None
            and proc.start_seconds <= ctx.backend_start + ctx.spawn_grace_seconds
        ):
            return "excluded", "infra_spawn_window"
        return "candidate", f"cwd_under:{ctx.session_cwd}"
    return "unrelated", ""


def process_gone(pid: int) -> bool:
    """True when ``pid`` is reaped or a zombie (an un-signalable corpse).

    ``os.kill(pid, 0)`` alone would call a zombie "alive" forever -
    the SIGKILL follow-up reads ``/proc`` state first.
    """
    try:
        with open(f"/proc/{pid}/stat", "rb") as fh:
            raw = fh.read()
        rparen = raw.rfind(b")")
        if rparen < 0 or raw[rparen + 2 : rparen + 3] == b"Z":
            return True
    except OSError:
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    except OSError:
        return True
    return False


def _depth(pid: int, procs_by_pid, backend_pids) -> int:
    """Tree depth below the backend - children signal before parents."""
    depth = 0
    cur = pid
    seen = {pid}
    while cur in procs_by_pid:
        parent = procs_by_pid[cur].ppid
        if parent in backend_pids or parent not in procs_by_pid or parent in seen:
            break
        seen.add(parent)
        cur = parent
        depth += 1
    return depth


def sweep(
    ctx: SweepContext,
    procs,
    *,
    signaler=None,
    gone=None,
    sleep=time.sleep,
    grace_seconds: float = SWEEP_TERM_GRACE_SECONDS,
    dry_run: bool = False,
) -> dict:
    """SIGTERM -> grace -> SIGKILL the attributed descendants.

    ``signaler``/``gone``/``sleep`` are the injection seams - tests
    pass fakes so no real PID is ever signalled. Returns the recorded
    action list: ``candidates`` (attribution reasons), ``skipped``
    (exclusion reasons) and ``swept`` (signal + observed outcome per
    pid). ``dry_run`` enumerates and reports without signalling.
    """
    signaler = signaler or os.kill
    gone = gone or process_gone
    procs = list(procs)
    by_pid = {p.pid: p for p in procs}
    tree = descendants(ctx.backend_pids, procs)
    candidates = []
    skipped = []
    for proc in tree:
        verdict, detail = classify(proc, ctx)
        if verdict == "candidate":
            candidates.append(
                {
                    "pid": proc.pid,
                    "reason": detail,
                    "cmdline": proc.cmdline[:200],
                    "depth": _depth(proc.pid, by_pid, ctx.backend_pids),
                }
            )
        elif verdict == "excluded":
            skipped.append({"pid": proc.pid, "exclusion": detail})
    result = {
        "dry_run": dry_run,
        "candidates": [
            {"pid": c["pid"], "reason": c["reason"], "cmdline": c["cmdline"]} for c in candidates
        ],
        "skipped": skipped,
        "swept": [],
        "status": "ok",
    }
    if dry_run:
        result["status"] = "dry_run"
        return result
    # deepest first: a parent killed before its children cannot
    # respawn or re-parent them out from under the sweep
    candidates.sort(key=lambda c: -c["depth"])
    record = {}
    for c in candidates:
        pid = c["pid"]
        entry = {"pid": pid, "reason": c["reason"], "signal": "SIGTERM"}
        record[pid] = entry
        try:
            signaler(pid, signal.SIGTERM)
        except OSError as e:
            entry["outcome"] = f"term_error:{e.__class__.__name__}"
        result["swept"].append(entry)
    if grace_seconds > 0 and candidates:
        sleep(grace_seconds)
    for c in candidates:
        pid = c["pid"]
        entry = record[pid]
        if "outcome" in entry:
            continue  # signal itself failed - already reported
        if gone(pid):
            entry["outcome"] = "gone"
            continue
        entry["signal"] = "SIGKILL"
        try:
            signaler(pid, signal.SIGKILL)
        except OSError as e:
            entry["outcome"] = f"kill_error:{e.__class__.__name__}"
            continue
        entry["outcome"] = "gone" if gone(pid) else "still_alive"
    return result


def plan_and_sweep(
    *,
    lock_pid,
    sid: str,
    session_cwd: str | None,
    mailbox_dir: str | None,
    registered,
    procs=None,
    spawn_grace_seconds: float = BACKEND_SPAWN_GRACE_SECONDS,
    grace_seconds: float = SWEEP_TERM_GRACE_SECONDS,
    dry_run: bool = False,
    signaler=None,
    gone=None,
    sleep=time.sleep,
    lister=None,
) -> dict:
    """Resolve the backend, build the context, run the sweep.

    ``registered`` is an iterable of ``(session_id, cwd)`` pairs -
    every registered session except the target feeds the foreign
    exclusion. Returns a report dict carrying ``backend_pid`` /
    ``acp_host_pid`` for the audit row plus the sweep verdict; a
    missing backend reports ``status="no_backend"`` honestly rather
    than guessing at a pid.
    """
    procs = list(procs) if procs is not None else list((lister or list_processes)())
    backends = resolve_backend(lock_pid, procs)
    base = {
        "sid": sid,
        "acp_host_pid": lock_pid,
        "backend_pid": None,
        "dry_run": dry_run,
        "candidates": [],
        "skipped": [],
        "swept": [],
    }
    if not backends:
        base["status"] = "no_backend"
        return base
    foreign = tuple((s, _norm(cwd)) for s, cwd in registered if s != sid)
    ctx = SweepContext(
        sid=sid,
        session_cwd=_norm(session_cwd),
        mailbox_dir=_norm(mailbox_dir),
        protected_pids=frozenset({lock_pid} | {b.pid for b in backends}),
        backend_pids=frozenset(b.pid for b in backends),
        backend_start=min(b.start_seconds for b in backends),
        foreign_sessions=foreign,
        spawn_grace_seconds=spawn_grace_seconds,
    )
    res = sweep(
        ctx,
        procs,
        signaler=signaler,
        gone=gone,
        sleep=sleep,
        grace_seconds=grace_seconds,
        dry_run=dry_run,
    )
    base.update(res)
    base["backend_pid"] = backends[0].pid
    base["backend_pids"] = [b.pid for b in backends]
    return base

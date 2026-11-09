"""Shared helpers for the bridge CLI commands (task #210 split).

Utility seams that more than one command group touches: queue/state
dirs, trigger files, notify-send, session lookups, tombstone merge,
lazy dead-session GC, JsonResponse emission and the ``--json`` flag.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

from session_bridge.cli.contract import (
    ExitCode,
    JsonResponse,
)
from session_bridge.daemon import core as daemon
from session_bridge.state import db
from session_bridge.state.contract import (
    NotificationKind,
    SessionMode,
    TaskStatus,
)

#: lease-owner prefixes: ``acp-`` for _work children, ``bridge-`` for
#: the transient claim used when deciding the hook transport.
ACP_OWNER_PREFIX = "acp-"

DISPATCH_OWNER_PREFIX = "bridge-"


def _log(msg: str) -> None:
    sys.stderr.write(f"sessions-bridge: {msg}\n")
    sys.stderr.flush()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


def _queue_dir(args) -> str:
    if args.queue_dir:
        return args.queue_dir
    base = args.db if args.db else db.DB_PATH
    return os.path.join(os.path.dirname(os.path.abspath(base)), "queue")


def _queue_depth(args) -> int:
    """True queued count - a COUNT(*), never the capped task list."""
    return db.count_tasks(status=TaskStatus.QUEUED, path=args.db)


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


def _session_mode(session_id: str, args) -> str:
    """sessions.mode for a session (unmanaged when the row is absent)."""
    row = db.get_session(session_id, path=args.db)
    if row is None:
        return SessionMode.UNMANAGED.value
    return row["mode"]


def _has_open_dispatch(task_id: int, args) -> bool:
    """True when the task already has an unacked TASK_DISPATCH row.

    The dedup guard for managed dispatch: one queued task maps to at
    most one open outbox notification, so pump passes do not spam the
    mailbox while the task waits for the daemon relay (item_13d) and
    the recipient's ack watermark (``ack_at``) marks delivery closed.
    """
    con = db.connect(args.db)
    try:
        row = con.execute(
            "SELECT 1 FROM notifications WHERE task_id=? AND kind=? AND ack_at IS NULL LIMIT 1",
            (task_id, NotificationKind.TASK_DISPATCH.value),
        ).fetchone()
        return row is not None
    finally:
        con.close()


def _gc_dead_sessions(args) -> None:
    """Lazy GC of dead registrations (task #207 item f).

    Every bus-touching call (``bridge status``, ``wait``, the
    ``pump`` pass behind ``push``/``_work``) marks unmanaged
    sessions dead when their liveness TTL passed - the same verdict
    the daemon sweep gives, so a ghost dies on the next operator
    call even with the daemon down. Two legs ride the shared
    ``db.stale_unmanaged_sessions`` scan: silent stamped heartbeats
    (``HEARTBEAT_STALE_SECONDS``) and no-heartbeat-row ghosts
    (``GHOST_TTL_SECONDS``). The ``set_session_status`` CAS dedups a
    racing daemon sweep: only the active->dead edge fires effects,
    whoever flips it.
    """
    now = int(time.time())
    for row in db.stale_unmanaged_sessions(now, path=args.db):
        daemon.mark_dead(row, args.db, _state_dir(args), _notify_send, now, tag="gc ")


def _emit(args, resp: JsonResponse, code: ExitCode, human: str | None = None) -> int:
    if getattr(args, "json", False):
        sys.stdout.write(resp.model_dump_json() + "\n")
        sys.stdout.flush()
    elif human is not None:
        sys.stdout.write(human + "\n")
    elif resp.error:
        sys.stderr.write(f"error: {resp.error}\n")
    return int(code)


def _tombstone_path(sid: str, mdir: str) -> str:
    """Park name for a mailbox being unregistered - unique per caller."""
    return os.path.join(
        os.path.dirname(mdir), f".unregistered-{sid}-{int(time.time())}-{os.getpid()}"
    )


def _merge_dir_no_clobber(src: str, dst: str) -> None:
    """Move src's files into dst; a live same-name file always wins.

    Mailbox names are msg_<notification_id>.txt, so a same-name file
    in a recreated dir is almost always the same notification (the
    consumer dedups by id anyway) - the post-delete write owns the
    slot and the parked duplicate is dropped with the park.
    """
    for name in os.listdir(src):
        if os.path.exists(os.path.join(dst, name)):
            continue
        os.replace(os.path.join(src, name), os.path.join(dst, name))


def _reap_unregister_tombstones(sid: str, mdir: str) -> None:
    """Merge leftover ``.unregistered-<sid>-*`` parks back into ``<sid>``.

    A crash between the tombstone rename and the db commit (or a
    failed restore) strands the mailbox under a name no writer
    targets - the registration lives but its mail is parked forever.
    The reaper runs where a live registration meets its mailbox
    again: top of ``bridge unregister`` and after a successful
    ``bridge register``. Best-effort: a sick fs must not block the
    command itself.
    """
    root = os.path.dirname(mdir)
    try:
        parked = [
            name
            for name in os.listdir(root)
            if name.startswith(f".unregistered-{sid}-") and os.path.isdir(os.path.join(root, name))
        ]
    except OSError:
        return
    for name in parked:
        park = os.path.join(root, name)
        try:
            os.makedirs(mdir, exist_ok=True)
            _merge_dir_no_clobber(park, mdir)
            shutil.rmtree(park, ignore_errors=True)
        except OSError:
            continue


def _managed_owner(sid: str, tid: int) -> str:
    """Deterministic lease owner for managed claim/finish (item_18).

    Derived from (session, task) so `bridge finish` recomputes the
    same owner without any state - the finish CAS binds on it.
    """
    return f"{DISPATCH_OWNER_PREFIX}{sid}-{tid}"


def _state_dir(args) -> str:
    base = args.db if args.db else db.DB_PATH
    return os.path.dirname(os.path.abspath(base))


def _add_json(sp: argparse.ArgumentParser) -> None:
    sp.add_argument("--json", action="store_true", help="emit the JsonResponse envelope")

"""Hook worker for Devin lifecycle events (item_06).

Runs as a subprocess inside a *foreign* repository: Devin spawns it for
every registered hook event, feeds one JSON payload on stdin and reads
an optional JSON answer on stdout. The worker always exits 0 - a
nonzero exit is an error-not-block anyway, and the fail-safe contract
is "emit nothing, log to stderr, exit 0" so a bridge bug can never
block the host agent.

Event handling:

- ``Stop`` with a queued task in state.db claims it (owner
  ``hook-<pid>``, lease set via mark_running) and answers
  ``{"decision":"block","reason":"[PARKED>TASK] ..."}`` - Devin itself
  feeds the reason back to the agent, which is how a queued task is
  delivered into a live session.
- ``Stop`` with an empty queue parks: it waits up to ``cycle_seconds``
  for the ``queue/<sid>.json`` trigger file (written by the bridge
  after the state.db commit; the file is only a wakeup signal, the db
  stays the source of truth). A valid trigger re-checks the db and
  delivers via ``block``. Otherwise the worker re-arms the stop with a
  timestamped ``[PARKED]`` block until ``park_cycles_left`` (persisted
  in the sessions row, budget = ``cycles_max``) is spent; then the
  queue is checked one last time (last-cycle race) and a live task is
  promoted to ``awaiting_user`` + ``mark_notified`` before the stop is
  allowed. A ``queue_paused`` session skips parking - a paused claim
  can never succeed.
- A ``Stop`` that finds a hook-delivered task still ``running`` means
  the agent finished the injected task: the task is parked as
  ``awaiting_user`` and flagged for notification.
- ``UserPromptSubmit`` is the smart-kick: a stripped prompt of <=3
  chars (empty counts as a kick) clears the pause and, when a task is
  queued, injects it as ``additionalContext``; a longer prompt sets
  ``queue_paused`` and injects nothing.
- ``SessionStart`` records the session (cwd when the payload carries
  it) and refreshes the park budget; ``source=resume`` additionally
  runs ``recover_stale``.
- ``SessionEnd`` records the fact and releases orphans: queued tasks
  of the dead session are driven to ``failed`` and tasks holding a
  lease are parked as ``awaiting_user``, all noted "session ended".

Budget accounting: ``stop_hook_active`` marks a re-armed stop chain;
the cycle budget is consumed where the re-arm block is emitted
(``park_cycles_left`` is decremented then), so chained stops count
down through the persisted value and the flag needs no extra handling.
"""

import argparse
import io
import json
import math
import os
import shutil
import subprocess
import sys
import time

from session_bridge.state import db
from session_bridge.state.contract import TaskStatus, Transport

from session_bridge.hooks.contract import (
    HOOK_TIMEOUT_DEFAULT,
    PARKED_TASK_PREFIX,
    SOURCE_RESUME,
    BlockOutput,
    ContextOutput,
    HookEventName,
    HookPayload,
    HookSpecificOutput,
    ParkConfig,
    SessionEndPayload,
    SessionStartPayload,
    StopPayload,
    UserPromptSubmitPayload,
    parse_hook_payload,
)

__all__ = ["main"]

PARK_CYCLE_SECONDS = 150
PARK_CYCLES_MAX = 5

#: Lease-owner prefix for tasks claimed by a hook worker. A Stop that
#: later finds a running task with this prefix knows the task was
#: delivered through a block, not through the acp-push transport.
HOOK_OWNER_PREFIX = "hook-"

SESSION_ENDED_NOTE = "session ended"

_POLL_SECONDS = 2.0  # fallback check interval when inotifywait is absent
_INOTIFY_GRACE_SECONDS = 10  # head-room around inotifywait's own -t


def _log(msg: str) -> None:
    sys.stderr.write(f"sessions-bridge hook: {msg}\n")
    sys.stderr.flush()


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default


def _parse_args(argv) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="session-bridge-hook",
        description="sessions-bridge Devin lifecycle hook worker",
    )
    p.add_argument(
        "--db",
        default=os.environ.get("SESSIONS_BRIDGE_DB"),
        help="path to state.db (default: SESSIONS_BRIDGE_DB or XDG path)",
    )
    p.add_argument(
        "--queue-dir",
        default=os.environ.get("SESSIONS_BRIDGE_QUEUE_DIR"),
        help="trigger directory (default: <state.db dir>/queue)",
    )
    p.add_argument(
        "--cycle-seconds",
        type=int,
        default=_env_int(
            "SESSIONS_BRIDGE_PARK_CYCLE_SECONDS", PARK_CYCLE_SECONDS
        ),
    )
    p.add_argument(
        "--cycles-max",
        type=int,
        default=_env_int("SESSIONS_BRIDGE_PARK_CYCLES_MAX", PARK_CYCLES_MAX),
    )
    p.add_argument(
        "--hook-timeout",
        type=int,
        default=_env_int("SESSIONS_BRIDGE_HOOK_TIMEOUT", HOOK_TIMEOUT_DEFAULT),
        help="timeout configured for this hook in hooks.v1.json",
    )
    return p.parse_args(argv)


def _park_config(args) -> ParkConfig | None:
    """Build the parking budget; invalid config disables parking."""
    try:
        return ParkConfig(
            cycle_seconds=args.cycle_seconds,
            cycles_max=args.cycles_max,
            hook_timeout=args.hook_timeout,
        )
    except Exception as e:  # ValidationError - contract precondition
        _log(f"invalid park config ({e}); parking disabled")
        return None


def _queue_dir(args) -> str:
    if args.queue_dir:
        return args.queue_dir
    base = args.db if args.db is not None else db.DB_PATH
    return os.path.join(os.path.dirname(os.path.abspath(base)), "queue")


def _owner() -> str:
    return f"{HOOK_OWNER_PREFIX}{os.getpid()}"


def _is_hook_owner(owner) -> bool:
    return isinstance(owner, str) and owner.startswith(HOOK_OWNER_PREFIX)


# --- stdout answers ---------------------------------------------------------


def _block(reason: str) -> str:
    return BlockOutput(reason=reason).model_dump_json()


def _context(text: str) -> str:
    return ContextOutput(
        hook_specific_output=HookSpecificOutput(
            hook_event_name=HookEventName.USER_PROMPT_SUBMIT,
            additional_context=text,
        )
    ).model_dump_json(by_alias=True)


def _deliver_text(task) -> str:
    if task["task_text"]:
        return task["task_text"]
    return f"execute spec file {task['task_file']}"


def _block_deliver(task) -> str:
    return _block(f"{PARKED_TASK_PREFIX} {_deliver_text(task)}")


# --- task claims -------------------------------------------------------------


def _claim_running(task_id: int, owner: str, args) -> bool:
    """queued > dispatch_decided > running under ``owner``."""
    if not db.claim_queued(
        task_id, owner, Transport.HOOK_QUEUE, path=args.db
    ):
        return False
    if db.mark_running(task_id, owner, path=args.db):
        return True
    # claimed but the running CAS lost: put the task back on the hook
    # queue atomically rather than leaving a wedged dispatch_decided
    db.fallback_to_hook(task_id, owner, path=args.db)
    return False


def _reset_cycles(session_id: str, cfg: ParkConfig | None, args) -> None:
    db.upsert_session(
        session_id,
        park_cycles_left=cfg.cycles_max if cfg else PARK_CYCLES_MAX,
        path=args.db,
    )


def _session_paused(session_id: str, args) -> bool:
    row = db.get_session(session_id, path=args.db)
    return bool(row["queue_paused"]) if row is not None else False


def _flag_awaiting_user(task, owner: str, args, note: str) -> None:
    """Promote a live task to awaiting_user + deduped notify flag.

    ``awaiting_user`` is only reachable from ``running``, so a queued
    task is claimed first; when the claim cannot win (paused session or
    a lost CAS race) the task keeps its status and only the notify
    marker is set.
    """
    tid = task["id"]
    st = task["status"]
    if st == TaskStatus.QUEUED.value:
        if _claim_running(tid, owner, args):
            db.set_awaiting_user(tid, owner, note=note, path=args.db)
        db.mark_notified(tid, path=args.db)
    elif st == TaskStatus.RUNNING.value and _is_hook_owner(
        task["lease_owner"]
    ):
        db.set_awaiting_user(
            tid, task["lease_owner"], note=note, path=args.db
        )
        db.mark_notified(tid, path=args.db)
    # dispatch_decided / acp-owned running / awaiting_user: another
    # worker owns the transition, or the flag is already in place


# --- Stop parking ------------------------------------------------------------


def _inotifywait() -> str | None:
    return shutil.which("inotifywait")


def _consume_trigger(queue_file: str) -> bool | None:
    """Read-and-remove the trigger file.

    Returns ``True`` when a valid JSON trigger was consumed, ``False``
    when an unparseable file was removed (logged), ``None`` when no
    trigger exists. The file is only a wakeup signal, so any valid JSON
    content counts; the db is re-checked for the actual task.
    """
    try:
        with open(queue_file, "rb") as fh:
            raw = fh.read()
    except FileNotFoundError:
        return None
    except OSError as e:
        _log(f"cannot read queue trigger {queue_file}: {e}")
        return None
    try:
        json.loads(raw)
        ok = True
    except json.JSONDecodeError as e:
        _log(f"invalid queue trigger {queue_file}: {e}")
        ok = False
    try:
        os.unlink(queue_file)
    except OSError:
        pass
    return ok


def _wait_for_trigger(queue_dir: str, session_id: str, seconds: float) -> bool:
    """Block up to ``seconds`` for ``queue/<sid>.json``.

    ``inotifywait`` on the queue directory when available, else a ~2s
    poll fallback. Returns True on a consumed valid trigger.
    """
    queue_file = os.path.join(queue_dir, f"{session_id}.json")
    deadline = time.monotonic() + seconds
    watcher = _inotifywait()
    while True:
        seen = _consume_trigger(queue_file)
        if seen is True:
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        if watcher is not None and os.path.isdir(queue_dir):
            try:
                res = subprocess.run(
                    [
                        watcher,
                        "-q",
                        "-t",
                        str(max(1, math.ceil(remaining))),
                        "-e",
                        "create",
                        "-e",
                        "moved_to",
                        "-e",
                        "close_write",
                        queue_dir,
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=remaining + _INOTIFY_GRACE_SECONDS,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired) as e:
                _log(f"inotifywait failed ({e}); polling instead")
                watcher = None
            else:
                # 0 = event seen, 1 = timeout; anything else is a
                # watcher error that would busy-loop - fall back to poll
                if res.returncode not in (0, 1):
                    _log(
                        f"inotifywait exited {res.returncode};"
                        " polling instead"
                    )
                    watcher = None
        else:
            time.sleep(min(_POLL_SECONDS, remaining))


def _cycles_left(session_id: str, args, cfg: ParkConfig | None) -> int:
    """Remaining park budget for the session (fresh = cycles_max)."""
    fallback = cfg.cycles_max if cfg is not None else PARK_CYCLES_MAX
    row = db.get_session(session_id, path=args.db)
    if row is None:
        return fallback
    return min(int(row["park_cycles_left"]), fallback)


def _park_exhausted(session_id: str, owner: str, args):
    """Budget spent or parking pointless: flag and allow the stop.

    The queue is checked one final time before flagging - a task that
    committed during the last cycle must not be silently passed over
    (last-cycle race).
    """
    task = db.get_live_task(session_id, path=args.db)
    if task is None:
        return None
    _flag_awaiting_user(task, owner, args, "park budget exhausted")
    return None


def _park_empty(session_id: str, owner: str, args, cfg: ParkConfig):
    """No live task: wait one cycle for the queue trigger, then decide."""
    session = db.get_session(session_id, path=args.db)
    if session is not None and session["queue_paused"]:
        return None  # a paused queue cannot be claimed; do not burn cycles
    cycles_left = _cycles_left(session_id, args, cfg)
    if cycles_left <= 0:
        return _park_exhausted(session_id, owner, args)

    if _wait_for_trigger(
        _queue_dir(args), session_id, cfg.cycle_seconds
    ):
        task = db.get_live_task(session_id, path=args.db)
        if (
            task is not None
            and task["status"] == TaskStatus.QUEUED.value
            and _claim_running(task["id"], owner, args)
        ):
            _reset_cycles(session_id, cfg, args)
            return _block_deliver(task)

    cycles_left -= 1
    db.upsert_session(
        session_id, park_cycles_left=cycles_left, path=args.db
    )
    if cycles_left <= 0 or _session_paused(session_id, args):
        return _park_exhausted(session_id, owner, args)
    stamp = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    return _block(
        f"[PARKED] {stamp} queue empty; waiting for a parked task"
        f" ({cycles_left}/{cfg.cycles_max} park cycles left)"
    )


def _on_stop(p: StopPayload, args, cfg: ParkConfig | None):
    session_id = p.session_id
    owner = _owner()
    task = db.get_live_task(session_id, path=args.db)

    if task is not None:
        st = task["status"]
        if st == TaskStatus.QUEUED.value:
            # an already-spent park budget means the block chain gave up:
            # flag for a human kick instead of burning another LLM turn
            if _cycles_left(session_id, args, cfg) <= 0:
                _flag_awaiting_user(
                    task, owner, args, "park budget exhausted"
                )
                return None
            if _claim_running(task["id"], owner, args):
                _reset_cycles(session_id, cfg, args)
                return _block_deliver(task)
            # the CAS refused: queue_paused or a racing claimant
            if _session_paused(session_id, args):
                _flag_awaiting_user(
                    task, owner, args, "queue paused; awaiting user kick"
                )
            return None
        if st == TaskStatus.RUNNING.value and _is_hook_owner(
            task["lease_owner"]
        ):
            # the agent finished the turn running the injected task
            _flag_awaiting_user(
                task,
                owner,
                args,
                "delivered task finished; awaiting user",
            )
            return None
        return None  # dispatch_decided / acp running / awaiting_user

    if cfg is None:
        return None
    return _park_empty(session_id, owner, args, cfg)


# --- UserPromptSubmit --------------------------------------------------------


def _on_user_prompt(p: UserPromptSubmitPayload, args, cfg):
    session_id = p.session_id
    text = p.prompt.strip()
    if len(text) > 3:
        # a real prompt: pause queue injection until the next kick
        db.session_pause(session_id, True, path=args.db)
        return None
    # a kick ("", ".", ...) resumes the queue and delivers a queued task
    db.session_pause(session_id, False, path=args.db)
    task = db.get_live_task(session_id, path=args.db)
    if task is None or task["status"] != TaskStatus.QUEUED.value:
        return None
    if not _claim_running(task["id"], _owner(), args):
        return None
    _reset_cycles(session_id, cfg, args)
    return _context(f"{PARKED_TASK_PREFIX} {_deliver_text(task)}")


# --- Session lifecycle -------------------------------------------------------


def _on_session_start(p: SessionStartPayload, args, cfg):
    session_id = p.session_id
    fields: dict = {
        "park_cycles_left": cfg.cycles_max if cfg else PARK_CYCLES_MAX
    }
    cwd = (p.model_extra or {}).get("cwd")
    if isinstance(cwd, str) and cwd:
        fields["cwd"] = cwd
    db.upsert_session(session_id, path=args.db, **fields)
    if p.source == SOURCE_RESUME:
        db.recover_stale(path=args.db)
    return None


def _on_session_end(p: SessionEndPayload, args):
    session_id = p.session_id
    db.upsert_session(
        session_id, note=f"session ended: {p.reason}", path=args.db
    )
    task = db.get_live_task(session_id, path=args.db)
    if task is None:
        return None
    tid, st = task["id"], task["status"]
    owner = _owner()
    if st == TaskStatus.QUEUED.value:
        # the session is dead: a stale pause must not block cleanup
        db.session_pause(session_id, False, path=args.db)
        if _claim_running(tid, owner, args):
            db.finish_task(
                tid,
                owner,
                ok=False,
                note=SESSION_ENDED_NOTE,
                path=args.db,
            )
        else:
            _log(f"session end: cannot retire queued task {tid}")
    elif st == TaskStatus.DISPATCH_DECIDED.value:
        lease_owner = task["lease_owner"]
        if lease_owner and db.mark_running(
            tid, lease_owner, path=args.db
        ):
            db.set_awaiting_user(
                tid, lease_owner, note=SESSION_ENDED_NOTE, path=args.db
            )
        elif lease_owner:
            db.release_lease(tid, lease_owner, path=args.db)
    elif st == TaskStatus.RUNNING.value:
        lease_owner = task["lease_owner"]
        if lease_owner:
            db.set_awaiting_user(
                tid, lease_owner, note=SESSION_ENDED_NOTE, path=args.db
            )
    return None


# --- entry point -------------------------------------------------------------


def _dispatch(payload: HookPayload, args, cfg: ParkConfig | None):
    if isinstance(payload, StopPayload):
        return _on_stop(payload, args, cfg)
    if isinstance(payload, UserPromptSubmitPayload):
        return _on_user_prompt(payload, args, cfg)
    if isinstance(payload, SessionStartPayload):
        return _on_session_start(payload, args, cfg)
    if isinstance(payload, SessionEndPayload):
        return _on_session_end(payload, args)
    _log(f"unhandled payload type: {type(payload).__name__}")
    return None


def main(argv=None) -> int:
    """Hook entry point: read one payload, maybe answer, always exit 0."""
    args = _parse_args(argv)
    try:
        raw = sys.stdin.read()
    except Exception as e:
        _log(f"cannot read stdin: {e}")
        return 0
    try:
        data = json.loads(raw) if raw.strip() else {}
        payload = parse_hook_payload(data)
    except Exception as e:
        _log(f"invalid hook payload: {e}")
        return 0
    cfg = _park_config(args)
    try:
        db.init(args.db)  # idempotent: schema may not exist yet
        out = _dispatch(payload, args, cfg)
    except Exception as e:
        # fail-safe: infra errors must never block the host agent
        _log(f"worker error (allowing event): {e!r}")
        return 0
    if out is not None:
        try:
            sys.stdout.write(out + "\n")
            sys.stdout.flush()
        except (OSError, io.IOBaseError, ValueError) as e:
            _log(f"cannot write stdout: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

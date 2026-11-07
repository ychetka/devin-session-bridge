"""fs mailbox for managed sessions (item_13c).

Layout: ``<queue root>/<sid>/msg_<notification_id>.txt``. The queue
root is the same ``queue/`` directory the hook trigger files live in
(``<state.db dir>/queue`` unless ``--queue-dir`` /
``SESSIONS_BRIDGE_QUEUE_DIR`` overrides it), one subdirectory per
session.

Write/read semantics (proven by the bead-beast spike):

- ``write_message`` is atomic: tmp file + fsync + rename, then a dir
  fsync, so a consumer never sees a half-written ``msg_*.txt``.
- ``next_message`` is level-triggered head-of-queue: the oldest
  pending message (lowest notification id) is returned without being
  removed - it survives across polls and watcher restarts.
- ``consume`` deletes the file after it was read. Consumption is
  idempotent by notification id, so at-least-once relay delivery can
  never double-apply.

The daemon relay (item_13d) is the producer: it turns outbox rows
(``notifications.relayed_at`` NULL) into mailbox files. The managed
session is the consumer, in one of two watcher modes picked by
context (item_15):

- ``resident``: a background watcher subagent runs ``WATCHER_LOOP`` -
  it polls the directory every ``POLL_SECONDS``, prints each pending
  message, removes it and exits after the first non-empty pass so its
  completion lands inside the parent's open turn. The watcher also
  exits on ``WATCHER_TTL_SECONDS`` so its transcript stays bounded;
  the parent re-arms a fresh watcher on every completion and rotates
  the whole turn after ``TURN_TTL_CYCLES`` (the SessionStart hook
  re-arms the duty). For autonomous/headless sessions only: the
  occupied turn slot blocks human input in interactive sessions.
- ``drain``: no resident watcher - the session itself drains the
  mailbox (``ls -v`` / ``cat`` / ``rm``) at the start of every turn
  and once more before the turn ends. For interactive Desktop
  sessions; the trade-off is delivery latency bounded by the next
  turn.
"""

import os
import re
import tempfile

from session_bridge.state import db
from session_bridge.state.contract import TURN_TTL_CYCLES, WATCHER_TTL_SECONDS

__all__ = [
    "POLL_SECONDS",
    "WATCHER_LOOP",
    "consume",
    "mailbox_dir",
    "mailbox_duty_text",
    "next_message",
    "queue_root",
    "watcher_loop_text",
    "write_message",
]

#: Watcher poll cadence (matches the proven ~5s spike loop).
POLL_SECONDS = 5

#: Printed by the watcher when it exits on TTL without deliveries;
#: lets the parent tell a rotation apart from a delivery.
TTL_EXPIRED_MARKER = "WATCHER_TTL_EXPIRED"

#: Printed before each delivered payload so one watcher completion can
#: carry several messages and the parent can split them by file name.
MESSAGE_HEADER = "=== mailbox message {name} ==="

_MSG_RE = re.compile(r"^msg_(.+)\.txt$")
_NAME_SAFE = re.compile(r"[^A-Za-z0-9._-]")

# Bash fragment of the mailbox watcher subagent. Rendered via
# ``watcher_loop_text`` - placeholders are ``{mailbox_dir}``,
# ``{poll_seconds}`` and ``{ttl_seconds}``; the bash body itself
# contains no braces. Level-triggered head-of-line consume:
# ``ls -v | head -1; cat; rm`` drains every pending message, prints each
# with a MESSAGE_HEADER banner, then exits so the completion lands
# inside the parent's open turn. ``ls -v`` (GNU version sort) keeps the
# head-of-queue numeric like ``next_message`` - msg_2 beats msg_10.
WATCHER_LOOP = """\
# sessions-bridge mailbox watcher: poll {mailbox_dir} for msg_*.txt
# every {poll_seconds}s, for up to {ttl_seconds}s. Level-triggered
# head-of-line consume: each pending message is printed and removed,
# then the watcher exits - its completion lands inside the parent turn.
# ls -v = GNU version sort so the lowest numeric id is head-of-queue
# (msg_2 before msg_10), matching next_message's numeric sort.
mailbox_dir="{mailbox_dir}"
deadline=$((SECONDS + {ttl_seconds}))
found=0
while [ "$SECONDS" -lt "$deadline" ]; do
    msg=$(ls -v "$mailbox_dir"/msg_*.txt 2>/dev/null | head -n 1)
    while [ -n "$msg" ]; do
        found=1
        echo "=== mailbox message $(basename "$msg") ==="
        cat "$msg"
        rm -f "$msg"
        msg=$(ls -v "$mailbox_dir"/msg_*.txt 2>/dev/null | head -n 1)
    done
    if [ "$found" -eq 1 ]; then
        exit 0
    fi
    sleep {poll_seconds}
done
echo "{ttl_expired_marker}"
exit 0
"""


def queue_root(db_path=None, queue_dir=None) -> str:
    """Root queue directory (trigger files + per-session mailboxes).

    Precedence: explicit ``queue_dir`` > ``SESSIONS_BRIDGE_QUEUE_DIR``
    env > ``<dir of db_path>/queue`` (``db_path`` defaults to the
    configured state.db path).
    """
    if queue_dir:
        return queue_dir
    override = os.environ.get("SESSIONS_BRIDGE_QUEUE_DIR")
    if override:
        return override
    base = db_path if db_path is not None else db.DB_PATH
    return os.path.join(os.path.dirname(os.path.abspath(base)), "queue")


def _dir_name(session_id: str) -> str:
    """Sanitize a session id into a mailbox directory name.

    Same whitelist as ``db.trigger_filename`` minus the ``.json``
    suffix, so the trigger file and the mailbox dir for one session
    can never collide and neither can escape the queue root.
    """
    return _NAME_SAFE.sub("_", session_id).lstrip(".") or "sid"


def mailbox_dir(session_id: str, *, db_path=None, queue_dir=None) -> str:
    """``<queue root>/<sid>/`` - the session's mailbox directory."""
    return os.path.join(queue_root(db_path, queue_dir), _dir_name(session_id))


def _msg_name(notification_id) -> str:
    """``msg_<notification_id>.txt`` - id drives consume ordering."""
    nid = _NAME_SAFE.sub("_", str(notification_id)).lstrip(".")
    return f"msg_{nid or '0'}.txt"


def _sort_key(name: str):
    """Order msg files by numeric notification id, unparseable last."""
    m = _MSG_RE.match(name)
    if m is None:
        return (1, 0, name)
    nid = m.group(1)
    if nid.isdigit():
        return (0, int(nid), name)
    return (1, 0, name)


def write_message(
    session_id: str,
    notification_id,
    payload_text: str,
    *,
    db_path=None,
    queue_dir=None,
) -> str:
    """Write ``queue/<sid>/msg_<notification_id>.txt`` atomically.

    tmp file + fsync + rename, then a directory fsync so the new entry
    is durable; returns the final path. A concurrent reader can only
    ever see the complete file or nothing at all.
    """
    mdir = mailbox_dir(session_id, db_path=db_path, queue_dir=queue_dir)
    os.makedirs(mdir, exist_ok=True)
    target = os.path.join(mdir, _msg_name(notification_id))
    fd, tmp = tempfile.mkstemp(prefix=".msg-", dir=mdir)
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(payload_text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, target)
        tmp = None
        try:
            dfd = os.open(mdir, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass  # dir fsync is durability polish, not correctness
    finally:
        if tmp is not None:
            try:
                os.unlink(tmp)
            except OSError:
                pass
    return target


def next_message(session_id: str, *, db_path=None, queue_dir=None):
    """Level-triggered head-of-queue: ``(path, text)`` or ``None``.

    The oldest pending ``msg_*.txt`` (lowest notification id) is read
    and returned without being removed - the caller decides when to
    ``consume`` it, so a crash between read and consume re-delivers
    the same message instead of losing it.
    """
    mdir = mailbox_dir(session_id, db_path=db_path, queue_dir=queue_dir)
    try:
        names = [n for n in os.listdir(mdir) if _MSG_RE.match(n)]
    except OSError:
        return None
    if not names:
        return None
    path = os.path.join(mdir, min(names, key=_sort_key))
    try:
        with open(path, "rb") as fh:
            text = fh.read().decode("utf-8", "replace")
    except OSError:
        return None
    return path, text


def consume(path) -> bool:
    """Delete a mailbox file after it was read; idempotent."""
    try:
        os.unlink(path)
        return True
    except OSError:
        return False


def watcher_loop_text(
    mailbox_directory: str,
    *,
    ttl_seconds: int = WATCHER_TTL_SECONDS,
    poll_seconds: int = POLL_SECONDS,
) -> str:
    """Render ``WATCHER_LOOP`` for a concrete mailbox directory."""
    return WATCHER_LOOP.format(
        mailbox_dir=mailbox_directory,
        poll_seconds=poll_seconds,
        ttl_seconds=ttl_seconds,
        ttl_expired_marker=TTL_EXPIRED_MARKER,
    )


def mailbox_duty_text(session_id: str, *, db_path=None, queue_dir=None) -> str:
    """additionalContext for a managed session: the mailbox duty.

    Imperative instructions the SessionStart/UserPromptSubmit hooks
    inject. Two watcher modes, selected by the session itself from
    context: ``resident`` (background watcher subagent, for
    autonomous sessions - the occupied turn slot blocks human input)
    or ``drain`` (self-drain at turn start and end, for interactive
    Desktop sessions). Both process deliveries by notification kind,
    dedup by notification id, ack and exit on disarm; resident also
    rotates the turn after TURN_TTL_CYCLES.
    """
    mdir = mailbox_dir(session_id, db_path=db_path, queue_dir=queue_dir)
    loop = watcher_loop_text(mdir)
    # Optional drain-mode observer: polls the mailbox and heartbeats
    # each cycle, keeping the session inside HEARTBEAT_STALE_SECONDS
    # between turns (a drain-mode session has no turn open to
    # heartbeat from, so only a shell can do it).
    observer = (
        f"while true; do bridge heartbeat {session_id}; "
        f"ls -v {mdir}/msg_*.txt 2>/dev/null; sleep {POLL_SECONDS}; done"
    )
    return f"""[sessions-bridge] MANAGED MODE - mailbox duty (session {session_id})

You are a managed sessions-bridge worker. Tasks, question answers and
orchestrator directives arrive as files in your mailbox directory:
{mdir}

HARD RULE - questions go through the bus, never through the UI:
managed sessions MUST NOT open interactive question dialogs
(ask_user_question or any multiple-choice/UI question widget) -
nobody sits behind your window, so the dialog parks your turn
forever with nobody to answer it. EVERY question to the
orchestrator goes through `bridge ask <task_id> --session
{session_id} "<text>"` only; the answer arrives in this mailbox
as question_answer. If a question blocks you, ask on the bus
and END your turn - the answer is delivered by a wake, never
in-turn. Waiting inside a turn for a dialog or a human reply is
a protocol violation.

Pick your watcher mode NOW - the choice is yours, by context:
if a live human may be sitting behind this session (Desktop/GUI
interactive), use DRAIN mode; if you are an autonomous worker, use
RESIDENT mode. A resident watcher occupies the turn slot, which
blocks the human's input in an interactive session - that is the
live finding this split exists for. Drain mode trades delivery
latency for a free input line.

== MODE RESIDENT - background watcher subagent (autonomous sessions) ==

1. Arm the watcher NOW: spawn a background subagent running this bash
   loop verbatim (never run it inline - its completion must land
   inside this turn):

```bash
{loop}
```

2. Keep working while the watcher lives. On its completion:
   - "{TTL_EXPIRED_MARKER}" = watcher TTL rotation, nothing arrived.
   - Otherwise every "=== mailbox message msg_<id>.txt ===" block is
     one delivery; the file was already removed by the watcher.
     Deliveries are idempotent by notification id: if you already
     handled id N, skip it.
3. Process each delivery by notification kind. Priority rule: when
   a batch contains `halt` or `disarm` deliveries, apply them FIRST
   regardless of file order - they bound the whole turn; everything
   else processes in numeric id order:
   - task_dispatch: the payload is your task. Claim it FIRST:
     `bridge claim <task_id> --session {session_id}` - a refusal
     means the task is already claimed/done elsewhere, skip it.
     On success execute it, then close the lifecycle:
     `bridge finish <task_id> --session {session_id}` (or
     `--fail --note "<why>"` on failure). Repeat `bridge claim`
     on long tasks to renew the lease.
     EXTERNAL WAITS: while blocked on something out-of-band (CI, a
     PR review, a partner consult, an orchestrator reply), call
     `bridge wait <task_id> --session {session_id} --reason "<what>"`
     periodically - it renews your lease and records WHY you are
     parked. A claim that never renews dies on `lease expired`
     exactly like a dead worker.
   - question_answer: apply the answer to your pending question.
     An "expired" notice means no answer arrived in time: the
     pending slot is free - re-ask via `bridge ask` or close the
     task with `bridge finish <task_id> --session {session_id}
     --fail`.
   - directive: obey the orchestrator directive.
   - notice: an informational report - read and incorporate it; it
     is not an instruction. Report back the same way:
     `bridge notice <sid> "<text>"`.
   - heartbeat_tick: liveness poke - no work needed.
   - halt: fleet stop - finish your CURRENT atomic action only, save
     state (commit/push WIP, notes in the task file), run
     `bridge ack --session {session_id}` and END YOUR TURN. Do not
     claim new queued tasks: they persist and resume later, and
     `bridge claim` is refused while halted - a claim refusal that
     says 'halted' is the enforced half of this contract, not a race.
     Stay halted until a `resume` delivery arrives.
   - resume: the halt is lifted - normal mailbox duty resumes; drain
     the backlog and continue per this kind table.
   - disarm: managed mode is off - do NOT re-arm a watcher; resume
     normal unmanaged operation.
4. If `bridge ack` is available, run `bridge ack --session {session_id}`
   after processing to advance your restart-ack watermark.
5. ALWAYS re-arm: before your turn ends, spawn a NEW watcher subagent
   for the next cycle. Never end a managed turn without a live
   watcher unless a disarm arrived.
6. Turn rotation: after {TURN_TTL_CYCLES} watcher cycles in this
   turn, end the turn cleanly - the SessionStart hook re-injects
   this duty and re-arms automatically.

== MODE DRAIN - self-drain each turn (interactive/Desktop sessions) ==

No resident watcher: do NOT spawn a mailbox subagent - it would hold
your turn slot open and block the human's input. You drain the
mailbox yourself, twice per turn:

1. Turn start (right after this duty arrives): run
   `ls -v {mdir}/msg_*.txt 2>/dev/null` (version sort keeps numeric
   order - msg_2 before msg_10), then for each listed file in order: `cat`
   it, handle it by the same notification-kind table as resident
   mode above, and `rm` it. Deliveries are idempotent by
   notification id: if you already handled id N, skip it.
2. Heartbeat: `bridge heartbeat {session_id}` at turn start, the
   same liveness write resident mode does on every watcher cycle.
3. Before your turn ends: run one more drain pass (the same
   `ls -v` / `cat` / `rm`) so nothing that arrived mid-turn sits
   unhandled.
4. If `bridge ack` is available, run `bridge ack --session {session_id}`
   after each drain pass to advance your restart-ack watermark.
5. Optional observation aid only: you may run a light background
   exec-shell (timeout=0) that watches the mailbox, e.g.
   `{observer}`
   - the observer shell keeps your heartbeat alive between turns,
   but a shell cannot inject into your turn, so NEVER wait for a
   push from it; the drain steps above are still yours to run.
6. Latency trade-off: a delivery may wait until your next turn. That
   is the conscious price of keeping the human's input line free -
   do not "fix" it by spawning a resident watcher in an interactive
   session.
7. disarm still applies: on a disarm delivery, managed mode is off -
   stop draining and resume normal unmanaged operation."""

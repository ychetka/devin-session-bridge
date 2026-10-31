"""state.db persistence layer for sessions-bridge.

Implements the c_state contract (contract.py): SQLite WAL storage, a
single live task per session, owner-CAS lease transitions and lazy
lease-expiry recovery.

Concurrency model: every public operation opens its own connection in
autocommit mode (``isolation_level=None``). Each mutation is a single
UPDATE guarded by a WHERE clause on the expected status/lease owner;
``rowcount == 1`` means the CAS won, ``0`` means it lost to a
concurrent transition - no SELECT-then-UPDATE on the claim path.
Writers retry SQLITE_BUSY with exponential backoff on top of the
``busy_timeout`` pragma.

Message bus slice (schema v2, item_13b1): the sessions table gains
``role``/``mode``/``status`` (via an idempotent migration for pre-v2
files), ``worker_heartbeats`` holds the authoritative liveness and
generation rows, and ``register``/``heartbeat`` implement the
orchestrator CAS singleton and the liveness protocol.

Bus slice 2 (item_13b2) adds the worker>orchestrator question
channel (``questions``, one pending per task enforced by a partial
unique index) and the append-only ``audit_events`` journal. Both are
plain CREATE TABLE IF NOT EXISTS additions - an existing state.db
picks them up on the next init() with no migration.

Bus slice 3 (item_13b3) adds the ``notifications`` outbox
(orchestrator>worker and system notices): rows commit with
``relayed_at`` NULL, the daemon relay marks them after the fs mailbox
write (at-least-once delivery, idempotent consume by id), and the
recipient's ``ack_at`` watermark drives level-triggered restart
recovery.
"""

import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from enum import Enum

from .contract import (
    BUSY_TIMEOUT_MS,
    CLAIM_TIMEOUT_SECONDS,
    JOURNAL_MODE,
    LEASE_SECONDS,
    LIVE_STATUSES,
    MAX_ATTEMPTS,
    MAX_QUESTIONS_PER_TASK,
    WRITER_RETRIES,
    AuditEventKind,
    NotificationKind,
    QuestionState,
    SessionMode,
    SessionRole,
    SessionStatus,
    TaskStatus,
    Transport,
    validate_question_transition,
)


class BridgeDBError(Exception):
    """Unrecoverable state.db problem (e.g. a corrupt database file)."""


def _default_db_path() -> str:
    override = os.environ.get("SESSIONS_BRIDGE_DB")
    if override:
        return override
    data_home = os.environ.get("XDG_DATA_HOME") or os.path.expanduser("~/.local/share")
    return os.path.join(data_home, "sessions-bridge", "state.db")


#: Default location of state.db. state.db must live at a fixed absolute
#: path because hook workers run in foreign repositories; env override
#: is provided for tests and manual operation.
DB_PATH = _default_db_path()

#: Devin CLI storage (read-only for us - Devin's acp owns these files).
DEVIN_CLI_DIR = os.path.expanduser("~/.local/share/devin/cli")
DEVIN_SESSIONS_DB = os.path.join(DEVIN_CLI_DIR, "sessions.db")
SESSION_LOCKS_DIR = os.path.join(DEVIN_CLI_DIR, "session_locks")

#: mark_notified refuses to re-flag a task inside this window.
NOTIFY_DEDUP_SECONDS = 300

#: Orchestration heartbeat cadence: every registered session is
#: expected to call heartbeat() within HEARTBEAT_INTERVAL_SECONDS; a
#: heartbeat silent for HEARTBEAT_STALE_SECONDS marks the holder dead
#: (bounded refusal window for the orchestrator CAS singleton).
HEARTBEAT_INTERVAL_SECONDS = 60
HEARTBEAT_STALE_SECONDS = 2 * HEARTBEAT_INTERVAL_SECONDS

_BUSY_BASE_DELAY = 0.1

# Live statuses as a SQL literal list, derived from the contract so the
# enqueue guard and the live-task index can never drift apart.
_LIVE_SQL = ", ".join(f"'{v}'" for v in sorted(s.value for s in LIVE_STATUSES))

_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  task_text TEXT,
  task_file TEXT,
  status TEXT NOT NULL,
  transport TEXT,
  lease_owner TEXT,
  lease_expires_at INTEGER,
  lease_renewed_at INTEGER,
  attempt INTEGER NOT NULL DEFAULT 0,
  hook_pid INTEGER,
  last_notified_at INTEGER,
  last_assistant_message TEXT,
  note TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);

-- At most one live task per session; this is what enqueue() relies on.
CREATE UNIQUE INDEX IF NOT EXISTS ux_tasks_live_session
  ON tasks(session_id) WHERE status IN ({_LIVE_SQL});

-- Supports the status scans (queue depth, recovery, dispatch).
CREATE INDEX IF NOT EXISTS ix_tasks_status ON tasks(status);

CREATE TABLE IF NOT EXISTS sessions (
  session_id TEXT PRIMARY KEY,
  cwd TEXT,
  title TEXT,
  queue_paused INTEGER NOT NULL DEFAULT 0,
  park_cycles_left INTEGER NOT NULL DEFAULT 5,
  role TEXT NOT NULL DEFAULT 'worker',
  mode TEXT NOT NULL DEFAULT 'unmanaged',
  status TEXT NOT NULL DEFAULT 'active',
  note TEXT,
  updated_at INTEGER NOT NULL
);

-- Authoritative liveness + generation rows for the bus (the sessions
-- table is deliberately not denormalized with them).
CREATE TABLE IF NOT EXISTS worker_heartbeats (
  session_id TEXT PRIMARY KEY,
  last_seen_at INTEGER NOT NULL,
  generation INTEGER NOT NULL DEFAULT 0
);

-- Question channel (worker > orchestrator). ``session_id`` is the
-- asking worker, ``claimed_by`` the claiming orchestrator.
CREATE TABLE IF NOT EXISTS questions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id INTEGER NOT NULL,
  session_id TEXT NOT NULL,
  text TEXT NOT NULL,
  state TEXT NOT NULL,
  answer TEXT,
  claimed_by TEXT,
  asked_at INTEGER NOT NULL,
  claimed_at INTEGER,
  answered_at INTEGER
);

-- At most one PENDING question per task; this is what ask() relies on
-- (a single row cannot see its siblings, so the invariant lives in
-- the index, mirroring ux_tasks_live_session).
CREATE UNIQUE INDEX IF NOT EXISTS ux_questions_pending_task
  ON questions(task_id) WHERE state='pending';

-- Supports the orchestrator's pending-question scan.
CREATE INDEX IF NOT EXISTS ix_questions_state ON questions(state);

-- Append-only audit log: deliberation pipeline rows (question >
-- partner outputs > synthesis > directive), escalation doa triggers
-- and turn/watcher rotations. Written once; the single exception is
-- resolve_escalation(), which CAS-fills the directive column on
-- ESCALATION rows (directive IS NULL = an open escalation).
CREATE TABLE IF NOT EXISTS audit_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  kind TEXT NOT NULL,
  session_id TEXT,
  task_id INTEGER,
  question_id INTEGER,
  question_text TEXT,
  partner_outputs TEXT,
  synthesis TEXT,
  directive TEXT,
  doa_trigger TEXT,
  asked_at INTEGER,
  claimed_at INTEGER,
  answered_at INTEGER,
  created_at INTEGER NOT NULL
);

-- Notifications outbox (orchestrator > worker and system notices).
-- ``session_id`` is the recipient. ``relayed_at`` stays NULL until the
-- daemon relay writes the fs mailbox file; ``ack_at`` is the consuming
-- session's restart watermark. ``task_id``/``question_id`` are optional
-- routing refs the consumer correlates deliveries on.
CREATE TABLE IF NOT EXISTS notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  body TEXT NOT NULL,
  task_id INTEGER,
  question_id INTEGER,
  relayed_at INTEGER,
  ack_at INTEGER,
  created_at INTEGER NOT NULL
);

-- Backs the relay loop's unrelayed scan; rows leave the index the
-- moment relayed_at is marked.
CREATE INDEX IF NOT EXISTS ix_notifications_unrelayed
  ON notifications(relayed_at) WHERE relayed_at IS NULL;

PRAGMA user_version=1;
"""

# Columns the v2 schema adds to sessions. CREATE TABLE IF NOT EXISTS
# cannot extend a pre-v2 sessions table, so init() applies these
# ALTERs idempotently (guarded on PRAGMA table_info inside one
# BEGIN IMMEDIATE transaction).
_SESSION_COLUMN_MIGRATIONS = (
    ("role", "TEXT NOT NULL DEFAULT 'worker'"),
    ("mode", "TEXT NOT NULL DEFAULT 'unmanaged'"),
    ("status", "TEXT NOT NULL DEFAULT 'active'"),
)

_SESSION_FIELDS = frozenset({"cwd", "title", "queue_paused", "park_cycles_left", "note"})


def _now() -> int:
    return int(time.time())


def _val(v):
    """Accept both plain strings and contract enums."""
    return v.value if isinstance(v, Enum) else v


def _path(path) -> str:
    return os.fspath(path) if path is not None else DB_PATH


def _is_busy(exc: sqlite3.OperationalError) -> bool:
    return "locked" in str(exc).lower()


def _retry(op, tries: int = WRITER_RETRIES, base: float = _BUSY_BASE_DELAY):
    """Run ``op`` retrying SQLITE_BUSY with exponential backoff."""
    for i in range(tries):
        try:
            return op()
        except sqlite3.OperationalError as e:
            if not _is_busy(e) or i == tries - 1:
                raise
            time.sleep(base * (2**i))


def connect(path=None) -> sqlite3.Connection:
    """Open a state.db connection: Row factory, WAL, busy_timeout.

    Autocommit mode (``isolation_level=None``); single statements are
    atomic and no explicit transaction is required. Raises
    ``BridgeDBError`` when the file exists but is not a readable SQLite
    database; a missing file yields a connection to a fresh empty db.
    """
    target = _path(path)
    con = None
    try:
        con = sqlite3.connect(target, isolation_level=None)
        con.row_factory = sqlite3.Row
        con.execute(f"PRAGMA journal_mode={JOURNAL_MODE}")
        con.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
        # Probe the header so a corrupt file fails here, not mid-op.
        con.execute("PRAGMA schema_version").fetchone()
        return con
    except sqlite3.DatabaseError as e:
        if con is not None:
            con.close()
        if isinstance(e, sqlite3.OperationalError) and _is_busy(e):
            raise  # transient; callers' _retry handles it
        raise BridgeDBError(f"state.db at {target} is not a readable database: {e}") from e


def init(path=None) -> None:
    """Create the schema (idempotent) and stamp user_version=2.

    The v2 sessions-column migration is a check-then-ALTER sequence:
    it must run inside one BEGIN IMMEDIATE transaction so concurrent
    init() calls on a pre-v2 file serialize on the writer lock. Outside
    a transaction both racers could pass the table_info check and the
    loser would die on a "duplicate column name" error, which is not a
    retryable SQLITE_BUSY. A busy BEGIN IMMEDIATE IS retryable, so the
    loser waits out the winner's migration and then finds every column
    already present. user_version=2 commits atomically with the ALTERs,
    so it only ever stamps a file that actually has the v2 columns.
    """
    target = _path(path)
    parent = os.path.dirname(target)
    if parent:
        os.makedirs(parent, exist_ok=True)

    def op():
        con = connect(target)
        try:
            con.executescript(_SCHEMA)
            con.execute("BEGIN IMMEDIATE")
            try:
                existing = {r["name"] for r in con.execute("PRAGMA table_info(sessions)")}
                for col, decl in _SESSION_COLUMN_MIGRATIONS:
                    if col not in existing:
                        con.execute(f"ALTER TABLE sessions ADD COLUMN {col} {decl}")
                con.execute("PRAGMA user_version=2")
                con.execute("COMMIT")
            except BaseException:
                if con.in_transaction:
                    con.execute("ROLLBACK")
                raise
        finally:
            con.close()

    _retry(op)


# --- tasks -----------------------------------------------------------------


def enqueue(
    session_id: str,
    task_text: str | None = None,
    task_file: str | None = None,
    *,
    path=None,
) -> int | None:
    """Insert a queued task; return its id.

    Returns ``None`` when a live task already exists for the session -
    enforced atomically by the ``ux_tasks_live_session`` partial unique
    index, so a concurrent enqueue can never produce two live tasks.
    """
    if not (task_text or task_file):
        raise ValueError("enqueue requires task_text or task_file")
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "INSERT INTO tasks"
                "(session_id,task_text,task_file,status,created_at,updated_at)"
                " VALUES(?,?,?,?,?,?)",
                (
                    session_id,
                    task_text,
                    task_file,
                    TaskStatus.QUEUED.value,
                    ts,
                    ts,
                ),
            )
            return cur.lastrowid
        except sqlite3.IntegrityError:
            return None  # ux_tasks_live_session: live task exists
        finally:
            con.close()

    return _retry(op)


def claim_queued(task_id: int, owner: str, transport, *, path=None) -> bool:
    """CAS ``queued > dispatch_decided`` for ``owner``.

    Tasks of sessions with ``queue_paused=1`` are skipped; a session
    without a sessions row is claimable.

    The claim carries a lease so a crash between the claim and the
    transport does not wedge the task invisibly: ``recover_stale``
    treats an expired dispatch_decided lease like an expired running
    one and the owner must re-confirm via ``mark_running`` before the
    transport runs.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE tasks SET status=?, transport=?, lease_owner=?,"
                " lease_expires_at=?, lease_renewed_at=?, updated_at=?"
                " WHERE id=? AND status=? AND COALESCE("
                "  (SELECT queue_paused FROM sessions"
                "   WHERE session_id=tasks.session_id), 0) = 0",
                (
                    TaskStatus.DISPATCH_DECIDED.value,
                    _val(transport),
                    owner,
                    ts + LEASE_SECONDS,
                    ts,
                    ts,
                    task_id,
                    TaskStatus.QUEUED.value,
                ),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def mark_running(task_id: int, owner: str, *, max_running: int | None = None, path=None) -> bool:
    """CAS ``dispatch_decided > running`` for the claimant; sets lease.

    The CAS requires the claim lease to be owned and unexpired: once a
    lease lapses, ``recover_stale`` owns the transition and a late
    owner can never promote the task (no double transport).

    ``max_running`` adds atomic admission to the same statement: the
    count of ``running`` acp-push tasks must stay below the cap, so
    concurrent pump processes can never oversubscribe the semaphore.
    A False return with the row still dispatch_decided under ``owner``
    means the cap was hit - the caller should ``release_claim``.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            sql = (
                "UPDATE tasks SET status=?, lease_expires_at=?,"
                " lease_renewed_at=?, updated_at=?"
                " WHERE id=? AND status=? AND lease_owner=?"
                " AND lease_expires_at IS NOT NULL"
                " AND lease_expires_at>?"
            )
            args: list = [
                TaskStatus.RUNNING.value,
                ts + LEASE_SECONDS,
                ts,
                ts,
                task_id,
                TaskStatus.DISPATCH_DECIDED.value,
                owner,
                ts,
            ]
            if max_running is not None:
                sql += " AND (SELECT COUNT(*) FROM tasks WHERE status=? AND transport=?)<?"
                args += [
                    TaskStatus.RUNNING.value,
                    Transport.ACP_PUSH.value,
                    int(max_running),
                ]
            cur = con.execute(sql, args)
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def release_claim(task_id: int, owner: str, *, path=None) -> bool:
    """CAS ``dispatch_decided > queued`` dropping the claim and lease.

    Used when admission is refused after ``claim_queued`` (semaphore
    full): the task folds back to queued keeping its transport
    decision instead of sitting in dispatch_decided on a dying lease.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE tasks SET status=?, lease_owner=NULL,"
                " lease_expires_at=NULL, lease_renewed_at=NULL,"
                " updated_at=?"
                " WHERE id=? AND status=? AND lease_owner=?",
                (
                    TaskStatus.QUEUED.value,
                    ts,
                    task_id,
                    TaskStatus.DISPATCH_DECIDED.value,
                    owner,
                ),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def renew_lease(task_id: int, owner: str, *, path=None) -> bool:
    """Extend the lease; CAS on ``status='running' AND lease_owner``."""
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE tasks SET lease_expires_at=?, lease_renewed_at=?,"
                " updated_at=? WHERE id=? AND status=? AND lease_owner=?",
                (
                    ts + LEASE_SECONDS,
                    ts,
                    ts,
                    task_id,
                    TaskStatus.RUNNING.value,
                    owner,
                ),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def release_lease(
    task_id: int,
    owner: str | None = None,
    *,
    expected_status=None,
    path=None,
) -> bool:
    """Clear all lease fields; CAS on owner when one is given.

    ``expected_status`` additionally guards the release on the task
    still being in that status, so a stale cleanup cannot strip the
    lease of a task that already moved on (e.g. dispatch_decided >
    running - a running task without a lease is unrecoverable).
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            sql = (
                "UPDATE tasks SET lease_owner=NULL,"
                " lease_expires_at=NULL, lease_renewed_at=NULL,"
                " updated_at=? WHERE id=?"
            )
            args: list = [ts, task_id]
            if owner is not None:
                sql += " AND lease_owner=?"
                args.append(owner)
            if expected_status is not None:
                sql += " AND status=?"
                args.append(_val(expected_status))
            cur = con.execute(sql, args)
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def requeue_or_fail(task_id: int, note: str | None = None, *, path=None):
    """Lease-expiry recovery for a leased live task.

    Covers ``running`` and ``dispatch_decided``: an expired claim lease
    means the claimant died before the transport ran (a wedge that was
    previously invisible). ``attempt + 1 < MAX_ATTEMPTS`` requeues to
    ``queued``; otherwise the task lands on terminal ``failed`` - a
    repeated wedge is a poison pill, not another requeue. Both branches
    are a single CAS UPDATE guarded on the still-expired lease, so
    concurrent recoveries (and a racing owner re-confirm, which loses
    its CAS the moment the lease lapses) are consistent: exactly one
    side wins. Returns the new ``TaskStatus`` or ``None`` when the task
    was already moved.
    """
    ts = _now()
    guard = (
        " WHERE id=? AND status IN (?, ?) AND lease_expires_at IS NOT NULL AND lease_expires_at<?"
    )
    guard_args = (
        TaskStatus.RUNNING.value,
        TaskStatus.DISPATCH_DECIDED.value,
        ts,
    )

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE tasks SET status=?, attempt=attempt+1,"
                " lease_owner=NULL, lease_expires_at=NULL,"
                " lease_renewed_at=NULL,"
                " note=COALESCE(?,note), updated_at=?" + guard + " AND attempt+1<?",
                (
                    TaskStatus.QUEUED.value,
                    note,
                    ts,
                    task_id,
                    *guard_args,
                    MAX_ATTEMPTS,
                ),
            )
            if cur.rowcount == 1:
                return TaskStatus.QUEUED
            cur = con.execute(
                "UPDATE tasks SET status=?, attempt=attempt+1,"
                " lease_owner=NULL, lease_expires_at=NULL,"
                " lease_renewed_at=NULL,"
                " note=COALESCE(?,note), updated_at=?" + guard + " AND attempt+1>=?",
                (
                    TaskStatus.FAILED.value,
                    note,
                    ts,
                    task_id,
                    *guard_args,
                    MAX_ATTEMPTS,
                ),
            )
            if cur.rowcount == 1:
                return TaskStatus.FAILED
            return None
        finally:
            con.close()

    return _retry(op)


def recover_stale(*, path=None) -> list[int]:
    """Requeue-or-fail every leased task whose lease has expired.

    Covers ``running`` and ``dispatch_decided`` rows; lazy recovery:
    safe and cheap to call on any read path, returns the ids this call
    actually transitioned.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            rows = con.execute(
                "SELECT id FROM tasks WHERE status IN (?, ?)"
                " AND lease_expires_at IS NOT NULL AND lease_expires_at<?",
                (
                    TaskStatus.RUNNING.value,
                    TaskStatus.DISPATCH_DECIDED.value,
                    ts,
                ),
            ).fetchall()
            return [r["id"] for r in rows]
        finally:
            con.close()

    stale = _retry(op)
    recovered = []
    for tid in stale:
        if requeue_or_fail(tid, note="lease expired", path=path) is not None:
            recovered.append(tid)
    return recovered


def requeue_dead_worker(task_id: int, note: str | None = None, *, path=None):
    """Supervisor requeue for a live task whose session went dead.

    ``requeue_or_fail`` is triggered by an expired lease; this writer's
    trigger is the daemon's dead-session verdict (item_13d): any live
    status folds to ``queued`` inside the attempt budget, otherwise to
    terminal ``failed`` - the attempt counter is incremented and the
    lease fields are cleared either way. A single CAS guarded on the
    live-status set keeps a racing ``finish_task`` or claim consistent:
    exactly one side wins. Returns the new ``TaskStatus`` or ``None``
    when the task already left the live set.
    """
    ts = _now()
    guard = f" WHERE id=? AND status IN ({_LIVE_SQL})"

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE tasks SET status=?, attempt=attempt+1,"
                " lease_owner=NULL, lease_expires_at=NULL,"
                " lease_renewed_at=NULL,"
                " note=COALESCE(?,note), updated_at=?" + guard + " AND attempt+1<?",
                (
                    TaskStatus.QUEUED.value,
                    note,
                    ts,
                    task_id,
                    MAX_ATTEMPTS,
                ),
            )
            if cur.rowcount == 1:
                return TaskStatus.QUEUED
            cur = con.execute(
                "UPDATE tasks SET status=?, attempt=attempt+1,"
                " lease_owner=NULL, lease_expires_at=NULL,"
                " lease_renewed_at=NULL,"
                " note=COALESCE(?,note), updated_at=?" + guard + " AND attempt+1>=?",
                (
                    TaskStatus.FAILED.value,
                    note,
                    ts,
                    task_id,
                    MAX_ATTEMPTS,
                ),
            )
            if cur.rowcount == 1:
                return TaskStatus.FAILED
            return None
        finally:
            con.close()

    return _retry(op)


def finish_task(
    task_id: int,
    owner: str,
    ok: bool,
    last_assistant_message: str | None = None,
    note: str | None = None,
    *,
    path=None,
) -> bool:
    """CAS ``running > done|failed`` for the lease owner.

    A stale worker (owner mismatch, e.g. after lease expiry and reclaim)
    loses the CAS and cannot overwrite the current owner's state.
    """
    target = TaskStatus.DONE.value if ok else TaskStatus.FAILED.value
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE tasks SET status=?,"
                " last_assistant_message=COALESCE(?,last_assistant_message),"
                " note=COALESCE(?,note), lease_owner=NULL,"
                " lease_expires_at=NULL, updated_at=?"
                " WHERE id=? AND status=? AND lease_owner=?",
                (
                    target,
                    last_assistant_message,
                    note,
                    ts,
                    task_id,
                    TaskStatus.RUNNING.value,
                    owner,
                ),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def retry(task_id: int, note: str | None = None, *, path=None) -> bool:
    """Manual ``retry`` command: terminal ``failed > queued``.

    Resets ``attempt`` and clears all lease fields. The automatic
    transition map has no edge out of ``failed`` - this is the only exit.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE tasks SET status=?, attempt=0, lease_owner=NULL,"
                " lease_expires_at=NULL, lease_renewed_at=NULL,"
                " note=COALESCE(?,note), updated_at=?"
                " WHERE id=? AND status=?",
                (
                    TaskStatus.QUEUED.value,
                    note,
                    ts,
                    task_id,
                    TaskStatus.FAILED.value,
                ),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def mark_notified(task_id: int, *, path=None) -> bool:
    """Flag a task as notified; deduplicated over NOTIFY_DEDUP_SECONDS.

    Returns True when the flag was set (a notification should go out),
    False when the task was already notified inside the window.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE tasks SET last_notified_at=?, updated_at=?"
                " WHERE id=? AND (last_notified_at IS NULL"
                " OR last_notified_at<=?)",
                (ts, ts, task_id, ts - NOTIFY_DEDUP_SECONDS),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def set_awaiting_user(task_id: int, owner: str, note: str | None = None, *, path=None) -> bool:
    """CAS ``running > awaiting_user`` for the lease owner.

    ``awaiting_user`` is live (still blocks ``enqueue``) but carries no
    lease - lease owner and expiry are cleared. A stale worker loses
    the CAS and cannot park somebody else's run.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE tasks SET status=?, note=COALESCE(?,note),"
                " lease_owner=NULL, lease_expires_at=NULL, updated_at=?"
                " WHERE id=? AND status=? AND lease_owner=?",
                (
                    TaskStatus.AWAITING_USER.value,
                    note,
                    ts,
                    task_id,
                    TaskStatus.RUNNING.value,
                    owner,
                ),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def fallback_to_hook(task_id: int, owner: str, *, path=None) -> bool:
    """CAS ``dispatch_decided > queued`` switching transport to hook-queue.

    The atomic transport fallback: an acp-push refused because the
    session turned out locked requeues the task for the hook path in
    one statement - no half-state. The stale claim's lease is cleared
    so the next claimant starts clean.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE tasks SET status=?, transport=?, lease_owner=NULL,"
                " lease_expires_at=NULL, lease_renewed_at=NULL,"
                " updated_at=? WHERE id=? AND status=? AND lease_owner=?",
                (
                    TaskStatus.QUEUED.value,
                    Transport.HOOK_QUEUE.value,
                    ts,
                    task_id,
                    TaskStatus.DISPATCH_DECIDED.value,
                    owner,
                ),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def demote_running_to_hook(task_id: int, owner: str, *, path=None) -> bool:
    """CAS ``running > queued`` switching transport to hook-queue.

    The running-side mirror of ``fallback_to_hook``: the acp transport
    was already admitted when the session turned out locked after all
    (e.g. session/load answered session_locked), so the task folds
    back to queued on the hook transport in one statement. The lease
    is cleared - a queued task carries no lease.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE tasks SET status=?, transport=?, lease_owner=NULL,"
                " lease_expires_at=NULL, lease_renewed_at=NULL,"
                " updated_at=? WHERE id=? AND status=? AND lease_owner=?",
                (
                    TaskStatus.QUEUED.value,
                    Transport.HOOK_QUEUE.value,
                    ts,
                    task_id,
                    TaskStatus.RUNNING.value,
                    owner,
                ),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def pause(task_id: int, *, path=None) -> bool:
    """Manual ``pause`` command: ``queued > paused``.

    ``paused`` is not a live status, so the task stops blocking
    ``enqueue`` and is skipped by ``claim_queued`` until unparked.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE tasks SET status=?, updated_at=? WHERE id=? AND status=?",
                (TaskStatus.PAUSED.value, ts, task_id, TaskStatus.QUEUED.value),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def unpark(task_id: int, note: str | None = None, *, path=None) -> bool:
    """Manual ``unpark``: ``awaiting_user > queued`` or ``paused > queued``.

    Both edges are declared on the manual path and converge on queued,
    so a single guarded UPDATE covers both. Lease fields are cleared
    defensively. Returns False when the task is not parked/awaiting, or
    when another live task already holds the session slot (the unique
    live-task index rejects the move; the task stays as it is).
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE tasks SET status=?, note=COALESCE(?,note),"
                " lease_owner=NULL, lease_expires_at=NULL,"
                " lease_renewed_at=NULL, updated_at=?"
                " WHERE id=? AND status IN (?, ?)",
                (
                    TaskStatus.QUEUED.value,
                    note,
                    ts,
                    task_id,
                    TaskStatus.AWAITING_USER.value,
                    TaskStatus.PAUSED.value,
                ),
            )
            return cur.rowcount == 1
        except sqlite3.IntegrityError:
            return False  # ux_tasks_live_session: slot already taken
        finally:
            con.close()

    return _retry(op)


def clear(task_id: int, *, path=None) -> bool:
    """Cancel a ``queued`` task by deleting its row.

    Deletion is not a status transition, so only queued tasks are
    clearable - anything further along the lifecycle must be resolved
    through the transition API.
    """

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "DELETE FROM tasks WHERE id=? AND status=?",
                (task_id, TaskStatus.QUEUED.value),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


# --- task reads -------------------------------------------------------------


def get_task(task_id: int, *, path=None):
    def op():
        con = connect(path)
        try:
            return con.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        finally:
            con.close()

    return _retry(op)


def get_live_task(session_id: str, *, path=None):
    def op():
        con = connect(path)
        try:
            return con.execute(
                f"SELECT * FROM tasks WHERE session_id=? AND status IN ({_LIVE_SQL}) ORDER BY id",
                (session_id,),
            ).fetchone()
        finally:
            con.close()

    return _retry(op)


def list_tasks(status=None, *, path=None, limit: int = 50, after_id=None):
    """List tasks (``ORDER BY id`` with a status filter, newest-first
    without one). ``after_id`` paginates the filtered listing by id so
    a dispatch pass can scan the whole queue in capped batches."""

    def op():
        con = connect(path)
        try:
            if status is not None:
                sql = "SELECT * FROM tasks WHERE status=?"
                args: list = [_val(status)]
                if after_id is not None:
                    sql += " AND id>?"
                    args.append(after_id)
                sql += " ORDER BY id LIMIT ?"
                args.append(limit)
                return con.execute(sql, args).fetchall()
            return con.execute("SELECT * FROM tasks ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        finally:
            con.close()

    return _retry(op)


def count_tasks(status=None, *, transport=None, path=None) -> int:
    """Exact ``COUNT(*)`` for a status (optionally a transport too).

    Unlike ``list_tasks`` there is no cap - ``status --json`` reports
    the true queue depth from this even when the embedded task list
    stays truncated.
    """

    def op():
        con = connect(path)
        try:
            sql = "SELECT COUNT(*) AS c FROM tasks"
            conds: list[str] = []
            args: list = []
            if status is not None:
                conds.append("status=?")
                args.append(_val(status))
            if transport is not None:
                conds.append("transport=?")
                args.append(_val(transport))
            if conds:
                sql += " WHERE " + " AND ".join(conds)
            return int(con.execute(sql, args).fetchone()["c"])
        finally:
            con.close()

    return _retry(op)


# --- sessions ---------------------------------------------------------------


def upsert_session(session_id: str, *, path=None, **fields) -> None:
    """Insert-or-update a sessions row (single atomic statement).

    Only whitelisted columns may be written: cwd, title, queue_paused,
    park_cycles_left, note.
    """
    bad = set(fields) - _SESSION_FIELDS
    if bad:
        raise ValueError(f"unknown session fields: {sorted(bad)}")
    ts = _now()
    values = {k: int(v) if isinstance(v, bool) else v for k, v in fields.items()}

    def op():
        con = connect(path)
        try:
            if values:
                cols = [*values, "updated_at"]
                ph = ", ".join("?" for _ in cols)
                sets = ", ".join(f"{c}=excluded.{c}" for c in cols)
                con.execute(
                    f"INSERT INTO sessions(session_id,{', '.join(cols)})"
                    f" VALUES(?,{ph}) ON CONFLICT(session_id)"
                    f" DO UPDATE SET {sets}",
                    (session_id, *values.values(), ts),
                )
            else:
                con.execute(
                    "INSERT INTO sessions(session_id,updated_at)"
                    " VALUES(?,?) ON CONFLICT(session_id) DO NOTHING",
                    (session_id, ts),
                )
        finally:
            con.close()

    _retry(op)


def session_pause(session_id: str, paused: bool, *, path=None) -> None:
    """Set or clear Session.queue_paused (the smart-kick pause flag).

    Creates the sessions row when absent - pausing a session the bridge
    has not seen yet is valid.
    """
    upsert_session(session_id, queue_paused=bool(paused), path=path)


def get_session(session_id: str, *, path=None):
    def op():
        con = connect(path)
        try:
            return con.execute(
                "SELECT * FROM sessions WHERE session_id=?", (session_id,)
            ).fetchone()
        finally:
            con.close()

    return _retry(op)


def decrement_park_cycles(session_id: str, fresh_budget: int, *, path=None) -> int:
    """Atomically consume one park cycle; returns the cycles left.

    Single-statement read-modify-write - no lost update against a
    concurrent SessionStart reset or a second parked hook. A missing
    sessions row is created with ``fresh_budget - 1``, and a persisted
    value above ``fresh_budget`` is clamped down first so a reduced
    configured budget takes effect immediately.
    """
    ts = _now()
    budget = max(int(fresh_budget), 0)

    def op():
        con = connect(path)
        try:
            row = con.execute(
                "INSERT INTO"
                " sessions(session_id,park_cycles_left,updated_at)"
                " VALUES(?,?,?) ON CONFLICT(session_id) DO UPDATE SET"
                " park_cycles_left=MAX(MIN(park_cycles_left,?)-1,0),"
                " updated_at=excluded.updated_at"
                " RETURNING park_cycles_left",
                (session_id, max(budget - 1, 0), ts, budget),
            ).fetchone()
            return int(row["park_cycles_left"])
        finally:
            con.close()

    return _retry(op)


_SESSION_MODES = frozenset(m.value for m in SessionMode)


def set_session_mode(session_id: str, mode, *, path=None) -> None:
    """Set ``sessions.mode`` (managed|unmanaged) for a session.

    ``mode`` is deliberately not in the ``_SESSION_FIELDS`` upsert
    whitelist (item_13b1): arming/disarming managed mode goes through
    this dedicated writer only, so the transition stays explicit. A
    single INSERT ... ON CONFLICT statement - atomic like every other
    writer, and it creates the sessions row when absent (pre-arming a
    session the bridge has not seen yet is valid).
    """
    mode_v = _val(mode)
    if mode_v not in _SESSION_MODES:
        raise ValueError(f"unknown session mode: {mode_v!r}")
    if not session_id or not session_id.strip():
        raise ValueError("set_session_mode requires a session_id")
    ts = _now()

    def op():
        con = connect(path)
        try:
            con.execute(
                "INSERT INTO sessions(session_id,mode,updated_at)"
                " VALUES(?,?,?) ON CONFLICT(session_id)"
                " DO UPDATE SET mode=excluded.mode,"
                " updated_at=excluded.updated_at",
                (session_id, mode_v, ts),
            )
        finally:
            con.close()

    _retry(op)


_SESSION_STATUSES = frozenset(s.value for s in SessionStatus)


def set_session_status(session_id: str, status, *, path=None) -> bool:
    """Set ``sessions.status`` (active|dead); True only on a real flip.

    Supervisor writer (item_13d): like ``mode``, ``status`` stays out
    of the ``_SESSION_FIELDS`` upsert whitelist so the liveness verdict
    goes through this dedicated writer only. The ``status<>target``
    guard makes the write a transition-CAS: only the caller that
    actually flips the bit returns True, which is what dedups the
    daemon's per-cycle dead-marking - the side effects (task requeue,
    orchestrator notify-send) fire once per ``active > dead`` edge,
    and ``register`` revives the row back to ``active``.
    """
    status_v = _val(status)
    if status_v not in _SESSION_STATUSES:
        raise ValueError(f"unknown session status: {status_v!r}")
    if not session_id or not session_id.strip():
        raise ValueError("set_session_status requires a session_id")
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE sessions SET status=?, updated_at=? WHERE session_id=? AND status<>?",
                (status_v, ts, session_id, status_v),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


# --- message bus: register + heartbeat ----------------------------------------


# Shared upsert for register() and heartbeat(): the first sighting of a
# session lands at generation 1, every later call bumps it - a caller
# holding an older generation can tell it has been superseded.
_HEARTBEAT_UPSERT = (
    "INSERT INTO worker_heartbeats(session_id,last_seen_at,generation)"
    " VALUES(?,?,1) ON CONFLICT(session_id) DO UPDATE SET"
    " last_seen_at=excluded.last_seen_at, generation=generation+1"
    " RETURNING generation"
)


@dataclass(frozen=True)
class RegisterResult:
    """Outcome of ``register()``.

    ``accepted=False`` means the orchestrator CAS refused: ``holder``
    is the live orchestrator's session_id and ``heartbeat_age`` the age
    of its heartbeat in seconds. On acceptance ``generation`` is the
    session's new heartbeat generation.
    """

    accepted: bool
    session_id: str
    role: str
    holder: str | None = None
    heartbeat_age: int | None = None
    generation: int | None = None


def register(
    session_id: str,
    workspace: str,
    role="worker",
    force: bool = False,
    *,
    path=None,
) -> RegisterResult:
    """Register a session on the bus; returns a ``RegisterResult``.

    The contract's ``workspace`` maps to the sessions.cwd column.
    Worker registrations are a plain upsert plus a heartbeat write
    (generation bump). Orchestrator registrations are a CAS singleton:
    while another session holds ``role='orchestrator'`` with a fresh
    heartbeat (younger than HEARTBEAT_STALE_SECONDS) the registration
    is refused - no state is written and the result carries the holder
    sid plus its heartbeat age. A stale or heartbeat-less holder is
    demoted to worker and the new sid takes over; ``force=True``
    demotes any holder unconditionally. The whole read-decide-write
    runs inside one BEGIN IMMEDIATE transaction so concurrent
    orchestrator registrations serialize on the SQLite writer lock.

    Registering is a liveness signal, so the sessions row is written
    with ``status='active'`` (a session the supervisor marked dead
    revives itself by re-registering).
    """
    role_v = _val(role)
    if role_v not in (SessionRole.WORKER.value, SessionRole.ORCHESTRATOR.value):
        raise ValueError(f"unknown session role: {role_v!r}")
    ts = _now()

    def op():
        con = connect(path)
        try:
            con.execute("BEGIN IMMEDIATE")
            try:
                if role_v == SessionRole.ORCHESTRATOR.value and not force:
                    holder = con.execute(
                        "SELECT s.session_id AS sid, wh.last_seen_at AS seen"
                        " FROM sessions s"
                        " JOIN worker_heartbeats wh"
                        "  ON wh.session_id=s.session_id"
                        " WHERE s.role=? AND s.session_id<>? AND wh.last_seen_at>?",
                        (
                            SessionRole.ORCHESTRATOR.value,
                            session_id,
                            ts - HEARTBEAT_STALE_SECONDS,
                        ),
                    ).fetchone()
                    if holder is not None:
                        con.execute("ROLLBACK")
                        return RegisterResult(
                            accepted=False,
                            session_id=session_id,
                            role=role_v,
                            holder=holder["sid"],
                            heartbeat_age=ts - int(holder["seen"]),
                        )
                if role_v == SessionRole.ORCHESTRATOR.value:
                    # singleton takeover: demote every other holder
                    # (stale on the normal path, any when forced)
                    con.execute(
                        "UPDATE sessions SET role=?, updated_at=? WHERE role=? AND session_id<>?",
                        (
                            SessionRole.WORKER.value,
                            ts,
                            SessionRole.ORCHESTRATOR.value,
                            session_id,
                        ),
                    )
                con.execute(
                    "INSERT INTO sessions(session_id,cwd,role,status,updated_at)"
                    " VALUES(?,?,?,?,?)"
                    " ON CONFLICT(session_id) DO UPDATE SET"
                    " cwd=excluded.cwd, role=excluded.role,"
                    " status=excluded.status, updated_at=excluded.updated_at",
                    (session_id, workspace, role_v, SessionStatus.ACTIVE.value, ts),
                )
                gen = con.execute(_HEARTBEAT_UPSERT, (session_id, ts)).fetchone()["generation"]
                con.execute("COMMIT")
                return RegisterResult(
                    accepted=True,
                    session_id=session_id,
                    role=role_v,
                    generation=int(gen),
                )
            except BaseException:
                if con.in_transaction:
                    con.execute("ROLLBACK")
                raise
        finally:
            con.close()

    return _retry(op)


def heartbeat(session_id: str, *, path=None) -> int:
    """Refresh ``last_seen_at`` for a session; returns the generation.

    Inserts the worker_heartbeats row when missing (generation 1) and
    bumps the generation on every call, per the contract: a caller
    holding an older generation detects it has been superseded.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            row = con.execute(_HEARTBEAT_UPSERT, (session_id, ts)).fetchone()
            return int(row["generation"])
        finally:
            con.close()

    return _retry(op)


# --- message bus: questions ---------------------------------------------------


@dataclass(frozen=True)
class AskResult:
    """Outcome of ``ask()``.

    On ``accepted=False`` ``reason`` says why the ask was refused:
    ``"question_limit"`` when the task already carries
    MAX_QUESTIONS_PER_TASK questions counting every state (the
    escalation to the human lives above this layer - here it is only
    the refusal), ``"pending_exists"`` when another pending question
    already holds the task's slot.
    """

    accepted: bool
    question_id: int | None = None
    reason: str | None = None


def ask(session_id: str, task_id: int, text: str, *, path=None) -> AskResult:
    """Post a worker question for ``task_id``; returns an ``AskResult``.

    Two refusals, both structured in the result: the per-task question
    budget (MAX_QUESTIONS_PER_TASK across all states) and a second
    pending question on the same task. The pending guard is atomic -
    the ``ux_questions_pending_task`` partial unique index turns a
    concurrent double-ask into an IntegrityError for the loser, so no
    SELECT-then-INSERT race can produce two pending rows.
    """
    if not text:
        raise ValueError("ask requires question text")
    ts = _now()

    def op():
        con = connect(path)
        try:
            count = con.execute(
                "SELECT COUNT(*) AS c FROM questions WHERE task_id=?",
                (task_id,),
            ).fetchone()["c"]
            if count >= MAX_QUESTIONS_PER_TASK:
                return AskResult(accepted=False, reason="question_limit")
            try:
                cur = con.execute(
                    "INSERT INTO questions(task_id,session_id,text,state,asked_at)"
                    " VALUES(?,?,?,?,?)",
                    (
                        task_id,
                        session_id,
                        text,
                        QuestionState.PENDING.value,
                        ts,
                    ),
                )
            except sqlite3.IntegrityError:
                return AskResult(accepted=False, reason="pending_exists")
            return AskResult(accepted=True, question_id=cur.lastrowid)
        finally:
            con.close()

    return _retry(op)


def claim_question(question_id: int, claimant: str, *, path=None) -> bool:
    """CAS ``pending > claimed`` for ``claimant``; rowcount decides.

    A single atomic UPDATE guarded on ``state='pending'`` - concurrent
    claims yield exactly one winner, a late claim on a claimed or
    answered question simply loses.
    """
    if not claimant or not claimant.strip():
        raise ValueError("claim_question requires a claimant")
    validate_question_transition(QuestionState.PENDING, QuestionState.CLAIMED)
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE questions SET state=?, claimed_by=?, claimed_at=? WHERE id=? AND state=?",
                (
                    QuestionState.CLAIMED.value,
                    claimant,
                    ts,
                    question_id,
                    QuestionState.PENDING.value,
                ),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def answer_question(
    question_id: int,
    answer_text: str,
    *,
    claimant: str | None = None,
    path=None,
) -> bool:
    """CAS ``claimed > answered``; stores the answer verbatim.

    Returns False on a pending question (not yet claimed) and on an
    already answered one - ``answered`` is terminal. ``claimant``
    optionally binds the CAS to ``claimed_by``: an orchestrator whose
    claim was released and re-taken by a successor (janitor timeout)
    loses the CAS instead of answering into somebody else's slot.
    """
    if answer_text is None:
        raise ValueError("answer_question requires answer text")
    validate_question_transition(QuestionState.CLAIMED, QuestionState.ANSWERED)
    ts = _now()

    def op():
        con = connect(path)
        try:
            sql = "UPDATE questions SET state=?, answer=?, answered_at=? WHERE id=? AND state=?"
            args: list = [
                QuestionState.ANSWERED.value,
                answer_text,
                ts,
                question_id,
                QuestionState.CLAIMED.value,
            ]
            if claimant is not None:
                sql += " AND claimed_by=?"
                args.append(claimant)
            cur = con.execute(sql, args)
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def janitor_questions(*, path=None) -> list[int]:
    """Release claims older than CLAIM_TIMEOUT_SECONDS back to pending.

    Mirrors ``Question.release_claim``: a single UPDATE returns every
    stale ``claimed`` row to ``pending`` with the claim fields cleared,
    keeping the row (and its id) so a claim timeout never produces a
    duplicate re-ask. A stale claim is skipped while a newer pending
    question already holds the task's slot - releasing it would trip
    the unique pending index, so it stays claimed for the next pass.

    At most one stale claim per task is released per pass (the lowest
    id): the pending guard is evaluated against the pre-update
    snapshot, so two stale claims on one task would both pass it and
    the second would abort the whole sweep on the unique pending
    index. Remaining stale claims are caught by the next pass once the
    slot frees. Returns the ids this call actually released.
    """
    validate_question_transition(QuestionState.CLAIMED, QuestionState.PENDING, janitor=True)
    cutoff = _now() - CLAIM_TIMEOUT_SECONDS

    def op():
        con = connect(path)
        try:
            rows = con.execute(
                "UPDATE questions SET state=?, claimed_by=NULL,"
                " claimed_at=NULL"
                " WHERE state=? AND claimed_at<?"
                " AND NOT EXISTS ("
                "  SELECT 1 FROM questions q2"
                "  WHERE q2.task_id=questions.task_id AND q2.state=?)"
                " AND id = ("
                "  SELECT MIN(q3.id) FROM questions q3"
                "  WHERE q3.task_id=questions.task_id"
                "  AND q3.state=? AND q3.claimed_at<?)"
                " RETURNING id",
                (
                    QuestionState.PENDING.value,
                    QuestionState.CLAIMED.value,
                    cutoff,
                    QuestionState.PENDING.value,
                    QuestionState.CLAIMED.value,
                    cutoff,
                ),
            ).fetchall()
            return [r["id"] for r in rows]
        finally:
            con.close()

    return _retry(op)


def get_question(question_id: int, *, path=None):
    def op():
        con = connect(path)
        try:
            return con.execute("SELECT * FROM questions WHERE id=?", (question_id,)).fetchone()
        finally:
            con.close()

    return _retry(op)


def list_pending_questions(*, task_id=None, path=None):
    """Pending questions in ask order (``ORDER BY id``), the queue the
    orchestrator deliberates over. ``task_id`` narrows the scan to a
    single task."""

    def op():
        con = connect(path)
        try:
            if task_id is not None:
                return con.execute(
                    "SELECT * FROM questions WHERE state=? AND task_id=? ORDER BY id",
                    (QuestionState.PENDING.value, task_id),
                ).fetchall()
            return con.execute(
                "SELECT * FROM questions WHERE state=? ORDER BY id",
                (QuestionState.PENDING.value,),
            ).fetchall()
        finally:
            con.close()

    return _retry(op)


def pending_questions_count(task_id: int, *, path=None) -> int:
    """COUNT of pending questions for a task (0 or 1 - the partial
    unique index caps it)."""

    def op():
        con = connect(path)
        try:
            return int(
                con.execute(
                    "SELECT COUNT(*) AS c FROM questions WHERE task_id=? AND state=?",
                    (task_id, QuestionState.PENDING.value),
                ).fetchone()["c"]
            )
        finally:
            con.close()

    return _retry(op)


# --- message bus: audit log ---------------------------------------------------

_AUDIT_KINDS = frozenset(k.value for k in AuditEventKind)


def audit(
    kind,
    *,
    session_id: str | None = None,
    task_id: int | None = None,
    question_id: int | None = None,
    question_text: str | None = None,
    partner_outputs=None,
    synthesis: str | None = None,
    directive: str | None = None,
    doa_trigger: str | None = None,
    asked_at: int | None = None,
    claimed_at: int | None = None,
    answered_at: int | None = None,
    path=None,
) -> int:
    """Append one row to ``audit_events``; returns the event id.

    Append-only: the journal is written, never updated. ``kind``
    accepts the contract enum or its string value. ``partner_outputs``
    is a dict keyed by partner name stored verbatim as JSON text (a
    pre-serialized JSON string is stored as-is); it defaults to the
    contract's empty dict.
    """
    kind_v = _val(kind)
    if kind_v not in _AUDIT_KINDS:
        raise ValueError(f"unknown audit event kind: {kind_v!r}")
    if isinstance(partner_outputs, str):
        outputs_json = partner_outputs
    else:
        outputs_json = json.dumps(partner_outputs or {})
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "INSERT INTO audit_events"
                "(kind,session_id,task_id,question_id,question_text,"
                " partner_outputs,synthesis,directive,doa_trigger,"
                " asked_at,claimed_at,answered_at,created_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    kind_v,
                    session_id,
                    task_id,
                    question_id,
                    question_text,
                    outputs_json,
                    synthesis,
                    directive,
                    doa_trigger,
                    asked_at,
                    claimed_at,
                    answered_at,
                    ts,
                ),
            )
            return cur.lastrowid
        finally:
            con.close()

    return _retry(op)


# --- message bus: notifications outbox ----------------------------------------

_NOTIFICATION_KINDS = frozenset(k.value for k in NotificationKind)


def notify(
    session_id: str,
    kind,
    body: str,
    task_id: int | None = None,
    question_id: int | None = None,
    *,
    path=None,
) -> int:
    """Append one row to the notifications outbox; returns its id.

    The row commits with ``relayed_at``/``ack_at`` NULL: the daemon
    relay loop picks it up via the ``ix_notifications_unrelayed``
    partial index, writes the fs mailbox file and then marks
    ``relayed_at``. ``kind`` accepts the contract enum or its string
    value; ``task_id``/``question_id`` are optional routing refs the
    consumer correlates deliveries on.
    """
    if not session_id or not session_id.strip():
        raise ValueError("notify requires a session_id")
    kind_v = _val(kind)
    if kind_v not in _NOTIFICATION_KINDS:
        raise ValueError(f"unknown notification kind: {kind_v!r}")
    if not body:
        raise ValueError("notify requires a body")
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "INSERT INTO notifications"
                "(session_id,kind,body,task_id,question_id,created_at)"
                " VALUES(?,?,?,?,?,?)",
                (session_id, kind_v, body, task_id, question_id, ts),
            )
            return cur.lastrowid
        finally:
            con.close()

    return _retry(op)


def unrelayed_notifications(session_id: str | None = None, *, path=None) -> list:
    """Outbox rows still waiting for the relay, oldest first.

    ``WHERE relayed_at IS NULL`` is backed by the
    ``ix_notifications_unrelayed`` partial index - this is the scan the
    daemon relay loop runs, so a row committed but never marked (relay
    killed between the two steps) is always picked up on the next
    pass. ``session_id`` narrows the scan to one recipient.
    """

    def op():
        con = connect(path)
        try:
            if session_id is not None:
                return con.execute(
                    "SELECT * FROM notifications WHERE relayed_at IS NULL"
                    " AND session_id=? ORDER BY id",
                    (session_id,),
                ).fetchall()
            return con.execute(
                "SELECT * FROM notifications WHERE relayed_at IS NULL ORDER BY id"
            ).fetchall()
        finally:
            con.close()

    return _retry(op)


def mark_relayed(notification_id: int, *, path=None) -> bool:
    """CAS ``relayed_at NULL > now`` after the fs mailbox write.

    A single guarded UPDATE: exactly one caller wins the mark, so a
    relay crash between the fs write and the mark re-delivers the row
    on the next pass (at-least-once) while a replayed mark loses the
    CAS and changes nothing - no duplicate relay signal.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE notifications SET relayed_at=? WHERE id=? AND relayed_at IS NULL",
                (ts, notification_id),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def unacked_notifications(session_id: str, *, path=None) -> list:
    """Notifications the recipient has not acked yet, oldest first.

    This is the restart-recovery read: the ack watermark is
    level-triggered, so a restarted session sees exactly the rows it
    still must process - the watcher dedups by ``id`` on consumption.
    """

    def op():
        con = connect(path)
        try:
            return con.execute(
                "SELECT * FROM notifications WHERE session_id=? AND ack_at IS NULL ORDER BY id",
                (session_id,),
            ).fetchall()
        finally:
            con.close()

    return _retry(op)


def ack_notifications(session_id: str, up_to_id: int | None = None, *, path=None) -> int:
    """Advance the recipient's ack watermark; returns rows acked.

    Sets ``ack_at`` on every unacked row for ``session_id`` (or only up
    to ``up_to_id``). The ``ack_at IS NULL`` guard keeps it idempotent:
    rows already acked are never touched again, so a repeated call
    only moves the watermark forward over newly arrived rows.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            sql = "UPDATE notifications SET ack_at=? WHERE session_id=? AND ack_at IS NULL"
            args: list = [ts, session_id]
            if up_to_id is not None:
                sql += " AND id<=?"
                args.append(up_to_id)
            cur = con.execute(sql, args)
            return cur.rowcount
        finally:
            con.close()

    return _retry(op)


# --- message bus: escalations (item_13e1) -------------------------------------


def get_audit_event(event_id: int, *, path=None):
    """Fetch one ``audit_events`` row by id (None when absent)."""

    def op():
        con = connect(path)
        try:
            return con.execute("SELECT * FROM audit_events WHERE id=?", (event_id,)).fetchone()
        finally:
            con.close()

    return _retry(op)


def list_open_escalations(*, path=None) -> list:
    """ESCALATION rows still waiting on a human directive, oldest first.

    An escalation is open while its ``directive`` column is NULL; this
    is the queue ``bridge escalate show`` renders.
    """

    def op():
        con = connect(path)
        try:
            return con.execute(
                "SELECT * FROM audit_events WHERE kind=? AND directive IS NULL ORDER BY id",
                (AuditEventKind.ESCALATION.value,),
            ).fetchall()
        finally:
            con.close()

    return _retry(op)


def resolve_escalation(event_id: int, directive: str, *, path=None) -> bool:
    """CAS-fill ``directive`` on an open ESCALATION audit row.

    The single mutation ever applied to the append-only journal: an
    ESCALATION row is open while ``directive IS NULL`` and exactly one
    caller wins the fill - a repeated ``escalate reply`` loses the CAS
    and is reported as already resolved. Returns False when the row is
    missing, not an escalation, or already carries a directive.
    """
    if not directive:
        raise ValueError("resolve_escalation requires a directive")

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                "UPDATE audit_events SET directive=? WHERE id=? AND kind=? AND directive IS NULL",
                (directive, event_id, AuditEventKind.ESCALATION.value),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def _block_task(task_id: int, target, note: str | None, *, path=None) -> bool:
    """Shared CAS for the human-blocking escalation directives.

    Moves any live task to ``target`` (paused|failed) in one guarded
    UPDATE: the lease fields are cleared so the terminal/parked row
    holds no stale lease, and the live-status guard makes the write a
    transition-CAS - a task that already left the live set (finished,
    cleared, already blocked by a previous reply) is untouched and the
    caller sees False.
    """
    ts = _now()

    def op():
        con = connect(path)
        try:
            cur = con.execute(
                f"UPDATE tasks SET status=?, note=COALESCE(?,note),"
                f" lease_owner=NULL, lease_expires_at=NULL,"
                f" lease_renewed_at=NULL, updated_at=?"
                f" WHERE id=? AND status IN ({_LIVE_SQL})",
                (_val(target), note, ts, task_id),
            )
            return cur.rowcount == 1
        finally:
            con.close()

    return _retry(op)


def defer_task(task_id: int, note: str | None = None, *, path=None) -> bool:
    """Escalation directive ``defer``: park a live task on ``paused``.

    ``queued > paused`` is the declared manual edge; for the other
    live statuses the same park is applied as a human override - the
    escalation reply is a human decision recorded outside the
    automatic transition map (no new status is introduced). A paused
    task is not live, so the session's live-task slot frees for the
    next queued task while the row keeps its note.
    """
    return _block_task(task_id, TaskStatus.PAUSED, note, path=path)


def abort_task(task_id: int, note: str | None = None, *, path=None) -> bool:
    """Escalation directive ``abort``: drop a live task to ``failed``.

    Terminal like any other ``failed`` - only a manual ``retry``
    revives it. For ``running`` this mirrors ``finish_task``'s
    running > failed edge without requiring the lease owner (a human
    abort must not depend on the zombie it is cancelling); for the
    other live statuses it lands the same human-override terminal.
    """
    return _block_task(task_id, TaskStatus.FAILED, note, path=path)


# --- queue trigger naming -----------------------------------------------------


_TRIGGER_SAFE = re.compile(r"[^A-Za-z0-9._-]")


def trigger_filename(session_id: str) -> str:
    """Map a session id to a queue-trigger file name, sanitized.

    Session ids arrive over the hook/CLI boundary unchecked; the
    whitelist keeps the file inside the queue directory (slashes and
    ``..`` segments cannot escape). The write and the consume sides
    share this mapping so they always agree on the file name.
    """
    name = _TRIGGER_SAFE.sub("_", session_id).lstrip(".")
    return f"{name or 'sid'}.json"


# --- devin cli integration (read-only) --------------------------------------


def sessions_db_lookup(session_id: str, *, db_path=None) -> dict | None:
    """Read-only lookup into Devin's own sessions.db.

    Small retry on busy; graceful ``None`` when the file is missing or
    unreadable. Never writes - Devin's acp owns that file.
    """
    target = os.path.expanduser(db_path or DEVIN_SESSIONS_DB)
    if not os.path.exists(target):
        return None

    def op():
        con = None
        try:
            con = sqlite3.connect(f"file:{target}?mode=ro", uri=True, timeout=1)
            con.row_factory = sqlite3.Row
            row = con.execute("SELECT * FROM sessions WHERE id=?", (session_id,)).fetchone()
            return dict(row) if row is not None else None
        except sqlite3.Error as e:
            if isinstance(e, sqlite3.OperationalError) and _is_busy(e):
                raise  # transient; retried below
            return None  # unreadable file or unexpected schema
        finally:
            if con is not None:
                con.close()

    try:
        return _retry(op, tries=3, base=0.1)
    except sqlite3.OperationalError:
        return None


def is_locked(session_id: str) -> tuple[bool, int | None]:
    """Whether the session is held by a live devin process.

    Reads the lock-file PID and checks ``/proc/<pid>/cmdline`` for a
    process actually named 'devin' - the proc read doubles as the
    liveness check and guards against PID reuse. Returns
    ``(locked, pid_or_None)``.
    """
    lock = os.path.join(os.path.expanduser(SESSION_LOCKS_DIR), f"{session_id}.lock")
    try:
        with open(lock) as f:
            pid = int(f.read().strip())
    except (OSError, ValueError):
        return False, None
    if _pid_is_devin(pid):
        return True, pid
    return False, pid


def _pid_is_devin(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            argv0 = f.read().split(b"\0", 1)[0]
    except OSError:
        return False
    # match the process name (basename of argv[0]), not any 'devin'
    # substring in the cmdline - a recycled pid with devin in a path
    # argument must not mark the session locked
    return argv0.rsplit(b"/", 1)[-1] in (b"devin", b"devin.exe")

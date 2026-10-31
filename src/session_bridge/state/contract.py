"""c_state: domain contract for the state.db persistence layer.

state.db (SQLite) is the single source of truth for sessions-bridge.
This module pins the storage constants (WAL + busy timeout), the task
status lifecycle, the lease fields and the retry boundary:

- automatic transitions follow ``TRANSITIONS`` only; ``failed`` has no
  outgoing edges, so an auto-requeue after ``failed`` is not expressible.
- the manual ``retry`` command (``Task.retry``) is the only exit from
  ``failed``: it moves the task back to ``queued`` and resets ``attempt``.

Message bus schema v2 (item_13a) extends the contract with the
orchestration protocol tables: session roles/modes, the question
claim protocol, the notifications outbox, worker heartbeats (the
authoritative liveness/generation rows) and the append-only audit
log.
"""

from enum import Enum
from typing import Final

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)

# --- storage constants (contract requirements, not tunables) -------------

JOURNAL_MODE: Final[str] = "wal"
BUSY_TIMEOUT_MS: Final[int] = 5000
WRITER_RETRIES: Final[int] = 5

# --- lease and retry policy ----------------------------------------------

LEASE_SECONDS: Final[int] = 1800
MAX_ATTEMPTS: Final[int] = 2  # one auto-requeue, then terminal failed


class TaskStatus(str, Enum):
    """Task lifecycle status. Live: queued, dispatch_decided, running,
    awaiting_user. Terminal: done, failed."""

    QUEUED = "queued"
    DISPATCH_DECIDED = "dispatch_decided"
    RUNNING = "running"
    AWAITING_USER = "awaiting_user"
    DONE = "done"
    FAILED = "failed"
    PAUSED = "paused"


LIVE_STATUSES: Final[frozenset] = frozenset(
    {
        TaskStatus.QUEUED,
        TaskStatus.DISPATCH_DECIDED,
        TaskStatus.RUNNING,
        TaskStatus.AWAITING_USER,
    }
)
TERMINAL_STATUSES: Final[frozenset] = frozenset({TaskStatus.DONE, TaskStatus.FAILED})


class Transport(str, Enum):
    """How a task reaches a session."""

    ACP_PUSH = "acp-push"
    HOOK_QUEUE = "hook-queue"


# Automatic transition map, driven by the dispatcher/worker machinery.
# Lifecycle: queued > dispatch_decided > running >
# {done, failed, requeue(queued), awaiting_user}.
# dispatch_decided > queued is the atomic transport fallback (e.g. an ACP
# push refused because the session turned out locked requeues for the
# hook path). dispatch_decided > queued/failed is the lease-expiry
# recovery of a dead claimant (requeue within the attempt budget, then
# failed - a repeated wedge is a poison pill). FAILED has no outgoing
# edges: it is terminal.
TRANSITIONS: Final[dict] = {
    TaskStatus.QUEUED: frozenset({TaskStatus.DISPATCH_DECIDED}),
    TaskStatus.DISPATCH_DECIDED: frozenset(
        {TaskStatus.RUNNING, TaskStatus.QUEUED, TaskStatus.FAILED}
    ),
    TaskStatus.RUNNING: frozenset(
        {
            TaskStatus.DONE,
            TaskStatus.FAILED,
            TaskStatus.QUEUED,
            TaskStatus.AWAITING_USER,
        }
    ),
    TaskStatus.AWAITING_USER: frozenset(),
    TaskStatus.PAUSED: frozenset(),
    TaskStatus.DONE: frozenset(),
    TaskStatus.FAILED: frozenset(),
}

# Edges reserved for explicit operator commands (unpark, retry, pause).
# The only exit from FAILED lives here: manual retry > queued.
MANUAL_TRANSITIONS: Final[dict] = {
    TaskStatus.FAILED: frozenset({TaskStatus.QUEUED}),
    TaskStatus.AWAITING_USER: frozenset({TaskStatus.QUEUED}),
    TaskStatus.PAUSED: frozenset({TaskStatus.QUEUED}),
    TaskStatus.QUEUED: frozenset({TaskStatus.PAUSED}),
}


def allowed_targets(from_status: TaskStatus, *, manual: bool = False) -> frozenset:
    """Statuses reachable from ``from_status`` on the given path."""
    table = MANUAL_TRANSITIONS if manual else TRANSITIONS
    return table.get(TaskStatus(from_status), frozenset())


def is_valid_transition(
    from_status: TaskStatus, to_status: TaskStatus, *, manual: bool = False
) -> bool:
    """True when ``from_status > to_status`` is a declared edge."""
    return TaskStatus(to_status) in allowed_targets(from_status, manual=manual)


def validate_transition(
    from_status: TaskStatus, to_status: TaskStatus, *, manual: bool = False
) -> None:
    """Raise ``ValidationError`` unless the edge is declared."""
    Transition(from_status=from_status, to_status=to_status, manual=manual)


class Transition(BaseModel):
    """A single status change, validated against the transition maps."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    from_status: TaskStatus
    to_status: TaskStatus
    manual: bool = False

    @model_validator(mode="after")
    def _edge_is_declared(self) -> "Transition":
        if not is_valid_transition(self.from_status, self.to_status, manual=self.manual):
            path = "manual" if self.manual else "automatic"
            raise ValueError(
                f"{path} transition {self.from_status.value} > "
                f"{self.to_status.value} is not allowed"
            )
        return self


class Task(BaseModel):
    """Row model for the ``tasks`` table."""

    model_config = ConfigDict(extra="forbid")

    id: int | None = Field(default=None, ge=1)  # SQLite AUTOINCREMENT
    session_id: str = Field(min_length=1)
    task_text: str | None = None
    task_file: str | None = None
    status: TaskStatus = TaskStatus.QUEUED
    transport: Transport | None = None
    lease_owner: str | None = None
    lease_expires_at: int | None = None
    lease_renewed_at: int | None = None
    attempt: int = Field(default=0, ge=0, le=MAX_ATTEMPTS)
    hook_pid: int | None = None
    last_notified_at: int | None = None
    last_assistant_message: str | None = None
    note: str | None = None
    # Scheduling fold-in (issue #15): the task is not dispatchable
    # before this unix timestamp; None means immediately.
    run_after: int | None = None
    created_at: int
    updated_at: int

    @model_validator(mode="after")
    def _payload_present(self) -> "Task":
        if not (self.task_text or self.task_file):
            raise ValueError("task requires task_text or task_file")
        return self

    def transition_to(
        self,
        to_status: TaskStatus,
        *,
        manual: bool = False,
        **updates,
    ) -> "Task":
        """Return a validated copy moved to ``to_status``.

        Raises ``ValidationError`` when the edge is not declared for the
        chosen path. Extra keyword arguments patch other fields on the
        returned copy (validated again).
        """
        Transition(from_status=self.status, to_status=to_status, manual=manual)
        data = self.model_dump()
        data.update(updates)
        data["status"] = TaskStatus(to_status)
        return type(self).model_validate(data)

    def requeue_or_fail(self, note: str | None = None) -> "Task":
        """Automatic lease-expiry recovery for a leased live task.

        ``attempt + 1 < MAX_ATTEMPTS`` requeues the task; otherwise it
        lands on terminal ``failed``. The attempt counter is incremented
        and the lease fields are cleared either way.
        """
        attempt = self.attempt + 1
        target = TaskStatus.QUEUED if attempt < MAX_ATTEMPTS else TaskStatus.FAILED
        updates = {
            "attempt": attempt,
            "lease_owner": None,
            "lease_expires_at": None,
            "lease_renewed_at": None,
        }
        if note is not None:
            updates["note"] = note
        return self.transition_to(target, **updates)

    def retry(self, note: str | None = None) -> "Task":
        """Manual ``retry`` command: terminal ``failed`` > ``queued``.

        Resets ``attempt`` and clears the lease. Defined only for failed
        tasks; the automatic map has no edges out of ``failed``, so an
        auto-retry after ``failed`` is not expressible.
        """
        if self.status is not TaskStatus.FAILED:
            raise ValidationError.from_exception_data(
                "Task.retry",
                [
                    {
                        "type": "value_error",
                        "loc": ("status",),
                        "input": self.status.value,
                        "ctx": {
                            "error": ValueError("manual retry is only defined for failed tasks")
                        },
                    }
                ],
            )
        updates = {
            "attempt": 0,
            "lease_owner": None,
            "lease_expires_at": None,
            "lease_renewed_at": None,
        }
        if note is not None:
            updates["note"] = note
        return self.transition_to(TaskStatus.QUEUED, manual=True, **updates)


class SessionRole(str, Enum):
    """Role a session plays in the orchestration protocol.

    ORCHESTRATOR is a CAS-enforced singleton (register refuses while
    its heartbeat is fresh); every other session registers as WORKER.
    """

    ORCHESTRATOR = "orchestrator"
    WORKER = "worker"


class SessionMode(str, Enum):
    """MANAGED sessions run the background mailbox watcher subagent,
    which keeps the parent turn open and delivers notifications
    zero-touch. UNMANAGED sessions are plain bridge clients."""

    MANAGED = "managed"
    UNMANAGED = "unmanaged"


class SessionStatus(str, Enum):
    """Liveness verdict written by the supervisor daemon.

    A session is ACTIVE while its heartbeat stays fresh; the daemon
    marks it DEAD once ``last_seen_at`` goes stale (bounded window,
    about two heartbeat intervals).
    """

    ACTIVE = "active"
    DEAD = "dead"


class Session(BaseModel):
    """Row model for the ``sessions`` table.

    Liveness and the heartbeat generation counter live in
    ``WorkerHeartbeat`` rows (table ``worker_heartbeats``), which are
    the single source of truth - the sessions row is not denormalized
    with them.
    """

    model_config = ConfigDict(extra="forbid")

    session_id: str = Field(min_length=1)
    cwd: str | None = None
    title: str | None = None
    queue_paused: bool = False
    park_cycles_left: int = Field(default=5, ge=0)
    role: SessionRole = SessionRole.WORKER
    mode: SessionMode = SessionMode.UNMANAGED
    status: SessionStatus = SessionStatus.ACTIVE
    note: str | None = None
    updated_at: int


# --- orchestration protocol: message bus schema v2 ----------------------
#
# Hub-and-spoke: workers never talk to each other; every exchange flows
# through state.db rows (worker > questions > orchestrator >
# notifications > fs mailbox > worker). All row transitions are CAS-only
# single statements in db.py; this section pins the state maps and the
# tunable protocol constants.

# More than this many questions on one task means the task prompt is
# broken: the next ask escalates to the human (delegation of authority).
MAX_QUESTIONS_PER_TASK: Final[int] = 3
# A claim older than this is stale; the janitor releases it back to
# pending, preserving question_id (no duplicate re-ask).
CLAIM_TIMEOUT_SECONDS: Final[int] = 300
# Re-arm cycles a managed worker turn survives before rotating
# (end_turn + SessionStart hook re-arms); bounds parent context growth.
TURN_TTL_CYCLES: Final[int] = 10
# Lifetime of the mailbox watcher inside one turn; bounds watcher
# transcript growth only, independent of TURN_TTL_CYCLES.
WATCHER_TTL_SECONDS: Final[int] = 7200


class QuestionState(str, Enum):
    """Worker question lifecycle: pending > claimed > answered.

    ``claimed > pending`` exists only on the janitor path: a stale
    claim returns the row to pending with the same question_id, so a
    claim timeout never produces a duplicate re-ask.
    """

    PENDING = "pending"
    CLAIMED = "claimed"
    ANSWERED = "answered"


# Regular path, driven by ask/claim/answer. A claim is a single atomic
# UPDATE guarded on state='pending' (rowcount decides the winner, so
# concurrent claims yield exactly one claimant). ANSWERED is terminal.
QUESTION_TRANSITIONS: Final[dict] = {
    QuestionState.PENDING: frozenset({QuestionState.CLAIMED}),
    QuestionState.CLAIMED: frozenset({QuestionState.ANSWERED}),
    QuestionState.ANSWERED: frozenset(),
}

# Edge reserved for the janitor: releases a claim whose claimed_at is
# older than CLAIM_TIMEOUT_SECONDS back to pending. The same question_id
# persists - a timeout never creates a new row.
QUESTION_JANITOR_TRANSITIONS: Final[dict] = {
    QuestionState.CLAIMED: frozenset({QuestionState.PENDING}),
}


def question_allowed_targets(from_state: QuestionState, *, janitor: bool = False) -> frozenset:
    """States reachable from ``from_state`` on the given path."""
    table = QUESTION_JANITOR_TRANSITIONS if janitor else QUESTION_TRANSITIONS
    return table.get(QuestionState(from_state), frozenset())


def is_valid_question_transition(
    from_state: QuestionState, to_state: QuestionState, *, janitor: bool = False
) -> bool:
    """True when ``from_state > to_state`` is a declared edge."""
    return QuestionState(to_state) in question_allowed_targets(from_state, janitor=janitor)


def validate_question_transition(
    from_state: QuestionState, to_state: QuestionState, *, janitor: bool = False
) -> None:
    """Raise ``ValidationError`` unless the edge is declared."""
    QuestionTransition(from_state=from_state, to_state=to_state, janitor=janitor)


class QuestionTransition(BaseModel):
    """A single question state change, validated against the maps."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    from_state: QuestionState
    to_state: QuestionState
    janitor: bool = False

    @model_validator(mode="after")
    def _edge_is_declared(self) -> "QuestionTransition":
        if not is_valid_question_transition(self.from_state, self.to_state, janitor=self.janitor):
            path = "janitor" if self.janitor else "regular"
            raise ValueError(
                f"{path} transition {self.from_state.value} > {self.to_state.value} is not allowed"
            )
        return self


class Question(BaseModel):
    """Row model for the ``questions`` table.

    At most one PENDING question per task: the invariant is a partial
    unique index in db.py (``UNIQUE(task_id) WHERE state='pending'``),
    not a model rule - a single row cannot see its siblings.
    ``session_id`` is the asking worker; ``claimed_by`` the claiming
    orchestrator. ``asked_at``/``claimed_at``/``answered_at`` are the
    phase timestamps mirrored into the audit log.
    """

    model_config = ConfigDict(extra="forbid")

    id: int | None = Field(default=None, ge=1)  # SQLite AUTOINCREMENT
    task_id: int = Field(ge=1)
    session_id: str = Field(min_length=1)
    text: str = Field(min_length=1)
    state: QuestionState = QuestionState.PENDING
    answer: str | None = None
    claimed_by: str | None = None
    asked_at: int
    claimed_at: int | None = None
    answered_at: int | None = None

    @model_validator(mode="after")
    def _state_fields_coherent(self) -> "Question":
        """Keep the state column and its phase fields in lockstep.

        PENDING carries no claim/answer fields; CLAIMED requires
        ``claimed_by`` + ``claimed_at`` and still has no answer fields;
        ANSWERED requires ``answer`` + ``answered_at`` and keeps the
        claim fields (the row passed through claimed). The janitor
        path stays expressible: ``release_claim`` clears the claim
        fields when it returns a stale CLAIMED row to PENDING.
        """
        if self.state is QuestionState.PENDING:
            leftovers = [
                name
                for name in ("claimed_by", "claimed_at", "answer", "answered_at")
                if getattr(self, name) is not None
            ]
            if leftovers:
                raise ValueError(f"pending question must not carry {', '.join(leftovers)}")
        elif self.state is QuestionState.CLAIMED:
            if self.claimed_by is None or self.claimed_at is None:
                raise ValueError("claimed question requires claimed_by and claimed_at")
            if self.answer is not None or self.answered_at is not None:
                raise ValueError("claimed question must not carry answer/answered_at")
        elif (
            self.claimed_by is None
            or self.claimed_at is None
            or self.answer is None
            or self.answered_at is None
        ):
            raise ValueError(
                "answered question requires claimed_by, claimed_at, answer and answered_at"
            )
        return self

    def transition_to(
        self,
        to_state: QuestionState,
        *,
        janitor: bool = False,
        **updates,
    ) -> "Question":
        """Return a validated copy moved to ``to_state``.

        Raises ``ValidationError`` when the edge is not declared for the
        chosen path. Extra keyword arguments patch other fields on the
        returned copy (validated again).
        """
        QuestionTransition(from_state=self.state, to_state=to_state, janitor=janitor)
        data = self.model_dump()
        data.update(updates)
        data["state"] = QuestionState(to_state)
        return type(self).model_validate(data)

    def release_claim(self) -> "Question":
        """Janitor edge: stale ``claimed`` > ``pending``.

        Clears the claim fields and keeps the row (and its id), so the
        question is re-offered without a duplicate re-ask. Callers must
        only invoke this on claims older than CLAIM_TIMEOUT_SECONDS.
        """
        return self.transition_to(
            QuestionState.PENDING,
            janitor=True,
            claimed_by=None,
            claimed_at=None,
        )


class NotificationKind(str, Enum):
    """What a ``notifications`` row delivers to its recipient.

    Consumers route mailbox deliveries by kind plus the correlation
    refs ``task_id``/``question_id``: TASK_DISPATCH hands the worker a
    task, QUESTION_ANSWER carries the orchestrator's answer to a
    pending question, DIRECTIVE is a steering instruction, DISARM
    orders a watcher/turn teardown, HEARTBEAT_TICK is a liveness poke.
    """

    TASK_DISPATCH = "task_dispatch"
    QUESTION_ANSWER = "question_answer"
    DIRECTIVE = "directive"
    DISARM = "disarm"
    HEARTBEAT_TICK = "heartbeat_tick"


class Notification(BaseModel):
    """Row model for the ``notifications`` outbox table.

    Outbox semantics: the writer's transaction commits the row with
    ``relayed_at`` NULL; the daemon relay loop writes the fs mailbox
    file, then marks ``relayed_at``. Delivery is at-least-once - a
    relay crash between the fs write and the mark re-delivers the same
    row, so consumers deduplicate by ``id`` (idempotent consumption)
    and route by ``kind`` plus the correlation refs
    ``task_id``/``question_id`` (e.g. a QUESTION_ANSWER delivery is
    matched to the pending question it resolves). ``ack_at`` is the
    consuming session's restart watermark: the recipient records what
    it has processed, so a restart resumes from the watermark without
    double-processing.
    """

    model_config = ConfigDict(extra="forbid")

    id: int | None = Field(default=None, ge=1)  # SQLite AUTOINCREMENT
    session_id: str = Field(min_length=1)  # recipient session
    kind: NotificationKind
    body: str = Field(min_length=1)
    # Optional correlation refs the consumer routes on.
    task_id: int | None = Field(default=None, ge=1)
    question_id: int | None = Field(default=None, ge=1)
    relayed_at: int | None = None  # NULL until the daemon relay marks it
    ack_at: int | None = None  # recipient session restart watermark
    created_at: int


class WorkerHeartbeat(BaseModel):
    """Row model for the ``worker_heartbeats`` table (plural, per the
    repo table-naming convention; the model name stays singular).

    This row is authoritative for liveness and the generation counter -
    the ``sessions`` row is not denormalized with either.

    ``register`` inserts the row; every re-register and heartbeat bumps
    ``generation``, so a caller holding a stale generation detects it
    has been superseded. ``last_seen_at`` feeds the daemon's staleness
    check: a silent heartbeat past the bounded refusal window marks the
    session dead and its task is requeued.
    """

    model_config = ConfigDict(extra="forbid")

    session_id: str = Field(min_length=1)
    last_seen_at: int
    generation: int = Field(default=0, ge=0)


class AuditEventKind(str, Enum):
    """What an ``audit_events`` row records.

    DELIBERATION rows capture the question > deliberation > directive
    pipeline verbatim. ESCALATION rows record a delegation-of-authority
    trigger firing to the human. The rotation kinds bound context
    growth: TURN_ROTATION for worker turn re-arm, WATCHER_ROTATION for
    the mailbox watcher TTL.
    """

    DELIBERATION = "deliberation"
    ESCALATION = "escalation"
    TURN_ROTATION = "turn_rotation"
    WATCHER_ROTATION = "watcher_rotation"


class AuditEvent(BaseModel):
    """Row model for the ``audit_events`` table (append-only log).

    A DELIBERATION row stores the worker question verbatim, the partner
    outputs verbatim (``partner_outputs`` keyed by partner name, so
    partner rotation changes dict keys, not the schema), the
    orchestrator synthesis and the directive sent back, plus the phase
    timestamps of the underlying question
    (``asked_at``/``claimed_at``/``answered_at``).
    ``doa_trigger`` names the delegation-of-authority whitelist rule
    that fired; NULL when the committee answered autonomously.
    """

    model_config = ConfigDict(extra="forbid")

    id: int | None = Field(default=None, ge=1)  # SQLite AUTOINCREMENT
    kind: AuditEventKind
    session_id: str | None = None
    task_id: int | None = Field(default=None, ge=1)
    question_id: int | None = Field(default=None, ge=1)
    question_text: str | None = None
    # Partner outputs verbatim, keyed by partner name - rotation-proof.
    partner_outputs: dict[str, str] = Field(default_factory=dict)
    synthesis: str | None = None
    directive: str | None = None
    doa_trigger: str | None = None
    asked_at: int | None = None
    claimed_at: int | None = None
    answered_at: int | None = None
    created_at: int

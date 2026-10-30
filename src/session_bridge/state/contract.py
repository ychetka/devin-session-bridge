"""c_state: domain contract for the state.db persistence layer.

state.db (SQLite) is the single source of truth for sessions-bridge.
This module pins the storage constants (WAL + busy timeout), the task
status lifecycle, the lease fields and the retry boundary:

- automatic transitions follow ``TRANSITIONS`` only; ``failed`` has no
  outgoing edges, so an auto-requeue after ``failed`` is not expressible.
- the manual ``retry`` command (``Task.retry``) is the only exit from
  ``failed``: it moves the task back to ``queued`` and resets ``attempt``.
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
TERMINAL_STATUSES: Final[frozenset] = frozenset(
    {TaskStatus.DONE, TaskStatus.FAILED}
)


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


def allowed_targets(
    from_status: TaskStatus, *, manual: bool = False
) -> frozenset:
    """Statuses reachable from ``from_status`` on the given path."""
    table = MANUAL_TRANSITIONS if manual else TRANSITIONS
    return table.get(TaskStatus(from_status), frozenset())


def is_valid_transition(
    from_status: TaskStatus, to_status: TaskStatus, *, manual: bool = False
) -> bool:
    """True when ``from_status > to_status`` is a declared edge."""
    return TaskStatus(to_status) in allowed_targets(
        from_status, manual=manual
    )


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
        if not is_valid_transition(
            self.from_status, self.to_status, manual=self.manual
        ):
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
        Transition(
            from_status=self.status, to_status=to_status, manual=manual
        )
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
        target = (
            TaskStatus.QUEUED
            if attempt < MAX_ATTEMPTS
            else TaskStatus.FAILED
        )
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
                            "error": ValueError(
                                "manual retry is only defined "
                                "for failed tasks"
                            )
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


class Session(BaseModel):
    """Row model for the ``sessions`` table."""

    model_config = ConfigDict(extra="forbid")

    session_id: str = Field(min_length=1)
    cwd: str | None = None
    title: str | None = None
    queue_paused: bool = False
    park_cycles_left: int = Field(default=5, ge=0)
    note: str | None = None
    updated_at: int

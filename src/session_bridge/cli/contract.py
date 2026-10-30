"""c_cli: command surface, exit codes and the ``--json`` output schema.

Every ``bridge`` command accepts ``--json`` and answers with a
``JsonResponse`` envelope on stdout. The envelope is the machine-facing
contract consumed by the AI orchestrator: it carries the session, the
resulting status, the queue depth, orchestration ``events`` (e.g.
``NEEDS_KICK``, ``FAILED``) and ``agent_instruction``, a free-text hint
such as "do not retry".
"""

from enum import Enum, IntEnum
from typing import Any, Final

from pydantic import BaseModel, ConfigDict, Field


class Command(str, Enum):
    """CLI subcommand surface."""

    PUSH = "push"
    STATUS = "status"
    INSPECT = "inspect"
    CLEAR = "clear"
    UNPARK = "unpark"
    RETRY = "retry"
    WORK = "_work"  # internal per-task worker entry point


INTERNAL_COMMANDS: Final[frozenset] = frozenset({Command.WORK})


class ExitCode(IntEnum):
    """Process exit code classes. ``OK`` is the only zero."""

    OK = 0
    USAGE = 2
    NOT_FOUND = 3
    CONFLICT = 4
    INTERNAL = 5


class EventKind(str, Enum):
    """Event kinds surfaced to the AI orchestrator."""

    NEEDS_KICK = "NEEDS_KICK"
    FAILED = "FAILED"


class Event(BaseModel):
    """A single orchestration event inside ``JsonResponse.events``."""

    model_config = ConfigDict(extra="forbid")

    kind: EventKind
    session_id: str | None = None
    task_id: int | None = None
    detail: str | None = None
    ts: int | None = None


class JsonResponse(BaseModel):
    """Envelope printed on stdout by every command under ``--json``."""

    model_config = ConfigDict(extra="forbid")

    session: str | None = None
    status: str = Field(min_length=1)
    queue_depth: int = Field(default=0, ge=0)
    events: list[Event] = Field(default_factory=list)
    agent_instruction: str | None = None
    error: str | None = None
    data: dict[str, Any] | None = None

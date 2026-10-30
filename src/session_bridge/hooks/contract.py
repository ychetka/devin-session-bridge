"""c_hooks: wire contract for Devin lifecycle hooks.

A hook is a subprocess spawned by Devin: it reads one JSON payload on
stdin and answers on stdout (or by exit code). Pinned from devin
3000.11.228:

- every stdin payload carries ``hook_event_name`` and ``session_id``;
  ``prompt_id`` is absent until the first prompt of the session.
- event-specific fields: UserPromptSubmit adds ``prompt``, Stop adds
  ``stop_hook_active`` + ``last_assistant_message``, SessionStart adds
  ``source`` (``startup``|``resume``|...), SessionEnd adds ``reason``.
- stdout answers: ``{"decision":"block","reason":...}`` or
  ``{"hookSpecificOutput":{"hookEventName":...,"additionalContext":...}}``.
- exit code ``2`` means block; any other nonzero code is an error and
  does not block.

Inbound payloads keep ``extra="allow"`` (Devin owns the shape and may
add fields); the outputs we serialize are ``extra="forbid"``.
"""

from enum import Enum
from typing import Any, Final, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)

# --- hook runtime constants --------------------------------------------------

HOOK_TIMEOUT_DEFAULT: Final[int] = 60  # devin default hook timeout, seconds
EXIT_BLOCK: Final[int] = 2  # other nonzero exits are errors, not blocks

# reason prefix marking a block that injects a queued task
PARKED_TASK_PREFIX: Final[str] = "[PARKED>TASK]"

# known SessionStart source values; the field itself stays str
SOURCE_STARTUP: Final[str] = "startup"
SOURCE_RESUME: Final[str] = "resume"


class HookEventName(str, Enum):
    """Lifecycle events the bridge installs hooks for (bounded set:
    only these events are registered, so strict parsing is safe)."""

    SESSION_START = "SessionStart"
    SESSION_END = "SessionEnd"
    USER_PROMPT_SUBMIT = "UserPromptSubmit"
    STOP = "Stop"


# --- inbound: stdin payloads -------------------------------------------------


class HookPayload(BaseModel):
    """Common stdin payload of every hook event."""

    model_config = ConfigDict(extra="allow")

    hook_event_name: HookEventName
    session_id: str = Field(min_length=1)
    prompt_id: str | None = None  # absent before the first prompt


class SessionStartPayload(HookPayload):
    """SessionStart payload: ``source`` says how the session began."""

    hook_event_name: Literal[HookEventName.SESSION_START] = HookEventName.SESSION_START
    source: str = Field(min_length=1)


class SessionEndPayload(HookPayload):
    """SessionEnd payload: ``reason`` says why the session ended."""

    hook_event_name: Literal[HookEventName.SESSION_END] = HookEventName.SESSION_END
    reason: str = Field(min_length=1)


class UserPromptSubmitPayload(HookPayload):
    """UserPromptSubmit payload: the raw user ``prompt`` text."""

    hook_event_name: Literal[HookEventName.USER_PROMPT_SUBMIT] = HookEventName.USER_PROMPT_SUBMIT
    prompt: str


class StopPayload(HookPayload):
    """Stop payload: ``stop_hook_active`` marks a re-armed stop chain,
    ``last_assistant_message`` carries the last agent reply."""

    hook_event_name: Literal[HookEventName.STOP] = HookEventName.STOP
    stop_hook_active: bool
    last_assistant_message: str | None


PAYLOAD_BY_EVENT: Final[dict] = {
    HookEventName.SESSION_START: SessionStartPayload,
    HookEventName.SESSION_END: SessionEndPayload,
    HookEventName.USER_PROMPT_SUBMIT: UserPromptSubmitPayload,
    HookEventName.STOP: StopPayload,
}


def parse_hook_payload(data: dict[str, Any]) -> HookPayload:
    """Validate raw stdin JSON into the event-specific payload model.

    Raises ``ValidationError`` for malformed input and for
    ``hook_event_name`` values outside ``HookEventName``.
    """
    event = data.get("hook_event_name")
    model = PAYLOAD_BY_EVENT.get(event)
    if model is None:
        raise ValidationError.from_exception_data(
            "parse_hook_payload",
            [
                {
                    "type": "value_error",
                    "loc": ("hook_event_name",),
                    "input": event,
                    "ctx": {"error": ValueError(f"unsupported hook_event_name: {event!r}")},
                }
            ],
        )
    return model.model_validate(data)


# --- outbound: stdout answers -------------------------------------------------


class BlockOutput(BaseModel):
    """stdout form 1: block and feed ``reason`` back to the agent (the
    parking mechanism re-prompts with the queued task)."""

    model_config = ConfigDict(extra="forbid")

    decision: Literal["block"] = "block"
    reason: str = Field(min_length=1)


class HookSpecificOutput(BaseModel):
    """Inner object of the context-injection output form."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    hook_event_name: HookEventName = Field(alias="hookEventName")
    additional_context: str = Field(alias="additionalContext", min_length=1)


class ContextOutput(BaseModel):
    """stdout form 2: inject ``additionalContext`` into the session."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    hook_specific_output: HookSpecificOutput = Field(alias="hookSpecificOutput")


# --- parking ------------------------------------------------------------------


class ParkConfig(BaseModel):
    """Stop-hook parking budget.

    A parked Stop hook sleeps ``cycle_seconds`` per cycle for at most
    ``cycles_max`` cycles, waiting for a queued task. The precondition
    ``cycle_seconds <= hook_timeout`` lives in the contract, not only
    in the installer: with devin's default timeout of
    ``HOOK_TIMEOUT_DEFAULT`` seconds a longer cycle would be killed
    mid-park.
    """

    model_config = ConfigDict(extra="forbid")

    cycle_seconds: int = Field(gt=0)
    cycles_max: int = Field(ge=1)
    hook_timeout: int = Field(default=HOOK_TIMEOUT_DEFAULT, gt=0)

    @model_validator(mode="after")
    def _cycle_within_timeout(self) -> "ParkConfig":
        if self.cycle_seconds > self.hook_timeout:
            raise ValueError(
                f"cycle_seconds ({self.cycle_seconds}) exceeds hook_timeout ({self.hook_timeout})"
            )
        return self

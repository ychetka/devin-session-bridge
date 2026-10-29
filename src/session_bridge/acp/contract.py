"""c_acp: wire contract for ``devin acp`` (JSON-RPC 2.0 over stdio).

Pinned from devin 3000.11.228. ``devin acp`` speaks newline-delimited
JSON-RPC: requests carry ``jsonrpc``/``id``/``method``/``params``, one
message per line. ``session/prompt`` first streams ``session/update``
notifications (text chunks, tool calls, usage) and then answers with
``stopReason`` + ``usage`` + ``_meta``.

Field-shape policy:

- inbound models (server > bridge) use ``extra="allow"``: Devin owns
  the shape and may add fields; the contract pins what we rely on, not
  what we ignore.
- outbound models (bridge > server, and shapes we serialize) use
  ``extra="forbid"`` so drift in our own emission fails early.
- enum-typed fields are reserved for bounded sets we control
  (``Method``). Values the wire may extend (``errorKind``,
  ``sessionUpdate`` kind, ``stopReason``) stay ``str`` with the known
  members pinned as enums for reference and comparison.
"""

from enum import Enum
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

# --- protocol constants ----------------------------------------------------

JSONRPC_VERSION: Final[str] = "2.0"
PROTOCOL_VERSION: Final[int] = 1
AUTH_METHOD_ID: Final[str] = "devin-browser"

# --- namespaced wire keys --------------------------------------------------

META_FORCE: Final[str] = "cognition.ai/force"
META_IS_LOCKED: Final[str] = "cognition.ai/isLocked"
META_LOCK_HOLDER_PID: Final[str] = "cognition.ai/lockHolderPid"
META_HAS_USER_MESSAGES: Final[str] = "cognition.ai/hasUserMessages"
META_ERROR_KIND: Final[str] = "cognition.ai/errorKind"
META_RETRYABLE: Final[str] = "cognition.ai/retryable"

# usage dict keys inside the session/prompt result
USAGE_TOTAL_TOKENS: Final[str] = "totalTokens"
USAGE_INPUT_TOKENS: Final[str] = "inputTokens"
USAGE_OUTPUT_TOKENS: Final[str] = "outputTokens"
USAGE_CACHED_READ_TOKENS: Final[str] = "cachedReadTokens"


class Method(str, Enum):
    """JSON-RPC methods spoken by ``devin acp`` (verified set)."""

    INITIALIZE = "initialize"
    AUTHENTICATE = "authenticate"
    SESSION_NEW = "session/new"
    SESSION_LIST = "session/list"
    SESSION_LOAD = "session/load"
    SESSION_PROMPT = "session/prompt"
    SESSION_CLOSE = "session/close"
    SESSION_CANCEL = "session/cancel"
    SESSION_UPDATE = "session/update"  # server > client notification


class ErrorKind(str, Enum):
    """Known ``cognition.ai/errorKind`` values. Wire fields keep the raw
    ``str``: kinds outside this set must not break parsing."""

    SESSION_LOCKED = "session_locked"


class SessionUpdateKind(str, Enum):
    """Known ``update.sessionUpdate`` discriminators. The list is not
    exhaustive on the wire, so payload fields stay ``str``."""

    AGENT_MESSAGE_CHUNK = "agent_message_chunk"
    AGENT_THOUGHT_CHUNK = "agent_thought_chunk"
    TOOL_CALL = "tool_call"
    TOOL_CALL_UPDATE = "tool_call_update"
    USAGE_UPDATE = "usage_update"
    SESSION_INFO_UPDATE = "session_info_update"


# --- outbound: requests ----------------------------------------------------


class JsonRpcRequest(BaseModel):
    """Outbound request envelope, one per line on the process stdin.
    Requests always carry an ``id`` the response is matched by;
    notifications (``session/cancel``) have none - use
    ``JsonRpcNotification`` for those."""

    model_config = ConfigDict(extra="forbid")

    jsonrpc: Literal["2.0"] = JSONRPC_VERSION
    id: int = Field(ge=1)
    method: Method
    params: dict[str, Any] = Field(default_factory=dict)


class JsonRpcNotification(BaseModel):
    """Outbound notification envelope: a request without ``id`` (the
    server sends no response). Used for ``session/cancel``."""

    model_config = ConfigDict(extra="forbid")

    jsonrpc: Literal["2.0"] = JSONRPC_VERSION
    method: Method
    params: dict[str, Any] = Field(default_factory=dict)


class InitializeParams(BaseModel):
    """``params`` of initialize: protocol version + empty capabilities."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    protocol_version: int = Field(
        default=PROTOCOL_VERSION, alias="protocolVersion"
    )
    client_capabilities: dict[str, Any] = Field(
        default_factory=dict, alias="clientCapabilities"
    )


class AuthenticateMeta(BaseModel):
    """``params._meta`` of authenticate. The ``api_key`` value is a
    secret: it must never reach logs, prompts or the repository."""

    model_config = ConfigDict(extra="forbid")

    api_key: str = Field(min_length=1)


class AuthenticateParams(BaseModel):
    """``params`` of authenticate (methodId + _meta.api_key)."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    method_id: str = Field(default=AUTH_METHOD_ID, alias="methodId")
    meta: AuthenticateMeta = Field(alias="_meta")


class SessionNewParams(BaseModel):
    """``params`` of session/new."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    cwd: str = Field(min_length=1)
    mcp_servers: list[Any] = Field(default_factory=list, alias="mcpServers")


class SessionLoadParams(SessionNewParams):
    """``params`` of session/load: new-session params + sessionId."""

    session_id: str = Field(alias="sessionId", min_length=1)


class SessionListParams(BaseModel):
    """``params`` of session/list: optional cwd filter."""

    model_config = ConfigDict(extra="forbid")

    cwd: str | None = None


class PromptBlock(BaseModel):
    """One content block of a session/prompt payload."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["text"] = "text"
    text: str


class SessionPromptParams(BaseModel):
    """``params`` of session/prompt."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    session_id: str = Field(alias="sessionId", min_length=1)
    prompt: list[PromptBlock] = Field(min_length=1)


class SessionCloseMeta(BaseModel):
    """``params._meta`` of session/close: the only optional flag."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    force: bool = Field(alias=META_FORCE)


class SessionCloseParams(BaseModel):
    """``params`` of session/close; ``_meta`` carries the force flag."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    session_id: str = Field(alias="sessionId", min_length=1)
    meta: SessionCloseMeta | None = Field(default=None, alias="_meta")


class SessionCancelParams(BaseModel):
    """``params`` of session/cancel (notification)."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    session_id: str = Field(alias="sessionId", min_length=1)


# --- inbound: results and notifications ------------------------------------


class SessionMeta(BaseModel):
    """``_meta`` block of a session/list entry (lock state)."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    is_locked: bool | None = Field(default=None, alias=META_IS_LOCKED)
    lock_holder_pid: int | None = Field(
        default=None, alias=META_LOCK_HOLDER_PID
    )
    has_user_messages: bool | None = Field(
        default=None, alias=META_HAS_USER_MESSAGES
    )


class SessionEntry(BaseModel):
    """One element of the ``sessions[]`` array of session/list."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    session_id: str = Field(alias="sessionId", min_length=1)
    cwd: str | None = None
    title: str | None = None
    updated_at: str | None = Field(default=None, alias="updatedAt")
    meta: SessionMeta | None = Field(default=None, alias="_meta")


class SessionListResult(BaseModel):
    """Result object of session/list."""

    model_config = ConfigDict(extra="allow")

    sessions: list[SessionEntry] = Field(default_factory=list)


class SessionUpdateParams(BaseModel):
    """``params`` of session/update. ``update`` stays a dict: its shape
    varies with the ``sessionUpdate`` discriminator
    (``SessionUpdateKind`` pins the known kinds)."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    session_id: str = Field(alias="sessionId")
    update: dict[str, Any]


class SessionUpdateNotification(BaseModel):
    """Notification streamed by the server during session/prompt."""

    model_config = ConfigDict(extra="allow")

    jsonrpc: Literal["2.0"] = JSONRPC_VERSION
    method: Literal[Method.SESSION_UPDATE] = Method.SESSION_UPDATE
    params: SessionUpdateParams


def message_chunk_text(update: dict[str, Any]) -> str | None:
    """Text of an ``agent_message_chunk`` update payload, else ``None``.

    Lets the caller fold the notification stream into prompt text with
    a single filter: anything that is not a text message chunk returns
    ``None`` (thought chunks, tool calls, usage updates, ...).
    """
    if update.get("sessionUpdate") != SessionUpdateKind.AGENT_MESSAGE_CHUNK:
        return None
    content = update.get("content") or {}
    if content.get("type") != "text":
        return None
    return content.get("text")


class Usage(BaseModel):
    """``usage`` block of the session/prompt result."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    total_tokens: int = Field(alias=USAGE_TOTAL_TOKENS, ge=0)
    input_tokens: int = Field(alias=USAGE_INPUT_TOKENS, ge=0)
    output_tokens: int = Field(alias=USAGE_OUTPUT_TOKENS, ge=0)
    cached_read_tokens: int = Field(
        default=0, alias=USAGE_CACHED_READ_TOKENS, ge=0
    )


class PromptResponse(BaseModel):
    """Result object of session/prompt (arrives after the update
    stream): ``stopReason`` + ``usage`` + ``_meta``."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    stop_reason: str = Field(alias="stopReason", min_length=1)
    usage: Usage
    meta: dict[str, Any] | None = Field(default=None, alias="_meta")


class PromptResult(BaseModel):
    """Assembled client-facing outcome of a prompt: joined agent
    message text plus the wire response fields. ``usage`` stays a
    wire-shaped dict (``totalTokens`` keys, see USAGE_* constants);
    ``stop_reason`` is ``None`` when the stream broke before the
    response arrived."""

    model_config = ConfigDict(extra="forbid")

    text: str = ""
    stop_reason: str | None = None
    usage: dict[str, Any] = Field(default_factory=dict)


# --- inbound: error envelope ------------------------------------------------


class ErrorData(BaseModel):
    """``error.data`` payload. All keys are namespaced under
    ``cognition.ai/`` on the wire."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    kind: str | None = Field(default=None, alias=META_ERROR_KIND)
    retryable: bool | None = Field(default=None, alias=META_RETRYABLE)
    lock_holder_pid: int | None = Field(
        default=None, alias=META_LOCK_HOLDER_PID
    )


class JsonRpcError(BaseModel):
    """``error`` member of a JSON-RPC error response."""

    model_config = ConfigDict(extra="allow")

    code: int
    message: str
    data: ErrorData | None = None


class JsonRpcResponse(BaseModel):
    """Inbound response envelope: ``result`` or ``error`` is set."""

    model_config = ConfigDict(extra="allow")

    jsonrpc: Literal["2.0"] = JSONRPC_VERSION
    id: int | None = None
    result: dict[str, Any] | None = None
    error: JsonRpcError | None = None


class AcpError(Exception):
    """Error raised by the client for a JSON-RPC error response."""

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        kind: str | None = None,
        retryable: bool = False,
        lock_holder_pid: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.kind = kind
        self.retryable = retryable
        self.lock_holder_pid = lock_holder_pid

    @classmethod
    def from_wire(cls, error: JsonRpcError) -> "AcpError":
        """Build the exception from a validated error envelope."""
        data = error.data or ErrorData()
        return cls(
            error.message,
            code=error.code,
            kind=data.kind,
            retryable=bool(data.retryable),
            lock_holder_pid=data.lock_holder_pid,
        )

    @property
    def is_session_locked(self) -> bool:
        """True for the session_locked kind (queue for hook path)."""
        return self.kind == ErrorKind.SESSION_LOCKED

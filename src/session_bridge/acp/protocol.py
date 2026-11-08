"""Shared ACP wire-protocol primitives (task #203, item_37c).

Both JSON-RPC transports - ``acp/client.py`` (a ``devin acp``
subprocess over stdio) and ``wake/acp_host.py`` (the TCP attach
into the shared acp-host) - speak the same dialect:
newline-delimited JSON frames carrying JSON-RPC 2.0. The framing,
request envelope, response classification and the ``initialize``
payload lived in each module once each, and every fix landed on one
side only (the statusless-session and v1/v2 hello incidents were
both one-sided drifts). This module is the single home for the
transport-neutral pieces; the transports stay thin shells that map
``NdjsonError`` onto their own error types (``AcpError`` /
``WakeError``) and keep their own timeout semantics (per-read
silence bound in the client, overall deadline in the host channel).
"""

import json

__all__ = [
    "NdjsonBuffer",
    "NdjsonError",
    "encode_message",
    "error_detail",
    "error_payload",
    "initialize_params",
    "is_server_call",
    "response_result",
    "rpc_request",
]


def encode_message(payload: dict) -> bytes:
    """One NDJSON frame: compact JSON object + trailing LF."""
    return json.dumps(payload).encode() + b"\n"


class NdjsonError(Exception):
    """Framing failure; ``kind`` maps onto the caller's error type.

    Kinds mirror the existing per-transport reason vocabularies so
    wrapping is a rename-free ``WakeError(e.kind, ...)`` /
    ``AcpError(kind=e.kind)``: ``malformed_message`` for a bad JSON
    line or a non-object payload.
    """

    def __init__(self, kind: str, message: str = ""):
        self.kind = kind
        super().__init__(message or kind)


class NdjsonBuffer:
    """Buffered newline-delimited JSON splitter shared by both readers.

    ``feed`` appends raw bytes; ``pop_message`` returns the next
    complete decoded object, or ``None`` while no full line is
    buffered. Blank lines are skipped. The consumer's read loop keeps
    owning I/O and timeouts - the buffer only owns framing. On
    transport EOF mid-line the leftover partial frame is available
    via ``pending`` / ``take_pending`` for the caller's
    truncated-message reporting.
    """

    def __init__(self) -> None:
        self._buf = b""

    def feed(self, chunk: bytes) -> None:
        self._buf += chunk

    @property
    def pending(self) -> bytes:
        """Bytes buffered past the last newline (a partial frame)."""
        return self._buf

    def take_pending(self) -> bytes:
        """Return and drop the partial frame (EOF reporting)."""
        pending, self._buf = self._buf, b""
        return pending

    def pop_message(self) -> dict | None:
        """Next complete NDJSON object, or None while none is buffered."""
        while True:
            nl = self._buf.find(b"\n")
            if nl < 0:
                return None
            line, self._buf = self._buf[:nl], self._buf[nl + 1 :]
            if not line.strip():
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError as e:
                raise NdjsonError("malformed_message", f"bad NDJSON line: {e}") from e
            if not isinstance(msg, dict):
                raise NdjsonError("malformed_message", "non-object NDJSON message")
            return msg


def rpc_request(req_id: int, method: str, params: dict | None = None) -> dict:
    """The JSON-RPC 2.0 request envelope both transports send."""
    return {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params or {}}


def is_server_call(msg: dict) -> bool:
    """Server->client request or notification: carries a ``method``."""
    return "method" in msg


def error_payload(msg: dict) -> dict | None:
    """The ``error`` object of a response message, or None."""
    err = msg.get("error")
    return err if err else None


def error_detail(err) -> str:
    """Human-readable detail from a wire error of either shape."""
    if isinstance(err, dict):
        detail = err.get("message")
        return str(detail) if detail else str(err)
    return str(err)


def response_result(msg: dict) -> dict:
    """The ``result`` of a response message as a dict ({} if absent)."""
    result = msg.get("result")
    return result if isinstance(result, dict) else {}


def initialize_params(
    version: int,
    *,
    client_name: str | None = None,
    client_version: str = "0",
    capabilities: dict | None = None,
) -> dict:
    """The ``initialize`` params payload, one shape for both transports.

    ``protocolVersion`` is the negotiated field - the wake transport
    offers its maximum and honours a down-negotiated answer;
    ``clientInfo`` is included only when ``client_name`` is given
    (the host attaches identify themselves; the subprocess client's
    proven hello carried none).
    """
    params = {
        "protocolVersion": version,
        "clientCapabilities": capabilities if capabilities is not None else {},
    }
    if client_name is not None:
        params["clientInfo"] = {"name": client_name, "version": client_version}
    return params

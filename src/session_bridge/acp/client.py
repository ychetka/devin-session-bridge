"""Synchronous ACP client for ``devin acp`` (implements contract.py).

JSON-RPC 2.0 over stdio, newline-delimited (one message per line).
Handshake: ``initialize`` is answered synchronously; ``authenticate``
is sent fire-and-forget because the server emits notifications before
answering it, followed by a short settle delay.

Concurrency model: at most one request is in flight per client.
``rpc``/``prompt`` take the in-flight slot; a second overlapping call
fails fast with ``AcpError(kind="client_busy")`` instead of queuing -
chosen over a queue because bridge dispatch is already sequential and
a silent queue would hide caller bugs. ``cancel`` only writes a
notification and never touches the slot.

Notifications and stray responses (before the handshake answer, while
another request is in flight, replies to ids nobody waits on) are
buffered in a bounded deque - see :meth:`drain_notifications` - and
never crash the read loop.

Timeouts are silence-based: the read loop waits up to ``timeout``
seconds for the *next* bytes on the pipe, so a slow-but-talking peer
never trips it and a fully hung one does. ``prompt`` defaults to
``timeout=None`` (wait indefinitely) because agent turns can stream
for a long time; pass seconds to bound it.

stderr of the child goes to DEVNULL (MVP choice, configurable via the
``stderr`` ctor arg): the output is discarded, so no pipe-buffer
deadlock can occur.
"""

import json
import os
import re
import select
import subprocess
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any

from pydantic import ValidationError

from .contract import (
    AcpError,
    AuthenticateMeta,
    AuthenticateParams,
    ErrorKind,
    InitializeParams,
    JsonRpcError,
    JsonRpcNotification,
    JsonRpcRequest,
    JsonRpcResponse,
    Method,
    PromptBlock,
    PromptResponse,
    PromptResult,
    SessionCancelParams,
    SessionCloseMeta,
    SessionCloseParams,
    SessionEntry,
    SessionListParams,
    SessionListResult,
    SessionLoadParams,
    SessionNewParams,
    SessionPromptParams,
    SessionUpdateNotification,
    message_chunk_text,
)

__all__ = [
    "DEFAULT_CREDENTIALS_PATH",
    "DEFAULT_HANDSHAKE_DELAY",
    "DEFAULT_REQUEST_TIMEOUT",
    "AcpClient",
    "read_api_key",
]

DEFAULT_CREDENTIALS_PATH = os.path.expanduser("~/.local/share/devin/credentials.toml")
DEFAULT_REQUEST_TIMEOUT = 45.0
DEFAULT_HANDSHAKE_DELAY = 1.5
_KILL_WAIT_SECONDS = 5.0
_READ_CHUNK = 65536
_NOTIFICATION_BUFFER = 256

_DEFAULT = object()

#: JSON-RPC error code -> AcpError.kind, used when the server did not
#: send ``cognition.ai/errorKind`` in error.data.
_ERROR_KIND_BY_CODE: dict[int, str] = {
    -32700: "parse_error",
    -32600: "invalid_request",
    -32601: "method_not_found",
    -32602: "invalid_params",
    -32603: "internal_error",
    -32015: ErrorKind.SESSION_LOCKED.value,
}

_API_KEY_RE = re.compile(rb'windsurf_api_key\s*=\s*["\']([^"\']+)["\']')


def read_api_key(
    credentials_path: str = DEFAULT_CREDENTIALS_PATH,
) -> str:
    """Return ``windsurf_api_key`` from devin's credentials.toml.

    Parsed with a regex instead of tomllib to stay stdlib-only on
    Python 3.10 (tomllib exists since 3.11). Raises ``AcpError`` with
    ``kind="credentials"`` on a missing file or key. The key value
    never reaches logs or the repository.
    """
    try:
        with open(credentials_path, "rb") as fh:
            raw = fh.read()
    except FileNotFoundError as e:
        raise AcpError(
            f"devin credentials file not found: {credentials_path}",
            kind="credentials",
        ) from e
    except OSError as e:
        raise AcpError(
            f"cannot read devin credentials {credentials_path}: {e}",
            kind="credentials",
        ) from e
    m = _API_KEY_RE.search(raw)
    if not m:
        raise AcpError(
            "windsurf_api_key missing in devin credentials file",
            kind="credentials",
        )
    return m.group(1).decode()


def _acp_error(err: JsonRpcError) -> AcpError:
    """Map a validated wire error to AcpError, filling ``kind`` from the
    code table when ``error.data`` carried no namespaced kind."""
    exc = AcpError.from_wire(err)
    if exc.kind is None:
        exc.kind = _ERROR_KIND_BY_CODE.get(exc.code, "rpc_error")
    return exc


def _validated(model: type, payload: Any, what: str) -> Any:
    try:
        return model.model_validate(payload)
    except ValidationError as e:
        raise AcpError(f"invalid {what} from acp: {e}", kind="invalid_response") from e


class AcpClient:
    """Owns one ``devin acp`` subprocess and its stdin/stdout pipes.

    ``command``: str ``"devin"`` becomes ``["devin", "acp"]``; a
    sequence is used verbatim (tests inject
    ``[sys.executable, fake_acp.py, scenario.json]``).
    """

    def __init__(
        self,
        command: str | Sequence[str] = "devin",
        *,
        api_key: str | None = None,
        credentials_path: str = DEFAULT_CREDENTIALS_PATH,
        request_timeout: float | None = DEFAULT_REQUEST_TIMEOUT,
        handshake_delay: float = DEFAULT_HANDSHAKE_DELAY,
        stderr: Any = subprocess.DEVNULL,
    ) -> None:
        argv = [command, "acp"] if isinstance(command, str) else list(command)
        # resolve credentials before spawning so a missing key cannot
        # leak a half-initialised process
        key = api_key if api_key is not None else read_api_key(credentials_path)
        self._request_timeout = request_timeout
        self._next_id = 0
        self._rbuf = b""
        self._wlock = threading.Lock()
        self._slot = threading.Lock()
        self._notifications: deque[dict[str, Any]] = deque(maxlen=_NOTIFICATION_BUFFER)
        self._stray_responses = 0
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=stderr,
            bufsize=0,
        )
        self._fd = self.proc.stdout.fileno()
        try:
            self.rpc(
                Method.INITIALIZE,
                InitializeParams().model_dump(by_alias=True),
            )
            auth = AuthenticateParams(meta=AuthenticateMeta(api_key=key))
            # fire-and-forget per protocol: the server streams
            # notifications before it answers authenticate, so the id
            # is allocated (wire-valid request) but never awaited
            self._send_request(
                self._alloc_id(),
                Method.AUTHENTICATE,
                auth.model_dump(by_alias=True),
            )
            if handshake_delay > 0:
                time.sleep(handshake_delay)
        except Exception:
            self.kill()
            raise

    # --- low-level io -------------------------------------------------------

    def _alloc_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def _write_line(self, payload: dict[str, Any]) -> None:
        data = json.dumps(payload).encode() + b"\n"
        with self._wlock:
            if self.proc.poll() is not None:
                raise AcpError("acp process is not running", kind="process_dead")
            try:
                self.proc.stdin.write(data)
                self.proc.stdin.flush()
            except (BrokenPipeError, OSError, ValueError) as e:
                raise AcpError(f"acp stdin write failed: {e}", kind="process_dead") from e

    def _send_request(self, req_id: int, method: Method | str, params: dict[str, Any]) -> None:
        try:
            m = Method(method)
        except ValueError as e:
            raise AcpError(f"unknown ACP method: {method!r}", kind="invalid_request") from e
        req = JsonRpcRequest(id=req_id, method=m, params=params)
        self._write_line(req.model_dump(mode="json"))

    def _read_message(self, timeout: float | None) -> dict[str, Any]:
        """Return the next complete NDJSON message as a dict.

        ``timeout`` bounds each wait for new bytes (silence timeout -
        it resets whenever data arrives). Raises AcpError with kind
        ``timeout`` / ``eof`` / ``truncated_message`` /
        ``malformed_message``.
        """
        while True:
            nl = self._rbuf.find(b"\n")
            if nl >= 0:
                line, self._rbuf = self._rbuf[:nl], self._rbuf[nl + 1 :]
                if not line.strip():
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError as e:
                    raise AcpError(
                        f"malformed JSON line from acp ({e.msg})",
                        kind="malformed_message",
                    ) from e
                if not isinstance(msg, dict):
                    raise AcpError(
                        "non-object JSON message from acp",
                        kind="malformed_message",
                    )
                return msg
            try:
                ready, _, _ = select.select([self._fd], [], [], timeout)
            except (OSError, ValueError) as e:
                raise AcpError(f"acp stdout poll failed: {e}", kind="process_dead") from e
            if not ready:
                raise AcpError(
                    f"no acp traffic for {timeout}s while waiting for a response",
                    kind="timeout",
                    retryable=True,
                )
            chunk = os.read(self._fd, _READ_CHUNK)
            if not chunk:
                if self._rbuf.strip():
                    lost, self._rbuf = len(self._rbuf), b""
                    raise AcpError(
                        f"acp closed stdout mid-message ({lost} buffered bytes lost)",
                        kind="truncated_message",
                    )
                raise AcpError("acp closed stdout (EOF)", kind="eof")
            self._rbuf += chunk

    def _await_response(
        self,
        req_id: int,
        timeout: float | None,
        on_notification: Callable[[dict[str, Any]], None] | None = None,
    ) -> JsonRpcResponse:
        """Read messages until the response for ``req_id`` arrives."""
        while True:
            msg = self._read_message(timeout)
            if "method" in msg:
                # server->client notification or request: buffer it and
                # keep waiting - it must never crash the loop
                self._notifications.append(msg)
                if on_notification is not None:
                    on_notification(msg)
                continue
            if msg.get("id") != req_id:
                # late reply to an id nobody waits on (e.g. the
                # fire-and-forget authenticate) - drop it
                self._stray_responses += 1
                continue
            return _validated(JsonRpcResponse, msg, "JSON-RPC response envelope")

    @staticmethod
    def _unwrap_result(resp: JsonRpcResponse) -> dict[str, Any]:
        if resp.error is not None:
            raise _acp_error(resp.error)
        if not isinstance(resp.result, dict):
            raise AcpError(
                "acp response has neither result nor error",
                kind="invalid_response",
            )
        return resp.result

    @contextmanager
    def _inflight(self) -> Iterator[None]:
        """Serialize rpc/prompt: one in-flight request per client.

        Fails fast rather than queuing - bridge dispatch is already
        sequential, and a hidden queue would mask caller bugs.
        """
        if not self._slot.acquire(blocking=False):
            raise AcpError(
                "another request is already in flight on this client",
                kind="client_busy",
                retryable=True,
            )
        try:
            yield
        finally:
            self._slot.release()

    # --- requests ------------------------------------------------------------

    def rpc(
        self,
        method: Method | str,
        params: dict[str, Any] | None = None,
        timeout: float | object | None = _DEFAULT,
    ) -> dict[str, Any]:
        """One request/response round-trip; returns ``result``.

        ``timeout`` defaults to the client ``request_timeout``; pass
        ``None`` for no bound or seconds for a silence bound.
        """
        if timeout is _DEFAULT:
            timeout = self._request_timeout
        with self._inflight():
            req_id = self._alloc_id()
            self._send_request(req_id, method, params or {})
            resp = self._await_response(req_id, timeout)
        return self._unwrap_result(resp)

    def list_sessions(
        self,
        cwd: str | None = None,
        timeout: float | object | None = _DEFAULT,
    ) -> list[SessionEntry]:
        params = SessionListParams(cwd=cwd).model_dump(exclude_none=True)
        result = self.rpc(Method.SESSION_LIST, params, timeout)
        return _validated(SessionListResult, result, "session/list result").sessions

    def new_session(
        self,
        cwd: str,
        timeout: float | object | None = _DEFAULT,
    ) -> str:
        params = SessionNewParams(cwd=cwd).model_dump(by_alias=True)
        result = self.rpc(Method.SESSION_NEW, params, timeout)
        sid = result.get("sessionId")
        if not sid:
            raise AcpError(
                "session/new result has no sessionId",
                kind="invalid_response",
            )
        return sid

    def load_session(
        self,
        session_id: str,
        cwd: str,
        timeout: float | object | None = _DEFAULT,
    ) -> dict[str, Any]:
        params = SessionLoadParams(session_id=session_id, cwd=cwd).model_dump(by_alias=True)
        return self.rpc(Method.SESSION_LOAD, params, timeout)

    def close_session(
        self,
        session_id: str,
        force: bool = False,
        timeout: float | object | None = _DEFAULT,
    ) -> None:
        meta = SessionCloseMeta(force=True) if force else None
        params = SessionCloseParams(session_id=session_id, meta=meta).model_dump(
            by_alias=True, exclude_none=True
        )
        self.rpc(Method.SESSION_CLOSE, params, timeout)

    def prompt(
        self,
        session_id: str,
        text: str,
        on_chunk: Callable[[str], None] | None = None,
        timeout: float | None = None,
    ) -> PromptResult:
        """Send a prompt; fold session/update chunks + final response.

        Text is assembled from ``agent_message_chunk`` notifications;
        ``stop_reason`` and ``usage`` come from the final response.
        ``timeout=None`` (default) waits indefinitely - agent turns can
        stream for a long time; pass seconds for a silence bound.
        """
        chunks: list[str] = []

        def collect(msg: dict[str, Any]) -> None:
            try:
                note = SessionUpdateNotification.model_validate(msg)
            except ValidationError:
                # other notification kinds stay buffered only
                return
            piece = message_chunk_text(note.params.update)
            if piece:
                chunks.append(piece)
                if on_chunk is not None:
                    on_chunk(piece)

        params = SessionPromptParams(session_id=session_id, prompt=[PromptBlock(text=text)])
        with self._inflight():
            req_id = self._alloc_id()
            self._send_request(
                req_id,
                Method.SESSION_PROMPT,
                params.model_dump(by_alias=True),
            )
            resp = self._await_response(req_id, timeout, on_notification=collect)
        result = self._unwrap_result(resp)
        wire = _validated(PromptResponse, result, "session/prompt result")
        return PromptResult(
            text="".join(chunks),
            stop_reason=wire.stop_reason,
            usage=dict(result.get("usage") or {}),
        )

    def cancel(self, session_id: str) -> None:
        """Send ``session/cancel`` as a notification (no response id).

        The in-flight ``prompt`` keeps resolving from whatever the
        server ultimately answers on the request id.
        """
        note = JsonRpcNotification(
            method=Method.SESSION_CANCEL,
            params=SessionCancelParams(session_id=session_id).model_dump(by_alias=True),
        )
        self._write_line(note.model_dump(mode="json"))

    def drain_notifications(self) -> list[dict[str, Any]]:
        """Pop and return buffered server notifications (FIFO)."""
        out: list[dict[str, Any]] = []
        while self._notifications:
            out.append(self._notifications.popleft())
        return out

    # --- lifecycle -----------------------------------------------------------

    def alive(self) -> bool:
        return self.proc.poll() is None

    def kill(self) -> None:
        """SIGKILL the child and wait for exit (idempotent)."""
        if self.proc.poll() is not None:
            return
        try:
            self.proc.kill()
        except OSError:
            return
        try:
            self.proc.wait(timeout=_KILL_WAIT_SECONDS)
        except subprocess.TimeoutExpired:
            pass  # SIGKILL cannot be blocked; poll() catches it later

    def terminate(self, timeout: float = _KILL_WAIT_SECONDS) -> None:
        """Graceful shutdown: close stdin (server exits on EOF), wait,
        then kill as fallback."""
        if self.proc.poll() is not None:
            return
        try:
            if self.proc.stdin is not None:
                self.proc.stdin.close()
        except (BrokenPipeError, OSError, ValueError):
            pass
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.kill()

    def __enter__(self) -> "AcpClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.terminate()

"""Validation tests for the c_acp wire contract (item_03)."""

import pytest
from pydantic import ValidationError

from session_bridge.acp.contract import (
    AUTH_METHOD_ID,
    JSONRPC_VERSION,
    USAGE_CACHED_READ_TOKENS,
    USAGE_INPUT_TOKENS,
    USAGE_OUTPUT_TOKENS,
    USAGE_TOTAL_TOKENS,
    AcpError,
    AuthenticateMeta,
    AuthenticateParams,
    ErrorKind,
    JsonRpcError,
    JsonRpcRequest,
    Method,
    PromptBlock,
    PromptResponse,
    PromptResult,
    SessionCloseMeta,
    SessionCloseParams,
    SessionEntry,
    SessionListResult,
    SessionPromptParams,
    SessionUpdateKind,
    SessionUpdateNotification,
    Usage,
    message_chunk_text,
)


def test_item03_gwt1_session_locked_error_kind_modelled() -> None:
    err = JsonRpcError.model_validate(
        {
            "code": -32603,
            "message": "session is locked",
            "data": {
                "cognition.ai/errorKind": "session_locked",
                "retryable": False,
                "lockHolderPid": 4242,
            },
        }
    )
    assert err.code == -32603
    assert err.data.kind == ErrorKind.SESSION_LOCKED == "session_locked"
    assert err.data.retryable is False
    assert err.data.lock_holder_pid == 4242

    exc = AcpError.from_wire(err)
    assert exc.is_session_locked
    assert exc.code == -32603
    assert exc.kind == "session_locked"
    assert exc.lock_holder_pid == 4242
    assert exc.retryable is False


def test_item03_gwt1_malformed_error_payload_raises() -> None:
    bad_payloads = [
        {"message": "no code"},
        {"code": "not-an-int", "message": "x"},
        {"code": 1},  # message missing
        {"code": 1, "message": "x", "data": "oops"},
        {"code": 1, "message": "x", "data": {"cognition.ai/errorKind": 7}},
    ]
    for bad in bad_payloads:
        with pytest.raises(ValidationError):
            JsonRpcError.model_validate(bad)


def test_item03_gwt1_unknown_error_kind_survives_parsing() -> None:
    # kinds outside the verified set must not break the error path
    err = JsonRpcError.model_validate(
        {
            "code": 1,
            "message": "m",
            "data": {"cognition.ai/errorKind": "some_future_kind"},
        }
    )
    assert err.data.kind == "some_future_kind"
    assert not AcpError.from_wire(err).is_session_locked


def test_item03_gwt2_prompt_result_validates() -> None:
    usage = {
        USAGE_TOTAL_TOKENS: 11000,
        USAGE_INPUT_TOKENS: 10000,
        USAGE_OUTPUT_TOKENS: 1000,
        USAGE_CACHED_READ_TOKENS: 0,
    }
    r = PromptResult(text="hello", stop_reason="end_turn", usage=usage)
    assert isinstance(r.usage, dict)
    assert r.usage["totalTokens"] == 11000
    assert PromptResult.model_validate_json(r.model_dump_json()) == r
    # an interrupted stream is representable (no text, no stop reason)
    empty = PromptResult()
    assert empty.text == ""
    assert empty.stop_reason is None
    assert empty.usage == {}
    # usage is a dict, not an arbitrary value
    with pytest.raises(ValidationError):
        PromptResult(text="t", usage="not-a-dict")


def test_item03_gwt2_prompt_response_wire_shape() -> None:
    res = PromptResponse.model_validate(
        {
            "stopReason": "end_turn",
            "usage": {
                "totalTokens": 11,
                "inputTokens": 9,
                "outputTokens": 2,
                "cachedReadTokens": 5,
            },
            "_meta": {"userMessageId": "msg-1"},
        }
    )
    assert res.stop_reason == "end_turn"
    assert res.usage.total_tokens == 11
    assert res.usage.cached_read_tokens == 5
    assert res.meta["userMessageId"] == "msg-1"
    with pytest.raises(ValidationError):
        PromptResponse.model_validate({"usage": {}})  # no stopReason


def test_item03_usage_model_types() -> None:
    u = Usage.model_validate(
        {"totalTokens": 3, "inputTokens": 2, "outputTokens": 1}
    )
    assert u.cached_read_tokens == 0
    with pytest.raises(ValidationError):
        Usage.model_validate(
            {"totalTokens": -1, "inputTokens": 0, "outputTokens": 0}
        )


def test_item03_session_list_entries_with_meta() -> None:
    res = SessionListResult.model_validate(
        {
            "sessions": [
                {
                    "sessionId": "s1",
                    "cwd": "/repo",
                    "title": "t",
                    "updatedAt": "2025-10-29T10:00:00Z",
                    "_meta": {
                        "cognition.ai/isLocked": True,
                        "cognition.ai/lockHolderPid": 4242,
                        "cognition.ai/hasUserMessages": True,
                    },
                    "futureField": "ignored-tolerantly",
                }
            ]
        }
    )
    entry = res.sessions[0]
    assert entry.session_id == "s1"
    assert entry.meta.is_locked is True
    assert entry.meta.lock_holder_pid == 4242
    assert entry.meta.has_user_messages is True
    with pytest.raises(ValidationError):
        SessionEntry.model_validate({"cwd": "/repo"})  # no sessionId


def test_item03_method_surface_declared() -> None:
    assert {m.value for m in Method} == {
        "initialize",
        "authenticate",
        "session/new",
        "session/list",
        "session/load",
        "session/prompt",
        "session/close",
        "session/cancel",
        "session/update",
    }


def test_item03_request_envelope_pins_jsonrpc() -> None:
    req = JsonRpcRequest(id=1, method=Method.SESSION_LIST)
    assert req.model_dump(mode="json")["jsonrpc"] == JSONRPC_VERSION == "2.0"
    with pytest.raises(ValidationError):
        JsonRpcRequest.model_validate(
            {"jsonrpc": "1.0", "id": 1, "method": "session/list"}
        )
    with pytest.raises(ValidationError):
        JsonRpcRequest(id=0, method=Method.SESSION_LIST)


def test_item03_authenticate_params_shape() -> None:
    p = AuthenticateParams(meta=AuthenticateMeta(api_key="k"))
    assert p.model_dump(by_alias=True) == {
        "methodId": AUTH_METHOD_ID,
        "_meta": {"api_key": "k"},
    }
    assert AUTH_METHOD_ID == "devin-browser"


def test_item03_session_close_force_flag_serializes() -> None:
    plain = SessionCloseParams(session_id="s1")
    assert plain.model_dump(by_alias=True) == {
        "sessionId": "s1",
        "_meta": None,
    }
    forced = SessionCloseParams(
        session_id="s1", meta=SessionCloseMeta(force=True)
    )
    assert forced.model_dump(by_alias=True)["_meta"] == {
        "cognition.ai/force": True
    }


def test_item03_session_prompt_params_shape() -> None:
    p = SessionPromptParams(
        session_id="s1", prompt=[PromptBlock(text="do it")]
    )
    assert p.model_dump(by_alias=True) == {
        "sessionId": "s1",
        "prompt": [{"type": "text", "text": "do it"}],
    }
    with pytest.raises(ValidationError):
        SessionPromptParams(session_id="s1", prompt=[])


def test_item03_update_notification_and_chunk_helper() -> None:
    note = SessionUpdateNotification.model_validate(
        {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "s1",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "hi"},
                },
            },
        }
    )
    assert note.params.session_id == "s1"
    assert message_chunk_text(note.params.update) == "hi"
    # non-text kinds yield no chunk text
    for kind in (
        SessionUpdateKind.AGENT_THOUGHT_CHUNK,
        SessionUpdateKind.TOOL_CALL,
        SessionUpdateKind.TOOL_CALL_UPDATE,
        SessionUpdateKind.USAGE_UPDATE,
        SessionUpdateKind.SESSION_INFO_UPDATE,
    ):
        assert message_chunk_text({"sessionUpdate": kind.value}) is None
    # chunk kind without text content yields nothing too
    assert (
        message_chunk_text({"sessionUpdate": "agent_message_chunk"}) is None
    )

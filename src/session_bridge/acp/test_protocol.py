"""Tests for the shared ACP wire-protocol primitives (task #203).

The module exists to kill the drift class where a protocol fix
landed in one transport and not the other - these tests pin the
single source of framing, envelope and hello semantics that both
acp/client.py and wake/acp_host.py now consume.
"""

import json

import pytest

from session_bridge.acp import protocol
from session_bridge.acp.contract import InitializeParams


def test_gwt1_ndjson_buffer_frames_and_blank_lines():
    """[ITEM-203.GWT-1] the shared codec decodes complete NDJSON
    objects, skips blank lines, returns None on a partial frame and
    reports malformed/non-object payloads as NdjsonError - the exact
    semantics both read loops used to implement by hand."""
    buf = protocol.NdjsonBuffer()
    assert buf.pop_message() is None  # empty

    buf.feed(b'{"a": 1}\n\n{"b"')  # one full line, a blank, a partial
    assert buf.pop_message() == {"a": 1}
    assert buf.pop_message() is None  # partial frame stays buffered
    assert buf.pending == b'{"b"'

    buf.feed(b": 2}\n")
    assert buf.pop_message() == {"b": 2}

    buf.feed(b"not-json\n")
    with pytest.raises(protocol.NdjsonError) as e:
        buf.pop_message()
    assert e.value.kind == "malformed_message"

    buf.feed(b"[1,2]\n")
    with pytest.raises(protocol.NdjsonError) as e:
        buf.pop_message()
    assert e.value.kind == "malformed_message"


def test_gwt2_ndjson_take_pending_reports_truncation():
    """[ITEM-203.GWT-2] take_pending returns and clears the partial
    frame - the client's truncated_message accounting and the host's
    eof path both consume it."""
    buf = protocol.NdjsonBuffer()
    buf.feed(b'{"a": 1}\n{"trunc')
    assert buf.pop_message() == {"a": 1}
    assert buf.take_pending() == b'{"trunc'
    assert buf.pending == b""


def test_gwt3_rpc_request_envelope():
    """[ITEM-203.GWT-3] rpc_request produces the canonical wire
    envelope; params defaults to an empty dict."""
    assert protocol.rpc_request(7, "session/list") == {
        "jsonrpc": "2.0",
        "id": 7,
        "method": "session/list",
        "params": {},
    }
    req = protocol.rpc_request(1, "session/prompt", {"sessionId": "s"})
    assert req["params"] == {"sessionId": "s"}


def test_gwt4_response_classification_helpers():
    """[ITEM-203.GWT-4] is_server_call / error_payload / error_detail /
    response_result carry the response-side semantics both transports
    share: method-bearing frames are server calls, wire errors of
    either shape unwrap to a detail string, a missing/non-dict result
    reads as {}."""
    assert protocol.is_server_call({"method": "session/update"}) is True
    assert protocol.is_server_call({"id": 1, "result": {}}) is False

    assert protocol.error_payload({"id": 1, "error": {"code": -1, "message": "boom"}}) == {
        "code": -1,
        "message": "boom",
    }
    assert protocol.error_payload({"id": 1, "result": {}}) is None
    assert protocol.error_detail({"message": "boom"}) == "boom"
    assert protocol.error_detail({"code": -1}) == "{'code': -1}"
    assert protocol.error_detail("raw-failure") == "raw-failure"
    assert protocol.response_result({"result": {"x": 1}}) == {"x": 1}
    assert protocol.response_result({}) == {}
    assert protocol.response_result({"result": [1]}) == {}


def test_gwt5_initialize_params_one_shape_both_consumers():
    """[ITEM-203.GWT-5] the hello payload is built once: the wake
    transport gets clientInfo + fs capabilities, the subprocess
    client gets the bare shape - and the subprocess shape is
    byte-identical to the pydantic InitializeParams it replaces."""
    host_hello = protocol.initialize_params(
        2,
        client_name="sessions-bridge-wake",
        capabilities={"fs": {"readTextFile": False, "writeTextFile": False}},
    )
    assert host_hello == {
        "protocolVersion": 2,
        "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}},
        "clientInfo": {"name": "sessions-bridge-wake", "version": "0"},
    }

    client_hello = protocol.initialize_params(1)
    assert client_hello == InitializeParams().model_dump(by_alias=True)
    assert "clientInfo" not in client_hello


def test_gwt6_encode_message_is_ndjson():
    """[ITEM-203.GWT-6] the send-side codec emits exactly one LF-
    terminated JSON object per frame on both transports."""
    frame = protocol.encode_message({"jsonrpc": "2.0", "id": 1, "method": "m"})
    assert frame.endswith(b"\n") and frame.count(b"\n") == 1
    assert json.loads(frame.decode()) == {"jsonrpc": "2.0", "id": 1, "method": "m"}

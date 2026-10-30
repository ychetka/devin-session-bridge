"""Tests for the ACP client (item_05) against a scriptable fake
``devin acp`` subprocess (tests/fake_acp.py)."""

import json
import sys
import threading
import time
from pathlib import Path

import pytest

from session_bridge.acp.client import AcpClient, read_api_key
from session_bridge.acp.contract import AcpError, ErrorKind, Method

REPO_ROOT = Path(__file__).resolve().parents[3]
FAKE_ACP = REPO_ROOT / "tests" / "fake_acp.py"


@pytest.fixture
def spawn_client(tmp_path):
    """Build an AcpClient on the fake process for a given scenario."""
    clients = []

    def spawn(scenario, **kwargs):
        scenario_path = tmp_path / f"scenario_{len(clients)}.json"
        scenario_path.write_text(json.dumps(scenario))
        client = AcpClient(
            [sys.executable, str(FAKE_ACP), str(scenario_path)],
            api_key="test-api-key",
            handshake_delay=0.05,
            **kwargs,
        )
        clients.append(client)
        return client

    yield spawn
    for client in clients:
        client.kill()


def _chunk(text, session_id="s1"):
    return {
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": session_id,
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "content": {"type": "text", "text": text},
            },
        },
    }


def test_item05_gwt1_rpc_session_list_parses(spawn_client):
    scenario = {
        "rules": [
            {
                "method": "session/list",
                "actions": [
                    # a notification arriving before the response must
                    # be skipped, not crash the wait
                    {
                        "send": {
                            "jsonrpc": "2.0",
                            "method": "session/update",
                            "params": {
                                "sessionId": "s0",
                                "update": {"sessionUpdate": "session_info_update"},
                            },
                        }
                    },
                    {
                        "send": {
                            "jsonrpc": "2.0",
                            "id": "$id",
                            "result": {
                                "sessions": [
                                    {
                                        "sessionId": "s1",
                                        "cwd": "/repo",
                                        "title": "t",
                                        "_meta": {
                                            "cognition.ai/isLocked": True,
                                            "cognition.ai/lockHolderPid": 4242,
                                        },
                                    },
                                    {"sessionId": "s2"},
                                ]
                            },
                        }
                    },
                ],
            }
        ]
    }
    client = spawn_client(scenario)
    sessions = client.list_sessions()
    assert [s.session_id for s in sessions] == ["s1", "s2"]
    assert sessions[0].meta.is_locked is True
    assert sessions[0].meta.lock_holder_pid == 4242
    assert sessions[1].meta is None
    # the pre-response notification was buffered, not lost or fatal
    notes = client.drain_notifications()
    assert [n["method"] for n in notes] == ["session/update"]


def test_item05_gwt1_one_inflight_request_per_client(spawn_client):
    scenario = {
        "rules": [
            {
                "method": "session/list",
                "actions": [
                    {"sleep": 0.5},
                    {
                        "send": {
                            "jsonrpc": "2.0",
                            "id": "$id",
                            "result": {"sessions": []},
                        }
                    },
                ],
            }
        ]
    }
    client = spawn_client(scenario)
    results = {}

    def run():
        try:
            results["r"] = client.rpc(Method.SESSION_LIST)
        except Exception as e:  # pragma: no cover - diagnostic
            results["err"] = e

    t = threading.Thread(target=run)
    t.start()
    time.sleep(0.1)  # let the thread take the in-flight slot
    with pytest.raises(AcpError) as exc_info:
        client.list_sessions()
    assert exc_info.value.kind == "client_busy"
    assert exc_info.value.retryable is True
    t.join(timeout=5)
    assert "err" not in results
    assert results["r"] == {"sessions": []}


def test_item05_gwt2_session_locked_maps_to_acp_error(spawn_client):
    scenario = {
        "rules": [
            {
                "method": "session/load",
                "actions": [
                    {
                        "send": {
                            "jsonrpc": "2.0",
                            "id": "$id",
                            "error": {
                                "code": -32015,
                                "message": "session is locked",
                                "data": {
                                    "cognition.ai/errorKind": "session_locked",
                                    "cognition.ai/retryable": False,
                                    "cognition.ai/lockHolderPid": 4242,
                                },
                            },
                        }
                    }
                ],
            }
        ]
    }
    client = spawn_client(scenario)
    with pytest.raises(AcpError) as exc_info:
        client.load_session("s1", "/repo")
    exc = exc_info.value
    assert exc.kind == ErrorKind.SESSION_LOCKED == "session_locked"
    assert exc.code == -32015
    assert exc.lock_holder_pid == 4242
    assert exc.retryable is False
    assert exc.is_session_locked


def test_item05_gwt2_error_code_fallback_kind(spawn_client):
    # no cognition.ai/errorKind in data -> kind comes from the code map
    scenario = {
        "rules": [
            {
                "method": "session/new",
                "actions": [
                    {
                        "send": {
                            "jsonrpc": "2.0",
                            "id": "$id",
                            "error": {
                                "code": -32601,
                                "message": "no such method",
                            },
                        }
                    }
                ],
            }
        ]
    }
    client = spawn_client(scenario)
    with pytest.raises(AcpError) as exc_info:
        client.new_session("/repo")
    assert exc_info.value.kind == "method_not_found"
    assert exc_info.value.code == -32601


def test_item05_gwt3_prompt_assembles_result(spawn_client):
    usage = {
        "totalTokens": 11000,
        "inputTokens": 10000,
        "outputTokens": 1000,
        "cachedReadTokens": 0,
    }
    scenario = {
        "rules": [
            {
                "method": "session/prompt",
                "actions": [
                    {"send": _chunk("Hello, ")},
                    # non-text update kinds are ignored for text
                    {
                        "send": {
                            "jsonrpc": "2.0",
                            "method": "session/update",
                            "params": {
                                "sessionId": "s1",
                                "update": {
                                    "sessionUpdate": "agent_thought_chunk",
                                    "content": {
                                        "type": "text",
                                        "text": "internal",
                                    },
                                },
                            },
                        }
                    },
                    {"send": _chunk("world")},
                    {
                        "send": {
                            "jsonrpc": "2.0",
                            "id": "$id",
                            "result": {
                                "stopReason": "end_turn",
                                "usage": usage,
                                "_meta": {"userMessageId": "m1"},
                            },
                        }
                    },
                ],
            }
        ]
    }
    client = spawn_client(scenario)
    seen = []
    result = client.prompt("s1", "hi", on_chunk=seen.append)
    assert result.text == "Hello, world"
    assert result.stop_reason == "end_turn"
    assert result.usage["totalTokens"] == 11000
    assert seen == ["Hello, ", "world"]


def test_item05_gwt3_cancel_does_not_steal_inflight_prompt(spawn_client):
    scenario = {
        "rules": [
            {
                "method": "session/prompt",
                "actions": [
                    {"send": _chunk("partial")},
                    {"sleep": 0.4},
                    {
                        "send": {
                            "jsonrpc": "2.0",
                            "id": "$id",
                            "result": {
                                "stopReason": "cancelled",
                                "usage": {
                                    "totalTokens": 1,
                                    "inputTokens": 1,
                                    "outputTokens": 0,
                                },
                            },
                        }
                    },
                ],
            }
        ]
    }
    client = spawn_client(scenario)
    results = {}

    def run():
        try:
            results["r"] = client.prompt("s1", "hi")
        except Exception as e:  # pragma: no cover - diagnostic
            results["err"] = e

    t = threading.Thread(target=run)
    t.start()
    time.sleep(0.1)  # prompt is in flight now
    client.cancel("s1")  # notification only - the slot stays with prompt
    t.join(timeout=5)
    assert "err" not in results
    # the in-flight prompt resolves by the server's own response
    assert results["r"].text == "partial"
    assert results["r"].stop_reason == "cancelled"


def test_item05_gwt4_malformed_line_is_controlled_error(spawn_client):
    scenario = {
        "rules": [
            {
                "method": "session/list",
                "actions": [{"raw": "{not valid json"}],
            }
        ]
    }
    client = spawn_client(scenario)
    with pytest.raises(AcpError) as exc_info:
        client.list_sessions()
    assert exc_info.value.kind == "malformed_message"


def test_item05_gwt4_truncated_eof_is_distinct_error(spawn_client):
    scenario = {
        "rules": [
            {
                "method": "session/list",
                "actions": [
                    {"partial": '{"jsonrpc": "2.0", "id": 1, "result": {'},
                    {"close_stdout": True},
                    {"exit": 0},
                ],
            }
        ]
    }
    client = spawn_client(scenario)
    with pytest.raises(AcpError) as exc_info:
        client.list_sessions()
    # a truncated tail is NOT conflated with a malformed complete line
    assert exc_info.value.kind == "truncated_message"


def test_item05_gwt4_silence_timeout_is_retryable(spawn_client):
    scenario = {"rules": [{"method": "session/list", "actions": [{"sleep": 2.0}]}]}
    client = spawn_client(scenario)
    t0 = time.monotonic()
    with pytest.raises(AcpError) as exc_info:
        client.rpc(Method.SESSION_LIST, timeout=0.3)
    assert exc_info.value.kind == "timeout"
    assert exc_info.value.retryable is True
    assert time.monotonic() - t0 < 2.0


def test_item05_gwt4_missing_credentials_raise_acp_error(tmp_path):
    # credential failure happens before any process is spawned
    with pytest.raises(AcpError) as exc_info:
        AcpClient(
            "definitely-not-a-real-devin-binary",
            credentials_path=str(tmp_path / "nope.toml"),
        )
    assert exc_info.value.kind == "credentials"


def test_item05_gwt4_credentials_file_without_key(tmp_path):
    p = tmp_path / "credentials.toml"
    p.write_text('other_key = "x"\n')
    with pytest.raises(AcpError) as exc_info:
        read_api_key(str(p))
    assert exc_info.value.kind == "credentials"


def test_item05_gwt4_credentials_key_parsed(tmp_path):
    p = tmp_path / "credentials.toml"
    p.write_text('windsurf_api_key = "sk-test-123"\n')
    assert read_api_key(str(p)) == "sk-test-123"


def test_item05_gwt5_dead_process_alive_and_kill(spawn_client):
    client = spawn_client({"rules": []})
    assert client.alive() is True
    client.kill()
    assert client.alive() is False
    client.kill()  # idempotent on an already-dead process
    with pytest.raises(AcpError) as exc_info:
        client.list_sessions()
    assert exc_info.value.kind == "process_dead"


def test_item05_gwt5_process_exit_mid_request(spawn_client):
    scenario = {"rules": [{"method": "session/list", "actions": [{"exit": 1}]}]}
    client = spawn_client(scenario)
    with pytest.raises(AcpError) as exc_info:
        client.list_sessions()
    # either the dead-process write guard or the EOF read trips first
    assert exc_info.value.kind in ("process_dead", "eof")
    assert client.alive() is False

"""Validation tests for the c_hooks wire contract (item_03)."""

import pytest
from pydantic import ValidationError

from session_bridge.hooks.contract import (
    EXIT_BLOCK,
    HOOK_TIMEOUT_DEFAULT,
    PARKED_TASK_PREFIX,
    BlockOutput,
    ContextOutput,
    HookEventName,
    HookPayload,
    HookSpecificOutput,
    ParkConfig,
    SessionEndPayload,
    SessionStartPayload,
    StopPayload,
    UserPromptSubmitPayload,
    parse_hook_payload,
)


def test_item03_gwt3_stop_payload_fields_typed() -> None:
    p = StopPayload.model_validate(
        {
            "hook_event_name": "Stop",
            "session_id": "s1",
            "prompt_id": "p1",
            "stop_hook_active": True,
            "last_assistant_message": "done so far",
        }
    )
    assert p.hook_event_name is HookEventName.STOP
    assert isinstance(p.stop_hook_active, bool)
    assert p.last_assistant_message == "done so far"
    assert p.prompt_id == "p1"

    # prompt_id absent pre-first-prompt; message may be null
    p2 = StopPayload.model_validate(
        {
            "hook_event_name": "Stop",
            "session_id": "s1",
            "stop_hook_active": False,
            "last_assistant_message": None,
        }
    )
    assert p2.prompt_id is None
    assert p2.last_assistant_message is None

    # wrong types on the typed fields are rejected
    with pytest.raises(ValidationError):
        StopPayload.model_validate(
            {
                "hook_event_name": "Stop",
                "session_id": "s1",
                "stop_hook_active": "maybe",
                "last_assistant_message": None,
            }
        )
    with pytest.raises(ValidationError):
        StopPayload.model_validate(
            {
                "hook_event_name": "Stop",
                "session_id": "s1",
                "stop_hook_active": False,
                "last_assistant_message": 123,
            }
        )


def test_item03_gwt3_stop_payload_requires_stop_fields() -> None:
    base = {"hook_event_name": "Stop", "session_id": "s1"}
    with pytest.raises(ValidationError):
        StopPayload.model_validate(base)  # stop fields absent
    with pytest.raises(ValidationError):
        StopPayload.model_validate(
            {**base, "stop_hook_active": False}
        )  # last_assistant_message absent


def test_item03_gwt3_common_fields_required() -> None:
    with pytest.raises(ValidationError):
        HookPayload.model_validate({"session_id": "s1"})  # no event name
    with pytest.raises(ValidationError):
        parse_hook_payload({"hook_event_name": "Stop"})  # no session_id
    with pytest.raises(ValidationError):
        parse_hook_payload({"session_id": "s1"})  # no event name
    with pytest.raises(ValidationError):
        parse_hook_payload({"hook_event_name": "Stop", "session_id": ""})  # empty session_id
    # events outside the installed set fail in a controlled way
    with pytest.raises(ValidationError):
        parse_hook_payload({"hook_event_name": "PreToolUse", "session_id": "s1"})


def test_item03_gwt3_event_dispatch() -> None:
    p = parse_hook_payload(
        {
            "hook_event_name": "UserPromptSubmit",
            "session_id": "s1",
            "prompt": ".",
        }
    )
    assert isinstance(p, UserPromptSubmitPayload)
    assert p.prompt == "."

    s = parse_hook_payload(
        {
            "hook_event_name": "SessionStart",
            "session_id": "s1",
            "source": "startup",
        }
    )
    assert isinstance(s, SessionStartPayload)
    assert s.source == "startup"

    e = parse_hook_payload(
        {
            "hook_event_name": "SessionEnd",
            "session_id": "s1",
            "reason": "clear",
        }
    )
    assert isinstance(e, SessionEndPayload)
    assert e.reason == "clear"

    st = parse_hook_payload(
        {
            "hook_event_name": "Stop",
            "session_id": "s1",
            "stop_hook_active": False,
            "last_assistant_message": None,
        }
    )
    assert isinstance(st, StopPayload)


def test_item03_gwt3_payload_tolerates_unknown_fields() -> None:
    # inbound payloads keep extra=allow: fields Devin adds later
    # (cwd, transcript_path, ...) must not break parsing
    p = parse_hook_payload(
        {
            "hook_event_name": "Stop",
            "session_id": "s1",
            "stop_hook_active": False,
            "last_assistant_message": None,
            "cwd": "/repo",
            "transcript_path": "/tmp/t.jsonl",
        }
    )
    assert isinstance(p, StopPayload)
    assert p.model_extra["cwd"] == "/repo"


def test_item03_gwt4_parkconfig_invariants() -> None:
    ok = ParkConfig(cycle_seconds=150, cycles_max=5, hook_timeout=600)
    assert ok.hook_timeout == 600
    # default timeout is the verified devin default
    assert ParkConfig(cycle_seconds=60, cycles_max=1).hook_timeout == HOOK_TIMEOUT_DEFAULT == 60
    # boundary: cycle equal to the timeout is allowed
    ParkConfig(cycle_seconds=60, cycles_max=1, hook_timeout=60)

    # a cycle longer than the hook timeout dies mid-park: rejected
    with pytest.raises(ValidationError):
        ParkConfig(cycle_seconds=150, cycles_max=5)  # > default 60
    with pytest.raises(ValidationError):
        ParkConfig(cycle_seconds=61, cycles_max=5, hook_timeout=60)

    # the budget needs at least one cycle
    with pytest.raises(ValidationError):
        ParkConfig(cycle_seconds=10, cycles_max=0, hook_timeout=60)
    with pytest.raises(ValidationError):
        ParkConfig(cycle_seconds=10, cycles_max=-1, hook_timeout=60)

    # a zero-length cycle is meaningless
    with pytest.raises(ValidationError):
        ParkConfig(cycle_seconds=0, cycles_max=1, hook_timeout=60)


def test_item03_gwt5_block_output_serializes() -> None:
    reason = f"{PARKED_TASK_PREFIX} do the thing"
    out = BlockOutput(reason=reason)
    assert out.model_dump() == {"decision": "block", "reason": reason}
    assert out.model_dump_json() == ('{"decision":"block","reason":"[PARKED>TASK] do the thing"}')
    with pytest.raises(ValidationError):
        BlockOutput(reason="")  # an empty reason is useless
    with pytest.raises(ValidationError):
        BlockOutput.model_validate({"decision": "block", "reason": "r", "extra": 1})
    with pytest.raises(ValidationError):
        BlockOutput.model_validate({"decision": "allow", "reason": "r"})


def test_item03_gwt5_context_output_serializes() -> None:
    out = ContextOutput(
        hook_specific_output=HookSpecificOutput(
            hook_event_name=HookEventName.USER_PROMPT_SUBMIT,
            additional_context="queued task text",
        )
    )
    assert out.model_dump(by_alias=True) == {
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": "queued task text",
        }
    }
    # round-trip from the wire form
    parsed = ContextOutput.model_validate(out.model_dump(by_alias=True))
    assert parsed == out
    with pytest.raises(ValidationError):
        ContextOutput.model_validate(
            {"hookSpecificOutput": {"hookEventName": "UserPromptSubmit"}}
        )  # no additionalContext


def test_item03_exit_code_semantics() -> None:
    # exit 2 = block; 0 = allow; other nonzero = error, does not block
    assert EXIT_BLOCK == 2

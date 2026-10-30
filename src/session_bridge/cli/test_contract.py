"""Validation tests for the c_cli contract (item_02)."""

import pytest
from pydantic import ValidationError

from session_bridge.cli.contract import (
    INTERNAL_COMMANDS,
    Command,
    Event,
    EventKind,
    ExitCode,
    JsonResponse,
)


def test_item02_gwt3_command_enum_covers_surface() -> None:
    assert {c.value for c in Command} == {
        "push",
        "status",
        "inspect",
        "clear",
        "unpark",
        "retry",
        "_work",
    }
    # _work is the internal per-task worker entry, not an operator command
    assert Command.WORK.value == "_work"
    assert Command.WORK in INTERNAL_COMMANDS


def test_item02_gwt3_json_response_declares_orchestrator_fields() -> None:
    r = JsonResponse(
        session="s1",
        status="queued",
        queue_depth=2,
        events=[
            Event(kind=EventKind.NEEDS_KICK, detail="kick the session"),
            Event(kind=EventKind.FAILED, task_id=7),
        ],
        agent_instruction="do not retry",
    )
    d = r.model_dump(mode="json")
    for field in (
        "session",
        "status",
        "queue_depth",
        "events",
        "agent_instruction",
    ):
        assert field in d
    assert d["session"] == "s1"
    assert d["queue_depth"] == 2
    assert [e["kind"] for e in d["events"]] == ["NEEDS_KICK", "FAILED"]
    assert d["agent_instruction"] == "do not retry"
    # JSON round-trip is lossless for the orchestrator-facing schema
    assert JsonResponse.model_validate_json(r.model_dump_json()) == r


def test_item02_gwt3_event_kinds_declared() -> None:
    assert {k.value for k in EventKind} == {"NEEDS_KICK", "FAILED"}
    with pytest.raises(ValidationError):
        Event(kind="SOMETHING_ELSE")


def test_item02_gwt3_exit_code_enum_declared() -> None:
    assert ExitCode.OK == 0
    names = {e.name.lower() for e in ExitCode}
    assert {"not_found", "conflict", "internal"} <= names
    for code in ExitCode:
        if code is ExitCode.OK:
            assert code.value == 0
        else:
            assert code.value != 0

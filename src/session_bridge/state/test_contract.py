"""Validation tests for the c_state domain contract (item_02)."""

import pytest
from pydantic import ValidationError

from session_bridge.state.contract import (
    BUSY_TIMEOUT_MS,
    JOURNAL_MODE,
    LIVE_STATUSES,
    MANUAL_TRANSITIONS,
    MAX_ATTEMPTS,
    TERMINAL_STATUSES,
    TRANSITIONS,
    Session,
    Task,
    TaskStatus,
    Transport,
    is_valid_transition,
    validate_transition,
)

Q = TaskStatus.QUEUED
DD = TaskStatus.DISPATCH_DECIDED
R = TaskStatus.RUNNING
AU = TaskStatus.AWAITING_USER
DONE = TaskStatus.DONE
FAILED = TaskStatus.FAILED
PAUSED = TaskStatus.PAUSED


def _task(**over) -> Task:
    base = {
        "session_id": "s1",
        "task_text": "do the thing",
        "created_at": 100,
        "updated_at": 100,
    }
    base.update(over)
    return Task(**base)


def test_item02_gwt1_task_status_lists_full_set() -> None:
    assert {s.value for s in TaskStatus} == {
        "queued",
        "dispatch_decided",
        "running",
        "awaiting_user",
        "done",
        "failed",
        "paused",
    }
    assert LIVE_STATUSES == frozenset({Q, DD, R, AU})
    assert TERMINAL_STATUSES == frozenset({DONE, FAILED})


def test_item02_gwt1_transition_map_declares_lifecycle() -> None:
    # queued > dispatch_decided > running > {done, failed, requeue, awaiting_user}
    assert TRANSITIONS[Q] == frozenset({DD})
    assert TRANSITIONS[DD] == frozenset({R, Q})
    assert TRANSITIONS[R] == frozenset({DONE, FAILED, Q, AU})
    # every status is a key; terminal statuses have no outgoing edges
    assert set(TRANSITIONS) == set(TaskStatus)
    assert TRANSITIONS[DONE] == frozenset()
    assert TRANSITIONS[FAILED] == frozenset()
    # each declared edge validates on the automatic path
    for src, targets in TRANSITIONS.items():
        for dst in targets:
            validate_transition(src, dst)


def test_item02_gwt1_invalid_transition_raises_validation_error() -> None:
    invalid = [
        (Q, R),
        (Q, DONE),
        (Q, FAILED),
        (DD, FAILED),
        (DD, DONE),
        (R, DD),
        (R, PAUSED),
        (DONE, Q),
        (DONE, FAILED),
        (PAUSED, R),
    ]
    for src, dst in invalid:
        assert not is_valid_transition(src, dst)
        with pytest.raises(ValidationError):
            validate_transition(src, dst)
    # same rule through the Task API
    with pytest.raises(ValidationError):
        _task(status=Q).transition_to(R)


def test_item02_gwt2_valid_task_validates_and_round_trips() -> None:
    t = _task(
        id=7,
        status=R,
        transport=Transport.ACP_PUSH,
        lease_owner="worker-1",
        lease_expires_at=1900,
        lease_renewed_at=600,
        attempt=1,
        hook_pid=4242,
        last_notified_at=900,
        last_assistant_message="done so far",
        note="n",
    )
    rt = Task.model_validate_json(t.model_dump_json())
    assert rt == t
    rt2 = Task.model_validate(t.model_dump())
    assert rt2 == t
    for field in (
        "session_id",
        "task_text",
        "status",
        "transport",
        "lease_owner",
        "attempt",
        "created_at",
        "updated_at",
    ):
        assert getattr(rt, field) == getattr(t, field)


def test_item02_gwt2_lease_fields_typed_int_or_none() -> None:
    t = _task()
    assert t.lease_owner is None
    assert t.lease_expires_at is None
    assert t.lease_renewed_at is None
    ok = _task(lease_owner="w", lease_expires_at=123, lease_renewed_at=100)
    assert isinstance(ok.lease_expires_at, int)
    assert isinstance(ok.lease_renewed_at, int)
    with pytest.raises(ValidationError):
        _task(lease_expires_at="soon")
    with pytest.raises(ValidationError):
        _task(lease_renewed_at="soon")


def test_item02_gwt2_session_model_round_trip() -> None:
    s = Session(
        session_id="s1",
        cwd="/repo",
        title="t",
        queue_paused=True,
        park_cycles_left=3,
        updated_at=1,
    )
    assert Session.model_validate_json(s.model_dump_json()) == s
    with pytest.raises(ValidationError):
        Session(session_id="s1", park_cycles_left=-1, updated_at=1)


def test_item02_gwt4_failed_is_terminal_for_auto_path() -> None:
    assert TRANSITIONS[FAILED] == frozenset()
    for dst in TaskStatus:
        assert not is_valid_transition(FAILED, dst)
    t = _task(status=FAILED, attempt=MAX_ATTEMPTS)
    with pytest.raises(ValidationError):
        t.transition_to(Q)  # automatic path: no edge out of failed


def test_item02_gwt4_manual_retry_resets_attempt() -> None:
    t = _task(
        status=FAILED,
        attempt=MAX_ATTEMPTS,
        lease_owner="w",
        lease_expires_at=9,
        lease_renewed_at=5,
    )
    r = t.retry(note="operator retry")
    assert r.status is Q
    assert r.attempt == 0
    assert r.lease_owner is None
    assert r.lease_expires_at is None
    assert r.lease_renewed_at is None
    assert r.note == "operator retry"
    # retried task re-enters the lifecycle
    assert is_valid_transition(r.status, DD)
    # retry is defined only for failed tasks
    with pytest.raises(ValidationError):
        _task(status=R).retry()


def test_item02_gwt4_auto_retry_after_failed_not_expressible() -> None:
    # the only failed > queued edge lives on the manual path
    assert Q not in TRANSITIONS[FAILED]
    assert Q in MANUAL_TRANSITIONS[FAILED]
    with pytest.raises(ValidationError):
        validate_transition(FAILED, Q)
    validate_transition(FAILED, Q, manual=True)


def test_item02_gwt4_requeue_or_fail_attempt_budget() -> None:
    # attempt 0 > requeue once (the single auto-requeue of MAX_ATTEMPTS=2)
    t0 = _task(status=R, attempt=0, lease_owner="w", lease_expires_at=9)
    r1 = t0.requeue_or_fail()
    assert r1.status is Q
    assert r1.attempt == 1
    assert r1.lease_owner is None
    assert r1.lease_expires_at is None
    # second expiry lands on terminal failed
    t1 = _task(status=R, attempt=MAX_ATTEMPTS - 1)
    f = t1.requeue_or_fail()
    assert f.status is FAILED
    assert f.attempt == MAX_ATTEMPTS
    # terminal: no further automatic requeue
    with pytest.raises(ValidationError):
        f.requeue_or_fail()


def test_item02_storage_constants_pinned() -> None:
    assert JOURNAL_MODE == "wal"
    assert BUSY_TIMEOUT_MS == 5000
    assert MAX_ATTEMPTS == 2

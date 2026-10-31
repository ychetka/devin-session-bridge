"""Validation tests for the c_state domain contract (item_02)."""

import pytest
from pydantic import ValidationError

from session_bridge.state.contract import (
    BUSY_TIMEOUT_MS,
    CLAIM_TIMEOUT_SECONDS,
    JOURNAL_MODE,
    LIVE_STATUSES,
    MANUAL_TRANSITIONS,
    MAX_ATTEMPTS,
    MAX_QUESTIONS_PER_TASK,
    QUESTION_JANITOR_TRANSITIONS,
    QUESTION_TRANSITIONS,
    TERMINAL_STATUSES,
    TRANSITIONS,
    TURN_TTL_CYCLES,
    WATCHER_TTL_SECONDS,
    AuditEvent,
    AuditEventKind,
    Notification,
    NotificationKind,
    Question,
    QuestionState,
    Session,
    SessionMode,
    SessionRole,
    SessionStatus,
    Task,
    TaskStatus,
    Transport,
    WorkerHeartbeat,
    is_valid_question_transition,
    is_valid_transition,
    validate_question_transition,
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
    assert TRANSITIONS[DD] == frozenset({R, Q, FAILED})
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


# --- item_13a: message bus schema v2 -------------------------------------


def _question(**over) -> Question:
    base = {
        "task_id": 1,
        "session_id": "w1",
        "text": "which endpoint?",
        "asked_at": 100,
    }
    base.update(over)
    return Question(**base)


def test_item13a_gwt2_question_state_machine_declared_edges() -> None:
    # pending > claimed > answered; answered is terminal
    assert QUESTION_TRANSITIONS[QuestionState.PENDING] == frozenset({QuestionState.CLAIMED})
    assert QUESTION_TRANSITIONS[QuestionState.CLAIMED] == frozenset({QuestionState.ANSWERED})
    assert QUESTION_TRANSITIONS[QuestionState.ANSWERED] == frozenset()
    assert set(QUESTION_TRANSITIONS) == set(QuestionState)
    for src, targets in QUESTION_TRANSITIONS.items():
        for dst in targets:
            validate_question_transition(src, dst)


def test_item13a_gwt2_question_lifecycle_through_model() -> None:
    q = _question()
    c = q.transition_to(QuestionState.CLAIMED, claimed_by="orc-1", claimed_at=110)
    assert c.state is QuestionState.CLAIMED
    a = c.transition_to(QuestionState.ANSWERED, answer="/v2/items", answered_at=120)
    assert a.state is QuestionState.ANSWERED
    assert a.answer == "/v2/items"
    # answered is terminal through the model too
    with pytest.raises(ValidationError):
        a.transition_to(QuestionState.PENDING)


def test_item13a_gwt2_question_invalid_edges_rejected() -> None:
    invalid = [
        (QuestionState.PENDING, QuestionState.ANSWERED),
        (QuestionState.PENDING, QuestionState.PENDING),
        (QuestionState.ANSWERED, QuestionState.PENDING),
        (QuestionState.ANSWERED, QuestionState.CLAIMED),
    ]
    for src, dst in invalid:
        assert not is_valid_question_transition(src, dst)
        with pytest.raises(ValidationError):
            validate_question_transition(src, dst)
    # the janitor edge is not on the regular path
    assert not is_valid_question_transition(QuestionState.CLAIMED, QuestionState.PENDING)
    claimed = _question(state=QuestionState.CLAIMED, claimed_by="o", claimed_at=50)
    with pytest.raises(ValidationError):
        claimed.transition_to(QuestionState.PENDING)


def test_item13a_gwt2_janitor_release_preserves_question_id() -> None:
    q = _question(id=42, state=QuestionState.CLAIMED, claimed_by="orc-1", claimed_at=50)
    r = q.release_claim()
    # same question_id persists on timeout - no duplicate re-ask
    assert r.id == q.id
    assert r.state is QuestionState.PENDING
    assert r.claimed_by is None
    assert r.claimed_at is None
    # claimed > pending exists only on the janitor path
    assert QuestionState.PENDING in QUESTION_JANITOR_TRANSITIONS[QuestionState.CLAIMED]
    assert QuestionState.PENDING not in QUESTION_TRANSITIONS[QuestionState.CLAIMED]
    assert is_valid_question_transition(QuestionState.CLAIMED, QuestionState.PENDING, janitor=True)


def test_item13a_gwt2_question_state_field_coherence() -> None:
    # claimed requires the claim fields
    with pytest.raises(ValidationError):
        _question(state=QuestionState.CLAIMED)
    with pytest.raises(ValidationError):
        _question(state=QuestionState.CLAIMED, claimed_by="o")
    # claimed must not carry answer fields yet
    with pytest.raises(ValidationError):
        _question(
            state=QuestionState.CLAIMED,
            claimed_by="o",
            claimed_at=50,
            answer="x",
        )
    # answered requires the answer fields and keeps the claim fields
    with pytest.raises(ValidationError):
        _question(state=QuestionState.ANSWERED, answer="x", answered_at=60)
    with pytest.raises(ValidationError):
        _question(
            state=QuestionState.ANSWERED,
            claimed_by="o",
            claimed_at=50,
            answer="x",
        )
    ok = _question(
        state=QuestionState.ANSWERED,
        claimed_by="o",
        claimed_at=50,
        answer="x",
        answered_at=60,
    )
    assert ok.state is QuestionState.ANSWERED
    # pending carries no claim/answer leftovers
    with pytest.raises(ValidationError):
        _question(claimed_by="o")
    with pytest.raises(ValidationError):
        _question(answered_at=60)


def test_item13a_gwt1_unique_pending_per_task_is_db_level() -> None:
    # Two pending rows for the same task both validate as models: the
    # invariant is a declared partial unique index in db.py
    # (UNIQUE(task_id) WHERE state='pending'), not a model rule, so the
    # second ask is rejected by the db layer, not by validation here.
    a = _question(id=1)
    b = _question(id=2)
    assert a.state is b.state is QuestionState.PENDING
    assert a.task_id == b.task_id
    # the DoA question budget is pinned
    assert MAX_QUESTIONS_PER_TASK == 3


def test_item13a_gwt3_notification_outbox_semantics() -> None:
    # writer commits with relayed_at NULL; the daemon relay marks it
    n = Notification(
        session_id="w1",
        kind=NotificationKind.TASK_DISPATCH,
        task_id=1,
        body="task #1 assigned",
        created_at=100,
    )
    assert n.relayed_at is None
    assert n.ack_at is None
    r = Notification(
        id=5,
        session_id="w1",
        kind=NotificationKind.QUESTION_ANSWER,
        task_id=1,
        question_id=9,
        body="answer to question #9",
        relayed_at=110,
        ack_at=120,
        created_at=100,
    )
    assert Notification.model_validate_json(r.model_dump_json()) == r
    # at-least-once delivery: a relay retry re-delivers the same row id,
    # which is the consumer's idempotent-dedup key; the kind + refs are
    # the routing key to the pending question
    redelivery = r.model_copy()
    assert redelivery.id == r.id
    assert r.kind is NotificationKind.QUESTION_ANSWER
    assert r.question_id == 9
    # kind is required for correlation; the refs stay optional
    with pytest.raises(ValidationError):
        Notification(session_id="w1", body="b", created_at=100)
    assert {k.value for k in NotificationKind} == {
        "task_dispatch",
        "question_answer",
        "directive",
        "disarm",
        "heartbeat_tick",
    }


def test_item13a_worker_heartbeat_generation() -> None:
    hb = WorkerHeartbeat(session_id="w1", last_seen_at=100)
    assert hb.generation == 0  # register bumps it from here
    bumped = WorkerHeartbeat(session_id="w1", last_seen_at=110, generation=1)
    assert bumped.generation == 1
    with pytest.raises(ValidationError):
        WorkerHeartbeat(session_id="w1", last_seen_at=1, generation=-1)


def test_item13a_audit_event_verbatim_and_rotation_kinds() -> None:
    e = AuditEvent(
        kind=AuditEventKind.DELIBERATION,
        session_id="orc-1",
        task_id=3,
        question_id=9,
        question_text="which endpoint?",
        partner_outputs={
            "gemini": "verbatim gemini",
            "copilot": "verbatim copilot",
        },
        synthesis="use /v2",
        directive="call /v2/items",
        asked_at=100,
        claimed_at=110,
        answered_at=120,
        created_at=121,
    )
    assert AuditEvent.model_validate_json(e.model_dump_json()) == e
    # DoA trigger names the whitelist rule that fired
    esc = AuditEvent(kind=AuditEventKind.ESCALATION, doa_trigger="destructive op", created_at=1)
    assert esc.doa_trigger == "destructive op"
    # rotation event kinds are declared alongside the pipeline kinds
    assert {k.value for k in AuditEventKind} == {
        "deliberation",
        "escalation",
        "turn_rotation",
        "watcher_rotation",
    }


def test_item13a_session_orchestration_fields() -> None:
    s = Session(session_id="s1", updated_at=1)
    assert s.role is SessionRole.WORKER
    assert s.mode is SessionMode.UNMANAGED
    assert s.status is SessionStatus.ACTIVE
    # liveness/generation live in WorkerHeartbeat, not on the session
    assert "heartbeat_generation" not in Session.model_fields
    o = Session(
        session_id="o1",
        role=SessionRole.ORCHESTRATOR,
        mode=SessionMode.MANAGED,
        status=SessionStatus.DEAD,
        updated_at=1,
    )
    assert Session.model_validate_json(o.model_dump_json()) == o
    with pytest.raises(ValidationError):
        Session(session_id="s1", heartbeat_generation=-1, updated_at=1)


def test_item13a_task_accepts_run_after() -> None:
    assert _task().run_after is None
    t = _task(run_after=200)
    assert t.run_after == 200
    assert Task.model_validate_json(t.model_dump_json()) == t


def test_item13a_models_reject_extra_fields() -> None:
    cases = [
        (Question, {"task_id": 1, "session_id": "w1", "text": "t", "asked_at": 1}),
        (
            Notification,
            {
                "session_id": "w1",
                "kind": NotificationKind.DIRECTIVE,
                "body": "b",
                "created_at": 1,
            },
        ),
        (WorkerHeartbeat, {"session_id": "w1", "last_seen_at": 1}),
        (AuditEvent, {"kind": AuditEventKind.DELIBERATION, "created_at": 1}),
        (Session, {"session_id": "s1", "updated_at": 1}),
        (Task, {"session_id": "s1", "task_text": "t", "created_at": 1, "updated_at": 1}),
    ]
    for cls, kwargs in cases:
        with pytest.raises(ValidationError):
            cls(**kwargs, bogus=1)


def test_item13a_orchestration_constants_pinned() -> None:
    assert MAX_QUESTIONS_PER_TASK == 3
    assert CLAIM_TIMEOUT_SECONDS == 300
    assert TURN_TTL_CYCLES == 10
    assert WATCHER_TTL_SECONDS == 7200

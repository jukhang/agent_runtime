"""An incomplete success must reach bounded correction, not silent SQL downgrade."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from test_decision_validation import facts

from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.candidate_repair import CandidateRejected
from agent_runtime.contracts.decisions import Finish, FinishPayload
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.candidate_repair import SqlCandidateRepairs, recovery_feedback


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome,complete", [("success", False), ("partial", False), ("success", True)]
)
async def test_completion_preflight_consumes_verifier_result(monkeypatch, outcome, complete):
    state, context, _ = facts()
    actor = ActorContext(
        scope_id=state.scope_id, actor_id="owner", auth_revision="1", grants_ref="g"
    )
    failed = uuid4()
    result = SimpleNamespace(scalars=lambda: [failed])

    @asynccontextmanager
    async def transaction():
        yield None

    @asynccontextmanager
    async def sessions():
        yield SimpleNamespace(begin=transaction, execute=AsyncMock(return_value=result))

    repairs = object.__new__(SqlCandidateRepairs)
    repairs._actor, repairs._schema, repairs._sessions = actor, "test", sessions
    verifier = AsyncMock(return_value=complete)
    monkeypatch.setattr("agent_runtime.storage.candidate_repair.verify_tool_recoveries", verifier)
    frame = DecisionFrame(
        actor=actor,
        context=context,
        attempt_id=uuid4(),
        action=Finish(
            kind="finish",
            payload=FinishPayload(outcome=outcome, evidence_refs=(), final_item_ids=()),
        ),
    )
    if outcome == "success" and not complete:
        with pytest.raises(CandidateRejected) as rejected:
            await repairs.validate(state, frame)
        assert rejected.value.reason == "invalid_evidence"
        assert str(failed) not in rejected.value.detail  # Not delivered in this snapshot.
        assert "1 total" in rejected.value.detail
        assert rejected.value.attempt_id == frame.attempt_id
        assert rejected.value.snapshot_id == context.snapshot_id
    else:
        await repairs.validate(state, frame)
    verifier.assert_awaited_once()


def test_recovery_feedback_is_bounded_and_does_not_disclose_undelivered_ids():
    failures = {uuid4() for _ in range(128)}
    visible_failure = next(iter(failures))
    completed = uuid4()
    value = recovery_feedback(
        "Recovery assessment does not name an unsuccessful tool.",
        failures,
        {str(visible_failure), str(completed)},
    )
    assert len(value) <= 1000
    assert str(visible_failure) in value and str(completed) in value
    assert all(str(identity) not in value for identity in failures - {visible_failure})
    assert "128 total" in value and "FULL recoveredToolFailures" in value
    assert "never invent" in value


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["INVALID_EVIDENCE", "AUDIT_UNAVAILABLE", "SIDE_EFFECT_UNKNOWN"])
async def test_invalid_identity_gets_hints_but_integrity_failures_are_not_repairable(
    monkeypatch, code
):
    state, context, _ = facts()
    actor = ActorContext(
        scope_id=state.scope_id, actor_id="owner", auth_revision="1", grants_ref="g"
    )

    @asynccontextmanager
    async def transaction():
        yield None

    @asynccontextmanager
    async def sessions():
        yield SimpleNamespace(
            begin=transaction, execute=AsyncMock(return_value=SimpleNamespace(scalars=lambda: []))
        )

    repairs = object.__new__(SqlCandidateRepairs)
    repairs._actor, repairs._schema, repairs._sessions = actor, "test", sessions
    failure = runtime_failure(code, category="validation", public_message="Wrong failed call.")
    monkeypatch.setattr(
        "agent_runtime.storage.candidate_repair.verify_tool_recoveries",
        AsyncMock(side_effect=failure),
    )
    frame = DecisionFrame(
        actor=actor,
        context=context,
        attempt_id=uuid4(),
        action=Finish(
            kind="finish",
            payload=FinishPayload(outcome="success", evidence_refs=(), final_item_ids=()),
        ),
    )
    if code == "INVALID_EVIDENCE":
        with pytest.raises(CandidateRejected) as rejected:
            await repairs.validate(state, frame)
        assert "Wrong failed call." in rejected.value.detail
        assert "visible failedToolCallIds" in rejected.value.detail
    else:
        with pytest.raises(type(failure)) as raised:
            await repairs.validate(state, frame)
        assert raised.value is failure

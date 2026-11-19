"""S01 expected failures exercised against actual SDK boundaries, not new stub APIs."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from test_agent_loop import build
from test_decision_validation import facts

from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.contracts.context import ContextSourceRecord
from agent_runtime.contracts.decisions import Finish, FinishPayload, RecoveredToolFailure, Respond
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.loop_runner import AgentStepRunner
from agent_runtime.storage.answer_commit import SqlAnswerActionCommitter
from agent_runtime.terminal_guidance import terminal_guidance


@pytest.mark.asyncio
async def test_owned_run_missing_recovery_evidence_is_candidate_error_not_user_permission():
    state, context, _ = facts()
    actor = ActorContext(
        scope_id=state.scope_id, actor_id="owner", auth_revision="1", grants_ref="g"
    )
    failed, phantom = uuid4(), uuid4()
    source = ContextSourceRecord.model_validate(
        {
            "sourceId": "failed-result",
            "resource": {
                "resourceType": "tool_call_result",
                "resourceId": str(failed),
                "resourceVersion": "1",
                "scope": {"scopeId": state.scope_id},
            },
            "required": True,
            "disposition": "included",
            "reason": None,
            "summaryRef": None,
            "summaryStatus": None,
        }
    )
    frame = DecisionFrame(
        actor,
        context.model_copy(update={"sources": (source,)}),
        uuid4(),
        Finish(
            kind="finish",
            payload=FinishPayload(
                outcome="partial",
                evidence_refs=(),
                final_item_ids=(uuid4(),),
                recovered_tool_failures=(
                    RecoveredToolFailure(
                        failed_tool_call_id=failed,
                        recovery_tool_call_ids=(phantom,),
                        reason="The candidate claims a recovery that was never visible.",
                    ),
                ),
            ),
        ),
    )
    decisions = SimpleNamespace(
        recover=AsyncMock(
            return_value=SimpleNamespace(frame=frame, checkpoint_ref=state.checkpoint_ref)
        )
    )
    committer = SqlAnswerActionCommitter(
        actor=actor,
        lease=SimpleNamespace(
            scope_id=state.scope_id, session_id=state.session_id, run_id=state.run_id
        ),
        decisions=decisions,
        state_store=SimpleNamespace(),
        journal=SimpleNamespace(),
        publisher=SimpleNamespace(),
        resources=SimpleNamespace(),
        live=SimpleNamespace(require_running=AsyncMock()),
    )
    with pytest.raises(AgentRuntimeError) as caught:
        await committer.commit(state, frame)
    assert caught.value.failure.code == "INVALID_EVIDENCE"


@pytest.mark.asyncio
async def test_legacy_budget_rejection_preserves_hard_limit_without_dispatch_or_free_retry():
    loop, state, inputs, budget, _, _ = build("answer")
    response = await loop.decide(state, inputs, budget)
    assert isinstance(response.action, Respond)
    exhausted = budget.model_copy(
        update={
            "tool_calls": budget.tool_calls.model_copy(update={"spent": budget.tool_calls.limit}),
        }
    )

    class DecisionSource:
        attempts = 0

        async def decide(self, *args, **kwargs):
            self.attempts += 1
            if self.attempts == 1:
                raise runtime_failure(
                    "BUDGET_EXHAUSTED",
                    category="budget",
                    public_message="The proposed tool calls exceed the available budget.",
                )
            return response

    source = DecisionSource()
    recovery = SimpleNamespace(recover=AsyncMock(return_value=None))
    dispatch = SimpleNamespace(
        dispatch_result=AsyncMock(return_value=SimpleNamespace(state=state, outcomes=()))
    )
    committer = SimpleNamespace(
        commit=AsyncMock(
            return_value=state.model_copy(
                update={
                    "state_version": str(int(state.state_version) + 1),
                    "checkpoint_ref": "closed:1",
                    "status": RunStatus.COMPLETED,
                }
            )
        )
    )
    runner = AgentStepRunner(
        loop=source,
        dispatch=dispatch,
        recovery=recovery,
        budgets=SimpleNamespace(read=AsyncMock(return_value=exhausted)),
        committer=committer,
        max_decisions=2,
    )
    # No protected closing policy exists in this legacy assembly. Do not invent one
    # in a fixture or assert that the runtime should silently spend another call.
    with pytest.raises(AgentRuntimeError) as caught:
        await runner.run(state, inputs)
    assert caught.value.failure.code == "BUDGET_EXHAUSTED" and source.attempts == 1
    dispatch.dispatch_result.assert_not_awaited()
    committer.commit.assert_not_awaited()


def test_budget_guidance_does_not_claim_untyped_references_are_saved_work():
    failure = runtime_failure(
        "BUDGET_EXHAUSTED",
        category="budget",
        public_message="The proposed tool calls exceed the available budget.",
    ).failure
    message = terminal_guidance("failed", failure=failure, retained_count=1).message
    assert "The proposed tool calls" not in message
    assert "1 项已有成果" not in message

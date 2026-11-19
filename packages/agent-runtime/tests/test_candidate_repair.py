"""Candidate retry control; real SQL persistence is a separate integration gate."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from test_agent_loop import build

from agent_runtime.action_dispatch import DispatchResult
from agent_runtime.candidate_repair import CandidateRejected
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.provider import ContentDelta, ProviderFinish, ToolCallComplete
from agent_runtime.loop_runner import AgentStepRunner


@pytest.mark.asyncio
async def test_bad_native_name_drains_stream_before_a_candidate_rejection():
    loop, state, inputs, budget, _, answers = build("tool")
    completed = []

    class Backend:
        async def stream(self, request, **kwargs):
            yield ToolCallComplete(
                attempt_id=request.attempt_id,
                sequence="0",
                kind="tool.complete",
                call_id="bad",
                name="invented_alias",
                arguments={},
            )
            yield ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="finish",
                reason="tool_calls",
                raw_reason=None,
            )
            completed.append(True)

    loop._provider = Backend()
    loop._candidate_repair = True
    with pytest.raises(CandidateRejected) as error:
        await loop.decide(state, inputs, budget)
    assert completed == [True] and error.value.reason == "unknown_tool"
    assert not answers.saved


@pytest.mark.asyncio
async def test_complete_but_malformed_structured_candidate_is_repairable():
    loop, state, inputs, budget, backend, answers = build("bad_structured")
    loop._candidate_repair = True
    with pytest.raises(CandidateRejected) as error:
        await loop.decide(state, inputs, budget)
    assert error.value.reason == "invalid_decision"
    assert backend.opened == 1 and not answers.saved


@pytest.mark.asyncio
@pytest.mark.parametrize("quoted", [False, True])
async def test_textual_native_envelope_is_not_promoted_to_a_tool_or_success(quoted):
    loop, state, inputs, budget, _, answers = build("tool")
    raw = "<tool_call><function=not_a_native_call></function></tool_call>"
    text = f"```xml\n{raw}\n```" if quoted else raw

    class Backend:
        async def stream(self, request, **kwargs):
            yield ContentDelta(
                attempt_id=request.attempt_id,
                sequence="0",
                kind="content.delta",
                text=text,
            )
            yield ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="finish",
                reason="stop",
                raw_reason=None,
            )

    loop._provider, loop._candidate_repair = Backend(), True
    if quoted:
        frame = await loop.decide(state, inputs, budget)
        assert frame.action.kind == "respond" and answers.saved == [text]
    else:
        with pytest.raises(CandidateRejected) as error:
            await loop.decide(state, inputs, budget)
        assert error.value.reason == "invalid_decision" and not answers.saved


@pytest.mark.asyncio
async def test_incomplete_stream_is_not_turned_into_a_completed_candidate_repair():
    loop, state, inputs, budget, _, answers = build("truncated")
    loop._candidate_repair = True
    with pytest.raises(AgentRuntimeError) as error:
        await loop.decide(state, inputs, budget)
    assert not isinstance(error.value, CandidateRejected)
    assert error.value.failure.code == "INCOMPLETE_MODEL_OUTPUT"
    assert not answers.saved


@pytest.mark.asyncio
async def test_repair_uses_same_runner_and_preserves_its_new_checkpoint_for_the_next_attempt():
    _, state, inputs, budget, _, _ = build("answer")
    model, *_ = build("answer")
    frame = await model.decide(state, inputs, budget)
    rejected = CandidateRejected(
        attempt_id=uuid4(), snapshot_id=uuid4(), reason="unknown_tool", detail="bad"
    )
    seen = []

    class Loop:
        async def decide(self, current, current_inputs, current_budget, **kwargs):
            seen.append(
                (
                    current.checkpoint_ref,
                    current_inputs.checkpoint_ref,
                    kwargs.get("candidate_feedback"),
                )
            )
            if len(seen) == 1:
                raise rejected
            return frame

    repaired = state.model_copy(update={"checkpoint_ref": "corrected-checkpoint"})

    async def latest(current):
        return rejected if seen else None

    repairs = SimpleNamespace(
        latest=AsyncMock(side_effect=latest),
        record=AsyncMock(return_value=repaired),
        validate=AsyncMock(),
    )
    output = SimpleNamespace(finalize_rejection=AsyncMock())
    result = await AgentStepRunner(
        loop=Loop(),
        dispatch=SimpleNamespace(
            dispatch_result=AsyncMock(return_value=DispatchResult(repaired, ()))
        ),
        recovery=SimpleNamespace(recover=AsyncMock(return_value=None)),
        budgets=SimpleNamespace(read=AsyncMock(return_value=budget)),
        committer=SimpleNamespace(
            commit=AsyncMock(
                return_value=repaired.model_copy(
                    update={
                        "state_version": str(int(state.state_version) + 1),
                        "checkpoint_ref": "terminal",
                    }
                )
            )
        ),
        candidate_repairs=repairs,
        tool_output=output,
    ).run(state, inputs)
    assert result.state.checkpoint_ref == "terminal"
    assert seen[1][0:2] == ("corrected-checkpoint", "corrected-checkpoint")
    assert '"reason":"unknown_tool"' in seen[1][2]
    repairs.record.assert_awaited_once()


@pytest.mark.asyncio
async def test_real_authorization_failure_is_not_caught_as_a_candidate_repair():
    from agent_runtime.contracts.failure import runtime_failure

    _, state, inputs, budget, _, _ = build("answer")
    repairs = SimpleNamespace(latest=AsyncMock(return_value=None), record=AsyncMock())
    loop = SimpleNamespace(
        decide=AsyncMock(
            side_effect=runtime_failure(
                "FORBIDDEN", category="authorization", public_message="Current grant was revoked."
            )
        )
    )
    with pytest.raises(AgentRuntimeError):
        await AgentStepRunner(
            loop=loop,
            dispatch=None,
            recovery=SimpleNamespace(recover=AsyncMock(return_value=None)),
            budgets=SimpleNamespace(read=AsyncMock(return_value=budget)),
            committer=None,
            candidate_repairs=repairs,
        ).run(state, inputs)
    repairs.record.assert_not_awaited()

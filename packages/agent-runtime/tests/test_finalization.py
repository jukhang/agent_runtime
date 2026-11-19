from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError
from test_agent_loop import build
from test_decision_validation import facts
from test_provider_invoker import make_request

from agent_runtime.action_dispatch import DispatchResult
from agent_runtime.agent import Budget
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.loop import BudgetCounter
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.finalization import budget_guidance, closing_budget
from agent_runtime.loop_runner import AgentStepRunner


@pytest.mark.parametrize("remaining", [0, 1, 3, 4, 24])
def test_closing_view_releases_only_its_known_purpose_not_extra_budget(remaining):
    _, _, budget = facts()
    budget = budget.model_copy(
        update={
            "model_calls": BudgetCounter(
                limit="32", spent=str(31 - remaining), reserved="1", unknown_reserved="0"
            ),
            "tokens": BudgetCounter(
                limit="100000", spent="0", reserved="8192", unknown_reserved="0"
            ),
            "closing_tokens": "8192",
        }
    )
    closed = closing_budget(budget)
    assert closed.closing and closed.model_calls.available == remaining + 1
    assert closed.model_calls.limit == budget.model_calls.limit
    assert closed.model_calls.spent == budget.model_calls.spent
    assert closed.tokens.available == budget.tokens.available + 8192
    assert budget.tokens.reserved == "8192"


def test_unknown_or_missing_hold_cannot_be_used_to_open_a_closing_call():
    _, _, budget = facts()
    with pytest.raises(AgentRuntimeError):
        closing_budget(budget)
    budget = budget.model_copy(
        update={
            "closing_tokens": "8192",
            "model_calls": BudgetCounter(limit="32", spent="1", reserved="1", unknown_reserved="0"),
            "tokens": BudgetCounter(
                limit="100000", spent="0", reserved="8193", unknown_reserved="1"
            ),
        }
    )
    with pytest.raises(AgentRuntimeError) as error:
        closing_budget(budget)
    assert error.value.failure.code == "USAGE_UNRESOLVED"


def test_closing_cannot_carry_tools_and_old_agent_budget_hash_shape_is_preserved():
    request = make_request()
    data = request.model_dump()
    data.update(purpose="closing", tools=(request.model.ref,))
    with pytest.raises(ValidationError):
        ProviderRequest.model_validate(data)
    assert "finalizationTokens" not in Budget().model_dump(by_alias=True)
    assert Budget(finalization_tokens=8192).model_dump(by_alias=True)["finalizationTokens"] == 8192


@pytest.mark.asyncio
@pytest.mark.parametrize("remaining", [0, 1, 3, 4, 24])
@pytest.mark.parametrize("decisions", [1, 24])
async def test_runner_closes_only_at_tool_boundary_without_an_extra_normal_model_call(
    remaining, decisions
):
    model, state, inputs, budget, _, _ = build("answer")
    frame = await model.decide(state, inputs, budget)
    budget = budget.model_copy(
        update={
            "closing_tokens": "8192",
            "tokens": BudgetCounter(
                limit="100000", spent="0", reserved="8192", unknown_reserved="0"
            ),
            "model_calls": BudgetCounter(limit="32", spent="0", reserved="1", unknown_reserved="0"),
            "tool_calls": BudgetCounter(
                limit="24", spent=str(24 - remaining), reserved="0", unknown_reserved="0"
            ),
        }
    )
    loop = SimpleNamespace(decide=AsyncMock(return_value=frame))
    await AgentStepRunner(
        loop=loop,
        dispatch=SimpleNamespace(dispatch_result=AsyncMock(return_value=DispatchResult(state, ()))),
        recovery=SimpleNamespace(recover=AsyncMock(return_value=None)),
        budgets=SimpleNamespace(read=AsyncMock(return_value=budget)),
        committer=SimpleNamespace(
            commit=AsyncMock(
                return_value=state.model_copy(
                    update={
                        "state_version": str(int(state.state_version) + 1),
                        "checkpoint_ref": "terminal",
                    }
                )
            )
        ),
        max_decisions=decisions,
    ).run(state, inputs)
    assert loop.decide.await_count == 1
    used = loop.decide.await_args.args[2]
    assert used.closing is (remaining == 0 or decisions == 1)
    assert used.tool_calls.available == remaining
    guidance = budget_guidance(budget)
    assert bool(guidance) is (remaining < 4)
    if guidance:
        assert f"{remaining} tool calls remain" in guidance

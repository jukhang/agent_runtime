"""Only trusted setup can isolate an independent model stage from prior tool output."""

from dataclasses import replace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from test_agent_loop import build

from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.references import ResourceRef, ResourceScope


@pytest.mark.asyncio
@pytest.mark.parametrize("policy", ("include", "isolate"))
async def test_context_observation_policy_preserves_explicit_sources_and_default(policy):
    loop, state, inputs, budget, _, _ = build("answer")
    setup = await loop._setup.resolve(state, inputs, budget)
    assert setup.observation_policy == "include"
    prepared = object()
    loop._context.prepare = AsyncMock(return_value=prepared)
    loop._access.verify = AsyncMock()
    observation = ResourceRef(
        resource_type="tool_call_result",
        resource_id=str(uuid4()),
        resource_version="1",
        scope=ResourceScope(scope_id=state.scope_id),
    )
    await loop._prepare_context(
        state, budget, replace(setup, observation_policy=policy), (observation,)
    )
    actual = loop._context.prepare.call_args.args[0].resource_refs
    assert actual == setup.context_request.resource_refs + (
        (observation,) if policy == "include" else ()
    )


@pytest.mark.asyncio
async def test_isolation_does_not_waive_cross_scope_observation_invariant():
    loop, state, inputs, budget, _, _ = build("answer")
    setup = replace(await loop._setup.resolve(state, inputs, budget), observation_policy="isolate")
    with pytest.raises(AgentRuntimeError) as failure:
        await loop._prepare_context(
            state,
            budget,
            setup,
            (
                ResourceRef(
                    resource_type="tool_call_result",
                    resource_id=str(uuid4()),
                    resource_version="1",
                    scope=ResourceScope(scope_id="another-scope"),
                ),
            ),
        )
    assert failure.value.failure.code == "FORBIDDEN"


def test_unknown_observation_policy_is_a_configuration_error():
    from agent_runtime.agent_loop import DecisionSetup

    with pytest.raises(ValueError, match="observation policy"):
        DecisionSetup(None, None, observation_policy="ignore")


@pytest.mark.asyncio
async def test_context_refresh_cannot_switch_a_trusted_observation_policy():
    from types import SimpleNamespace

    loop, state, inputs, budget, _, _ = build("answer")
    initial = await loop._setup.resolve(state, inputs, budget)
    fresh = replace(initial, observation_policy="isolate")
    loop._setup = SimpleNamespace(resolve=AsyncMock(return_value=fresh))
    loop._context.prepare = AsyncMock(
        side_effect=runtime_failure(
            "VERSION_CONFLICT",
            category="context",
            public_message="Refresh required",
        )
    )
    with pytest.raises(AgentRuntimeError) as failure:
        await loop._prepare_setup(state, inputs, budget, initial, ())
    assert failure.value.failure.code == "VERSION_CONFLICT"
    assert loop._context.prepare.await_count == 1

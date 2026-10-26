"""The main Run has one explicit non-tool SQL action owner."""

from dataclasses import replace

import pytest
from test_agent_loop import build

from agent_runtime.contracts.decisions import (
    AskUser,
    Delegate,
    Finish,
    Respond,
    UpdatePlan,
    Wait,
)
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.storage.action_router import SqlActionCommitRouter


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("action_type", "owner"),
    [
        (Respond, "answers"),
        (Finish, "answers"),
        (AskUser, "waits"),
        (Wait, "waits"),
        (UpdatePlan, "plans"),
        (Delegate, "delegates"),
    ],
)
async def test_action_routes_to_exactly_one_committer(action_type, owner):
    loop, state, inputs, budget, _, _ = build("answer")
    frame = await loop.decide(state, inputs, budget)
    frame = replace(frame, action=action_type.model_construct(kind=action_type.__name__.lower()))
    calls = []

    class Committer:
        def __init__(self, name):
            self.name = name

        async def commit(self, current, candidate):
            assert current == state
            assert candidate == frame
            calls.append(self.name)
            return current.model_copy(update={"state_version": "2"})

    router = SqlActionCommitRouter(
        answers=Committer("answers"),
        waits=Committer("waits"),
        plans=Committer("plans"),
        delegates=Committer("delegates"),
    )
    committed = await router.commit(state, frame)
    assert committed.state_version == "2"
    assert calls == [owner]


@pytest.mark.asyncio
async def test_action_router_refuses_tool_dispatch():
    loop, state, inputs, budget, _, _ = build("tool")
    frame = await loop.decide(state, inputs, budget)

    class NoCommitter:
        async def commit(self, *_):
            raise AssertionError("tool decisions never use action committers")

    router = SqlActionCommitRouter(
        answers=NoCommitter(), waits=NoCommitter(),
        plans=NoCommitter(), delegates=NoCommitter(),
    )
    with pytest.raises(AgentRuntimeError) as error:
        await router.commit(state, frame)
    assert error.value.failure.code == "INVALID_INPUT"

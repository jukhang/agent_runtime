"""Route a staged non-tool decision to its single SQL transition owner."""

from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.contracts.decisions import (
    AskUser,
    Delegate,
    Finish,
    InvokeTools,
    Respond,
    UpdatePlan,
    Wait,
)
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.loop import RunState
from agent_runtime.loop_runner import ActionCommitter


class SqlActionCommitRouter:
    def __init__(
        self,
        *,
        answers: ActionCommitter,
        waits: ActionCommitter,
        plans: ActionCommitter,
        delegates: ActionCommitter,
    ) -> None:
        self._answers = answers
        self._waits = waits
        self._plans = plans
        self._delegates = delegates

    async def commit(self, state: RunState, frame: DecisionFrame) -> RunState:
        action = frame.action
        if isinstance(action, (Respond, Finish)):
            return await self._answers.commit(state, frame)
        if isinstance(action, (AskUser, Wait)):
            return await self._waits.commit(state, frame)
        if isinstance(action, UpdatePlan):
            return await self._plans.commit(state, frame)
        if isinstance(action, Delegate):
            return await self._delegates.commit(state, frame)
        if isinstance(action, InvokeTools):
            raise runtime_failure(
                "INVALID_INPUT",
                category="validation",
                public_message="Tool decisions must be committed by the tool dispatcher.",
            )
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="This decision has no durable transition handler.",
        )


__all__ = ["SqlActionCommitRouter"]

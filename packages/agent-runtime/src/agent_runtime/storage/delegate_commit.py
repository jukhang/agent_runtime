"""Bind a staged Delegate decision to the trusted subagent transaction."""

from collections.abc import Callable

from agent_runtime.action_dispatch import LiveRunGuard
from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.contracts.decisions import Delegate
from agent_runtime.contracts.loop import RunState
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.subagents import SpawnRequest
from agent_runtime.contracts.transactions import ActorContext, CommitGuard, LeaseToken
from agent_runtime.storage.decisions import SqlDecisionStore
from agent_runtime.storage.subagents import SqlSubagentRuntime
from agent_runtime.storage.uow import errors


class SqlDelegateActionCommitter:
    def __init__(
        self,
        *,
        actor: ActorContext,
        lease: LeaseToken | Callable[[], LeaseToken],
        decisions: SqlDecisionStore,
        subagents: SqlSubagentRuntime,
        live: LiveRunGuard,
    ) -> None:
        self._actor = actor
        self._lease = lease if callable(lease) else lambda: lease
        self._decisions, self._subagents, self._live = decisions, subagents, live

    async def commit(self, state: RunState, frame: DecisionFrame) -> RunState:
        lease = self._lease()
        if not isinstance(frame.action, Delegate):
            errors.invalid_input("delegate committer requires Delegate")
        if frame.actor != self._actor or (state.scope_id, state.session_id, state.run_id) != (
            lease.scope_id,
            lease.session_id,
            lease.run_id,
        ):
            errors.forbidden("delegate decision has another owner")
        recovered = await self._decisions.recover()
        if (
            recovered is None
            or recovered.frame != frame
            or recovered.checkpoint_ref != state.checkpoint_ref
        ):
            errors.version_conflict("delegate differs from staged decision")
        await self._live.require_running(state, frame)
        cp = await self._decisions.prepare_next_checkpoint(state, frame)
        await self._subagents.spawn(
            SpawnRequest(
                parent_guard=CommitGuard(
                    actor=self._actor,
                    lease=self._lease(),
                    expected_state_version=state.state_version,
                ),
                task=frame.action.payload.task_package,
                idempotency_key=f"decision:{frame.attempt_id}",
            ),
            checkpoint_id=cp.checkpoint_id,
        )
        return state.model_copy(
            update={
                "state_version": str(int(state.state_version) + 1),
                "checkpoint_ref": cp.state_ref,
                "status": RunStatus.WAITING_USER,
            }
        )


__all__ = ["SqlDelegateActionCommitter"]

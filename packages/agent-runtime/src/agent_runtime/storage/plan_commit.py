"""Commit staged plan decisions through the fenced SQL state boundary."""

from collections.abc import Callable
from uuid import NAMESPACE_URL, uuid5

from agent_runtime.action_dispatch import LiveRunGuard
from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.contracts.decisions import UpdatePlan
from agent_runtime.contracts.loop import RunState
from agent_runtime.contracts.transactions import ActorContext, CommitGuard, LeaseToken
from agent_runtime.storage.context_access import TransactionalResourceAccess
from agent_runtime.storage.decisions import SqlDecisionStore
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.state import SqlStateUnitOfWork


class SqlPlanActionCommitter:
    def __init__(
        self,
        *,
        actor: ActorContext,
        lease: LeaseToken | Callable[[], LeaseToken],
        decisions: SqlDecisionStore,
        state_store: SqlStateUnitOfWork,
        journal: SqlJournal,
        resources: TransactionalResourceAccess,
        live: LiveRunGuard,
    ) -> None:
        self._actor = actor
        self._lease = lease if callable(lease) else lambda: lease
        self._decisions = decisions
        self._state_store = state_store
        self._journal = journal
        self._resources = resources
        self._live = live

    async def commit(self, state: RunState, frame: DecisionFrame) -> RunState:
        if not isinstance(frame.action, UpdatePlan):
            errors.invalid_input("plan committer requires UpdatePlan")
        lease = self._lease()
        if frame.actor != self._actor or (state.scope_id, state.session_id, state.run_id) != (
            lease.scope_id,
            lease.session_id,
            lease.run_id,
        ):
            errors.forbidden("plan action has another owner")
        recovered = await self._decisions.recover()
        if (
            recovered is None
            or recovered.frame != frame
            or recovered.checkpoint_ref != state.checkpoint_ref
        ):
            errors.version_conflict("plan action differs from the staged decision")
        await self._live.require_running(state, frame)
        plan = frame.action.payload
        revision = int(plan.expected_revision) + 1
        revision_id = uuid5(
            NAMESPACE_URL, f"plan:{state.scope_id}:{plan.plan_id}:revision:{revision}"
        )
        checkpoint = await self._decisions.prepare_next_checkpoint(
            state,
            frame,
            result={
                "planId": str(plan.plan_id),
                "revision": str(revision),
                "revisionId": str(revision_id),
            },
        )
        updated = await self._state_store.commit_plan(
            guard=CommitGuard(
                actor=self._actor, lease=self._lease(), expected_state_version=state.state_version
            ),
            checkpoint_id=checkpoint.checkpoint_id,
            plan=plan,
            revision_id=revision_id,
            journal=self._journal,
            resources=self._resources,
        )
        return state.model_copy(
            update={
                "state_version": updated.state_version,
                "checkpoint_ref": checkpoint.state_ref,
                "status": updated.status,
            }
        )


__all__ = ["SqlPlanActionCommitter"]

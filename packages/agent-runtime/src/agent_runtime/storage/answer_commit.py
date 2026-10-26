"""Durable answer publication and completion, separate from model decisions."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from agent_runtime.action_dispatch import LiveRunGuard
from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.contracts.decisions import Finish, Respond
from agent_runtime.contracts.loop import RunState
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.transactions import ActorContext, CommitGuard, LeaseToken
from agent_runtime.storage.context_access import TransactionalResourceAccess
from agent_runtime.storage.decisions import SqlDecisionStore
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.uow import SqlStateUnitOfWork, errors


@dataclass(frozen=True, slots=True)
class AnswerPublication:
    item_ids: tuple[UUID, ...]


class AnswerPublisher(Protocol):
    async def publish(self, state: RunState, frame: DecisionFrame) -> AnswerPublication:
        """Reuse by Run/decision Attempt; return only after Journal + seal are durable.

        The host owns model invocation, context preparation and answer request lookup.
        Worker takeover must reuse completed output, never blindly restart a stream.
        """
        ...


class SqlAnswerActionCommitter:
    """Respond queues continuation; Finish commits a verified successful/partial terminal."""

    def __init__(
        self,
        *,
        actor: ActorContext,
        lease: LeaseToken | Callable[[], LeaseToken],
        decisions: SqlDecisionStore,
        state_store: SqlStateUnitOfWork,
        journal: SqlJournal,
        publisher: AnswerPublisher,
        resources: TransactionalResourceAccess,
        live: LiveRunGuard,
    ) -> None:
        self._actor = actor
        self._lease = lease if callable(lease) else lambda: lease
        self._decisions = decisions
        self._state = state_store
        self._journal = journal
        self._publisher = publisher
        self._resources = resources
        self._live = live

    async def commit(self, state: RunState, frame: DecisionFrame) -> RunState:
        action = frame.action
        if not isinstance(action, (Respond, Finish)):
            errors.invalid_input("answer committer only accepts Respond or Finish")
        lease = self._lease()
        if frame.actor != self._actor or (state.scope_id, state.session_id, state.run_id) != (
            lease.scope_id,
            lease.session_id,
            lease.run_id,
        ):
            errors.forbidden("answer action belongs to another worker")
        # Bind to the staged decision before invoking a potentially expensive publisher.
        recovered = await self._decisions.recover()
        if (
            recovered is None
            or recovered.frame != frame
            or recovered.checkpoint_ref != state.checkpoint_ref
        ):
            errors.version_conflict("answer differs from its staged decision")
        await self._live.require_running(state, frame)
        refs: tuple[ResourceRef, ...]
        if isinstance(action, Respond):
            publication = await self._publisher.publish(state, frame)
            ids = publication.item_ids
            checkpoint = await self._decisions.prepare_next_checkpoint(
                state, frame, result={"answerItemIds": [str(item) for item in ids]}
            )
            outcome = None
            refs = ()
        else:
            ids = action.payload.final_item_ids
            outcome = action.payload.outcome
            refs = action.payload.evidence_refs
            checkpoint = await self._decisions.prepare_next_checkpoint(state, frame)
        updated = await self._state.commit_answer(
            guard=CommitGuard(
                actor=self._actor, lease=self._lease(), expected_state_version=state.state_version
            ),
            checkpoint_id=checkpoint.checkpoint_id,
            item_ids=ids,
            outcome=outcome,
            evidence_refs=refs,
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


__all__ = ["AnswerPublication", "AnswerPublisher", "SqlAnswerActionCommitter"]

"""Commit a staged wait through the existing fenced Scheduler transition."""

from __future__ import annotations

from collections.abc import Callable, Collection
from datetime import datetime
from uuid import UUID

from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.contracts.decisions import AskUser, Wait
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.loop import RunState
from agent_runtime.contracts.payloads import InputRequired
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.scheduler import InputWatermark, YieldRequest
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.tasks import WaitSpec
from agent_runtime.contracts.transactions import ActorContext, CommitGuard, LeaseToken
from agent_runtime.storage.decisions import SqlDecisionStore
from agent_runtime.storage.scheduler import SqlScheduler


class SqlWaitActionCommitter:
    """Staged Wait/AskUser -> next checkpoint -> waiting/queued Run.

    Checkpoint preparation may commit before the yield transaction; an orphan
    checkpoint is inert because only run.checkpoint_id selects executable state.
    """

    def __init__(
        self,
        *,
        actor: ActorContext,
        lease: LeaseToken | Callable[[], LeaseToken],
        decisions: SqlDecisionStore,
        scheduler: SqlScheduler,
        allowed_predicates: Collection[CapabilityRef],
        message_predicate: CapabilityRef | None = None,
        message_deadline_at: datetime | None = None,
    ) -> None:
        if not scheduler.emits_state_events:
            raise ValueError("wait transitions require atomic Journal/Outbox publication")
        self._actor = actor
        self._lease = lease if callable(lease) else lambda: lease
        self._decisions = decisions
        self._scheduler = scheduler
        self._predicates = frozenset(allowed_predicates)
        self._message_predicate = message_predicate
        self._message_deadline_at = message_deadline_at

    async def commit(self, state: RunState, frame: DecisionFrame) -> RunState:
        input_request: InputRequired | None = None
        if isinstance(frame.action, AskUser):
            predicate = self._message_predicate
            deadline = self._message_deadline_at
            if predicate is None or deadline is None or predicate not in self._predicates:
                raise runtime_failure(
                    "FORBIDDEN",
                    category="authorization",
                    public_message="The user-input wait is not configured for this run.",
                )
            spec = WaitSpec(
                kind="message",
                subject_id=str(state.run_id),
                deadline_at=deadline,
                resume_predicate=predicate,
            )
            input_request = InputRequired(
                state_version=str(int(state.state_version) + 1),
                question=frame.action.payload.question,
                required_facts=frame.action.payload.required_facts,
                reply_to_run_id=state.run_id,
            )
        elif isinstance(frame.action, Wait):
            spec = frame.action.payload.wait_spec
        else:
            raise runtime_failure(
                "INVALID_INPUT",
                category="validation",
                public_message="This action requires its own durable transition handler.",
            )
        if spec.resume_predicate not in self._predicates:
            raise runtime_failure(
                "FORBIDDEN",
                category="authorization",
                public_message="The wait condition is not a registered runtime predicate.",
            )
        if spec.kind == "message" and spec.subject_id != str(state.run_id):
            raise runtime_failure(
                "INVALID_INPUT",
                category="validation",
                public_message="The message wait must target its current run.",
            )
        if spec.kind in {"tool", "approval"}:
            try:
                UUID(spec.subject_id)
            except ValueError as exc:
                raise runtime_failure(
                    "INVALID_INPUT",
                    category="validation",
                    public_message="The wait target must be a tool call or approval ID.",
                ) from exc
        lease = self._lease()
        if frame.actor != self._actor or (state.scope_id, state.session_id, state.run_id) != (
            lease.scope_id,
            lease.session_id,
            lease.run_id,
        ):
            raise runtime_failure(
                "FORBIDDEN",
                category="authorization",
                public_message="The wait action has another owner.",
            )
        checkpoint = await self._decisions.prepare_next_checkpoint(state, frame)
        result = await self._scheduler.yield_run(
            YieldRequest(
                guard=CommitGuard(
                    actor=self._actor,
                    lease=self._lease(),
                    expected_state_version=state.state_version,
                ),
                checkpoint_id=checkpoint.checkpoint_id,
                wait_spec=spec,
                inputs=InputWatermark(
                    pending_version=state.pending_input_seq,
                    applied_version=state.applied_input_seq,
                ),
                input_request=input_request,
            )
        )
        return state.model_copy(
            update={
                "state_version": result.state_version,
                "checkpoint_ref": checkpoint.state_ref,
                "status": (
                    RunStatus(result.status)
                    if result.disposition == "waiting"
                    else RunStatus.QUEUED
                ),
            }
        )


__all__ = ["SqlWaitActionCommitter"]

"""Recover a staged decision before asking the model to decide again."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from agent_runtime.action_dispatch import RunActionDispatcher
from agent_runtime.agent_loop import DecisionFrame, ModelAgentLoop
from agent_runtime.candidate_repair import CandidateRejected, CandidateRepairs
from agent_runtime.contracts.canonical import canonical_json
from agent_runtime.contracts.decisions import InvokeTools, NextAction
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.loop import AppliedInputs, BudgetView, RunState
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.tools import (
    PendingExecution,
    ToolCancelled,
    ToolFailed,
    ToolNotExecuted,
    ToolOutcome,
    ToolWaitingApproval,
)
from agent_runtime.finalization import decision_budget


@dataclass(frozen=True, slots=True)
class RecoveredDecision:
    frame: DecisionFrame
    checkpoint_ref: str
    outcomes: tuple[ToolOutcome, ...]
    unresolved_call_ids: tuple[UUID, ...]
    observation_refs: tuple[ResourceRef, ...]

    def require_next_model_sources(self) -> tuple[ResourceRef, ...]:
        """Never replay an unresolved call automatically after worker takeover."""
        if self.unresolved_call_ids:
            raise runtime_failure(
                "SIDE_EFFECT_UNKNOWN",
                category="tool",
                public_message="A previous tool call needs reconciliation before continuing.",
            )
        if any(
            isinstance(outcome, (ToolFailed, ToolCancelled)) and outcome.effect == "unknown"
            for outcome in self.outcomes
        ):
            raise runtime_failure(
                "SIDE_EFFECT_UNKNOWN",
                category="tool",
                public_message="A previous tool effect is unknown and needs reconciliation.",
            )
        if any(isinstance(outcome, PendingExecution) for outcome in self.outcomes):
            raise runtime_failure(
                "CONTROL_PENDING",
                category="tool",
                public_message="A tool execution is still pending.",
            )
        if any(isinstance(outcome, ToolWaitingApproval) for outcome in self.outcomes):
            raise runtime_failure(
                "CONTROL_PENDING",
                category="authorization",
                public_message="The tool call is waiting for approval.",
            )
        return self.observation_refs


class DecisionRecovery(Protocol):
    async def recover(self) -> RecoveredDecision | None: ...


class BudgetReader(Protocol):
    async def read(self, state: RunState) -> BudgetView:
        """Return current authoritative budget before each new model call."""
        ...


class ActionCommitter(Protocol):
    async def commit(self, state: RunState, frame: DecisionFrame) -> RunState:
        """Atomically commit a non-tool action and its Run/WorkItem transition."""
        ...


class ToolOutputFinalizer(Protocol):
    async def finalize_tool(self, frame: DecisionFrame) -> None:
        """Idempotently seal streamed output before another model decision."""
        ...

    async def finalize_rejection(self, rejection: CandidateRejected) -> None: ...


class ExecutionWaiter(Protocol):
    async def park(self, state: RunState, decision: RecoveredDecision) -> RunState:
        """Commit a durable wait and relinquish the worker lease."""
        ...


class ApprovalResumer(Protocol):
    async def resume(
        self, state: RunState, budget: BudgetView, decision: RecoveredDecision
    ) -> RecoveredDecision:
        """Resolve a woken approval against its original tool call and operation."""
        ...


@dataclass(frozen=True, slots=True)
class RunnerResult:
    state: RunState
    action: NextAction
    outcomes: tuple[ToolOutcome, ...]


class AgentStepRunner:
    """Bounded decide/execute/observe loop for one claimed WorkItem.

    State transition commits for non-tool actions are mandatory. This runner never
    treats a pending call as an idempotent read.
    """

    def __init__(
        self,
        *,
        loop: ModelAgentLoop,
        dispatch: RunActionDispatcher,
        recovery: DecisionRecovery,
        budgets: BudgetReader,
        committer: ActionCommitter,
        tool_output: ToolOutputFinalizer | None = None,
        execution_waiter: ExecutionWaiter | None = None,
        approval_resumer: ApprovalResumer | None = None,
        max_decisions: int = 24,
        candidate_repairs: CandidateRepairs | None = None,
    ) -> None:
        if max_decisions < 1:
            raise ValueError("max_decisions must be positive")
        self._loop = loop
        self._dispatch = dispatch
        self._recovery = recovery
        self._budgets = budgets
        self._committer = committer
        self._tool_output = tool_output
        self._execution_waiter = execution_waiter
        self._approval_resumer = approval_resumer
        self._max_decisions = max_decisions
        self._candidate_repairs = candidate_repairs

    async def _commit_action(self, state: RunState, frame: DecisionFrame) -> RunState:
        committed = await self._committer.commit(state, frame)
        if (
            (committed.scope_id, committed.session_id, committed.run_id)
            != (state.scope_id, state.session_id, state.run_id)
            or int(committed.state_version) != int(state.state_version) + 1
            or committed.checkpoint_ref == state.checkpoint_ref
        ):
            raise runtime_failure(
                "AUDIT_UNAVAILABLE",
                category="storage",
                public_message="Action state was not durably advanced.",
            )
        return committed

    async def _record_rejections(self, state: RunState, observed: RecoveredDecision) -> RunState:
        if self._candidate_repairs is None:
            return state
        for outcome in observed.outcomes:
            if not isinstance(outcome, (ToolFailed, ToolNotExecuted)):
                continue
            if outcome.failure.category in {"authorization", "storage"}:
                raise AgentRuntimeError(outcome.failure)
            if (
                outcome.effect in {"none", "rolled_back"}
                and outcome.failure.category == "validation"
            ):
                rejected = CandidateRejected(
                    attempt_id=observed.frame.attempt_id,
                    snapshot_id=observed.frame.context.snapshot_id,
                    reason="tool_rejected",
                    tool_call_id=outcome.tool_call_id,
                    detail=canonical_json(
                        {"code": outcome.failure.code, "message": outcome.failure.public_message}
                    ),
                )
                state = await self._candidate_repairs.record(state, rejected)
        return state

    async def run(self, state: RunState, inputs: AppliedInputs) -> RunnerResult:
        refs: tuple[ResourceRef, ...] = ()
        recovered = await self._recovery.recover()
        if recovered is not None:
            state = state.model_copy(update={"checkpoint_ref": recovered.checkpoint_ref})
            if not isinstance(recovered.frame.action, InvokeTools):
                committed = await self._commit_action(state, recovered.frame)
                return RunnerResult(committed, recovered.frame.action, recovered.outcomes)
            if any(isinstance(item, ToolWaitingApproval) for item in recovered.outcomes):
                if self._approval_resumer is not None:
                    recovered = await self._approval_resumer.resume(
                        state, await self._budgets.read(state), recovered
                    )
            parked = await self._park_execution(state, recovered)
            if parked is not None:
                return parked
            refs = recovered.require_next_model_sources()
            if self._tool_output is not None:
                await self._tool_output.finalize_tool(recovered.frame)
            state = await self._record_rejections(state, recovered)
            inputs = inputs.model_copy(update={"checkpoint_ref": state.checkpoint_ref})

        for decision_index in range(self._max_decisions):
            budget = await self._budgets.read(state)
            budget = decision_budget(
                budget, last_decision=decision_index + 1 == self._max_decisions
            )
            feedback = None
            if self._candidate_repairs is not None:
                previous = await self._candidate_repairs.latest(state)
                if previous is not None:
                    if self._tool_output is not None:
                        await self._tool_output.finalize_rejection(previous)
                    feedback = canonical_json(
                        {"reason": previous.reason, "detail": previous.detail}
                    )
            try:
                if self._candidate_repairs is None:
                    frame = await self._loop.decide(state, inputs, budget, observation_refs=refs)
                else:
                    frame = await self._loop.decide(
                        state, inputs, budget, observation_refs=refs, candidate_feedback=feedback
                    )
                    await self._candidate_repairs.validate(state, frame)
            except CandidateRejected as rejection:
                if self._candidate_repairs is None:
                    raise
                try:
                    state = await self._candidate_repairs.record(state, rejection)
                finally:
                    if self._tool_output is not None:
                        await self._tool_output.finalize_rejection(rejection)
                inputs = inputs.model_copy(update={"checkpoint_ref": state.checkpoint_ref})
                continue
            result = await self._dispatch.dispatch_result(state, budget, frame)
            state = result.state
            if not isinstance(frame.action, InvokeTools):
                committed = await self._commit_action(state, frame)
                return RunnerResult(committed, frame.action, result.outcomes)
            observed = await self._recovery.recover()
            if observed is None or observed.frame.attempt_id != frame.attempt_id:
                raise runtime_failure(
                    "AUDIT_UNAVAILABLE",
                    category="storage",
                    public_message="Tool observations were not committed for this decision.",
                )
            parked = await self._park_execution(state, observed)
            if parked is not None:
                return parked
            refs = observed.require_next_model_sources()
            if self._tool_output is not None:
                await self._tool_output.finalize_tool(frame)
            state = await self._record_rejections(state, observed)
            inputs = inputs.model_copy(update={"checkpoint_ref": state.checkpoint_ref})
        raise runtime_failure(
            "BUDGET_EXHAUSTED",
            category="budget",
            public_message="The run reached its decision limit before completing.",
        )

    async def _park_execution(
        self, state: RunState, observed: RecoveredDecision
    ) -> RunnerResult | None:
        waits = [
            item
            for item in observed.outcomes
            if isinstance(item, (PendingExecution, ToolWaitingApproval))
        ]
        if not waits:
            return None
        if self._execution_waiter is None:
            observed.require_next_model_sources()
            return None
        if (
            len(waits) != 1
            or observed.unresolved_call_ids
            or any(
                isinstance(item, (ToolFailed, ToolCancelled)) and item.effect == "unknown"
                for item in observed.outcomes
            )
        ):
            observed.require_next_model_sources()
        if self._tool_output is not None:
            await self._tool_output.finalize_tool(observed.frame)
        parked = await self._execution_waiter.park(state, observed)
        if (
            (parked.scope_id, parked.session_id, parked.run_id)
            != (state.scope_id, state.session_id, state.run_id)
            or parked.checkpoint_ref == state.checkpoint_ref
            or parked.status.value
            != ("waiting_user" if isinstance(waits[0], ToolWaitingApproval) else "waiting_tool")
            or int(parked.state_version) != int(state.state_version) + 1
        ):
            raise runtime_failure(
                "AUDIT_UNAVAILABLE",
                category="storage",
                public_message="Execution wait was not committed.",
            )
        return RunnerResult(parked, observed.frame.action, observed.outcomes)


__all__ = [
    "AgentStepRunner",
    "ActionCommitter",
    "ApprovalResumer",
    "BudgetReader",
    "DecisionRecovery",
    "RecoveredDecision",
    "RunnerResult",
    "ToolOutputFinalizer",
    "ExecutionWaiter",
]

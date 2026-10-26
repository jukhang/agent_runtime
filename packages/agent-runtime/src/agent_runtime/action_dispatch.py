"""Trusted dispatch of a persisted model action to policy and tool ports."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.contracts.decisions import InvokeTools, ToolCallRequest
from agent_runtime.contracts.failure import FailureEnvelope, runtime_failure
from agent_runtime.contracts.loop import BudgetView, RunState
from agent_runtime.contracts.policy import (
    PolicyAllow,
    PolicyAsk,
    PolicyDecision,
    PolicyDeny,
    PolicyRequest,
    PolicyReview,
)
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.tools import (
    PendingExecution,
    ToolCancelled,
    ToolFailed,
    ToolInvocation,
    ToolNotExecuted,
    ToolOutcome,
    ToolSpec,
    ToolWaitingApproval,
)
from agent_runtime.decision_validation import validate_candidate_decision
from agent_runtime.policy_validation import validate_policy_decision
from agent_runtime.ports import Policy
from agent_runtime.tool_validation import validate_tool_invocation, validate_tool_outcome

type Clock = Callable[[], datetime]


class DecisionRecorder(Protocol):
    async def persist(self, state: RunState, frame: DecisionFrame) -> RunState:
        """CAS-check state/lease and durably record decision before any tool effect."""
        ...


class LiveRunGuard(Protocol):
    async def require_running(self, state: RunState, frame: DecisionFrame) -> None:
        """Recheck lease, control, actor revision, state version and context access."""
        ...


class ToolSpecResolver(Protocol):
    async def get(self, call: ToolCallRequest) -> ToolSpec:
        """Load exact pinned tool revision from a trusted registry."""
        ...


class ToolResourceResolver(Protocol):
    async def resources(
        self, invocation: ToolInvocation, spec: ToolSpec
    ) -> tuple[ResourceRef, ...]:
        """Resolve versioned targets; never trust arbitrary model-provided references."""
        ...


class AuthorizedToolExecutor(Protocol):
    async def execute(self, invocation: ToolInvocation, grant: PolicyAllow) -> ToolOutcome:
        """Revalidate grant and target versions atomically with execution/idempotency."""
        ...


class ToolObservationStore(Protocol):
    async def record(self, frame: DecisionFrame, outcome: ToolOutcome) -> None:
        """Persist observation before any later model decision or tool call."""
        ...


@dataclass(frozen=True, slots=True)
class DispatchResult:
    state: RunState
    outcomes: tuple[ToolOutcome, ...]


class RunActionDispatcher:
    """Enforces dispatch order while leaving durable UoW ownership to host ports."""

    def __init__(
        self,
        *,
        decisions: DecisionRecorder,
        live: LiveRunGuard,
        specs: ToolSpecResolver,
        resources: ToolResourceResolver,
        policy: Policy,
        tools: AuthorizedToolExecutor,
        observations: ToolObservationStore,
        clock: Clock = lambda: datetime.now(UTC),
    ) -> None:
        self._decisions = decisions
        self._live = live
        self._specs = specs
        self._resources = resources
        self._policy = policy
        self._tools = tools
        self._observations = observations
        self._clock = clock

    async def dispatch(
        self, state: RunState, budget: BudgetView, frame: DecisionFrame
    ) -> tuple[ToolOutcome, ...]:
        return (await self.dispatch_result(state, budget, frame)).outcomes

    async def dispatch_result(
        self, state: RunState, budget: BudgetView, frame: DecisionFrame
    ) -> DispatchResult:
        action = validate_candidate_decision(
            state, frame.context, budget, frame.action, now=self._clock()
        )
        if (
            frame.actor.scope_id != state.scope_id
            or frame.context.auth_revision != frame.actor.auth_revision
        ):
            raise runtime_failure(
                "FORBIDDEN", category="authorization", public_message="Decision actor changed."
            )
        await self._live.require_running(state, frame)
        active_state = await self._decisions.persist(state, frame)
        if (
            active_state.scope_id != state.scope_id
            or active_state.session_id != state.session_id
            or active_state.run_id != state.run_id
        ):
            raise runtime_failure(
                "FENCED",
                category="concurrency",
                public_message="Decision commit changed ownership.",
            )
        if not isinstance(action, InvokeTools):
            return DispatchResult(active_state, ())
        if active_state.status != RunStatus.RUNNING or active_state.desired_control is not None:
            raise runtime_failure(
                "FENCED",
                category="concurrency",
                public_message="The committed run is not active for tool dispatch.",
            )
        outcomes: list[ToolOutcome] = []
        for index, call in enumerate(action.payload.validated_calls):
            await self._live.require_running(active_state, frame)
            spec = await self._specs.get(call)
            now = self._clock()
            invocation = ToolInvocation(
                actor=frame.actor,
                session_id=state.session_id,
                run_id=state.run_id,
                model_attempt_id=frame.attempt_id,
                context_snapshot_id=frame.context.snapshot_id,
                context_version=frame.context.context_version,
                call=call,
                operation_id=(
                    uuid5(NAMESPACE_URL, f"operation:{state.run_id}:{call.tool_call_id}")
                    if spec.side_effect != "none"
                    else None
                ),
                deadline_at=min(now + timedelta(seconds=spec.timeout_seconds), budget.deadline_at),
            )
            validate_tool_invocation(spec, invocation, now=now)
            refs = await self._resources.resources(invocation, spec)
            policy_request = PolicyRequest(
                invocation=invocation, policy=spec.approval_policy, resources=refs
            )
            decision: PolicyDecision = await self._policy.evaluate(policy_request)
            validate_policy_decision(policy_request, decision, now=self._clock())
            if isinstance(decision, PolicyAllow):
                await self._live.require_running(active_state, frame)
                outcome = await self._tools.execute(invocation, decision)
            elif isinstance(decision, PolicyAsk):
                outcome = ToolWaitingApproval(
                    tool_call_id=call.tool_call_id,
                    run_id=state.run_id,
                    operation_id=invocation.operation_id,
                    status="waiting_approval",
                    approval=decision.approval,
                )
            else:
                failure = (
                    decision.failure
                    if isinstance(decision, PolicyDeny)
                    else FailureEnvelope(
                        code="FORBIDDEN",
                        category="authorization",
                        retryable=False,
                        public_message="Tool execution requires manual review.",
                    )
                )
                outcome = ToolNotExecuted(
                    tool_call_id=call.tool_call_id,
                    run_id=state.run_id,
                    operation_id=invocation.operation_id,
                    status="not_executed",
                    effect="none",
                    effect_refs=(),
                    failure=failure,
                )
            validate_tool_outcome(spec, invocation, outcome)
            await self._observations.record(frame, outcome)
            outcomes.append(outcome)
            unresolved_effect = (
                isinstance(outcome, (ToolFailed, ToolCancelled))
                and outcome.effect == "unknown"
            ) or isinstance(outcome, PendingExecution)
            if isinstance(decision, (PolicyAsk, PolicyDeny, PolicyReview)) or unresolved_effect:
                for skipped in action.payload.validated_calls[index + 1 :]:
                    not_executed = ToolNotExecuted(
                        tool_call_id=skipped.tool_call_id,
                        run_id=state.run_id,
                        operation_id=None,
                        status="not_executed",
                        effect="none",
                        effect_refs=(),
                        failure=FailureEnvelope(
                            code="CONTROL_PENDING",
                            category="tool",
                            retryable=False,
                            public_message="A prior tool call stopped this decision batch.",
                        ),
                    )
                    await self._observations.record(frame, not_executed)
                    outcomes.append(not_executed)
                break
        return DispatchResult(active_state, tuple(outcomes))

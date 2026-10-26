"""Resume a decided approval without asking the model to recreate the tool call."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from agent_runtime.action_dispatch import (
    AuthorizedToolExecutor,
    ToolResourceResolver,
    ToolSpecResolver,
)
from agent_runtime.contracts.decisions import InvokeTools
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.loop import BudgetView, RunState
from agent_runtime.contracts.policy import PolicyAllow, PolicyRequest
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.tools import (
    ToolInvocation,
    ToolNotExecuted,
    ToolWaitingApproval,
)
from agent_runtime.loop_runner import RecoveredDecision
from agent_runtime.policy_validation import policy_input_digest, validate_policy_decision
from agent_runtime.storage.approvals import SqlApprovalService
from agent_runtime.storage.decisions import SqlDecisionStore
from agent_runtime.tool_validation import validate_tool_invocation, validate_tool_outcome


class SqlApprovalResumer:
    """Re-execute only the original staged call; SQL ToolHost owns effect idempotency."""

    def __init__(
        self,
        *,
        approvals: SqlApprovalService,
        specs: ToolSpecResolver,
        resources: ToolResourceResolver,
        tools: AuthorizedToolExecutor,
        decisions: SqlDecisionStore,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._approvals = approvals
        self._specs = specs
        self._resources = resources
        self._tools = tools
        self._decisions = decisions
        self._clock = clock

    async def resume(
        self, state: RunState, budget: BudgetView, decision: RecoveredDecision
    ) -> RecoveredDecision:
        waiting = [item for item in decision.outcomes if isinstance(item, ToolWaitingApproval)]
        if len(waiting) != 1 or decision.unresolved_call_ids:
            return decision
        item = waiting[0]
        frame = decision.frame
        if not isinstance(frame.action, InvokeTools):
            return decision
        call = next(
            (
                call
                for call in frame.action.payload.validated_calls
                if call.tool_call_id == item.tool_call_id
            ),
            None,
        )
        if call is None:
            return decision
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
            operation_id=item.operation_id,
            deadline_at=min(now + timedelta(seconds=spec.timeout_seconds), budget.deadline_at),
        )
        validate_tool_invocation(spec, invocation, now=now)
        refs = await self._resources.resources(invocation, spec)
        request = PolicyRequest(invocation=invocation, policy=spec.approval_policy, resources=refs)
        resolution = await self._approvals.read_for_resume(request, spec, item.approval.approval_id)
        if resolution.status == "pending":
            return decision
        if resolution.status == "approved" and resolution.grant_ref is not None:
            evidence = refs or (
                ResourceRef(
                    resource_type="run",
                    resource_id=str(state.run_id),
                    resource_version=state.state_version,
                    scope=ResourceScope(
                        scope_id=state.scope_id,
                        session_id=str(state.session_id),
                        owner_id=frame.actor.actor_id,
                    ),
                ),
            )
            grant = PolicyAllow(
                decision="allow",
                decision_id=item.approval.approval_id,
                policy=request.policy,
                input_digest=policy_input_digest(request),
                evidence_refs=evidence,
                evaluated_at=now,
                expires_at=invocation.deadline_at,
                grant_ref=resolution.grant_ref,
            )
            validate_policy_decision(request, grant, now=self._clock())
            outcome = await self._tools.execute(invocation, grant)
        else:
            outcome = ToolNotExecuted(
                status="not_executed",
                tool_call_id=item.tool_call_id,
                run_id=state.run_id,
                operation_id=item.operation_id,
                effect="none",
                effect_refs=(),
                failure=FailureEnvelope(
                    code="DELIVERY_EXPIRED" if resolution.status == "expired" else "FORBIDDEN",
                    category="delivery" if resolution.status == "expired" else "authorization",
                    retryable=False,
                    public_message=(
                        "The approval expired before execution."
                        if resolution.status == "expired"
                        else "The requested tool action was not approved."
                    ),
                ),
            )
        validate_tool_outcome(spec, invocation, outcome)
        await self._decisions.record(frame, outcome)
        refreshed = await self._decisions.recover()
        if refreshed is None:
            raise RuntimeError("approval result was not recoverable after persistence")
        return refreshed


__all__ = ["SqlApprovalResumer"]

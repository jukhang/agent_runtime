"""Tool boundary checks; live policy, approval and ledger checks remain mandatory.

DS-P01-03: digests use ``contracts.canonical.content_digest`` and boundary rejections
raise ``AgentRuntimeError(FailureEnvelope)``.
"""

from datetime import datetime
from typing import NoReturn

from pydantic import JsonValue

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope, runtime_failure
from agent_runtime.contracts.tools import (
    ApprovalBinding,
    ApprovedGrant,
    PendingExecution,
    TerminalToolOutcome,
    ToolCompleted,
    ToolInvocation,
    ToolOutcome,
    ToolSpec,
    ToolWaitingApproval,
    tool_outcome_adapter,
)


def _reject(message: str) -> NoReturn:
    raise runtime_failure(
        "INVALID_INPUT",
        category="validation",
        public_message=message,
    )


def _aware(now: datetime) -> None:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")


def arguments_digest(arguments: dict[str, JsonValue]) -> str:
    try:
        return content_digest(arguments)
    except (ValueError, RecursionError, UnicodeError):
        _reject("Tool arguments must be finite, UTF-8 JSON.")


def tool_timeout_failure(invocation: ToolInvocation) -> AgentRuntimeError:
    return runtime_failure(
        "TOOL_TIMEOUT",
        category="tool",
        retryable=False,
        public_message="The tool invocation deadline has expired.",
        operation_id=str(invocation.operation_id) if invocation.operation_id is not None else None,
        deadline_at=invocation.deadline_at,
    )


def validate_tool_invocation(spec: ToolSpec, invocation: ToolInvocation, *, now: datetime) -> None:
    spec = ToolSpec.model_validate(spec.model_dump())
    invocation = ToolInvocation.model_validate(invocation.model_dump())
    _aware(now)
    if spec.ref != invocation.call.tool:
        _reject("Tool revision does not match invocation.")
    if spec.side_effect != "none" and invocation.operation_id is None:
        _reject("Tool side effects require a stable operation identity.")
    if now >= invocation.deadline_at:
        raise tool_timeout_failure(invocation)
    arguments_digest(invocation.call.arguments)


def validate_approved_grant(
    grant: ApprovedGrant, expected: ApprovalBinding, *, now: datetime
) -> None:
    grant = ApprovedGrant.model_validate(grant.model_dump())
    expected = ApprovalBinding.model_validate(expected.model_dump())
    _aware(now)
    if grant.binding != expected or not grant.approved_at <= now < grant.expires_at:
        raise AgentRuntimeError(
            FailureEnvelope(
                code="FORBIDDEN",
                category="authorization",
                retryable=False,
                public_message="Approval does not match this operation or is no longer valid.",
            )
        )


def validate_tool_outcome(spec: ToolSpec, invocation: ToolInvocation, outcome: ToolOutcome) -> None:
    spec = ToolSpec.model_validate(spec.model_dump())
    invocation = ToolInvocation.model_validate(invocation.model_dump())
    outcome = tool_outcome_adapter.validate_python(outcome.model_dump())
    if spec.ref != invocation.call.tool:
        _reject("Tool revision does not match invocation.")
    if spec.side_effect != "none" and invocation.operation_id is None:
        _reject("Tool side effects require a stable operation identity.")
    if (outcome.tool_call_id, outcome.run_id, outcome.operation_id) != (
        invocation.call.tool_call_id,
        invocation.run_id,
        invocation.operation_id,
    ):
        _reject("Tool outcome identity mismatch.")
    if isinstance(outcome, TerminalToolOutcome):
        if spec.side_effect == "none" and outcome.effect != "none":
            _reject("Read-only tool reported side effects.")
    if isinstance(outcome, ToolCompleted) and outcome.result.policy != spec.public_result_policy:
        _reject("Tool result does not follow its public projection policy.")
    if isinstance(outcome, PendingExecution):
        ref = outcome.execution_ref
        if ref.resource_type != "execution" or ref.scope.scope_id != invocation.actor.scope_id:
            _reject("Pending execution reference is outside the invocation scope.")
    if isinstance(outcome, ToolWaitingApproval):
        binding = outcome.approval.binding
        if (
            binding.scope_id != invocation.actor.scope_id
            or binding.actor_id != invocation.actor.actor_id
            or binding.auth_revision != invocation.actor.auth_revision
            or binding.session_id != invocation.session_id
            or binding.tool != spec.ref
            or binding.policy != spec.approval_policy
            or binding.arguments_digest != arguments_digest(invocation.call.arguments)
        ):
            _reject("Approval request does not match the invocation.")

"""Canonical policy identity checks do not replace live permission evaluation.

DS-P01-03: digests use ``contracts.canonical.content_digest`` and domain rejections
raise ``AgentRuntimeError(FailureEnvelope)``; a naive ``now`` stays a ``ValueError``
programming error.
"""

from datetime import datetime

from agent_runtime.contracts.canonical import CanonicalJSONError, content_digest
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.policy import PolicyAsk, PolicyDecision, PolicyRequest, policy_adapter
from agent_runtime.tool_validation import arguments_digest


def policy_input_digest(request: PolicyRequest) -> str:
    request = PolicyRequest.model_validate(request.model_dump())
    try:
        return content_digest(request.model_dump(mode="json", by_alias=True))
    except CanonicalJSONError as exc:
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="The policy request has no canonical JSON encoding.",
        ) from exc


def validate_policy_decision(
    request: PolicyRequest, decision: PolicyDecision, *, now: datetime
) -> None:
    decision = policy_adapter.validate_python(decision.model_dump())
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if decision.policy != request.policy or decision.input_digest != policy_input_digest(request):
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="The policy decision does not match the submitted request.",
        )
    if not decision.evaluated_at <= now < decision.expires_at:
        raise runtime_failure(
            "DELIVERY_EXPIRED",
            category="delivery",
            public_message="The policy decision is no longer valid.",
        )
    if decision.expires_at > request.invocation.deadline_at:
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="The policy decision outlives the invocation deadline.",
        )
    if any(
        ref.scope.scope_id != request.invocation.actor.scope_id for ref in decision.evidence_refs
    ):
        raise runtime_failure(
            "FORBIDDEN",
            category="authorization",
            public_message="Policy evidence is outside the invocation scope.",
        )
    if isinstance(decision, PolicyAsk):
        binding, invocation = decision.approval.binding, request.invocation
        if (
            binding.scope_id != invocation.actor.scope_id
            or binding.actor_id != invocation.actor.actor_id
            or binding.auth_revision != invocation.actor.auth_revision
            or binding.session_id != invocation.session_id
            or binding.operation_id != invocation.operation_id
            or binding.tool != invocation.call.tool
            or binding.arguments_digest != arguments_digest(invocation.call.arguments)
            or binding.resources != request.resources
        ):
            raise runtime_failure(
                "FORBIDDEN",
                category="authorization",
                public_message="The approval binding does not match the policy input.",
            )

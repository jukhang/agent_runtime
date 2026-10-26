"""Compare prepared context with a provider request before consulting live authorization."""

from agent_runtime.contracts.context import PreparedContext
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.model import ProviderRequest


def validate_context_binding(context: PreparedContext, request: ProviderRequest) -> None:
    context = PreparedContext.model_validate(context.model_dump())
    request = ProviderRequest.model_validate(request.model_dump())
    expected = (
        context.scope_id,
        context.session_id,
        context.run_id,
        context.snapshot_id,
        context.context_version,
        context.payload_ref,
        context.content_hash,
        context.budget.allocation_id,
        context.estimated_input_tokens,
        context.model,
        context.tools,
        context.budget.reserved_output_tokens,
    )
    actual = (
        request.scope_id,
        request.session_id,
        request.run_id,
        request.context_snapshot_id,
        request.context_version,
        request.input_ref,
        request.input_digest,
        request.budget_allocation_id,
        request.estimated_input_tokens,
        request.model.ref,
        request.tools,
        request.model.max_output_tokens,
    )
    if expected != actual:
        raise AgentRuntimeError(
            FailureEnvelope(
                code="INVALID_INPUT",
                category="context",
                retryable=False,
                public_message="Provider request differs from its prepared context snapshot.",
            )
        )

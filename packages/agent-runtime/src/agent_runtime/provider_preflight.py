"""Pure preflight checks, not runtime authorization or an HTTP adapter."""

from collections.abc import Callable
from copy import deepcopy
from datetime import datetime

from pydantic import JsonValue

from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.model import ModelConfig, ProviderCapabilities, ProviderRequest

type OptionsValidator = Callable[[ModelConfig, dict[str, JsonValue]], None]


def validate_provider_request(
    request: ProviderRequest,
    capabilities: ProviderCapabilities,
    *,
    now: datetime,
    validate_options: OptionsValidator | None = None,
) -> None:
    # Revalidate nested mutable dictionaries; frozen models alone do not freeze their contents.
    request = ProviderRequest.model_validate(request.model_dump())
    capabilities = ProviderCapabilities.model_validate(capabilities.model_dump())
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must include timezone")
    if now >= request.deadline_at:
        raise AgentRuntimeError(
            FailureEnvelope(
                code="PROVIDER_TIMEOUT",
                category="provider",
                retryable=False,
                public_message="The model request deadline has expired.",
            )
        )

    def require(condition: bool, message: str) -> None:
        if not condition:
            raise AgentRuntimeError(
                FailureEnvelope(
                    code="INVALID_INPUT",
                    category="validation",
                    retryable=False,
                    public_message=message,
                )
            )

    require(request.model.ref == capabilities.model, "Model capability revision mismatch.")
    require(capabilities.streaming == "supported", "Streaming support has not been verified.")
    if request.tools:
        require(capabilities.tool_calls == "supported", "Tool calling is not supported.")
    if request.output_schema is not None:
        require(
            capabilities.structured_output == "supported", "Structured output is not supported."
        )
        if request.tools:
            require(
                capabilities.tools_with_structured_output == "supported",
                "Tools with structured output are not supported.",
            )
    if request.model.thinking != "default":
        require(capabilities.thinking_control == "supported", "Thinking control is not supported.")
    if request.tools and request.model.thinking != "disabled":
        require(
            capabilities.tools_with_thinking == "supported",
            "Tool compatibility with enabled or default thinking has not been verified.",
        )
    require(
        capabilities.context_window is not None and capabilities.max_output_tokens is not None,
        "Model token limits have not been verified.",
    )
    output = request.model.max_output_tokens
    if output > (capabilities.max_output_tokens or 0) or (
        request.estimated_input_tokens + output > (capabilities.context_window or 0)
    ):
        raise AgentRuntimeError(
            FailureEnvelope(
                code="CONTEXT_LIMIT",
                category="context",
                retryable=False,
                public_message="The prepared request exceeds the verified model token limits.",
            )
        )
    options = request.model.provider_options
    require(set(options) <= set(capabilities.allowed_option_keys), "Unregistered provider option.")
    if options:
        require(validate_options is not None, "Provider option validation is required.")
        if validate_options is not None:
            try:
                validate_options(request.model.model_copy(deep=True), deepcopy(options))
            except ValueError:
                raise AgentRuntimeError(
                    FailureEnvelope(
                        code="INVALID_INPUT",
                        category="validation",
                        retryable=False,
                        public_message="Provider options are invalid for this model configuration.",
                    )
                ) from None

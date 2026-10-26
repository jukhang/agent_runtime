"""Pinned model configuration and prepared request shapes, without credentials."""

import json
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, JsonValue, model_validator

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.views import Digest

type Support = Literal["supported", "unsupported", "unknown"]
type PositiveInt = Annotated[int, Field(strict=True, gt=0)]

# These values are assembled by the adapter from trusted request fields, never merged from
# options. The comparison normalizes case and separators, so SDK spellings such as
# ``apiBase``, ``api-base`` or ``API_BASE`` cannot smuggle a reserved field past the check.
RESERVED_OPTIONS = frozenset(
    {
        # Request shape and instructions.
        "model",
        "model_name",
        "messages",
        "input",
        "instructions",
        "system",
        "system_prompt",
        "prompt",
        "tools",
        "tool_choice",
        "stream",
        "stream_options",
        "response_format",
        "max_tokens",
        "max_completion_tokens",
        "max_output_tokens",
        # Transport endpoints, credentials and identity.
        "api_key",
        "api_key_header",
        "apikey",
        "authorization",
        "auth",
        "token",
        "access_token",
        "secret",
        "credentials",
        "password",
        "base_url",
        "baseurl",
        "api_base",
        "api_type",
        "api_version",
        "url",
        "endpoint",
        "azure_endpoint",
        "azure_ad_token",
        "azure_ad_token_provider",
        "azure_deployment",
        "deployment_name",
        "resource",
        "organization",
        "org_id",
        "project",
        "project_id",
        # HTTP client plumbing.
        "headers",
        "extra_headers",
        "default_headers",
        "custom_headers",
        "http_client",
        "client",
        "session",
        "transport",
        "proxy",
        "proxies",
        "verify",
        "cert",
        "ca_bundle",
        "ssl_context",
        "timeout",
        "request_timeout",
        "connect_timeout",
        "read_timeout",
        "max_retries",
        "retries",
        "websocket_base_url",
        "websocket_extra_headers",
        # Out-of-band side channels and pass-through bodies.
        "extra_body",
        "extra_query",
        "metadata",
        "user",
        "store",
        "background",
        "callback_url",
        "webhook_url",
        "notify_url",
    }
)


def _normalized_option_key(key: str) -> str:
    """Collapse case and separators so SDK alias spellings share one reserved key."""
    return key.replace("_", "").replace("-", "").replace(".", "").lower()


RESERVED_OPTION_KEYS = frozenset(_normalized_option_key(key) for key in RESERVED_OPTIONS)


def is_reserved_option(key: str) -> bool:
    """True when an option key addresses a field the adapter assembles itself."""
    return _normalized_option_key(key) in RESERVED_OPTION_KEYS


class ModelConfig(Contract):
    ref: CapabilityRef
    connection: CapabilityRef
    model_name: Identifier
    max_output_tokens: PositiveInt
    thinking: Literal["default", "enabled", "disabled"]
    provider_options: dict[Identifier, JsonValue]

    @model_validator(mode="after")
    def bounded_options(self) -> Self:
        if any(is_reserved_option(key) for key in self.provider_options):
            raise ValueError("provider options cannot override runtime or transport fields")
        try:
            encoded = json.dumps(self.provider_options, allow_nan=False, ensure_ascii=True)
        except (ValueError, RecursionError) as exc:
            raise ValueError("provider options must be finite JSON") from exc
        if len(encoded.encode("utf-8")) > 65536:
            raise ValueError("provider options exceed 64 KiB")
        return self


class ProviderCapabilities(Contract):
    model: CapabilityRef
    streaming: Support
    tool_calls: Support
    structured_output: Support
    thinking_control: Support
    tools_with_structured_output: Support
    tools_with_thinking: Support
    usage: Support
    cancellation: Support
    context_window: PositiveInt | None
    max_output_tokens: PositiveInt | None
    allowed_option_keys: tuple[Identifier, ...]

    @model_validator(mode="after")
    def unique_options(self) -> Self:
        if len(self.allowed_option_keys) != len(set(self.allowed_option_keys)):
            raise ValueError("duplicate provider option key")
        if any(is_reserved_option(key) for key in self.allowed_option_keys):
            raise ValueError("cannot allow reserved provider options")
        return self


class ProviderRequest(Contract):
    scope_id: Identifier
    session_id: UUID
    run_id: UUID
    attempt_id: UUID
    context_snapshot_id: UUID
    context_version: Revision
    input_ref: Identifier
    input_digest: Digest
    estimated_input_tokens: Annotated[int, Field(strict=True, ge=0)]
    budget_allocation_id: UUID
    deadline_at: AwareDatetime
    model: ModelConfig
    tools: tuple[CapabilityRef, ...]
    output_schema: CapabilityRef | None

    @model_validator(mode="after")
    def unique_tools(self) -> Self:
        keys = [tool.key for tool in self.tools]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate requested tool")
        return self

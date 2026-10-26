"""Normalized provider events, distinct from public runtime timeline events."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, JsonValue, TypeAdapter, model_validator

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.results import Usage


class ProviderBase(Contract):
    attempt_id: UUID
    sequence: Revision


class ContentDelta(ProviderBase):
    kind: Literal["content.delta"]
    text: str


class AnalysisDelta(ProviderBase):
    kind: Literal["analysis.delta"]
    text: str


class ToolArgumentsDelta(ProviderBase):
    kind: Literal["tool.arguments.delta"]
    call_id: Identifier
    text: str


class ToolCallComplete(ProviderBase):
    kind: Literal["tool.complete"]
    call_id: Identifier
    name: Identifier
    arguments: dict[str, JsonValue]


#: Reasons a provider stream may assert. ``estimated`` and ``collection_failed`` are
#: produced by the runtime convergence layer on ModelAttempt/Budget settlement, never by
#: the provider protocol (DS-P02-00 P2-2).
PROVIDER_USAGE_REASONS = ("provider_reported", "provider_not_reported")


class ProviderUsage(ProviderBase):
    kind: Literal["usage"]
    usage: Usage

    @model_validator(mode="after")
    def provider_reported_reason_only(self) -> Self:
        if self.usage.reason not in PROVIDER_USAGE_REASONS:
            raise ValueError("provider usage cannot carry a runtime-derived reason")
        return self


class ProviderFinish(ProviderBase):
    kind: Literal["finish"]
    reason: Literal["stop", "tool_calls", "length", "content_filter", "cancelled", "unknown"]
    raw_reason: str | None


class ProviderError(ProviderBase):
    kind: Literal["error"]
    failure: FailureEnvelope


type ProviderEvent = Annotated[
    ContentDelta
    | AnalysisDelta
    | ToolArgumentsDelta
    | ToolCallComplete
    | ProviderUsage
    | ProviderFinish
    | ProviderError,
    Field(discriminator="kind"),
]
provider_event_adapter: TypeAdapter[ProviderEvent] = TypeAdapter(ProviderEvent)

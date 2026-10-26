"""Context request and prepared snapshot metadata; no source is loaded here."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.references import CapabilityRef, ResourceRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.views import Digest


class ContextBudget(Contract):
    allocation_id: UUID
    max_input_tokens: Annotated[int, Field(strict=True, ge=1)]
    reserved_output_tokens: Annotated[int, Field(strict=True, ge=1)]


class ContextRequest(Contract):
    actor: ActorContext
    session_id: UUID
    run_id: UUID
    profile: CapabilityRef
    model: CapabilityRef
    prompt: CapabilityRef
    tools: tuple[CapabilityRef, ...]
    resource_refs: tuple[ResourceRef, ...]
    budget: ContextBudget
    prompt_text: str | None = None
    user_input: str | None = None


class ContextSourceRecord(Contract):
    source_id: Identifier
    resource: ResourceRef
    required: Annotated[bool, Field(strict=True)]
    disposition: Literal["included", "summarized", "truncated", "omitted"]
    reason: str | None
    summary_ref: ResourceRef | None
    summary_status: Literal["completed"] | None

    @model_validator(mode="after")
    def safe_processing(self) -> Self:
        if self.resource.resource_version is None:
            raise ValueError("prepared source requires a pinned resource version")
        if self.required and self.disposition in {"truncated", "omitted"}:
            raise ValueError("required context source cannot be truncated or omitted")
        if self.disposition != "included" and (self.reason is None or not self.reason.strip()):
            raise ValueError("context transformation requires an explicit reason")
        if self.disposition == "summarized":
            if self.summary_ref is None or self.summary_status != "completed":
                raise ValueError("summary source must reference a completed summary")
            if self.summary_ref.resource_version is None:
                raise ValueError("prepared summary requires a pinned version")
        elif self.summary_ref is not None or self.summary_status is not None:
            raise ValueError("non-summary source cannot carry summary metadata")
        return self


class PreparedContext(Contract):
    scope_id: Identifier
    session_id: UUID
    run_id: UUID
    auth_revision: Revision
    snapshot_id: UUID
    context_version: Revision
    payload_ref: Identifier
    content_hash: Digest
    model: CapabilityRef
    prompt: CapabilityRef
    tools: tuple[CapabilityRef, ...]
    budget: ContextBudget
    estimated_input_tokens: Annotated[int, Field(strict=True, ge=0)]
    sources: tuple[ContextSourceRecord, ...]

    @model_validator(mode="after")
    def consistent_snapshot(self) -> Self:
        if self.estimated_input_tokens > self.budget.max_input_tokens:
            raise ValueError("prepared input exceeds context budget")
        ids = [source.source_id for source in self.sources]
        keys = [tool.key for tool in self.tools]
        if len(ids) != len(set(ids)) or len(keys) != len(set(keys)):
            raise ValueError("duplicate context source or tool")
        return self

"""Scoped, versioned metadata for delegation across Agent runtimes."""

from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from agent_runtime.contracts.base import Contract, Identifier
from agent_runtime.contracts.references import ResourceRef


class A2ADelegationEnvelope(Contract):
    scope_id: Identifier
    parent_run_id: UUID
    child_run_id: UUID
    message_id: Identifier
    sequence: int = Field(strict=True, ge=1)
    budget_model_calls: int = Field(strict=True, ge=1)
    budget_tool_calls: int = Field(strict=True, ge=0)
    budget_tokens: int = Field(strict=True, ge=1)
    deadline_at: AwareDatetime
    resources: tuple[ResourceRef, ...]

    @model_validator(mode="after")
    def pinned_resources(self) -> "A2ADelegationEnvelope":
        if self.parent_run_id == self.child_run_id:
            raise ValueError("delegation cannot target its own parent Run")
        if any(
            ref.scope.scope_id != self.scope_id or ref.resource_version is None
            for ref in self.resources
        ):
            raise ValueError("delegation resources require matching scope and pinned versions")
        return self


__all__ = ["A2ADelegationEnvelope"]

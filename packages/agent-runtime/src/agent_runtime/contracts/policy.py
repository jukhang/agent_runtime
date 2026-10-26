"""Policy evidence is not an executable grant; trusted services resolve its references."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, TypeAdapter, model_validator

from agent_runtime.contracts.base import Contract, Identifier
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.references import CapabilityRef, ResourceRef
from agent_runtime.contracts.tools import ApprovalRequest, ToolInvocation
from agent_runtime.contracts.views import Digest


class PolicyRequest(Contract):
    invocation: ToolInvocation
    policy: CapabilityRef
    resources: tuple[ResourceRef, ...]

    @model_validator(mode="after")
    def pinned_targets(self) -> Self:
        keys = [(ref.resource_type, ref.resource_id) for ref in self.resources]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate policy target")
        for ref in self.resources:
            if ref.scope.scope_id != self.invocation.actor.scope_id or ref.resource_version is None:
                raise ValueError("policy targets require matching scope and a pinned version")
        return self


class PolicyBase(Contract):
    decision_id: UUID
    policy: CapabilityRef
    input_digest: Digest
    evidence_refs: Annotated[tuple[ResourceRef, ...], Field(min_length=1)]
    evaluated_at: AwareDatetime
    expires_at: AwareDatetime

    @model_validator(mode="after")
    def valid_interval(self) -> Self:
        if self.expires_at <= self.evaluated_at:
            raise ValueError("policy decision validity must be positive")
        return self


class PolicyAllow(PolicyBase):
    decision: Literal["allow"]
    grant_ref: Identifier


class PolicyDeny(PolicyBase):
    decision: Literal["deny"]
    failure: FailureEnvelope


class PolicyAsk(PolicyBase):
    decision: Literal["ask"]
    approval: ApprovalRequest

    @model_validator(mode="after")
    def bound_policy(self) -> Self:
        if self.approval.binding.policy != self.policy:
            raise ValueError("approval uses another policy revision")
        if self.approval.expires_at <= self.evaluated_at:
            raise ValueError("approval request must remain valid after evaluation")
        return self


class PolicyReview(PolicyBase):
    decision: Literal["review"]
    review_request_ref: Identifier


type PolicyDecision = Annotated[
    PolicyAllow | PolicyDeny | PolicyAsk | PolicyReview, Field(discriminator="decision")
]
policy_adapter: TypeAdapter[PolicyDecision] = TypeAdapter(PolicyDecision)

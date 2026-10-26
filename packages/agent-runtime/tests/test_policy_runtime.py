import json
from datetime import UTC, datetime, timedelta
from importlib.resources import files

import pytest

from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.policy import PolicyAllow, PolicyAsk, PolicyRequest
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.tools import ApprovalRequest, ToolSpec
from agent_runtime.policy_runtime import PolicyFacts, PolicyRuntime

DATA = json.loads(files("agent_runtime.contracts").joinpath("fixtures/tools.json").read_text())
NOW = datetime(2026, 9, 14, 10, tzinfo=UTC)


class Specs:
    def __init__(self, spec: ToolSpec):
        self.spec = spec

    async def get(self, request: PolicyRequest) -> ToolSpec:
        return self.spec


class Authority:
    def __init__(self, disposition: str, approval_expires_at: datetime | None = None):
        self.disposition = disposition
        self.approval_expires_at = approval_expires_at
        self.issued = 0
        self.approvals = 0

    async def inspect(self, request: PolicyRequest, spec: ToolSpec) -> PolicyFacts:
        evidence = ResourceRef.model_validate(
            {
                "resourceType": "policy_evidence",
                "resourceId": "acl-1",
                "resourceVersion": "1",
                "scope": {"scopeId": request.invocation.actor.scope_id},
            }
        )
        failure = (
            FailureEnvelope(
                code="FORBIDDEN",
                category="authorization",
                retryable=False,
                public_message="Access denied.",
            )
            if self.disposition == "deny"
            else None
        )
        return PolicyFacts(self.disposition, (evidence,), failure)  # type: ignore[arg-type]

    async def issue_grant(self, request: PolicyRequest, spec: ToolSpec) -> str:
        self.issued += 1
        return "grant-1"

    async def request_approval(self, request: PolicyRequest, spec: ToolSpec) -> ApprovalRequest:
        self.approvals += 1
        return ApprovalRequest.model_validate(
            {
                "binding": DATA["binding"],
                "approvalId": "00000000-0000-0000-0000-000000000007",
                "idempotencyKey": "approve-1",
                "publicSummary": "Approve change",
                "expiresAt": (self.approval_expires_at or NOW + timedelta(seconds=30)).isoformat(),
            }
        )


def request() -> PolicyRequest:
    return PolicyRequest.model_validate(
        {
            "invocation": DATA["invocation"],
            "policy": DATA["spec"]["approvalPolicy"],
            "resources": [],
        }
    )


@pytest.mark.asyncio
async def test_policy_allow_requires_authority_issued_grant():
    authority = Authority("allow")
    decision = await PolicyRuntime(
        specs=Specs(ToolSpec.model_validate(DATA["spec"])), authority=authority, clock=lambda: NOW
    ).evaluate(request())
    assert isinstance(decision, PolicyAllow)
    assert decision.grant_ref == "grant-1"
    assert authority.issued == 1


@pytest.mark.asyncio
async def test_policy_denial_does_not_issue_grant_or_approval():
    authority = Authority("deny")
    decision = await PolicyRuntime(
        specs=Specs(ToolSpec.model_validate(DATA["spec"])), authority=authority, clock=lambda: NOW
    ).evaluate(request())
    assert decision.decision == "deny"
    assert authority.issued == authority.approvals == 0


@pytest.mark.asyncio
async def test_policy_approval_is_bound_to_operation():
    authority = Authority("approval_required", NOW + timedelta(hours=24))
    decision = await PolicyRuntime(
        specs=Specs(ToolSpec.model_validate(DATA["spec"])), authority=authority, clock=lambda: NOW
    ).evaluate(request())
    assert isinstance(decision, PolicyAsk)
    assert authority.approvals == 1
    assert authority.issued == 0
    assert decision.expires_at < decision.approval.expires_at


@pytest.mark.asyncio
async def test_policy_revision_mismatch_fails_closed():
    authority = Authority("allow")
    changed = ToolSpec.model_validate(
        {**DATA["spec"], "approvalPolicy": {**DATA["spec"]["approvalPolicy"], "revision": "2"}}
    )
    with pytest.raises(AgentRuntimeError):
        await PolicyRuntime(specs=Specs(changed), authority=authority, clock=lambda: NOW).evaluate(
            request()
        )
    assert authority.issued == 0

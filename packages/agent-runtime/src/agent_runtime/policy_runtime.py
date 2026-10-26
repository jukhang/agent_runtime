"""Trusted policy orchestration; model output never creates an executable grant."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol
from uuid import uuid4

from agent_runtime.contracts.failure import FailureEnvelope, runtime_failure
from agent_runtime.contracts.policy import (
    PolicyAllow,
    PolicyAsk,
    PolicyDecision,
    PolicyDeny,
    PolicyRequest,
)
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.tools import ApprovalRequest, ToolSpec
from agent_runtime.policy_validation import policy_input_digest, validate_policy_decision
from agent_runtime.tool_validation import validate_tool_invocation

type Clock = Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class PolicyFacts:
    """Current facts returned by a trusted, transaction-backed host authority.

    The authority must check actor and resource ACL, tool permissions, budget and
    concurrency capacity. ``allow`` is not a grant: issuance is a separate write.
    """

    disposition: Literal["allow", "deny", "approval_required"]
    evidence_refs: tuple[ResourceRef, ...]
    failure: FailureEnvelope | None = None

    def __post_init__(self) -> None:
        if self.disposition not in {"allow", "deny", "approval_required"}:
            raise ValueError("unknown policy disposition")
        if not self.evidence_refs:
            raise ValueError("policy facts require durable evidence")
        if any(ref.resource_version is None for ref in self.evidence_refs):
            raise ValueError("policy evidence must be version pinned")
        if (self.disposition == "deny") != (self.failure is not None):
            raise ValueError("only denial carries a failure")


class PolicyAuthority(Protocol):
    async def inspect(self, request: PolicyRequest, spec: ToolSpec) -> PolicyFacts:
        """Read current ACL, resource versions, budget and capacity; deny by default."""
        ...

    async def issue_grant(self, request: PolicyRequest, spec: ToolSpec) -> str:
        """Atomically recheck facts and issue a short-lived, invocation-bound grant."""
        ...

    async def request_approval(
        self, request: PolicyRequest, spec: ToolSpec
    ) -> ApprovalRequest:
        """Recheck facts and persist an idempotent, version-bound approval request."""
        ...


class ToolSpecReader(Protocol):
    async def get(self, request: PolicyRequest) -> ToolSpec:
        """Resolve the exact pinned tool revision visible to this actor."""
        ...


class PolicyRuntime:
    """Construct a validated decision from host facts, never from model text.

    The returned grant reference is evidence for ToolRuntime. It is deliberately
    insufficient by itself: ToolRuntime must revalidate it under its execution
    transaction, including actor, operation, resource versions and expiry.
    """

    def __init__(
        self,
        *,
        specs: ToolSpecReader,
        authority: PolicyAuthority,
        clock: Clock = lambda: datetime.now(UTC),
        max_decision_seconds: int = 30,
    ) -> None:
        if max_decision_seconds <= 0:
            raise ValueError("policy decision lifetime must be positive")
        self._specs = specs
        self._authority = authority
        self._clock = clock
        self._lifetime = timedelta(seconds=max_decision_seconds)

    async def evaluate(self, request: PolicyRequest) -> PolicyDecision:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("policy clock must be timezone-aware")
        if now >= request.invocation.deadline_at:
            raise runtime_failure(
                "TOOL_TIMEOUT", category="tool", public_message="Tool deadline has expired."
            )
        spec = await self._specs.get(request)
        if spec.ref != request.invocation.call.tool or spec.approval_policy != request.policy:
            raise runtime_failure(
                "FORBIDDEN", category="authorization", public_message="Tool policy changed."
            )
        validate_tool_invocation(spec, request.invocation, now=now)
        facts = await self._authority.inspect(request, spec)
        if any(
            ref.scope.scope_id != request.invocation.actor.scope_id for ref in facts.evidence_refs
        ):
            raise runtime_failure(
                "FORBIDDEN",
                category="authorization",
                public_message="Policy evidence is out of scope.",
            )
        expiry = min(now + self._lifetime, request.invocation.deadline_at)
        common = dict(
            decision_id=uuid4(),
            policy=request.policy,
            input_digest=policy_input_digest(request),
            evidence_refs=facts.evidence_refs,
            evaluated_at=now,
            expires_at=expiry,
        )
        if facts.disposition == "deny":
            assert facts.failure is not None
            decision: PolicyDecision = PolicyDeny.model_validate(
                {**common, "decision": "deny", "failure": facts.failure}
            )
        elif facts.disposition == "approval_required":
            if request.invocation.operation_id is None:
                raise runtime_failure(
                    "INVALID_INPUT",
                    category="validation",
                    public_message="Approval requires a stable operation identity.",
                )
            approval = await self._authority.request_approval(request, spec)
            decision = PolicyAsk.model_validate(
                {**common, "decision": "ask", "approval": approval}
            )
        else:
            grant_ref = await self._authority.issue_grant(request, spec)
            decision = PolicyAllow.model_validate(
                {**common, "decision": "allow", "grant_ref": grant_ref}
            )
        validate_policy_decision(request, decision, now=self._clock())
        return decision

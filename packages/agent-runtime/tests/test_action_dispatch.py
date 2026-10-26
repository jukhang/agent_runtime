"""Effectful dispatch requires a persisted decision and a current policy grant."""

from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from test_agent_loop import build

from agent_runtime.action_dispatch import RunActionDispatcher
from agent_runtime.contracts.decisions import decision_adapter
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope, runtime_failure
from agent_runtime.contracts.policy import PolicyAllow, PolicyRequest
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.tools import (
    ApprovalRequest,
    FullResult,
    PendingExecution,
    ToolCancelled,
    ToolCompleted,
    ToolFailed,
    ToolInvocation,
    ToolOutcome,
    ToolSpec,
)
from agent_runtime.policy_runtime import PolicyFacts, PolicyRuntime
from agent_runtime.tool_validation import arguments_digest

NOW = datetime(2026, 9, 28, tzinfo=UTC)


class Recorder:
    def __init__(
        self, events: list[str], *, fail: bool = False, next_checkpoint: str | None = None
    ):
        self.events = events
        self.fail = fail
        self.next_checkpoint = next_checkpoint

    async def persist(self, state, frame):
        self.events.append("persist")
        if self.fail:
            raise runtime_failure(
                "FENCED", category="concurrency", public_message="Lease lost."
            )
        return (
            state.model_copy(update={"checkpoint_ref": self.next_checkpoint})
            if self.next_checkpoint is not None
            else state
        )


class Live:
    def __init__(
        self,
        events: list[str],
        *,
        fail_after_persist: bool = False,
        fail_after_policy: bool = False,
    ):
        self.events = events
        self.fail_after_persist = fail_after_persist
        self.fail_after_policy = fail_after_policy

    async def require_running(self, state, frame):
        self.events.append("live")
        if self.fail_after_persist and "persist" in self.events:
            raise runtime_failure(
                "FENCED", category="concurrency", public_message="Run was cancelled."
            )
        if self.fail_after_policy and "grant" in self.events:
            raise runtime_failure(
                "FENCED", category="concurrency", public_message="Run was cancelled."
            )


class Specs:
    def __init__(self, spec: ToolSpec):
        self.spec = spec

    async def get(self, call):
        return self.spec


class PolicySpecs:
    def __init__(self, spec: ToolSpec):
        self.spec = spec

    async def get(self, request: PolicyRequest) -> ToolSpec:
        return self.spec


class Resources:
    async def resources(
        self, invocation: ToolInvocation, spec: ToolSpec
    ) -> tuple[ResourceRef, ...]:
        return ()


class Authority:
    def __init__(self, events: list[str], *, deny: bool = False, approval: bool = False):
        self.events = events
        self.deny = deny
        self.approval = approval

    async def inspect(self, request: PolicyRequest, spec: ToolSpec) -> PolicyFacts:
        self.events.append("policy")
        evidence = ResourceRef.model_validate(
            {
                "resourceType": "policy_evidence",
                "resourceId": "acl-1",
                "resourceVersion": "1",
                "scope": {"scopeId": request.invocation.actor.scope_id},
            }
        )
        if self.deny:
            return PolicyFacts(
                "deny",
                (evidence,),
                FailureEnvelope(
                    code="FORBIDDEN",
                    category="authorization",
                    retryable=False,
                    public_message="Access denied.",
                ),
            )
        if self.approval:
            return PolicyFacts("approval_required", (evidence,))
        return PolicyFacts("allow", (evidence,))

    async def issue_grant(self, request: PolicyRequest, spec: ToolSpec) -> str:
        self.events.append("grant")
        return "grant-1"

    async def request_approval(self, request: PolicyRequest, spec: ToolSpec):
        self.events.append("approval")
        invocation = request.invocation
        return ApprovalRequest.model_validate({
            "approvalId": "00000000-0000-0000-0000-000000000007",
            "binding": {
                "scopeId": invocation.actor.scope_id,
                "actorId": invocation.actor.actor_id,
                "sessionId": str(invocation.session_id),
                "tool": invocation.call.tool.model_dump(mode="json", by_alias=True),
                "operationId": str(invocation.operation_id),
                "argumentsDigest": arguments_digest(invocation.call.arguments),
                "policy": request.policy.model_dump(mode="json", by_alias=True),
                "authRevision": invocation.actor.auth_revision,
                "resources": [],
            },
            "idempotencyKey": "approval-1",
            "publicSummary": "Approve write",
            "expiresAt": "2026-09-28T00:00:20Z",
        })


class Tools:
    def __init__(self, events: list[str]):
        self.events = events

    async def execute(self, invocation: ToolInvocation, grant: PolicyAllow) -> ToolOutcome:
        self.events.append("execute")
        assert grant.grant_ref == "grant-1"
        return ToolCompleted(
            tool_call_id=invocation.call.tool_call_id,
            run_id=invocation.run_id,
            operation_id=None,
            status="completed",
            effect="none",
            effect_refs=(),
            result=FullResult(policy="full", data={"answer": "ok"}),
        )


class Observations:
    def __init__(self, events: list[str]):
        self.events = events

    async def record(self, frame, outcome: ToolOutcome) -> None:
        self.events.append("observe")


def dispatcher(
    events: list[str],
    *,
    deny: bool = False,
    approval: bool = False,
    commit_fail: bool = False,
    cancelled: bool = False,
    cancelled_during_policy: bool = False,
    next_checkpoint: str | None = None,
):
    ref = {"key": {"namespace": "builtin", "key": "read"}, "revision": "1"}
    spec = ToolSpec.model_validate(
        {
            "ref": ref,
            "description": "Read",
            "source": ref,
            "kind": "write" if approval else "read",
            "sideEffect": "database" if approval else "none",
            "inputSchema": ref,
            "outputSchema": ref,
            "permissions": [],
            "approvalPolicy": ref,
            "idempotency": "transactional" if approval else "read_only",
            "timeoutSeconds": 30,
            "concurrencyGroup": "read",
            "maxConcurrency": 1,
            "cancellation": "stop",
            "publicResultPolicy": "summary" if approval else "full",
        }
    )
    authority = Authority(events, deny=deny, approval=approval)
    return RunActionDispatcher(
        decisions=Recorder(events, fail=commit_fail, next_checkpoint=next_checkpoint),
        live=Live(
            events,
            fail_after_persist=cancelled,
            fail_after_policy=cancelled_during_policy,
        ),
        specs=Specs(spec),
        resources=Resources(),
        policy=PolicyRuntime(
            specs=PolicySpecs(spec), authority=authority, clock=lambda: NOW
        ),
        tools=Tools(events),
        observations=Observations(events),
        clock=lambda: NOW,
    )


@pytest.mark.asyncio
async def test_tool_is_executed_only_after_persist_and_grant():
    loop, state, inputs, budget, _, _ = build("tool")
    frame = await loop.decide(state, inputs, budget)
    events: list[str] = []
    outcomes = await dispatcher(events).dispatch(state, budget, frame)
    assert outcomes[0].status == "completed"
    assert events == ["live", "persist", "live", "policy", "grant", "live", "execute", "observe"]


@pytest.mark.asyncio
async def test_dispatch_result_propagates_committed_checkpoint():
    loop, state, inputs, budget, _, _ = build("tool")
    frame = await loop.decide(state, inputs, budget)
    result = await dispatcher([], next_checkpoint="staged:1").dispatch_result(
        state, budget, frame
    )
    assert result.state.checkpoint_ref == "staged:1"
    assert len(result.outcomes) == 1


@pytest.mark.asyncio
async def test_cancel_after_policy_prevents_tool_execution():
    loop, state, inputs, budget, _, _ = build("tool")
    frame = await loop.decide(state, inputs, budget)
    events: list[str] = []
    with pytest.raises(AgentRuntimeError):
        await dispatcher(events, cancelled_during_policy=True).dispatch(state, budget, frame)
    assert events[-2:] == ["grant", "live"]
    assert "execute" not in events


@pytest.mark.asyncio
async def test_policy_denial_records_not_executed():
    loop, state, inputs, budget, _, _ = build("tool")
    frame = await loop.decide(state, inputs, budget)
    events: list[str] = []
    outcomes = await dispatcher(events, deny=True).dispatch(state, budget, frame)
    assert outcomes[0].status == "not_executed"
    assert "execute" not in events
    assert events[-1] == "observe"


@pytest.mark.asyncio
async def test_denied_call_converges_remaining_batch_without_execution():
    loop, state, inputs, budget, _, _ = build("tool")
    frame = await loop.decide(state, inputs, budget)
    original_call = frame.action.payload.validated_calls[0]
    action = decision_adapter.validate_python(
        {
            "kind": "invoke_tools",
            "payload": {
                "validatedCalls": [
                    original_call.model_dump(mode="json", by_alias=True),
                    {
                        **original_call.model_dump(mode="json", by_alias=True),
                        "toolCallId": str(uuid4()),
                    },
                ]
            },
        }
    )
    events: list[str] = []
    outcomes = await dispatcher(events, deny=True).dispatch(
        state, budget, replace(frame, action=action)
    )
    assert [outcome.status for outcome in outcomes] == ["not_executed", "not_executed"]
    assert events.count("observe") == 2
    assert "execute" not in events


@pytest.mark.asyncio
@pytest.mark.parametrize("result_kind", ["failed", "cancelled", "pending_execution"])
async def test_unknown_effect_stops_remaining_tool_batch(result_kind):
    loop, state, inputs, budget, _, _ = build("tool")
    frame = await loop.decide(state, inputs, budget)
    first = frame.action.payload.validated_calls[0]
    action = decision_adapter.validate_python(
        {
            "kind": "invoke_tools",
            "payload": {
                "validatedCalls": [
                    first.model_dump(mode="json", by_alias=True),
                    {
                        **first.model_dump(mode="json", by_alias=True),
                        "toolCallId": str(uuid4()),
                    },
                ]
            },
        }
    )
    events: list[str] = []
    spec = dispatcher(events, approval=True)._specs.spec

    class UnknownTool:
        async def execute(self, invocation, grant):
            events.append("execute")
            identity = dict(
                tool_call_id=invocation.call.tool_call_id,
                run_id=invocation.run_id,
                operation_id=invocation.operation_id,
            )
            if result_kind == "pending_execution":
                return PendingExecution(
                    **identity,
                    status="pending_execution",
                    execution_ref=ResourceRef.model_validate(
                        {
                            "resourceType": "execution",
                            "resourceId": str(uuid4()),
                            "resourceVersion": "1",
                            "scope": {"scopeId": state.scope_id},
                        }
                    ),
                )
            outcome_type = ToolFailed if result_kind == "failed" else ToolCancelled
            return outcome_type(
                **identity,
                status=result_kind,
                effect="unknown",
                effect_refs=(),
                failure=FailureEnvelope(
                    code="SIDE_EFFECT_UNKNOWN",
                    category="tool",
                    retryable=False,
                    public_message="Remote effect is not known.",
                ),
            )

    runtime = RunActionDispatcher(
        decisions=Recorder(events),
        live=Live(events),
        specs=Specs(spec),
        resources=Resources(),
        policy=PolicyRuntime(
            specs=PolicySpecs(spec), authority=Authority(events), clock=lambda: NOW
        ),
        tools=UnknownTool(),
        observations=Observations(events),
        clock=lambda: NOW,
    )
    outcomes = await runtime.dispatch(state, budget, replace(frame, action=action))
    assert [outcome.status for outcome in outcomes] == [result_kind, "not_executed"]
    assert events.count("execute") == 1
    assert events.count("observe") == 2


@pytest.mark.asyncio
async def test_approval_wait_records_request_without_tool_execution():
    loop, state, inputs, budget, _, _ = build("tool")
    frame = await loop.decide(state, inputs, budget)
    events: list[str] = []
    outcomes = await dispatcher(events, approval=True).dispatch(state, budget, frame)
    assert outcomes[0].status == "waiting_approval"
    assert "execute" not in events
    assert events[-1] == "observe"


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["commit_fail", "cancelled"])
async def test_no_execution_when_commit_fails_or_cancel_arrives(reason: str):
    loop, state, inputs, budget, _, _ = build("tool")
    frame = await loop.decide(state, inputs, budget)
    events: list[str] = []
    with pytest.raises(AgentRuntimeError):
        await dispatcher(events, **{reason: True}).dispatch(state, budget, frame)
    assert "execute" not in events

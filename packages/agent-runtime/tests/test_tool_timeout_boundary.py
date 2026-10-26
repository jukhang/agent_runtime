"""Only trusted read-only timeouts become observations; cancellation stays cancellation."""

import asyncio
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_tool_extensions import objects

from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.policy import PolicyAllow, PolicyRequest
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.tools import FullResult, ToolCompleted, ToolFailed
from agent_runtime.policy_validation import policy_input_digest
from agent_runtime.storage import tool_host
from agent_runtime.storage.tool_host import SqlToolHost


def invocation_pair(*, effect="none", seconds=30):
    spec, invocation = objects()
    spec = spec.model_copy(
        update={
            "side_effect": effect,
            "idempotency": "read_only" if effect == "none" else "provider_key",
            "kind": "read" if effect == "none" else "write",
        }
    )
    from uuid import uuid4

    invocation = invocation.model_copy(
        update={
            "operation_id": None if effect == "none" else uuid4(),
            "deadline_at": datetime.now(UTC) + timedelta(seconds=seconds),
        }
    )
    return spec, invocation


def grant_for(spec, invocation):
    ref = ResourceRef(
        resource_type="run",
        resource_id=str(invocation.run_id),
        resource_version="1",
        scope=ResourceScope(scope_id=invocation.actor.scope_id),
    )
    from uuid import uuid4

    return PolicyAllow(
        decision="allow",
        decision_id=uuid4(),
        policy=spec.approval_policy,
        input_digest=policy_input_digest(
            PolicyRequest(invocation=invocation, policy=spec.approval_policy, resources=())
        ),
        evidence_refs=(ref,),
        evaluated_at=min(datetime.now(UTC), invocation.deadline_at) - timedelta(seconds=1),
        expires_at=invocation.deadline_at,
        grant_ref="timeout-test",
    )


class Transaction(AbstractAsyncContextManager):
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def begin(self):
        return Transaction()


def fake_host(spec):
    host = SqlToolHost.__new__(SqlToolHost)
    host._specs = SimpleNamespace(get=AsyncMock(return_value=spec))
    host._resources = SimpleNamespace(resources=AsyncMock(return_value=()))
    host._schemas = SimpleNamespace(input=lambda *args: None)
    host._sessions = Transaction
    host._check = AsyncMock(return_value=None)
    host._cached = AsyncMock(return_value=None)
    host._capacity = SimpleNamespace(
        reserve=AsyncMock(return_value=object()),
        release=AsyncMock(),
        settle_committed=AsyncMock(),
    )
    host._hooks = ()
    host._groups = {}
    host._execute_authorized = AsyncMock()
    return host


def assert_timeout(failure, invocation):
    assert failure.code == "TOOL_TIMEOUT"
    assert failure.category == "tool" and failure.retryable is False
    assert failure.deadline_at == invocation.deadline_at
    assert failure.operation_id == (
        str(invocation.operation_id) if invocation.operation_id else None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("effect", ["none", "external"])
async def test_real_timer_awaits_cancelled_handler_cleanup(effect):
    spec, invocation = invocation_pair(effect=effect, seconds=0.05)
    host = fake_host(spec)
    stopped = asyncio.Event()

    async def handler(*args):
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0.01)
            stopped.set()

    host._execute_authorized.side_effect = handler
    if effect == "none":
        outcome = await host.execute(invocation, grant_for(spec, invocation))
        assert isinstance(outcome, ToolFailed) and outcome.effect == "none"
        assert outcome.tool_call_id == invocation.call.tool_call_id
        assert outcome.run_id == invocation.run_id and outcome.operation_id is None
        assert_timeout(outcome.failure, invocation)
        host._capacity.release.assert_awaited_once()
    else:
        with pytest.raises(AgentRuntimeError) as caught:
            await host.execute(invocation, grant_for(spec, invocation))
        assert_timeout(caught.value.failure, invocation)
        assert isinstance(caught.value.__cause__, TimeoutError)
        host._capacity.release.assert_not_awaited()
    assert stopped.is_set()
    host._execute_authorized.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("effect", ["none", "external"])
@pytest.mark.parametrize("position", ["validation", "before_execution"])
async def test_expired_before_execution(effect, position, monkeypatch):
    spec, invocation = invocation_pair(
        effect=effect, seconds=-1 if position == "validation" else 30
    )
    grant = grant_for(spec, invocation)
    host = fake_host(spec)
    if position == "before_execution":
        times = iter([datetime.now(UTC), datetime.now(UTC), invocation.deadline_at])
        monkeypatch.setattr(tool_host, "datetime", SimpleNamespace(now=lambda *args: next(times)))
    if effect == "none":
        outcome = await host.execute(invocation, grant)
        assert isinstance(outcome, ToolFailed) and outcome.effect == "none"
        assert_timeout(outcome.failure, invocation)
    else:
        with pytest.raises(AgentRuntimeError) as caught:
            await host.execute(invocation, grant)
        assert_timeout(caught.value.failure, invocation)
    host._capacity.reserve.assert_not_awaited()
    host._execute_authorized.assert_not_awaited()


@pytest.mark.asyncio
async def test_database_prewrite_expiry_is_not_control(monkeypatch):
    spec, invocation = invocation_pair(effect="external")
    spec = spec.model_copy(update={"side_effect": "database", "idempotency": "transactional"})
    host = fake_host(spec)
    host._claim = AsyncMock(return_value={"status": "prepared"})
    handler = SimpleNamespace(invoke=AsyncMock())
    grant = grant_for(spec, invocation)

    async def execute(*args):
        monkeypatch.setattr(
            tool_host, "datetime", SimpleNamespace(now=lambda *args: invocation.deadline_at)
        )
        return await host._database_effect(invocation, grant, spec, (), handler)

    host._execute_authorized.side_effect = execute
    with pytest.raises(AgentRuntimeError) as caught:
        await host.execute(invocation, grant)
    assert_timeout(caught.value.failure, invocation)
    assert isinstance(caught.value.__cause__, TimeoutError)
    handler.invoke.assert_not_awaited()
    host._capacity.release.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["CONTROL_PENDING", "FORBIDDEN", "FENCED", "TOOL_TIMEOUT"])
async def test_structured_handler_error_keeps_identity(code):
    spec, invocation = invocation_pair()
    host = fake_host(spec)
    error = runtime_failure(code, category="tool", public_message="original")
    host._execute_authorized.side_effect = error
    with pytest.raises(AgentRuntimeError) as caught:
        await host.execute(invocation, grant_for(spec, invocation))
    assert caught.value is error and caught.value.__cause__ is None


@pytest.mark.asyncio
async def test_caller_cancel_is_not_timeout():
    spec, invocation = invocation_pair()
    host = fake_host(spec)
    entered = asyncio.Event()
    stopped = asyncio.Event()

    async def handler(*args):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            stopped.set()

    host._execute_authorized.side_effect = handler
    task = asyncio.create_task(host.execute(invocation, grant_for(spec, invocation)))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stopped.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("cached", [False, True])
async def test_completed_outcome_is_unchanged(cached):
    spec, invocation = invocation_pair()
    host = fake_host(spec)
    outcome = ToolCompleted(
        tool_call_id=invocation.call.tool_call_id,
        run_id=invocation.run_id,
        operation_id=None,
        status="completed",
        effect="none",
        effect_refs=(),
        result=FullResult(policy="full", data={"value": 420}),
    )
    if cached:
        host._cached.return_value = outcome
    host._execute_authorized.return_value = outcome
    assert await host.execute(invocation, grant_for(spec, invocation)) is outcome
    assert host._execute_authorized.await_count == (0 if cached else 1)


@pytest.mark.asyncio
async def test_unknown_outcome_and_untyped_non_timeout_are_unchanged():
    spec, invocation = invocation_pair(effect="external")
    host = fake_host(spec)
    outcome = host._unknown(invocation)
    host._execute_authorized.return_value = outcome
    assert await host.execute(invocation, grant_for(spec, invocation)) is outcome
    host._capacity.release.assert_not_awaited()
    original = ValueError("not timeout")
    host._execute_authorized.side_effect = original
    with pytest.raises(ValueError) as caught:
        await host.execute(invocation, grant_for(spec, invocation))
    assert caught.value is original

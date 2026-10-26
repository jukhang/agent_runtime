"""Consumer failures must synchronously close every provider forwarding layer."""

import asyncio

import pytest
from test_agent_loop import build
from test_decision_validation import NOW
from test_provider_invoker import Attempts, capabilities

from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.provider import ProviderInvoker


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["event", "save", "decode", "tool", "validation", "finish"])
async def test_consumer_fault_closes_provider_and_audits_before_return(fault):
    kind = "bad_structured" if fault == "decode" else "tool" if fault == "tool" else "answer"
    loop, state, inputs, budget, backend, _ = build(kind)
    closed = asyncio.Event()
    original_stream = backend.stream

    async def stream(*args, **kwargs):
        try:
            async for event in original_stream(*args, **kwargs):
                yield event
        finally:
            closed.set()

    backend.stream = stream
    attempts = Attempts()
    loop._provider = ProviderInvoker(
        backend, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )
    failure = runtime_failure(
        "CONTROL_PENDING", category="concurrency", public_message="Control accepted."
    )

    class Events:
        async def record(self, request, event):
            if fault == "event" or (fault == "finish" and event.kind == "finish"):
                raise failure

    class Answers:
        async def save(self, *args):
            assert closed.is_set() and len(attempts.terminals) == 1
            raise failure

    class Names:
        def resolve(self, *args):
            raise failure

    loop._events = Events()
    if fault == "save":
        loop._answers = Answers()
    if fault == "tool":
        loop._tool_names = Names()
    if fault == "validation":
        from dataclasses import replace

        resolve = loop._setup.resolve

        def reject(*args):
            assert closed.is_set() and len(attempts.terminals) == 1
            raise failure

        class Setup:
            async def resolve(self, *args):
                return replace(await resolve(*args), validate_action=reject)

        loop._setup = Setup()
    with pytest.raises(AgentRuntimeError) as exc:
        await loop.decide(state, inputs, budget)
    if fault != "decode":
        assert exc.value is failure
    assert closed.is_set()
    assert len(attempts.terminals) == 1
    terminal = attempts.terminals[0]
    assert terminal.status == ("interrupted" if fault == "event" else "completed")
    if fault == "event":
        assert terminal.usage.status == "unknown"
        assert terminal.usage.reason == "collection_failed"


@pytest.mark.asyncio
async def test_close_audit_failure_does_not_replace_consumer_failure():
    loop, state, inputs, budget, backend, _ = build("answer")
    closed = asyncio.Event()
    original_stream = backend.stream

    async def stream(*args, **kwargs):
        try:
            async for event in original_stream(*args, **kwargs):
                yield event
        finally:
            closed.set()

    backend.stream = stream
    audit_error = RuntimeError("audit unavailable")

    class FailedAudit(Attempts):
        async def finish(self, request, terminal):
            raise audit_error

    loop._provider = ProviderInvoker(
        backend, capabilities=capabilities, attempts=FailedAudit(), clock=lambda: NOW
    )
    failure = runtime_failure(
        "CONTROL_PENDING", category="concurrency", public_message="Control accepted."
    )

    class Events:
        async def record(self, *args):
            raise failure

    loop._events = Events()
    with pytest.raises(AgentRuntimeError) as exc:
        await loop.decide(state, inputs, budget)
    assert exc.value is failure
    assert exc.value.__cause__ is audit_error
    assert closed.is_set()


@pytest.mark.asyncio
async def test_repeated_cancellation_cannot_leave_close_audit_running():
    loop, state, inputs, budget, backend, _ = build("answer")
    writing, release = asyncio.Event(), asyncio.Event()

    class SlowAudit(Attempts):
        async def finish(self, request, terminal):
            writing.set()
            await release.wait()
            await super().finish(request, terminal)

    attempts = SlowAudit()
    loop._provider = ProviderInvoker(
        backend, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )
    failure = runtime_failure(
        "CONTROL_PENDING", category="concurrency", public_message="Control accepted."
    )

    class Events:
        async def record(self, *args):
            raise failure

    loop._events = Events()
    task = asyncio.create_task(loop.decide(state, inputs, budget))
    await writing.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(AgentRuntimeError) as exc:
        await task
    assert exc.value is failure
    assert len(attempts.terminals) == 1
    assert attempts.terminals[0].status == "interrupted"

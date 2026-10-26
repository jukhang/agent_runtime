import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.model import ProviderCapabilities, ProviderRequest
from agent_runtime.contracts.provider import (
    ContentDelta,
    ProviderError,
    ProviderFinish,
    ToolArgumentsDelta,
    ToolCallComplete,
)
from agent_runtime.provider import AttemptTerminal, ProviderInvoker, _close_stream

NOW = datetime(2026, 9, 28, 10, tzinfo=UTC)


def make_request(*, deadline_seconds: float = 60):
    return ProviderRequest.model_validate(
        {
            "scopeId": "scope",
            "sessionId": str(uuid4()),
            "runId": str(uuid4()),
            "attemptId": str(uuid4()),
            "contextSnapshotId": str(uuid4()),
            "contextVersion": "1",
            "inputRef": "input",
            "inputDigest": "a" * 64,
            "estimatedInputTokens": 100,
            "budgetAllocationId": str(uuid4()),
            "deadlineAt": (NOW + timedelta(seconds=deadline_seconds)).isoformat(),
            "model": {
                "ref": {"key": {"namespace": "test", "key": "model"}, "revision": "1"},
                "connection": {
                    "key": {"namespace": "test", "key": "connection"},
                    "revision": "1",
                },
                "modelName": "model",
                "maxOutputTokens": 100,
                "thinking": "disabled",
                "providerOptions": {},
            },
            "tools": [],
            "outputSchema": None,
        }
    )


async def capabilities(request):
    return ProviderCapabilities(
        model=request.model.ref,
        streaming="supported",
        tool_calls="supported",
        structured_output="supported",
        thinking_control="supported",
        tools_with_structured_output="supported",
        tools_with_thinking="supported",
        usage="unknown",
        cancellation="supported",
        context_window=4096,
        max_output_tokens=1024,
        allowed_option_keys=(),
    )


class Attempts:
    def __init__(self, *, start: str = "started"):
        self.started = []
        self.terminals: list[AttemptTerminal] = []
        self.start = start

    async def begin(self, request, *, started_at):
        self.started.append((request.attempt_id, started_at))
        return self.start

    async def finish(self, request, terminal):
        self.terminals.append(terminal)


class Backend:
    def __init__(self, events):
        self.events = events
        self.calls = 0

    async def stream(self, request, *, cancel=None):
        self.calls += 1
        for event in self.events:
            yield event


async def collect(provider, request):
    return [event async for event in provider.stream(request)]


class CancelEvent:
    def __init__(self, *, cancelled: bool = False) -> None:
        self._event = asyncio.Event()
        if cancelled:
            self._event.set()

    def is_cancelled(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        await self._event.wait()

    def cancel(self) -> None:
        self._event.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("text", ("", " \n\t"))
async def test_empty_stop_is_persisted_failure_not_success(text):
    request = make_request()
    events = []
    if text:
        events.append(
            ContentDelta(
                attempt_id=request.attempt_id, sequence="0", kind="content.delta", text=text
            )
        )
    events.append(
        ProviderFinish(
            attempt_id=request.attempt_id,
            sequence=str(len(events)),
            kind="finish",
            reason="stop",
            raw_reason="stop",
        )
    )
    attempts = Attempts()
    result = await collect(
        ProviderInvoker(
            Backend(events), capabilities=capabilities, attempts=attempts, clock=lambda: NOW
        ),
        request,
    )
    assert result[-1].kind == "error"
    assert result[-1].failure == attempts.terminals[0].failure
    assert result[-1].failure.attempt_id == str(request.attempt_id)
    assert result[-1].failure.retryable
    assert attempts.terminals[0].status == "failed"


@pytest.mark.asyncio
async def test_content_filter_failure_is_not_transparent_retry():
    request = make_request()
    attempts = Attempts()
    result = await collect(
        ProviderInvoker(
            Backend(
                [
                    ProviderFinish(
                        attempt_id=request.attempt_id,
                        sequence="0",
                        kind="finish",
                        reason="content_filter",
                        raw_reason="content_filter",
                    )
                ]
            ),
            capabilities=capabilities,
            attempts=attempts,
            clock=lambda: NOW,
        ),
        request,
    )
    assert result[-1].kind == "error"
    assert not result[-1].failure.retryable


@pytest.mark.asyncio
async def test_success_is_persisted_once_with_unknown_usage():
    request = make_request()
    backend = Backend(
        [
            ContentDelta(
                attempt_id=request.attempt_id, sequence="0", kind="content.delta", text="hi"
            ),
            ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="finish",
                reason="stop",
                raw_reason="stop",
            ),
        ]
    )
    attempts = Attempts()
    invoker = ProviderInvoker(
        backend, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )
    events = await collect(invoker, request)
    assert [event.kind for event in events] == ["content.delta", "finish"]
    assert len(attempts.started) == 1
    assert len(attempts.terminals) == 1
    terminal = attempts.terminals[0]
    assert terminal.status == "completed"
    assert terminal.usage.status == "unknown"
    assert terminal.usage.reason == "provider_not_reported"


@pytest.mark.asyncio
async def test_success_closes_backend_stream_before_returning():
    request = make_request()
    closed = asyncio.Event()

    class TerminalBackend:
        async def stream(self, _request, *, cancel=None):
            try:
                yield ContentDelta(
                    attempt_id=request.attempt_id, sequence="0", kind="content.delta", text="OK"
                )
                yield ProviderFinish(
                    attempt_id=request.attempt_id,
                    sequence="1",
                    kind="finish",
                    reason="stop",
                    raw_reason="stop",
                )
                await asyncio.Event().wait()
            finally:
                closed.set()

    invoker = ProviderInvoker(
        TerminalBackend(), capabilities=capabilities, attempts=Attempts(), clock=lambda: NOW
    )
    assert (await collect(invoker, request))[-1].kind == "finish"
    assert closed.is_set()


@pytest.mark.asyncio
async def test_tool_completion_is_released_only_after_valid_finish():
    request = make_request()
    backend = Backend(
        [
            ToolArgumentsDelta(
                attempt_id=request.attempt_id,
                sequence="0",
                kind="tool.arguments.delta",
                call_id="call",
                text="{}",
            ),
            ToolCallComplete(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="tool.complete",
                call_id="call",
                name="tool",
                arguments={},
            ),
            ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="2",
                kind="finish",
                reason="tool_calls",
                raw_reason="tool_calls",
            ),
        ]
    )
    attempts = Attempts()
    invoker = ProviderInvoker(
        backend, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )
    events = await collect(invoker, request)
    assert [event.kind for event in events] == [
        "tool.arguments.delta",
        "tool.complete",
        "finish",
    ]
    assert [event.sequence for event in events] == ["0", "1", "2"]
    assert attempts.terminals[0].status == "completed"


@pytest.mark.asyncio
async def test_length_is_failed_and_does_not_release_completed_tool():
    request = make_request()
    backend = Backend(
        [
            ToolCallComplete(
                attempt_id=request.attempt_id,
                sequence="0",
                kind="tool.complete",
                call_id="call",
                name="tool",
                arguments={},
            ),
            ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="finish",
                reason="length",
                raw_reason="length",
            ),
        ]
    )
    attempts = Attempts()
    invoker = ProviderInvoker(
        backend, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )
    events = await collect(invoker, request)
    assert [event.kind for event in events] == ["error"]
    assert events[0].failure == attempts.terminals[0].failure
    assert events[0].failure.retryable
    assert attempts.terminals[0].status == "failed"
    assert attempts.terminals[0].failure.code == "INCOMPLETE_MODEL_OUTPUT"


@pytest.mark.asyncio
async def test_provider_error_is_persisted_and_returned():
    request = make_request()
    failure = FailureEnvelope(
        code="PROVIDER_TIMEOUT",
        category="provider",
        retryable=True,
        public_message="Timed out.",
    )
    backend = Backend(
        [
            ProviderError(
                attempt_id=request.attempt_id,
                sequence="0",
                kind="error",
                failure=failure,
            )
        ]
    )
    attempts = Attempts()
    invoker = ProviderInvoker(
        backend, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )
    events = await collect(invoker, request)
    assert events == backend.events
    assert attempts.terminals[0].status == "failed"
    assert attempts.terminals[0].failure == failure


@pytest.mark.asyncio
async def test_preflight_failure_does_not_create_attempt_or_call_network():
    request = make_request().model_copy(update={"deadline_at": NOW - timedelta(seconds=1)})
    backend = Backend([])
    attempts = Attempts()
    invoker = ProviderInvoker(
        backend, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )
    with pytest.raises(Exception) as exc:
        await collect(invoker, request)
    assert "deadline" in str(exc.value).lower()
    assert not attempts.started
    assert backend.calls == 0


@pytest.mark.asyncio
async def test_running_attempt_is_fenced_before_calling_backend():
    request = make_request()
    backend = Backend([])
    attempts = Attempts(start="already_running")
    invoker = ProviderInvoker(
        backend, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )
    with pytest.raises(AgentRuntimeError) as exc:
        await collect(invoker, request)
    assert exc.value.failure.code == "FENCED"
    assert backend.calls == 0


@pytest.mark.asyncio
async def test_pre_cancelled_attempt_is_settled_without_calling_backend():
    request = make_request()
    backend = Backend([])
    attempts = Attempts()
    invoker = ProviderInvoker(
        backend, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )
    events = [event async for event in invoker.stream(request, cancel=CancelEvent(cancelled=True))]
    assert [event.kind for event in events] == ["finish"]
    assert events[0].reason == "cancelled"
    assert attempts.terminals[0].status == "cancelled"
    assert backend.calls == 0


class EndlessBackend:
    def __init__(self) -> None:
        self.calls = 0
        self.closed = asyncio.Event()

    async def stream(self, request, *, cancel=None):
        self.calls += 1
        try:
            sequence = 0
            while True:
                await asyncio.sleep(0.002)
                yield ContentDelta(
                    attempt_id=request.attempt_id,
                    sequence=str(sequence),
                    kind="content.delta",
                    text="x",
                )
                sequence += 1
        finally:
            self.closed.set()


@pytest.mark.asyncio
async def test_absolute_deadline_stops_a_continuously_streaming_backend():
    request = make_request().model_copy(
        update={"deadline_at": datetime.now(UTC) + timedelta(milliseconds=30)}
    )
    backend = EndlessBackend()
    attempts = Attempts()
    invoker = ProviderInvoker(backend, capabilities=capabilities, attempts=attempts)
    events = await collect(invoker, request)
    assert events[-1].kind == "error"
    assert events[-1].failure.code == "PROVIDER_TIMEOUT"
    assert attempts.terminals[0].status == "failed"
    assert backend.closed.is_set()


@pytest.mark.asyncio
async def test_absolute_deadline_applies_after_consumer_pause():
    request = make_request().model_copy(
        update={"deadline_at": datetime.now(UTC) + timedelta(milliseconds=30)}
    )
    backend = Backend(
        [
            ContentDelta(
                attempt_id=request.attempt_id,
                sequence="0",
                kind="content.delta",
                text="first",
            ),
            ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="finish",
                reason="stop",
                raw_reason="stop",
            ),
        ]
    )
    attempts = Attempts()
    invoker = ProviderInvoker(backend, capabilities=capabilities, attempts=attempts)
    iterator = invoker.stream(request)
    assert (await anext(iterator)).kind == "content.delta"
    await asyncio.sleep(0.05)
    result = [event async for event in iterator]
    assert result[-1].kind == "error"
    assert result[-1].failure.code == "PROVIDER_TIMEOUT"
    assert attempts.terminals[0].status == "failed"


class ResolverLikeBlockingBackend:
    def __init__(self) -> None:
        self.calls = 0
        self.started = asyncio.Event()
        self.closed = asyncio.Event()

    async def stream(self, request, *, cancel=None):
        self.calls += 1
        self.started.set()
        try:
            await asyncio.Event().wait()
            if False:  # pragma: no cover - keeps this function an async generator
                yield None
        finally:
            self.closed.set()


@pytest.mark.asyncio
async def test_explicit_cancel_interrupts_a_blocked_backend_start():
    request = make_request()
    backend = ResolverLikeBlockingBackend()
    attempts = Attempts()
    cancel = CancelEvent()
    invoker = ProviderInvoker(
        backend, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )

    async def consume():
        return [event async for event in invoker.stream(request, cancel=cancel)]

    task = asyncio.create_task(consume())
    await backend.started.wait()
    cancel.cancel()
    events = await task
    assert events[-1].kind == "finish"
    assert events[-1].reason == "cancelled"
    assert attempts.terminals[0].status == "cancelled"
    assert backend.closed.is_set()


class BlockingBackend:
    async def stream(self, request, *, cancel=None):
        yield ContentDelta(
            attempt_id=request.attempt_id,
            sequence="0",
            kind="content.delta",
            text="started",
        )
        await asyncio.sleep(60)


@pytest.mark.asyncio
async def test_task_cancellation_is_persisted_then_reraised():
    request = make_request()
    attempts = Attempts()
    invoker = ProviderInvoker(
        BlockingBackend(), capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )

    async def consume():
        return [event async for event in invoker.stream(request)]

    task = asyncio.create_task(consume())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert attempts.terminals[0].status == "cancelled"


class SlowTerminalAttempts(Attempts):
    def __init__(self):
        super().__init__()
        self.finish_started = asyncio.Event()
        self.allow_finish = asyncio.Event()

    async def finish(self, request, terminal):
        self.finish_started.set()
        await self.allow_finish.wait()
        await super().finish(request, terminal)


@pytest.mark.asyncio
async def test_cancellation_during_terminal_write_does_not_leave_attempt_running():
    request = make_request()
    backend = Backend(
        [
            ContentDelta(
                attempt_id=request.attempt_id, sequence="0", kind="content.delta", text="OK"
            ),
            ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="finish",
                reason="stop",
                raw_reason="stop",
            ),
        ]
    )
    attempts = SlowTerminalAttempts()
    invoker = ProviderInvoker(
        backend, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )

    async def consume():
        return [event async for event in invoker.stream(request)]

    task = asyncio.create_task(consume())
    await attempts.finish_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    attempts.allow_finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(attempts.terminals) == 1
    assert attempts.terminals[0].status == "completed"


@pytest.mark.asyncio
async def test_repeated_cancellation_waits_for_terminal_write():
    request = make_request()
    attempts = SlowTerminalAttempts()
    backend = Backend(
        [
            ContentDelta(
                attempt_id=request.attempt_id, sequence="0", kind="content.delta", text="OK"
            ),
            ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="finish",
                reason="stop",
                raw_reason="stop",
            ),
        ]
    )
    invoker = ProviderInvoker(
        backend, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )

    async def consume():
        return [event async for event in invoker.stream(request)]

    task = asyncio.create_task(consume())
    await attempts.finish_started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    attempts.allow_finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(attempts.terminals) == 1
    assert attempts.terminals[0].status == "completed"


@pytest.mark.asyncio
async def test_repeated_cancellation_waits_for_stream_close():
    class SlowClose:
        def __init__(self):
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.closed = False

        async def aclose(self):
            self.started.set()
            await self.release.wait()
            self.closed = True

    stream = SlowClose()
    task = asyncio.create_task(_close_stream(stream))
    await stream.started.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    stream.release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stream.closed


@pytest.mark.asyncio
async def test_terminal_write_failure_is_reported_without_false_completion():
    request = make_request()

    class FailingAttempts(Attempts):
        def __init__(self):
            super().__init__()
            self.finish_calls = 0

        async def finish(self, _request, _terminal):
            self.finish_calls += 1
            raise RuntimeError("database unavailable")

    attempts = FailingAttempts()
    backend = Backend(
        [
            ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="0",
                kind="finish",
                reason="stop",
                raw_reason="stop",
            )
        ]
    )
    invoker = ProviderInvoker(
        backend, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )
    with pytest.raises(AgentRuntimeError) as exc:
        await collect(invoker, request)
    assert exc.value.failure.code == "INTERNAL"
    assert attempts.finish_calls == 1
    assert not attempts.terminals

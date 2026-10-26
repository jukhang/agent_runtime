"""Absolute network deadline and cancellation cleanup, without remote services."""

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from test_provider_invoker import Attempts, CancelEvent, capabilities
from test_provider_openai import build_provider, request_data

from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.provider import ProviderInvoker


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["headers", "first_chunk", "after_delta", "error_body"])
@pytest.mark.parametrize("with_cancel", [False, True])
async def test_absolute_deadline_interrupts_http_stall(phase, with_cancel):
    request = ProviderRequest.model_validate(request_data()).model_copy(
        update={"deadline_at": datetime.now(UTC) + timedelta(milliseconds=100)}
    )
    reading = asyncio.Event()
    stopped = asyncio.Event()
    closed = asyncio.Event()

    class StalledBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            try:
                if phase == "after_delta":
                    yield (
                        b'data: {"choices":[{"delta":{"content":"hello"},'
                        b'"finish_reason":null}]}\n\n'
                    )
                reading.set()
                await asyncio.Event().wait()
                yield b""  # pragma: no cover
            finally:
                stopped.set()

        async def aclose(self):
            closed.set()

    async def handler(_):
        if phase == "headers":
            try:
                reading.set()
                await asyncio.Event().wait()
            finally:
                stopped.set()
        return httpx.Response(
            400 if phase == "error_body" else 200,
            headers={"content-type": "text/event-stream"},
            stream=StalledBody(),
        )

    provider, old_client = build_provider("", request)
    await old_client.aclose()
    attempts = Attempts()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider._client = client
        invoker = ProviderInvoker(provider, capabilities=capabilities, attempts=attempts)

        async def consume():
            return [
                event
                async for event in invoker.stream(
                    request, cancel=CancelEvent() if with_cancel else None
                )
            ]

        events = await asyncio.wait_for(consume(), 2)
        assert reading.is_set() and stopped.is_set()
        if phase != "headers":
            assert closed.is_set()
        assert events[-1].failure.code == "PROVIDER_TIMEOUT"
        assert len(attempts.terminals) == 1
        assert attempts.terminals[0].status == "failed"
        assert attempts.terminals[0].usage.status == "unknown"
        assert attempts.terminals[0].finish_reason is None

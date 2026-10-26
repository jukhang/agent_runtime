"""The fault harness must close upstream and never leak usage after interruption."""

import asyncio

import httpx
import pytest
from runtime_provider_faults import InterruptingTransport


@pytest.mark.parametrize("fault", ("timeout", "disconnect"))
@pytest.mark.parametrize("chunk_size", (1, 4096))
def test_http_fault_is_after_content_and_before_usage(fault, chunk_size):
    closed = []
    injected = []
    data = (
        b'data: {"choices":[{"delta":{"role":"assistant"}}]}\n\n'
        b'data: {"choices":[{"delta":{"content":"391"}}]}\n\n'
        b'data: {"choices":[],"usage":{"total_tokens":20}}\n\n'
    )

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            for offset in range(0, len(data), chunk_size):
                yield data[offset : offset + chunk_size]

        async def aclose(self):
            closed.append(True)

    async def scenario():
        transport = InterruptingTransport(
            httpx.MockTransport(lambda request: httpx.Response(200, stream=Stream())),
            fault,
            injected,
        )
        received = b""
        error = httpx.ReadTimeout if fault == "timeout" else httpx.RemoteProtocolError
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(error):
                async with client.stream("POST", "http://test/completions") as response:
                    async for chunk in response.aiter_bytes():
                        received += chunk
        assert b"391" in received and b"usage" not in received
        assert injected == [fault] and closed

    asyncio.run(scenario())

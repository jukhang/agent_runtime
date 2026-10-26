"""Test-only HTTP fault injection after a genuine upstream content SSE frame."""

import json
from collections.abc import AsyncIterator
from typing import Literal

import httpx


class InterruptingTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        upstream: httpx.AsyncBaseTransport,
        fault: Literal["timeout", "disconnect"],
        injected: list[str],
    ) -> None:
        self.upstream = upstream
        self.fault = fault
        self.injected = injected

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self.upstream.handle_async_request(request)
        if response.status_code != 200:
            return response
        return httpx.Response(
            response.status_code,
            headers=response.headers,
            stream=_InterruptedStream(response.stream, request, self.fault, self.injected),
            extensions=response.extensions,
        )

    async def aclose(self) -> None:
        await self.upstream.aclose()


class _InterruptedStream(httpx.AsyncByteStream):
    def __init__(
        self,
        upstream: httpx.AsyncByteStream,
        request: httpx.Request,
        fault: Literal["timeout", "disconnect"],
        injected: list[str],
    ) -> None:
        self.upstream = upstream
        self.request = request
        self.fault = fault
        self.injected = injected

    async def __aiter__(self) -> AsyncIterator[bytes]:
        buffer = b""
        try:
            async for chunk in self.upstream:
                buffer += chunk
                while b"\n\n" in buffer:
                    frame, buffer = buffer.split(b"\n\n", 1)
                    yield frame + b"\n\n"
                    if self._has_content(frame):
                        self.injected.append(self.fault)
                        error = (
                            httpx.ReadTimeout
                            if self.fault == "timeout"
                            else httpx.RemoteProtocolError
                        )
                        raise error("Explicit F05 HTTP stream fault", request=self.request)
                if len(buffer) > 1024 * 1024:
                    raise AssertionError("fault probe SSE frame exceeds 1 MiB")
            if buffer:
                yield buffer
        finally:
            await self.upstream.aclose()

    def _has_content(self, frame: bytes) -> bool:
        for line in frame.splitlines():
            if not line.startswith(b"data:"):
                continue
            try:
                payload = json.loads(line[5:].strip())
            except ValueError:
                continue
            if isinstance(payload, dict):
                for choice in payload.get("choices", []):
                    if isinstance(choice, dict) and isinstance(choice.get("delta"), dict):
                        if choice["delta"].get("content"):
                            return True
        return False

    async def aclose(self) -> None:
        await self.upstream.aclose()

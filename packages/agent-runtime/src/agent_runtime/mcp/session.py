"""Standard MCP lifecycle using the optional official Python MCP client."""

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from pydantic import JsonValue, TypeAdapter

from agent_runtime.mcp.client import McpClient

if TYPE_CHECKING:
    import httpx


class SessionTransport:
    """One initialized session; never shared between different endpoints or principals."""

    def __init__(self, session: Any, *, timeout_seconds: float, expected_version: str | None):
        import anyio

        self._session = session
        self._timeout = timeout_seconds
        self._expected = expected_version
        self._lock = anyio.Lock()
        self._initialized: dict[str, JsonValue] | None = None

    async def initialize(self) -> dict[str, JsonValue]:
        import anyio

        async with self._lock:
            if self._initialized is None:
                with anyio.fail_after(self._timeout):
                    result = await self._session.initialize()
                # The official client rejects unsupported negotiated versions and sends initialized.
                if self._expected is not None and result.protocolVersion != self._expected:
                    raise ValueError("Negotiated MCP protocol differs from the pinned version")
                self._initialized = result.model_dump(mode="json", by_alias=True, exclude_none=True)
            return dict(self._initialized)

    async def request(self, method: str, params: Mapping[str, JsonValue]) -> JsonValue:
        import anyio

        if self._initialized is None:
            raise ValueError("MCP session must initialize before requests")
        with anyio.fail_after(self._timeout):
            if method == "tools/list":
                result = await self._session.list_tools(cursor=params.get("cursor"))
            elif method == "tools/call":
                result = await self._session.call_tool(params["name"], params.get("arguments", {}))
            else:
                raise ValueError("MCP method is not enabled by this adapter")
        return TypeAdapter(JsonValue).validate_python(
            result.model_dump(mode="json", by_alias=True, exclude_none=True)
        )


@asynccontextmanager
async def connect_mcp(
    url: str,
    *,
    http_client: "httpx.AsyncClient",
    allowed_tools: frozenset[str] = frozenset(),
    expected_version: str | None = None,
    timeout_seconds: float = 30,
    max_tools: int = 128,
) -> AsyncIterator[McpClient]:
    """Connect to a host-approved endpoint. Discovery alone grants no tool execution.

    The host owns client credentials, ACL and trusted egress. Use a Broker/gateway
    client where required; this transport is not an authorization or SSRF boundary.
    ToolHost must still authorize each call. No write retries or protocol fallback.
    """
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "MCP endpoint must be an explicit HTTP(S) URL without embedded credentials"
        )
    if http_client.follow_redirects:
        raise ValueError("MCP redirects must be disabled to protect endpoint and credentials")
    if not 0 < timeout_seconds <= 3600:
        raise ValueError("MCP timeout must be positive and bounded")
    async with streamable_http_client(url, http_client=http_client) as (read, write, _):
        async with ClientSession(
            read, write, read_timeout_seconds=timedelta(seconds=timeout_seconds)
        ) as session:
            transport = SessionTransport(
                session, timeout_seconds=timeout_seconds, expected_version=expected_version
            )
            client = McpClient(
                transport, calls={name: transport for name in allowed_tools}, max_tools=max_tools
            )
            await client.verify_server()
            try:
                yield client
            finally:
                client.close()

"""Strict stateless MCP 2026-07-28 JSON-RPC boundary."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from pydantic import JsonValue

from agent_runtime.contracts.tools import ToolInvocation
from agent_runtime.storage.tool_host import ToolEffect


class McpTransport(Protocol):
    async def request(self, method: str, params: Mapping[str, JsonValue]) -> JsonValue:
        """Use a pinned server endpoint and Credential Broker, not a model URL/token."""


@dataclass(frozen=True, slots=True)
class McpToolInfo:
    name: str
    description: str
    input_schema: dict[str, JsonValue]


class McpClient:
    def __init__(
        self,
        discovery: McpTransport,
        *,
        calls: Mapping[str, McpTransport] | None = None,
        max_tools: int = 128,
    ) -> None:
        if max_tools <= 0:
            raise ValueError("MCP tool limit must be positive")
        self._discovery, self._calls, self._max_tools = discovery, dict(calls or {}), max_tools

    async def verify_server(self) -> None:
        result = await self._discovery.request("server/discover", {})
        if not isinstance(result, dict) or result.get("resultType") != "complete":
            raise ValueError("MCP server did not complete modern discovery")
        versions = result.get("supportedVersions")
        capabilities = result.get("capabilities")
        if (
            not isinstance(versions, list)
            or "2026-07-28" not in versions
            or not isinstance(capabilities, dict)
            or not isinstance(capabilities.get("tools"), dict)
        ):
            raise ValueError("MCP server does not support pinned tools protocol")

    async def test_connection(self) -> tuple[McpToolInfo, ...]:
        await self.verify_server()
        return await self.list_tools()

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        result = await self._discovery.request("tools/list", {})
        if not isinstance(result, dict) or result.get("resultType") != "complete":
            raise ValueError("MCP server returned an invalid tool catalog")
        items = result.get("tools")
        if not isinstance(items, list):
            raise ValueError("MCP server returned an invalid tool catalog")
        if len(items) > self._max_tools or result.get("nextCursor"):
            raise ValueError("MCP tool catalog exceeded the pinned discovery limit")
        tools: list[McpToolInfo] = []
        seen: set[str] = set()
        for item in items:
            if not isinstance(item, dict):
                raise ValueError("MCP tool entry is invalid")
            name, schema = item.get("name"), item.get("inputSchema")
            description = item.get("description", "")
            if (
                not isinstance(name, str)
                or not name
                or not name.isascii()
                or len(name) > 128
                or any(not (char.isalnum() or char in "_.-/") for char in name)
                or name in seen
                or not isinstance(schema, dict)
                or schema.get("type") != "object"
                or not isinstance(description, str)
                or len(description) > 4000
            ):
                raise ValueError("MCP tool name or schema conflicts")
            seen.add(name)
            tools.append(McpToolInfo(name, description, schema))
        return tuple(tools)

    async def call_read(self, name: str, arguments: dict[str, JsonValue]) -> ToolEffect:
        transport = self._calls.get(name)
        if transport is None:
            raise ValueError("MCP tool has no pinned call transport")
        result = await transport.request("tools/call", {"name": name, "arguments": arguments})
        if not isinstance(result, dict) or result.get("resultType") != "complete":
            raise ValueError("MCP tool returned an invalid result")
        blocks = result.get("content")
        if not isinstance(blocks, list):
            raise ValueError("MCP tool returned an invalid result")
        if result.get("isError") is True:
            raise ValueError("MCP tool execution failed")
        if len(blocks) > 64:
            raise ValueError("MCP response exceeds its content block limit")
        texts: list[str] = []
        for block in blocks:
            if isinstance(block, dict) and block.get("type") == "text":
                value = block.get("text")
                if isinstance(value, str):
                    texts.append(value)
        summary = "\n".join(texts)[:4000]
        return ToolEffect(data=result, summary=summary or "MCP tool returned a result")


class McpReadTool:
    """Only read-only MCP tools may use this adapter; ToolHost still authorizes each call."""

    def __init__(self, client: McpClient, *, name: str) -> None:
        if not name:
            raise ValueError("MCP tool name is required")
        self._client, self._name = client, name

    async def invoke(self, invocation: ToolInvocation) -> ToolEffect:
        return await self._client.call_read(self._name, invocation.call.arguments)


__all__ = ["McpClient", "McpReadTool", "McpToolInfo", "McpTransport"]

"""Effectful MCP tools require a provider-enforced key and result lookup extension."""

from typing import Protocol

from pydantic import JsonValue, TypeAdapter

from agent_runtime.contracts.tools import ToolInvocation
from agent_runtime.mcp.client import McpTransport
from agent_runtime.storage.tool_host import ToolEffect


class McpEffectExtension(Protocol):
    async def call_once(
        self, *, name: str, arguments: dict[str, object], operation_key: str
    ) -> ToolEffect:
        """The remote server must atomically dedup operation_key before any effect."""

    async def lookup(self, *, name: str, operation_key: str) -> ToolEffect | None:
        """Query without re-sending the write. None means the effect is still unknown."""


class McpIdempotentWriteTool:
    """Use only for a server whose fixed revision advertises the extension above."""

    def __init__(self, *, name: str, remote: McpEffectExtension) -> None:
        if not name:
            raise ValueError("MCP write tool name is required")
        self._name, self._remote = name, remote

    async def dispatch(self, invocation: ToolInvocation, *, key: str) -> ToolEffect:
        if invocation.operation_id is None or key != str(invocation.operation_id):
            raise ValueError("MCP effect key must equal the persistent operation identity")
        return await self._remote.call_once(
            name=self._name, arguments=dict(invocation.call.arguments), operation_key=key
        )

    async def lookup(self, invocation: ToolInvocation, *, key: str) -> ToolEffect | None:
        if invocation.operation_id is None or key != str(invocation.operation_id):
            raise ValueError("MCP lookup key must equal the persistent operation identity")
        return await self._remote.lookup(name=self._name, operation_key=key)


class McpKeyedEffectClient:
    """One pinned effect tool and one independently pinned authoritative lookup tool.

    This is an opt-in server extension, not an MCP guarantee. The server must
    atomically deduplicate ``operation_key`` before applying any effect and the
    lookup tool must be read-only. The host owns both transports and grants.
    """

    def __init__(
        self,
        *,
        effect_name: str,
        lookup_name: str,
        effect_transport: McpTransport,
        lookup_transport: McpTransport,
    ) -> None:
        if not effect_name or not lookup_name or effect_name == lookup_name:
            raise ValueError("MCP effect and lookup names must be distinct")
        self._effect_name, self._lookup_name = effect_name, lookup_name
        self._effect_transport, self._lookup_transport = effect_transport, lookup_transport

    @staticmethod
    def _structured(value: JsonValue) -> dict[str, JsonValue]:
        if (
            not isinstance(value, dict)
            or value.get("resultType") != "complete"
            or value.get("isError") is True
        ):
            raise ValueError("MCP keyed effect returned an error")
        data = value.get("structuredContent")
        if not isinstance(data, dict):
            raise ValueError("MCP keyed effect requires structured content")
        return data

    async def call_once(
        self, *, name: str, arguments: dict[str, object], operation_key: str
    ) -> ToolEffect:
        if name != self._effect_name or not operation_key or "operation_key" in arguments:
            raise ValueError("MCP effect identity or reserved argument differs")
        safe_arguments = TypeAdapter(dict[str, JsonValue]).validate_python(arguments)
        result = self._structured(
            await self._effect_transport.request(
                "tools/call",
                {
                    "name": name,
                    "arguments": {**safe_arguments, "operation_key": operation_key},
                },
            )
        )
        if result.get("operation_key") != operation_key:
            raise ValueError("MCP effect response does not echo the operation identity")
        return ToolEffect(result, "MCP effect completed")

    async def lookup(self, *, name: str, operation_key: str) -> ToolEffect | None:
        if name != self._effect_name or not operation_key:
            raise ValueError("MCP lookup identity differs")
        result = self._structured(
            await self._lookup_transport.request(
                "tools/call",
                {
                    "name": self._lookup_name,
                    "arguments": {"operation_key": operation_key},
                },
            )
        )
        if result.get("operation_key") != operation_key:
            raise ValueError("MCP lookup response does not echo the operation identity")
        if result.get("found") is False:
            return None
        if result.get("found") is not True:
            raise ValueError("MCP lookup has no authoritative found state")
        return ToolEffect(result, "MCP effect reconciled")


__all__ = ["McpEffectExtension", "McpIdempotentWriteTool", "McpKeyedEffectClient"]

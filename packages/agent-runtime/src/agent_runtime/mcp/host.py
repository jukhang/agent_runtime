"""Bind pinned MCP tools to ToolHost without giving models destinations or grants."""

from collections.abc import Awaitable, Callable
from typing import Literal

from agent_runtime.contracts.credentials import GrantRef
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.tools import ToolInvocation, ToolSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.credentials.broker import CredentialBroker
from agent_runtime.credentials.jsonrpc import BrokerJsonRpcTransport
from agent_runtime.credentials.ports import BrokerBlobs
from agent_runtime.mcp.client import McpClient
from agent_runtime.mcp.write import McpIdempotentWriteTool, McpKeyedEffectClient
from agent_runtime.storage.tool_host import ToolEffect

type GrantIssuer = Callable[[ToolInvocation, CapabilityRef, str], Awaitable[GrantRef]]
type MaintenanceGrantIssuer = Callable[[str, CapabilityRef, str], Awaitable[GrantRef]]


class _McpBrokerBinding:
    def __init__(
        self,
        *,
        broker: CredentialBroker,
        blobs: BrokerBlobs,
        issue_grant: GrantIssuer,
    ) -> None:
        self._broker, self._blobs, self._issue_grant = broker, blobs, issue_grant

    async def _transport(
        self, invocation: ToolInvocation, destination: CapabilityRef, action: str
    ) -> BrokerJsonRpcTransport:
        grant = await self._issue_grant(invocation, destination, action)
        return BrokerJsonRpcTransport(
            broker=self._broker,
            blobs=self._blobs,
            actor=invocation.actor,
            grant=grant,
            action=action,
            mcp_client_info={"name": "centn-runtime", "version": "1.0"},
        )


class McpBrokerReadTool(_McpBrokerBinding):
    def __init__(
        self,
        *,
        spec: ToolSpec,
        source: CapabilityRef,
        broker: CredentialBroker,
        blobs: BrokerBlobs,
        issue_grant: GrantIssuer,
    ) -> None:
        super().__init__(broker=broker, blobs=blobs, issue_grant=issue_grant)
        if (
            spec.mcp is None
            or spec.source != source
            or spec.kind != "read"
            or spec.side_effect != "none"
        ):
            raise ValueError("MCP read handler differs from its pinned ToolSpec")
        self._name, self._destination = spec.mcp.tool_name, spec.mcp.destination

    async def invoke(self, invocation: ToolInvocation) -> ToolEffect:
        transport = await self._transport(invocation, self._destination, "read")
        client = McpClient(transport, calls={self._name: transport})
        return await client.call_read(self._name, invocation.call.arguments)


class McpBrokerEffectTool(_McpBrokerBinding):
    def __init__(
        self,
        *,
        spec: ToolSpec,
        source: CapabilityRef,
        broker: CredentialBroker,
        blobs: BrokerBlobs,
        issue_grant: GrantIssuer,
    ) -> None:
        super().__init__(broker=broker, blobs=blobs, issue_grant=issue_grant)
        if (
            spec.mcp is None
            or spec.source != source
            or spec.kind != "write"
            or spec.side_effect != "external"
            or spec.mcp.lookup_name is None
            or spec.mcp.lookup_destination is None
        ):
            raise ValueError("MCP effect handler differs from its pinned ToolSpec")
        self._name, self._effect_destination = spec.mcp.tool_name, spec.mcp.destination
        self._lookup_name, self._lookup_destination = (
            spec.mcp.lookup_name,
            spec.mcp.lookup_destination,
        )

    async def dispatch(self, invocation: ToolInvocation, *, key: str) -> ToolEffect:
        effect = await self._transport(invocation, self._effect_destination, "write")
        remote = McpKeyedEffectClient(
            effect_name=self._name,
            lookup_name=self._lookup_name,
            effect_transport=effect,
            lookup_transport=effect,
        )
        return await McpIdempotentWriteTool(name=self._name, remote=remote).dispatch(
            invocation, key=key
        )

    async def lookup(self, invocation: ToolInvocation, *, key: str) -> ToolEffect | None:
        lookup = await self._transport(invocation, self._lookup_destination, "read")
        remote = McpKeyedEffectClient(
            effect_name=self._name,
            lookup_name=self._lookup_name,
            effect_transport=lookup,
            lookup_transport=lookup,
        )
        return await McpIdempotentWriteTool(name=self._name, remote=remote).lookup(
            invocation, key=key
        )


class McpMaintenanceInspector:
    """Lookup/cancel a known effect under a separately fenced maintenance grant."""

    def __init__(
        self,
        *,
        spec: ToolSpec,
        actor: ActorContext,
        broker: CredentialBroker,
        blobs: BrokerBlobs,
        issue_grant: MaintenanceGrantIssuer,
    ) -> None:
        binding = spec.mcp
        if (
            binding is None
            or spec.side_effect != "external"
            or binding.lookup_name is None
            or binding.lookup_destination is None
        ):
            raise ValueError("maintenance requires a pinned MCP effect and lookup")
        self._binding = binding
        self._actor, self._broker, self._blobs = actor, broker, blobs
        self._issue_grant = issue_grant

    async def _call(
        self, *, key: str, name: str, destination: CapabilityRef, action: str
    ) -> dict[str, object]:
        grant = await self._issue_grant(key, destination, action)
        transport = BrokerJsonRpcTransport(
            broker=self._broker,
            blobs=self._blobs,
            actor=self._actor,
            grant=grant,
            action=action,
            mcp_client_info={"name": "centn-runtime", "version": "1.0"},
        )
        result = await transport.request(
            "tools/call", {"name": name, "arguments": {"operation_key": key}}
        )
        if (
            not isinstance(result, dict)
            or result.get("resultType") != "complete"
            or result.get("isError") is True
            or not isinstance(result.get("structuredContent"), dict)
        ):
            raise ValueError("maintenance MCP tool returned an invalid result")
        structured = result["structuredContent"]
        assert isinstance(structured, dict)
        structured_data: dict[str, object] = dict(structured)
        return structured_data

    async def inspect(
        self, remote_request_key: str
    ) -> Literal["running", "completed", "stopped", "unknown"]:
        assert self._binding.lookup_name is not None
        assert self._binding.lookup_destination is not None
        result = await self._call(
            key=remote_request_key,
            name=self._binding.lookup_name,
            destination=self._binding.lookup_destination,
            action="read",
        )
        if result.get("operation_key") != remote_request_key:
            raise ValueError("maintenance lookup identity changed")
        status = result.get("status")
        if status == "running":
            return "running"
        if status == "completed":
            return "completed"
        if status == "stopped":
            return "stopped"
        if status == "unknown":
            return "unknown"
        if result.get("found") is True:
            return "completed"
        if result.get("found") is False:
            return "unknown"
        raise ValueError("maintenance lookup has no authoritative state")

    async def cancel(self, remote_request_key: str) -> None:
        if self._binding.cancel_name is None or self._binding.cancel_destination is None:
            raise ValueError("MCP server has no pinned cancellation extension")
        result = await self._call(
            key=remote_request_key,
            name=self._binding.cancel_name,
            destination=self._binding.cancel_destination,
            action="write",
        )
        if result.get("operation_key") != remote_request_key or result.get("accepted") is not True:
            raise ValueError("MCP cancellation was not accepted by the server")


__all__ = ["McpBrokerEffectTool", "McpBrokerReadTool", "McpMaintenanceInspector"]

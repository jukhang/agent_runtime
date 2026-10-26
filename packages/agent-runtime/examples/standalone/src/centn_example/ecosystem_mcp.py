"""Pinned TLS MCP egress, with SQL grants bound to the currently claimed tool."""

import ssl
from uuid import uuid4

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.credentials import EgressDestination, ScopeRequest
from agent_runtime.credentials import CredentialBroker
from agent_runtime.credentials.https import OutboundProtocolProfile, PinnedHttpsTransport
from agent_runtime.credentials.jsonrpc import BrokerJsonRpcTransport
from agent_runtime.credentials.ports import FilteredBody
from agent_runtime.mcp.client import McpClient
from agent_runtime.mcp.host import McpBrokerReadTool
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.credential_blobs import SqlBrokerBlobs
from agent_runtime.storage.credential_broker import SqlCredentialGrantStore
from centn_example.tools import ref


def mcp_tool(host, actor, access):
    config = host.ecosystem.config
    spec = host.tools.ecosystem_specs[0]
    target = EgressDestination(
        destination=spec.mcp.destination,
        hostname="egress.test",
        port=config.mcp_port,
        action="read",
        method="POST",
        path="/mcp",
        private_addresses=("127.0.0.1",),
    )
    discovery = target.model_copy(update={"destination": ref("metrics-discovery")})
    targets = {item.destination: item for item in (target, discovery)}
    pending = {}

    async def authorize(db, caller, scope, request, phase):
        entry = pending.get(scope.tool_call_id)
        if (
            entry is None
            or caller != actor
            or scope.destination not in targets
            or scope.actions != ("read",)
        ):
            raise ValueError("MCP scope is not a live host invocation")
        invocation, resources = entry
        if (
            scope.run_id != invocation.run_id
            or scope.session_id != invocation.session_id
            or scope.input_digest != content_digest(invocation.call.arguments)
        ):
            raise ValueError("MCP grant binding changed")
        await access.authorize(db, invocation, resources)

    transport = PinnedHttpsTransport(
        tls=ssl.create_default_context(cafile=str(config.mcp_ca)),
        protocol_profiles={
            target.destination: OutboundProtocolProfile(kind="mcp_tool", tool_name="read_metrics"),
            discovery.destination: OutboundProtocolProfile(kind="mcp_discovery"),
        },
    )
    host.ecosystem.transports.append(transport)

    class Destinations:
        def resolve(self, capability):
            if capability not in targets:
                raise ValueError("MCP destination is not pinned")
            return targets[capability]

    class Vault:
        async def bearer(self, scope, capability):
            if scope != actor.scope_id or capability not in targets:
                raise ValueError("MCP credential owner changed")
            return config.mcp_token

    class Resolver:
        async def resolve(self, hostname, port):
            if (hostname, port) != (target.hostname, target.port):
                raise ValueError("MCP endpoint changed")
            return ("127.0.0.1",)

    class Filter:
        async def apply(self, destination, reply):
            if destination not in targets.values():
                raise ValueError("MCP response destination changed")
            return FilteredBody(reply.body, destination.destination, "allowed")

    blobs = SqlBrokerBlobs(SqlContextBlobBackend(host.engine, host.settings.schema), actor=actor)
    broker = CredentialBroker(
        store=SqlCredentialGrantStore(host.engine, host.settings.schema, authorize=authorize),
        vault=Vault(),
        blobs=blobs,
        registry=Destinations(),
        resolver=Resolver(),
        transport=transport,
        response_filter=Filter(),
    )

    async def issue(invocation, destination, action):
        if destination not in targets or action != "read":
            raise ValueError("MCP action changed")
        resources = await host.tools.resources(invocation, spec)
        pending[invocation.call.tool_call_id] = invocation, resources
        return await broker.authorize(
            actor,
            ScopeRequest(
                actor=actor,
                session_id=invocation.session_id,
                run_id=invocation.run_id,
                tool_call_id=invocation.call.tool_call_id,
                destination=destination,
                actions=(action,),
                policy_decision_id=uuid4(),
                input_digest=content_digest(invocation.call.arguments),
                expires_at=invocation.deadline_at,
                max_requests=2,
                max_response_bytes=16384,
            ),
        )

    read = McpBrokerReadTool(
        spec=spec, source=spec.source, broker=broker, blobs=blobs, issue_grant=issue
    )

    class DiscoveredRead:
        async def invoke(self, invocation):
            grant = await issue(invocation, discovery.destination, "read")
            protocol = BrokerJsonRpcTransport(
                broker=broker,
                blobs=blobs,
                actor=actor,
                grant=grant,
                action="read",
                max_response_bytes=16384,
                mcp_client_info={"name": "centn-ecosystem", "version": "1.0"},
            )
            catalog = await McpClient(protocol).test_connection()
            found = next((tool for tool in catalog if tool.name == spec.mcp.tool_name), None)
            if found is None or content_digest(found.input_schema) != content_digest(
                spec.mcp.input_schema
            ):
                raise ValueError("MCP catalog differs from the installed revision")
            return await read.invoke(invocation)

    return DiscoveredRead()

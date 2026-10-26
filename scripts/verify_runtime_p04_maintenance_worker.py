"""Disposable role-specific SQL worker process for the P04 process gate."""

import json
import os
import ssl
from contextlib import asynccontextmanager

import httpx
import sqlalchemy as sa
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.credentials import EgressDestination
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.tools import McpToolBinding, ToolSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.work_claims import WorkClaimRequest
from agent_runtime.credentials import CredentialBroker
from agent_runtime.credentials.https import OutboundProtocolProfile, PinnedHttpsTransport
from agent_runtime.credentials.ports import FilteredBody
from agent_runtime.storage.approval_wake import SqlApprovalWakeScanner
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.credential_blobs import SqlBrokerBlobs
from agent_runtime.storage.credential_broker import SqlCredentialGrantStore
from agent_runtime.storage.execution_supervisor import SqlExecutionSupervisor
from agent_runtime.storage.mcp_maintenance import SqlMcpMaintenanceInspectorFactory
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.uow.capacity import SqlSystemCapacityStore
from agent_runtime.storage.uow.schema import require_schema
from agent_runtime.storage.worker_topology import SqlWorkerTopology
from agent_runtime.worker import WorkerConfig
from sqlalchemy.ext.asyncio import create_async_engine


class NetworkRemote:
    def __init__(self, client: httpx.AsyncClient, origin: str) -> None:
        if not origin.startswith("http://127.0.0.1:"):
            raise ValueError("the disposable remote must bind loopback")
        self._client, self._origin = client, origin.rstrip("/")

    async def inspect(self, key: str) -> str:
        if not key:
            raise ValueError("remote request key is missing")
        response = await self._client.get(f"{self._origin}/executions/{key}")
        response.raise_for_status()
        value = response.json()["state"]
        if value not in {"running", "completed", "stopped", "unknown"}:
            raise ValueError("remote returned an unknown execution state")
        return value

    async def cancel(self, key: str) -> None:
        response = await self._client.post(f"{self._origin}/executions/{key}/cancel")
        response.raise_for_status()


def claim(scope: str, role: str) -> WorkClaimRequest:
    lanes = ["main", "control"] if role == "main" else [role]
    return WorkClaimRequest.model_validate(
        {
            "scopeId": scope,
            "worker": {
                "workerId": f"p04-{role}-{os.getpid()}",
                "runtimeRevision": {
                    "key": {"namespace": "builtin", "key": "runtime"},
                    "revision": "1",
                },
                "capabilities": [],
            },
            "leaseSeconds": int(os.environ.get("RUNTIME_TEST_LEASE_SECONDS", "30")),
            "lanes": lanes,
            "maintenanceKinds": ["reconcile_execution"] if role == "maintenance" else [],
        }
    )


def _mcp_ref(key: str) -> CapabilityRef:
    return CapabilityRef.model_validate(
        {"key": {"namespace": "test.mcp", "key": key}, "revision": "1"}
    )


@asynccontextmanager
async def mcp_inspector_factory(engine, schema: str, scope: str):
    port = int(os.environ["RUNTIME_TEST_MCP_PORT"])
    actor = ActorContext.model_validate(json.loads(os.environ["RUNTIME_TEST_MCP_ACTOR"]))
    lookup, cancel = _mcp_ref("maintenance-lookup"), _mcp_ref("maintenance-cancel")
    targets = {
        ref: EgressDestination(
            destination=ref, hostname="mcp.test", port=port,
            action="read" if ref == lookup else "write", method="POST", path="/mcp",
            private_addresses=("127.0.0.1",),
        )
        for ref in (lookup, cancel)
    }
    input_schema, output_schema = {"type": "object"}, {"type": "object"}
    binding = McpToolBinding(
        protocol_version="2026-07-28", tool_name="increment_counter",
        destination=_mcp_ref("maintenance-effect"),
        input_schema=input_schema, output_schema=output_schema,
        lookup_name="lookup_effect", lookup_destination=lookup,
        cancel_name="cancel_effect", cancel_destination=cancel,
    )
    spec = ToolSpec(
        ref=_mcp_ref("effect"), description="Test MCP effect", source=_mcp_ref("server"),
        kind="write", side_effect="external",
        input_schema=CapabilityRef.model_validate({"key": {
            "namespace": "mcp.input", "key": content_digest(input_schema)}, "revision": "1"}),
        output_schema=CapabilityRef.model_validate({"key": {
            "namespace": "mcp.output", "key": content_digest(output_schema)}, "revision": "1"}),
        permissions=(_mcp_ref("permission"),), approval_policy=_mcp_ref("approval"),
        idempotency="provider_key", timeout_seconds=30, concurrency_group="mcp",
        max_concurrency=1, cancellation="stop", public_result_policy="full", mcp=binding,
    )

    async def authorize(db, caller, credential_scope, request, phase):
        if (
            caller != actor or caller.scope_id != scope
            or credential_scope.maintenance_lease is None
            or credential_scope.destination not in targets
            or credential_scope.actions != (targets[credential_scope.destination].action,)
        ):
            raise ValueError("maintenance credential has an unpinned destination")

    class Vault:
        async def bearer(self, scope_id, destination):
            if scope_id != scope or destination not in targets:
                raise ValueError("MCP vault destination is not pinned")
            return os.environ["RUNTIME_TEST_MCP_TOKEN"]

    class Registry:
        def resolve(self, destination):
            return targets[destination]

    class Resolver:
        async def resolve(self, hostname, requested_port):
            if hostname != "mcp.test" or requested_port != port:
                raise ValueError("MCP endpoint is not pinned")
            return ("127.0.0.1",)

    class Filter:
        async def apply(self, target, reply):
            return FilteredBody(reply.body, target.destination, "allowed")

    transport = PinnedHttpsTransport(
        tls=ssl.create_default_context(cafile=os.environ["RUNTIME_TEST_MCP_CERT"]),
        protocol_profiles={
            lookup: OutboundProtocolProfile(kind="mcp_tool", tool_name="lookup_effect"),
            cancel: OutboundProtocolProfile(kind="mcp_tool", tool_name="cancel_effect"),
        },
    )
    blobs = SqlBrokerBlobs(SqlContextBlobBackend(engine, schema), actor=actor)
    broker = CredentialBroker(
        store=SqlCredentialGrantStore(engine, schema, authorize=authorize),
        vault=Vault(), blobs=blobs, registry=Registry(), resolver=Resolver(),
        transport=transport, response_filter=Filter(),
    )
    try:
        yield SqlMcpMaintenanceInspectorFactory(
            engine, schema, actor=actor, spec=spec, broker=broker, blobs=blobs
        )
    finally:
        await transport.aclose()


@asynccontextmanager
async def bootstrap():
    schema = require_schema(os.environ["RUNTIME_TEST_SCHEMA"])
    scope = os.environ["RUNTIME_TEST_SCOPE"]
    deployment = os.environ["RUNTIME_TEST_DEPLOYMENT"]
    role = os.environ["RUNTIME_TEST_ROLE"]
    dsn = os.environ["AGENT_RUNTIME_TEST_DSN"]
    if sa.make_url(dsn).database != "aletai_wangxh":
        raise ValueError("P04 maintenance probe requires aletai_wangxh")
    engine = create_async_engine(dsn, pool_pre_ping=True)
    try:
        topology = SqlWorkerTopology(engine, schema, scope_id=scope)
        request = claim(scope, role)
        config = WorkerConfig(concurrency=1, idle_sleep_seconds=0.1)
        if role == "maintenance":
            async with httpx.AsyncClient(timeout=3, trust_env=False) as client:
                capacity = SqlToolCapacity(
                    SqlSystemCapacityStore(engine, schema), deployment_id=deployment
                )
                @asynccontextmanager
                async def inspector_binding():
                    if os.environ.get("RUNTIME_TEST_MCP_PORT"):
                        async with mcp_inspector_factory(engine, schema, scope) as factory:
                            yield factory
                    else:
                        yield None

                async with inspector_binding() as factory:
                    supervisor = SqlExecutionSupervisor(
                        engine,
                        schema,
                        inspector=NetworkRemote(
                            client, os.environ.get("RUNTIME_TEST_REMOTE_URL", "http://127.0.0.1:1")
                        ),
                        inspector_for_work=factory,
                        capacity=capacity,
                    )
                    yield topology.maintenance(
                        request,
                        execution=supervisor,
                        approval_wakes=SqlApprovalWakeScanner(engine, schema),
                        config=config,
                        recovery_interval_seconds=0.2,
                    )
        elif role == "main":
            class MainHandlers:
                async def open(self, actor):
                    raise AssertionError("disposable liveness probe must not claim main work")

            yield topology.main(request, handlers=MainHandlers(), config=config)
        elif role == "child":
            class ChildHandler:
                async def __call__(self, work, lease):
                    raise AssertionError("disposable liveness probe must not claim child work")

            class ChildHandlers:
                async def open(self, actor, work, lease):
                    return ChildHandler()

            class ChildErrors:
                async def __call__(self, work, lease, error):
                    raise error

            yield topology.child(
                request, handlers=ChildHandlers(), errors=ChildErrors(), config=config
            )
        else:
            raise ValueError("unknown worker role")
    finally:
        await engine.dispose()

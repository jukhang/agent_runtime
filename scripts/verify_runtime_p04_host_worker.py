"""Isolated, revision-pinned host used by the R02 SQL process acceptance test."""

import os
import ssl
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import sqlalchemy as sa
from agent_runtime.agent import Agent
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.credentials import EgressDestination, ScopeRequest
from agent_runtime.contracts.model import ModelConfig, ProviderCapabilities
from agent_runtime.contracts.policy import PolicyAllow
from agent_runtime.contracts.references import CapabilityRef, ResourceRef, ResourceScope
from agent_runtime.contracts.tools import ToolSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.credentials import CredentialBroker
from agent_runtime.credentials.https import OutboundProtocolProfile, PinnedHttpsTransport
from agent_runtime.credentials.ports import FilteredBody
from agent_runtime.mcp.host import McpBrokerReadTool
from agent_runtime.mcp.install import McpToolSchemas
from agent_runtime.policy_validation import policy_input_digest
from agent_runtime.providers.openai_compatible import OpenAIAdapterProfile, ProviderConnection
from agent_runtime.storage.admission import AdmissionAgent
from agent_runtime.storage.approval_wake import SqlApprovalWakeScanner
from agent_runtime.storage.approvals import SqlApprovalService
from agent_runtime.storage.child_factory import (
    ChildAgentProfile,
    ChildHostPorts,
    SqlChildHandlerFactory,
)
from agent_runtime.storage.child_tools import SqlChildToolAuthorizer
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.context_sources import SqlRuntimeSourceReader
from agent_runtime.storage.credential_blobs import SqlBrokerBlobs
from agent_runtime.storage.credential_broker import SqlCredentialGrantStore
from agent_runtime.storage.execution_supervisor import SqlExecutionSupervisor
from agent_runtime.storage.host_tool_access import SqlHostToolAccess
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.main_factory import (
    MainAgentProfile,
    MainHostPorts,
    SqlMainHandlerFactory,
)
from agent_runtime.storage.recovery import SqlRecovery
from agent_runtime.storage.subagents import SqlSubagentRuntime
from agent_runtime.storage.tool_assembly import SqlToolRuntimeAssembly
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.tool_execution_gate import SqlToolExecutionGate
from agent_runtime.storage.tool_host import SqlToolHost, ToolEffect
from agent_runtime.storage.uow.capacity import SqlSystemCapacityStore
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.storage.worker_topology import SqlWorkerTopology
from agent_runtime.tool_names import provider_tool_name
from agent_runtime.worker import WorkerConfig
from sqlalchemy.ext.asyncio import create_async_engine

from scripts.verify_runtime_p04_maintenance_worker import NetworkRemote, claim


class Estimator:
    def count(self, value: str) -> int:
        return len(value.encode("utf-8"))


class RegisteredTool:
    def __init__(self, engine, schema: str, spec: ToolSpec) -> None:
        self._engine, self._schema, self._spec = engine, schema, spec

    async def definition(self, ref: CapabilityRef):
        if ref != self._spec.ref:
            raise ValueError("unregistered tool")
        return {
            "type": "function",
            "function": {
                "name": provider_tool_name(ref),
                "description": self._spec.description,
                "parameters": self._spec.mcp.input_schema
                if self._spec.mcp
                else {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
            },
        }

    async def get(self, call):
        if call.tool != self._spec.ref:
            raise ValueError("unregistered tool")
        return self._spec

    async def resources(self, invocation, spec):
        if spec != self._spec:
            raise ValueError("tool revision changed")
        async with self._engine.connect() as db:
            turn = (
                await db.execute(
                    scoped(
                        "SELECT id FROM {t}turn WHERE scope_id=:scope AND primary_run_id=:run",
                        self._schema,
                    ),
                    {"scope": invocation.actor.scope_id, "run": invocation.run_id},
                )
            ).scalar_one()
        return (
            ResourceRef(
                resource_type="turn_input",
                resource_id=str(turn),
                resource_version="0",
                scope=ResourceScope(
                    scope_id=invocation.actor.scope_id,
                    session_id=str(invocation.session_id),
                    owner_id=invocation.actor.actor_id,
                ),
            ),
        )

    async def evaluate(self, request):
        return PolicyAllow(
            decision="allow",
            decision_id=uuid4(),
            policy=request.policy,
            input_digest=policy_input_digest(request),
            evidence_refs=request.resources,
            evaluated_at=datetime.now(UTC),
            expires_at=request.invocation.deadline_at,
            grant_ref="host-read-policy",
        )


class Schemas:
    def input(self, spec, arguments):
        if arguments:
            raise ValueError("host read tool accepts no arguments")

    def output(self, spec, data):
        if data != {"value": 5837}:
            raise ValueError("host read result changed")


class ReadCounter:
    async def invoke(self, invocation):
        return ToolEffect(data={"value": 5837}, summary="Counter read")


class Verifier:
    async def verify_in_transaction(self, db, request, proposal):
        from agent_runtime.contracts.subagents import VerifiedResult

        return VerifiedResult(
            child=request.child,
            proposal_id=request.proposal_id,
            proposal_revision="1",
            acceptance_spec=request.acceptance_spec,
            disposition="accepted",
            evidence_refs=proposal.evidence_refs,
            result_refs=proposal.artifact_refs,
            public_reason="Child result is sealed and accepted.",
        )


class HostPorts:
    def __init__(self, engine, schema: str, *, deployment: str, spec: ToolSpec) -> None:
        self._engine, self._schema = engine, schema
        self._deployment, self._spec = deployment, spec
        self._mcp_transport = None

    async def aclose(self):
        if self._mcp_transport is not None:
            await self._mcp_transport.aclose()

    def _mcp_read(self, actor, authorize):
        binding = self._spec.mcp
        if binding is None:
            return ReadCounter()
        target = EgressDestination(
            destination=binding.destination,
            hostname="mcp.test",
            port=int(os.environ["RUNTIME_TEST_MCP_PORT"]),
            action="read",
            method="POST",
            path="/mcp",
            private_addresses=("127.0.0.1",),
        )
        pending = {}

        async def authorize_credential(db, caller, scope, request, phase):
            if (
                caller != actor
                or scope.destination != target.destination
                or scope.actions != ("read",)
                or scope.tool_call_id is None
            ):
                raise ValueError("MCP credential scope is not a live read tool")
            binding_state = pending.get(scope.tool_call_id)
            if binding_state is None:
                raise ValueError("MCP invocation is not active in this Worker")
            invocation, resources = binding_state
            if (
                scope.run_id != invocation.run_id
                or scope.session_id != invocation.session_id
                or scope.input_digest != content_digest(invocation.call.arguments)
            ):
                raise ValueError("MCP credential input or run changed")
            await authorize(db, invocation, resources)

        transport = PinnedHttpsTransport(
            tls=ssl.create_default_context(cafile=os.environ["RUNTIME_TEST_MCP_CERT"]),
            protocol_profiles={
                binding.destination: OutboundProtocolProfile(
                    kind="mcp_tool", tool_name=binding.tool_name
                )
            },
        )
        self._mcp_transport = transport

        class Registry:
            def resolve(self, ref):
                if ref != binding.destination:
                    raise ValueError("MCP destination is not pinned")
                return target

        class Vault:
            async def bearer(self, scope_id, ref):
                if scope_id != actor.scope_id or ref != binding.destination:
                    raise ValueError("MCP credential is not scoped")
                return os.environ["RUNTIME_TEST_MCP_TOKEN"]

        class Resolver:
            async def resolve(self, hostname, port):
                if hostname != target.hostname or port != target.port:
                    raise ValueError("MCP endpoint changed")
                return ("127.0.0.1",)

        class Filter:
            async def apply(self, destination, reply):
                return FilteredBody(reply.body, destination.destination, "allowed")

        blobs = SqlBrokerBlobs(SqlContextBlobBackend(self._engine, self._schema), actor=actor)
        broker = CredentialBroker(
            store=SqlCredentialGrantStore(
                self._engine, self._schema, authorize=authorize_credential
            ),
            vault=Vault(),
            blobs=blobs,
            registry=Registry(),
            resolver=Resolver(),
            transport=transport,
            response_filter=Filter(),
        )

        async def issue(invocation, destination, action):
            if destination != binding.destination or action != "read":
                raise ValueError("MCP read scope changed")
            resources = await RegisteredTool(self._engine, self._schema, self._spec).resources(
                invocation, self._spec
            )
            pending[invocation.call.tool_call_id] = (invocation, resources)
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
                    max_response_bytes=4096,
                ),
            )

        return McpBrokerReadTool(
            spec=self._spec,
            source=self._spec.source,
            broker=broker,
            blobs=blobs,
            issue_grant=issue,
        )

    async def _open(self, actor: ActorContext, guard=None, lease=None):
        reader = SqlRuntimeSourceReader(
            self._engine,
            self._schema,
            blobs=SqlContextBlobBackend(self._engine, self._schema),
        )
        access = SqlHostToolAccess(self._schema, actor, reader, lease=lease)
        if guard is None:
            authorize = access.authorize
        else:
            child_access = SqlChildToolAuthorizer(self._schema, actor, reader)

            async def authorize(db, invocation, resources):
                await child_access.require_in_transaction(db, guard(), invocation, resources)

        registry = RegisteredTool(self._engine, self._schema, self._spec)
        capacity = SqlToolCapacity(
            SqlSystemCapacityStore(self._engine, self._schema),
            deployment_id=self._deployment,
        )
        host = SqlToolHost(
            self._engine,
            self._schema,
            specs=registry,
            resources=registry,
            schemas=McpToolSchemas() if self._spec.mcp else Schemas(),
            authorize=authorize,
            capacity=capacity,
            execution_gate=SqlToolExecutionGate(
                self._schema, approval_required=lambda invocation, spec, refs: False
            ),
            read_tools={self._spec.ref: self._mcp_read(actor, authorize)},
        )
        approval = SqlApprovalService(
            self._engine,
            self._schema,
            access=SqlHostToolAccess(self._schema, actor, reader),
        )
        subagents = SqlSubagentRuntime(
            self._engine,
            self._schema,
            actor=actor,
            journal=SqlJournal(self._engine, self._schema, actor),
            resources=reader,
            policies={},
            message_predicate=ref("message"),
            verifier=Verifier(),
        )
        return reader, registry, host, SqlToolRuntimeAssembly(host, capacity), approval, subagents

    async def open(self, actor: ActorContext, lease) -> MainHostPorts:
        reader, registry, host, assembly, approval, subagents = await self._open(actor, lease=lease)
        return MainHostPorts(
            definitions=registry,
            specs=registry,
            tool_resources=registry,
            policy=registry,
            tools=host,
            resources=reader,
            subagents=subagents,
            tool_runtime=assembly,
            approvals=approval,
        )


class ChildPorts(HostPorts):
    async def open(self, actor: ActorContext, guard) -> ChildHostPorts:
        _, registry, host, assembly, approval, subagents = await self._open(actor, guard)
        return ChildHostPorts(
            definitions=registry,
            specs=registry,
            tool_resources=registry,
            policy=registry,
            read_handlers={},
            subagents=subagents,
            effect_tools=host,
            tool_runtime=assembly,
            approvals=approval,
        )


def ref(key: str) -> CapabilityRef:
    return CapabilityRef.model_validate(
        {"key": {"namespace": "builtin", "key": key}, "revision": "1"}
    )


def profile(role: str):
    agent = Agent.model_validate_json(os.environ[f"RUNTIME_TEST_{role}_AGENT"])
    revision_id = UUID(os.environ[f"RUNTIME_TEST_{role}_REVISION"])
    agent_ref = CapabilityRef.model_validate_json(os.environ[f"RUNTIME_TEST_{role}_REF"])
    model = ModelConfig.model_validate_json(os.environ[f"RUNTIME_TEST_{role}_MODEL"])
    installed = AdmissionAgent(agent_ref, revision_id, agent, 100000)
    if role == "MAIN":
        return MainAgentProfile(installed, model, ref("profile"), ref("prompt"))
    return ChildAgentProfile(installed, model, ref("profile"), ref("prompt"))


@asynccontextmanager
async def bootstrap():
    schema = require_schema(os.environ["RUNTIME_TEST_SCHEMA"])
    scope = os.environ["RUNTIME_TEST_SCOPE"]
    deployment = os.environ["RUNTIME_TEST_DEPLOYMENT"]
    role = os.environ["RUNTIME_TEST_ROLE"]
    dsn = os.environ["AGENT_RUNTIME_TEST_DSN"]
    if sa.make_url(dsn).database != "aletai_wangxh":
        raise ValueError("R02 host acceptance requires isolated aletai_wangxh")
    engine = create_async_engine(dsn, pool_pre_ping=True)
    print(f"r02-host role={role} pid={os.getpid()} started", flush=True)
    ports = None
    async with httpx.AsyncClient(timeout=20, trust_env=False) as client:
        try:
            topology = SqlWorkerTopology(engine, schema, scope_id=scope)
            request = claim(scope, role)
            config = WorkerConfig(concurrency=1, idle_sleep_seconds=0.1)
            spec = ToolSpec.model_validate_json(os.environ["RUNTIME_TEST_TOOL_SPEC"])
            ports = HostPorts(engine, schema, deployment=deployment, spec=spec)

            async def connection(request):
                return ProviderConnection(
                    base_url=os.environ["RUNTIME_TEST_PROVIDER_URL"], api_key="local-test"
                )

            async def capabilities(request):
                return ProviderCapabilities(
                    model=request.model.ref,
                    streaming="supported",
                    tool_calls="supported",
                    structured_output="supported",
                    thinking_control="supported",
                    tools_with_structured_output="unknown",
                    tools_with_thinking="supported",
                    usage="supported",
                    cancellation="supported",
                    context_window=262144,
                    max_output_tokens=65536,
                    allowed_option_keys=("chat_template_kwargs",),
                )

            if role == "main":
                main = profile("MAIN")
                factory = SqlMainHandlerFactory(
                    engine,
                    schema,
                    client=client,
                    estimator=Estimator(),
                    agents={(scope, main.installed.revision_id): main},
                    ports=ports,
                    connections=connection,
                    capabilities=capabilities,
                    adapter=OpenAIAdapterProfile.qwen(),
                    allowed_predicates=(ref("message"),),
                    message_predicate=ref("message"),
                )
                yield topology.main(request, handlers=factory, config=config)
            elif role == "child":
                child = profile("CHILD")
                ports = ChildPorts(engine, schema, deployment=deployment, spec=spec)
                factory = SqlChildHandlerFactory(
                    engine,
                    schema,
                    client=client,
                    estimator=Estimator(),
                    agents={(scope, child.installed.revision_id): child},
                    ports=ports,
                    connections=connection,
                    capabilities=capabilities,
                    adapter=OpenAIAdapterProfile.qwen(),
                )

                async def child_error(work, lease, error):
                    await SqlRecovery(engine, schema).handle_worker_failure(work, error)

                yield topology.child(request, handlers=factory, errors=child_error, config=config)
            elif role == "maintenance":
                capacity = SqlToolCapacity(
                    SqlSystemCapacityStore(engine, schema), deployment_id=deployment
                )
                supervisor = SqlExecutionSupervisor(
                    engine,
                    schema,
                    inspector=NetworkRemote(
                        client, os.environ.get("RUNTIME_TEST_REMOTE_URL", "http://127.0.0.1:1")
                    ),
                    capacity=capacity,
                )
                yield topology.maintenance(
                    request,
                    execution=supervisor,
                    approval_wakes=SqlApprovalWakeScanner(engine, schema),
                    config=config,
                    recovery_interval_seconds=0.2,
                )
            else:
                raise ValueError("unknown host role")
        finally:
            if ports is not None:
                await ports.aclose()
            await engine.dispose()
            print(f"r02-host role={role} pid={os.getpid()} stopped", flush=True)

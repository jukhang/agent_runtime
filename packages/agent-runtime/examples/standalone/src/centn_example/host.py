"""Independent host assembly. API acceptance and Worker execution share only SQL."""

import os
import traceback
from pathlib import Path
from uuid import uuid4

import httpx
from sqlalchemy.ext.asyncio import create_async_engine

from agent_runtime import Agent, Runtime
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.model import ModelConfig, ProviderCapabilities
from agent_runtime.contracts.registry import CapabilityManifest, ToolCapability
from agent_runtime.contracts.scheduler import ClaimedWork, WorkerCapabilities
from agent_runtime.contracts.transactions import ActorContext, CommitGuard
from agent_runtime.contracts.work_claims import WorkClaimRequest
from agent_runtime.providers.openai_compatible import OpenAIAdapterProfile, ProviderConnection
from agent_runtime.storage.admission import AdmissionAgent, SqlAdmission
from agent_runtime.storage.agent_install import SqlAgentInstaller
from agent_runtime.storage.approval_wake import SqlApprovalWakeScanner
from agent_runtime.storage.approvals import SqlApprovalService
from agent_runtime.storage.capability_registry import SqlCapabilityRegistry
from agent_runtime.storage.child_factory import (
    ChildAgentProfile,
    ChildHostPorts,
    SqlChildHandlerFactory,
)
from agent_runtime.storage.host_tool_access import SqlHostToolAccess
from agent_runtime.storage.interactions import SqlInteractionService
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.main_factory import (
    MainAgentProfile,
    MainHostPorts,
    SqlMainHandlerFactory,
)
from agent_runtime.storage.main_run_loader import SqlMainRunLoader
from agent_runtime.storage.planning import (
    INITIAL_PLAN_SCHEMA,
    PLAN_REVIEW_SCHEMA,
    SqlPlanningSetupFactory,
)
from agent_runtime.storage.run_backend import SqlRunBackend
from agent_runtime.storage.sessions import SqlSessionStore
from agent_runtime.storage.skill_extensions import SqlSkillExtensionResolver
from agent_runtime.storage.subagents import SqlSubagentRuntime
from agent_runtime.storage.tool_assembly import SqlToolRuntimeAssembly
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.tool_execution_gate import SqlToolExecutionGate
from agent_runtime.storage.tool_host import SqlToolHost
from agent_runtime.storage.uow import SqlSystemCapacityStore, errors
from agent_runtime.storage.worker_topology import SqlWorkerTopology
from agent_runtime.worker import WorkerConfig
from centn_example.approvals import Approvals
from centn_example.compression import build_compressed_context
from centn_example.delegation import (
    DELEGATE_REVIEW_SCHEMA,
    DELEGATE_SCHEMA,
    DELEGATE_WAIT_SCHEMA,
    Delegation,
    SealedReview,
)
from centn_example.ecosystem import Ecosystem
from centn_example.ecosystem_catalog import EcosystemCatalog
from centn_example.ecosystem_config import EcosystemSettings
from centn_example.ecosystem_inventory import InventoryMaintenanceService
from centn_example.ecosystem_mcp import mcp_tool
from centn_example.resources import Resources, Sources
from centn_example.tools import ReportWriter, ToolPolicy, Tools, ref
from centn_example.waits import EXTERNAL, WAIT_SCHEMA, ExternalWait, HostWakeScanner


class Estimator:
    def count(self, text):
        # Conservative upper bound, not a provider token billing estimate.
        return len(text.encode("utf-8"))


class RejectUnconfiguredChild:
    async def verify_in_transaction(self, *args):
        errors.forbidden("No child Agent is installed in this example")


class Host:
    def __init__(self, settings):
        self.settings = settings
        self.engine = create_async_engine(
            settings.dsn,
            pool_pre_ping=True,
            connect_args={
                "server_settings": {
                    "statement_timeout": "15000",
                    "idle_in_transaction_session_timeout": "15000",
                }
            },
        )
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(240, connect=10),
            trust_env=False,
            limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
        )
        self.tools = Tools(self.engine, settings.schema)
        ecosystem = EcosystemSettings.from_env()
        self.ecosystem = None
        if ecosystem is not None:
            self.tools = EcosystemCatalog(self.tools)
        self.resources = Resources(self.engine, settings.schema)
        self.sources = Sources(self.resources)
        if ecosystem is not None:
            self.ecosystem = Ecosystem(self, ecosystem)
        self.approvals = Approvals(self.resources, self.sources)
        self.delegation = Delegation(self)
        self.external_wait = ExternalWait(self)
        self.backend = SqlRunBackend(
            self.engine, settings.schema, include_analysis=settings.thinking
        )
        self.sessions = SqlSessionStore(
            self.engine, settings.schema, authorize=self.authorize_actor
        )
        self.runtime = None
        self.installed = None

    def actor(self):
        return ActorContext(
            scope_id=self.settings.scope,
            actor_id=self.settings.actor,
            auth_revision="1",
            grants_ref="example-owner",
        )

    async def authorize_actor(self, db, actor):
        if actor != self.actor():
            errors.forbidden("Unknown or disabled host principal")

    async def initialize(self):
        model_key = "qwen-thinking" if self.settings.thinking else "qwen"
        model = ModelConfig(
            ref=ref(model_key),
            connection=ref("provider"),
            model_name=self.settings.model,
            max_output_tokens=8192,
            thinking="enabled" if self.settings.thinking else "disabled",
            provider_options={},
        )
        agent_id = "assistant-thinking" if self.settings.thinking else "assistant"
        if self.settings.agent_profile == "planner":
            agent_id = "planner-thinking" if self.settings.thinking else "planner"
        if self.settings.agent_profile == "external_wait":
            agent_id = "external-wait-thinking" if self.settings.thinking else "external-wait"
        if self.settings.agent_profile == "delegate":
            agent_id = "review-thinking" if self.settings.thinking else "review"
            reviewer = Agent(
                id="reviewer",
                name="Independent reviewer",
                model=model.ref,
                prompt=ref("prompt"),
                tools=(),
                instructions=(
                    "Review the assigned facts and calculations independently. "
                    "Use no tools, do not invent evidence, explain uncertainty. "
                    "Answer the user's language in no more than 150 words.",
                ),
                budget={"max_model_calls": 4, "max_tool_calls": 0, "max_runtime_seconds": 300},
            )
            revision = await SqlAgentInstaller(
                self.engine, self.settings.schema, namespace="example"
            ).install(self.settings.scope, ref("reviewer"), reviewer)
            self.child = AdmissionAgent(ref("reviewer"), revision, reviewer, 32768)
        agent = Agent(
            id=agent_id,
            name="centn Planner" if self.settings.agent_profile == "planner" else "centn Assistant",
            model=model.ref,
            prompt=ref("prompt"),
            instructions=(
                "Answer the user's actual question using the conversation context. "
                "Use the multiply tool for requested numerical multiplication. "
                "Do not invent tool results. Ask only for information necessary "
                "to satisfy the request. Reply in the user's language. "
                "Use the least side-effectful operation that satisfies the requested outcome. "
                "Answering or calculating is not a request to save or publish: only request "
                "persistence when a persisted artifact is part of the user's requested outcome."
                + (
                    " The installed report Skill is executed only by chart-report. "
                    "Its argv is an array containing exactly one JSON-encoded string of "
                    "read-metrics structuredContent. That tool persists both output files. "
                    "save-report is a separate approval-protected text writer, "
                    "not the Skill runner."
                    if self.ecosystem
                    else ""
                ),
            ),
            tools=tuple(spec.ref for spec in self.tools.specs),
            skills=(self.ecosystem.config.skill.ref,) if self.ecosystem else (),
            subagents=(ref("reviewer"),) if self.settings.agent_profile == "delegate" else (),
            add_datetime_to_context=True,
            timezone="Asia/Shanghai",
        )
        identity = await SqlAgentInstaller(
            self.engine, self.settings.schema, namespace="example"
        ).install(self.settings.scope, ref(agent.id), agent)
        self.installed = AdmissionAgent(
            ref(agent.id), identity, agent, self.settings.context_input_tokens
        )
        self.model = model

        host = self

        class RegistryAuthority:
            async def manage(self, actor, capability):
                await host.authorize_actor(None, actor)
                allowed = {spec.ref for spec in host.tools.specs}
                if host.ecosystem:
                    allowed.add(host.ecosystem.config.skill.ref)
                if capability not in allowed:
                    errors.forbidden("Unconfigured capability")

            async def use_tool(self, actor, run_id, capability):
                await self.manage(
                    actor.model_copy(update={"grants_ref": "example-owner"}), capability
                )
                from datetime import UTC, datetime

                from agent_runtime.storage.uow import locks
                from agent_runtime.storage.uow.authority import revalidate_authority
                from agent_runtime.storage.uow.schema import scoped

                async with host.resources.sessions() as db, db.begin():
                    row = (
                        (
                            await db.execute(
                                scoped(
                                    "SELECT * FROM {t}run WHERE scope_id=:scope AND id=:run",
                                    host.settings.schema,
                                ),
                                {"scope": actor.scope_id, "actor": actor.actor_id, "run": run_id},
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if row is None:
                        errors.forbidden("Tool Run is unavailable")
                    sql = locks.build_lock_statements(host.settings.schema)
                    await revalidate_authority(db, sql, actor, run=row, now=datetime.now(UTC))
                    grant = await locks.current_grant(
                        db, sql, actor.scope_id, run_id, actor.actor_id
                    )
                    if grant is None or capability.model_dump(
                        mode="json", by_alias=True
                    ) not in grant["allowed_scope"].get("tools", []):
                        errors.forbidden("Tool is outside the current Run grant")
                    await host.sources.authorize_session_in_transaction(
                        db, actor, row["session_id"]
                    )

        registry = SqlCapabilityRegistry(
            self.engine, self.settings.schema, authority=RegistryAuthority()
        )
        self.registry = registry
        if self.ecosystem:
            await self.ecosystem.install(registry)
        for spec in self.tools.specs:
            await registry.install(
                self.actor(),
                CapabilityManifest(
                    ref=spec.ref,
                    status="draft",
                    spec=ToolCapability(kind="tool", spec=spec),
                    content_digest=content_digest(spec.model_dump(mode="json", by_alias=True)),
                ),
            )
            await registry.publish(self.actor(), spec.ref)
        slots = SqlToolCapacity(
            SqlSystemCapacityStore(self.engine, self.settings.schema), deployment_id="centn-example"
        )
        # Configuration, not per-call pool creation. Conflicting limits are rejected
        # by Registry and every executor checks the pinned limit again.
        for spec in self.tools.specs:
            await slots._store.seed(
                slots.key(self.settings.scope, spec), capacity=spec.max_concurrency
            )

        async def authorize(db, actor, request, installed):
            await self.authorize_actor(db, actor)
            for part in request.inputs:
                if part.kind == "resource":
                    await self.sources.authorize_resource_in_transaction(
                        db, actor, part.resource_ref, session_id=request.session_id
                    )
            return installed == self.installed

        admission = SqlAdmission(
            self.engine,
            self.settings.schema,
            agents={(self.settings.scope, self.installed.ref): self.installed},
            authorize=authorize,
        )
        self.runtime = Runtime(
            admission=admission,
            backend=self.backend,
            interactions=SqlInteractionService(self.engine, self.settings.schema),
        )

    async def open(self, actor, lease):
        await self.authorize_actor(None, actor.model_copy(update={"grants_ref": "example-owner"}))
        reader = self.sources
        access = SqlHostToolAccess(self.settings.schema, actor, reader, lease=lease)
        approvals = SqlApprovalService(self.engine, self.settings.schema, access=access)
        policies = {}
        if self.settings.agent_profile == "delegate":
            policy = await self.delegation.policy(lease().run_id)
            policies[policy.agent] = policy
        capacity = SqlToolCapacity(
            SqlSystemCapacityStore(self.engine, self.settings.schema), deployment_id="centn-example"
        )
        gate = SqlToolExecutionGate(
            self.settings.schema,
            approval_required=lambda invocation, spec, resources: spec.ref == ref("save-report"),
        )
        specs = self.tools
        reads = {self.tools.spec.ref: self.tools}
        background = {}
        collector = None
        if self.ecosystem:
            specs, script = self.ecosystem.script(actor, lease, access, capacity, gate)
            reads[ref("read-metrics")] = mcp_tool(self, actor, access)
            background[ref("chart-report")] = script
            collector = self.ecosystem.collector(actor, capacity)
        tools = SqlToolHost(
            self.engine,
            self.settings.schema,
            specs=specs,
            resources=self.tools,
            schemas=self.tools.schemas,
            authorize=access.authorize,
            capacity=capacity,
            execution_gate=gate,
            read_tools=reads,
            background_tools=background,
            database_tools={ref("save-report"): ReportWriter(self.resources)},
        )
        return MainHostPorts(
            definitions=self.tools,
            specs=specs,
            tool_resources=self.tools,
            policy=ToolPolicy(self.tools, approvals),
            tools=tools,
            resources=reader,
            tool_runtime=SqlToolRuntimeAssembly(tools, capacity, results=collector),
            approvals=approvals,
            context=build_compressed_context(self, actor, lease),
            subagents=SqlSubagentRuntime(
                self.engine,
                self.settings.schema,
                actor=actor,
                journal=SqlJournal(self.engine, self.settings.schema, actor),
                resources=reader,
                policies=policies,
                message_predicate=ref("message"),
                verifier=SealedReview(self.settings.schema, reader)
                if policies
                else RejectUnconfiguredChild(),
            ),
        )

    async def worker(self, *, child=False):
        if self.installed is None:
            raise RuntimeError("Initialize the host before starting a Worker")

        async def connection(request):
            if request.model != self.model:
                errors.forbidden("Model is not pinned to this host")
            return ProviderConnection(
                base_url=self.settings.provider_url, api_key=self.settings.provider_key
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
                max_output_tokens=32768,
                allowed_option_keys=("chat_template_kwargs",),
            )

        if child:
            if self.settings.agent_profile != "delegate":
                raise ValueError("Child Worker requires the delegate profile")
            host = self

            class ChildPorts:
                async def open(self, actor, guard):
                    return ChildHostPorts(
                        definitions=host.tools,
                        specs=host.tools,
                        tool_resources=host.tools,
                        policy=host.tools,
                        read_handlers={},
                        subagents=SqlSubagentRuntime(
                            host.engine,
                            host.settings.schema,
                            actor=actor,
                            journal=SqlJournal(host.engine, host.settings.schema, actor),
                            resources=host.sources,
                            policies={},
                            message_predicate=ref("message"),
                            verifier=RejectUnconfiguredChild(),
                        ),
                    )

            factory = SqlChildHandlerFactory(
                self.engine,
                self.settings.schema,
                client=self.client,
                estimator=Estimator(),
                agents={
                    (self.settings.scope, self.child.revision_id): ChildAgentProfile(
                        self.child, self.model, ref("child-profile"), ref("prompt")
                    )
                },
                ports=ChildPorts(),
                connections=connection,
                capabilities=capabilities,
                adapter=OpenAIAdapterProfile.qwen(),
            )
            request = WorkClaimRequest(
                scope_id=self.settings.scope,
                worker=WorkerCapabilities(
                    worker_id=f"example-child-{os.getpid()}-{uuid4().hex[:8]}",
                    runtime_revision=ref("worker"),
                    capabilities=(),
                ),
                lease_seconds=30,
                lanes=("child",),
                maintenance_kinds=(),
            )

            async def child_error(work, lease, error):
                actor = self.actor().model_copy(
                    update={"grants_ref": f"run-grant:{work.lease.run_id}:1"}
                )
                await SqlSubagentRuntime(
                    self.engine,
                    self.settings.schema,
                    actor=actor,
                    journal=SqlJournal(self.engine, self.settings.schema, actor),
                    resources=self.sources,
                    policies={},
                    message_predicate=ref("message"),
                    verifier=RejectUnconfiguredChild(),
                ).handle_worker_failure(work, lease, error)

            return SqlWorkerTopology(
                self.engine, self.settings.schema, scope_id=self.settings.scope
            ).child(
                request, handlers=factory, errors=child_error, config=WorkerConfig(concurrency=2)
            )

        planner = SqlPlanningSetupFactory(self.engine, self.settings.schema)
        profile_ref = (
            ref("planner-profile") if self.settings.agent_profile == "planner" else ref("profile")
        )
        setups, formats = {}, {}
        if self.settings.agent_profile == "planner":
            setups[profile_ref] = planner
            formats = {
                INITIAL_PLAN_SCHEMA: planner.output_format,
                PLAN_REVIEW_SCHEMA: planner.output_format,
            }
        elif self.settings.agent_profile == "delegate":
            setups[profile_ref] = self.delegation
            formats = {
                DELEGATE_SCHEMA: self.delegation.output_format,
                DELEGATE_REVIEW_SCHEMA: self.delegation.output_format,
                DELEGATE_WAIT_SCHEMA: self.delegation.output_format,
            }
        elif self.settings.agent_profile == "external_wait":
            setups[profile_ref] = self.external_wait
            formats = {WAIT_SCHEMA: self.external_wait.output_format}
        factory = SqlMainHandlerFactory(
            self.engine,
            self.settings.schema,
            client=self.client,
            estimator=Estimator(),
            agents={
                (self.settings.scope, self.installed.revision_id): MainAgentProfile(
                    self.installed,
                    self.model,
                    profile_ref,
                    ref("prompt"),
                    max_input_tokens=self.settings.context_input_tokens,
                )
            },
            ports=self,
            connections=connection,
            capabilities=capabilities,
            adapter=OpenAIAdapterProfile.qwen(),
            allowed_predicates=(ref("message"), ref("execution"), EXTERNAL)
            if self.settings.agent_profile == "external_wait"
            else (ref("message"), ref("execution")),
            message_predicate=ref("message"),
            execution_predicate=ref("execution"),
            setups=setups,
            output_formats=formats,
            extensions=SqlSkillExtensionResolver(self.ecosystem.store) if self.ecosystem else None,
        )

        host = self

        class ObservedFactory:
            async def open(self, actor, work, lease):
                handler = await factory.open(actor, work, lease)

                async def run(current, handle):
                    try:
                        if host.settings.agent_profile == "delegate" and isinstance(
                            current, ClaimedWork
                        ):
                            await SqlMainRunLoader(
                                host.engine, host.settings.schema, actor=actor, agents={}
                            ).apply_notifications(
                                CommitGuard(
                                    actor=actor,
                                    lease=handle.current(),
                                    expected_state_version=current.state_version,
                                )
                            )
                            ports = await host.open(actor, handle.current)
                            if await host.delegation.collect(actor, handle, ports.subagents):
                                return
                        await handler(current, handle)
                    except Exception as exc:
                        frames = [
                            f"{Path(frame.filename).name}:{frame.lineno}:{frame.name}"
                            for frame in traceback.extract_tb(exc.__traceback__)
                        ]
                        # Only type and locations; never args, headers or user text.
                        print({"errorType": type(exc).__name__, "frames": frames}, flush=True)
                        raise

                return run

        request = WorkClaimRequest(
            scope_id=self.settings.scope,
            worker=WorkerCapabilities(
                worker_id=f"example-{os.getpid()}-{uuid4().hex[:8]}",
                runtime_revision=ref("worker"),
                capabilities=(),
            ),
            lease_seconds=30,
            lanes=("main", "control"),
            maintenance_kinds=(),
        )
        return SqlWorkerTopology(
            self.engine, self.settings.schema, scope_id=self.settings.scope
        ).main(
            request,
            handlers=ObservedFactory(),
            config=WorkerConfig(concurrency=4),
        )

    async def aclose(self):
        if self.ecosystem:
            await self.ecosystem.aclose()
        await self.client.aclose()
        await self.engine.dispose()

    async def maintenance(self):
        async def approval_resources(db, actor, session_id, resources):
            for resource in resources:
                await self.sources.authorize_resource_in_transaction(
                    db, actor, resource, session_id=session_id
                )

        async def reject_unconfigured(work):
            raise ValueError("No external execution is installed in this host")

        request = WorkClaimRequest(
            scope_id=self.settings.scope,
            worker=WorkerCapabilities(
                worker_id=f"example-maintenance-{os.getpid()}-{uuid4().hex[:8]}",
                runtime_revision=ref("worker"),
                capabilities=(),
            ),
            lease_seconds=30,
            lanes=("maintenance",),
            maintenance_kinds=("reconcile_execution",),
        )
        service = SqlWorkerTopology(
            self.engine, self.settings.schema, scope_id=self.settings.scope
        ).maintenance(
            request,
            execution=self.ecosystem.maintenance(
                SqlToolCapacity(
                    SqlSystemCapacityStore(self.engine, self.settings.schema),
                    deployment_id="centn-example",
                )
            )
            if self.ecosystem
            else reject_unconfigured,
            approval_wakes=HostWakeScanner(
                self,
                SqlApprovalWakeScanner(
                    self.engine, self.settings.schema, resource_validity=approval_resources
                ),
            ),
            recovery_interval_seconds=1,
        )
        return InventoryMaintenanceService(service, self.ecosystem) if self.ecosystem else service

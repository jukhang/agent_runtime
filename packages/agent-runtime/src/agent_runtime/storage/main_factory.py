"""Concrete per-claim assembly over process-owned clients and installed Agents."""

import hashlib
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

import httpx
from pydantic import JsonValue, TypeAdapter
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.action_dispatch import (
    AuthorizedToolExecutor,
    ToolResourceResolver,
    ToolSpecResolver,
)
from agent_runtime.agent_loop import LoopSetup
from agent_runtime.context_runtime import TokenEstimator
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.context import PreparedContext
from agent_runtime.contracts.decisions import Finish
from agent_runtime.contracts.model import ModelConfig, ProviderRequest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.scheduler import ClaimedWork
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.contracts.work_claims import WorkClaimResult
from agent_runtime.native_controls import ASK_USER_CONTROL
from agent_runtime.ports import Policy
from agent_runtime.provider import CapabilityResolver
from agent_runtime.providers.context_input import (
    ContextOpenAIInputResolver,
    OutputFormatFactory,
    ToolDefinitionReader,
)
from agent_runtime.providers.openai_compatible import (
    ConnectionResolver,
    OpenAIAdapterProfile,
    OpenAICompatibleProvider,
)
from agent_runtime.skills.git import VerifiedSkillBundle
from agent_runtime.storage.admission import AdmissionAgent
from agent_runtime.storage.approvals import SqlApprovalService
from agent_runtime.storage.context_access import TransactionalResourceAccess
from agent_runtime.storage.context_factory import SqlContextComponents, build_sql_context_components
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.main_run_loader import MainAgentBinding
from agent_runtime.storage.main_setup import MAIN_DECISION_SCHEMA, SqlMainLoopSetup
from agent_runtime.storage.main_stream import SqlMainOutputFactory
from agent_runtime.storage.main_worker import SqlMainWorkerHandler
from agent_runtime.storage.scheduler import SqlScheduler
from agent_runtime.storage.subagents import SqlSubagentRuntime
from agent_runtime.storage.tool_assembly import SqlToolRuntimeAssembly
from agent_runtime.storage.tool_host import SqlToolHost
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.worker import LeaseHandle

CONTROL_DECISIONS: TypeAdapter[Finish] = TypeAdapter(Finish)


@dataclass(frozen=True, slots=True)
class MainHostPorts:
    """Host effects remain explicit; no default-allow policy or fallback executor."""

    definitions: ToolDefinitionReader
    specs: ToolSpecResolver
    tool_resources: ToolResourceResolver
    policy: Policy
    tools: AuthorizedToolExecutor
    resources: TransactionalResourceAccess
    subagents: SqlSubagentRuntime
    tool_runtime: SqlToolRuntimeAssembly | None = None
    approvals: SqlApprovalService | None = None
    context: SqlContextComponents | None = None


@dataclass(frozen=True, slots=True)
class MainAgentProfile:
    installed: AdmissionAgent
    model: ModelConfig
    profile: CapabilityRef
    prompt: CapabilityRef
    max_input_tokens: int = 16384
    history_items: int = 12


class MainPortsFactory(Protocol):
    async def open(self, actor: ActorContext, lease: Callable[[], LeaseToken]) -> MainHostPorts:
        """Resolve currently authorized host capabilities for this claim's actor."""


class MainExtensionResolver(Protocol):
    async def skills(
        self, actor: ActorContext, run_id: UUID, profile: MainAgentProfile
    ) -> Mapping[CapabilityRef, VerifiedSkillBundle]:
        """Return only installed, pinned Skill bundles for the claimed Agent."""
        ...


class MainSetupFactory(Protocol):
    """Trusted host profile; SQL committers still authorize every model decision."""

    def validate(self, profile: MainAgentProfile) -> None:
        """Reject unsupported Agent dependencies before the Worker accepts traffic."""
        ...

    def create(
        self,
        actor: ActorContext,
        profile: MainAgentProfile,
        skills: Mapping[CapabilityRef, str],
    ) -> LoopSetup: ...


class SqlMainHandlerFactory:
    """Reusable factory. The process owns engine/client and must close both on exit."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        client: httpx.AsyncClient,
        estimator: TokenEstimator,
        agents: Mapping[tuple[str, UUID], MainAgentProfile],
        ports: MainPortsFactory,
        connections: ConnectionResolver,
        capabilities: CapabilityResolver,
        adapter: OpenAIAdapterProfile,
        allowed_predicates: Collection[CapabilityRef],
        message_predicate: CapabilityRef | None = None,
        human_wait_seconds: int | None = None,
        execution_predicate: CapabilityRef | None = None,
        extensions: MainExtensionResolver | None = None,
        setups: Mapping[CapabilityRef, MainSetupFactory] | None = None,
        output_formats: Mapping[CapabilityRef, Mapping[str, JsonValue] | OutputFormatFactory]
        | None = None,
    ) -> None:
        self._engine, self._schema, self._client = engine, require_schema(schema), client
        self._estimator, self._agents, self._ports = estimator, dict(agents), ports
        self._connections, self._capabilities, self._adapter = connections, capabilities, adapter
        self._predicates, self._message = tuple(allowed_predicates), message_predicate
        self._execution_predicate = execution_predicate
        from agent_runtime.storage.scheduler import SchedulerConfig

        self._scheduler_config = SchedulerConfig(human_wait_seconds=human_wait_seconds)
        self._extensions = extensions
        self._setups = dict(setups or {})
        self._formats = dict(output_formats or {})
        if MAIN_DECISION_SCHEMA in self._formats:
            raise ValueError("Custom profiles cannot replace the built-in review schema")
        if set(self._setups) - {profile.profile for profile in self._agents.values()}:
            raise ValueError("Setup profile is not bound to any installed Agent")
        for (_, revision_id), profile in self._agents.items():
            if revision_id != profile.installed.revision_id:
                raise ValueError("Agent profile revision differs from its installation")
            if profile.installed.agent.model != profile.model.ref:
                raise ValueError("Agent model profile is not pinned")
            agent = profile.installed.agent
            if ASK_USER_CONTROL in agent.tools:
                raise ValueError("Runtime controls cannot be registered as business Tools")
            if agent.prompt is not None and agent.prompt != profile.prompt:
                raise ValueError("Agent prompt profile is not pinned")
            custom = self._setups.get(profile.profile)
            if (agent.skills or agent.hooks) and extensions is None:
                raise ValueError("Agent Skills/Hooks require a host binding: extension resolver")
            if custom is not None:
                custom.validate(profile)
            elif (
                agent.knowledge
                or agent.subagents
                or ((agent.skills or agent.hooks) and extensions is None)
                or agent.policy
                or agent.context is not None
                or agent.memory is not None
                or agent.input_schema is not None
                or agent.output_schema is not None
            ):
                raise ValueError("Agent requires a host binding not supported by this factory")

    async def open(
        self, actor: ActorContext, work: WorkClaimResult, lease: LeaseHandle
    ) -> SqlMainWorkerHandler:
        profiles = {
            revision: profile
            for (scope, revision), profile in self._agents.items()
            if scope == actor.scope_id
        }
        if not profiles:
            errors.forbidden("No Agent profiles are installed in this scope")
        if isinstance(work, ClaimedWork):
            profile = await self._profile_for_run(actor, work.lease.run_id)
            profiles = {profile.installed.revision_id: profile}
        ports = await self._ports.open(actor, lease.current)
        if isinstance(ports.tools, SqlToolHost):
            if ports.tool_runtime is None or ports.tool_runtime.host is not ports.tools:
                raise ValueError("SQL ToolHost requires its terminal-capacity runtime assembly")
            if ports.approvals is None:
                raise ValueError("SQL ToolHost requires a durable approval service")
        elif ports.tool_runtime is not None:
            raise ValueError("tool runtime assembly does not match the active tool executor")
        instructions: dict[UUID, dict[CapabilityRef, str]] = {}
        for revision, profile in profiles.items():
            agent = profile.installed.agent
            if not isinstance(work, ClaimedWork):
                instructions[revision] = {}
                continue
            if not agent.skills and not agent.hooks:
                if isinstance(ports.tools, SqlToolHost) and ports.tools.installed_hook_refs:
                    raise ValueError("ToolHost hooks are not bound to this Agent")
                instructions[revision] = {}
                continue
            assert self._extensions is not None
            bundles = await self._extensions.skills(actor, work.lease.run_id, profile)
            if set(bundles) != set(agent.skills):
                raise ValueError("Agent Skill bundles do not match installed references")
            if agent.hooks:
                if not isinstance(ports.tools, SqlToolHost) or (
                    ports.tools.installed_hook_refs != frozenset(agent.hooks)
                ):
                    raise ValueError("Agent hooks do not match the active ToolHost")
            elif isinstance(ports.tools, SqlToolHost) and ports.tools.installed_hook_refs:
                raise ValueError("Unbound ToolHost hooks cannot run for this Agent")
            selected: dict[CapabilityRef, str] = {}
            total_bytes = 0
            for ref in agent.skills:
                bundle = bundles[ref]
                if bundle.spec.ref != ref:
                    raise ValueError("Skill bundle revision differs from the Agent binding")
                if not set(bundle.spec.required_tools) <= set(agent.tools):
                    raise ValueError("Skill requires Tools outside the installed Agent binding")
                instruction = next(
                    (
                        item
                        for item in bundle.spec.files
                        if item.path == bundle.spec.instruction_path
                    ),
                    None,
                )
                raw = bundle.files.get(bundle.spec.instruction_path)
                if (
                    instruction is None
                    or raw is None
                    or len(raw) != instruction.size_bytes
                    or hashlib.sha256(raw).hexdigest() != instruction.digest
                ):
                    raise ValueError("Skill instruction content differs from its pinned manifest")
                total_bytes += len(raw)
                if total_bytes > 32768:
                    raise ValueError("Agent Skill instructions exceed the context limit")
                selected[ref] = raw.decode("utf-8", errors="strict")
            instructions[revision] = selected
        context = ports.context or build_sql_context_components(
            self._engine, self._schema, estimator=self._estimator, optional_history=True
        )
        provider = OpenAICompatibleProvider(
            self._client,
            inputs=ContextOpenAIInputResolver(
                context=context.runtime,
                actor=actor,
                access=context.access,
                tools=ports.definitions,
                output_formats={MAIN_DECISION_SCHEMA: self._decision_format, **self._formats},
            ),
            connections=self._connections,
            profile=self._adapter,
        )
        bindings = {
            revision: MainAgentBinding(
                profile.installed.ref,
                self._setups[profile.profile].create(actor, profile, instructions[revision])
                if profile.profile in self._setups
                else SqlMainLoopSetup(
                    self._engine,
                    self._schema,
                    actor=actor,
                    agent_ref=profile.installed.ref,
                    agent=profile.installed.agent,
                    model=profile.model,
                    profile=profile.profile,
                    prompt=profile.prompt,
                    max_input_tokens=profile.max_input_tokens,
                    history_items=profile.history_items,
                    skill_instructions=instructions[revision],
                ),
                manifest_hash=content_digest(
                    profile.installed.agent.model_dump(mode="json", by_alias=True)
                ),
            )
            for revision, profile in profiles.items()
        }
        return SqlMainWorkerHandler(
            self._engine,
            self._schema,
            actor=actor,
            estimator=self._estimator,
            provider=provider,
            capabilities=self._capabilities,
            validate_provider_options=self._adapter.validate_options,
            agents=bindings,
            specs=ports.specs,
            tool_resources=ports.tool_resources,
            policy=ports.policy,
            tools=ports.tools,
            approvals=ports.approvals,
            resources=ports.resources,
            context=context,
            outputs=SqlMainOutputFactory(self._engine, self._schema, actor),
            scheduler=SqlScheduler(
                self._engine,
                self._schema,
                journal=SqlJournal(self._engine, self._schema, actor),
                config=self._scheduler_config,
            ),
            subagents=ports.subagents,
            allowed_predicates=self._predicates,
            message_predicate=self._message,
            execution_predicate=self._execution_predicate,
            max_decisions=max(p.installed.agent.budget.max_model_calls for p in profiles.values()),
        )

    async def _profile_for_run(self, actor: ActorContext, run_id: UUID) -> MainAgentProfile:
        # This selects configuration only. The loader still fences the full claim
        # and revalidates current authority before any model or tool execution.
        async with async_sessionmaker(self._engine)() as db:
            revision = (
                await db.execute(
                    scoped(
                        "SELECT agent_revision_id FROM {t}run WHERE scope_id=:scope AND id=:run",
                        self._schema,
                    ),
                    {"scope": actor.scope_id, "run": run_id},
                )
            ).scalar_one_or_none()
        profile = self._agents.get((actor.scope_id, revision)) if revision is not None else None
        if profile is None:
            errors.forbidden("Run Agent revision is not installed in this scope")
        return profile

    async def _decision_format(
        self,
        request: ProviderRequest,
        snapshot: PreparedContext,
    ) -> Mapping[str, JsonValue]:
        # Constrain model-selected references to actual runtime facts. This is not
        # authorization: committers still check current ACL, seals and versions.
        async with async_sessionmaker(self._engine)() as db:
            ids = (
                (
                    await db.execute(
                        scoped(
                            "SELECT i.id FROM {t}timeline_item i WHERE i.scope_id=:scope "
                            "AND i.run_id=:run AND i.status='completed' AND i.item_type='message' "
                            "AND EXISTS (SELECT 1 FROM {t}output_seal os JOIN {t}model_attempt m "
                            "ON m.scope_id=os.scope_id AND m.id=os.model_attempt_id "
                            "WHERE os.scope_id=i.scope_id AND os.item_id=i.id "
                            "AND os.sealed_revision=i.final_revision AND m.status='completed')",
                            self._schema,
                        ),
                        {"scope": request.scope_id, "run": request.run_id},
                    )
                )
                .scalars()
                .all()
            )
        schema = CONTROL_DECISIONS.json_schema(by_alias=True)
        finish = schema["$defs"]["FinishPayload"]["properties"]
        finish["finalItemIds"]["items"] = (
            {"type": "string", "enum": [str(id) for id in ids]} if ids else False
        )
        finish["finalItemIds"]["minItems"] = 1
        evidence = [
            source.resource.model_dump(mode="json", by_alias=True)
            for source in snapshot.sources
            if source.resource.resource_version is not None
            and source.disposition in {"included", "summarized"}
        ]
        finish["evidenceRefs"]["items"] = {"enum": evidence} if evidence else False
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "next_action",
                "strict": True,
                "schema": schema,
            },
        }


__all__ = [
    "MainAgentProfile",
    "MainExtensionResolver",
    "MainHostPorts",
    "MainPortsFactory",
    "MainSetupFactory",
    "SqlMainHandlerFactory",
]

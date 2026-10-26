"""Per-claim child assembly using installed Agents and host-owned dependencies."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine

from agent_runtime.action_dispatch import ToolResourceResolver, ToolSpecResolver
from agent_runtime.context_runtime import TokenEstimator
from agent_runtime.contracts.model import ModelConfig
from agent_runtime.contracts.ownership import ChildCommitGuard
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.work_claims import ChildClaimedWork
from agent_runtime.ports import Policy
from agent_runtime.provider import CapabilityResolver
from agent_runtime.providers.context_input import ContextOpenAIInputResolver, ToolDefinitionReader
from agent_runtime.providers.openai_compatible import (
    ConnectionResolver,
    OpenAIAdapterProfile,
    OpenAICompatibleProvider,
)
from agent_runtime.storage.admission import AdmissionAgent
from agent_runtime.storage.approvals import SqlApprovalService
from agent_runtime.storage.child_local_tools import ChildLocalReadTool
from agent_runtime.storage.child_setup import SqlChildLoopSetup
from agent_runtime.storage.child_worker import ChildAgentBinding, SqlChildWorkerHandler
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.context_factory import build_sql_context_components
from agent_runtime.storage.context_sources import SqlRuntimeSourceReader
from agent_runtime.storage.subagents import SqlSubagentRuntime
from agent_runtime.storage.tool_assembly import SqlToolRuntimeAssembly
from agent_runtime.storage.tool_host import SqlToolHost
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema
from agent_runtime.worker import ChildLeaseHandle


@dataclass(frozen=True, slots=True)
class ChildAgentProfile:
    installed: AdmissionAgent
    model: ModelConfig
    profile: CapabilityRef
    prompt: CapabilityRef
    max_input_tokens: int = 16384


@dataclass(frozen=True, slots=True)
class ChildHostPorts:
    definitions: ToolDefinitionReader
    specs: ToolSpecResolver
    tool_resources: ToolResourceResolver
    policy: Policy
    read_handlers: Mapping[CapabilityRef, ChildLocalReadTool]
    subagents: SqlSubagentRuntime
    effect_tools: SqlToolHost | None = None
    tool_runtime: SqlToolRuntimeAssembly | None = None
    approvals: SqlApprovalService | None = None


class ChildPortsFactory(Protocol):
    async def open(
        self, actor: ActorContext, guard: Callable[[], ChildCommitGuard]
    ) -> ChildHostPorts: ...


class SqlChildHandlerFactory:
    """Build a child handler after SQL has revalidated the claimed actor."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        client: httpx.AsyncClient,
        estimator: TokenEstimator,
        agents: Mapping[tuple[str, UUID], ChildAgentProfile],
        ports: ChildPortsFactory,
        connections: ConnectionResolver,
        capabilities: CapabilityResolver,
        adapter: OpenAIAdapterProfile,
    ) -> None:
        self._engine, self._schema, self._client = engine, require_schema(schema), client
        self._estimator, self._agents, self._ports = estimator, dict(agents), ports
        self._connections, self._capabilities, self._adapter = connections, capabilities, adapter
        for (_, revision_id), profile in self._agents.items():
            agent = profile.installed.agent
            if (
                revision_id != profile.installed.revision_id
                or agent.model != profile.model.ref
                or (agent.prompt is not None and agent.prompt != profile.prompt)
            ):
                raise ValueError("child Agent revision, model or prompt is not pinned")
            if (
                agent.knowledge or agent.subagents or agent.skills or agent.hooks or agent.policy
                or agent.context is not None or agent.memory is not None
                or agent.input_schema is not None or agent.output_schema is not None
            ):
                raise ValueError("child Agent requires an unsupported host binding")

    async def open(
        self, actor: ActorContext, work: ChildClaimedWork, lease: ChildLeaseHandle
    ) -> SqlChildWorkerHandler:
        profiles = {
            revision: profile for (scope, revision), profile in self._agents.items()
            if scope == actor.scope_id
        }
        if not profiles:
            errors.forbidden("No child Agents are installed in this scope")
        def guard() -> ChildCommitGuard:
            return ChildCommitGuard(
                actor=actor,
                lease=lease.current(),
                expected_state_version=work.state_version,
                root_run_id=work.root_run_id,
                expected_root_cancel_version=work.root_cancel_version,
                workspace_ref=work.workspace_ref,
                workspace_owner_run_id=work.lease.run_id,
                workspace_generation=work.workspace_generation,
            )

        ports = await self._ports.open(actor, guard)
        if ports.subagents.actor != actor or ports.subagents.schema != self._schema:
            raise ValueError("child runtime is bound to another actor or schema")
        if ports.effect_tools is not None:
            if ports.tool_runtime is None or ports.tool_runtime.host is not ports.effect_tools:
                raise ValueError("child ToolHost requires its capacity assembly")
            if ports.approvals is None:
                raise ValueError("child ToolHost requires durable approval service")
        elif ports.tool_runtime is not None:
            raise ValueError("child tool runtime has no ToolHost")
        context = build_sql_context_components(
            self._engine, self._schema, estimator=self._estimator
        )
        reader = SqlRuntimeSourceReader(
            self._engine, self._schema,
            blobs=SqlContextBlobBackend(self._engine, self._schema),
        )
        provider = OpenAICompatibleProvider(
            self._client,
            inputs=ContextOpenAIInputResolver(
                context=context.runtime,
                actor=actor,
                access=context.access,
                tools=ports.definitions,
            ),
            connections=self._connections,
            profile=self._adapter,
        )
        bindings = {
            revision: ChildAgentBinding(
                profile.installed.ref,
                SqlChildLoopSetup(
                    actor=actor,
                    agent_ref=profile.installed.ref,
                    agent=profile.installed.agent,
                    model=profile.model,
                    profile=profile.profile,
                    prompt=profile.prompt,
                    reader=reader,
                    max_input_tokens=profile.max_input_tokens,
                ),
            )
            for revision, profile in profiles.items()
        }
        return SqlChildWorkerHandler(
            self._engine,
            self._schema,
            actor=actor,
            subagents=ports.subagents,
            estimator=self._estimator,
            provider=provider,
            capabilities=self._capabilities,
            validate_provider_options=self._adapter.validate_options,
            agents=bindings,
            specs=ports.specs,
            resources=ports.tool_resources,
            policy=ports.policy,
            read_handlers=ports.read_handlers,
            effect_tools=ports.effect_tools,
            tool_runtime=ports.tool_runtime,
            max_decisions=max(p.installed.agent.budget.max_model_calls for p in profiles.values()),
        )


__all__ = ["ChildAgentProfile", "ChildHostPorts", "ChildPortsFactory", "SqlChildHandlerFactory"]

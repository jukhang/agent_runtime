"""Capacity-bound Skill executor for the SQL ToolHost background protocol."""

from dataclasses import dataclass
from typing import Any, Literal, Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.action_dispatch import ToolResourceResolver, ToolSpecResolver
from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.policy import PolicyAllow
from agent_runtime.contracts.references import CapabilityRef, ResourceRef
from agent_runtime.contracts.tools import PendingExecution, ToolInvocation
from agent_runtime.sandbox.runtime import ExecutionBinding, SandboxBackend, SandboxExecutionRuntime
from agent_runtime.sandbox.supervision import ExecutionLimits
from agent_runtime.skills.sandbox import OpenSandboxSkillRunner
from agent_runtime.storage.sandbox_start import ExecutionAuthorizer, SqlSandboxStartAuthority
from agent_runtime.storage.skill_bundles import SqlSkillBundleStore
from agent_runtime.storage.skill_execution import SqlSkillExecutionAuthorizer
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.tool_execution_gate import SqlToolExecutionGate
from agent_runtime.storage.tool_host import TransactionAuthorizer


@dataclass(frozen=True, slots=True)
class SkillSandboxExecution:
    """The host owns SDK/backend lifetime, fresh isolation, TTL and pre-dispatch cleanup."""

    sandbox: Any
    backend: SandboxBackend
    binding: ExecutionBinding
    limits: ExecutionLimits
    timeout_seconds: float


class SkillSandboxResolver(Protocol):
    async def resolve(
        self, invocation: ToolInvocation, grant: PolicyAllow
    ) -> SkillSandboxExecution: ...


class SqlSkillScriptTool:
    def __init__(
        self,
        *,
        capacity: SqlToolCapacity,
        store: SqlSkillBundleStore,
        skill: CapabilityRef,
        executable_path: str,
        interpreter: Literal["python", "sh"],
        specs: ToolSpecResolver,
        resources: ToolResourceResolver,
        authorize: TransactionAuthorizer,
        authorize_execution: ExecutionAuthorizer,
        execution_gate: SqlToolExecutionGate,
        sandboxes: SkillSandboxResolver,
        payloads: BlobContextPayloadStore,
    ) -> None:
        if store.engine is not capacity.engine or store.schema != capacity.schema:
            raise ValueError("Skill executor and store must share the capacity database")
        if execution_gate.schema != capacity.schema:
            raise ValueError("Skill executor gate must use the same schema")
        self._capacity, self._store, self._skill = capacity, store, skill
        self._path, self._interpreter = executable_path, interpreter
        self._specs, self._resources = specs, resources
        self._authorize, self._authorize_execution = authorize, authorize_execution
        self._gate, self._sandboxes, self._payloads = execution_gate, sandboxes, payloads

    @property
    def capacity(self) -> SqlToolCapacity:
        return self._capacity

    async def execute(self, invocation: ToolInvocation, grant: PolicyAllow) -> PendingExecution:
        spec = await self._specs.get(invocation.call)
        if spec.side_effect != "external" or spec.idempotency != "reconcile":
            raise ValueError("SQL Skill scripts require supervised external execution")
        bundle = await self._store.load(invocation.actor, invocation.run_id, self._skill)
        resources = await self._resources.resources(invocation, spec)
        sandbox = await self._sandboxes.resolve(invocation, grant)

        async def authorize(
            db: AsyncSession, current: ToolInvocation, refs: tuple[ResourceRef, ...]
        ) -> None:
            await self._authorize(db, current, refs)
            await self._gate.verify(db, current, grant, spec, refs)

        authority = SqlSandboxStartAuthority(
            self._capacity.engine,
            self._capacity.schema,
            invocation=invocation,
            spec=spec,
            grant=grant,
            resources=resources,
            authorize=authorize,
            authorize_execution=SqlSkillExecutionAuthorizer(
                self._capacity.schema, skill=self._skill, authorize=self._authorize_execution
            ),
            payloads=self._payloads,
            capacity=self._capacity,
        )

        async def binding(current: ToolInvocation, allowed: PolicyAllow) -> ExecutionBinding:
            if current != invocation or allowed != grant:
                raise ValueError("Skill sandbox resolver changed the invocation")
            return sandbox.binding

        runner = OpenSandboxSkillRunner(
            sandbox=sandbox.sandbox,
            runtime=SandboxExecutionRuntime(sandbox.backend, authority),
            binding=binding,
            limits=sandbox.limits,
            timeout_seconds=sandbox.timeout_seconds,
            interpreter=self._interpreter,
        )
        return await runner.start(invocation, grant, bundle=bundle, executable_path=self._path)


__all__ = ["SkillSandboxExecution", "SkillSandboxResolver", "SqlSkillScriptTool"]

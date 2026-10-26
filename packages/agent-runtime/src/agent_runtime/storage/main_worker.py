"""Assemble one claimed root WorkItem from durable runtime components."""

from __future__ import annotations

import asyncio
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine

from agent_runtime.action_dispatch import (
    AuthorizedToolExecutor,
    RunActionDispatcher,
    ToolResourceResolver,
    ToolSpecResolver,
)
from agent_runtime.agent_loop import AnswerStore, ModelAgentLoop, ModelEventSink
from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.context_runtime import TokenEstimator
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.scheduler import ClaimedWork
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.contracts.work_claims import ControlClaimedWork, WorkClaimResult
from agent_runtime.loop_runner import AgentStepRunner, ToolOutputFinalizer
from agent_runtime.ports import Policy, Provider
from agent_runtime.provider import CapabilityResolver, ProviderInvoker
from agent_runtime.provider_preflight import OptionsValidator
from agent_runtime.storage.action_commit import SqlWaitActionCommitter
from agent_runtime.storage.action_router import SqlActionCommitRouter
from agent_runtime.storage.answer_commit import AnswerPublisher, SqlAnswerActionCommitter
from agent_runtime.storage.approval_resume import SqlApprovalResumer
from agent_runtime.storage.approvals import SqlApprovalService
from agent_runtime.storage.context_access import TransactionalResourceAccess
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.context_factory import SqlContextComponents, build_sql_context_components
from agent_runtime.storage.decisions import SqlDecisionStore, SqlExecutionWaiter
from agent_runtime.storage.delegate_commit import SqlDelegateActionCommitter
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.live_run import SqlLiveRunGuard
from agent_runtime.storage.main_budget import SqlMainBudgetReader
from agent_runtime.storage.main_cancel import SqlMainCancelSignal
from agent_runtime.storage.main_control import SqlMainControlHandler
from agent_runtime.storage.main_output_recovery import SqlMainOutputRecovery
from agent_runtime.storage.main_run_loader import (
    LoadedMainRun,
    MainAgentBinding,
    SqlMainRunLoader,
)
from agent_runtime.storage.model_attempts import SqlModelAttemptStore
from agent_runtime.storage.plan_commit import SqlPlanActionCommitter
from agent_runtime.storage.scheduler import SqlScheduler
from agent_runtime.storage.subagents import SqlSubagentRuntime
from agent_runtime.storage.uow import SqlStateUnitOfWork
from agent_runtime.storage.uow.schema import require_schema
from agent_runtime.worker import LeaseHandle


@dataclass(frozen=True, slots=True)
class MainOutputs:
    answers: AnswerStore
    events: ModelEventSink
    publisher: AnswerPublisher
    tool_output: ToolOutputFinalizer


class MainOutputFactory(Protocol):
    async def open(self, loaded: LoadedMainRun, lease: LeaseHandle) -> MainOutputs:
        """Provide durable, per-attempt output and final-answer publication."""
        ...


class MainControlHandler(Protocol):
    async def __call__(self, work: ControlClaimedWork, lease: LeaseHandle) -> None: ...


class SqlMainWorkerHandler:
    """Only compose the root loop; SQL transitions retain their own CAS owners."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        estimator: TokenEstimator,
        provider: Provider,
        capabilities: CapabilityResolver,
        validate_provider_options: OptionsValidator | None = None,
        agents: Mapping[UUID, MainAgentBinding],
        specs: ToolSpecResolver,
        tool_resources: ToolResourceResolver,
        policy: Policy,
        tools: AuthorizedToolExecutor,
        resources: TransactionalResourceAccess,
        outputs: MainOutputFactory,
        scheduler: SqlScheduler,
        subagents: SqlSubagentRuntime,
        allowed_predicates: Collection[CapabilityRef],
        message_predicate: CapabilityRef | None = None,
        message_deadline_at: datetime | None = None,
        execution_predicate: CapabilityRef | None = None,
        approvals: SqlApprovalService | None = None,
        control: MainControlHandler | None = None,
        max_decisions: int = 24,
        context: SqlContextComponents | None = None,
    ) -> None:
        if max_decisions < 1:
            raise ValueError("main decision limit must be positive")
        self._engine = engine
        self._schema = require_schema(schema)
        self._actor = actor
        self._loader = SqlMainRunLoader(engine, schema, actor=actor, agents=agents)
        self._estimator = estimator
        self._context = context
        self._provider = provider
        self._capabilities = capabilities
        self._validate_provider_options = validate_provider_options
        self._specs = specs
        self._tool_resources = tool_resources
        self._policy = policy
        self._tools = tools
        self._resources = resources
        self._outputs = outputs
        self._scheduler = scheduler
        self._subagents = subagents
        self._allowed_predicates = tuple(allowed_predicates)
        self._message_predicate = message_predicate
        self._message_deadline_at = message_deadline_at
        if execution_predicate is not None and execution_predicate not in allowed_predicates:
            raise ValueError("execution wait predicate must be registered")
        self._execution_predicate = execution_predicate
        self._approvals = approvals
        self._control = (
            control if control is not None else SqlMainControlHandler(engine, schema, actor)
        )
        self._max_decisions = max_decisions

    async def __call__(self, work: WorkClaimResult, lease: LeaseHandle) -> None:
        if isinstance(work, ControlClaimedWork):
            if not isinstance(work.lease, LeaseToken):
                raise runtime_failure(
                    "INVALID_INPUT",
                    category="validation",
                    public_message="A child control claim was routed to the root handler.",
                )
            await self._control(work, lease)
            return
        if not isinstance(work, ClaimedWork):
            raise runtime_failure(
                "INVALID_INPUT",
                category="validation",
                public_message="Root Agent handler requires an advance_run claim.",
            )
        loaded = await self._loader.load(work, lease)
        budgets = SqlMainBudgetReader(
            self._engine,
            self._schema,
            actor=self._actor,
            lease=lease.current,
            allocation_id=loaded.budget_allocation_id,
        )
        budget = await budgets.read(loaded.state)
        context = self._context or build_sql_context_components(
            self._engine, self._schema, estimator=self._estimator, optional_history=True
        )
        journal = SqlJournal(self._engine, self._schema, self._actor)
        payloads = BlobContextPayloadStore(SqlContextBlobBackend(self._engine, self._schema))
        decisions = SqlDecisionStore(
            self._engine,
            self._schema,
            actor=self._actor,
            lease=lease.current,
            payloads=payloads,
            context=context.runtime,
            access=context.access,
            journal=journal,
        )
        staged = await decisions.recover()
        await SqlMainOutputRecovery(self._engine, self._schema, self._actor).recover(
            work,
            lease,
            protected_attempt_id=None if staged is None else staged.frame.attempt_id,
        )
        live = SqlLiveRunGuard(
            self._engine,
            self._schema,
            actor=self._actor,
            lease=lease.current,
            context=context.runtime,
            access=context.access,
        )
        output = await self._outputs.open(loaded, lease)
        cancel = SqlMainCancelSignal(self._engine, self._schema, lease.current, actor=self._actor)
        model = ModelAgentLoop(
            context=context.runtime,
            access=context.access,
            provider=ProviderInvoker(
                self._provider,
                capabilities=self._capabilities,
                attempts=SqlModelAttemptStore(
                    self._engine, self._schema, actor=self._actor, lease=lease.current
                ),
                validate_options=self._validate_provider_options,
            ),
            setup=loaded.binding.setup,
            answers=output.answers,
            events=output.events,
            cancel=cancel,
        )
        dispatch = RunActionDispatcher(
            decisions=decisions,
            live=live,
            specs=self._specs,
            resources=self._tool_resources,
            policy=self._policy,
            tools=self._tools,
            observations=decisions,
        )
        state_store = SqlStateUnitOfWork(self._engine, self._schema, self._actor)
        committer = SqlActionCommitRouter(
            answers=SqlAnswerActionCommitter(
                actor=self._actor,
                lease=lease.current,
                decisions=decisions,
                state_store=state_store,
                journal=journal,
                publisher=output.publisher,
                resources=self._resources,
                live=live,
            ),
            waits=SqlWaitActionCommitter(
                actor=self._actor,
                lease=lease.current,
                decisions=decisions,
                scheduler=self._scheduler,
                allowed_predicates=self._allowed_predicates,
                message_predicate=self._message_predicate,
                message_deadline_at=min(self._message_deadline_at, budget.deadline_at)
                if self._message_deadline_at is not None
                else budget.deadline_at,
            ),
            plans=SqlPlanActionCommitter(
                actor=self._actor,
                lease=lease.current,
                decisions=decisions,
                state_store=state_store,
                journal=journal,
                resources=self._resources,
                live=live,
            ),
            delegates=SqlDelegateActionCommitter(
                actor=self._actor,
                lease=lease.current,
                decisions=decisions,
                subagents=self._subagents,
                live=live,
            ),
        )
        runner = AgentStepRunner(
            loop=model,
            dispatch=dispatch,
            recovery=decisions,
            budgets=budgets,
            committer=committer,
            tool_output=output.tool_output,
            execution_waiter=SqlExecutionWaiter(decisions, self._execution_predicate)
            if self._execution_predicate is not None
            else None,
            approval_resumer=(
                SqlApprovalResumer(
                    approvals=self._approvals,
                    specs=self._specs,
                    resources=self._tool_resources,
                    tools=self._tools,
                    decisions=decisions,
                )
                if self._approvals is not None
                else None
            ),
            max_decisions=self._max_decisions,
        )
        watcher = asyncio.create_task(cancel.watch())
        try:
            await runner.run(loaded.state, loaded.inputs)
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)


__all__ = [
    "MainControlHandler",
    "MainOutputFactory",
    "MainOutputs",
    "SqlMainWorkerHandler",
]

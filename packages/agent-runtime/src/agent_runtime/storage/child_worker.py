"""Assemble a claimed child run from fenced, durable runtime components."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.action_dispatch import (
    RunActionDispatcher,
    ToolResourceResolver,
    ToolSpecResolver,
)
from agent_runtime.agent_loop import LoopSetup, ModelAgentLoop
from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.context_runtime import TokenEstimator
from agent_runtime.contracts.decisions import InvokeTools, Respond
from agent_runtime.contracts.failure import FailureEnvelope, runtime_failure
from agent_runtime.contracts.loop import AppliedInputs, RunState
from agent_runtime.contracts.message_payloads import ResultProposed
from agent_runtime.contracts.ownership import ChildCommitGuard, ChildProposalRequest
from agent_runtime.contracts.policy import (
    PolicyAllow,
    PolicyAsk,
    PolicyDecision,
    PolicyDeny,
    PolicyRequest,
    PolicyReview,
)
from agent_runtime.contracts.references import CapabilityRef, ResourceRef
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.tasks import ValidationEvidence
from agent_runtime.contracts.tools import PendingExecution, ToolFailed, ToolInvocation, ToolOutcome
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.work_claims import ChildClaimedWork
from agent_runtime.loop_runner import RecoveredDecision
from agent_runtime.ports import Policy, Provider
from agent_runtime.provider import CapabilityResolver, ProviderInvoker
from agent_runtime.provider_preflight import OptionsValidator
from agent_runtime.storage.child_authority import require_child
from agent_runtime.storage.child_budget import SqlChildBudgetReader
from agent_runtime.storage.child_cancel import SqlChildCancelSignal
from agent_runtime.storage.child_local_tools import ChildLocalReadTool, SqlChildLocalTools
from agent_runtime.storage.child_stream import SqlChildStream
from agent_runtime.storage.child_tools import SqlChildToolAuthorizer
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.context_factory import build_sql_context_components
from agent_runtime.storage.context_sources import SqlRuntimeSourceReader
from agent_runtime.storage.decisions import SqlDecisionStore
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.live_run import SqlLiveRunGuard
from agent_runtime.storage.message_rows import parse_agent_message_row
from agent_runtime.storage.model_attempts import SqlModelAttemptStore
from agent_runtime.storage.subagents import SqlSubagentRuntime
from agent_runtime.storage.tool_assembly import SqlToolRuntimeAssembly
from agent_runtime.storage.tool_host import SqlToolHost
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.worker import ChildLeaseHandle


@dataclass(frozen=True, slots=True)
class ChildAgentBinding:
    """Host-owned, revision-pinned setup for one installed child Agent."""

    agent: CapabilityRef
    setup: LoopSetup


class SqlChildWorkerHandler:
    """Execute native Respond/InvokeTools for a one-level child run.

    Only registered local read or effect handlers may execute. Pending sandbox
    effects release the child lease; the child error sink handles actual failures.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        subagents: SqlSubagentRuntime,
        estimator: TokenEstimator,
        provider: Provider,
        capabilities: CapabilityResolver,
        validate_provider_options: OptionsValidator | None = None,
        agents: Mapping[UUID, ChildAgentBinding],
        specs: ToolSpecResolver,
        resources: ToolResourceResolver,
        policy: Policy,
        read_handlers: Mapping[CapabilityRef, ChildLocalReadTool],
        effect_tools: SqlToolHost | None = None,
        tool_runtime: SqlToolRuntimeAssembly | None = None,
        max_decisions: int = 24,
        execution_predicate: CapabilityRef | None = None,
    ) -> None:
        if max_decisions < 1:
            raise ValueError("child decision limit must be positive")
        if effect_tools is not None:
            if tool_runtime is None or tool_runtime.host is not effect_tools:
                raise ValueError("child SQL ToolHost requires its terminal-capacity assembly")
        elif tool_runtime is not None:
            raise ValueError("child tool runtime assembly has no active ToolHost")
        self._engine = engine
        self._schema = require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._actor = actor
        self._subagents = subagents
        self._estimator = estimator
        self._provider = provider
        self._capabilities = capabilities
        self._validate_provider_options = validate_provider_options
        self._agents = dict(agents)
        self._specs, self._resources, self._policy = specs, resources, policy
        self._read_handlers = dict(read_handlers)
        self._effect_tools = effect_tools
        self._max_decisions = max_decisions
        self._execution_predicate = execution_predicate

    async def __call__(self, work: ChildClaimedWork, lease: ChildLeaseHandle) -> None:
        cancel = SqlChildCancelSignal(self._engine, self._schema, self._actor, work, lease)
        watcher = asyncio.create_task(cancel.watch())
        try:
            await self._run(work, lease, cancel)
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)

    async def _run(
        self, work: ChildClaimedWork, lease: ChildLeaseHandle, cancel: SqlChildCancelSignal
    ) -> None:
        if work.lease.scope_id != self._actor.scope_id:
            errors.forbidden("child claim belongs to another scope")

        def guard() -> ChildCommitGuard:
            return ChildCommitGuard(
                actor=self._actor,
                lease=lease.current(),
                expected_state_version=work.state_version,
                root_run_id=work.root_run_id,
                expected_root_cancel_version=work.root_cancel_version,
                workspace_ref=work.workspace_ref,
                workspace_owner_run_id=work.lease.run_id,
                workspace_generation=work.workspace_generation,
            )

        unresolved = await self._unknown_external_effect(work.lease.run_id)
        if unresolved == "active":
            await self._subagents.park_unknown_execution(guard())
            return
        if unresolved == "resolved":
            raise runtime_failure(
                "DEPENDENCY_UNAVAILABLE",
                category="tool",
                public_message=(
                    "The delegated external tool effect was reconciled, but its result "
                    "was not confirmed. The task stopped without repeating the write."
                ),
            )

        state, inputs, task_id, binding = await self._load(work, guard())
        context = build_sql_context_components(
            self._engine, self._schema, estimator=self._estimator
        )
        journal = SqlJournal(self._engine, self._schema, self._actor)
        stream = SqlChildStream(self._engine, self._schema, journal, guard)
        blobs = BlobContextPayloadStore(SqlContextBlobBackend(self._engine, self._schema))
        decisions = SqlDecisionStore(
            self._engine,
            self._schema,
            actor=self._actor,
            lease=lease.current,
            payloads=blobs,
            context=context.runtime,
            access=context.access,
            journal=journal,
            child_guard=guard,
        )
        live = SqlLiveRunGuard(
            self._engine,
            self._schema,
            actor=self._actor,
            lease=lease.current,
            context=context.runtime,
            access=context.access,
            child_guard=guard,
        )
        loop = ModelAgentLoop(
            context=context.runtime,
            access=context.access,
            provider=ProviderInvoker(
                self._provider,
                capabilities=self._capabilities,
                attempts=SqlModelAttemptStore(self._engine, self._schema),
                validate_options=self._validate_provider_options,
            ),
            setup=binding.setup,
            answers=stream,
            events=stream,
            cancel=cancel,
        )
        tools = SqlChildLocalTools(
            self._engine,
            guard=guard,
            authorizer=SqlChildToolAuthorizer(
                self._schema,
                self._actor,
                SqlRuntimeSourceReader(
                    self._engine,
                    self._schema,
                    blobs=SqlContextBlobBackend(self._engine, self._schema),
                ),
            ),
            specs=self._specs,
            resources=self._resources,
            handlers=self._read_handlers,
        )
        dispatch = RunActionDispatcher(
            decisions=decisions,
            live=live,
            specs=self._specs,
            resources=self._resources,
            policy=_NonInteractiveChildPolicy(self._policy),
            tools=_ChildToolRouter(tools, self._effect_tools, self._specs),
            observations=decisions,
        )
        budgets = SqlChildBudgetReader(self._engine, self._schema, work.budget_allocation_id, guard)
        recovered = await decisions.recover()
        if await self._latest_attempt(work.lease.run_id) not in (
            None,
            recovered.frame.attempt_id if recovered else None,
        ):
            errors.version_conflict(
                "child has an uncommitted model attempt; reconcile before retry"
            )
        refs: tuple[ResourceRef, ...] = ()
        if recovered is not None:
            state = state.model_copy(update={"checkpoint_ref": recovered.checkpoint_ref})
            inputs = inputs.model_copy(update={"checkpoint_ref": state.checkpoint_ref})
            published = await stream.publish(recovered.frame)
            if isinstance(recovered.frame.action, Respond):
                await budgets.read(state)
                await self._propose(
                    guard(),
                    task_id,
                    recovered.frame.action,
                    stream,
                    recovered.frame.attempt_id,
                    published,
                )
                return
            if await self._park_execution(state, decisions, recovered):
                return
            refs = recovered.require_next_model_sources()
        for _ in range(self._max_decisions):
            budget = await budgets.read(state)
            frame = await loop.decide(state, inputs, budget, observation_refs=refs)
            if isinstance(frame.action, InvokeTools):
                try:
                    await self._require_registered_calls(frame.action)
                except Exception:
                    await stream.publish(frame)
                    raise
            dispatched = await dispatch.dispatch_result(state, budget, frame)
            state = dispatched.state
            if any(
                isinstance(outcome, ToolFailed) and outcome.effect == "unknown"
                for outcome in dispatched.outcomes
            ):
                await self._subagents.park_unknown_execution(guard())
                return
            published = await stream.publish(frame)
            if isinstance(frame.action, Respond):
                await budgets.read(state)
                await self._propose(
                    guard(),
                    task_id,
                    frame.action,
                    stream,
                    frame.attempt_id,
                    published,
                )
                return
            if not isinstance(frame.action, InvokeTools):
                errors.invalid_input("child Agent can only respond or use an authorized read tool")
            observed = await decisions.recover()
            if observed is None or observed.frame.attempt_id != frame.attempt_id:
                errors.version_conflict("child tool observations are not durable")
            if await self._park_execution(state, decisions, observed):
                return
            refs = observed.require_next_model_sources()
            inputs = inputs.model_copy(update={"checkpoint_ref": state.checkpoint_ref})
        raise runtime_failure(
            "BUDGET_EXHAUSTED",
            category="budget",
            public_message="The child reached its decision limit.",
        )

    async def _park_execution(
        self, state: RunState, decisions: SqlDecisionStore, observed: RecoveredDecision
    ) -> bool:
        if not any(isinstance(item, PendingExecution) for item in observed.outcomes):
            return False
        if self._execution_predicate is None:
            observed.require_next_model_sources()
        else:
            await decisions.park_execution(state, observed, self._execution_predicate)
        return True

    async def _require_registered_calls(self, action: InvokeTools) -> None:
        for call in action.payload.validated_calls:
            spec = await self._specs.get(call)
            local_read = (
                call.tool in self._read_handlers
                and spec.kind == "read"
                and spec.side_effect == "none"
                and spec.idempotency == "read_only"
                and spec.public_result_policy == "full"
            )
            effect = self._effect_tools is not None and self._effect_tools.supports(spec)
            if spec.ref != call.tool or not (local_read or effect):
                errors.forbidden("child tool has no registered, authorized executor")

    async def _load(
        self, work: ChildClaimedWork, guard: ChildCommitGuard
    ) -> tuple[RunState, AppliedInputs, UUID, ChildAgentBinding]:
        async with self._sessions() as db, db.begin():
            run = await require_child(db, self._schema, guard, now=datetime.now(UTC))
            checkpoint = (
                (
                    await db.execute(
                        scoped(
                            "SELECT state_ref FROM {t}checkpoint WHERE scope_id=:scope "
                            "AND id=:id AND run_id=:run",
                            self._schema,
                        ),
                        {
                            "scope": self._actor.scope_id,
                            "id": run["checkpoint_id"],
                            "run": run["id"],
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            messages = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}agent_message WHERE scope_id=:scope "
                            "AND recipient_run_id=:run AND status='applied' "
                            "ORDER BY recipient_seq",
                            self._schema,
                        ),
                        {"scope": self._actor.scope_id, "run": run["id"]},
                    )
                )
                .mappings()
                .all()
            )
            agent = (
                await db.execute(
                    scoped(
                        "SELECT revision FROM {t}agent_revision WHERE scope_id=:scope AND id=:id",
                        self._schema,
                    ),
                    {"scope": self._actor.scope_id, "id": run["agent_revision_id"]},
                )
            ).scalar_one_or_none()
            if checkpoint is None or agent is None or not messages:
                errors.version_conflict("child assignment, checkpoint or Agent is missing")
            if int(run["pending_input_version"]) != int(run["applied_input_version"]):
                errors.control_pending("child input must be applied before model execution")
            envelopes = tuple(parse_agent_message_row(dict(message)) for message in messages)
            if len(envelopes) != int(run["applied_input_version"]):
                errors.version_conflict("child input watermark disagrees with its messages")
            assignment = envelopes[0]
            if assignment.kind != "task.assign" or assignment.task_id != run["task_id"]:
                errors.version_conflict("child assignment changed identity")
            binding = self._agents.get(run["agent_revision_id"])
            if binding is None or binding.agent.revision != str(agent):
                errors.forbidden("child Agent revision is not installed")
            state = RunState(
                scope_id=self._actor.scope_id,
                session_id=run["session_id"],
                run_id=run["id"],
                state_version=str(run["state_version"]),
                checkpoint_ref=checkpoint["state_ref"],
                agent=binding.agent,
                status=RunStatus.RUNNING,
                desired_control=None,
                cancel_version=str(run["cancel_version"]),
                pending_input_seq=str(run["pending_input_version"]),
                applied_input_seq=str(run["applied_input_version"]),
            )
            inputs = AppliedInputs(
                run_id=run["id"],
                checkpoint_ref=checkpoint["state_ref"],
                previous_seq="0",
                applied_seq=str(len(envelopes)),
                messages=envelopes,
            )
            return state, inputs, run["task_id"], binding

    async def _latest_attempt(self, run_id: UUID) -> UUID | None:
        async with self._sessions() as db:
            return (
                await db.execute(
                    scoped(
                        "SELECT id FROM {t}model_attempt WHERE scope_id=:scope AND run_id=:run "
                        "ORDER BY started_at DESC,id DESC LIMIT 1",
                        self._schema,
                    ),
                    {"scope": self._actor.scope_id, "run": run_id},
                )
            ).scalar_one_or_none()

    async def _unknown_external_effect(self, run_id: UUID) -> str | None:
        async with self._sessions() as db:
            statuses = (
                (
                    await db.execute(
                        scoped(
                            "SELECT e.status FROM {t}operation o JOIN {t}execution e "
                            "ON e.scope_id=o.scope_id AND e.operation_id=o.id "
                            "JOIN {t}tool_call c ON c.scope_id=o.scope_id AND c.operation_id=o.id "
                            "WHERE o.scope_id=:scope AND e.run_id=:run AND o.status='unknown' "
                            "AND c.run_id=e.run_id AND c.status='failed'",
                            self._schema,
                        ),
                        {"scope": self._actor.scope_id, "run": run_id},
                    )
                )
                .scalars()
                .all()
            )
        if not statuses:
            return None
        return (
            "active"
            if any(status not in {"completed", "failed", "cancelled"} for status in statuses)
            else "resolved"
        )

    async def _propose(
        self,
        guard: ChildCommitGuard,
        task_id: UUID,
        action: Respond,
        stream: SqlChildStream,
        attempt_id: UUID,
        ref: ResourceRef | None,
    ) -> None:
        text = await stream.answer_text(action.payload.answer_request_ref)
        if not text.strip():
            errors.invalid_input("child result is empty")
        if ref is None:
            errors.version_conflict("child result has no sealed output")
        proposal = ResultProposed(
            result_id=uuid5(NAMESPACE_URL, f"child-result:{guard.lease.run_id}:{attempt_id}"),
            summary=text[:500],
            artifact_refs=(ref,),
            evidence_refs=(ref,),
            validation=ValidationEvidence(status="unknown", evidence_refs=(ref,)),
            remaining_issues=(),
        )
        await self._subagents.propose(
            ChildProposalRequest(
                guard=guard,
                task_id=task_id,
                proposal=proposal,
            )
        )


class _ChildToolRouter:
    def __init__(
        self,
        local: SqlChildLocalTools,
        effects: SqlToolHost | None,
        specs: ToolSpecResolver,
    ) -> None:
        self._local, self._effects, self._specs = local, effects, specs

    async def execute(self, invocation: ToolInvocation, grant: PolicyAllow) -> ToolOutcome:
        spec = await self._specs.get(invocation.call)
        if spec.side_effect == "none":
            return await self._local.execute(invocation, grant)
        if self._effects is None or not self._effects.supports(spec):
            errors.forbidden("child effect tool is not registered")
        return await self._effects.execute(invocation, grant)


class _NonInteractiveChildPolicy:
    def __init__(self, delegate: Policy) -> None:
        self._delegate = delegate

    async def evaluate(self, request: PolicyRequest) -> PolicyDecision:
        decision = await self._delegate.evaluate(request)
        if not isinstance(decision, (PolicyAsk, PolicyReview)):
            return decision
        return PolicyDeny(
            decision="deny",
            decision_id=decision.decision_id,
            policy=decision.policy,
            input_digest=decision.input_digest,
            evidence_refs=decision.evidence_refs,
            evaluated_at=decision.evaluated_at,
            expires_at=decision.expires_at,
            failure=FailureEnvelope(
                code="FORBIDDEN",
                category="authorization",
                retryable=False,
                public_message="The delegated tool requires an interactive approval.",
            ),
        )


__all__ = ["ChildAgentBinding", "SqlChildWorkerHandler"]

"""Fenced SQL tool effects. External dispatch is never retried without reconciliation."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from pydantic import JsonValue
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.action_dispatch import ToolResourceResolver, ToolSpecResolver
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.policy import PolicyAllow, PolicyRequest
from agent_runtime.contracts.references import CapabilityRef, ResourceRef
from agent_runtime.contracts.tools import (
    FullResult,
    PendingExecution,
    ReferenceResult,
    SummaryResult,
    ToolCompleted,
    ToolFailed,
    ToolInvocation,
    ToolNotExecuted,
    ToolOutcome,
    ToolSpec,
    tool_outcome_adapter,
)
from agent_runtime.policy_validation import validate_policy_decision
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.tool_execution_gate import SqlToolExecutionGate
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.tool_effect import ToolEffect as ToolEffect
from agent_runtime.tool_validation import (
    tool_timeout_failure,
    validate_tool_invocation,
    validate_tool_outcome,
)


class ToolSchemaValidator(Protocol):
    def input(self, spec: ToolSpec, arguments: dict[str, JsonValue]) -> None: ...

    def output(self, spec: ToolSpec, data: JsonValue) -> None: ...


class TransactionalTool(Protocol):
    async def invoke(self, db: AsyncSession, invocation: ToolInvocation) -> ToolEffect: ...


class ReadTool(Protocol):
    async def invoke(self, invocation: ToolInvocation) -> ToolEffect: ...


class ToolHook(Protocol):
    async def before(self, invocation: ToolInvocation, spec: ToolSpec) -> None:
        """Reject an invocation if needed; never grant authority or mutate arguments."""

    async def output(self, invocation: ToolInvocation, spec: ToolSpec, effect: ToolEffect) -> None:
        """Validate private output before the local operation is committed."""

    async def after(self, invocation: ToolInvocation, spec: ToolSpec, outcome: ToolOutcome) -> None:
        """Observe the result. Failure cannot turn a denied call into an allowed one."""


class IdempotentExternalTool(Protocol):
    async def dispatch(self, invocation: ToolInvocation, *, key: str) -> ToolEffect:
        """Send once with a provider-enforced stable idempotency key."""

    async def lookup(self, invocation: ToolInvocation, *, key: str) -> ToolEffect | None:
        """Query remote outcome without sending the effect again."""


class BackgroundExecutionTool(Protocol):
    @property
    def capacity(self) -> SqlToolCapacity:
        """The executor reserves this pool in its SQL dispatch-intent transaction."""
        ...

    async def execute(self, invocation: ToolInvocation, grant: PolicyAllow) -> PendingExecution:
        """Reserve and start through a durable, fenced sandbox authority."""
        ...


type TransactionAuthorizer = Callable[
    [AsyncSession, ToolInvocation, tuple[ResourceRef, ...]], Awaitable[None]
]


class SqlToolHost:
    """Execute only exact registered effects with a durable operation identity.

    The injected authorizer must recheck live lease, ACL and target versions in
    this host's transaction. An external transport must enforce its key remotely;
    an unresolved dispatch is reported unknown and never sent a second time.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        specs: ToolSpecResolver,
        resources: ToolResourceResolver,
        schemas: ToolSchemaValidator,
        authorize: TransactionAuthorizer,
        capacity: SqlToolCapacity,
        execution_gate: SqlToolExecutionGate,
        read_tools: Mapping[CapabilityRef, ReadTool] | None = None,
        database_tools: Mapping[CapabilityRef, TransactionalTool] | None = None,
        external_tools: Mapping[CapabilityRef, IdempotentExternalTool] | None = None,
        background_tools: Mapping[CapabilityRef, BackgroundExecutionTool] | None = None,
        hooks: tuple[ToolHook, ...] = (),
    ) -> None:
        self._schema = require_schema(schema)
        if capacity.engine is not engine or capacity.schema != self._schema:
            raise ValueError("ToolHost capacity must use the same engine and schema")
        if execution_gate.schema != self._schema:
            raise ValueError("ToolHost execution gate must use the same schema")
        self._engine = engine
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._specs, self._resources = specs, resources
        self._schemas, self._authorize = schemas, authorize
        self._capacity = capacity
        self._execution_gate = execution_gate
        self._read = dict(read_tools or {})
        self._database = dict(database_tools or {})
        self._external = dict(external_tools or {})
        self._background = dict(background_tools or {})
        if any(
            getattr(tool, "capacity", None) is not capacity for tool in self._background.values()
        ):
            raise ValueError("background tool must own the ToolHost dispatch capacity")
        self._hooks = hooks
        self._groups: dict[tuple[str, str], tuple[int, asyncio.Semaphore]] = {}

    def supports(self, spec: ToolSpec) -> bool:
        """Exact, trusted handler registration is required before model dispatch."""
        return (
            (
                spec.side_effect == "none"
                and spec.idempotency == "read_only"
                and spec.ref in self._read
            )
            or (
                spec.side_effect == "database"
                and spec.idempotency == "transactional"
                and spec.ref in self._database
            )
            or (
                spec.side_effect == "external"
                and spec.idempotency == "provider_key"
                and spec.ref in self._external
            )
            or (
                spec.side_effect == "external"
                and spec.idempotency == "reconcile"
                and spec.ref in self._background
            )
        )

    @property
    def capacity(self) -> SqlToolCapacity:
        return self._capacity

    @property
    def execution_gate(self) -> SqlToolExecutionGate:
        return self._execution_gate

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    @property
    def schema(self) -> str:
        return self._schema

    @property
    def requires_external_supervisor(self) -> bool:
        return bool(self._external)

    @property
    def requires_sandbox_collector(self) -> bool:
        return bool(self._background)

    @property
    def installed_hook_refs(self) -> frozenset[CapabilityRef]:
        refs: set[CapabilityRef] = set()
        for hook in self._hooks:
            installed = getattr(hook, "refs", None)
            if not isinstance(installed, frozenset):
                raise ValueError("ToolHost contains a hook without pinned references")
            refs.update(installed)
        return frozenset(refs)

    async def execute(self, invocation: ToolInvocation, grant: PolicyAllow) -> ToolOutcome:
        spec: ToolSpec | None = None
        try:
            spec = await self._specs.get(invocation.call)
            try:
                validate_tool_invocation(spec, invocation, now=datetime.now(UTC))
            except AgentRuntimeError as exc:
                if exc.failure.code == "TOOL_TIMEOUT" and self._read_only(spec):
                    return self._timeout_observation(invocation, exc)
                raise
            return await self._execute(invocation, grant, spec)
        except TimeoutError as exc:
            failure = tool_timeout_failure(invocation)
            if spec is not None and self._read_only(spec):
                return self._timeout_observation(invocation, failure)
            raise failure from exc

    @staticmethod
    def _read_only(spec: ToolSpec) -> bool:
        return spec.side_effect == "none" and spec.idempotency == "read_only"

    @staticmethod
    def _timeout_observation(invocation: ToolInvocation, error: AgentRuntimeError) -> ToolFailed:
        return ToolFailed(
            tool_call_id=invocation.call.tool_call_id,
            run_id=invocation.run_id,
            operation_id=invocation.operation_id,
            status="failed",
            effect="none",
            effect_refs=(),
            failure=error.failure,
        )

    async def _execute(
        self, invocation: ToolInvocation, grant: PolicyAllow, spec: ToolSpec
    ) -> ToolOutcome:
        now = datetime.now(UTC)
        try:
            self._schemas.input(spec, invocation.call.arguments)
        except (AgentRuntimeError, ValueError, TypeError) as exc:
            if isinstance(exc, AgentRuntimeError):
                if exc.failure.code != "INVALID_INPUT" or exc.failure.category != "validation":
                    raise
                failure = exc.failure
            else:
                failure = FailureEnvelope(
                    code="INVALID_INPUT",
                    category="validation",
                    retryable=False,
                    public_message="Tool arguments do not match the pinned input schema.",
                    operation_id=(
                        str(invocation.operation_id)
                        if invocation.operation_id is not None
                        else None
                    ),
                )
            return ToolNotExecuted(
                tool_call_id=invocation.call.tool_call_id,
                run_id=invocation.run_id,
                operation_id=invocation.operation_id,
                status="not_executed",
                effect="none",
                effect_refs=(),
                failure=failure,
            )
        refs = await self._resources.resources(invocation, spec)
        request = PolicyRequest(invocation=invocation, policy=spec.approval_policy, resources=refs)
        validate_policy_decision(request, grant, now=now)
        async with self._sessions() as db, db.begin():
            task_id = await self._check(db, invocation, grant, spec, refs)
            cached = await self._cached(db, invocation, task_id, spec)
        if cached is not None:
            await self._capacity.settle_committed(invocation, spec)
            return cached
        for hook in self._hooks:
            async with asyncio.timeout(spec.timeout_seconds):
                await hook.before(invocation, spec)
        group = (invocation.actor.scope_id, str(spec.concurrency_group))
        configured = self._groups.get(group)
        if configured is None:
            configured = (spec.max_concurrency, asyncio.Semaphore(spec.max_concurrency))
            self._groups[group] = configured
        elif configured[0] != spec.max_concurrency:
            errors.invalid_input("tools in one concurrency group require the same limit")
        remaining = (invocation.deadline_at - datetime.now(UTC)).total_seconds()
        if remaining <= 0:
            raise TimeoutError("tool deadline elapsed before execution")
        async with asyncio.timeout(remaining):
            async with configured[1]:
                if spec.side_effect == "external" and spec.idempotency == "reconcile":
                    # The background authority commits slot + intent together.
                    outcome = await self._execute_authorized(invocation, grant, spec, refs)
                else:
                    reservation = await self._capacity.reserve(invocation, spec)
                    try:
                        outcome = await self._execute_authorized(invocation, grant, spec, refs)
                    except BaseException:
                        if spec.side_effect in {"none", "database"}:
                            await self._capacity.release(invocation, reservation)
                        raise
                    if isinstance(outcome, ToolCompleted):
                        await self._capacity.release(invocation, reservation)
        for hook in self._hooks:
            async with asyncio.timeout(spec.timeout_seconds):
                await hook.after(invocation, spec, outcome)
        return outcome

    async def _execute_authorized(
        self,
        invocation: ToolInvocation,
        grant: PolicyAllow,
        spec: ToolSpec,
        refs: tuple[ResourceRef, ...],
    ) -> ToolOutcome:
        if spec.side_effect == "none" and spec.idempotency == "read_only":
            handler = self._read.get(spec.ref)
            if handler is None:
                errors.forbidden("read tool has no registered handler")
            async with self._sessions() as db, db.begin():
                await self._check(db, invocation, grant, spec, refs)
            async with asyncio.timeout(spec.timeout_seconds):
                effect = await handler.invoke(invocation)
            async with self._sessions() as db, db.begin():
                await self._check(db, invocation, grant, spec, refs)
            return await self._completed(spec, invocation, effect)
        if invocation.operation_id is None:
            errors.invalid_input("effectful tool requires an operation identity")
        if spec.side_effect == "external" and spec.idempotency == "reconcile":
            background = self._background.get(spec.ref)
            if background is None:
                errors.forbidden("background tool has no durable executor")
            outcome = await background.execute(invocation, grant)
            validate_tool_outcome(spec, invocation, outcome)
            return outcome
        if spec.side_effect == "database" and spec.idempotency == "transactional":
            database_handler = self._database.get(spec.ref)
            if database_handler is None:
                errors.forbidden("database tool has no registered handler")
            return await self._database_effect(invocation, grant, spec, refs, database_handler)
        if spec.side_effect == "external" and spec.idempotency == "provider_key":
            external_handler = self._external.get(spec.ref)
            if external_handler is None:
                errors.forbidden("external tool has no idempotent transport")
            return await self._external_effect(invocation, grant, spec, refs, external_handler)
        errors.forbidden("tool effect mode requires a different executor")

    async def _check(
        self,
        db: AsyncSession,
        invocation: ToolInvocation,
        grant: PolicyAllow,
        spec: ToolSpec,
        refs: tuple[ResourceRef, ...],
    ) -> str:
        validate_policy_decision(
            PolicyRequest(invocation=invocation, policy=spec.approval_policy, resources=refs),
            grant,
            now=datetime.now(UTC),
        )
        await self._authorize(db, invocation, refs)
        approved_resume = await self._execution_gate.verify(db, invocation, grant, spec, refs)
        row = (
            (
                await db.execute(
                    scoped(
                        "SELECT r.task_id FROM {t}run r JOIN {t}tool_call c "
                        "ON c.scope_id=r.scope_id AND c.run_id=r.id "
                        "JOIN {t}model_attempt a ON a.scope_id=c.scope_id "
                        "AND a.id=c.attempt_id "
                        "WHERE r.scope_id=:scope AND r.id=:run AND r.session_id=:session "
                        "AND c.id=:call "
                        "AND c.attempt_id=:attempt AND "
                        "(c.status='pending' OR (c.status='waiting_approval' "
                        "AND :approved_resume)) "
                        "AND a.run_id=r.id AND a.status='completed' "
                        "AND a.context_snapshot_id=:snapshot "
                        "AND c.provider_call_id=:provider "
                        "AND c.spec_revision=CAST(:spec AS jsonb) AND c.input_hash=:hash "
                        "FOR UPDATE OF c",
                        self._schema,
                    ),
                    {
                        "scope": invocation.actor.scope_id,
                        "run": invocation.run_id,
                        "session": invocation.session_id,
                        "call": invocation.call.tool_call_id,
                        "attempt": invocation.model_attempt_id,
                        "snapshot": invocation.context_snapshot_id,
                        "provider": str(invocation.call.tool_call_id),
                        "spec": json.dumps(spec.ref.model_dump(mode="json", by_alias=True)),
                        "hash": content_digest(invocation.call.arguments),
                        "approved_resume": approved_resume,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            errors.forbidden("tool call is not the current persisted model decision")
        return str(row["task_id"])

    async def _claim(
        self, db: AsyncSession, invocation: ToolInvocation, task_id: str, spec: ToolSpec
    ) -> dict[str, object]:
        assert invocation.operation_id is not None
        scope = invocation.actor.scope_id
        digest = self._operation_digest(invocation, spec)
        await db.execute(
            scoped(
                "INSERT INTO {t}operation (id,scope_id,task_id,idempotency_key,input_hash,status) "
                "VALUES (:id,:scope,:task,:key,:hash,'prepared') "
                "ON CONFLICT (scope_id,task_id,idempotency_key) DO NOTHING",
                self._schema,
            ),
            {
                "id": invocation.operation_id,
                "scope": scope,
                "task": task_id,
                "key": str(invocation.operation_id),
                "hash": digest,
            },
        )
        row = (
            (
                await db.execute(
                    scoped(
                        "SELECT id,input_hash,status,result_ref FROM {t}operation "
                        "WHERE scope_id=:scope AND task_id=:task AND idempotency_key=:key "
                        "FOR UPDATE",
                        self._schema,
                    ),
                    {"scope": scope, "task": task_id, "key": str(invocation.operation_id)},
                )
            )
            .mappings()
            .one()
        )
        if row["id"] != invocation.operation_id or row["input_hash"] != digest:
            errors.invalid_input("operation identity was reused for different input")
        await db.execute(
            scoped(
                "UPDATE {t}tool_call SET operation_id=:operation,updated_at=:now "
                "WHERE scope_id=:scope AND id=:call "
                "AND status IN ('pending','waiting_approval') "
                "AND (operation_id IS NULL OR operation_id=:operation)",
                self._schema,
            ),
            {
                "operation": invocation.operation_id,
                "now": datetime.now(UTC),
                "scope": scope,
                "call": invocation.call.tool_call_id,
            },
        )
        return dict(row)

    @staticmethod
    def _operation_digest(invocation: ToolInvocation, spec: ToolSpec) -> str:
        return content_digest(
            {
                "tool": spec.ref.model_dump(mode="json", by_alias=True),
                "arguments": invocation.call.arguments,
                "runId": str(invocation.run_id),
                "toolCallId": str(invocation.call.tool_call_id),
            }
        )

    async def _cached(
        self, db: AsyncSession, invocation: ToolInvocation, task_id: str, spec: ToolSpec
    ) -> ToolCompleted | None:
        if invocation.operation_id is None or spec.idempotency == "reconcile":
            # Supervised tools own their dispatch digest and sealed result format.
            # Their authority must resolve the durable handle before any send.
            return None
        row = (
            (
                await db.execute(
                    scoped(
                        "SELECT id,input_hash,status,result_ref FROM {t}operation "
                        "WHERE scope_id=:scope AND task_id=:task AND idempotency_key=:key",
                        self._schema,
                    ),
                    {
                        "scope": invocation.actor.scope_id,
                        "task": task_id,
                        "key": str(invocation.operation_id),
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return None
        if row["id"] != invocation.operation_id or row["input_hash"] != self._operation_digest(
            invocation, spec
        ):
            errors.invalid_input("operation identity was reused for different input")
        return self._replay(dict(row), invocation) if row["status"] == "committed" else None

    async def _completed(
        self, spec: ToolSpec, invocation: ToolInvocation, effect: ToolEffect
    ) -> ToolCompleted:
        self._schemas.output(spec, effect.data)
        for hook in self._hooks:
            async with asyncio.timeout(spec.timeout_seconds):
                await hook.output(invocation, spec, effect)
        if any(
            ref.scope.scope_id != invocation.actor.scope_id or ref.resource_version is None
            for ref in effect.refs
        ):
            errors.forbidden("tool returned unversioned or out-of-scope references")
        if spec.public_result_policy == "full":
            result = FullResult(policy="full", data=effect.data)
        elif spec.public_result_policy == "summary":
            return self._completed_summary(spec, invocation, effect)
        else:
            return self._completed_references(spec, invocation, effect)
        return self._outcome(spec, invocation, effect, result)

    def _completed_summary(
        self, spec: ToolSpec, invocation: ToolInvocation, effect: ToolEffect
    ) -> ToolCompleted:
        return self._outcome(
            spec, invocation, effect, SummaryResult(policy="summary", text=effect.summary)
        )

    def _completed_references(
        self, spec: ToolSpec, invocation: ToolInvocation, effect: ToolEffect
    ) -> ToolCompleted:
        return self._outcome(
            spec, invocation, effect, ReferenceResult(policy="reference_only", refs=effect.refs)
        )

    def _outcome(
        self,
        spec: ToolSpec,
        invocation: ToolInvocation,
        effect: ToolEffect,
        result: FullResult | SummaryResult | ReferenceResult,
    ) -> ToolCompleted:
        outcome = ToolCompleted(
            tool_call_id=invocation.call.tool_call_id,
            run_id=invocation.run_id,
            operation_id=invocation.operation_id,
            status="completed",
            effect="none" if spec.side_effect == "none" else "committed",
            effect_refs=effect.refs,
            result=result,
        )
        validate_tool_outcome(spec, invocation, outcome)
        return outcome

    async def _commit(
        self, db: AsyncSession, invocation: ToolInvocation, outcome: ToolCompleted
    ) -> None:
        changed = await db.execute(
            scoped(
                "UPDATE {t}operation SET status='committed',result_ref=:result,"
                "failure=NULL,updated_at=:now WHERE scope_id=:scope AND id=:id "
                "AND status IN ('prepared','dispatching','unknown')",
                self._schema,
            ),
            {
                "result": json.dumps(outcome.model_dump(mode="json", by_alias=True)),
                "now": datetime.now(UTC),
                "scope": invocation.actor.scope_id,
                "id": invocation.operation_id,
            },
        )
        if getattr(changed, "rowcount", None) != 1:
            errors.version_conflict("operation was resolved concurrently")

    def _replay(self, row: Mapping[str, object], invocation: ToolInvocation) -> ToolCompleted:
        raw = row["result_ref"]
        if not isinstance(raw, str):
            errors.version_conflict("committed operation has no result")
        result = tool_outcome_adapter.validate_json(raw)
        if (
            not isinstance(result, ToolCompleted)
            or result.operation_id != invocation.operation_id
            or result.run_id != invocation.run_id
            or result.tool_call_id != invocation.call.tool_call_id
        ):
            errors.version_conflict("committed operation result is inconsistent")
        return result

    async def _database_effect(
        self,
        invocation: ToolInvocation,
        grant: PolicyAllow,
        spec: ToolSpec,
        refs: tuple[ResourceRef, ...],
        handler: TransactionalTool,
    ) -> ToolCompleted:
        async with self._sessions() as db, db.begin():
            task = await self._check(db, invocation, grant, spec, refs)
            row = await self._claim(db, invocation, task, spec)
            if row["status"] == "committed":
                return self._replay(row, invocation)
            if row["status"] != "prepared":
                errors.version_conflict("database effect is not safe to re-execute")
            remaining = (invocation.deadline_at - datetime.now(UTC)).total_seconds()
            if remaining <= 0:
                raise TimeoutError("tool deadline elapsed before database write")
            async with asyncio.timeout(min(remaining, spec.timeout_seconds)):
                effect = await handler.invoke(db, invocation)
            outcome = await self._completed(spec, invocation, effect)
            await self._commit(db, invocation, outcome)
            return outcome

    async def _external_effect(
        self,
        invocation: ToolInvocation,
        grant: PolicyAllow,
        spec: ToolSpec,
        refs: tuple[ResourceRef, ...],
        handler: IdempotentExternalTool,
    ) -> ToolCompleted | ToolFailed:
        fresh = False
        async with self._sessions() as db, db.begin():
            task = await self._check(db, invocation, grant, spec, refs)
            row = await self._claim(db, invocation, task, spec)
            if row["status"] == "committed":
                return self._replay(row, invocation)
            if row["status"] == "prepared":
                await self._ensure_execution(db, invocation, spec)
                await db.execute(
                    scoped(
                        "UPDATE {t}operation SET status='dispatching',updated_at=:now "
                        "WHERE scope_id=:scope AND id=:id AND status='prepared'",
                        self._schema,
                    ),
                    {
                        "now": datetime.now(UTC),
                        "scope": invocation.actor.scope_id,
                        "id": invocation.operation_id,
                    },
                )
                fresh = True
            elif row["status"] in {"dispatching", "unknown"}:
                await self._ensure_execution(db, invocation, spec)
            else:
                errors.version_conflict("external operation cannot be replayed")
        key = str(invocation.operation_id)
        if fresh:
            # A cancelled or revoked run must not send after the prepare commit.
            async with self._sessions() as db, db.begin():
                await self._check(db, invocation, grant, spec, refs)
        try:
            remaining = (invocation.deadline_at - datetime.now(UTC)).total_seconds()
            if remaining <= 0:
                await self._mark_unknown(invocation)
                return self._unknown(invocation)
            async with asyncio.timeout(min(remaining, spec.timeout_seconds)):
                effect = (
                    await handler.dispatch(invocation, key=key)
                    if fresh
                    else await handler.lookup(invocation, key=key)
                )
        except Exception:
            # A transport exception can occur after the remote effect was committed.
            await self._mark_unknown(invocation)
            return self._unknown(invocation)
        if effect is None:
            await self._mark_unknown(invocation)
            return self._unknown(invocation)
        try:
            outcome = await self._completed(spec, invocation, effect)
        except Exception:
            await self._mark_unknown(invocation)
            return self._unknown(invocation)
        try:
            async with self._sessions() as db, db.begin():
                await self._check(db, invocation, grant, spec, refs)
                row = await self._claim(db, invocation, task, spec)
                if row["status"] == "committed":
                    return self._replay(row, invocation)
                await self._commit(db, invocation, outcome)
                await self._finish_execution(db, invocation)
        except Exception:
            # A lost lease or database error after the send does not undo the remote effect.
            await self._mark_unknown(invocation)
            return self._unknown(invocation)
        return outcome

    async def _ensure_execution(
        self, db: AsyncSession, invocation: ToolInvocation, spec: ToolSpec
    ) -> None:
        assert invocation.operation_id is not None
        scope = invocation.actor.scope_id
        execution_id = uuid5(NAMESPACE_URL, f"execution:{scope}:{invocation.operation_id}")
        work_id = uuid5(NAMESPACE_URL, f"reconcile:{scope}:{invocation.operation_id}:0")
        now = datetime.now(UTC)
        deadline = max(invocation.deadline_at, now) + timedelta(minutes=10)
        supervise_after = min(invocation.deadline_at, now + timedelta(seconds=spec.timeout_seconds))
        subject = {
            "resourceType": "execution",
            "resourceId": str(execution_id),
            "resourceVersion": "0",
            "scope": {"scopeId": scope, "sessionId": None, "ownerId": None},
        }
        await db.execute(
            scoped(
                "INSERT INTO {t}work_item "
                "(id,scope_id,run_id,subject_ref,kind,dedup_key,status,payload_ref,"
                "wake_reason,priority,lease_epoch,attempt_count,available_at,deadline_at) "
                "VALUES (:id,:scope,:run,CAST(:subject AS jsonb),'reconcile_execution',"
                ":dedup,'pending',:payload,NULL,1000,0,0,:available,:deadline) "
                "ON CONFLICT (scope_id,dedup_key) DO NOTHING",
                self._schema,
            ),
            {
                "id": work_id,
                "scope": scope,
                "run": invocation.run_id,
                "subject": json.dumps(subject),
                "dedup": f"execution:{execution_id}:reconcile:0",
                "payload": f"execution:{execution_id}",
                "available": supervise_after,
                "deadline": deadline,
            },
        )
        await db.execute(
            scoped(
                "INSERT INTO {t}execution "
                "(id,scope_id,operation_id,run_id,supervisor_work_id,remote_request_key,"
                "status,log_cursor,cleanup_status,generation,reconciliation_round,deadline_at) "
                "VALUES (:id,:scope,:operation,:run,:work,:key,'starting','',"
                "'pending',0,0,:deadline) "
                "ON CONFLICT (scope_id,remote_request_key) DO NOTHING",
                self._schema,
            ),
            {
                "id": execution_id,
                "scope": scope,
                "operation": invocation.operation_id,
                "run": invocation.run_id,
                "work": work_id,
                "key": str(invocation.operation_id),
                "deadline": deadline,
            },
        )
        found = (
            (
                await db.execute(
                    scoped(
                        "SELECT id,operation_id,run_id,supervisor_work_id,status "
                        "FROM {t}execution WHERE scope_id=:scope AND remote_request_key=:key",
                        self._schema,
                    ),
                    {"scope": scope, "key": str(invocation.operation_id)},
                )
            )
            .mappings()
            .one()
        )
        if (
            found["id"] != execution_id
            or found["operation_id"] != invocation.operation_id
            or found["run_id"] != invocation.run_id
            or found["supervisor_work_id"] != work_id
            or found["status"] in {"failed", "cancelled"}
        ):
            errors.version_conflict("external execution identity changed")
        if found["status"] in {"starting", "unknown"}:
            changed = await db.execute(
                scoped(
                    "UPDATE {t}execution SET status='running',failure=NULL,updated_at=:now "
                    "WHERE scope_id=:scope AND id=:id AND status IN ('starting','unknown')",
                    self._schema,
                ),
                {"now": now, "scope": scope, "id": execution_id},
            )
            if getattr(changed, "rowcount", None) != 1:
                errors.version_conflict("external execution was claimed concurrently")

    async def _finish_execution(self, db: AsyncSession, invocation: ToolInvocation) -> None:
        assert invocation.operation_id is not None
        scope = invocation.actor.scope_id
        row = (
            (
                await db.execute(
                    scoped(
                        "UPDATE {t}execution SET status='completed',"
                        "cleanup_status='completed',failure=NULL,updated_at=:now "
                        "WHERE scope_id=:scope AND operation_id=:operation "
                        "AND status IN ('starting','running','unknown') "
                        "RETURNING supervisor_work_id",
                        self._schema,
                    ),
                    {
                        "now": datetime.now(UTC),
                        "scope": scope,
                        "operation": invocation.operation_id,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            errors.version_conflict("external execution was resolved concurrently")
        changed = await db.execute(
            scoped(
                "UPDATE {t}work_item SET status='completed',owner_id=NULL,lease_until=NULL,"
                "lease_epoch=lease_epoch+1,updated_at=:now "
                "WHERE scope_id=:scope AND id=:work AND status IN ('pending','leased')",
                self._schema,
            ),
            {"now": datetime.now(UTC), "scope": scope, "work": row["supervisor_work_id"]},
        )
        if getattr(changed, "rowcount", None) != 1:
            errors.version_conflict("execution supervision work was not active")

    async def _mark_unknown(self, invocation: ToolInvocation) -> None:
        async with self._sessions() as db, db.begin():
            await db.execute(
                scoped(
                    "UPDATE {t}operation SET status='unknown',updated_at=:now "
                    "WHERE scope_id=:scope AND id=:id AND status='dispatching'",
                    self._schema,
                ),
                {
                    "now": datetime.now(UTC),
                    "scope": invocation.actor.scope_id,
                    "id": invocation.operation_id,
                },
            )
            await db.execute(
                scoped(
                    "UPDATE {t}execution SET status='unknown',updated_at=:now "
                    "WHERE scope_id=:scope AND operation_id=:id "
                    "AND status IN ('starting','running')",
                    self._schema,
                ),
                {
                    "now": datetime.now(UTC),
                    "scope": invocation.actor.scope_id,
                    "id": invocation.operation_id,
                },
            )
            await db.execute(
                scoped(
                    "UPDATE {t}work_item SET available_at=:now,updated_at=:now "
                    "WHERE scope_id=:scope AND id IN "
                    "(SELECT supervisor_work_id FROM {t}execution "
                    "WHERE scope_id=:scope AND operation_id=:id) AND status='pending'",
                    self._schema,
                ),
                {
                    "now": datetime.now(UTC),
                    "scope": invocation.actor.scope_id,
                    "id": invocation.operation_id,
                },
            )

    def _unknown(self, invocation: ToolInvocation) -> ToolFailed:
        return ToolFailed(
            tool_call_id=invocation.call.tool_call_id,
            run_id=invocation.run_id,
            operation_id=invocation.operation_id,
            status="failed",
            effect="unknown",
            effect_refs=(),
            failure=FailureEnvelope(
                code="DEPENDENCY_UNAVAILABLE",
                category="tool",
                retryable=False,
                public_message="The external result is unknown; reconcile before retrying.",
                operation_id=str(invocation.operation_id),
            ),
        )


__all__ = [
    "IdempotentExternalTool",
    "SqlToolHost",
    "ToolEffect",
    "ToolSchemaValidator",
    "TransactionalTool",
    "TransactionAuthorizer",
]

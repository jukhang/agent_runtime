"""Seal validated sandbox results before cleanup, then retry durable wake delivery."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.payloads import ToolFinished
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.state_events import (
    ServiceEventAuthority,
    StateEventBatch,
    ToolFinishedDraft,
)
from agent_runtime.contracts.tasks import WaitSpec
from agent_runtime.contracts.tools import (
    FullResult,
    ReferenceResult,
    SummaryResult,
    ToolCompleted,
    ToolFailed,
    ToolInvocation,
    ToolSpec,
)
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.work_claims import MaintenanceClaimedWork
from agent_runtime.sandbox.runtime import SandboxBackend
from agent_runtime.sandbox.supervision import STOP_CATEGORIES
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.tool_host import ToolEffect, ToolSchemaValidator
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.storage.wake import SqlWaitWaker, WakeRequest
from agent_runtime.tool_transcript import ToolTranscript
from agent_runtime.tool_validation import validate_tool_outcome

type ResultReader = Callable[[SandboxBackend, dict[str, Any]], Awaitable[ToolEffect]]
type ResultAuthorizer = Callable[[AsyncSession, ToolInvocation], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class SandboxResult:
    invocation: ToolInvocation
    spec: ToolSpec
    outcome: ToolCompleted | ToolFailed
    ref: str
    digest: str
    collection_ref: str
    collection_digest: str


class SqlSandboxResultCollector:
    """Host reader must bound file/log reads and exclude credentials/private data.

    Result authorization validates current scope/ACL/resource generations without
    requiring the relinquished main lease. The maintenance worker owns the fenced
    transaction; this collector never dispatches or retries remote code.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        payloads: BlobContextPayloadStore,
        journal: SqlJournal,
        schemas: ToolSchemaValidator,
        reader: ResultReader,
        authorize: ResultAuthorizer,
        waker: SqlWaitWaker,
        capacity: SqlToolCapacity | None = None,
    ) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._engine = engine
        self._schema = require_schema(schema)
        if capacity is not None and (
            capacity.engine is not engine or capacity.schema != self._schema
        ):
            raise ValueError("sandbox result capacity must use the same engine and schema")
        self._actor, self._payloads, self._journal = actor, payloads, journal
        self._schemas, self._reader, self._authorize, self._waker = (
            schemas,
            reader,
            authorize,
            waker,
        )
        self._capacity = capacity

    @property
    def capacity(self) -> SqlToolCapacity | None:
        return self._capacity

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    @property
    def schema(self) -> str:
        return self._schema

    async def read(
        self, backend: SandboxBackend, row: dict[str, Any], payload: dict[str, Any]
    ) -> SandboxResult:
        if row["status"] not in {"completed", "failed"}:
            errors.version_conflict("sandbox output cannot be sealed without exit evidence")
        invocation = ToolInvocation.model_validate(payload["invocation"])
        spec = ToolSpec.model_validate(payload["spec"])
        if (
            invocation.actor != self._actor
            or invocation.run_id != row["run_id"]
            or invocation.operation_id != row["operation_id"]
            or spec.ref != invocation.call.tool
            or payload["call"] != invocation.call.model_dump(mode="json", by_alias=True)
        ):
            errors.forbidden("sandbox result has a different owner or tool")
        effect = await self._reader(
            backend,
            {
                **payload,
                "remoteExecutionId": row["remote_execution_id"],
                "logCursor": row["log_cursor"],
                "executionStatus": row["status"],
            },
        )
        validation_failed = False
        if row["status"] == "completed":
            try:
                self._schemas.output(spec, effect.data)
            except AgentRuntimeError as exc:
                if exc.failure.category != "validation":
                    raise
                validation_failed = True
            except ValueError:
                validation_failed = True
        if any(
            ref.scope.scope_id != self._actor.scope_id or ref.resource_version is None
            for ref in effect.refs
        ):
            errors.forbidden("sandbox result contains unpinned or foreign resources")
        collection: dict[str, object] = {
            "executionId": str(row["id"]),
            "generation": row["generation"],
            "status": row["status"],
            "exitEvidence": row["failure"],
            "data": effect.data,
            "summary": effect.summary,
        }
        collection_digest = content_digest(collection)
        collection_ref = await self._payloads.put(
            self._actor.scope_id, collection, collection_digest
        )
        if row["status"] == "completed" and not validation_failed:
            result = (
                FullResult(policy="full", data=effect.data)
                if spec.public_result_policy == "full"
                else SummaryResult(policy="summary", text=effect.summary)
                if spec.public_result_policy == "summary"
                else ReferenceResult(policy="reference_only", refs=effect.refs)
            )
            outcome: ToolCompleted | ToolFailed = ToolCompleted(
                status="completed",
                tool_call_id=invocation.call.tool_call_id,
                run_id=invocation.run_id,
                operation_id=invocation.operation_id,
                effect="committed",
                effect_refs=effect.refs,
                result=result,
            )
        else:
            stop_category = str((row["failure"] or {}).get("category") or "")
            stopped_by_policy = stop_category in STOP_CATEGORIES
            outcome = ToolFailed(
                status="failed",
                tool_call_id=invocation.call.tool_call_id,
                run_id=invocation.run_id,
                operation_id=invocation.operation_id,
                effect="committed",
                effect_refs=effect.refs,
                failure=FailureEnvelope(
                    code="INVALID_INPUT"
                    if validation_failed
                    else "BUDGET_EXHAUSTED"
                    if stop_category in {"quota_exceeded", "execution_deadline"}
                    else "DEPENDENCY_UNAVAILABLE",
                    category="tool",
                    retryable=False,
                    detail_ref=collection_ref,
                    public_message=(
                        "The sandbox output did not match its result schema. "
                        if validation_failed
                        else {
                            "quota_exceeded": "The sandbox exceeded its resource limits. ",
                            "execution_deadline": "The sandbox exceeded its execution deadline. ",
                            "execution_cancelled": "The sandbox execution was cancelled. ",
                            "quota_inspection_unavailable": "Sandbox quota verification failed. ",
                        }[stop_category]
                        if stopped_by_policy
                        else "The sandbox command exited unsuccessfully. "
                    )
                    + "Inspect the saved output before choosing another action.",
                ),
            )
        validate_tool_outcome(spec, invocation, outcome)
        # Model history has one strict call/result contract; execution details
        # remain in the separately sealed collection and execution ledger.
        observation = ToolTranscript(call=invocation.call, outcome=outcome).model_dump(
            mode="json", by_alias=True
        )
        digest = content_digest(observation)
        ref = await self._payloads.put(self._actor.scope_id, observation, digest)
        return SandboxResult(
            invocation, spec, outcome, ref, digest, collection_ref, collection_digest
        )

    async def seal(self, db: AsyncSession, row: dict[str, Any], result: SandboxResult) -> None:
        """Called only inside the supervisor's revalidated maintenance transaction."""
        invocation, outcome = result.invocation, result.outcome
        await self._authorize(db, invocation)
        if (
            row["run_id"] != invocation.run_id
            or row["operation_id"] != invocation.operation_id
            or row["status"] not in {"completed", "failed"}
        ):
            errors.version_conflict("execution changed before result sealing")
        tool = (
            (
                await db.execute(
                    scoped(
                        "SELECT * FROM {t}tool_call WHERE scope_id=:scope AND id=:id FOR UPDATE",
                        self._schema,
                    ),
                    {"scope": self._actor.scope_id, "id": outcome.tool_call_id},
                )
            )
            .mappings()
            .one()
        )
        if (
            tool["run_id"] != invocation.run_id
            or tool["operation_id"] != invocation.operation_id
            or tool["attempt_id"] != invocation.model_attempt_id
            or tool["input_hash"] != content_digest(invocation.call.arguments)
            or tool["spec_revision"] != invocation.call.tool.model_dump(mode="json", by_alias=True)
        ):
            errors.version_conflict("sandbox result does not match the durable tool call")
        if tool["status"] in {"completed", "failed"}:
            if tool["result_ref"] != result.ref:
                errors.version_conflict("sandbox tool already has another sealed result")
            return
        if tool["status"] != "running":
            errors.version_conflict("pending execution observation is not committed yet")
        blob = (
            await db.execute(
                scoped(
                    "SELECT digest FROM {t}context_blob WHERE scope_id=:scope "
                    "AND blob_key=:ref FOR KEY SHARE",
                    self._schema,
                ),
                {"scope": self._actor.scope_id, "ref": result.ref},
            )
        ).scalar_one_or_none()
        if blob != result.digest:
            errors.version_conflict("sandbox output blob is not sealed")
        detail_digest = (
            await db.execute(
                scoped(
                    "SELECT digest FROM {t}context_blob WHERE scope_id=:scope "
                    "AND blob_key=:ref FOR KEY SHARE",
                    self._schema,
                ),
                {"scope": self._actor.scope_id, "ref": result.collection_ref},
            )
        ).scalar_one_or_none()
        if detail_digest != result.collection_digest:
            errors.version_conflict("sandbox collected output is not sealed")
        if self._capacity is not None:
            await self._capacity.settle_in_transaction(db, invocation, result.spec)
        now = datetime.now(UTC)
        failure = (
            outcome.failure.model_dump_json(by_alias=True)
            if isinstance(outcome, ToolFailed)
            else None
        )
        await db.execute(
            scoped(
                "UPDATE {t}tool_call SET status=:status,result_ref=:ref,"
                "failure=CAST(:failure AS jsonb),"
                "finished_at=:now,updated_at=:now WHERE scope_id=:scope AND id=:id",
                self._schema,
            ),
            {
                "scope": self._actor.scope_id,
                "id": outcome.tool_call_id,
                "status": outcome.status,
                "ref": result.ref,
                "failure": failure,
                "now": now,
            },
        )
        await db.execute(
            scoped(
                "UPDATE {t}operation SET status='committed',result_ref=:ref,"
                "failure=CAST(:failure AS jsonb),"
                "updated_at=:now WHERE scope_id=:scope AND id=:id "
                "AND status IN ('dispatching','unknown')",
                self._schema,
            ),
            {
                "scope": self._actor.scope_id,
                "id": invocation.operation_id,
                "ref": result.ref,
                "failure": failure,
                "now": now,
            },
        )
        run = (
            (
                await db.execute(
                    scoped(
                        "SELECT session_id,turn_id,parent_run_id FROM {t}run "
                        "WHERE scope_id=:scope AND id=:id",
                        self._schema,
                    ),
                    {"scope": self._actor.scope_id, "id": invocation.run_id},
                )
            )
            .mappings()
            .one()
        )
        await self._journal.append_state_in_transaction(
            db,
            StateEventBatch(
                authority=ServiceEventAuthority(
                    kind="service",
                    actor=self._actor,
                    purpose="tool_result",
                    source=ResourceRef(
                        resource_type="execution",
                        resource_id=str(row["id"]),
                        resource_version=str(row["generation"]),
                        scope=ResourceScope(
                            scope_id=self._actor.scope_id, session_id=str(run["session_id"])
                        ),
                    ),
                    transaction_id=row["supervisor_work_id"],
                ),
                session_id=run["session_id"],
                trace_id=str(row["id"]),
                events=(
                    ToolFinishedDraft(
                        kind="tool.finished",
                        producer_key=f"tool-finish:{outcome.tool_call_id}",
                        occurred_at=now,
                        turn_id=run["turn_id"],
                        run_id=invocation.run_id,
                        parent_run_id=run["parent_run_id"],
                        step_id=None,
                        item_id=None,
                        model_attempt_id=invocation.model_attempt_id,
                        payload=ToolFinished(
                            tool_call_id=outcome.tool_call_id,
                            status=outcome.status,
                            result_refs=outcome.effect_refs,
                            failure=outcome.failure if isinstance(outcome, ToolFailed) else None,
                            finished_at=now,
                        ),
                    ),
                ),
            ),
        )

    async def notify(self, work: MaintenanceClaimedWork, row: dict[str, Any]) -> bool:
        """False retains the durable maintenance work for a later notification attempt."""
        async with self._sessions() as db:
            run = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}run WHERE scope_id=:scope AND id=:id", self._schema
                        ),
                        {"scope": work.lease.scope_id, "id": row["run_id"]},
                    )
                )
                .mappings()
                .one()
            )
            checkpoint_ref = (
                await db.execute(
                    scoped(
                        "SELECT state_ref FROM {t}checkpoint WHERE scope_id=:scope AND id=:id",
                        self._schema,
                    ),
                    {"scope": work.lease.scope_id, "id": run["checkpoint_id"]},
                )
            ).scalar_one()
        if run["status"] in {"completed", "failed", "cancelled"}:
            return True
        if isinstance(checkpoint_ref, str) and checkpoint_ref.startswith("context-"):
            checkpoint = await self._payloads.get(work.lease.scope_id, checkpoint_ref)
            dispatch = await self._payloads.get(work.lease.scope_id, row["payload_ref"])
            if checkpoint.get("executionResumed") is True and checkpoint.get(
                "attemptId"
            ) == dispatch.get("attemptId"):
                return True
        if run["status"] != "waiting_tool" or run["desired_control"] is not None:
            # Completion can precede the owner's wait transaction. Keep notifying
            # until a wait is actually applied or the owner has reached a terminal state.
            return False
        spec = WaitSpec.model_validate(run["waiting_spec"])
        if spec.kind != "tool":
            return False
        try:
            await self._waker.wake(
                WakeRequest(
                    self._actor,
                    run["session_id"],
                    run["id"],
                    run["state_version"],
                    "tool",
                    spec.subject_id,
                    work,
                )
            )
        except AgentRuntimeError as exc:
            if exc.failure.code in {"VERSION_CONFLICT", "CONTROL_PENDING"}:
                return False
            raise
        return True

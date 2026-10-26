"""Independent leased supervision and retryable cleanup of dedicated sandboxes."""

import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.work_claims import MaintenanceClaimedWork, WorkClaimResult
from agent_runtime.sandbox.runtime import SandboxBackend
from agent_runtime.sandbox.supervision import (
    STOP_CATEGORIES,
    ExecutionLimits,
    ExecutionUsage,
    observed_status,
    quota_failure,
)
from agent_runtime.storage.sandbox_results import SandboxResult, SqlSandboxResultCollector
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.worker import WorkHandler

SandboxConnection = Callable[[str], Awaitable[SandboxBackend]]
ResourceInspector = Callable[[SandboxBackend, dict[str, Any]], Awaitable[ExecutionUsage]]


class SqlSandboxMaintenanceWorker:
    """WorkerRunner maintenance handler; never needs a main Run lease or slot.

    A completed process is persisted before a later claim attempts cleanup. The
    Operation stays unknown until the injected result collector seals a validated
    ToolResult. Cleanup and durable owner notification remain separate obligations.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        connect: SandboxConnection,
        confirm_deleted: Callable[[str], Awaitable[bool]],
        payloads: BlobContextPayloadStore,
        api_timeout_seconds: float = 15,
        retry_seconds: int = 5,
        results: SqlSandboxResultCollector | None = None,
        inspect_resources: ResourceInspector | None = None,
    ) -> None:
        if not 0 < api_timeout_seconds <= 300 or retry_seconds < 1:
            raise ValueError("invalid sandbox maintenance bounds")
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._schema = require_schema(schema)
        self._connect, self._payloads = connect, payloads
        self._confirm_deleted = confirm_deleted
        self._timeout, self._retry = api_timeout_seconds, retry_seconds
        self._results = results
        self._inspect_resources = inspect_resources

    async def _stop_intent(
        self, work: MaintenanceClaimedWork, row: dict[str, Any], reason: dict[str, Any]
    ) -> dict[str, Any]:
        """Commit the reason before interrupt; a restarted owner must keep stopping."""
        async with self._sessions() as db, db.begin():
            current = await self._lock(db, work)
            if (
                current["status"] != row["status"]
                or current["remote_execution_id"] != row["remote_execution_id"]
                or current["payload_ref"] != row["payload_ref"]
            ):
                errors.version_conflict("execution changed before stop intent")
            previous = current["failure"]
            if isinstance(previous, dict) and previous.get("category") in STOP_CATEGORIES:
                return previous
            await db.execute(
                scoped(
                    "UPDATE {t}execution SET failure=CAST(:failure AS jsonb),updated_at=:now "
                    "WHERE scope_id=:scope AND id=:id",
                    self._schema,
                ),
                {
                    "scope": work.lease.scope_id,
                    "id": row["id"],
                    "failure": json.dumps(reason),
                    "now": datetime.now(UTC),
                },
            )
        return reason

    async def _lock(self, db: AsyncSession, work: MaintenanceClaimedWork) -> dict[str, Any]:
        # Start/reuse transactions lock Operation before Execution. Keep the same
        # order here so supervision cannot deadlock a concurrent handle save/reuse.
        operation = (
            await db.execute(
                scoped(
                    "SELECT operation_id FROM {t}execution WHERE scope_id=:scope AND id=:id",
                    self._schema,
                ),
                {"scope": work.lease.scope_id, "id": work.subject_ref.resource_id},
            )
        ).scalar_one_or_none()
        if operation is None:
            errors.version_conflict("sandbox execution is missing")
        await db.execute(
            scoped(
                "SELECT id FROM {t}operation WHERE scope_id=:scope AND id=:id FOR UPDATE",
                self._schema,
            ),
            {"scope": work.lease.scope_id, "id": operation},
        )
        row = (
            (
                await db.execute(
                    scoped(
                        "SELECT e.*,w.payload_ref,w.status AS work_status,w.owner_id,w.lease_epoch,"
                        "w.lease_until,w.run_id AS work_run_id,"
                        "r.desired_control,r.status AS run_status,"
                        "o.status AS operation_status,o.result_ref AS operation_result_ref "
                        "FROM {t}execution e JOIN {t}work_item w "
                        "ON w.scope_id=e.scope_id AND w.id=e.supervisor_work_id "
                        "JOIN {t}run r ON r.scope_id=e.scope_id AND r.id=e.run_id "
                        "JOIN {t}operation o ON o.scope_id=e.scope_id AND o.id=e.operation_id "
                        "WHERE e.scope_id=:scope AND e.id=:id FOR UPDATE OF e,w",
                        self._schema,
                    ),
                    {"scope": work.lease.scope_id, "id": work.subject_ref.resource_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if (
            row is None
            or row["supervisor_work_id"] != work.lease.work_id
            or row["work_run_id"] != row["run_id"]
            or str(row["generation"]) != work.subject_ref.resource_version
            or row["work_status"] != "leased"
            or row["owner_id"] != work.lease.owner_id
            or int(row["lease_epoch"]) != int(work.lease.work_epoch)
            or row["lease_until"] <= datetime.now(UTC)
        ):
            errors.version_conflict("sandbox maintenance claim was fenced")
        # This adapter owns dedicated sandboxes only. Other executions, including
        # uncertain ones, must not lose their environment during cleanup.
        shared = (
            await db.execute(
                scoped(
                    "SELECT id FROM {t}execution WHERE sandbox_id=:sandbox AND id<>:id LIMIT 1",
                    self._schema,
                ),
                {"sandbox": row["sandbox_id"], "id": row["id"]},
            )
        ).first()
        if shared is not None:
            errors.forbidden("sandbox is shared by another unresolved execution")
        return dict(row)

    async def __call__(self, claim: WorkClaimResult) -> None:
        import asyncio

        if (
            not isinstance(claim, MaintenanceClaimedWork)
            or claim.kind != "reconcile_execution"
            or claim.subject_ref.resource_type != "execution"
            or claim.subject_ref.scope.scope_id != claim.lease.scope_id
        ):
            errors.invalid_input("sandbox worker requires scoped execution maintenance work")
        async with self._sessions() as db, db.begin():
            row = await self._lock(db, claim)
        payload = await self._payloads.get(claim.lease.scope_id, row["payload_ref"])
        if (
            payload.get("kind") != "sandbox_dispatch"
            or payload.get("scopeId") != claim.lease.scope_id
            or payload.get("executionId") != str(row["id"])
            or payload.get("generation") != row["generation"]
            or payload.get("sandboxId") != row["sandbox_id"]
        ):
            errors.version_conflict("sandbox maintenance binding is corrupt")
        terminal = row["status"] in {"completed", "failed", "cancelled"}
        status = "unknown"
        exit_code: int | None = None
        cleaned = row["cleanup_status"] == "completed"
        backend: SandboxBackend | None = None
        error: str | None = None
        result: SandboxResult | None = None
        stop_reason = row["failure"]
        if not isinstance(stop_reason, dict) or stop_reason.get("category") not in STOP_CATEGORIES:
            stop_reason = None
        notified = self._results is None
        if terminal and row["operation_status"] == "committed" and self._results is not None:
            notified = await self._results.notify(claim, row)
        try:
            async with asyncio.timeout(self._timeout):
                if cleaned:
                    status = row["status"]
                else:
                    backend = await self._connect(row["sandbox_id"])
                if backend is None:
                    pass
                elif backend.sandbox_id != row["sandbox_id"]:
                    errors.forbidden("sandbox connector returned another sandbox")
                elif terminal:
                    if (
                        not cleaned
                        and row["operation_status"] == "committed"
                        and row["operation_result_ref"] is not None
                    ):
                        cleaned = await backend.destroy()
                    elif not cleaned:
                        if self._results is not None:
                            result = await self._results.read(backend, row, payload)
                        else:
                            error = "result_collection_required"
                    status = row["status"]
                elif row["remote_execution_id"] is not None:
                    usage: ExecutionUsage | None = None
                    if "limits" in payload:
                        try:
                            limits = ExecutionLimits.parse(payload["limits"])
                            if self._inspect_resources is None:
                                raise ValueError("trusted resource inspector required")
                            async with asyncio.timeout(self._timeout / 3):
                                usage = await self._inspect_resources(
                                    backend,
                                    {**payload, "remoteExecutionId": row["remote_execution_id"]},
                                )
                            measured = quota_failure(
                                limits,
                                usage,
                                sandbox_id=row["sandbox_id"],
                                command_id=row["remote_execution_id"],
                            )
                            stop_reason = stop_reason or measured
                        except Exception:
                            usage = None
                            stop_reason = stop_reason or {
                                "category": "quota_inspection_unavailable"
                            }
                    cancel = (
                        row["desired_control"] == "cancel"
                        or row["run_status"] in {"cancelling", "cancelled"}
                        or row["deadline_at"] <= datetime.now(UTC)
                    )
                    if cancel and stop_reason is None:
                        stop_reason = {
                            "category": "execution_deadline"
                            if row["deadline_at"] <= datetime.now(UTC)
                            else "execution_cancelled"
                        }
                    if stop_reason is not None:
                        stop_reason = await self._stop_intent(claim, row, stop_reason)
                        try:
                            await backend.interrupt(row["remote_execution_id"])
                        except Exception:
                            pass
                    remote = await backend.status(row["remote_execution_id"])
                    status = observed_status(
                        remote,
                        stop_requested=stop_reason is not None,
                        tree_stopped=(usage.process_tree_stopped if usage else False)
                        if "limits" in payload
                        else None,
                    )
                    if status in {"completed", "failed"}:
                        exit_code = remote.exit_code
                else:
                    # The remote start response was lost: no query-by-id is possible.
                    # Do not dispatch again or delete a potentially running sandbox.
                    error = "dispatch_handle_unknown"
        except Exception:
            error = "cleanup_unconfirmed" if terminal else "inspector_unavailable"
            if (
                terminal
                and row["operation_status"] == "committed"
                and row["operation_result_ref"] is not None
            ):
                try:
                    async with asyncio.timeout(self._timeout):
                        cleaned = await self._confirm_deleted(row["sandbox_id"])
                except Exception:
                    pass
        finally:
            if backend is not None:
                try:
                    async with asyncio.timeout(self._timeout):
                        await backend.aclose()
                except Exception:
                    # Closing local connections does not invalidate remote evidence.
                    pass
        now = datetime.now(UTC)
        async with self._sessions() as db, db.begin():
            current = await self._lock(db, claim)
            if (
                current["status"] != row["status"]
                or current["remote_execution_id"] != row["remote_execution_id"]
                or current["payload_ref"] != row["payload_ref"]
            ):
                errors.version_conflict("sandbox execution changed during inspection")
            failure = (
                {**stop_reason, "exitCode": exit_code}
                if stop_reason is not None
                else {"category": error}
                if error
                else {"category": "process_exit", "exitCode": exit_code}
                if exit_code is not None and exit_code != 0
                else None
            )
            if terminal:
                failure = current["failure"]
            if result is not None and self._results is not None:
                await self._results.seal(db, current, result)
            await db.execute(
                scoped(
                    "UPDATE {t}execution SET status=:status,cleanup_status=:cleanup,"
                    "failure=CAST(:failure AS jsonb),updated_at=:now "
                    "WHERE scope_id=:scope AND id=:id",
                    self._schema,
                ),
                {
                    "scope": claim.lease.scope_id,
                    "id": row["id"],
                    "status": row["status"] if terminal else status,
                    "cleanup": "completed"
                    if cleaned
                    else "failed"
                    if terminal and error != "result_collection_required"
                    else "pending",
                    "failure": json.dumps(failure) if failure else None,
                    "now": now,
                },
            )
            if status in {"completed", "failed"} or terminal:
                await db.execute(
                    scoped(
                        "UPDATE {t}operation SET status='unknown',updated_at=:now "
                        "WHERE scope_id=:scope AND id=:id AND status='dispatching'",
                        self._schema,
                    ),
                    {"scope": claim.lease.scope_id, "id": row["operation_id"], "now": now},
                )
            done = terminal and cleaned and notified
            await db.execute(
                scoped(
                    "UPDATE {t}work_item SET status=:status,owner_id=NULL,lease_until=NULL,"
                    "lease_epoch=lease_epoch+1,attempt_count=attempt_count+1,available_at=:available,"
                    "deadline_at=GREATEST(deadline_at,:deadline),updated_at=:now "
                    "WHERE scope_id=:scope AND id=:id",
                    self._schema,
                ),
                {
                    "scope": claim.lease.scope_id,
                    "id": claim.lease.work_id,
                    "status": "completed" if done else "pending",
                    "available": now + timedelta(seconds=self._retry),
                    "deadline": now + timedelta(minutes=10),
                    "now": now,
                },
            )


class SqlExecutionMaintenanceRouter:
    """Route existing external-effect and sandbox work using the persisted payload.

    Both injected handlers must enforce their own maintenance lease. Unknown blob
    work is not downgraded to the legacy inspector if sandbox validation rejects it.
    """

    def __init__(
        self, engine: AsyncEngine, schema: str, *, sandbox: WorkHandler, external: WorkHandler
    ) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._schema = require_schema(schema)
        self._sandbox, self._external = sandbox, external

    async def __call__(self, work: WorkClaimResult) -> None:
        if not isinstance(work, MaintenanceClaimedWork) or work.kind != "reconcile_execution":
            errors.invalid_input("execution router requires maintenance work")
        async with self._sessions() as db:
            ref = (
                await db.execute(
                    scoped(
                        "SELECT payload_ref FROM {t}work_item WHERE scope_id=:scope AND id=:id "
                        "AND status='leased' AND owner_id=:owner AND lease_epoch=:epoch "
                        "AND lease_until>:now",
                        self._schema,
                    ),
                    {
                        "scope": work.lease.scope_id,
                        "id": work.lease.work_id,
                        "owner": work.lease.owner_id,
                        "epoch": int(work.lease.work_epoch),
                        "now": datetime.now(UTC),
                    },
                )
            ).scalar_one_or_none()
        if not isinstance(ref, str):
            errors.version_conflict("execution router claim was fenced")
        handler = self._sandbox if ref.startswith("context-") else self._external
        await handler(work)


__all__ = ["SqlSandboxMaintenanceWorker", "SqlExecutionMaintenanceRouter", "SandboxConnection"]

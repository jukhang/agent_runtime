"""Fenced maintenance reconciliation for externally supervised executions."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.contracts.work_claims import MaintenanceClaimedWork, WorkClaimResult
from agent_runtime.storage.subagents import SqlSubagentRuntime
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped

type RemoteState = Literal["running", "completed", "stopped", "unknown"]


class RemoteExecutionInspector(Protocol):
    async def inspect(self, remote_request_key: str) -> RemoteState:
        """Read remote state without invoking the effect again."""

    async def cancel(self, remote_request_key: str) -> None:
        """Request remote cancellation; this alone is not proof of process termination."""


class SqlExecutionSupervisor:
    """Resolve an Execution only with a live maintenance lease and remote evidence.

    A confirmed remote effect does not synthesize a lost Agent Observation. The
    Operation remains unknown until a separate, authorized recovery path links
    the confirmed effect to a persisted result.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        inspector: RemoteExecutionInspector,
        inspector_for_work: Callable[[MaintenanceClaimedWork], RemoteExecutionInspector]
        | None = None,
        capacity: SqlToolCapacity,
        subagents: SqlSubagentRuntime | None = None,
        retry_seconds: int = 5,
        supervision_window_seconds: int = 900,
    ) -> None:
        if retry_seconds < 1 or supervision_window_seconds < retry_seconds:
            raise ValueError("execution supervision retry/window interval is invalid")
        self._schema = require_schema(schema)
        if capacity.engine is not engine or capacity.schema != self._schema:
            raise ValueError("execution capacity must use the same engine and schema")
        self._engine = engine
        if subagents is not None and subagents.schema != self._schema:
            raise ValueError("execution supervisor and subagent runtime use different schemas")
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._inspector = inspector
        self._inspector_for_work = inspector_for_work
        self._capacity = capacity
        self._retry_seconds = retry_seconds
        self._supervision_window_seconds = supervision_window_seconds
        self._subagents = subagents

    @property
    def capacity(self) -> SqlToolCapacity:
        return self._capacity

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    @property
    def schema(self) -> str:
        return self._schema

    async def __call__(self, work: WorkClaimResult) -> None:
        if not isinstance(work, MaintenanceClaimedWork) or work.kind != "reconcile_execution":
            errors.invalid_input("execution supervisor received non-execution work")
        if work.subject_ref.resource_type != "execution":
            errors.invalid_input("execution supervisor subject is not an execution")
        scope = work.lease.scope_id
        execution_id = work.subject_ref.resource_id
        async with self._sessions() as db:
            row = (
                (
                    await db.execute(
                        scoped(
                            "SELECT e.id,e.operation_id,e.run_id,e.supervisor_work_id,"
                            "e.remote_request_key,e.generation,e.status,e.deadline_at,"
                            "r.desired_control,r.status AS run_status,r.session_id,"
                            "r.state_version,r.waiting_spec,r.parent_run_id,"
                            "w.run_id AS work_run_id,"
                            "w.status AS work_status,w.owner_id,"
                            "w.lease_epoch,w.lease_until FROM {t}execution e JOIN {t}run r "
                            "ON r.scope_id=e.scope_id AND r.id=e.run_id "
                            "JOIN {t}work_item w ON w.scope_id=e.scope_id "
                            "AND w.id=e.supervisor_work_id "
                            "WHERE e.scope_id=:scope AND e.id=:execution",
                            self._schema,
                        ),
                        {"scope": scope, "execution": execution_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
        if (
            row is None
            or row["supervisor_work_id"] != work.lease.work_id
            or row["work_run_id"] != row["run_id"]
        ):
            errors.version_conflict("execution supervision ownership changed")
        if str(row["generation"]) != work.subject_ref.resource_version:
            errors.version_conflict("execution generation changed")
        child_waiting = row["parent_run_id"] is not None and row["run_status"] == "waiting_tool"
        if child_waiting:
            if self._subagents is None:
                errors.version_conflict("parked child execution needs a subagent recovery adapter")
            if self._subagents.actor.scope_id != scope:
                errors.forbidden("parked child execution adapter belongs to another scope")
        if row["status"] in {"completed", "failed", "cancelled"} and not child_waiting:
            errors.version_conflict("execution was already resolved")
        if (
            row["work_status"] != "leased"
            or row["owner_id"] != work.lease.owner_id
            or int(row["lease_epoch"]) != int(work.lease.work_epoch)
            or row["lease_until"] <= datetime.now(UTC)
        ):
            errors.version_conflict("execution supervision lease was fenced")
        key = str(row["remote_request_key"])
        should_cancel = row["desired_control"] == "cancel" or row["deadline_at"] <= datetime.now(
            UTC
        )
        remote: RemoteState = (
            "completed"
            if row["status"] == "completed"
            else "stopped"
            if row["status"] == "cancelled"
            else "unknown"
        )
        inspection_error = False
        if row["status"] not in {"completed", "failed", "cancelled"}:
            try:
                inspector = (
                    self._inspector_for_work(work)
                    if self._inspector_for_work is not None
                    else self._inspector
                )
                if should_cancel:
                    await inspector.cancel(key)
                remote = await inspector.inspect(key)
            except Exception:
                # An unreachable inspector cannot prove that the remote process stopped.
                inspection_error = True
        now = datetime.now(UTC)
        async with self._sessions() as db, db.begin():
            execution = (
                (
                    await db.execute(
                        scoped(
                            "SELECT id,operation_id,run_id,supervisor_work_id,generation,status "
                            "FROM {t}execution WHERE scope_id=:scope AND id=:execution FOR UPDATE",
                            self._schema,
                        ),
                        {"scope": scope, "execution": execution_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            item = (
                (
                    await db.execute(
                        scoped(
                            "SELECT run_id,status,owner_id,lease_epoch,lease_until "
                            "FROM {t}work_item "
                            "WHERE scope_id=:scope AND id=:work FOR UPDATE",
                            self._schema,
                        ),
                        {"scope": scope, "work": work.lease.work_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if (
                execution is None
                or item is None
                or execution["supervisor_work_id"] != work.lease.work_id
                or execution["run_id"] != item["run_id"]
                or str(execution["generation"]) != work.subject_ref.resource_version
                or (
                    execution["status"] in {"completed", "failed", "cancelled"}
                    and not child_waiting
                )
                or item["status"] != "leased"
                or item["owner_id"] != work.lease.owner_id
                or int(item["lease_epoch"]) != int(work.lease.work_epoch)
                or item["lease_until"] <= now
            ):
                errors.version_conflict("execution supervision lease was fenced")
            if remote in {"completed", "stopped"}:
                final_status = "completed" if remote == "completed" else "cancelled"
                await self._capacity.settle_execution_in_transaction(
                    db, scope, execution["run_id"], execution["operation_id"]
                )
                await db.execute(
                    scoped(
                        "UPDATE {t}execution SET status=:status,"
                        "failure=NULL,updated_at=:now WHERE scope_id=:scope AND id=:execution",
                        self._schema,
                    ),
                    {"status": final_status, "now": now, "scope": scope, "execution": execution_id},
                )
                await db.execute(
                    scoped(
                        "UPDATE {t}operation SET status='unknown',"
                        "failure=CAST(:failure AS jsonb),updated_at=:now "
                        "WHERE scope_id=:scope AND id=:operation "
                        "AND status IN ('dispatching','unknown')",
                        self._schema,
                    ),
                    {
                        "failure": json.dumps(
                            {"category": "reconciled_external_effect", "remoteStatus": remote}
                        ),
                        "now": now,
                        "scope": scope,
                        "operation": execution["operation_id"],
                    },
                )
                if not child_waiting:
                    await db.execute(
                        scoped(
                            "UPDATE {t}work_item SET status='completed',owner_id=NULL,"
                            "lease_until=NULL,lease_epoch=lease_epoch+1,updated_at=:now "
                            "WHERE scope_id=:scope AND id=:work",
                            self._schema,
                        ),
                        {"now": now, "scope": scope, "work": work.lease.work_id},
                    )
            else:
                await db.execute(
                    scoped(
                        "UPDATE {t}execution SET status=:status,"
                        "failure=CAST(:failure AS jsonb),updated_at=:now "
                        "WHERE scope_id=:scope AND id=:execution",
                        self._schema,
                    ),
                    {
                        "status": "running"
                        if remote == "running" and not inspection_error
                        else "unknown",
                        "failure": None
                        if remote == "running" and not inspection_error
                        else json.dumps(
                            {
                                "category": (
                                    "inspector_unavailable"
                                    if inspection_error
                                    else "remote_execution_incomplete"
                                )
                            }
                        ),
                        "now": now,
                        "scope": scope,
                        "execution": execution_id,
                    },
                )
                await db.execute(
                    scoped(
                        "UPDATE {t}work_item SET status='pending',owner_id=NULL,"
                        "lease_until=NULL,lease_epoch=lease_epoch+1,attempt_count=attempt_count+1,"
                        "available_at=:available,deadline_at=GREATEST(deadline_at,:deadline),"
                        "updated_at=:now "
                        "WHERE scope_id=:scope AND id=:work",
                        self._schema,
                    ),
                    {
                        "available": now + timedelta(seconds=self._retry_seconds),
                        "deadline": now + timedelta(seconds=self._supervision_window_seconds),
                        "now": now,
                        "scope": scope,
                        "work": work.lease.work_id,
                    },
                )

        if remote in {"completed", "stopped"} and child_waiting:
            assert self._subagents is not None
            await self._subagents.fail_reconciled_external(work, UUID(str(row["operation_id"])))


__all__ = ["RemoteExecutionInspector", "RemoteState", "SqlExecutionSupervisor"]

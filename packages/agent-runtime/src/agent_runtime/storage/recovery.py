"""Lease recovery and external-execution supervision for the PostgreSQL adapter."""

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.ownership import ChildLeaseToken
from agent_runtime.contracts.scheduler import ClaimedWork
from agent_runtime.contracts.transactions import LeaseToken, MaintenanceLease
from agent_runtime.contracts.work_claims import (
    ChildClaimedWork,
    ControlClaimedWork,
    MaintenanceClaimedWork,
    WorkClaimResult,
)
from agent_runtime.storage.child_recovery import (
    cancel_child_in_transaction,
    finalize_child_failure,
    lock_recovery_runs,
    unresolved_child_effects,
)
from agent_runtime.storage.model_retry import propose_model_retry
from agent_runtime.storage.uow import locks
from agent_runtime.storage.uow.schema import require_schema, scoped

Clock = Callable[[], datetime]


def _external_supervision(work: Mapping[str, Any]) -> bool:
    subject = work["subject_ref"]
    return (
        work["kind"] == "reconcile_execution"
        and isinstance(subject, Mapping)
        and subject.get("resourceType") == "execution"
        and isinstance(work["payload_ref"], str)
        and (
            work["payload_ref"] == f"execution:{subject.get('resourceId')}"
            # Sandbox dispatches use immutable, scoped context Blobs. The
            # maintenance handler still validates that Blob against Execution;
            # recognizing it here grants retry, never remote execution authority.
            or work["payload_ref"].startswith("context-")
        )
    )


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class RecoveryConfig:
    scan_limit: int = 128
    maximum_attempts: int = 5
    base_retry_seconds: int = 2
    maximum_retry_seconds: int = 300
    supervision_deadline_seconds: int = 900

    def __post_init__(self) -> None:
        if self.scan_limit < 1 or self.scan_limit > 2048:
            raise ValueError("scan_limit must be between 1 and 2048")
        if self.maximum_attempts < 1:
            raise ValueError("maximum_attempts must be positive")
        if self.base_retry_seconds < 1 or self.maximum_retry_seconds < self.base_retry_seconds:
            raise ValueError("retry interval is invalid")
        if self.supervision_deadline_seconds < 1:
            raise ValueError("supervision deadline must be positive")


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    requeued: int = 0
    failed: int = 0
    fenced: int = 0
    supervision_created: int = 0
    supervision_reused: int = 0
    cancelled: int = 0


@dataclass(frozen=True, slots=True)
class FailureSettlement:
    disposition: str
    work_id: UUID
    failure: FailureEnvelope
    reason: str | None = None
    retry_at: datetime | None = None


class SqlRecovery:
    """Converge expired database leases and persist external supervision work."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        clock: Clock = utcnow,
        config: RecoveryConfig | None = None,
    ) -> None:
        self._engine = engine
        self._schema = require_schema(schema)
        self._clock = clock
        self._config = config or RecoveryConfig()
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._locks = locks.build_lock_statements(self._schema)
        self._expired_sql = scoped(
            "SELECT w.id, w.run_id, w.kind, r.session_id, r.parent_run_id, "
            "r.desired_control "
            "FROM {t}work_item w LEFT JOIN {t}run r "
            "ON r.scope_id = w.scope_id AND r.id = w.run_id "
            "WHERE w.scope_id = :scope AND w.status = 'leased' AND w.lease_until <= :now "
            "ORDER BY w.lease_until, w.id LIMIT :limit",
            self._schema,
        )
        self._expired_pending_sql = scoped(
            "SELECT w.id, w.run_id, w.kind, r.session_id, r.parent_run_id, "
            "r.desired_control FROM {t}work_item w LEFT JOIN {t}run r "
            "ON r.scope_id = w.scope_id AND r.id = w.run_id "
            "WHERE w.scope_id = :scope AND w.status = 'pending' AND w.deadline_at <= :now "
            "ORDER BY w.deadline_at, w.id LIMIT :limit",
            self._schema,
        )
        self._stale_execution_sql = scoped(
            "SELECT id FROM {t}execution WHERE scope_id = :scope "
            "AND status IN ('pending','starting','running','cancelling','unknown') "
            "AND (deadline_at <= :now OR cleanup_status IN ('pending','failed')) "
            "ORDER BY deadline_at, id LIMIT :limit",
            self._schema,
        )
        self._execution_lock_sql = scoped(
            "SELECT * FROM {t}execution WHERE scope_id = :scope AND id = :id FOR UPDATE",
            self._schema,
        )

    async def reconcile_scope(self, scope_id: str) -> RecoveryReport:
        """Run one bounded recovery pass; callers schedule repeated passes durably."""
        cancelled = await self.recover_cancelled_children(scope_id)
        expired = await self.recover_expired_leases(scope_id)
        waiting = await self.expire_message_waits(scope_id)
        supervision = await self.schedule_stale_executions(scope_id)
        return RecoveryReport(
            requeued=expired.requeued,
            failed=expired.failed + waiting,
            fenced=expired.fenced,
            supervision_created=supervision.supervision_created,
            supervision_reused=supervision.supervision_reused,
            cancelled=cancelled,
        )

    async def recover_cancelled_children(self, scope_id: str) -> int:
        """Service-owned cancellation, including children stranded by a terminal root."""
        now = self._aware_now()
        async with self._sessions() as db:
            ids = (
                (
                    await db.execute(
                        scoped(
                            "SELECT c.id FROM {t}run c JOIN {t}run p "
                            "ON p.scope_id=c.scope_id AND p.id=c.parent_run_id "
                            "WHERE c.scope_id=:scope "
                            "AND c.status NOT IN ('completed','failed','cancelled') "
                            "AND (c.desired_control='cancel' OR p.desired_control='cancel' "
                            "OR p.status='cancelled') "
                            "ORDER BY c.updated_at,c.id LIMIT :limit",
                            self._schema,
                        ),
                        {"scope": scope_id, "limit": self._config.scan_limit},
                    )
                )
                .scalars()
                .all()
            )
        count = 0
        for run_id in ids:
            async with self._sessions() as db, db.begin():
                _, child, parent = await lock_recovery_runs(
                    db, self._schema, self._locks, scope_id, run_id
                )
                if (
                    child is None
                    or parent is None
                    or child["session_id"] != parent["session_id"]
                    or child["root_run_id"] != parent["id"]
                    or child["status"] in {"completed", "failed", "cancelled"}
                    or not (
                        child["desired_control"] == "cancel"
                        or parent["desired_control"] == "cancel"
                        or parent["status"] == "cancelled"
                    )
                ):
                    continue
                work_ids = (
                    (
                        await db.execute(
                            scoped(
                                "SELECT id FROM {t}work_item WHERE scope_id=:scope AND run_id=:run "
                                "AND status IN ('pending','leased') AND "
                                "(kind='advance_run' OR payload_ref='cancel-convergence') "
                                "ORDER BY id",
                                self._schema,
                            ),
                            {"scope": scope_id, "run": run_id},
                        )
                    )
                    .scalars()
                    .all()
                )
                for work_id in work_ids:
                    await locks.lock_work_item(db, self._locks, scope_id, work_id)
                count += await cancel_child_in_transaction(
                    db, self._engine, self._schema, child, parent, now
                )
                await db.execute(
                    scoped(
                        "UPDATE {t}run SET updated_at=:now WHERE scope_id=:scope AND id=:run",
                        self._schema,
                    ),
                    {"scope": scope_id, "run": run_id, "now": now},
                )
        return count

    async def expire_message_waits(self, scope_id: str) -> int:
        now = self._aware_now()
        async with self._sessions() as db:
            rows = (
                await db.execute(
                    scoped(
                        "SELECT id,state_version,waiting_spec->>'subjectId' AS subject FROM {t}run "
                        "WHERE scope_id=:scope AND parent_run_id IS NULL AND status='waiting_user' "
                        "AND desired_control IS NULL AND waiting_spec->>'kind'='message' "
                        "AND (deadline_at<=:now OR "
                        "CAST(waiting_spec->>'deadlineAt' AS timestamptz)<=:now) "
                        "ORDER BY updated_at,id LIMIT :limit",
                        self._schema,
                    ),
                    {"scope": scope_id, "now": now, "limit": self._config.scan_limit},
                )
            ).all()
        count = 0
        for row in rows:
            count += await self._fail_waiting(
                scope_id,
                row.id,
                row.subject,
                row.state_version,
                FailureEnvelope(
                    code="DELIVERY_EXPIRED",
                    category="delivery",
                    retryable=False,
                    public_message="The delegated result wait expired.",
                ),
                kind="message",
            )
        return count

    async def fail_waiting_approval(
        self,
        scope_id: str,
        run_id: UUID,
        approval_id: UUID,
        expected_state_version: int,
        failure: FailureEnvelope,
    ) -> bool:
        """Fail a revoked root approval wait without acquiring a new user grant."""
        return await self._fail_waiting(
            scope_id, run_id, str(approval_id), expected_state_version, failure, kind="approval"
        )

    async def fail_waiting_external(
        self,
        scope_id: str,
        run_id: UUID,
        subject_id: str,
        expected_state_version: int,
        failure: FailureEnvelope,
    ) -> bool:
        """A trusted scanner closes expired or definitively revoked external waits."""
        if failure.code not in {"FORBIDDEN", "DELIVERY_EXPIRED"}:
            raise ValueError("External wait cleanup requires definite revocation or expiry")
        return await self._fail_waiting(
            scope_id, run_id, subject_id, expected_state_version, failure, kind="external"
        )

    async def _fail_waiting(
        self,
        scope_id: str,
        run_id: UUID,
        subject_id: str,
        expected_state_version: int,
        failure: FailureEnvelope,
        *,
        kind: Literal["approval", "external", "message"],
    ) -> bool:
        now = self._aware_now()
        status = "waiting_tool" if kind == "external" else "waiting_user"
        async with self._sessions() as db, db.begin():
            run_session = (
                await db.execute(
                    scoped(
                        "SELECT session_id FROM {t}run WHERE scope_id=:scope AND id=:run",
                        self._schema,
                    ),
                    {"scope": scope_id, "run": run_id},
                )
            ).scalar_one_or_none()
            if run_session is None:
                return False
            slot = await locks.lock_slot(db, self._locks, scope_id, run_session)
            # Child completion takes the same Slot and sorted Run locks before Session.
            children = (
                (
                    await db.execute(
                        scoped(
                            "SELECT id FROM {t}run WHERE scope_id=:scope AND parent_run_id=:run",
                            self._schema,
                        ),
                        {"scope": scope_id, "run": run_id},
                    )
                )
                .scalars()
                .all()
            )
            rows = {
                key: await locks.lock_run(db, self._locks, scope_id, key)
                for key in sorted({run_id, *children}, key=str)
            }
            run = rows[run_id]
            if (
                run is None
                or run["parent_run_id"] is not None
                or run["status"] != status
                or run["desired_control"] is not None
                or int(run["state_version"]) != expected_state_version
                or not isinstance(run["waiting_spec"], dict)
                or run["waiting_spec"].get("kind") != kind
                or run["waiting_spec"].get("subjectId") != subject_id
            ):
                return False
            if (
                kind in {"external", "message"}
                and failure.code == "DELIVERY_EXPIRED"
                and (
                    run["deadline_at"] > now
                    and datetime.fromisoformat(run["waiting_spec"]["deadlineAt"]) > now
                )
            ):
                return False
            approval = (
                (
                    await db.execute(
                        scoped(
                            "SELECT status FROM {t}approval WHERE scope_id=:scope "
                            "AND id=:approval AND run_id=:run",
                            self._schema,
                        ),
                        {"scope": scope_id, "approval": UUID(subject_id), "run": run_id},
                    )
                ).scalar_one_or_none()
                if kind == "approval"
                else None
            )
            if kind == "approval" and approval not in {
                "approved",
                "rejected",
                "expired",
                "cancelled",
            }:
                return False
            active = (
                await db.execute(
                    scoped(
                        "SELECT EXISTS(SELECT 1 FROM {t}model_attempt "
                        "WHERE scope_id=:scope AND run_id=:run "
                        "AND status IN ('pending','running')) "
                        "OR EXISTS(SELECT 1 FROM {t}execution WHERE scope_id=:scope "
                        "AND run_id=:run AND status NOT IN ('completed','failed','cancelled')) "
                        "OR EXISTS(SELECT 1 FROM {t}run WHERE scope_id=:scope "
                        "AND parent_run_id=:run "
                        "AND status NOT IN ('completed','failed','cancelled')) "
                        "OR EXISTS(SELECT 1 FROM {t}work_item WHERE scope_id=:scope "
                        "AND run_id=:run AND status='leased') "
                        "OR EXISTS(SELECT 1 FROM {t}tool_call "
                        "WHERE scope_id=:scope AND run_id=:run AND status='running') "
                        "OR EXISTS(SELECT 1 FROM {t}operation o JOIN {t}tool_call t "
                        "ON t.scope_id=o.scope_id AND t.operation_id=o.id "
                        "WHERE t.scope_id=:scope AND t.run_id=:run "
                        "AND o.status IN ('prepared','dispatching','unknown'))",
                        self._schema,
                    ),
                    {"scope": scope_id, "run": run_id},
                )
            ).scalar_one()
            if kind == "message" and not active:
                for child_id in children:
                    child = rows[child_id]
                    if child is not None and await unresolved_child_effects(
                        db, self._schema, child
                    ):
                        active = True
                        break
            if active:
                # Rotate blocked waits so a fixed first page cannot starve cleanup.
                if kind == "message":
                    await db.execute(
                        scoped(
                            "UPDATE {t}run SET updated_at=:now WHERE scope_id=:scope AND id=:run",
                            self._schema,
                        ),
                        {"scope": scope_id, "run": run_id, "now": now},
                    )
                return False
            work_ids = (
                (
                    await db.execute(
                        scoped(
                            "SELECT id FROM {t}work_item WHERE scope_id=:scope AND run_id=:run "
                            "AND kind='advance_run' AND status='pending' ORDER BY id",
                            self._schema,
                        ),
                        {"scope": scope_id, "run": run_id},
                    )
                )
                .scalars()
                .all()
            )
            for work_id in work_ids:
                await locks.lock_work_item(db, self._locks, scope_id, work_id)
            from agent_runtime.storage.terminal_failure import finalize_root_failure

            settled = await finalize_root_failure(db, self._engine, self._schema, run, failure, now)
            await db.execute(
                scoped(
                    "UPDATE {t}run SET status='failed',phase='terminal',waiting_spec=NULL,"
                    "owner_id=NULL,lease_until=NULL,owner_epoch=owner_epoch+1,"
                    "failure=CAST(:failure AS jsonb),phase_started_at=:now,updated_at=:now "
                    "WHERE scope_id=:scope AND id=:run AND status=:waiting_status",
                    self._schema,
                ),
                {
                    "scope": scope_id,
                    "run": run_id,
                    "failure": json.dumps(settled.model_dump(mode="json", by_alias=True)),
                    "now": now,
                    "waiting_status": status,
                },
            )
            if slot is not None and slot["owner_run_id"] == run_id:
                await db.execute(
                    scoped(
                        "UPDATE {t}session_slot SET owner_run_id=NULL,"
                        "slot_version=slot_version+1,updated_at=:now "
                        "WHERE scope_id=:scope AND session_id=:session AND owner_run_id=:run",
                        self._schema,
                    ),
                    {
                        "scope": scope_id,
                        "session": run_session,
                        "run": run_id,
                        "now": now,
                    },
                )
            return True

    async def handle_worker_failure(
        self, work: WorkClaimResult, exc: BaseException
    ) -> FailureSettlement:
        """Durably settle a handler error; suitable as ``WorkerRunner.error_sink``."""
        if isinstance(exc, AgentRuntimeError):
            failure = exc.failure
        else:
            failure = FailureEnvelope(
                code="INTERNAL",
                category="internal",
                retryable=False,
                public_message="The worker failed while executing this operation.",
            )
        if isinstance(work, ClaimedWork):
            return await self._settle_main_failure(
                work.lease, failure, budget_id=work.budget_allocation_id
            )
        if isinstance(work, ChildClaimedWork):
            return await self._settle_child_failure(work.lease, failure)
        if isinstance(work, ControlClaimedWork):
            if isinstance(work.lease, LeaseToken):
                return await self._settle_main_failure(work.lease, failure)
            return await self._settle_child_failure(work.lease, failure)
        if isinstance(work, MaintenanceClaimedWork):
            return await self._settle_maintenance_failure(work.lease, failure)
        raise ValueError("idle work cannot fail execution")

    async def recover_expired_leases(self, scope_id: str) -> RecoveryReport:
        now = self._aware_now()
        async with self._sessions() as session:
            ids = tuple(
                dict(row)
                for row in (
                    await session.execute(
                        self._expired_sql,
                        {"scope": scope_id, "now": now, "limit": self._config.scan_limit},
                    )
                ).mappings()
            )
        requeued = failed = fenced = 0
        for candidate in ids:
            disposition = await self._recover_one(scope_id, candidate, now)
            if disposition == "requeued":
                requeued += 1
            elif disposition == "failed":
                failed += 1
            else:
                fenced += 1
        async with self._sessions() as session:
            pending = tuple(
                dict(row)
                for row in (
                    await session.execute(
                        self._expired_pending_sql,
                        {"scope": scope_id, "now": now, "limit": self._config.scan_limit},
                    )
                ).mappings()
            )
        for candidate in pending:
            disposition = await self._expire_pending_one(scope_id, candidate, now)
            if disposition == "requeued":
                requeued += 1
            elif disposition == "failed":
                failed += 1
            else:
                fenced += 1
        return RecoveryReport(requeued=requeued, failed=failed, fenced=fenced)

    async def schedule_stale_executions(self, scope_id: str) -> RecoveryReport:
        now = self._aware_now()
        async with self._sessions() as session:
            ids = tuple(
                row[0]
                for row in (
                    await session.execute(
                        self._stale_execution_sql,
                        {"scope": scope_id, "now": now, "limit": self._config.scan_limit},
                    )
                )
            )
        created = reused = 0
        for execution_id in ids:
            if await self._ensure_supervision(scope_id, execution_id, now):
                created += 1
            else:
                reused += 1
        return RecoveryReport(supervision_created=created, supervision_reused=reused)

    async def _recover_one(self, scope_id: str, candidate: Mapping[str, Any], now: datetime) -> str:
        async with self._sessions() as session, session.begin():
            run: Mapping[str, Any] | None = None
            slot: Mapping[str, Any] | None = None
            parent: Mapping[str, Any] | None = None
            run_id = candidate["run_id"]
            if run_id is not None:
                slot, run, parent = await lock_recovery_runs(
                    session, self._schema, self._locks, scope_id, run_id
                )
            work = await locks.lock_work_item(session, self._locks, scope_id, candidate["id"])
            if (
                work is None
                or work["status"] != "leased"
                or work["lease_until"] is None
                or work["lease_until"] > now
            ):
                return "fenced"
            owns_run_lease = bool(
                run is not None
                and run["owner_id"] == work["owner_id"]
                and run["lease_until"] is not None
                and run["lease_until"] <= now
                and (work["kind"] == "advance_run" or run["desired_control"] is not None)
            )
            if owns_run_lease and run is not None and parent is not None:
                if run["desired_control"] is None and await unresolved_child_effects(
                    session, self._schema, run
                ):
                    await self._defer_child_effects(session, run, work, now)
                    return "requeued"
            retryable = (
                int(work["attempt_count"]) < self._config.maximum_attempts
                and work["deadline_at"] > now
            )
            supervision = _external_supervision(work) or bool(
                owns_run_lease and run is not None and run["desired_control"] is not None
            )
            if supervision:
                retryable = True
            if (
                not supervision
                and run is not None
                and run["status"] in {"completed", "failed", "cancelled", "paused"}
            ):
                retryable = False
            if retryable:
                delay = min(
                    self._config.maximum_retry_seconds,
                    self._config.base_retry_seconds
                    * 2 ** max(min(int(work["attempt_count"]), 20) - 1, 0),
                )
                await session.execute(
                    sa.text(
                        f"UPDATE {self._schema}.work_item SET status = 'pending', "
                        "owner_id = NULL, lease_until = NULL, lease_epoch = lease_epoch + 1, "
                        "available_at = :available, "
                        "deadline_at = CASE WHEN :supervision THEN GREATEST(deadline_at,:deadline) "
                        "ELSE deadline_at END, updated_at = :now WHERE id = :id"
                    ),
                    {
                        "available": now + timedelta(seconds=delay),
                        "supervision": supervision,
                        "deadline": now
                        + timedelta(seconds=self._config.supervision_deadline_seconds),
                        "now": now,
                        "id": work["id"],
                    },
                )
                if owns_run_lease and run is not None:
                    await self._release_run(session, run, slot, now, retry=True, parent=parent)
                return "requeued"
            deadline_expired = work["deadline_at"] <= now
            failure = FailureEnvelope(
                code="DELIVERY_EXPIRED" if deadline_expired else "INTERNAL",
                category="delivery" if deadline_expired else "internal",
                retryable=False,
                public_message="The runtime could not safely resume this work item.",
                deadline_at=work["deadline_at"],
            )
            await session.execute(
                sa.text(
                    f"UPDATE {self._schema}.work_item SET status = 'failed', owner_id = NULL, "
                    "lease_until = NULL, lease_epoch = lease_epoch + 1, "
                    "failure = CAST(:failure AS jsonb), "
                    "updated_at = :now WHERE id = :id"
                ),
                {
                    "failure": json.dumps(failure.model_dump(mode="json", by_alias=True)),
                    "now": now,
                    "id": work["id"],
                },
            )
            if owns_run_lease and run is not None:
                await self._release_run(
                    session, run, slot, now, retry=False, failure=failure, parent=parent
                )
            return "failed"

    async def _expire_pending_one(
        self, scope_id: str, candidate: Mapping[str, Any], now: datetime
    ) -> Literal["requeued", "failed", "fenced"]:
        async with self._sessions() as session, session.begin():
            run: Mapping[str, Any] | None = None
            slot: Mapping[str, Any] | None = None
            parent: Mapping[str, Any] | None = None
            if candidate["run_id"] is not None:
                slot, run, parent = await lock_recovery_runs(
                    session, self._schema, self._locks, scope_id, candidate["run_id"]
                )
            work = await locks.lock_work_item(session, self._locks, scope_id, candidate["id"])
            if work is None or work["status"] != "pending" or work["deadline_at"] > now:
                return "fenced"
            if run is not None and work["kind"] == "advance_run" and run["owner_id"] is not None:
                # An old pending envelope must not terminate a different live owner.
                return "fenced"
            if (
                run is not None
                and run["desired_control"] is not None
                and (work["kind"] == "advance_run" or work["payload_ref"] == "cancel-convergence")
            ):
                await self._requeue_control(session, work, now)
                return "requeued"
            if (
                run is not None
                and parent is not None
                and work["kind"] == "advance_run"
                and run["status"] not in {"completed", "failed", "cancelled", "paused"}
                and await unresolved_child_effects(session, self._schema, run)
            ):
                await self._defer_child_effects(session, run, work, now)
                return "requeued"
            if _external_supervision(work):
                await session.execute(
                    sa.text(
                        f"UPDATE {self._schema}.work_item SET "
                        "deadline_at=:deadline,updated_at=:now WHERE id=:id"
                    ),
                    {
                        "deadline": now
                        + timedelta(seconds=self._config.supervision_deadline_seconds),
                        "now": now,
                        "id": work["id"],
                    },
                )
                return "requeued"
            failure = FailureEnvelope(
                code="DELIVERY_EXPIRED",
                category="delivery",
                retryable=False,
                public_message="The work item expired before a worker could execute it.",
                deadline_at=work["deadline_at"],
            )
            await session.execute(
                sa.text(
                    f"UPDATE {self._schema}.work_item SET status = 'failed', "
                    "lease_epoch = lease_epoch + 1, failure = CAST(:failure AS jsonb), "
                    "updated_at = :now WHERE id = :id"
                ),
                {
                    "failure": json.dumps(failure.model_dump(mode="json", by_alias=True)),
                    "now": now,
                    "id": work["id"],
                },
            )
            if (
                run is not None
                and work["kind"] == "advance_run"
                and run["status"] not in {"completed", "failed", "cancelled", "paused"}
            ):
                await self._release_run(
                    session,
                    run,
                    slot,
                    now,
                    retry=False,
                    failure=failure,
                    parent=parent,
                )
            return "failed"

    async def _settle_main_failure(
        self, lease: LeaseToken, failure: FailureEnvelope, *, budget_id: UUID | None = None
    ) -> FailureSettlement:
        now = self._aware_now()
        async with self._sessions() as session, session.begin():
            slot = await locks.lock_slot(session, self._locks, lease.scope_id, lease.session_id)
            run = await locks.lock_run(session, self._locks, lease.scope_id, lease.run_id)
            work = await locks.lock_work_item(session, self._locks, lease.scope_id, lease.work_id)
            now = self._aware_now()
            if not self._main_claim_matches(slot, run, work, lease, now):
                return FailureSettlement("fenced", lease.work_id, failure)
            assert run is not None and work is not None
            reason: str | None = None
            retry_at: datetime | None = None
            input_pending = (
                run["pending_input_version"] > run["applied_input_version"]
                and run["deadline_at"] > now
            )
            if run["desired_control"] is not None or input_pending:
                # Interruption is not a task failure. Keep a claimable safe-point work
                # even if the model's CONTROL_PENDING error is non-retryable or the
                # normal retry budget was exhausted.
                await self._requeue_control(
                    session,
                    work,
                    now,
                    maximum_deadline=(
                        None if run["desired_control"] is not None else run["deadline_at"]
                    ),
                )
                disposition = "requeued"
                reason = (
                    "control_pending" if run["desired_control"] is not None else "input_pending"
                )
            else:
                choice = await propose_model_retry(
                    session,
                    self._schema,
                    lease,
                    run,
                    work,
                    failure,
                    budget_id=budget_id,
                    clock=self._aware_now,
                    maximum_attempts=self._config.maximum_attempts,
                    base_backoff=timedelta(seconds=self._config.base_retry_seconds),
                    max_backoff=timedelta(seconds=self._config.maximum_retry_seconds),
                )
                reason, retry_at = choice.reason, choice.retry_at
                if not self._main_claim_matches(slot, run, work, lease, self._aware_now()):
                    return FailureSettlement("fenced", lease.work_id, failure)
                disposition = await self._settle_locked_work(
                    session,
                    work,
                    failure,
                    now,
                    model_retry_at=retry_at,
                    restrict_model_retry=True,
                )
            await self._release_run(
                session,
                run,
                slot,
                now,
                retry=disposition == "requeued",
                failure=failure,
            )
        return FailureSettlement(disposition, lease.work_id, failure, reason, retry_at)

    async def _settle_child_failure(
        self, lease: ChildLeaseToken, failure: FailureEnvelope
    ) -> FailureSettlement:
        now = self._aware_now()
        async with self._sessions() as session, session.begin():
            slot, run, parent = await lock_recovery_runs(
                session, self._schema, self._locks, lease.scope_id, lease.run_id
            )
            work = await locks.lock_work_item(session, self._locks, lease.scope_id, lease.work_id)
            if not self._child_claim_matches(run, work, lease, now):
                return FailureSettlement("fenced", lease.work_id, failure)
            assert run is not None and work is not None
            if run["desired_control"] is not None:
                await self._requeue_control(session, work, now)
                disposition = "requeued"
            elif await unresolved_child_effects(session, self._schema, run):
                await self._defer_child_effects(session, run, work, now)
                return FailureSettlement("requeued", lease.work_id, failure)
            else:
                disposition = await self._settle_locked_work(session, work, failure, now)
            await self._release_run(
                session,
                run,
                slot,
                now,
                retry=disposition == "requeued",
                failure=failure,
                parent=parent,
            )
            return FailureSettlement(disposition, lease.work_id, failure)

    async def _requeue_control(
        self,
        db: AsyncSession,
        work: Mapping[str, Any],
        now: datetime,
        *,
        maximum_deadline: datetime | None = None,
    ) -> None:
        await db.execute(
            scoped(
                "UPDATE {t}work_item SET status='pending',owner_id=NULL,lease_until=NULL,"
                "lease_epoch=lease_epoch+1,available_at=:now,"
                "deadline_at=CASE WHEN :bounded THEN LEAST(deadline_at,:deadline) "
                "ELSE GREATEST(deadline_at,:deadline) END,failure=NULL,updated_at=:now "
                "WHERE scope_id=:scope AND id=:id",
                self._schema,
            ),
            {
                "scope": work["scope_id"],
                "id": work["id"],
                "now": now,
                "deadline": maximum_deadline
                or now + timedelta(seconds=self._config.supervision_deadline_seconds),
                "bounded": maximum_deadline is not None,
            },
        )

    async def _defer_child_effects(
        self, db: AsyncSession, run: Mapping[str, Any], work: Mapping[str, Any], now: datetime
    ) -> None:
        # available_at == deadline_at makes this a recovery-only envelope: the
        # scheduler cannot replay it, but the pending-expiry scan will revisit it.
        revisit = now + timedelta(seconds=self._config.base_retry_seconds)
        await db.execute(
            scoped(
                "UPDATE {t}work_item SET status='pending',owner_id=NULL,lease_until=NULL,"
                "lease_epoch=lease_epoch+1,available_at=:revisit,deadline_at=:revisit,"
                "updated_at=:now "
                "WHERE scope_id=:scope AND id=:id",
                self._schema,
            ),
            {"scope": run["scope_id"], "id": work["id"], "revisit": revisit, "now": now},
        )
        await db.execute(
            scoped(
                "UPDATE {t}run SET owner_id=NULL,lease_until=NULL,owner_epoch=owner_epoch+1,"
                "updated_at=:now WHERE scope_id=:scope AND id=:id",
                self._schema,
            ),
            {"scope": run["scope_id"], "id": run["id"], "now": now},
        )

    async def _settle_maintenance_failure(
        self, lease: MaintenanceLease, failure: FailureEnvelope
    ) -> FailureSettlement:
        now = self._aware_now()
        async with self._sessions() as session, session.begin():
            work = await locks.lock_work_item(session, self._locks, lease.scope_id, lease.work_id)
            if not self._maintenance_claim_matches(work, lease, now):
                return FailureSettlement("fenced", lease.work_id, failure)
            assert work is not None
            disposition = await self._settle_locked_work(session, work, failure, now)
            return FailureSettlement(disposition, lease.work_id, failure)

    async def _settle_locked_work(
        self,
        session: AsyncSession,
        work: Mapping[str, Any],
        failure: FailureEnvelope,
        now: datetime,
        *,
        model_retry_at: datetime | None = None,
        restrict_model_retry: bool = False,
    ) -> str:
        supervision = _external_supervision(work)
        retry = (
            (failure.retryable or supervision)
            and int(work["attempt_count"]) < self._config.maximum_attempts
            and work["deadline_at"] > now
        )
        if supervision:
            retry = True
        elif restrict_model_retry:
            retry = model_retry_at is not None
        status = "pending" if retry else "failed"
        delay = min(
            self._config.maximum_retry_seconds,
            self._config.base_retry_seconds * 2 ** max(min(int(work["attempt_count"]), 20) - 1, 0),
        )
        await session.execute(
            sa.text(
                f"UPDATE {self._schema}.work_item SET status = :status, owner_id = NULL, "
                "lease_until = NULL, lease_epoch = lease_epoch + 1, "
                "available_at = :available, "
                "deadline_at = CASE WHEN :supervision THEN GREATEST(deadline_at,:deadline) "
                "ELSE deadline_at END, failure = CAST(:failure AS jsonb), "
                "updated_at = :now WHERE id = :id"
            ),
            {
                "status": status,
                "available": (model_retry_at or now + timedelta(seconds=delay))
                if retry
                else work["available_at"],
                "supervision": supervision,
                "deadline": now + timedelta(seconds=self._config.supervision_deadline_seconds),
                "failure": json.dumps(failure.model_dump(mode="json", by_alias=True)),
                "now": now,
                "id": work["id"],
            },
        )
        return "requeued" if retry else "failed"

    async def _release_run(
        self,
        session: AsyncSession,
        run: Mapping[str, Any],
        slot: Mapping[str, Any] | None,
        now: datetime,
        *,
        retry: bool,
        failure: FailureEnvelope | None = None,
        parent: Mapping[str, Any] | None = None,
    ) -> None:
        # Callers hold and verify the expiring/current owner fence. Once relinquished,
        # its in-flight attempts cannot be resumed or treated as successful responses.
        # This records unknown usage, not a claim that the remote inference server
        # refunded tokens or stopped its GPU computation.
        if not retry and run["desired_control"] is None:
            live_ids = (
                (
                    await session.execute(
                        scoped(
                            "SELECT id FROM {t}work_item WHERE scope_id=:scope AND run_id=:run "
                            "AND kind='advance_run' AND status IN ('pending','leased') ORDER BY id",
                            self._schema,
                        ),
                        {"scope": run["scope_id"], "run": run["id"]},
                    )
                )
                .scalars()
                .all()
            )
            for work_id in live_ids:
                await locks.lock_work_item(session, self._locks, run["scope_id"], work_id)
        await session.execute(
            scoped(
                "UPDATE {t}model_attempt SET status='interrupted',finish_reason='cancelled',"
                "finished_at=:now,usage_status='unknown',usage_reason='collection_failed',"
                "usage=CAST(:usage AS jsonb),failure=NULL WHERE scope_id=:scope AND run_id=:run "
                "AND status IN ('pending','running')",
                self._schema,
            ),
            {
                "scope": run["scope_id"],
                "run": run["id"],
                "now": now,
                "usage": json.dumps(
                    {
                        "status": "unknown",
                        "reason": "collection_failed",
                        "inputTokens": None,
                        "outputTokens": None,
                        "totalTokens": None,
                    }
                ),
            },
        )
        if run["status"] in {"completed", "failed", "cancelled", "paused"}:
            await session.execute(
                sa.text(
                    f"UPDATE {self._schema}.run SET owner_id = NULL, lease_until = NULL, "
                    "owner_epoch = owner_epoch + 1, updated_at = :now WHERE id = :id"
                ),
                {"now": now, "id": run["id"]},
            )
            status = str(run["status"])
            phase = str(run["phase"])
            failure_json = None if run["failure"] is None else json.dumps(run["failure"])
        elif retry:
            status = "cancelling" if run["status"] == "cancelling" else "queued"
            phase = "cancelling" if status == "cancelling" else "queued"
            failure_json = None
        elif run["status"] == "cancelling":
            # Unknown external side effects cannot be converted into a failed terminal.
            # Keep the cancellation visible until supervision proves every execution stopped.
            status = "cancelling"
            phase = "cancelling"
            failure_json = None
        else:
            status = "failed"
            phase = "terminal"
            failure = failure or self._internal_failure()
            if run["parent_run_id"] is None:
                from agent_runtime.storage.terminal_failure import finalize_root_failure

                failure = await finalize_root_failure(
                    session, self._engine, self._schema, run, failure, now
                )
            else:
                if parent is None or parent["id"] != run["parent_run_id"]:
                    raise ValueError("child recovery requires its locked parent")
                failure = await finalize_child_failure(
                    session, self._engine, self._schema, run, parent, failure, now
                )
            failure_json = json.dumps(failure.model_dump(mode="json", by_alias=True))
        if run["status"] not in {"completed", "failed", "cancelled", "paused"}:
            await session.execute(
                sa.text(
                    f"UPDATE {self._schema}.run SET status = :status, phase = :phase, "
                    "owner_id = NULL, lease_until = NULL, "
                    "owner_epoch = owner_epoch + 1, failure = CAST(:failure AS jsonb), "
                    "phase_started_at = :now, updated_at = :now WHERE id = :id"
                ),
                {
                    "status": status,
                    "phase": phase,
                    "failure": failure_json,
                    "now": now,
                    "id": run["id"],
                },
            )
        # A cancelling root run retains its session slot until terminal convergence.  This
        # prevents unrelated work in the same session from overtaking cancellation while a
        # control worker is reclaiming the expired lease.
        if (
            slot is not None
            and slot["owner_run_id"] == run["id"]
            and status != "cancelling"
            and run["desired_control"] is None
        ):
            await session.execute(
                sa.text(
                    f"UPDATE {self._schema}.session_slot SET owner_run_id = NULL, "
                    "slot_version = slot_version + 1, updated_at = :now "
                    "WHERE scope_id = :scope AND session_id = :session AND owner_run_id = :run"
                ),
                {
                    "now": now,
                    "scope": run["scope_id"],
                    "session": run["session_id"],
                    "run": run["id"],
                },
            )

    async def _ensure_supervision(self, scope_id: str, execution_id: UUID, now: datetime) -> bool:
        async with self._sessions() as session, session.begin():
            execution = (
                (
                    await session.execute(
                        self._execution_lock_sql, {"scope": scope_id, "id": execution_id}
                    )
                )
                .mappings()
                .one_or_none()
            )
            if execution is None or execution["status"] in {"completed", "failed", "cancelled"}:
                return False
            if execution["supervisor_work_id"] is not None:
                existing = await locks.lock_work_item(
                    session, self._locks, scope_id, execution["supervisor_work_id"]
                )
                if existing is not None and existing["status"] in {"pending", "leased"}:
                    return False
            round_number = int(execution["reconciliation_round"]) + 1
            work_id = uuid4()
            dedup_key = f"execution:{execution_id}:reconcile:{round_number}"
            subject = {
                "resourceType": "execution",
                "resourceId": str(execution_id),
                "resourceVersion": str(execution["generation"]),
                "scope": {"scopeId": scope_id, "sessionId": None, "ownerId": None},
            }
            await session.execute(
                sa.text(
                    f"INSERT INTO {self._schema}.work_item "
                    "(id, scope_id, run_id, subject_ref, kind, dedup_key, status, payload_ref, "
                    "wake_reason, priority, lease_epoch, attempt_count, available_at, deadline_at) "
                    "VALUES (:id, :scope, :run, CAST(:subject AS jsonb), 'reconcile_execution', "
                    ":dedup, 'pending', :payload, NULL, 1000, 0, 0, :now, :deadline) "
                    "ON CONFLICT (scope_id, dedup_key) DO NOTHING"
                ),
                {
                    "id": work_id,
                    "scope": scope_id,
                    "run": execution["run_id"],
                    "subject": json.dumps(subject),
                    "dedup": dedup_key,
                    "payload": f"execution:{execution_id}",
                    "now": now,
                    "deadline": now + timedelta(seconds=self._config.supervision_deadline_seconds),
                },
            )
            found = (
                await session.execute(
                    sa.text(
                        f"SELECT id FROM {self._schema}.work_item "
                        "WHERE scope_id = :scope AND dedup_key = :dedup"
                    ),
                    {"scope": scope_id, "dedup": dedup_key},
                )
            ).scalar_one()
            await session.execute(
                sa.text(
                    f"UPDATE {self._schema}.execution SET supervisor_work_id = :work, "
                    "reconciliation_round = :round, updated_at = :now WHERE id = :id"
                ),
                {
                    "work": found,
                    "round": round_number,
                    "now": now,
                    "id": execution_id,
                },
            )
            return bool(found == work_id)

    @staticmethod
    def _internal_failure() -> FailureEnvelope:
        return FailureEnvelope(
            code="INTERNAL",
            category="internal",
            retryable=False,
            public_message="The runtime could not recover this run.",
        )

    @staticmethod
    def _main_claim_matches(
        slot: Mapping[str, Any] | None,
        run: Mapping[str, Any] | None,
        work: Mapping[str, Any] | None,
        lease: LeaseToken,
        now: datetime,
    ) -> bool:
        return bool(
            slot is not None
            and run is not None
            and work is not None
            and slot["owner_run_id"] == lease.run_id
            and int(slot["slot_version"]) == int(lease.slot_version)
            and run["owner_id"] == lease.owner_id
            and int(run["owner_epoch"]) == int(lease.run_epoch)
            and run["lease_until"] is not None
            and run["lease_until"] > now
            and work["status"] == "leased"
            and work["owner_id"] == lease.owner_id
            and int(work["lease_epoch"]) == int(lease.work_epoch)
            and work["lease_until"] is not None
            and work["lease_until"] > now
        )

    @staticmethod
    def _child_claim_matches(
        run: Mapping[str, Any] | None,
        work: Mapping[str, Any] | None,
        lease: ChildLeaseToken,
        now: datetime,
    ) -> bool:
        return bool(
            run is not None
            and work is not None
            and run["owner_id"] == lease.owner_id
            and int(run["owner_epoch"]) == int(lease.run_epoch)
            and run["lease_until"] is not None
            and run["lease_until"] > now
            and work["status"] == "leased"
            and work["owner_id"] == lease.owner_id
            and int(work["lease_epoch"]) == int(lease.work_epoch)
            and work["lease_until"] is not None
            and work["lease_until"] > now
        )

    @staticmethod
    def _maintenance_claim_matches(
        work: Mapping[str, Any] | None,
        lease: MaintenanceLease,
        now: datetime,
    ) -> bool:
        return bool(
            work is not None
            and work["status"] == "leased"
            and work["owner_id"] == lease.owner_id
            and int(work["lease_epoch"]) == int(lease.work_epoch)
            and work["lease_until"] is not None
            and work["lease_until"] > now
        )

    def _aware_now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("recovery clock must return an aware datetime")
        return now


__all__ = ["FailureSettlement", "RecoveryConfig", "RecoveryReport", "SqlRecovery"]

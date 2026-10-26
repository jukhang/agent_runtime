"""PostgreSQL scheduler with durable leases and fencing.

Candidate discovery is deliberately non-owning.  A claim becomes authoritative only after
the transaction locks and revalidates SessionSlot -> Run -> WorkItem (main/control) or
Run -> WorkItem (child).  Maintenance work owns only its WorkItem lease.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import NAMESPACE_URL, UUID, uuid5

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.ownership import ChildLeaseToken
from agent_runtime.contracts.payloads import RunStatusPayload
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.scheduler import (
    ClaimedWork,
    ClaimRequest,
    ClaimResult,
    IdleWork,
    LostLease,
    RenewedLease,
    RenewRequest,
    RenewResult,
    RequeuedRun,
    WaitingRun,
    YieldRequest,
    YieldResult,
)
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.state_events import (
    InputRequiredDraft,
    MainEventAuthority,
    RunStatusDraft,
    StateEventBatch,
    StateEventDraft,
)
from agent_runtime.contracts.transactions import LeaseToken, MaintenanceLease
from agent_runtime.contracts.transitions import StateWriteGuard
from agent_runtime.contracts.views import Phase
from agent_runtime.contracts.work_claims import (
    ChildClaimedWork,
    ChildRenewed,
    ChildRenewRequest,
    ChildRenewResult,
    ControlClaimedWork,
    MaintenanceClaimedWork,
    MaintenanceRenewed,
    MaintenanceRenewRequest,
    MaintenanceRenewResult,
    WorkClaimRequest,
    WorkClaimResult,
)
from agent_runtime.storage.uow import errors, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.facts import verify_lease, verify_run, verify_slot, verify_work
from agent_runtime.storage.uow.schema import require_schema, scoped

if TYPE_CHECKING:
    from agent_runtime.storage.journal import SqlJournal

Clock = Callable[[], datetime]


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    candidate_limit: int = 64
    urgent_deadline_seconds: int = 30
    aging_interval_seconds: int = 30
    maximum_aging_boost: int = 1000
    idle_retry_after_ms: int = 250

    def __post_init__(self) -> None:
        if self.candidate_limit < 1 or self.candidate_limit > 1024:
            raise ValueError("candidate_limit must be between 1 and 1024")
        if self.urgent_deadline_seconds < 0:
            raise ValueError("urgent_deadline_seconds must be non-negative")
        if self.aging_interval_seconds < 1 or self.maximum_aging_boost < 0:
            raise ValueError("aging configuration is invalid")
        if self.idle_retry_after_ms < 1 or self.idle_retry_after_ms > 60_000:
            raise ValueError("idle_retry_after_ms must be between 1 and 60000")


@dataclass(frozen=True, slots=True)
class _Candidate:
    work_id: UUID
    run_id: UUID | None
    session_id: UUID | None
    parent_run_id: UUID | None
    kind: str
    subject_ref: Mapping[str, Any]
    payload_ref: str
    handler_namespace: str | None
    handler_key: str | None
    handler_revision: int | None
    desired_control: str | None


def _lost(code: str, message: str) -> LostLease:
    return LostLease(
        disposition="lost",
        failure=FailureEnvelope(
            code=code,
            category="concurrency",
            retryable=False,
            public_message=message,
        ),
    )


class SqlScheduler:
    """Standard scheduler adapter; dependencies are explicit and instance-owned."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        clock: Clock = utcnow,
        config: SchedulerConfig | None = None,
        journal: SqlJournal | None = None,
    ) -> None:
        self._engine = engine
        self._schema = require_schema(schema)
        self._clock = clock
        self._config = config or SchedulerConfig()
        self._journal = journal
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._locks = locks.build_lock_statements(self._schema)
        self._candidate_sql = scoped(
            "SELECT w.id AS work_id, w.run_id, r.session_id, r.parent_run_id, w.kind, "
            "w.subject_ref, w.payload_ref, w.handler_namespace, w.handler_key, "
            "w.handler_revision, r.desired_control "
            "FROM {t}work_item w LEFT JOIN {t}run r "
            "ON r.scope_id = w.scope_id AND r.id = w.run_id "
            "WHERE w.scope_id = :scope_id AND w.status = 'pending' "
            "AND w.available_at <= :now AND w.deadline_at > :now "
            "AND (CASE WHEN w.kind='reconcile_execution' "
            "AND w.payload_ref='cancel-convergence' AND w.subject_ref->>'kind'='cancel' "
            "AND r.desired_control='cancel' THEN 'control' "
            "WHEN w.kind<>'advance_run' THEN 'maintenance' "
            "WHEN r.desired_control IS NOT NULL THEN 'control' "
            "WHEN r.parent_run_id IS NOT NULL THEN 'child' ELSE 'main' END)=ANY(:lanes) "
            "AND (w.kind='advance_run' OR w.kind=ANY(:maintenance_kinds) "
            "OR (w.kind='reconcile_execution' AND w.payload_ref='cancel-convergence' "
            "AND r.desired_control='cancel')) "
            "AND (w.kind<>'domain_job' OR CAST(:handlers AS jsonb) @> "
            "jsonb_build_array(jsonb_build_object('namespace',w.handler_namespace,"
            "'key',w.handler_key,'revision',w.handler_revision))) "
            "AND ((w.kind<>'advance_run' AND NOT (w.kind='reconcile_execution' "
            "AND w.payload_ref='cancel-convergence' AND r.desired_control='cancel')) "
            "OR (r.owner_id IS NULL AND ("
            "(r.desired_control IS NULL AND r.status='queued') OR "
            "(r.desired_control IS NOT NULL AND r.status IN "
            "('queued','running','waiting_tool','waiting_user','cancelling'))))) "
            "AND ((w.kind<>'advance_run' AND NOT (w.kind='reconcile_execution' "
            "AND w.payload_ref='cancel-convergence' AND r.desired_control='cancel')) "
            "OR r.parent_run_id IS NOT NULL OR EXISTS("
            "SELECT 1 FROM {t}session_slot s WHERE s.scope_id=r.scope_id "
            "AND s.session_id=r.session_id AND (s.owner_run_id IS NULL OR s.owner_run_id=r.id))) "
            "AND (w.kind<>'advance_run' OR r.parent_run_id IS NULL OR "
            "r.desired_control IS NOT NULL OR EXISTS(SELECT 1 FROM {t}run p "
            "WHERE p.scope_id=r.scope_id AND p.id=r.root_run_id "
            "AND p.status NOT IN ('completed','failed','cancelled','cancelling','paused') "
            "AND p.desired_control IS NULL)) "
            "ORDER BY CASE WHEN w.deadline_at <= :urgent_at THEN 0 ELSE 1 END, "
            "(w.priority + LEAST(:aging_cap, FLOOR(EXTRACT(EPOCH FROM "
            "(:now - w.available_at)) / :aging_seconds))) DESC, "
            "w.deadline_at, w.available_at, w.id LIMIT :candidate_limit",
            self._schema,
        )
        self._budget_sql = scoped(
            "SELECT id FROM {t}budget_allocation WHERE scope_id = :scope_id "
            "AND root_run_id = :root_run_id ORDER BY (parent_id IS NULL) DESC, created_at, id "
            "LIMIT 1",
            self._schema,
        )
        self._workspace_sql = scoped(
            "SELECT id, generation, revision FROM {t}workspace WHERE scope_id = :scope_id "
            "AND owner_run_id = :run_id ORDER BY updated_at DESC, id LIMIT 1",
            self._schema,
        )
        self._child_budget_sql = scoped(
            "SELECT b.id FROM {t}budget_allocation b JOIN {t}agent_message m "
            "ON m.scope_id=b.scope_id AND COALESCE(m.payload->'payload'->>'budgetAllocationId',"
            "m.payload->>'budgetAllocationId')=b.id::text "
            "WHERE b.scope_id=:scope_id AND b.root_run_id=:root_run_id "
            "AND b.parent_id IS NOT NULL AND m.recipient_run_id=:run_id "
            "AND m.sender_run_id=:parent_run_id AND m.kind='task.assign' "
            "AND m.task_id=:task_id",
            self._schema,
        )
        self._root_cancel_sql = scoped(
            "SELECT cancel_version FROM {t}run WHERE scope_id = :scope_id AND id = :run_id "
            "AND status NOT IN ('completed','failed','cancelled','cancelling','paused') "
            "AND desired_control IS NULL",
            self._schema,
        )
        self._checkpoint_sql = scoped(
            "SELECT id, run_id, state_version FROM {t}checkpoint "
            "WHERE scope_id = :scope_id AND id = :checkpoint_id",
            self._schema,
        )
        self._work_skip_locked_sql = scoped(
            "SELECT * FROM {t}work_item WHERE scope_id = :scope_id AND id = :work_id "
            "FOR UPDATE SKIP LOCKED",
            self._schema,
        )

    @property
    def emits_state_events(self) -> bool:
        return self._journal is not None

    async def claim(self, request: ClaimRequest) -> ClaimResult:
        expanded = WorkClaimRequest(
            scope_id=request.scope_id,
            worker=request.worker,
            lease_seconds=request.lease_seconds,
            lanes=("main",),
        )
        result = await self.claim_work(expanded)
        if isinstance(result, (ClaimedWork, IdleWork)):
            return result
        return IdleWork(
            disposition="idle",
            reason="no_work",
            retry_after_ms=self._config.idle_retry_after_ms,
        )

    async def claim_work(self, request: WorkClaimRequest) -> WorkClaimResult:
        now = self._aware_now()
        candidates = await self._candidates(request, now)
        capability_mismatch = False
        for candidate in candidates:
            lane = self._lane(candidate)
            if lane not in request.lanes:
                continue
            if lane == "maintenance":
                if candidate.kind not in request.maintenance_kinds:
                    continue
                if not self._supports_maintenance(request, candidate):
                    capability_mismatch = True
                    continue
            claimed = await self._claim_candidate(request, candidate, lane, now)
            if claimed is not None:
                return claimed
        if "maintenance" in request.lanes and "domain_job" in request.maintenance_kinds:
            async with self._sessions() as session:
                capability_mismatch = bool(
                    (
                        await session.execute(
                            scoped(
                                "SELECT EXISTS(SELECT 1 FROM {t}work_item w WHERE scope_id=:scope "
                                "AND status='pending' AND kind='domain_job' AND available_at<=:now "
                                "AND deadline_at>:now AND NOT (CAST(:handlers AS jsonb) @> "
                                "jsonb_build_array(jsonb_build_object('namespace',w.handler_namespace,"
                                "'key',w.handler_key,'revision',w.handler_revision))))",
                                self._schema,
                            ),
                            {
                                "scope": request.scope_id,
                                "now": now,
                                "handlers": json.dumps(
                                    [
                                        {
                                            "namespace": cap.key.namespace,
                                            "key": cap.key.key,
                                            "revision": int(cap.revision),
                                        }
                                        for cap in request.worker.capabilities
                                    ]
                                ),
                            },
                        )
                    ).scalar_one()
                )
        return IdleWork(
            disposition="idle",
            reason="capability_mismatch" if capability_mismatch else "no_work",
            retry_after_ms=self._config.idle_retry_after_ms,
        )

    async def renew(self, request: RenewRequest) -> RenewResult:
        lease = request.lease
        now = self._aware_now()
        if lease.run_lease_until <= now or lease.work_lease_until <= now:
            return _lost("LEASE_EXPIRED", "The run lease expired before renewal.")
        new_until = now + timedelta(seconds=request.lease_seconds)
        async with self._sessions() as session, session.begin():
            slot = await locks.lock_slot(session, self._locks, lease.scope_id, lease.session_id)
            run = await locks.lock_run(session, self._locks, lease.scope_id, lease.run_id)
            work = await locks.lock_work_item(session, self._locks, lease.scope_id, lease.work_id)
            if not self._main_lease_matches(slot, run, work, lease, now):
                return _lost("FENCED", "The run lease is no longer owned by this worker.")
            await self._extend_run_work(session, lease.run_id, lease.work_id, new_until, now)
        return RenewedLease(
            disposition="renewed",
            lease=lease.model_copy(
                update={"run_lease_until": new_until, "work_lease_until": new_until}
            ),
        )

    async def renew_child(self, request: ChildRenewRequest) -> ChildRenewResult:
        lease = request.lease
        now = self._aware_now()
        if lease.run_lease_until <= now or lease.work_lease_until <= now:
            return _lost("LEASE_EXPIRED", "The child lease expired before renewal.")
        new_until = now + timedelta(seconds=request.lease_seconds)
        async with self._sessions() as session, session.begin():
            run = await locks.lock_run(session, self._locks, lease.scope_id, lease.run_id)
            work = await locks.lock_work_item(session, self._locks, lease.scope_id, lease.work_id)
            if not self._child_lease_matches(run, work, lease, now):
                return _lost("FENCED", "The child lease is no longer owned by this worker.")
            await self._extend_run_work(session, lease.run_id, lease.work_id, new_until, now)
        return ChildRenewed(
            disposition="renewed",
            lease=lease.model_copy(
                update={"run_lease_until": new_until, "work_lease_until": new_until}
            ),
        )

    async def renew_maintenance(self, request: MaintenanceRenewRequest) -> MaintenanceRenewResult:
        lease = request.lease
        now = self._aware_now()
        if lease.work_lease_until <= now:
            return _lost("LEASE_EXPIRED", "The maintenance lease expired before renewal.")
        new_until = now + timedelta(seconds=request.lease_seconds)
        async with self._sessions() as session, session.begin():
            work = await locks.lock_work_item(session, self._locks, lease.scope_id, lease.work_id)
            if not self._maintenance_lease_matches(work, lease, now):
                return _lost("FENCED", "The maintenance lease is no longer owned by this worker.")
            await session.execute(
                sa.text(
                    f"UPDATE {self._schema}.work_item SET lease_until = :lease, updated_at = :now "
                    "WHERE scope_id = :scope AND id = :work"
                ),
                {"lease": new_until, "now": now, "scope": lease.scope_id, "work": lease.work_id},
            )
        return MaintenanceRenewed(
            disposition="renewed",
            lease=lease.model_copy(update={"work_lease_until": new_until}),
        )

    async def yield_run(self, request: YieldRequest) -> YieldResult:
        """Release a main slot after persisting a wait or an input-triggered requeue."""
        if request.input_request is not None and self._journal is None:
            errors.invalid_input("user-input requests require atomic Journal publication")
        lease = request.guard.lease
        now = self._aware_now()
        verify_lease(lease, now=now)
        async with self._sessions() as session, session.begin():
            slot = await locks.lock_slot(session, self._locks, lease.scope_id, lease.session_id)
            verify_slot(slot, lease)
            run = dict(
                verify_run(
                    await locks.lock_run(session, self._locks, lease.scope_id, lease.run_id),
                    request.guard.actor,
                    lease,
                    now=now,
                )
            )
            await revalidate_authority(session, self._locks, request.guard.actor, run=run, now=now)
            work = verify_work(
                await locks.lock_work_item(session, self._locks, lease.scope_id, lease.work_id),
                lease,
                now=now,
            )
            if int(run["state_version"]) != int(request.guard.expected_state_version):
                errors.version_conflict("run state changed before yield")
            if run["status"] != "running" or run["desired_control"] is not None:
                errors.control_pending("only an uncontrolled running run may yield")
            checkpoint = (
                (
                    await session.execute(
                        self._checkpoint_sql,
                        {"scope_id": lease.scope_id, "checkpoint_id": request.checkpoint_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            next_version = int(run["state_version"]) + 1
            if (
                checkpoint is None
                or checkpoint["run_id"] != run["id"]
                or int(checkpoint["state_version"]) != next_version
            ):
                errors.version_conflict("yield checkpoint does not match the next state version")
            if request.wait_spec.deadline_at > run["deadline_at"]:
                errors.invalid_input("wait deadline cannot exceed the run deadline")
            pending = int(request.inputs.pending_version)
            applied = int(request.inputs.applied_version)
            if (
                int(run["pending_input_version"]) != pending
                or int(run["applied_input_version"]) != applied
            ):
                errors.version_conflict("input watermark changed before yield")
            other_live_work = (
                await session.execute(
                    sa.text(
                        f"SELECT id FROM {self._schema}.work_item "
                        "WHERE scope_id = :scope AND run_id = :run "
                        "AND kind = 'advance_run' AND status IN ('pending', 'leased') "
                        "AND id <> :work LIMIT 1"
                    ),
                    {
                        "scope": lease.scope_id,
                        "run": run["id"],
                        "work": work["id"],
                    },
                )
            ).scalar_one_or_none()
            if other_live_work is not None:
                errors.version_conflict("run already has another live advance work item")
            if pending != applied:
                status = "queued"
                phase = "queued"
                waiting_spec = None
            else:
                status = (
                    "waiting_tool"
                    if request.wait_spec.kind in {"tool", "timer", "external"}
                    else "waiting_user"
                )
                phase = "waiting"
                waiting_spec = request.wait_spec.model_dump(mode="json", by_alias=True)
            if self._journal is not None:
                status_event = RunStatusDraft(
                    kind="run.status",
                    producer_key=f"run-status:{run['id']}:{next_version}",
                    occurred_at=now,
                    turn_id=run["turn_id"],
                    run_id=run["id"],
                    parent_run_id=run["parent_run_id"],
                    step_id=None,
                    item_id=None,
                    model_attempt_id=None,
                    payload=RunStatusPayload(
                        state_version=str(next_version),
                        status=RunStatus(status),
                        phase=cast("Phase", phase),
                        phase_started_at=now,
                        public_reason=(
                            "New input is ready."
                            if pending != applied
                            else "Waiting for the requested input or result."
                        ),
                    ),
                )
                events: tuple[StateEventDraft, ...] = (status_event,)
                if pending == applied and request.input_request is not None:
                    events += (
                        InputRequiredDraft(
                            kind="input.required",
                            producer_key=f"input-required:{run['id']}:{next_version}",
                            occurred_at=now,
                            turn_id=run["turn_id"],
                            run_id=run["id"],
                            parent_run_id=run["parent_run_id"],
                            step_id=None,
                            item_id=None,
                            model_attempt_id=None,
                            payload=request.input_request,
                        ),
                    )
                await self._journal.append_state_in_transaction(
                    session,
                    StateEventBatch(
                        authority=MainEventAuthority(
                            kind="main",
                            guard=StateWriteGuard(
                                actor=request.guard.actor,
                                lease=lease,
                                expected_state_version=request.guard.expected_state_version,
                                status=RunStatus.RUNNING,
                                desired_control=None,
                                control_command_id=None,
                                cancel_version=str(run["cancel_version"]),
                            ),
                        ),
                        session_id=lease.session_id,
                        trace_id=str(lease.work_id),
                        events=events,
                    ),
                )
            await session.execute(
                self._locks.work_finish_id,
                {
                    "status": "completed",
                    "now": now,
                    "scope_id": lease.scope_id,
                    "work_id": work["id"],
                },
            )
            work_id: UUID | None = None
            if pending != applied:
                work_id = await self._insert_requeue(
                    session,
                    run,
                    request.checkpoint_id,
                    pending,
                    next_version,
                    now,
                )
            await session.execute(
                sa.text(
                    f"UPDATE {self._schema}.run SET status = :status, phase = :phase, "
                    "state_version = :version, checkpoint_id = :checkpoint, "
                    "waiting_spec = CAST(:waiting AS jsonb), owner_id = NULL, lease_until = NULL, "
                    "owner_epoch = owner_epoch + 1, phase_started_at = :now, updated_at = :now "
                    "WHERE scope_id = :scope AND id = :run"
                ),
                {
                    "status": status,
                    "phase": phase,
                    "version": next_version,
                    "checkpoint": request.checkpoint_id,
                    "waiting": None if waiting_spec is None else json.dumps(waiting_spec),
                    "now": now,
                    "scope": lease.scope_id,
                    "run": run["id"],
                },
            )
            await session.execute(
                sa.text(
                    f"UPDATE {self._schema}.session_slot SET owner_run_id = NULL, "
                    "slot_version = slot_version + 1, updated_at = :now "
                    "WHERE scope_id = :scope AND session_id = :session AND owner_run_id = :run"
                ),
                {
                    "now": now,
                    "scope": lease.scope_id,
                    "session": run["session_id"],
                    "run": run["id"],
                },
            )
        if work_id is not None:
            return RequeuedRun(
                disposition="requeued",
                run_id=lease.run_id,
                state_version=str(next_version),
                checkpoint_id=request.checkpoint_id,
                inputs=request.inputs,
                work_id=work_id,
            )
        waiting_status = cast("Literal['waiting_tool', 'waiting_user']", status)
        return WaitingRun(
            disposition="waiting",
            run_id=lease.run_id,
            state_version=str(next_version),
            checkpoint_id=request.checkpoint_id,
            inputs=request.inputs,
            status=waiting_status,
        )

    async def _insert_requeue(
        self,
        session: AsyncSession,
        run: Mapping[str, Any],
        checkpoint_id: UUID,
        pending_version: int,
        state_version: int,
        now: datetime,
    ) -> UUID:
        dedup_key = f"input:{run['id']}:{pending_version}:state:{state_version}"
        work_id = uuid5(NAMESPACE_URL, dedup_key)
        subject = json.dumps({"kind": "input", "pendingVersion": pending_version})
        await session.execute(
            sa.text(
                f"INSERT INTO {self._schema}.work_item "
                "(id, scope_id, run_id, subject_ref, kind, dedup_key, status, payload_ref, "
                "wake_reason, priority, lease_epoch, attempt_count, available_at, deadline_at) "
                "VALUES (:id, :scope, :run, CAST(:subject AS jsonb), 'advance_run', :dedup, "
                "'pending', :payload, 'message', 1000, 0, 0, :now, :deadline) "
                "ON CONFLICT (scope_id, dedup_key) DO NOTHING"
            ),
            {
                "id": work_id,
                "scope": run["scope_id"],
                "run": run["id"],
                "subject": subject,
                "dedup": dedup_key,
                "payload": f"checkpoint:{checkpoint_id}",
                "now": now,
                "deadline": run["deadline_at"],
            },
        )
        found = (
            (
                await session.execute(
                    sa.text(
                        f"SELECT id, run_id, kind, status, payload_ref FROM "
                        f"{self._schema}.work_item "
                        "WHERE scope_id = :scope AND dedup_key = :dedup"
                    ),
                    {"scope": run["scope_id"], "dedup": dedup_key},
                )
            )
            .mappings()
            .one()
        )
        if (
            found["run_id"] != run["id"]
            or found["kind"] != "advance_run"
            or found["status"] != "pending"
            or found["payload_ref"] != f"checkpoint:{checkpoint_id}"
        ):
            errors.version_conflict("requeue identity is occupied by incompatible work")
        return cast("UUID", found["id"])

    async def _candidates(self, request: WorkClaimRequest, now: datetime) -> tuple[_Candidate, ...]:
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    self._candidate_sql,
                    {
                        "scope_id": request.scope_id,
                        "lanes": list(request.lanes),
                        "maintenance_kinds": list(request.maintenance_kinds),
                        "handlers": json.dumps(
                            [
                                {
                                    "namespace": cap.key.namespace,
                                    "key": cap.key.key,
                                    "revision": int(cap.revision),
                                }
                                for cap in request.worker.capabilities
                            ]
                        ),
                        "now": now,
                        "urgent_at": now + timedelta(seconds=self._config.urgent_deadline_seconds),
                        "aging_cap": self._config.maximum_aging_boost,
                        "aging_seconds": self._config.aging_interval_seconds,
                        "candidate_limit": self._config.candidate_limit,
                    },
                )
            ).mappings()
            return tuple(
                _Candidate(
                    work_id=row["work_id"],
                    run_id=row["run_id"],
                    session_id=row["session_id"],
                    parent_run_id=row["parent_run_id"],
                    kind=str(row["kind"]),
                    subject_ref=row["subject_ref"],
                    payload_ref=str(row["payload_ref"]),
                    handler_namespace=row["handler_namespace"],
                    handler_key=row["handler_key"],
                    handler_revision=row["handler_revision"],
                    desired_control=row["desired_control"],
                )
                for row in rows
            )

    @staticmethod
    def _lane(candidate: _Candidate) -> str:
        if (
            candidate.kind == "reconcile_execution"
            and candidate.payload_ref == "cancel-convergence"
            and isinstance(candidate.subject_ref, Mapping)
            and candidate.subject_ref.get("kind") == "cancel"
            and candidate.desired_control == "cancel"
        ):
            return "control"
        if candidate.kind != "advance_run":
            return "maintenance"
        if candidate.desired_control is not None:
            return "control"
        if candidate.parent_run_id is not None:
            return "child"
        return "main"

    @staticmethod
    def _supports_maintenance(request: WorkClaimRequest, candidate: _Candidate) -> bool:
        if candidate.kind != "domain_job":
            return True
        if (
            candidate.handler_namespace is None
            or candidate.handler_key is None
            or candidate.handler_revision is None
        ):
            return False
        keys = {
            (cap.key.namespace, cap.key.key, int(cap.revision))
            for cap in request.worker.capabilities
        }
        wanted = (
            candidate.handler_namespace,
            candidate.handler_key,
            int(candidate.handler_revision),
        )
        return wanted in keys

    async def _claim_candidate(
        self,
        request: WorkClaimRequest,
        candidate: _Candidate,
        lane: str,
        now: datetime,
    ) -> WorkClaimResult | None:
        lease_until = now + timedelta(seconds=request.lease_seconds)
        async with self._sessions() as session, session.begin():
            if lane in {"main", "control"}:
                if candidate.session_id is None or candidate.run_id is None:
                    return None
                if lane == "control" and candidate.parent_run_id is not None:
                    run = await locks.lock_run(
                        session, self._locks, request.scope_id, candidate.run_id
                    )
                    work = await locks.lock_work_item(
                        session, self._locks, request.scope_id, candidate.work_id
                    )
                    if not self._claimable_child_control(run, work, now):
                        return None
                    assert run is not None and work is not None
                    return await self._claim_child_control(
                        session, request, run, work, now, lease_until
                    )
                slot = await locks.lock_slot(
                    session, self._locks, request.scope_id, candidate.session_id
                )
                run = await locks.lock_run(session, self._locks, request.scope_id, candidate.run_id)
                work = await locks.lock_work_item(
                    session, self._locks, request.scope_id, candidate.work_id
                )
                if not self._claimable_main(slot, run, work, candidate, now, lane):
                    return None
                assert slot is not None and run is not None and work is not None
                return await self._claim_main_or_control(
                    session, request, slot, run, work, lane, now, lease_until
                )
            if lane == "child":
                if candidate.run_id is None:
                    return None
                if candidate.session_id is None or candidate.parent_run_id is None:
                    return None
                await locks.lock_slot(session, self._locks, request.scope_id, candidate.session_id)
                owned = {
                    identity: await locks.lock_run(session, self._locks, request.scope_id, identity)
                    for identity in sorted((candidate.parent_run_id, candidate.run_id), key=str)
                }
                run = owned[candidate.run_id]
                work = await locks.lock_work_item(
                    session, self._locks, request.scope_id, candidate.work_id
                )
                if not self._claimable_child(run, work, now):
                    return None
                assert run is not None and work is not None
                return await self._claim_child(session, request, run, work, now, lease_until)
            work_row = (
                (
                    await session.execute(
                        self._work_skip_locked_sql,
                        {"scope_id": request.scope_id, "work_id": candidate.work_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            work = None if work_row is None else dict(work_row)
            if not self._claimable_work(work, now):
                return None
            assert work is not None
            return await self._claim_maintenance(session, request, work, now, lease_until)

    async def _claim_main_or_control(
        self,
        session: AsyncSession,
        request: WorkClaimRequest,
        slot: Mapping[str, Any],
        run: Mapping[str, Any],
        work: Mapping[str, Any],
        lane: str,
        now: datetime,
        lease_until: datetime,
    ) -> ClaimedWork | ControlClaimedWork | None:
        new_slot = int(slot["slot_version"]) + 1
        new_run_epoch = int(run["owner_epoch"]) + 1
        new_work_epoch = int(work["lease_epoch"]) + 1
        await session.execute(
            sa.text(
                f"UPDATE {self._schema}.session_slot SET owner_run_id = :run, "
                "slot_version = :version, updated_at = :now WHERE scope_id = :scope "
                "AND session_id = :session"
            ),
            {
                "run": run["id"],
                "version": new_slot,
                "now": now,
                "scope": request.scope_id,
                "session": run["session_id"],
            },
        )
        status = "cancelling" if run["desired_control"] == "cancel" else "running"
        phase = "cancelling" if status == "cancelling" else "context"
        await self._set_run_lease(
            session,
            run["id"],
            request.worker.worker_id,
            new_run_epoch,
            lease_until,
            status,
            phase,
            now,
        )
        await self._set_work_lease(
            session, work["id"], request.worker.worker_id, new_work_epoch, lease_until, now
        )
        lease = LeaseToken(
            scope_id=request.scope_id,
            session_id=run["session_id"],
            run_id=run["id"],
            work_id=work["id"],
            owner_id=request.worker.worker_id,
            run_epoch=str(new_run_epoch),
            work_epoch=str(new_work_epoch),
            slot_version=str(new_slot),
            run_lease_until=lease_until,
            work_lease_until=lease_until,
        )
        if lane == "control":
            if run["control_command_id"] is None or run["desired_control"] is None:
                return None
            control_status = cast(
                "Literal['queued', 'running', 'waiting_tool', 'waiting_user', 'cancelling']",
                status,
            )
            return ControlClaimedWork(
                disposition="control_claimed",
                lease=lease,
                state_version=str(run["state_version"]),
                command_id=run["control_command_id"],
                desired_control=run["desired_control"],
                status=control_status,
                cancel_version=str(run["cancel_version"]),
                checkpoint_id=run["checkpoint_id"],
            )
        budget_id = await self._budget(session, request.scope_id, run["root_run_id"])
        if budget_id is None:
            errors.capacity_unavailable("claimable run has no budget allocation")
        return ClaimedWork(
            disposition="claimed",
            lease=lease,
            state_version=str(run["state_version"]),
            checkpoint_id=run["checkpoint_id"],
            budget_allocation_id=budget_id,
        )

    async def _claim_child(
        self,
        session: AsyncSession,
        request: WorkClaimRequest,
        run: Mapping[str, Any],
        work: Mapping[str, Any],
        now: datetime,
        lease_until: datetime,
    ) -> ChildClaimedWork | None:
        root_cancel_version = (
            await session.execute(
                self._root_cancel_sql, {"scope_id": request.scope_id, "run_id": run["root_run_id"]}
            )
        ).scalar_one_or_none()
        if root_cancel_version is None:
            return None
        new_run_epoch = int(run["owner_epoch"]) + 1
        new_work_epoch = int(work["lease_epoch"]) + 1
        await self._set_run_lease(
            session,
            run["id"],
            request.worker.worker_id,
            new_run_epoch,
            lease_until,
            "running",
            "context",
            now,
        )
        await self._set_work_lease(
            session, work["id"], request.worker.worker_id, new_work_epoch, lease_until, now
        )
        workspace = (
            (
                await session.execute(
                    self._workspace_sql, {"scope_id": request.scope_id, "run_id": run["id"]}
                )
            )
            .mappings()
            .one_or_none()
        )
        budget_id = (
            await session.execute(
                self._child_budget_sql,
                {
                    "scope_id": request.scope_id,
                    "root_run_id": run["root_run_id"],
                    "run_id": run["id"],
                    "parent_run_id": run["parent_run_id"],
                    "task_id": run["task_id"],
                },
            )
        ).scalar_one_or_none()
        if workspace is None or budget_id is None or root_cancel_version is None:
            errors.invalid_input("claimable child is missing workspace, budget or root run")
        lease = ChildLeaseToken(
            scope_id=request.scope_id,
            session_id=run["session_id"],
            run_id=run["id"],
            work_id=work["id"],
            owner_id=request.worker.worker_id,
            run_epoch=str(new_run_epoch),
            work_epoch=str(new_work_epoch),
            run_lease_until=lease_until,
            work_lease_until=lease_until,
        )
        return ChildClaimedWork(
            disposition="child_claimed",
            lease=lease,
            root_run_id=run["root_run_id"],
            root_cancel_version=str(root_cancel_version),
            state_version=str(run["state_version"]),
            checkpoint_id=run["checkpoint_id"],
            budget_allocation_id=budget_id,
            workspace_ref=ResourceRef(
                resource_type="workspace",
                resource_id=str(workspace["id"]),
                resource_version=str(workspace["revision"]),
                scope=ResourceScope(scope_id=request.scope_id),
            ),
            workspace_generation=str(workspace["generation"]),
        )

    async def _claim_child_control(
        self,
        session: AsyncSession,
        request: WorkClaimRequest,
        run: Mapping[str, Any],
        work: Mapping[str, Any],
        now: datetime,
        lease_until: datetime,
    ) -> ControlClaimedWork:
        new_run_epoch = int(run["owner_epoch"]) + 1
        new_work_epoch = int(work["lease_epoch"]) + 1
        status = "cancelling" if run["desired_control"] == "cancel" else "running"
        phase = "cancelling" if status == "cancelling" else "context"
        await self._set_run_lease(
            session,
            run["id"],
            request.worker.worker_id,
            new_run_epoch,
            lease_until,
            status,
            phase,
            now,
        )
        await self._set_work_lease(
            session, work["id"], request.worker.worker_id, new_work_epoch, lease_until, now
        )
        if run["control_command_id"] is None or run["desired_control"] is None:
            errors.invalid_input("control run has no command identity")
        return ControlClaimedWork(
            disposition="control_claimed",
            lease=ChildLeaseToken(
                scope_id=request.scope_id,
                session_id=run["session_id"],
                run_id=run["id"],
                work_id=work["id"],
                owner_id=request.worker.worker_id,
                run_epoch=str(new_run_epoch),
                work_epoch=str(new_work_epoch),
                run_lease_until=lease_until,
                work_lease_until=lease_until,
            ),
            state_version=str(run["state_version"]),
            command_id=run["control_command_id"],
            desired_control=run["desired_control"],
            status=cast(
                "Literal['queued', 'running', 'waiting_tool', 'waiting_user', 'cancelling']",
                status,
            ),
            cancel_version=str(run["cancel_version"]),
            checkpoint_id=run["checkpoint_id"],
        )

    async def _claim_maintenance(
        self,
        session: AsyncSession,
        request: WorkClaimRequest,
        work: Mapping[str, Any],
        now: datetime,
        lease_until: datetime,
    ) -> MaintenanceClaimedWork | None:
        try:
            subject = ResourceRef.model_validate(work["subject_ref"])
        except ValueError:
            failure = FailureEnvelope(
                code="INVALID_INPUT",
                category="validation",
                retryable=False,
                public_message="The maintenance work target is invalid.",
            )
            await session.execute(
                sa.text(
                    f"UPDATE {self._schema}.work_item SET status = 'failed', "
                    "failure = CAST(:failure AS jsonb), updated_at = :now "
                    "WHERE scope_id = :scope AND id = :work AND status = 'pending'"
                ),
                {
                    "failure": json.dumps(failure.model_dump(mode="json", by_alias=True)),
                    "now": now,
                    "scope": request.scope_id,
                    "work": work["id"],
                },
            )
            return None
        new_epoch = int(work["lease_epoch"]) + 1
        await self._set_work_lease(
            session, work["id"], request.worker.worker_id, new_epoch, lease_until, now
        )
        return MaintenanceClaimedWork(
            disposition="maintenance_claimed",
            lease=MaintenanceLease(
                scope_id=request.scope_id,
                work_id=work["id"],
                owner_id=request.worker.worker_id,
                work_epoch=str(new_epoch),
                work_lease_until=lease_until,
            ),
            kind=work["kind"],
            subject_ref=subject,
            payload_ref=work["payload_ref"],
        )

    async def _budget(self, session: AsyncSession, scope_id: str, root_run_id: UUID) -> UUID | None:
        value = (
            await session.execute(
                self._budget_sql, {"scope_id": scope_id, "root_run_id": root_run_id}
            )
        ).scalar_one_or_none()
        return cast("UUID | None", value)

    async def _set_run_lease(
        self,
        session: AsyncSession,
        run_id: UUID,
        owner_id: str,
        owner_epoch: int,
        lease_until: datetime,
        status: str,
        phase: str,
        now: datetime,
    ) -> None:
        await session.execute(
            sa.text(
                f"UPDATE {self._schema}.run SET status = :status, phase = :phase, "
                "phase_started_at = :now, owner_id = :owner, owner_epoch = :epoch, "
                "lease_until = :lease, updated_at = :now WHERE id = :run"
            ),
            {
                "status": status,
                "phase": phase,
                "now": now,
                "owner": owner_id,
                "epoch": owner_epoch,
                "lease": lease_until,
                "run": run_id,
            },
        )

    async def _set_work_lease(
        self,
        session: AsyncSession,
        work_id: UUID,
        owner_id: str,
        work_epoch: int,
        lease_until: datetime,
        now: datetime,
    ) -> None:
        await session.execute(
            sa.text(
                f"UPDATE {self._schema}.work_item SET status = 'leased', owner_id = :owner, "
                "lease_epoch = :epoch, attempt_count = attempt_count + 1, "
                "lease_until = :lease, updated_at = :now WHERE id = :work"
            ),
            {
                "owner": owner_id,
                "epoch": work_epoch,
                "lease": lease_until,
                "now": now,
                "work": work_id,
            },
        )

    async def _extend_run_work(
        self,
        session: AsyncSession,
        run_id: UUID,
        work_id: UUID,
        lease_until: datetime,
        now: datetime,
    ) -> None:
        await session.execute(
            sa.text(
                f"UPDATE {self._schema}.run SET lease_until = :lease, updated_at = :now "
                "WHERE id = :run"
            ),
            {"lease": lease_until, "now": now, "run": run_id},
        )
        await session.execute(
            sa.text(
                f"UPDATE {self._schema}.work_item SET lease_until = :lease, updated_at = :now "
                "WHERE id = :work"
            ),
            {"lease": lease_until, "now": now, "work": work_id},
        )

    @staticmethod
    def _claimable_work(work: Mapping[str, Any] | None, now: datetime) -> bool:
        return bool(
            work is not None
            and work["status"] == "pending"
            and work["available_at"] <= now < work["deadline_at"]
        )

    def _claimable_main(
        self,
        slot: Mapping[str, Any] | None,
        run: Mapping[str, Any] | None,
        work: Mapping[str, Any] | None,
        candidate: _Candidate,
        now: datetime,
        lane: str,
    ) -> bool:
        if slot is None or run is None or not self._claimable_work(work, now):
            return False
        if slot["owner_run_id"] not in {None, run["id"]}:
            return False
        if run["parent_run_id"] is not None or run["owner_id"] is not None:
            return False
        if lane == "main":
            return (
                candidate.kind == "advance_run"
                and run["status"] == "queued"
                and run["desired_control"] is None
            )
        return run["desired_control"] in {"pause", "cancel"} and run["status"] in {
            "queued",
            "running",
            "waiting_tool",
            "waiting_user",
            "cancelling",
        }

    def _claimable_child(
        self,
        run: Mapping[str, Any] | None,
        work: Mapping[str, Any] | None,
        now: datetime,
    ) -> bool:
        return bool(
            run is not None
            and run["parent_run_id"] is not None
            and run["status"] == "queued"
            and run["desired_control"] is None
            and run["owner_id"] is None
            and self._claimable_work(work, now)
        )

    def _claimable_child_control(
        self,
        run: Mapping[str, Any] | None,
        work: Mapping[str, Any] | None,
        now: datetime,
    ) -> bool:
        return bool(
            run is not None
            and run["parent_run_id"] is not None
            and run["desired_control"] in {"pause", "cancel"}
            and run["status"] in {"queued", "running", "waiting_tool", "waiting_user", "cancelling"}
            and run["owner_id"] is None
            and self._claimable_work(work, now)
        )

    @staticmethod
    def _main_lease_matches(
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
            and work["run_id"] == lease.run_id
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
    def _child_lease_matches(
        run: Mapping[str, Any] | None,
        work: Mapping[str, Any] | None,
        lease: ChildLeaseToken,
        now: datetime,
    ) -> bool:
        return bool(
            run is not None
            and work is not None
            and work["run_id"] == lease.run_id
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
    def _maintenance_lease_matches(
        work: Mapping[str, Any] | None, lease: MaintenanceLease, now: datetime
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
            raise ValueError("scheduler clock must return an aware datetime")
        return now


__all__ = ["SchedulerConfig", "SqlScheduler"]

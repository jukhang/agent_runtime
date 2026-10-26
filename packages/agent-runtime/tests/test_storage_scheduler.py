"""Real PostgreSQL scheduler races; every case uses a disposable random schema."""

import asyncio
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy", reason="requires agent-runtime[storage]")

import sqlalchemy as sa  # noqa: E402
from test_storage_model_attempts import provider_request  # noqa: E402
from test_storage_uow import (  # noqa: E402
    _seed,
    _seed_attempt,
    _seed_execution,
    _seed_work,
    actor,
    engine_for,
    migrate,
    run,
)

from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope  # noqa: E402
from agent_runtime.contracts.requests import ControlRequest  # noqa: E402
from agent_runtime.contracts.results import Usage  # noqa: E402
from agent_runtime.contracts.scheduler import (  # noqa: E402
    ClaimedWork,
    IdleWork,
    InputWatermark,
    RenewRequest,
    YieldRequest,
)
from agent_runtime.contracts.tasks import WaitSpec  # noqa: E402
from agent_runtime.contracts.transactions import CommitGuard  # noqa: E402
from agent_runtime.contracts.work_claims import (  # noqa: E402
    ChildClaimedWork,
    ControlClaimedWork,
    MaintenanceClaimedWork,
    WorkClaimRequest,
)
from agent_runtime.provider import AttemptTerminal  # noqa: E402
from agent_runtime.storage.model_attempts import (  # noqa: E402
    SqlModelAttemptStore,
    claim_attempt_suffix,
)
from agent_runtime.storage.recovery import RecoveryConfig, SqlRecovery  # noqa: E402
from agent_runtime.storage.scheduler import SchedulerConfig, SqlScheduler  # noqa: E402
from agent_runtime.storage.uow import SqlStateUnitOfWork  # noqa: E402
from agent_runtime.worker import WorkerConfig, WorkerRunner  # noqa: E402

DSN = os.environ.get("AGENT_RUNTIME_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="AGENT_RUNTIME_TEST_DSN is not configured")


def request(
    worker_id: str,
    *,
    lanes: tuple[str, ...] = ("main",),
    maintenance_kinds: tuple[str, ...] = (),
    capabilities: tuple[dict[str, Any], ...] = (),
) -> WorkClaimRequest:
    return WorkClaimRequest.model_validate(
        {
            "scopeId": "scope-a",
            "worker": {
                "workerId": worker_id,
                "runtimeRevision": {
                    "key": {"namespace": "builtin", "key": "runtime"},
                    "revision": "1",
                },
                "capabilities": capabilities,
            },
            "leaseSeconds": 30,
            "lanes": lanes,
            "maintenanceKinds": maintenance_kinds,
        }
    )


@pytest.fixture
def scheduler_world() -> Any:
    schema = f"p02_scheduler_{uuid.uuid4().hex[:12]}"
    assert migrate("upgrade", schema).status == "applied"
    world = run(lambda: _seed(schema, status="queued", phase="queued"))

    async def prepare() -> None:
        engine = engine_for(schema)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        "UPDATE session_slot SET owner_run_id = NULL "
                        "WHERE scope_id = :scope AND session_id = :session"
                    ),
                    {"scope": world.scope, "session": world.session_id},
                )
                await connection.execute(
                    sa.text(
                        "INSERT INTO budget_allocation "
                        "(id, scope_id, root_run_id, limits, reserved, unknown_reserved, "
                        "settled, usage_status, unsettled_count, version) VALUES "
                        "(:id, :scope, :run, '{}', '{}', '{}', '{}', 'complete', 0, 1)"
                    ),
                    {"id": uuid.uuid4(), "scope": world.scope, "run": world.run_id},
                )
        finally:
            await engine.dispose()

    run(prepare)
    yield world

    async def drop() -> None:
        engine = engine_for(schema)
        try:
            async with engine.begin() as connection:
                await connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            await engine.dispose()

    run(drop)


@pytest.fixture(scope="module", autouse=True)
def no_scheduler_schema_leaks() -> Any:
    yield

    async def leftovers() -> str:
        assert DSN is not None
        engine = engine_for("public")
        try:
            async with engine.connect() as connection:
                return str(
                    (
                        await connection.execute(
                            sa.text(
                                "SELECT coalesce(string_agg(nspname, ','), '') "
                                "FROM pg_namespace WHERE nspname LIKE 'p02_scheduler_%'"
                            )
                        )
                    ).scalar_one()
                )
        finally:
            await engine.dispose()

    assert not run(leftovers), "scheduler test schemas were left behind"


@pytest.mark.parametrize("workers", [1, 2, 4, 8])
def test_only_one_worker_claims_main_slot(scheduler_world: Any, workers: int) -> None:
    async def scenario() -> list[Any]:
        engine = engine_for(scheduler_world.schema, pool_size=max(workers, 2), search_path=False)
        scheduler = SqlScheduler(engine, scheduler_world.schema)
        try:
            return await asyncio.gather(
                *(scheduler.claim_work(request(f"worker-{index}")) for index in range(workers))
            )
        finally:
            await engine.dispose()

    results = run(scenario)
    claimed = [item for item in results if isinstance(item, ClaimedWork)]
    assert len(claimed) == 1
    assert sum(isinstance(item, IdleWork) for item in results) == workers - 1


def test_stale_worker_loses_renewal_after_recovery(scheduler_world: Any) -> None:
    async def scenario() -> tuple[Any, Any, str]:
        engine = engine_for(scheduler_world.schema, pool_size=4, search_path=False)
        scheduler = SqlScheduler(engine, scheduler_world.schema)
        recovery = SqlRecovery(
            engine,
            scheduler_world.schema,
            config=RecoveryConfig(base_retry_seconds=1),
        )
        try:
            first = await scheduler.claim_work(request("worker-old"))
            assert isinstance(first, ClaimedWork)
            expired = datetime.now(UTC) - timedelta(seconds=1)
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.run SET lease_until = :expired "
                        "WHERE id = :run"
                    ),
                    {"expired": expired, "run": scheduler_world.run_id},
                )
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.work_item SET lease_until = :expired "
                        "WHERE id = :work"
                    ),
                    {"expired": expired, "work": scheduler_world.work_id},
                )
            report = await recovery.recover_expired_leases(scheduler_world.scope)
            renewed = await scheduler.renew(RenewRequest(lease=first.lease, lease_seconds=30))
            async with engine.connect() as connection:
                status = str(
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT status FROM {scheduler_world.schema}.work_item "
                                "WHERE id = :work"
                            ),
                            {"work": scheduler_world.work_id},
                        )
                    ).scalar_one()
                )
            return report, renewed, status
        finally:
            await engine.dispose()

    report, renewed, status = run(scenario)
    assert report.requeued == 1
    assert renewed.disposition == "lost"
    assert status == "pending"


def test_worker_drain_cancels_local_handler_without_rewriting_lease(
    scheduler_world: Any,
) -> None:
    async def scenario() -> tuple[bool, dict[str, Any], int, int]:
        engine = engine_for(scheduler_world.schema, search_path=False)
        scheduler = SqlScheduler(engine, scheduler_world.schema)
        started = asyncio.Event()
        cancelled = asyncio.Event()
        claimed_epoch = 0

        async def handler(work: Any) -> None:
            nonlocal claimed_epoch
            claimed_epoch = int(work.lease.work_epoch)
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        worker = WorkerRunner(
            scheduler,
            request("drain-worker"),
            handler,
            config=WorkerConfig(
                concurrency=1,
                idle_sleep_seconds=0.01,
                renewal_fraction=0.9,
                drain_grace_seconds=0.01,
            ),
        )
        serving = asyncio.create_task(worker.serve())
        try:
            await asyncio.wait_for(started.wait(), timeout=1)
            worker.request_drain()
            await asyncio.wait_for(serving, timeout=1)
            await asyncio.wait_for(cancelled.wait(), timeout=1)
            async with engine.connect() as connection:
                row = dict(
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT status, owner_id, lease_epoch FROM "
                                f"{scheduler_world.schema}.work_item WHERE id = :work"
                            ),
                            {"work": scheduler_world.work_id},
                        )
                    )
                    .mappings()
                    .one()
                )
            return cancelled.is_set(), row, worker.inflight, claimed_epoch
        finally:
            if not serving.done():
                worker.request_drain()
                await asyncio.gather(serving, return_exceptions=True)
            await engine.dispose()

    cancelled, row, inflight, claimed_epoch = run(scenario)
    assert cancelled is True and inflight == 0
    assert row == {
        "status": "leased",
        "owner_id": "drain-worker",
        "lease_epoch": claimed_epoch,
    }


def test_renew_rejects_work_item_from_another_run(scheduler_world: Any) -> None:
    async def scenario() -> Any:
        engine = engine_for(scheduler_world.schema, search_path=False)
        scheduler = SqlScheduler(engine, scheduler_world.schema)
        other = await _seed(
            scheduler_world.schema,
            suffix="mixed-renew",
            scope=scheduler_world.scope,
            status="queued",
            phase="queued",
        )
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.session_slot SET owner_run_id = NULL "
                        "WHERE session_id = :session"
                    ),
                    {"session": other.session_id},
                )
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {scheduler_world.schema}.budget_allocation "
                        "(id, scope_id, root_run_id, limits, reserved, unknown_reserved, "
                        "settled, usage_status, unsettled_count, version) VALUES "
                        "(:id, :scope, :run, '{}', '{}', '{}', '{}', 'complete', 0, 1)"
                    ),
                    {"id": uuid.uuid4(), "scope": other.scope, "run": other.run_id},
                )
            first = await scheduler.claim_work(request("shared-renew-worker"))
            second = await scheduler.claim_work(request("shared-renew-worker"))
            assert isinstance(first, ClaimedWork) and isinstance(second, ClaimedWork)
            mixed = first.lease.model_copy(
                update={
                    "work_id": second.lease.work_id,
                    "work_epoch": second.lease.work_epoch,
                    "work_lease_until": second.lease.work_lease_until,
                }
            )
            return await scheduler.renew(RenewRequest(lease=mixed, lease_seconds=30))
        finally:
            await engine.dispose()

    assert run(scenario).disposition == "lost"


def test_claim_uses_persisted_priority_order(scheduler_world: Any) -> None:
    async def scenario() -> uuid.UUID:
        engine = engine_for(scheduler_world.schema, search_path=False)
        scheduler = SqlScheduler(
            engine,
            scheduler_world.schema,
            config=SchedulerConfig(urgent_deadline_seconds=0),
        )
        second = await _seed(
            scheduler_world.schema,
            suffix="priority",
            scope=scheduler_world.scope,
            status="queued",
            phase="queued",
        )
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.session_slot SET owner_run_id = NULL "
                        "WHERE session_id = :session"
                    ),
                    {"session": second.session_id},
                )
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.work_item SET priority = 999 "
                        "WHERE id = :work"
                    ),
                    {"work": second.work_id},
                )
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {scheduler_world.schema}.budget_allocation "
                        "(id, scope_id, root_run_id, limits, reserved, unknown_reserved, "
                        "settled, usage_status, unsettled_count, version) VALUES "
                        "(:id, :scope, :run, '{}', '{}', '{}', '{}', 'complete', 0, 1)"
                    ),
                    {"id": uuid.uuid4(), "scope": second.scope, "run": second.run_id},
                )
            result = await scheduler.claim_work(request("worker-priority"))
            assert isinstance(result, ClaimedWork)
            return result.lease.run_id
        finally:
            await engine.dispose()

    assert run(scenario) != scheduler_world.run_id


def test_claim_applies_bounded_waiting_age(scheduler_world: Any) -> None:
    async def scenario() -> uuid.UUID:
        engine = engine_for(scheduler_world.schema, search_path=False)
        scheduler = SqlScheduler(
            engine,
            scheduler_world.schema,
            config=SchedulerConfig(
                urgent_deadline_seconds=0,
                aging_interval_seconds=1,
                maximum_aging_boost=10,
            ),
        )
        older = await _seed(
            scheduler_world.schema,
            suffix="aged",
            scope=scheduler_world.scope,
            status="queued",
            phase="queued",
        )
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.session_slot SET owner_run_id = NULL "
                        "WHERE session_id = :session"
                    ),
                    {"session": older.session_id},
                )
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.work_item SET priority = 100 "
                        "WHERE id = :work"
                    ),
                    {"work": scheduler_world.work_id},
                )
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.work_item "
                        "SET priority = 99, available_at = :available WHERE id = :work"
                    ),
                    {
                        "available": datetime.now(UTC) - timedelta(seconds=5),
                        "work": older.work_id,
                    },
                )
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {scheduler_world.schema}.budget_allocation "
                        "(id, scope_id, root_run_id, limits, reserved, unknown_reserved, "
                        "settled, usage_status, unsettled_count, version) VALUES "
                        "(:id, :scope, :run, '{}', '{}', '{}', '{}', 'complete', 0, 1)"
                    ),
                    {"id": uuid.uuid4(), "scope": older.scope, "run": older.run_id},
                )
            result = await scheduler.claim_work(request("worker-aging"))
            assert isinstance(result, ClaimedWork)
            return result.lease.run_id
        finally:
            await engine.dispose()

    assert run(scenario) != scheduler_world.run_id


def test_stale_execution_supervision_is_deduplicated(scheduler_world: Any) -> None:
    async def scenario() -> tuple[Any, Any, int]:
        execution_id = await _seed_execution(scheduler_world, status="running")
        engine = engine_for(scheduler_world.schema, search_path=False)
        recovery = SqlRecovery(engine, scheduler_world.schema)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.execution SET cleanup_status = 'pending' "
                        "WHERE id = :id"
                    ),
                    {"id": execution_id},
                )
            first = await recovery.schedule_stale_executions(scheduler_world.scope)
            second = await recovery.schedule_stale_executions(scheduler_world.scope)
            async with engine.connect() as connection:
                count = int(
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT count(*) FROM {scheduler_world.schema}.work_item "
                                "WHERE kind = 'reconcile_execution' AND "
                                "subject_ref->>'resourceId' = :execution"
                            ),
                            {"execution": str(execution_id)},
                        )
                    ).scalar_one()
                )
            return first, second, count
        finally:
            await engine.dispose()

    first, second, count = run(scenario)
    assert first.supervision_created == 1
    assert second.supervision_reused == 1
    assert count == 1


def test_expired_maintenance_lease_does_not_fail_run(scheduler_world: Any) -> None:
    async def scenario() -> tuple[Any, str, str]:
        work_id = await _seed_work(scheduler_world, status="leased", lease_epoch=1)
        engine = engine_for(scheduler_world.schema, search_path=False)
        recovery = SqlRecovery(engine, scheduler_world.schema)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.work_item SET lease_until = :expired "
                        "WHERE id = :id"
                    ),
                    {"expired": datetime.now(UTC) - timedelta(seconds=1), "id": work_id},
                )
            report = await recovery.recover_expired_leases(scheduler_world.scope)
            async with engine.connect() as connection:
                run_status = str(
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT status FROM {scheduler_world.schema}.run WHERE id = :id"
                            ),
                            {"id": scheduler_world.run_id},
                        )
                    ).scalar_one()
                )
                work_status = str(
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT status FROM {scheduler_world.schema}.work_item "
                                "WHERE id = :id"
                            ),
                            {"id": work_id},
                        )
                    ).scalar_one()
                )
            return report, run_status, work_status
        finally:
            await engine.dispose()

    report, run_status, work_status = run(scenario)
    assert report.requeued == 1
    assert run_status == "queued"
    assert work_status == "pending"


def test_execution_supervision_survives_attempt_and_deadline_limits(
    scheduler_world: Any,
) -> None:
    async def scenario() -> tuple[Any, Any, str, bool, Any, str]:
        work_id = await _seed_work(scheduler_world, status="leased", lease_epoch=1)
        engine = engine_for(scheduler_world.schema, search_path=False)
        recovery = SqlRecovery(engine, scheduler_world.schema)
        expired = datetime.now(UTC) - timedelta(seconds=1)
        execution_id = uuid.uuid4()
        subject = json.dumps(
            {
                "resourceType": "execution",
                "resourceId": str(execution_id),
                "resourceVersion": "0",
                "scope": {"scopeId": scheduler_world.scope, "sessionId": None, "ownerId": None},
            }
        )
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.work_item SET lease_until=:expired,"
                        "deadline_at=:expired,attempt_count=99,"
                        "subject_ref=CAST(:subject AS jsonb),payload_ref=:payload WHERE id=:id"
                    ),
                    {
                        "expired": expired,
                        "id": work_id,
                        "subject": subject,
                        "payload": f"execution:{execution_id}",
                    },
                )
            leased = await recovery.recover_expired_leases(scheduler_world.scope)
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.work_item SET deadline_at=:expired "
                        "WHERE id=:id"
                    ),
                    {"expired": expired, "id": work_id},
                )
            pending = await recovery.recover_expired_leases(scheduler_world.scope)
            control_work_id = await _seed_work(scheduler_world, status="leased", lease_epoch=1)
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.work_item SET "
                        "payload_ref='cancel-convergence',lease_until=:expired,"
                        "deadline_at=:expired,attempt_count=99 WHERE id=:id"
                    ),
                    {"expired": expired, "id": control_work_id},
                )
            control = await recovery.recover_expired_leases(scheduler_world.scope)
            async with engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT status,deadline_at FROM "
                                f"{scheduler_world.schema}.work_item "
                                "WHERE id=:id"
                            ),
                            {"id": work_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                control_status = (
                    await connection.execute(
                        sa.text(
                            f"SELECT status FROM {scheduler_world.schema}.work_item WHERE id=:id"
                        ),
                        {"id": control_work_id},
                    )
                ).scalar_one()
            return (
                leased,
                pending,
                str(row["status"]),
                row["deadline_at"] > datetime.now(UTC),
                control,
                str(control_status),
            )
        finally:
            await engine.dispose()

    leased, pending, status, extended, control, control_status = run(scenario)
    assert leased.requeued == 1 and leased.failed == 0
    assert pending.requeued == 1 and pending.failed == 0
    assert status == "pending" and extended
    assert control.failed == 1 and control_status == "failed"


def test_domain_job_requires_exact_worker_capability(scheduler_world: Any) -> None:
    async def scenario() -> tuple[Any, Any]:
        engine = engine_for(scheduler_world.schema, search_path=False)
        scheduler = SqlScheduler(
            engine, scheduler_world.schema, config=SchedulerConfig(candidate_limit=1)
        )
        work_id = uuid.uuid4()
        now = datetime.now(UTC)
        subject = json.dumps(
            {
                "resourceType": "work_item",
                "resourceId": str(work_id),
                "resourceVersion": "1",
                "scope": {"scopeId": scheduler_world.scope},
            }
        )
        try:
            async with engine.begin() as connection:
                # More than the normal candidate window, all ranked ahead of the
                # eligible job. Filtering after LIMIT would permanently starve it.
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {scheduler_world.schema}.work_item "
                        "(id,scope_id,subject_ref,kind,dedup_key,status,payload_ref,"
                        "handler_namespace,handler_key,handler_revision,priority,lease_epoch,"
                        "attempt_count,available_at,deadline_at) VALUES "
                        "(:id,:scope,CAST(:subject AS jsonb),'domain_job',:dedup,'pending',"
                        "'payload:blocker','builtin','unavailable',1,9999,0,0,:now,:deadline)"
                    ),
                    [
                        {
                            "id": uuid.uuid4(),
                            "scope": scheduler_world.scope,
                            "subject": subject,
                            "dedup": f"unavailable:{i}:{work_id}",
                            "now": now,
                            "deadline": now + timedelta(hours=1),
                        }
                        for i in range(70)
                    ],
                )
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {scheduler_world.schema}.work_item "
                        "(id, scope_id, run_id, subject_ref, kind, dedup_key, status, "
                        "payload_ref, handler_namespace, handler_key, handler_revision, "
                        "wake_reason, priority, lease_epoch, attempt_count, available_at, "
                        "deadline_at) VALUES (:id, :scope, NULL, CAST(:subject AS jsonb), "
                        "'domain_job', :dedup, 'pending', 'payload:1', 'builtin', 'report', 2, "
                        "NULL, 100, 0, 0, :now, :deadline)"
                    ),
                    {
                        "id": work_id,
                        "scope": scheduler_world.scope,
                        "subject": subject,
                        "dedup": f"domain:{work_id}",
                        "now": now,
                        "deadline": now + timedelta(hours=1),
                    },
                )
            missing = await scheduler.claim_work(
                request(
                    "worker-maintenance",
                    lanes=("maintenance",),
                    maintenance_kinds=("domain_job",),
                )
            )
            matching = await scheduler.claim_work(
                request(
                    "worker-maintenance",
                    lanes=("maintenance",),
                    maintenance_kinds=("domain_job",),
                    capabilities=(
                        {
                            "key": {"namespace": "builtin", "key": "report"},
                            "revision": "2",
                        },
                    ),
                )
            )
            return missing, matching
        finally:
            await engine.dispose()

    missing, matching = run(scenario)
    assert isinstance(missing, IdleWork)
    assert missing.reason == "capability_mismatch"
    assert isinstance(matching, MaintenanceClaimedWork)


def test_maintenance_skip_locked_claims_are_disjoint(scheduler_world: Any) -> None:
    async def scenario() -> list[Any]:
        engine = engine_for(scheduler_world.schema, pool_size=8, search_path=False)
        scheduler = SqlScheduler(engine, scheduler_world.schema)
        now = datetime.now(UTC)
        try:
            async with engine.begin() as connection:
                for index in range(4):
                    work_id = uuid.uuid4()
                    subject = json.dumps(
                        {
                            "resourceType": "work_item",
                            "resourceId": str(work_id),
                            "resourceVersion": "1",
                            "scope": {"scopeId": scheduler_world.scope},
                        }
                    )
                    await connection.execute(
                        sa.text(
                            f"INSERT INTO {scheduler_world.schema}.work_item "
                            "(id, scope_id, run_id, subject_ref, kind, dedup_key, status, "
                            "payload_ref, wake_reason, priority, lease_epoch, attempt_count, "
                            "available_at, deadline_at) VALUES "
                            "(:id, :scope, NULL, CAST(:subject AS jsonb), "
                            "'reconcile_execution', :dedup, 'pending', :payload, NULL, 100, "
                            "0, 0, :now, :deadline)"
                        ),
                        {
                            "id": work_id,
                            "scope": scheduler_world.scope,
                            "subject": subject,
                            "dedup": f"maintenance:{index}:{work_id}",
                            "payload": f"execution:{index}",
                            "now": now,
                            "deadline": now + timedelta(hours=1),
                        },
                    )
            return await asyncio.gather(
                *(
                    scheduler.claim_work(
                        request(
                            f"maintenance-{index}",
                            lanes=("maintenance",),
                            maintenance_kinds=("reconcile_execution",),
                        )
                    )
                    for index in range(8)
                )
            )
        finally:
            await engine.dispose()

    results = run(scenario)
    claimed = [item for item in results if isinstance(item, MaintenanceClaimedWork)]
    assert len(claimed) == 4
    assert len({item.lease.work_id for item in claimed}) == 4
    assert sum(isinstance(item, IdleWork) for item in results) == 4


def test_cancel_recovery_retains_slot_for_control_reclaim(scheduler_world: Any) -> None:
    async def scenario() -> tuple[list[Any], Any, Any]:
        engine = engine_for(scheduler_world.schema, pool_size=4, search_path=False)
        scheduler = SqlScheduler(engine, scheduler_world.schema)
        recovery = SqlRecovery(
            engine,
            scheduler_world.schema,
            config=RecoveryConfig(base_retry_seconds=1),
        )
        try:
            claimed = await scheduler.claim_work(request("worker-before-cancel"))
            assert isinstance(claimed, ClaimedWork)
            expired = datetime.now(UTC) - timedelta(seconds=1)
            command_id = uuid.uuid4()
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {scheduler_world.schema}.command "
                        "(id, scope_id, run_id, actor_id, request_key, request_hash, kind, "
                        "status, expected_state_version) VALUES "
                        "(:id, :scope, :run, :actor, :key, :hash, 'cancel', 'accepted', 0)"
                    ),
                    {
                        "id": command_id,
                        "scope": scheduler_world.scope,
                        "run": scheduler_world.run_id,
                        "actor": scheduler_world.actor_id,
                        "key": f"cancel-{command_id}",
                        "hash": "c" * 64,
                    },
                )
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.run SET status = 'cancelling', "
                        "phase = 'cancelling', desired_control = 'cancel', "
                        "control_command_id = :command, lease_until = :expired "
                        "WHERE id = :run"
                    ),
                    {
                        "command": command_id,
                        "expired": expired,
                        "run": scheduler_world.run_id,
                    },
                )
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.work_item SET lease_until = :expired "
                        "WHERE id = :work"
                    ),
                    {"expired": expired, "work": scheduler_world.work_id},
                )
            reports = await asyncio.gather(
                recovery.recover_expired_leases(scheduler_world.scope),
                recovery.recover_expired_leases(scheduler_world.scope),
            )
            async with engine.begin() as connection:
                slot_owner = (
                    await connection.execute(
                        sa.text(
                            f"SELECT owner_run_id FROM {scheduler_world.schema}.session_slot "
                            "WHERE session_id = :session"
                        ),
                        {"session": scheduler_world.session_id},
                    )
                ).scalar_one()
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.work_item SET available_at = :now "
                        "WHERE id = :work"
                    ),
                    {"now": datetime.now(UTC), "work": scheduler_world.work_id},
                )
            control = await scheduler.claim_work(
                request("worker-cancel-control", lanes=("control",))
            )
            return reports, slot_owner, control
        finally:
            await engine.dispose()

    reports, slot_owner, control = run(scenario)
    assert sum(report.requeued for report in reports) == 1
    assert slot_owner == scheduler_world.run_id
    assert isinstance(control, ControlClaimedWork)
    assert control.desired_control == "cancel"


def test_expired_deadline_converges_to_explainable_failure(scheduler_world: Any) -> None:
    async def scenario() -> tuple[Any, dict[str, Any], dict[str, Any]]:
        engine = engine_for(scheduler_world.schema, search_path=False)
        scheduler = SqlScheduler(engine, scheduler_world.schema)
        recovery = SqlRecovery(engine, scheduler_world.schema)
        try:
            claimed = await scheduler.claim_work(request("worker-deadline"))
            assert isinstance(claimed, ClaimedWork)
            expired = datetime.now(UTC) - timedelta(seconds=1)
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.run SET lease_until = :expired "
                        "WHERE id = :run"
                    ),
                    {"expired": expired, "run": scheduler_world.run_id},
                )
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.work_item SET lease_until = :expired, "
                        "deadline_at = :expired WHERE id = :work"
                    ),
                    {"expired": expired, "work": scheduler_world.work_id},
                )
            report = await recovery.recover_expired_leases(scheduler_world.scope)
            async with engine.connect() as connection:
                run_row = dict(
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT status, failure FROM {scheduler_world.schema}.run "
                                "WHERE id = :id"
                            ),
                            {"id": scheduler_world.run_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                work_row = dict(
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT status, failure FROM {scheduler_world.schema}.work_item "
                                "WHERE id = :id"
                            ),
                            {"id": scheduler_world.work_id},
                        )
                    )
                    .mappings()
                    .one()
                )
            return report, run_row, work_row
        finally:
            await engine.dispose()

    report, run_row, work_row = run(scenario)
    assert report.failed == 1
    assert run_row["status"] == "failed"
    assert work_row["status"] == "failed"
    assert run_row["failure"]["code"] == "DELIVERY_EXPIRED"
    assert work_row["failure"]["code"] == "DELIVERY_EXPIRED"


def test_child_claim_is_fenced_without_taking_session_slot(scheduler_world: Any) -> None:
    async def scenario() -> tuple[list[Any], Any]:
        engine = engine_for(scheduler_world.schema, pool_size=4, search_path=False)
        scheduler = SqlScheduler(engine, scheduler_world.schema)
        child_run = uuid.uuid4()
        child_work = uuid.uuid4()
        workspace = uuid.uuid4()
        child_budget = uuid.uuid4()
        now = datetime.now(UTC)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {scheduler_world.schema}.run "
                        "(id, scope_id, session_id, turn_id, root_run_id, parent_run_id, "
                        "agent_revision_id, task_id, status, phase, state_version, owner_epoch, "
                        "cancel_version, wake_generation, pending_input_version, "
                        "applied_input_version, authority_revision, phase_started_at, deadline_at) "
                        "VALUES (:id, :scope, :session, :turn, :root, :root, :agent, :task, "
                        "'queued', 'queued', 1, 0, 0, 0, 0, 0, 0, :now, :deadline)"
                    ),
                    {
                        "id": child_run,
                        "scope": scheduler_world.scope,
                        "session": scheduler_world.session_id,
                        "turn": scheduler_world.turn_id,
                        "root": scheduler_world.run_id,
                        "agent": scheduler_world.agent_revision_id,
                        "task": scheduler_world.task_id,
                        "now": now,
                        "deadline": now + timedelta(hours=1),
                    },
                )
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {scheduler_world.schema}.work_item "
                        "(id, scope_id, run_id, subject_ref, kind, dedup_key, status, "
                        "payload_ref, wake_reason, priority, lease_epoch, attempt_count, "
                        "available_at, deadline_at) VALUES (:id, :scope, :run, '{}', "
                        "'advance_run', :dedup, 'pending', 'child:1', 'start', 200, 0, 0, "
                        ":now, :deadline)"
                    ),
                    {
                        "id": child_work,
                        "scope": scheduler_world.scope,
                        "run": child_run,
                        "dedup": f"child:{child_run}",
                        "now": now,
                        "deadline": now + timedelta(hours=1),
                    },
                )
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {scheduler_world.schema}.workspace "
                        "(id, scope_id, task_id, owner_run_id, generation, revision) "
                        "VALUES (:id, :scope, :task, :run, 1, 1)"
                    ),
                    {
                        "id": workspace,
                        "scope": scheduler_world.scope,
                        "task": scheduler_world.task_id,
                        "run": child_run,
                    },
                )
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {scheduler_world.schema}.budget_allocation "
                        "(id,scope_id,root_run_id,parent_id,limits,reserved,unknown_reserved,"
                        "settled,usage_status,unsettled_count,version) "
                        f"SELECT :id,scope_id,root_run_id,id,'{{}}','{{}}','{{}}','{{}}',"
                        f"'complete',0,0 FROM {scheduler_world.schema}.budget_allocation "
                        "WHERE root_run_id=:root AND parent_id IS NULL"
                    ),
                    {"id": child_budget, "root": scheduler_world.run_id},
                )
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {scheduler_world.schema}.agent_message "
                        "(id,scope_id,sender_run_id,recipient_run_id,task_id,correlation_id,"
                        "recipient_seq,protocol_revision,payload,kind,payload_hash,status,"
                        "trace_id,deadline_at) "
                        "VALUES (:id,:scope,:parent,:child,:task,:task,1,1,CAST(:payload AS jsonb),"
                        "'task.assign',:hash,'pending','scheduler-test',:deadline)"
                    ),
                    {
                        "id": uuid.uuid4(),
                        "scope": scheduler_world.scope,
                        "parent": scheduler_world.run_id,
                        "child": child_run,
                        "task": scheduler_world.task_id,
                        "payload": json.dumps({"budgetAllocationId": str(child_budget)}),
                        "hash": "a" * 64,
                        "deadline": now + timedelta(hours=1),
                    },
                )
            results = await asyncio.gather(
                *(
                    scheduler.claim_work(request(f"child-worker-{index}", lanes=("child",)))
                    for index in range(4)
                )
            )
            async with engine.connect() as connection:
                slot_owner = (
                    await connection.execute(
                        sa.text(
                            f"SELECT owner_run_id FROM {scheduler_world.schema}.session_slot "
                            "WHERE session_id = :session"
                        ),
                        {"session": scheduler_world.session_id},
                    )
                ).scalar_one_or_none()
            return results, slot_owner
        finally:
            await engine.dispose()

    results, slot_owner = run(scenario)
    assert sum(isinstance(item, ChildClaimedWork) for item in results) == 1
    assert slot_owner is None


@pytest.mark.parametrize(
    "case,expected",
    [
        ("retry", "retry_scheduled"),
        ("incomplete", "retry_scheduled"),
        ("context_limit", "retry_scheduled"),
        ("invalid_stream", "retry_disallowed"),
        ("unknown_usage", "usage_unresolved"),
        ("model_budget", "model_budget_exhausted"),
        ("token_budget", "token_budget_exhausted"),
        ("revoked", "authority_unavailable"),
        ("wrong_claim", "unproven_model_failure"),
        ("control", "control_pending"),
        ("control_cancel", "control_pending"),
        ("attempt_limit", "attempts_exhausted"),
        ("deadline", "run_deadline"),
        ("unknown_effect", "unresolved_effect"),
        ("rollback", "retry_scheduled"),
    ],
)
def test_model_retry_requires_locked_evidence(
    scheduler_world: Any, case: str, expected: str
) -> None:
    async def scenario() -> tuple[Any, str, str, Any]:
        engine = engine_for(scheduler_world.schema, search_path=False)
        now = datetime.now(UTC)
        scheduler = SqlScheduler(engine, scheduler_world.schema, clock=lambda: now)
        recovery = SqlRecovery(
            engine,
            scheduler_world.schema,
            clock=lambda: now,
            config=RecoveryConfig(base_retry_seconds=1),
        )
        try:
            claimed = await scheduler.claim_work(
                request("worker-retry").model_copy(update={"lease_seconds": 300})
            )
            assert isinstance(claimed, ClaimedWork)
            attempt = await _seed_attempt(scheduler_world)
            failure = FailureEnvelope(
                code=(
                    "INCOMPLETE_MODEL_OUTPUT"
                    if case in {"incomplete", "invalid_stream"}
                    else "CONTEXT_LIMIT"
                    if case == "context_limit"
                    else "DEPENDENCY_UNAVAILABLE"
                ),
                category="provider",
                retryable=case != "invalid_stream",
                public_message="Provider temporarily unavailable",
                attempt_id=str(attempt),
            )
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.model_attempt SET status='failed',"
                        "request_key=:key,failure=CAST(:failure AS jsonb),"
                        "usage=CAST(:usage AS jsonb) "
                        "WHERE id=:id"
                    ),
                    {
                        "id": attempt,
                        "key": f"test:{attempt}"
                        + (
                            ":wrong-claim"
                            if case == "wrong_claim"
                            else claim_attempt_suffix(claimed.lease)
                        ),
                        "failure": failure.model_dump_json(by_alias=True),
                        "usage": Usage(
                            status="complete",
                            reason="provider_reported",
                            input_tokens="1",
                            output_tokens="1",
                            total_tokens="2",
                        ).model_dump_json(by_alias=True),
                    },
                )
                await db.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.budget_allocation "
                        "SET limits=CAST(:limits AS jsonb) "
                        "WHERE id=:id"
                    ),
                    {
                        "id": claimed.budget_allocation_id,
                        "limits": json.dumps(
                            {
                                "model_calls": 1
                                if case in {"model_budget", "control", "control_cancel"}
                                else 10,
                                "tokens": 2 if case == "token_budget" else 1000,
                                "tool_calls": 10,
                            }
                        ),
                    },
                )
                changes = {
                    "unknown_usage": "UPDATE {s}.model_attempt "
                    "SET usage=NULL,usage_status='unknown',"
                    "usage_reason='collection_failed' WHERE id=:attempt",
                    "revoked": "UPDATE {s}.capability_grant SET revoked_at=now() WHERE run_id=:run",
                    "attempt_limit": "UPDATE {s}.work_item SET attempt_count=5 WHERE id=:work",
                    "deadline": "UPDATE {s}.task SET deadline_at=:now WHERE id=:task",
                    "control_cancel": "UPDATE {s}.work_item SET deadline_at=:now WHERE id=:work",
                }
                if case in changes:
                    await db.execute(
                        sa.text(changes[case].format(s=scheduler_world.schema)),
                        {
                            "attempt": attempt,
                            "run": scheduler_world.run_id,
                            "work": scheduler_world.work_id,
                            "task": scheduler_world.task_id,
                            "now": now,
                        },
                    )
                if case == "unknown_usage":
                    await db.execute(
                        sa.text(
                            f"UPDATE {scheduler_world.schema}.budget_allocation "
                            "SET reserved=CAST(:held AS jsonb),"
                            "unknown_reserved=CAST(:held AS jsonb) WHERE id=:id"
                        ),
                        {
                            "held": json.dumps({"model_calls": 1, "tokens": 100}),
                            "id": claimed.budget_allocation_id,
                        },
                    )
            if case == "unknown_effect":
                await _seed_execution(scheduler_world, status="unknown")
            if case in {"control", "control_cancel"}:
                await SqlStateUnitOfWork(
                    engine, scheduler_world.schema, actor(world=scheduler_world)
                ).record_control(
                    ControlRequest(
                        run_id=scheduler_world.run_id,
                        kind="cancel" if case == "control_cancel" else "pause",
                        expected_version=claimed.state_version,
                        idempotency_key="retry-control",
                    )
                )
            if case == "rollback":

                async def abort(*args: Any, **kwargs: Any) -> None:
                    raise RuntimeError("injected failure between Work and Run updates")

                with pytest.MonkeyPatch.context() as patch:
                    patch.setattr(recovery, "_release_run", abort)
                    with pytest.raises(RuntimeError, match="injected failure"):
                        await recovery.handle_worker_failure(claimed, AgentRuntimeError(failure))
                async with engine.connect() as db:
                    status = (
                        await db.execute(
                            sa.text(
                                "SELECT w.status,r.status FROM "
                                f"{scheduler_world.schema}.work_item w "
                                f"JOIN {scheduler_world.schema}.run r "
                                "ON r.id=w.run_id WHERE w.id=:id"
                            ),
                            {"id": scheduler_world.work_id},
                        )
                    ).one()
                    assert tuple(status) == ("leased", "running")
            settlement, duplicate = await asyncio.gather(
                recovery.handle_worker_failure(claimed, AgentRuntimeError(failure)),
                recovery.handle_worker_failure(claimed, AgentRuntimeError(failure)),
            )
            settlements = [settlement, duplicate]
            assert sorted(item.disposition for item in settlements).count("fenced") == 1
            settlement = next(item for item in settlements if item.disposition != "fenced")
            assert settlement.reason == expected
            if case in {"control", "control_cancel"}:
                async with engine.connect() as db:
                    deadline = (
                        await db.execute(
                            sa.text(
                                f"SELECT deadline_at FROM {scheduler_world.schema}.work_item "
                                "WHERE id=:id"
                            ),
                            {"id": scheduler_world.work_id},
                        )
                    ).scalar_one()
                    assert deadline > now
            async with engine.connect() as db:
                assert (
                    await db.execute(
                        sa.text(
                            f"SELECT count(*) FROM {scheduler_world.schema}.work_item "
                            "WHERE run_id=:id AND kind='advance_run'"
                        ),
                        {"id": scheduler_world.run_id},
                    )
                ).scalar_one() == 1
            if case == "unknown_usage":
                async with engine.connect() as db:
                    held = (
                        (
                            await db.execute(
                                sa.text(
                                    "SELECT reserved,unknown_reserved FROM "
                                    f"{scheduler_world.schema}.budget_allocation "
                                    "WHERE id=:id"
                                ),
                                {"id": claimed.budget_allocation_id},
                            )
                        )
                        .mappings()
                        .one()
                    )
                    assert (
                        held["reserved"]
                        == held["unknown_reserved"]
                        == {
                            "model_calls": 1,
                            "tokens": 100,
                        }
                    )
            if case == "retry":
                # Backoff is persistent, not an in-memory sleep or a duplicate WorkItem.
                assert isinstance(await scheduler.claim_work(request("too-early")), IdleWork)
                async with engine.begin() as db:
                    await db.execute(
                        sa.text(
                            f"UPDATE {scheduler_world.schema}.work_item "
                            "SET available_at=:now WHERE id=:id"
                        ),
                        {"id": scheduler_world.work_id, "now": now},
                    )
                next_claim = await scheduler.claim_work(request("worker-next"))
                assert isinstance(next_claim, ClaimedWork)
                scheduler_world.values.update(
                    {
                        "snapshot_id": (
                            await _snapshot_id(engine, scheduler_world.schema, attempt)
                        ),
                        "budget_id": claimed.budget_allocation_id,
                    }
                )
                next_request = provider_request(scheduler_world)
                expired_store = SqlModelAttemptStore(
                    engine,
                    scheduler_world.schema,
                    actor=actor(world=scheduler_world),
                    lease=lambda: next_claim.lease,
                    clock=lambda: next_claim.lease.run_lease_until + timedelta(seconds=1),
                )
                with pytest.raises(AgentRuntimeError):
                    await expired_store.begin(
                        provider_request(scheduler_world), started_at=datetime.now(UTC)
                    )
                store = SqlModelAttemptStore(
                    engine,
                    scheduler_world.schema,
                    actor=actor(world=scheduler_world),
                    lease=lambda: next_claim.lease,
                )
                await store.begin(next_request, started_at=datetime.now(UTC))
                await store.finish(
                    next_request,
                    AttemptTerminal(
                        status="completed",
                        finish_reason="stop",
                        failure=None,
                        usage=Usage(
                            status="complete",
                            reason="provider_reported",
                            input_tokens="1",
                            output_tokens="1",
                            total_tokens="2",
                        ),
                        finished_at=datetime.now(UTC),
                    ),
                )
                assert next_request.attempt_id != attempt
                # An old claim cannot create another paid provider attempt after takeover.
                old_store = SqlModelAttemptStore(
                    engine,
                    scheduler_world.schema,
                    actor=actor(world=scheduler_world),
                    lease=lambda: claimed.lease,
                )
                with pytest.raises(AgentRuntimeError):
                    await old_store.begin(
                        provider_request(scheduler_world), started_at=datetime.now(UTC)
                    )
                return (
                    settlement,
                    "pending",
                    "queued",
                    await scheduler.renew(RenewRequest(lease=claimed.lease, lease_seconds=30)),
                )
            async with engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT w.status AS work_status, r.status AS run_status, "
                                "s.owner_run_id FROM "
                                f"{scheduler_world.schema}.work_item w JOIN "
                                f"{scheduler_world.schema}.run r ON r.id = w.run_id JOIN "
                                f"{scheduler_world.schema}.session_slot s "
                                "ON s.session_id = r.session_id "
                                "WHERE w.id = :work"
                            ),
                            {"work": scheduler_world.work_id},
                        )
                    )
                    .mappings()
                    .one()
                )
            renewed = await scheduler.renew(RenewRequest(lease=claimed.lease, lease_seconds=30))
            return settlement, str(row["work_status"]), str(row["run_status"]), renewed
        finally:
            await engine.dispose()

    settlement, work_status, run_status, renewed = run(scenario)
    retry = expected in {"retry_scheduled", "control_pending"}
    assert settlement.disposition == ("requeued" if retry else "failed")
    assert work_status == ("pending" if retry else "failed")
    assert run_status == (
        "cancelling" if case == "control_cancel" else "queued" if retry else "failed"
    )
    assert renewed.disposition == "lost"


async def _snapshot_id(engine: Any, schema: str, attempt: uuid.UUID) -> uuid.UUID:
    async with engine.connect() as db:
        return (
            await db.execute(
                sa.text(f"SELECT context_snapshot_id FROM {schema}.model_attempt WHERE id=:id"),
                {"id": attempt},
            )
        ).scalar_one()


def test_unclaimed_work_deadline_is_converged(scheduler_world: Any) -> None:
    async def scenario() -> tuple[Any, str, str]:
        engine = engine_for(scheduler_world.schema, search_path=False)
        recovery = SqlRecovery(engine, scheduler_world.schema)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.work_item SET deadline_at = :expired "
                        "WHERE id = :work"
                    ),
                    {
                        "expired": datetime.now(UTC) - timedelta(seconds=1),
                        "work": scheduler_world.work_id,
                    },
                )
            report = await recovery.recover_expired_leases(scheduler_world.scope)
            async with engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT w.status AS work_status, r.status AS run_status FROM "
                                f"{scheduler_world.schema}.work_item w JOIN "
                                f"{scheduler_world.schema}.run r ON r.id = w.run_id "
                                "WHERE w.id = :work"
                            ),
                            {"work": scheduler_world.work_id},
                        )
                    )
                    .mappings()
                    .one()
                )
            return report, str(row["work_status"]), str(row["run_status"])
        finally:
            await engine.dispose()

    report, work_status, run_status = run(scenario)
    assert report.failed == 1
    assert work_status == "failed"
    assert run_status == "failed"


def _yield_request(
    scheduler_world: Any,
    claimed: ClaimedWork,
    checkpoint_id: uuid.UUID,
    *,
    expected_state_version: int,
    pending: int,
    applied: int,
) -> YieldRequest:
    return YieldRequest(
        guard=CommitGuard(
            actor=actor(scheduler_world.auth_revision, world=scheduler_world),
            lease=claimed.lease,
            expected_state_version=str(expected_state_version),
        ),
        checkpoint_id=checkpoint_id,
        wait_spec=WaitSpec.model_validate(
            {
                "kind": "message",
                "subjectId": "mailbox-input",
                "deadlineAt": datetime.now(UTC) + timedelta(hours=1),
                "resumePredicate": {
                    "key": {"namespace": "builtin", "key": "mailbox-ready"},
                    "revision": "1",
                },
            }
        ),
        inputs=InputWatermark(
            pending_version=str(pending),
            applied_version=str(applied),
        ),
    )


def test_consecutive_yield_keeps_a_live_advance_work(scheduler_world: Any) -> None:
    async def scenario() -> tuple[Any, Any, Any, list[dict[str, Any]]]:
        engine = engine_for(scheduler_world.schema, search_path=False)
        scheduler = SqlScheduler(engine, scheduler_world.schema)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.run SET pending_input_version = 3, "
                        "applied_input_version = 2 WHERE id = :run"
                    ),
                    {"run": scheduler_world.run_id},
                )
            first_claim = await scheduler.claim_work(request("yield-worker-1"))
            assert isinstance(first_claim, ClaimedWork)
            first_yield = await scheduler.yield_run(
                _yield_request(
                    scheduler_world,
                    first_claim,
                    scheduler_world.checkpoint_id,
                    expected_state_version=1,
                    pending=3,
                    applied=2,
                )
            )
            second_claim = await scheduler.claim_work(request("yield-worker-2"))
            assert isinstance(second_claim, ClaimedWork)
            second_checkpoint = uuid.uuid4()
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {scheduler_world.schema}.checkpoint "
                        "(id, scope_id, run_id, state_version, representation_revision, "
                        "event_seq, schema_revision, runtime_build, state_ref, content_hash) "
                        "VALUES (:id, :scope, :run, 3, 1, 0, 1, 'test', 'state:3', :hash)"
                    ),
                    {
                        "id": second_checkpoint,
                        "scope": scheduler_world.scope,
                        "run": scheduler_world.run_id,
                        "hash": "d" * 64,
                    },
                )
            second_yield = await scheduler.yield_run(
                _yield_request(
                    scheduler_world,
                    second_claim,
                    second_checkpoint,
                    expected_state_version=2,
                    pending=3,
                    applied=2,
                )
            )
            async with engine.connect() as connection:
                live = [
                    dict(row)
                    for row in (
                        await connection.execute(
                            sa.text(
                                f"SELECT id, status, owner_id, lease_epoch FROM "
                                f"{scheduler_world.schema}.work_item WHERE run_id = :run "
                                "AND kind = 'advance_run' AND status IN ('pending','leased')"
                            ),
                            {"run": scheduler_world.run_id},
                        )
                    ).mappings()
                ]
            third_claim = await scheduler.claim_work(request("yield-worker-3"))
            return first_yield, second_yield, third_claim, live
        finally:
            await engine.dispose()

    first_yield, second_yield, third_claim, live = run(scenario)
    assert first_yield.disposition == "requeued"
    assert second_yield.disposition == "requeued"
    assert first_yield.work_id != second_yield.work_id
    assert len(live) == 1 and live[0]["status"] == "pending"
    assert live[0]["owner_id"] is None
    assert isinstance(third_claim, ClaimedWork)


@pytest.mark.parametrize(
    ("stored_pending", "stored_applied", "request_pending", "request_applied"),
    [(0, 0, 1, 0), (1, 0, 0, 0)],
)
def test_yield_rejects_stale_input_watermark_without_writes(
    scheduler_world: Any,
    stored_pending: int,
    stored_applied: int,
    request_pending: int,
    request_applied: int,
) -> None:
    async def scenario() -> tuple[str, dict[str, Any]]:
        engine = engine_for(scheduler_world.schema, search_path=False)
        scheduler = SqlScheduler(engine, scheduler_world.schema)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"UPDATE {scheduler_world.schema}.run SET pending_input_version = "
                        ":pending, applied_input_version = :applied WHERE id = :run"
                    ),
                    {
                        "pending": stored_pending,
                        "applied": stored_applied,
                        "run": scheduler_world.run_id,
                    },
                )
            claimed = await scheduler.claim_work(request("yield-stale-worker"))
            assert isinstance(claimed, ClaimedWork)
            try:
                await scheduler.yield_run(
                    _yield_request(
                        scheduler_world,
                        claimed,
                        scheduler_world.checkpoint_id,
                        expected_state_version=1,
                        pending=request_pending,
                        applied=request_applied,
                    )
                )
            except AgentRuntimeError as exc:
                code = exc.failure.code
            else:
                raise AssertionError("stale input watermark was accepted")
            async with engine.connect() as connection:
                row = dict(
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT r.status, r.state_version, r.owner_id, "
                                "s.owner_run_id, w.status AS work_status, "
                                f"w.lease_epoch FROM {scheduler_world.schema}.run r "
                                f"JOIN {scheduler_world.schema}.session_slot s "
                                "ON s.session_id = r.session_id "
                                f"JOIN {scheduler_world.schema}.work_item w "
                                "ON w.run_id = r.id WHERE r.id = :run AND w.id = :work"
                            ),
                            {"run": scheduler_world.run_id, "work": scheduler_world.work_id},
                        )
                    )
                    .mappings()
                    .one()
                )
            return code, row
        finally:
            await engine.dispose()

    code, row = run(scenario)
    assert code == "VERSION_CONFLICT"
    assert row["status"] == "running" and int(row["state_version"]) == 1
    assert row["owner_id"] == "yield-stale-worker"
    assert row["owner_run_id"] == scheduler_world.run_id
    assert row["work_status"] == "leased" and int(row["lease_epoch"]) == 2


def test_waiting_pause_has_one_durable_control_work(scheduler_world: Any) -> None:
    async def exercise() -> None:
        world = scheduler_world
        engine = engine_for(world.schema, search_path=False)
        state = SqlStateUnitOfWork(engine, world.schema, actor(world=world))
        try:
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".work_item '
                        "SET status='completed',owner_id=NULL,lease_until=NULL WHERE run_id=:run"
                    ),
                    {"run": world.run_id},
                )
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".run '
                        "SET status='waiting_user',phase='waiting',owner_id=NULL,lease_until=NULL "
                        "WHERE id=:run"
                    ),
                    {"run": world.run_id},
                )
            command = ControlRequest(
                run_id=world.run_id,
                kind="pause",
                idempotency_key="waiting-pause",
                expected_version="1",
            )
            accepted = await state.record_control(command)
            duplicate = await state.record_control(command)
            assert accepted.command_id == duplicate.command_id
            async with engine.connect() as db:
                pending = (
                    await db.execute(
                        sa.text(
                            f'SELECT kind,wake_reason FROM "{world.schema}".work_item '
                            "WHERE run_id=:run AND status='pending'"
                        ),
                        {"run": world.run_id},
                    )
                ).all()
            assert len(pending) == 1 and pending[0] == ("advance_run", "control")
            claimed = await SqlScheduler(engine, world.schema).claim_work(
                request("waiting-pause-worker", lanes=("control",))
            )
            assert isinstance(claimed, ControlClaimedWork)
            assert claimed.desired_control == "pause"
        finally:
            await engine.dispose()

    run(exercise)


def test_pending_input_race_requeues_without_model_retry_or_deadline_extension(
    scheduler_world: Any,
) -> None:
    from test_storage_uow import _seed_messages

    async def exercise() -> None:
        world = scheduler_world
        engine = engine_for(world.schema, search_path=False)
        try:
            claimed = await SqlScheduler(engine, world.schema).claim_work(request("input-race"))
            assert isinstance(claimed, ClaimedWork)
            await _seed_messages(world, (1,), allocated=1, claim_token=uuid.uuid4())
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".agent_message '
                        "SET status='pending',claim_token=NULL,lease_until=NULL "
                        "WHERE recipient_run_id=:run"
                    ),
                    {"run": world.run_id},
                )
                original = (
                    await db.execute(
                        sa.text(
                            f'SELECT deadline_at FROM "{world.schema}".work_item WHERE id=:work'
                        ),
                        {"work": claimed.lease.work_id},
                    )
                ).scalar_one()
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".work_item SET attempt_count=99 WHERE id=:work'
                    ),
                    {"work": claimed.lease.work_id},
                )
            failed = AgentRuntimeError(
                FailureEnvelope(
                    code="VERSION_CONFLICT",
                    category="concurrency",
                    retryable=False,
                    public_message="Wait input watermark changed before commit.",
                )
            )
            outcome = await SqlRecovery(engine, world.schema).handle_worker_failure(claimed, failed)
            assert outcome.disposition == "requeued" and outcome.reason == "input_pending"
            async with engine.connect() as db:
                state = (
                    (
                        await db.execute(
                            sa.text(
                                "SELECT r.status,r.pending_input_version,r.applied_input_version,"
                                "w.status AS work_status,"
                                f'w.deadline_at FROM "{world.schema}".run r '
                                f'JOIN "{world.schema}".work_item w '
                                "ON w.run_id=r.id WHERE w.id=:work"
                            ),
                            {"work": claimed.lease.work_id},
                        )
                    )
                    .mappings()
                    .one()
                )
            assert state["status"] == "queued" and state["work_status"] == "pending"
            assert state["pending_input_version"] == 1 and state["applied_input_version"] == 0
            assert state["deadline_at"] <= original
            assert (
                await SqlRecovery(engine, world.schema).handle_worker_failure(claimed, failed)
            ).disposition == "fenced"
        finally:
            await engine.dispose()

    run(exercise)

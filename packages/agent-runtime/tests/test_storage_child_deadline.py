"""Real SQL deadline audit and limited recovery, including parent pause convergence."""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from test_provider_invoker import capabilities, make_request
from test_storage_child_recovery import pair  # noqa: F401
from test_storage_uow import _seed_attempt, actor, engine_for, intake, lease, migrate, run
from test_worker_topology import request as worker_request

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.ownership import ChildCommitGuard, ChildLeaseToken
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.work_claims import ChildClaimedWork, ControlClaimedWork
from agent_runtime.provider import ProviderInvoker
from agent_runtime.storage.child_authority import require_child
from agent_runtime.storage.child_cancel import SqlChildCancelSignal
from agent_runtime.storage.main_control import SqlMainControlHandler
from agent_runtime.storage.model_attempts import SqlModelAttemptStore
from agent_runtime.storage.recovery import SqlRecovery
from agent_runtime.storage.worker_topology import SqlWorkerTopology
from agent_runtime.worker import ChildLeaseHandle, LeaseHandle

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENT_RUNTIME_TEST_DSN"), reason="requires guarded real PostgreSQL"
)


@pytest.fixture(scope="module")
def recovery_schema():
    assert sa.make_url(os.environ["AGENT_RUNTIME_TEST_DSN"]).database == "aletai_wangxh"
    schema = f"r05_child_deadline_{uuid4().hex[:12]}"
    try:
        assert migrate("upgrade", schema).status == "applied"
        yield schema
    finally:

        async def drop():
            engine = engine_for(schema, search_path=False)
            try:
                async with engine.begin() as db:
                    await db.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            finally:
                await engine.dispose()

        run(drop)


@pytest.mark.parametrize("interrupt", ["watcher", "provider_timeout", "early_provider_timeout"])
def test_deadline_race_preserves_audit_and_only_recovers_expired_children(pair, interrupt):  # noqa: F811
    parent, child, engine, _, execute, rows = pair
    future = datetime.now(UTC) + timedelta(minutes=10)
    for world in (parent, child):
        run(
            lambda world=world: execute(
                "UPDATE {t}run SET status='running',phase='model',waiting_spec=NULL,"
                "owner_id=:owner,lease_until=:future WHERE id=:id",
                owner=world.owner,
                future=future,
                id=world.run_id,
            )
        )
        run(
            lambda world=world: execute(
                "UPDATE {t}work_item SET status='leased',owner_id=:owner,lease_until=:future "
                "WHERE id=:id",
                owner=world.owner,
                future=future,
                id=world.work_id,
            )
        )
    seeded = run(lambda: _seed_attempt(child))
    snapshot = run(
        lambda: rows("SELECT context_snapshot_id FROM {t}model_attempt WHERE id=:id", id=seeded)
    )[0]["context_snapshot_id"]
    receipt = intake(parent, "pause", f"deadline-pause:{uuid4()}")
    parent_row = run(lambda: rows("SELECT * FROM {t}run WHERE id=:id", id=parent.run_id))[0]
    control = ControlClaimedWork(
        disposition="control_claimed",
        lease=lease(parent),
        state_version=str(parent_row["state_version"]),
        command_id=receipt.command_id,
        desired_control="pause",
        status="running",
        cancel_version=str(parent_row["cancel_version"]),
        checkpoint_id=parent_row["checkpoint_id"],
    )
    handler = SqlMainControlHandler(engine, child.schema, actor(world=parent))
    with pytest.raises(AgentRuntimeError) as pending:
        run(lambda: handler(control, LeaseHandle(control.lease)))
    assert pending.value.failure.code == "CONTROL_PENDING"
    token = ChildLeaseToken.model_validate(lease(child).model_dump(exclude={"slot_version"}))
    workspace = ResourceRef(
        resource_type="workspace",
        resource_id=str(child.workspace_id),
        resource_version="1",
        scope=ResourceScope(scope_id=child.scope),
    )
    claimed = ChildClaimedWork(
        disposition="child_claimed",
        lease=token,
        root_run_id=parent.run_id,
        root_cancel_version=str(parent_row["cancel_version"]),
        state_version=str(child.state_version),
        checkpoint_id=child.checkpoint_id,
        budget_allocation_id=uuid4(),
        workspace_ref=workspace,
        workspace_generation="1",
    )
    guard = ChildCommitGuard(
        actor=actor(world=child),
        lease=token,
        expected_state_version=claimed.state_version,
        root_run_id=parent.run_id,
        expected_root_cancel_version=claimed.root_cancel_version,
        workspace_ref=workspace,
        workspace_owner_run_id=child.run_id,
        workspace_generation="1",
    )
    request = make_request().model_copy(
        update={
            "scope_id": child.scope,
            "session_id": child.session_id,
            "run_id": child.run_id,
            "context_snapshot_id": snapshot,
            "deadline_at": future,
        }
    )
    entered, closed = asyncio.Event(), asyncio.Event()

    class Provider:
        async def stream(self, request, *, cancel=None):
            try:
                entered.set()
                await asyncio.Event().wait()
                yield
            finally:
                closed.set()

    async def expire_during_call():
        async with AsyncSession(engine) as db, db.begin():
            await require_child(db, child.schema, guard, now=datetime.now(UTC))
        if interrupt != "watcher":
            request_deadline = datetime.now(UTC) + timedelta(seconds=5)
            active_request = request.model_copy(update={"deadline_at": request_deadline})
        else:
            active_request = request
        expired = (
            datetime.now(UTC) - timedelta(seconds=1)
            if interrupt == "watcher"
            else active_request.deadline_at
        )
        if interrupt == "provider_timeout":
            for table, identity in (("run", child.run_id), ("work_item", child.work_id)):
                await execute(
                    f"UPDATE {{t}}{table} SET deadline_at=:now WHERE id=:id",
                    now=expired,
                    id=identity,
                )
            await execute(
                "UPDATE {t}capability_grant SET expires_at=:now,"
                "issued_at=LEAST(issued_at,CAST(:now AS timestamptz) - interval '1 second') "
                "WHERE run_id=:id",
                now=expired,
                id=child.run_id,
            )
        signal = SqlChildCancelSignal(
            engine, child.schema, guard.actor, claimed, ChildLeaseHandle(token)
        )
        invoker = ProviderInvoker(
            Provider(),
            capabilities=capabilities,
            attempts=SqlModelAttemptStore(engine, child.schema),
        )

        async def collect():
            return [event async for event in invoker.stream(active_request, cancel=signal)]

        task = asyncio.create_task(collect())
        try:
            await asyncio.wait_for(entered.wait(), timeout=10)
            if interrupt == "watcher":
                for table, identity in (("run", child.run_id), ("work_item", child.work_id)):
                    await execute(
                        f"UPDATE {{t}}{table} SET deadline_at=:now WHERE id=:id",
                        now=expired,
                        id=identity,
                    )
                await execute(
                    "UPDATE {t}capability_grant SET expires_at=:now,"
                    "issued_at=LEAST(issued_at,CAST(:now AS timestamptz) - interval '1 second') "
                    "WHERE run_id=:id",
                    now=expired,
                    id=child.run_id,
                )
                await signal.watch()
                with pytest.raises(AgentRuntimeError) as caught:
                    await asyncio.wait_for(task, timeout=10)
                error = caught.value
            else:
                events = await asyncio.wait_for(task, timeout=10)
                assert events[-1].kind == "error"
                assert events[-1].failure.code == "PROVIDER_TIMEOUT"
                assert events[-1].failure.retryable
                assert signal._failure is None and not signal.is_cancelled()
                error = AgentRuntimeError(events[-1].failure)
            assert closed.is_set()
            if interrupt != "early_provider_timeout":
                async with AsyncSession(engine) as db, db.begin():
                    with pytest.raises(AgentRuntimeError) as forbidden:
                        await require_child(
                            db, child.schema, guard, now=datetime.now(UTC), allow_control=True
                        )
                    assert forbidden.value.failure.code == "FORBIDDEN"
            return error, expired
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    error, expired = run(expire_during_call)
    audit = run(
        lambda: rows(
            "SELECT status,usage_status,failure FROM {t}model_attempt WHERE id=:id",
            id=request.attempt_id,
        )
    )[0]
    assert audit["status"] == "failed" and audit["usage_status"] == "unknown"
    assert audit["failure"]["code"] == error.failure.code
    user_calls = []

    async def user_sink(work, handle, observed):
        user_calls.append((work, handle, observed))

    service = SqlWorkerTopology(engine, child.schema, scope_id=child.scope).child(
        worker_request(["child"]).model_copy(update={"scope_id": child.scope}),
        handlers=object(),
        errors=user_sink,
    )
    if interrupt == "early_provider_timeout":
        before = run(lambda: rows("SELECT * FROM {t}run WHERE id=:id", id=child.run_id))
        handle = ChildLeaseHandle(token)
        run(lambda: service._runner._child_error_sink(claimed, handle, error))
        assert user_calls == [(claimed, handle, error)]
        assert run(lambda: rows("SELECT * FROM {t}run WHERE id=:id", id=child.run_id)) == before
        assert run(
            lambda: rows("SELECT status FROM {t}work_item WHERE id=:id", id=child.work_id)
        ) == [{"status": "leased"}]
        assert not run(
            lambda: rows("SELECT id FROM {t}agent_message WHERE sender_run_id=:id", id=child.run_id)
        )
        return
    recovery = SqlRecovery(engine, child.schema)
    stale = token.model_copy(update={"run_epoch": str(int(token.run_epoch) + 1)})
    assert (
        run(
            lambda: recovery.handle_worker_failure(
                claimed.model_copy(update={"lease": stale}), error
            )
        ).disposition
        == "fenced"
    )

    stale_work = claimed.model_copy(update={"lease": stale})
    run(lambda: service._runner._child_error_sink(stale_work, ChildLeaseHandle(token), error))
    assert not user_calls
    assert run(lambda: recovery.handle_worker_failure(claimed, error)).disposition == "fenced"
    terminal = run(
        lambda: rows("SELECT status,failure,deadline_at FROM {t}run WHERE id=:id", id=child.run_id)
    )[0]
    assert terminal["status"] == "failed" and terminal["deadline_at"] == expired
    assert terminal["failure"]["code"] == error.failure.code
    assert run(
        lambda: rows("SELECT status,deadline_at FROM {t}work_item WHERE id=:id", id=child.work_id)
    ) == [{"status": "failed", "deadline_at": expired}]
    assert terminal["failure"]["recoveryOptions"]
    assert run(
        lambda: rows("SELECT expires_at FROM {t}capability_grant WHERE run_id=:id", id=child.run_id)
    ) == [{"expires_at": expired}]
    assert run(
        lambda: rows("SELECT kind FROM {t}agent_message WHERE sender_run_id=:id", id=child.run_id)
    ) == [{"kind": "task.failed"}]
    assert not run(
        lambda: rows(
            "SELECT id FROM {t}event WHERE run_id=:id "
            "AND kind IN ('run.completed','run.cancelled')",
            id=child.run_id,
        )
    )
    assert not run(lambda: rows("SELECT id FROM {t}budget_entry WHERE kind='settle'"))
    run(lambda: handler(control, LeaseHandle(control.lease)))
    assert run(lambda: rows("SELECT status FROM {t}run WHERE id=:id", id=parent.run_id)) == [
        {"status": "paused"}
    ]
    assert run(
        lambda: rows("SELECT status FROM {t}command WHERE id=:id", id=receipt.command_id)
    ) == [{"status": "applied"}]

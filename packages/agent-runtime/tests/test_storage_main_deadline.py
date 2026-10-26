"""Main model deadlines with live SQL leases; guarded disposable schemas only."""

import asyncio
import os
from datetime import UTC, datetime, timedelta

import pytest
import sqlalchemy as sa
from test_provider_invoker import ResolverLikeBlockingBackend, capabilities
from test_storage_model_attempts import attempt_world, provider_request  # noqa: F401
from test_storage_uow import actor, engine_for, lease, run

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.requests import ControlRequest
from agent_runtime.provider import ProviderInvoker
from agent_runtime.storage.main_cancel import SqlMainCancelSignal
from agent_runtime.storage.model_attempts import SqlModelAttemptStore
from agent_runtime.storage.uow import SqlStateUnitOfWork

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENT_RUNTIME_TEST_DSN"), reason="requires guarded real PostgreSQL"
)


@pytest.fixture(scope="module", autouse=True)
def guarded_database():
    assert sa.make_url(os.environ["AGENT_RUNTIME_TEST_DSN"]).database == "aletai_wangxh"


@pytest.mark.parametrize("stop", ["expired", "expires_while_streaming", "user_cancel"])
def test_main_deadline_interrupts_live_sql_model(attempt_world, stop):  # noqa: F811
    async def scenario():
        world = attempt_world
        engine = engine_for(world.schema)
        backend = ResolverLikeBlockingBackend()
        signal = SqlMainCancelSignal(
            engine,
            world.schema,
            lambda: lease(world),
            actor=actor(world=world),
            interval_seconds=0.01,
        )
        watcher = None
        try:
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        "UPDATE run SET deadline_at=:deadline,desired_control=NULL,"
                        "lease_until=:until WHERE id=:id"
                    ),
                    {
                        "id": world.run_id,
                        "deadline": datetime.now(UTC) + timedelta(minutes=1),
                        "until": datetime.now(UTC) + timedelta(minutes=5),
                    },
                )
            request = provider_request(world)
            invoker = ProviderInvoker(
                backend,
                capabilities=capabilities,
                attempts=SqlModelAttemptStore(engine, world.schema),
            )

            async def consume():
                return [event async for event in invoker.stream(request, cancel=signal)]

            consumer = asyncio.create_task(consume())
            try:
                await asyncio.wait_for(backend.started.wait(), 10)
                if stop == "expires_while_streaming":
                    watcher = asyncio.create_task(signal.watch())
                    async with asyncio.timeout(10):
                        while signal._deadline_at is None:
                            await asyncio.sleep(0.01)
                async with engine.begin() as db:
                    await db.execute(
                        sa.text("UPDATE run SET deadline_at=:deadline WHERE id=:id"),
                        {
                            "id": world.run_id,
                            "deadline": datetime.now(UTC)
                            + timedelta(seconds=60 if stop == "user_cancel" else -1),
                        },
                    )
                if stop == "user_cancel":
                    await SqlStateUnitOfWork(
                        engine, world.schema, actor(world=world)
                    ).record_control(
                        ControlRequest(
                            run_id=world.run_id,
                            kind="cancel",
                            idempotency_key="deadline-cancel",
                            expected_version="1",
                        )
                    )
                if watcher is None:
                    watcher = asyncio.create_task(signal.watch())
                if stop == "user_cancel":
                    events = await asyncio.wait_for(consumer, 10)
                    assert events[-1].reason == "cancelled"
                else:
                    with pytest.raises(AgentRuntimeError) as exc:
                        await asyncio.wait_for(consumer, 10)
                    assert exc.value.failure.code == "BUDGET_EXHAUSTED"
                assert backend.closed.is_set()
                async with engine.connect() as db:
                    row = (
                        (
                            await db.execute(
                                sa.text(
                                    "SELECT status,usage_status,failure,finished_at "
                                    "FROM model_attempt "
                                    "WHERE id=:id"
                                ),
                                {"id": request.attempt_id},
                            )
                        )
                        .mappings()
                        .one()
                    )
                    assert row["status"] == ("cancelled" if stop == "user_cancel" else "failed")
                    assert row["usage_status"] == "unknown"
                    assert row["finished_at"] is not None
                    if stop != "user_cancel":
                        assert row["failure"]["code"] == "BUDGET_EXHAUSTED"
                    else:
                        assert row["failure"] is None
                        assert (
                            await db.execute(
                                sa.text(
                                    "SELECT count(*) FROM work_item WHERE run_id=:id "
                                        "AND kind='reconcile_execution' "
                                        "AND payload_ref='cancel-convergence' "
                                        "AND status='pending'"
                                ),
                                {"id": world.run_id},
                            )
                        ).scalar_one() == 1
                    assert (
                        await db.execute(
                            sa.text("SELECT lease_until > now() FROM run WHERE id=:id"),
                            {"id": world.run_id},
                        )
                    ).scalar_one()
            finally:
                consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
        finally:
            if watcher is not None:
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)
            await engine.dispose()

    run(scenario)

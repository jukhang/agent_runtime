"""Explicit provider close commits an unknown interrupted SQL attempt, not a Run terminal."""

import os

import pytest
import sqlalchemy as sa
from test_provider_invoker import Backend, capabilities
from test_storage_model_attempts import attempt_world, provider_request  # noqa: F401
from test_storage_uow import engine_for, run

from agent_runtime.contracts.provider import ContentDelta
from agent_runtime.provider import ProviderInvoker
from agent_runtime.storage.model_attempts import SqlModelAttemptStore

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENT_RUNTIME_TEST_DSN"), reason="requires guarded real PostgreSQL"
)


@pytest.fixture(scope="module", autouse=True)
def guarded_database():
    assert sa.make_url(os.environ["AGENT_RUNTIME_TEST_DSN"]).database == "aletai_wangxh"


def test_loop_stream_close_commits_sql_interrupted_unknown(attempt_world):  # noqa: F811
    async def scenario():
        world = attempt_world
        engine = engine_for(world.schema)
        request = provider_request(world)
        backend = Backend(
            [
                ContentDelta(
                    attempt_id=request.attempt_id,
                    sequence="0",
                    kind="content.delta",
                    text="partial",
                )
            ]
        )
        invoker = ProviderInvoker(
            backend,
            capabilities=capabilities,
            attempts=SqlModelAttemptStore(engine, world.schema),
        )
        try:
            stream = invoker.stream(request)
            assert (await anext(stream)).kind == "content.delta"
            await stream.aclose()
            async with engine.connect() as db:
                row = (
                    (
                        await db.execute(
                            sa.text(
                                "SELECT status,usage_status,usage_reason,finished_at "
                                "FROM model_attempt "
                                "WHERE id=:id"
                            ),
                            {"id": request.attempt_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                assert row["status"] == "interrupted"
                assert row["usage_status"] == "unknown"
                assert row["usage_reason"] == "collection_failed"
                assert row["finished_at"] is not None
                assert (
                    await db.execute(
                        sa.text("SELECT status FROM run WHERE id=:id"), {"id": world.run_id}
                    )
                ).scalar_one() == "running"
                assert (
                    await db.execute(
                        sa.text(
                            "SELECT count(*) FROM event WHERE kind IN "
                            "('run.completed','run.failed','run.cancelled')"
                        )
                    )
                ).scalar_one() == 0
        finally:
            await engine.dispose()

    run(scenario)

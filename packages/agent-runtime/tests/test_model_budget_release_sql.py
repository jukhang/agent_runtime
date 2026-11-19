import json
import os
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_storage_model_attempts import attempt_world as attempt_world
from test_storage_uow import engine_for, run

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.storage.model_budget import closing_entries, release_closing, reserve_closing

pytestmark = pytest.mark.skipif(
    not os.getenv("AGENT_RUNTIME_TEST_DSN"), reason="requires PostgreSQL"
)


def test_unused_purpose_release_is_idempotent_and_preserves_unknown_reservations(attempt_world):
    world = attempt_world

    async def scenario():
        engine = engine_for(world.schema)
        try:
            for model_limit in (0, 1, 3, 4, 24):
                allocation = uuid4()
                async with engine.begin() as db:
                    await db.execute(
                        text(
                            "INSERT INTO budget_allocation(id,scope_id,root_run_id,limits,reserved,"
                            "unknown_reserved,settled,usage_status,unsettled_count,version) VALUES "
                            "(:id,:scope,:run,CAST(:limits AS jsonb),'{}','{}','{}','complete',0,0)"
                        ),
                        {
                            "id": allocation,
                            "scope": world.scope,
                            "run": world.run_id,
                            "limits": json.dumps(
                                {"model_calls": model_limit, "tokens": 1024, "tool_calls": 24}
                            ),
                        },
                    )
                if model_limit == 0:
                    async with engine.begin() as db:
                        with pytest.raises(AgentRuntimeError):
                            await reserve_closing(
                                db,
                                world.schema,
                                scope=world.scope,
                                allocation=allocation,
                                run=world.run_id,
                                tokens=128,
                            )
                    continue
                async with engine.begin() as db:
                    await reserve_closing(
                        db,
                        world.schema,
                        scope=world.scope,
                        allocation=allocation,
                        run=world.run_id,
                        tokens=128,
                    )
                    await db.execute(
                        text(
                            "UPDATE budget_allocation SET reserved=CAST(:held AS jsonb),"
                            "unknown_reserved=CAST(:unknown AS jsonb) WHERE id=:id"
                        ),
                        {
                            "id": allocation,
                            "held": json.dumps({"model_calls": 1, "tokens": 160}),
                            "unknown": json.dumps({"tokens": 32}),
                        },
                    )
                    await release_closing(db, world.schema, world.scope, world.run_id)
                    await release_closing(db, world.schema, world.scope, world.run_id)
                    row = (
                        (
                            await db.execute(
                                text(
                                    "SELECT reserved,unknown_reserved FROM budget_allocation "
                                    "WHERE id=:id"
                                ),
                                {"id": allocation},
                            )
                        )
                        .mappings()
                        .one()
                    )
                    assert row["reserved"] == {"model_calls": 0, "tokens": 32}
                    assert row["unknown_reserved"] == {"tokens": 32}
                    history = await closing_entries(
                        db, world.schema, world.scope, allocation, world.run_id
                    )
                    assert set(history) == {"reserve", "release"}
        finally:
            await engine.dispose()

    run(scenario)

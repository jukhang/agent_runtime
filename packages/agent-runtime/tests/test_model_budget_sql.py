"""Real SQL purpose reservation, no paid Provider calls or user-schema writes."""

import asyncio
import json
import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_storage_model_attempts import attempt_world as attempt_world
from test_storage_model_attempts import completed, provider_request
from test_storage_uow import actor, engine_for, lease, run

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.results import Usage
from agent_runtime.provider import AttemptTerminal
from agent_runtime.storage.model_attempts import SqlModelAttemptStore
from agent_runtime.storage.model_budget import closing_entries, release_closing, reserve_closing

pytestmark = pytest.mark.skipif(
    not os.getenv("AGENT_RUNTIME_TEST_DSN"), reason="requires PostgreSQL"
)


def test_purpose_floor_claim_races_usage_and_release(attempt_world):
    world = attempt_world

    async def scenario():
        engine = engine_for(world.schema)
        try:
            allocation = world.values["budget_id"]
            async with engine.begin() as db:
                await db.execute(
                    text("UPDATE budget_allocation SET limits=CAST(:limits AS jsonb) WHERE id=:id"),
                    {
                        "id": allocation,
                        "limits": json.dumps({"model_calls": 2, "tokens": 1000, "tool_calls": 24}),
                    },
                )
                await db.execute(
                    text("UPDATE context_snapshot SET content_hash=:hash WHERE id=:id"),
                    {"hash": "a" * 64, "id": world.values["snapshot_id"]},
                )
                await reserve_closing(
                    db,
                    world.schema,
                    scope=world.scope,
                    allocation=allocation,
                    run=world.run_id,
                    tokens=128,
                )

            def store():
                return SqlModelAttemptStore(
                    engine, world.schema, actor=actor(world=world), lease=lambda: lease(world)
                )

            ordinary = provider_request(world)
            with pytest.raises(AgentRuntimeError) as substituted:
                await store().begin(
                    ordinary.model_copy(update={"budget_allocation_id": uuid4()}),
                    started_at=datetime.now(UTC),
                )
            assert substituted.value.failure.code == "FORBIDDEN"
            assert await store().begin(ordinary, started_at=datetime.now(UTC)) == "started"
            await store().finish(ordinary, completed())
            with pytest.raises(AgentRuntimeError) as rejected:
                await store().begin(provider_request(world), started_at=datetime.now(UTC))
            assert rejected.value.failure.code == "BUDGET_EXHAUSTED"
            async with engine.begin() as db:
                closing_snapshot = uuid4()
                await db.execute(
                    text(
                        "INSERT INTO context_snapshot SELECT :new,scope_id,created_at,updated_at,"
                        "session_id,run_id,auth_revision,context_version+1,estimated_input_tokens,"
                        "schema_revision,'model.closing',payload_ref,content_hash,source_refs,degradation,"
                        "model_ref,prompt_ref,tool_refs,budget FROM context_snapshot WHERE id=:old"
                    ),
                    {"new": closing_snapshot, "old": world.values["snapshot_id"]},
                )
            closing = provider_request(world).model_copy(
                update={
                    "purpose": "closing",
                    "context_snapshot_id": closing_snapshot,
                }
            )
            other = closing.model_copy(update={"attempt_id": uuid4()})
            results = await asyncio.gather(
                store().begin(closing, started_at=datetime.now(UTC)),
                store().begin(other, started_at=datetime.now(UTC)),
                return_exceptions=True,
            )
            assert results.count("started") == 1
            winner = closing if results[0] == "started" else other
            assert await store().begin(winner, started_at=datetime.now(UTC)) == "already_running"
            async with engine.begin() as db:
                await release_closing(db, world.schema, world.scope, world.run_id)
                history = await closing_entries(
                    db, world.schema, world.scope, allocation, world.run_id
                )
                assert "adjust" in history and "release" not in history
            terminal = AttemptTerminal(
                status="completed",
                finish_reason="stop",
                failure=None,
                usage=Usage(
                    status="unknown",
                    reason="provider_not_reported",
                    input_tokens=None,
                    output_tokens=None,
                    total_tokens=None,
                ),
                finished_at=datetime.now(UTC),
            )
            await store().finish(winner, terminal)
            await store().finish(winner, terminal)
            async with engine.begin() as db:
                history = await closing_entries(
                    db, world.schema, world.scope, allocation, world.run_id
                )
                assert history["settle"]["quantities"] == {"model_calls": 1}
                assert (
                    await db.execute(
                        text("SELECT settled FROM budget_allocation WHERE id=:id"),
                        {"id": allocation},
                    )
                ).scalar_one() == {}
            with pytest.raises(AgentRuntimeError):
                await store().begin(provider_request(world), started_at=datetime.now(UTC))
        finally:
            await engine.dispose()

    run(scenario)

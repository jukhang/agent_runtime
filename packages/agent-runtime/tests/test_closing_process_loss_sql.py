"""Actual process loss after a committed closing claim; synthetic input, real SQL."""

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_storage_model_attempts import attempt_world as attempt_world
from test_storage_model_attempts import completed, provider_request
from test_storage_uow import actor, engine_for, lease, run

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.storage.model_attempts import SqlModelAttemptStore
from agent_runtime.storage.model_budget import closing_entries, release_closing, reserve_closing
from agent_runtime.storage.recovery import SqlRecovery

pytestmark = pytest.mark.skipif(
    not os.getenv("AGENT_RUNTIME_TEST_DSN"), reason="requires PostgreSQL"
)

CHILD = """
import asyncio, json, sys, time
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.storage.model_attempts import SqlModelAttemptStore
from test_storage_uow import engine_for
from datetime import UTC, datetime
async def main():
    data=json.loads(sys.stdin.readline())
    engine=engine_for(data['schema'])
    try:
        token=LeaseToken.model_validate(data['lease'])
        store=SqlModelAttemptStore(engine,data['schema'],
            actor=ActorContext.model_validate(data['actor']),lease=lambda:token)
        result=await store.begin(ProviderRequest.model_validate(data['request']),
            started_at=datetime.now(UTC))
        print(result,flush=True)
        await asyncio.sleep(300)
    finally:
        await engine.dispose()
asyncio.run(main())
"""

MAINTENANCE = """
import asyncio, json, sys
from datetime import UTC, datetime, timedelta
from test_storage_uow import engine_for
from agent_runtime.storage.recovery import SqlRecovery
async def main():
    data=json.loads(sys.stdin.read())
    engine=engine_for(data['schema'])
    try:
        recovery=SqlRecovery(engine,data['schema'],
            clock=lambda:datetime.now(UTC)+timedelta(hours=2))
        report=await recovery.recover_expired_leases(data['scope'])
        replay=await recovery.recover_expired_leases(data['scope'])
        print(json.dumps({'recovered':report.requeued+report.failed,
            'replayed':replay.requeued+replay.failed}),flush=True)
    finally:
        await engine.dispose()
asyncio.run(main())
"""


def test_process_loss_preserves_claim_and_fences_late_result(attempt_world):
    world = attempt_world
    request = provider_request(world).model_copy(update={"purpose": "closing"})

    async def prepare():
        engine = engine_for(world.schema)
        try:
            async with engine.begin() as db:
                await db.execute(
                    text("UPDATE budget_allocation SET limits=CAST(:limits AS jsonb) WHERE id=:id"),
                    {
                        "limits": json.dumps({"model_calls": 2, "tokens": 1000, "tool_calls": 24}),
                        "id": world.values["budget_id"],
                    },
                )
                await db.execute(
                    text(
                        "UPDATE context_snapshot SET task_type='model.closing',content_hash=:hash "
                        "WHERE id=:id"
                    ),
                    {"hash": request.input_digest, "id": request.context_snapshot_id},
                )
                await reserve_closing(
                    db,
                    world.schema,
                    scope=world.scope,
                    allocation=world.values["budget_id"],
                    run=world.run_id,
                    tokens=128,
                )
        finally:
            await engine.dispose()

    run(prepare)
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(Path(__file__).parent), env.get("PYTHONPATH", "")])
    child = subprocess.Popen(
        [sys.executable, "-c", CHILD],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    try:
        assert child.stdin is not None and child.stdout is not None
        child.stdin.write(
            json.dumps(
                {
                    "schema": world.schema,
                    "actor": actor(world=world).model_dump(mode="json"),
                    "lease": lease(world).model_dump(mode="json"),
                    "request": request.model_dump(mode="json"),
                }
            )
            + "\n"
        )
        child.stdin.flush()
        # communicate with a bounded deadline: no test may hang waiting for a failed child.
        import select

        ready, _, _ = select.select([child.stdout], [], [], 60)
        assert ready, "closing child did not commit within 60 seconds"
        assert child.stdout.readline().strip() == "started"
        child.kill()
        child.wait(timeout=10)
        assert child.returncode != 0
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=10)

    async def recover():
        engine = engine_for(world.schema)
        try:
            store = SqlModelAttemptStore(
                engine, world.schema, actor=actor(world=world), lease=lambda: lease(world)
            )
            assert await store.begin(request, started_at=datetime.now(UTC)) == "already_running"
            with pytest.raises(AgentRuntimeError):
                await store.begin(
                    request.model_copy(update={"attempt_id": uuid4()}),
                    started_at=datetime.now(UTC),
                )
            maintenance = subprocess.run(
                [sys.executable, "-c", MAINTENANCE],
                input=json.dumps({"schema": world.schema, "scope": world.scope}),
                capture_output=True,
                text=True,
                env=env,
                timeout=90,
                check=True,
            )
            assert json.loads(maintenance.stdout) == {"recovered": 1, "replayed": 0}
            await SqlRecovery(engine, world.schema).recover_expired_leases(world.scope)
            async with engine.begin() as db:
                row = (
                    (
                        await db.execute(
                            text("SELECT status,usage_status FROM model_attempt WHERE id=:id"),
                            {"id": request.attempt_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                assert dict(row) == {"status": "interrupted", "usage_status": "unknown"}
                await release_closing(db, world.schema, world.scope, world.run_id)
                history = await closing_entries(
                    db, world.schema, world.scope, world.values["budget_id"], world.run_id
                )
                assert set(history) == {"reserve", "adjust"}
                assert (
                    await db.execute(text("SELECT count(*) FROM model_attempt"))
                ).scalar_one() == 1
            with pytest.raises(AgentRuntimeError):
                await store.finish(request, completed())
        finally:
            await engine.dispose()

    run(recover)

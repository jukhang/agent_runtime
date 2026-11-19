"""Real savepoint/ledger/replay proof, isolated PostgreSQL; not live model evidence."""

import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from test_storage_uow import engine_for, run
from test_storage_uow import schema as schema  # noqa: F401
from test_storage_uow import world as world  # noqa: F401
from test_tool_timeout_boundary import invocation_pair

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.decisions import RecoveredToolFailure
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.tool_host import SqlToolHost
from agent_runtime.storage.tool_recovery_evidence import verify_tool_recoveries
from agent_runtime.storage.uow import SqlSystemCapacityStore
from agent_runtime.tool_effect import DatabaseToolRejected, ToolEffect

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENT_RUNTIME_TEST_DSN"), reason="requires guarded PostgreSQL"
)


@pytest.mark.parametrize("fault", ["domain", "connection", "savepoint_exit", "settlement"])
def test_domain_rollback_survives_in_operation_but_unknown_errors_do_not(world, fault):
    async def scenario():
        engine = engine_for(world.schema)
        spec, invocation = invocation_pair(effect="database", seconds=300)
        spec = spec.model_copy(update={"idempotency": "transactional"})
        invocation = invocation.model_copy(
            update={
                "actor": invocation.actor.model_copy(update={"scope_id": world.scope}),
                "run_id": world.run_id,
            }
        )
        host = SqlToolHost.__new__(SqlToolHost)
        host._schema = world.schema
        host._sessions = async_sessionmaker(engine, expire_on_commit=False)
        host._check = AsyncMock(return_value=str(world.task_id))
        capacity_store = SqlSystemCapacityStore(engine, world.schema)
        host._capacity = SqlToolCapacity(capacity_store, deployment_id="database-rejection")
        if fault == "settlement":
            host._capacity.settle_operation_in_transaction = AsyncMock(
                side_effect=ConnectionError("capacity settlement unavailable")
            )
        calls = []
        marker = uuid4()
        try:
            await capacity_store.seed(
                host._capacity.key(world.scope, spec), capacity=spec.max_concurrency
            )
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        "INSERT INTO tool_call "
                        "(id,scope_id,run_id,spec_revision,input_hash,status) "
                        "VALUES (:id,:scope,:run,'{}',:hash,'pending')"
                    ),
                    {
                        "id": invocation.call.tool_call_id,
                        "scope": world.scope,
                        "run": world.run_id,
                        "hash": "a" * 64,
                    },
                )

            async def invoke(db, current):
                calls.append(current.call.tool_call_id)
                # A real domain write, undone by the SDK savepoint.
                await db.execute(
                    sa.text(
                        "INSERT INTO context_blob (scope_id,blob_key,digest,content,size_bytes) "
                        "VALUES (:scope,:key,:digest,:content,:size)"
                    ),
                    {
                        "scope": world.scope,
                        "key": str(marker),
                        "digest": "b" * 64,
                        "content": b"domain-write-must-rollback",
                        "size": len(b"domain-write-must-rollback"),
                    },
                )
                if fault == "connection":
                    raise ConnectionError("unknown transport")
                if fault == "savepoint_exit":
                    # A typed rejection cannot salvage an aborted whole transaction.
                    await db.rollback()
                raise DatabaseToolRejected(
                    FailureEnvelope(
                        code="WORKFLOW_CONFLICT",
                        category="concurrency",
                        retryable=False,
                        public_message="State changed; read current facts.",
                    )
                )

            handler = SimpleNamespace(invoke=invoke)
            if fault != "domain":
                with pytest.raises(
                    ConnectionError
                    if fault in {"connection", "settlement"}
                    else InvalidRequestError
                ):
                    await host._database_effect(invocation, None, spec, (), handler)
            else:
                outcome = await host._database_effect(invocation, None, spec, (), handler)
                assert outcome.effect == "rolled_back"
                assert outcome.failure.operation_id == str(invocation.operation_id)
                replay = await host._database_effect(invocation, None, spec, (), handler)
                assert replay == outcome and len(calls) == 1
                success_call, success_operation = uuid4(), uuid4()
                success_invocation = invocation.model_copy(
                    update={
                        "call": invocation.call.model_copy(update={"tool_call_id": success_call}),
                        "operation_id": success_operation,
                    }
                )
                async with engine.begin() as db:
                    await db.execute(
                        sa.text(
                            "INSERT INTO tool_call "
                            "(id,scope_id,run_id,spec_revision,input_hash,status) "
                            "VALUES (:id,:scope,:run,'{}',:hash,'pending')"
                        ),
                        {
                            "id": success_call,
                            "scope": world.scope,
                            "run": world.run_id,
                            "hash": "a" * 64,
                        },
                    )
                host._schemas = SimpleNamespace(output=lambda *args: None)
                host._hooks = ()
                successful = await host._database_effect(
                    success_invocation,
                    None,
                    spec,
                    (),
                    SimpleNamespace(
                        invoke=AsyncMock(
                            return_value=ToolEffect(
                                data={"verifiedRollback": True}, summary="verified"
                            )
                        )
                    ),
                )
                payloads = BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema))
                for delta, result in enumerate((outcome, successful)):
                    payload = {"outcome": result.model_dump(mode="json", by_alias=True)}
                    ref = await payloads.put(world.scope, payload, content_digest(payload))
                    async with engine.begin() as db:
                        await db.execute(
                            sa.text(
                                "UPDATE tool_call SET status=:status,"
                                "result_ref=:ref,finished_at=:at "
                                "WHERE scope_id=:scope AND id=:id"
                            ),
                            {
                                "status": result.status,
                                "ref": ref,
                                "at": datetime.now(UTC) + timedelta(seconds=delta),
                                "scope": world.scope,
                                "id": result.tool_call_id,
                            },
                        )
                async with AsyncSession(engine) as db, db.begin():
                    assert await verify_tool_recoveries(
                        db,
                        world.schema,
                        scope_id=world.scope,
                        run_id=world.run_id,
                        failures={outcome.tool_call_id},
                        assessments=(
                            RecoveredToolFailure(
                                failed_tool_call_id=outcome.tool_call_id,
                                recovery_tool_call_ids=(successful.tool_call_id,),
                                reason="Re-read and verified current facts after rollback.",
                            ),
                        ),
                    )
            async with AsyncSession(engine) as db:
                assert (
                    await db.scalar(
                        sa.text(
                            "SELECT count(*) FROM context_blob "
                            "WHERE scope_id=:scope AND blob_key=:key"
                        ),
                        {"scope": world.scope, "key": str(marker)},
                    )
                    == 0
                )
                operation = (
                    (
                        await db.execute(
                            sa.text(
                                "SELECT status,result_ref FROM operation "
                                "WHERE scope_id=:scope AND id=:id"
                            ),
                            {"scope": world.scope, "id": invocation.operation_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if fault == "domain":
                    assert operation["status"] == "failed_safe"
                    assert '"rolled_back"' in operation["result_ref"]
                else:
                    assert operation is None
                assert (
                    await db.scalar(
                        sa.text(
                            "SELECT max(reserved) FROM system_capacity "
                            "WHERE deployment_id='database-rejection'"
                        )
                    )
                    == 0
                )
        finally:
            await engine.dispose()

    run(scenario)

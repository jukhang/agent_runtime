"""Opt-in real PostgreSQL evidence checks; never use application schemas."""

import json
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from test_storage_uow import engine_for, run
from test_storage_uow import schema as schema  # noqa: F401
from test_storage_uow import world as world  # noqa: F401

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.decisions import RecoveredToolFailure
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.tool_recovery_evidence import verify_tool_recoveries

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENT_RUNTIME_TEST_DSN"), reason="requires guarded real PostgreSQL"
)


@pytest.mark.parametrize("coverage", ("missing", "subset", "complete", "partial"))
def test_finish_preflight_requires_complete_recovery_evidence(world, coverage):
    from test_decision_validation import facts
    from test_storage_uow import actor, lease

    from agent_runtime.agent_loop import DecisionFrame
    from agent_runtime.candidate_repair import CandidateRejected
    from agent_runtime.contracts.context import ContextSourceRecord
    from agent_runtime.contracts.decisions import Finish, FinishPayload
    from agent_runtime.contracts.references import ResourceRef, ResourceScope
    from agent_runtime.storage.candidate_repair import SqlCandidateRepairs

    async def scenario():
        pairs = [await seed_recovery_calls(world), await seed_recovery_calls(world, write=True)]
        engine = engine_for(world.schema)
        try:
            current_actor = actor(world=world)
            state, context, _ = facts()
            state = state.model_copy(
                update={
                    "scope_id": world.scope,
                    "session_id": world.session_id,
                    "run_id": world.run_id,
                }
            )
            sources = tuple(
                ContextSourceRecord(
                    source_id=str(identity),
                    resource=ResourceRef(
                        resource_type="tool_call_result",
                        resource_id=str(identity),
                        resource_version="1",
                        scope=ResourceScope(scope_id=world.scope, session_id=str(world.session_id)),
                    ),
                    required=True,
                    disposition="included",
                    reason=None,
                    summary_ref=None,
                    summary_status=None,
                )
                for pair in pairs
                for identity in pair
            )
            context = context.model_copy(
                update={
                    "scope_id": world.scope,
                    "session_id": world.session_id,
                    "run_id": world.run_id,
                    "sources": sources,
                }
            )
            entries = tuple(
                RecoveredToolFailure(
                    failed_tool_call_id=failed,
                    recovery_tool_call_ids=(success,),
                    reason="The corrected operation saved and verified the requested result.",
                )
                for failed, success in pairs
            )
            payload = FinishPayload(
                outcome="partial" if coverage == "partial" else "success",
                evidence_refs=(),
                final_item_ids=(),
                recovered_tool_failures=entries
                if coverage == "complete"
                else (entries[:1] if coverage == "subset" else ()),
            )
            frame = DecisionFrame(
                actor=current_actor,
                context=context,
                attempt_id=uuid4(),
                action=Finish(kind="finish", payload=payload),
            )
            repairs = SqlCandidateRepairs(
                engine, world.schema, actor=current_actor, lease=lambda: lease(world)
            )
            if coverage in {"missing", "subset"}:
                with pytest.raises(CandidateRejected) as rejected:
                    await repairs.validate(state, frame)
                assert rejected.value.reason == "invalid_evidence"
                assert all(str(pair[0]) in rejected.value.detail for pair in pairs)
                assert "FULL recoveredToolFailures" in rejected.value.detail
            else:
                await repairs.validate(state, frame)
            async with engine.connect() as db:
                unchanged = await db.scalar(
                    sa.text("SELECT count(*) FROM tool_call WHERE status='failed'")
                )
                assert unchanged == 2
        finally:
            await engine.dispose()

    run(scenario)


async def seed_recovery_calls(world, *, write=False, unexecuted=False):
    engine = engine_for(world.schema)
    payloads = BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema))
    failed, success = uuid4(), uuid4()
    now = datetime.now(UTC)
    try:
        for identity, status, delta in ((failed, "failed", 0), (success, "completed", 1)):
            intent = uuid4() if unexecuted and status == "failed" else None
            if intent:
                status = "not_executed"
            operation = uuid4() if write else None
            outcome = {
                "toolCallId": str(identity),
                "runId": str(world.run_id),
                "operationId": str(intent or operation) if intent or operation else None,
                "status": status,
                "effectRefs": [],
                "effect": ("committed" if status == "completed" else "rolled_back")
                if write
                else "none",
            }
            if status in {"failed", "not_executed"}:
                outcome["failure"] = {
                    "code": "INVALID_INPUT",
                    "category": "tool",
                    "retryable": False,
                    "publicMessage": "Incorrect original argument.",
                    "operationId": str(intent) if intent else None,
                }
            else:
                outcome["result"] = {"policy": "full", "data": {"savedAndVerified": True}}
            payload = {"outcome": outcome}
            ref = await payloads.put(world.scope, payload, content_digest(payload))
            async with engine.begin() as db:
                if operation:
                    await db.execute(
                        sa.text(
                            "INSERT INTO operation "
                            "(id,scope_id,task_id,idempotency_key,input_hash,status,result_ref) "
                            "VALUES (:id,:scope,:task,:key,:hash,:status,:result)"
                        ),
                        {
                            "id": operation,
                            "scope": world.scope,
                            "task": world.task_id,
                            "key": str(identity),
                            "hash": "a" * 64,
                            "status": "committed" if status == "completed" else "failed_safe",
                            "result": json.dumps(outcome) if status == "completed" else None,
                        },
                    )
                await db.execute(
                    sa.text(
                        "INSERT INTO tool_call (id,scope_id,run_id,spec_revision,input_hash,status,"
                        "result_ref,operation_id,finished_at,failure) VALUES "
                        "(:id,:scope,:run,'{}',:hash,:status,:ref,:operation,:finished,"
                        "CAST(:failure AS jsonb))"
                    ),
                    {
                        "id": identity,
                        "scope": world.scope,
                        "run": world.run_id,
                        "hash": "a" * 64,
                        "status": status,
                        "ref": ref,
                        "operation": operation,
                        "finished": now + timedelta(microseconds=delta),
                        "failure": json.dumps(outcome["failure"])
                        if status != "completed"
                        else None,
                    },
                )
        return failed, success
    finally:
        await engine.dispose()


def test_real_unexecuted_intent_is_not_a_missing_operation(world):
    async def scenario():
        failed, success = await seed_recovery_calls(world, unexecuted=True)
        engine = engine_for(world.schema)
        try:
            async with AsyncSession(engine) as db, db.begin():
                assert await verify_tool_recoveries(
                    db,
                    world.schema,
                    scope_id=world.scope,
                    run_id=world.run_id,
                    failures={failed},
                    assessments=(
                        RecoveredToolFailure(
                            failed_tool_call_id=failed,
                            recovery_tool_call_ids=(success,),
                            reason="Corrected invalid parameters and verified saved result.",
                        ),
                    ),
                )
                row = (
                    await db.execute(
                        sa.text("SELECT status,operation_id FROM tool_call WHERE id=:id"),
                        {"id": failed},
                    )
                ).one()
                assert row.status == "not_executed" and row.operation_id is None
        finally:
            await engine.dispose()

    run(scenario)


@pytest.mark.parametrize(
    "write,fault",
    [
        (write, fault)
        for write in (False, True)
        for fault in (
            None,
            "old",
            "foreign",
            "foreign_scope",
            "unknown_operation",
            "missing_outcome",
        )
        if write or fault != "unknown_operation"
    ],
)
def test_recovery_uses_real_scoped_ledger_and_preserves_failure(world, fault, write):
    assert sa.make_url(os.environ["AGENT_RUNTIME_TEST_DSN"]).database == "aletai_wangxh"

    async def scenario():
        failed, success = await seed_recovery_calls(world, write=write)
        engine = engine_for(world.schema)
        try:
            async with AsyncSession(engine) as db, db.begin():
                if fault == "old":
                    await db.execute(
                        sa.text(
                            "UPDATE tool_call SET finished_at=finished_at-interval '10 seconds' "
                            "WHERE id=:id"
                        ),
                        {"id": success},
                    )
                elif fault == "foreign":
                    await db.execute(
                        sa.text("UPDATE tool_call SET run_id=:run WHERE id=:id"),
                        {"run": world.sender_run_id, "id": success},
                    )
                elif fault == "unknown_operation":
                    await db.execute(
                        sa.text(
                            "UPDATE operation SET status='unknown' WHERE id="
                            "(SELECT operation_id FROM tool_call WHERE id=:id)"
                        ),
                        {"id": success},
                    )
                elif fault == "missing_outcome":
                    await db.execute(
                        sa.text("UPDATE tool_call SET result_ref=NULL WHERE id=:id"), {"id": failed}
                    )
                entries = (
                    RecoveredToolFailure(
                        failed_tool_call_id=failed,
                        recovery_tool_call_ids=(success,),
                        reason="Corrected and verified the result.",
                    ),
                )
                if fault:
                    with pytest.raises(AgentRuntimeError):
                        await verify_tool_recoveries(
                            db,
                            world.schema,
                            scope_id=world.scope + "-other"
                            if fault == "foreign_scope"
                            else world.scope,
                            run_id=world.run_id,
                            failures={failed},
                            assessments=entries,
                        )
                else:
                    assert await verify_tool_recoveries(
                        db,
                        world.schema,
                        scope_id=world.scope,
                        run_id=world.run_id,
                        failures={failed},
                        assessments=entries,
                    )
                assert (
                    await db.execute(
                        sa.text("SELECT status FROM tool_call WHERE id=:id"), {"id": failed}
                    )
                ).scalar_one() == "failed"
        finally:
            await engine.dispose()

    run(scenario)

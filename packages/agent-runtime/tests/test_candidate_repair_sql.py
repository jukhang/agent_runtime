"""Controlled SQL audit fixtures, not Provider or browser acceptance."""

import json
import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_decision_validation import facts
from test_storage_uow import _seed_attempt, actor, engine_for, lease, run
from test_storage_uow import no_leftover_schemas as no_leftover_schemas
from test_storage_uow import schema as schema
from test_storage_uow import world as world

from agent_runtime.candidate_repair import CandidateRejected
from agent_runtime.context_payloads import context_blob_key
from agent_runtime.contracts.canonical import canonical_bytes, content_digest
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.storage.candidate_repair import SqlCandidateRepairs
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.model_attempts import claim_attempt_suffix

pytestmark = pytest.mark.skipif(
    not os.getenv("AGENT_RUNTIME_TEST_DSN"), reason="requires PostgreSQL"
)


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "missing_blob",
        "corrupt_blob",
        "pause",
        "cancel",
        "new_input",
        "lost_lease",
        "unknown_usage",
        "rollback",
        "takeover",
        "old_checkpoint",
    ],
)
def test_checkpoint_counters_are_atomic_idempotent_and_survive_new_store_instances(
    world, fault, monkeypatch
):
    async def scenario():
        engine = engine_for(world.schema)
        try:
            initial = {"kind": "initial-fixture", "runId": str(world.run_id)}
            digest = content_digest(initial)
            key = context_blob_key(world.scope, digest)
            await SqlContextBlobBackend(engine, world.schema).put_immutable(
                world.scope, key, canonical_bytes(initial)
            )
            async with engine.begin() as db:
                await db.execute(
                    text("UPDATE checkpoint SET state_ref=:ref,content_hash=:hash WHERE id=:id"),
                    {"ref": key, "hash": digest, "id": world.checkpoint_id},
                )
            state, _, _ = facts()
            state = state.model_copy(
                update={
                    "scope_id": world.scope,
                    "session_id": world.session_id,
                    "run_id": world.run_id,
                    "checkpoint_ref": key,
                    "state_version": "1",
                }
            )

            async def rejected(detail):
                attempt = await _seed_attempt(world)
                payload = {"fixture": "explicit synthetic model input", "attemptId": str(attempt)}
                payload_hash = content_digest(payload)
                payload_ref = context_blob_key(world.scope, payload_hash)
                await SqlContextBlobBackend(engine, world.schema).put_immutable(
                    world.scope, payload_ref, canonical_bytes(payload)
                )
                async with engine.begin() as db:
                    snapshot = (
                        await db.execute(
                            text(
                                "UPDATE model_attempt SET request_key=:key,started_at=:now, "
                                "input_ref=:ref,usage=CAST(:usage AS jsonb) "
                                "WHERE id=:id RETURNING context_snapshot_id"
                            ),
                            {
                                "id": attempt,
                                "key": str(attempt) + claim_attempt_suffix(lease(world)),
                                "now": datetime.now(UTC),
                                "ref": payload_ref,
                                "usage": json.dumps(
                                    {
                                        "status": "complete",
                                        "reason": "provider_reported",
                                        "inputTokens": "0",
                                        "outputTokens": "0",
                                        "totalTokens": "0",
                                    }
                                ),
                            },
                        )
                    ).scalar_one()
                    await db.execute(
                        text(
                            "UPDATE context_snapshot SET payload_ref=:ref,content_hash=:hash "
                            "WHERE id=:id"
                        ),
                        {"ref": payload_ref, "hash": payload_hash, "id": snapshot},
                    )
                return CandidateRejected(
                    attempt_id=attempt, snapshot_id=snapshot, reason="unknown_tool", detail=detail
                )

            def store():
                return SqlCandidateRepairs(
                    engine, world.schema, actor=actor(world=world), lease=lambda: lease(world)
                )

            first = await rejected("bad-name")
            state = await store().record(state, first)
            saved = state.checkpoint_ref
            assert (await store().record(state, first)).checkpoint_ref == saved
            assert (await store().latest(state)).attempt_id == first.attempt_id
            async with engine.connect() as db:
                raw = (
                    await db.execute(
                        text("SELECT content FROM context_blob WHERE blob_key=:ref"), {"ref": saved}
                    )
                ).scalar_one()
                observation = json.loads(raw)["candidateRepair"]["observation"]
                assert observation["attemptId"] == str(first.attempt_id)
                assert observation["total"] == 1 and observation["effect"] == "none"
            if fault == "takeover":
                old_token = lease(world)
                async with engine.begin() as db:
                    await db.execute(
                        text(
                            "UPDATE run SET owner_id='worker-2',owner_epoch=owner_epoch+1 "
                            "WHERE id=:id"
                        ),
                        {"id": world.run_id},
                    )
                    await db.execute(
                        text(
                            "UPDATE work_item SET owner_id='worker-2',lease_epoch=lease_epoch+1 "
                            "WHERE id=:id"
                        ),
                        {"id": world.work_id},
                    )
                    await db.execute(
                        text(
                            "UPDATE session_slot SET slot_version=slot_version+1 "
                            "WHERE scope_id=:scope AND session_id=:id"
                        ),
                        {"scope": world.scope, "id": world.session_id},
                    )
                world.owner, world.owner_epoch, world.work_epoch, world.slot_version = (
                    "worker-2",
                    2,
                    2,
                    2,
                )
                assert (await store().latest(state)).attempt_id == first.attempt_id
                old_store = SqlCandidateRepairs(
                    engine, world.schema, actor=actor(world=world), lease=lambda: old_token
                )
                with pytest.raises(AgentRuntimeError) as stale:
                    await old_store.record(state, first)
                assert stale.value.failure.code == "FENCED"
            if fault == "old_checkpoint":
                legacy = json.loads(raw)
                legacy["candidateRepair"].pop("observation")
                legacy_hash = content_digest(legacy)
                legacy_ref = context_blob_key(world.scope, legacy_hash)
                await SqlContextBlobBackend(engine, world.schema).put_immutable(
                    world.scope, legacy_ref, canonical_bytes(legacy)
                )
                async with engine.begin() as db:
                    old_id = (
                        await db.execute(
                            text("SELECT checkpoint_id FROM run WHERE id=:id"), {"id": world.run_id}
                        )
                    ).scalar_one()
                    new_id = uuid4()
                    await db.execute(
                        text(
                            "INSERT INTO checkpoint(id,scope_id,run_id,source_checkpoint_id,"
                            "state_version,representation_revision,event_seq,schema_revision,"
                            "runtime_build,state_ref,content_hash) SELECT :id,scope_id,run_id,id,"
                            "state_version,representation_revision+1,event_seq,"
                            "schema_revision,runtime_build,:ref,:hash FROM checkpoint WHERE id=:old"
                        ),
                        {"id": new_id, "ref": legacy_ref, "hash": legacy_hash, "old": old_id},
                    )
                    await db.execute(
                        text("UPDATE run SET checkpoint_id=:new WHERE id=:id"),
                        {"new": new_id, "id": world.run_id},
                    )
                state = state.model_copy(update={"checkpoint_ref": legacy_ref})
                assert (await store().latest(state)).attempt_id == first.attempt_id
            if fault and fault not in {"takeover", "old_checkpoint"}:
                second = await rejected("second-defect")
                if fault == "rollback":
                    original_put = SqlContextBlobBackend.put_in_transaction

                    async def interrupted(self, *args, **kwargs):
                        await original_put(self, *args, **kwargs)
                        raise RuntimeError("injected after observation Blob")

                    with monkeypatch.context() as patch:
                        patch.setattr(SqlContextBlobBackend, "put_in_transaction", interrupted)
                        with pytest.raises(RuntimeError, match="injected"):
                            await store().record(state, second)
                    assert (
                        await store().latest(state) is None
                    )  # A later model exists; stale feedback is not replayed.
                    async with engine.connect() as db:
                        pointer = (
                            await db.execute(
                                text(
                                    "SELECT c.state_ref FROM run r JOIN checkpoint c "
                                    "ON c.id=r.checkpoint_id WHERE r.id=:id"
                                ),
                                {"id": world.run_id},
                            )
                        ).scalar_one()
                        assert pointer == saved
                    state = await store().record(state, second)
                    assert (await store().latest(state)).attempt_id == second.attempt_id
                    async with engine.connect() as db:
                        assert (
                            await db.execute(
                                text("SELECT count(*) FROM checkpoint WHERE run_id=:run"),
                                {"run": world.run_id},
                            )
                        ).scalar_one() == 3
                    return
                async with engine.begin() as db:
                    if fault == "missing_blob":
                        await db.execute(
                            text("DELETE FROM context_blob WHERE blob_key=:ref"), {"ref": saved}
                        )
                    elif fault == "corrupt_blob":
                        await db.execute(
                            text(
                                "UPDATE context_blob SET content=:raw,size_bytes=:size "
                                "WHERE blob_key=:ref"
                            ),
                            {
                                "ref": saved,
                                "raw": b'{"corrupt":true}',
                                "size": len(b'{"corrupt":true}'),
                            },
                        )
                    elif fault in {"pause", "cancel"}:
                        from agent_runtime.contracts.requests import ControlRequest
                        from agent_runtime.storage.uow import SqlStateUnitOfWork

                        await SqlStateUnitOfWork(
                            engine, world.schema, actor(world=world)
                        ).record_control(
                            ControlRequest(
                                run_id=world.run_id,
                                kind=fault,
                                idempotency_key=f"candidate-control-{fault}",
                                expected_version="1",
                            )
                        )
                    elif fault == "new_input":
                        await db.execute(
                            text("UPDATE run SET pending_input_version=1 WHERE id=:id"),
                            {"id": world.run_id},
                        )
                    elif fault == "lost_lease":
                        await db.execute(
                            text("UPDATE run SET owner_epoch=owner_epoch+1 WHERE id=:id"),
                            {"id": world.run_id},
                        )
                    else:
                        await db.execute(
                            text(
                                "UPDATE model_attempt SET usage_status='unknown',"
                                "usage_reason='provider_not_reported',"
                                "usage=NULL WHERE id=:id"
                            ),
                            {"id": second.attempt_id},
                        )
                with pytest.raises(AgentRuntimeError) as error:
                    await store().record(state, second)
                expected = {
                    "missing_blob": "AUDIT_UNAVAILABLE",
                    "corrupt_blob": "AUDIT_UNAVAILABLE",
                    "pause": "CONTROL_PENDING",
                    "cancel": "CONTROL_PENDING",
                    "new_input": "VERSION_CONFLICT",
                    "lost_lease": "FENCED",
                    "unknown_usage": "FORBIDDEN",
                }
                assert error.value.failure.code == expected[fault]
                async with engine.connect() as db:
                    assert (
                        await db.execute(
                            text("SELECT count(*) FROM checkpoint WHERE run_id=:run"),
                            {"run": world.run_id},
                        )
                    ).scalar_one() == 2
                return
            state = await store().record(state, await rejected("bad-name"))
            with pytest.raises(AgentRuntimeError, match="有限次数"):
                await store().record(state, await rejected("bad-name"))
            for detail in ("another-name", "last-name"):
                state = await store().record(state, await rejected(detail))
            with pytest.raises(AgentRuntimeError, match="有限次数"):
                await store().record(state, await rejected("over-total"))
            async with engine.connect() as db:
                count = (
                    await db.execute(
                        text("SELECT count(*) FROM checkpoint WHERE run_id=:run"),
                        {"run": world.run_id},
                    )
                ).scalar_one()
            assert count == 5 + int(fault == "old_checkpoint")
        finally:
            await engine.dispose()

    run(scenario)

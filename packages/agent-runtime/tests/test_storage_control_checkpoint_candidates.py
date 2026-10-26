"""Real SQL recovery after an immutable, unselected Finish checkpoint exists."""

import os
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from test_decision_validation import facts
from test_storage_uow import _seed, _seed_attempt, actor, engine_for, guard, lease, migrate, run

from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.decisions import decision_adapter
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.scheduler import ClaimedWork
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.decisions import SqlDecisionStore
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.recovery import SqlRecovery
from agent_runtime.storage.uow import SqlStateUnitOfWork, locks
from agent_runtime.storage.uow import control_checkpoints as checkpoints

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENT_RUNTIME_TEST_DSN"), reason="requires guarded real PostgreSQL"
)


@pytest.fixture(scope="module")
def candidate_schema():
    assert sa.make_url(os.environ["AGENT_RUNTIME_TEST_DSN"]).database == "aletai_wangxh"
    name = f"r05_cp_candidates_{uuid4().hex[:12]}"
    try:
        assert migrate("upgrade", name).status == "applied"
        yield name
    finally:

        async def drop():
            engine = engine_for(name, search_path=False)
            try:
                async with engine.begin() as db:
                    await db.execute(sa.text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))
            finally:
                await engine.dispose()

        run(drop)


def test_finish_forbidden_candidate_cannot_block_failure(candidate_schema, monkeypatch):
    world = run(lambda: _seed(candidate_schema))

    async def scenario():
        engine = engine_for(world.schema, search_path=False)
        statements = locks.build_lock_statements(world.schema)

        def sql(statement):
            return sa.text(statement.replace("{t}", f'"{world.schema}".'))

        async def read(statement, **params):
            async with engine.connect() as db:
                return [dict(row) for row in (await db.execute(sql(statement), params)).mappings()]

        async def inventory():
            return {
                table: await read(
                    f"SELECT * FROM {{t}}{table} ORDER BY "
                    + ("blob_key" if table == "context_blob" else "id")
                )
                for table in ("checkpoint", "context_blob", "run", "work_item", "event", "outbox")
            }

        try:
            attempt = await _seed_attempt(world)
            snapshot = (
                await read(
                    "SELECT context_snapshot_id FROM {t}model_attempt WHERE id=:id", id=attempt
                )
            )[0]["context_snapshot_id"]
            initial = (
                await read(
                    "SELECT state_ref FROM {t}checkpoint WHERE id=:id", id=world.checkpoint_id
                )
            )[0]["state_ref"]
            state, context, _ = facts()
            state = state.model_copy(
                update={
                    "scope_id": world.scope,
                    "session_id": world.session_id,
                    "run_id": world.run_id,
                    "state_version": "1",
                    "checkpoint_ref": initial,
                }
            )
            context = context.model_copy(
                update={
                    "scope_id": world.scope,
                    "session_id": world.session_id,
                    "run_id": world.run_id,
                    "snapshot_id": snapshot,
                }
            )
            evidence = ResourceRef(
                resource_type="timeline_item",
                resource_id=str(uuid4()),
                resource_version="1",
                scope=ResourceScope(scope_id="wrong-scope"),
            )
            action = decision_adapter.validate_python(
                {
                    "kind": "finish",
                    "payload": {
                        "outcome": "partial",
                        "finalItemIds": [str(uuid4())],
                        "evidenceRefs": [evidence.model_dump(mode="json", by_alias=True)],
                    },
                }
            )
            frame = DecisionFrame(
                actor=actor(world=world), context=context, attempt_id=attempt, action=action
            )

            class Context:
                async def load(self, scope_id, identity):
                    assert scope_id == world.scope and identity == snapshot
                    return context, {}

            class Access:
                async def verify(self, current_actor, prepared):
                    assert current_actor == frame.actor and prepared == context

                async def authorize_session_in_transaction(self, db, current_actor, session_id):
                    assert current_actor == frame.actor and session_id == world.session_id

                async def authorize_resource_in_transaction(self, *args, **kwargs):
                    raise AssertionError("cross-scope evidence must fail before resource access")

            journal = SqlJournal(engine, world.schema, frame.actor)
            decisions = SqlDecisionStore(
                engine,
                world.schema,
                actor=frame.actor,
                lease=lease(world),
                payloads=BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema)),
                context=Context(),
                access=Access(),
                journal=journal,
            )
            state = await decisions.persist(state, frame)
            candidate = await decisions.prepare_next_checkpoint(state, frame)
            before = await inventory()
            before_run = (await read("SELECT * FROM {t}run WHERE id=:id", id=world.run_id))[0]
            candidate_row = next(
                row for row in before["checkpoint"] if row["id"] == candidate.checkpoint_id
            )
            assert (candidate_row["state_version"], candidate_row["representation_revision"]) == (
                2,
                0,
            )
            assert before_run["checkpoint_id"] != candidate.checkpoint_id
            with pytest.raises(AgentRuntimeError) as rejected:
                await SqlStateUnitOfWork(engine, world.schema, frame.actor).commit_answer(
                    guard=guard(world),
                    checkpoint_id=candidate.checkpoint_id,
                    item_ids=action.payload.final_item_ids,
                    outcome="partial",
                    evidence_refs=(evidence,),
                    journal=journal,
                    resources=Access(),
                )
            assert rejected.value.failure.code == "FORBIDDEN"
            assert await inventory() == before
            claimed = ClaimedWork(
                disposition="claimed",
                lease=lease(world),
                state_version="1",
                checkpoint_id=before_run["checkpoint_id"],
                budget_allocation_id=uuid4(),
            )
            recovery = SqlRecovery(engine, world.schema)

            original_select = checkpoints._select_checkpoint

            async def move_before_cas(db, schema, current, checkpoint_id):
                await db.execute(
                    sql("UPDATE {t}run SET checkpoint_id=:cp WHERE id=:id"),
                    {"cp": candidate.checkpoint_id, "id": world.run_id},
                )
                await original_select(db, schema, current, checkpoint_id)

            with monkeypatch.context() as patch:
                patch.setattr(checkpoints, "_select_checkpoint", move_before_cas)
                with pytest.raises(AgentRuntimeError) as stale:
                    await recovery.handle_worker_failure(claimed, rejected.value)
                assert stale.value.failure.code == "VERSION_CONFLICT"
            assert await inventory() == before

            original_append = SqlJournal.append_state_in_transaction

            async def fail_after_terminal(self, db, batch):
                result = await original_append(self, db, batch)
                if any(event.kind == "run.failed" for event in batch.events):
                    raise RuntimeError("injected terminal rollback")
                return result

            with monkeypatch.context() as patch:
                patch.setattr(SqlJournal, "append_state_in_transaction", fail_after_terminal)
                with pytest.raises(RuntimeError, match="terminal rollback"):
                    await recovery.handle_worker_failure(claimed, rejected.value)
            assert await inventory() == before

            # Leave an identical, unselected control representation to exercise reuse.
            async with AsyncSession(engine) as db, db.begin():
                await locks.lock_slot(db, statements, world.scope, world.session_id)
                current = dict(await locks.lock_run(db, statements, world.scope, world.run_id))
                prepared = await checkpoints.transition_checkpoint(
                    db,
                    world.schema,
                    current,
                    current["checkpoint_id"],
                    kind="failure",
                    status="failed",
                    target_version=2,
                )
                assert prepared != candidate.checkpoint_id
                await db.execute(
                    sql(
                        "UPDATE {t}run SET checkpoint_id=:old WHERE id=:id "
                        "AND checkpoint_id=:new AND state_version=1"
                    ),
                    {"old": current["checkpoint_id"], "new": prepared, "id": world.run_id},
                )
            prepared_row = (await read("SELECT * FROM {t}checkpoint WHERE id=:id", id=prepared))[0]
            assert prepared_row["representation_revision"] > max(
                row["representation_revision"]
                for row in before["checkpoint"]
                if row["state_version"] == 2
            )
            assert (
                await recovery.handle_worker_failure(claimed, rejected.value)
            ).disposition == "failed"
            assert (
                await recovery.handle_worker_failure(claimed, rejected.value)
            ).disposition == "fenced"
            final = (await read("SELECT * FROM {t}run WHERE id=:id", id=world.run_id))[0]
            assert (final["status"], final["state_version"], final["checkpoint_id"]) == (
                "failed",
                2,
                prepared,
            )
            for original in before["checkpoint"]:
                assert (
                    await read("SELECT * FROM {t}checkpoint WHERE id=:id", id=original["id"])
                ) == [original]
            assert (await read("SELECT * FROM {t}checkpoint WHERE id=:id", id=prepared)) == [
                prepared_row
            ]
            assert len(await read("SELECT id FROM {t}event WHERE kind='run.failed'")) == 1
            assert not await read("SELECT id FROM {t}event WHERE kind='run.completed'")
            assert len(await read("SELECT id FROM {t}event")) == len(
                await read("SELECT id FROM {t}outbox")
            )

            after = await inventory()
            async with AsyncSession(engine) as db, db.begin():
                await locks.lock_slot(db, statements, world.scope, world.session_id)
                current = dict(await locks.lock_run(db, statements, world.scope, world.run_id))
                assert (
                    await checkpoints.transition_checkpoint(
                        db,
                        world.schema,
                        current,
                        prepared,
                        kind="failure",
                        status="failed",
                        target_version=2,
                    )
                    == prepared
                )
                with pytest.raises(AgentRuntimeError):
                    await checkpoints.transition_checkpoint(
                        db,
                        world.schema,
                        before_run,
                        current["checkpoint_id"],
                        kind="failure",
                        status="failed",
                        target_version=2,
                    )
            assert await inventory() == after
        finally:
            await engine.dispose()

    run(scenario)

"""Failure/outbox rollback and retained output on real isolated PostgreSQL schemas."""

import os
import uuid

import pytest

pytest.importorskip("sqlalchemy", reason="requires agent-runtime[storage]")

import sqlalchemy as sa
from test_storage_uow import (  # noqa: F401
    _output_batch,
    _row,
    _seed_attempt,
    _seed_execution,
    _seed_item,
    _seed_work,
    actor,
    count,
    engine_for,
    lease,
    run,
    schema,
    world,
)

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.journal import SealOutputRequest
from agent_runtime.contracts.requests import ControlRequest, StreamRequest
from agent_runtime.contracts.scheduler import ClaimedWork
from agent_runtime.contracts.work_claims import ControlClaimedWork
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.journal import SqlJournal, SqlJournalReader
from agent_runtime.storage.main_control import SqlMainControlHandler
from agent_runtime.storage.recovery import SqlRecovery
from agent_runtime.storage.uow import SqlStateUnitOfWork
from agent_runtime.worker import LeaseHandle

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENT_RUNTIME_TEST_DSN"), reason="AGENT_RUNTIME_TEST_DSN is not configured"
)


@pytest.mark.parametrize("content", ("retained answer", "", "deleted"))
def test_root_failure_projection_rolls_back_and_replays_once(world, monkeypatch, content):  # noqa: F811
    async def scenario():
        engine = engine_for(world.schema, search_path=False)
        journal = SqlJournal(engine, world.schema, actor(world=world))
        recovery = SqlRecovery(engine, world.schema)
        try:
            item = await _seed_item(world)
            attempt = await _seed_attempt(world)
            await journal.append(
                _output_batch(world, attempt_id=attempt, item_id=item, content=content)
            )
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f"UPDATE {world.schema}.timeline_item SET status='streaming' WHERE id=:id"
                    ),
                    {"id": item},
                )
                await db.execute(
                    sa.text(f"UPDATE {world.schema}.turn SET primary_run_id=:id"),
                    {"id": world.run_id},
                )
                if content == "deleted":
                    await db.execute(
                        sa.text(f"UPDATE {world.schema}.session SET deleted_at=CURRENT_TIMESTAMP")
                    )
            claimed = ClaimedWork(
                disposition="claimed",
                lease=lease(world),
                state_version="1",
                checkpoint_id=world.checkpoint_id,
                budget_allocation_id=uuid.uuid4(),
            )
            error = AgentRuntimeError(
                FailureEnvelope(
                    code="INVALID_INPUT",
                    category="validation",
                    retryable=False,
                    public_message="缺少必要的任务参数。",
                )
            )
            original = SqlJournal.append_state_in_transaction

            async def fail_after_terminal(self, db, batch):
                result = await original(self, db, batch)
                if any(e.kind == "run.failed" for e in batch.events):
                    raise RuntimeError("injected terminal rollback")
                return result

            with monkeypatch.context() as patch:
                patch.setattr(SqlJournal, "append_state_in_transaction", fail_after_terminal)
                with pytest.raises(RuntimeError, match="terminal rollback"):
                    await recovery.handle_worker_failure(claimed, error)
            async with engine.connect() as db:
                assert (
                    await db.execute(
                        sa.text(f"SELECT status FROM {world.schema}.run WHERE id=:id"),
                        {"id": world.run_id},
                    )
                ).scalar_one() == "running"
                assert (
                    await db.execute(sa.text(f"SELECT count(*) FROM {world.schema}.output_seal"))
                ).scalar_one() == 0
            first = await recovery.handle_worker_failure(claimed, error)
            assert first.disposition == "failed"
            assert (await recovery.handle_worker_failure(claimed, error)).disposition == "fenced"
            row = await _row(world, "SELECT * FROM run WHERE id=:id", id=world.run_id)
            assert row["status"] == "failed" and row["outcome"] is None
            assert int(row["state_version"]) == 2
            assert row["failure"]["recoveryOptions"]
            assert len(row["failure"]["partialResultRefs"]) == (1 if content else 0)
            timeline = await _row(world, "SELECT * FROM timeline_item WHERE id=:id", id=item)
            assert timeline["status"] == "failed"
            saved = await BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema)).get(
                world.scope, timeline["content_ref"]
            )
            assert saved["text"] == content and "private reasoning" not in str(saved)
            turn = await _row(world, "SELECT * FROM turn WHERE id=:id", id=world.turn_id)
            assert turn["outcome_run_id"] == world.run_id
            assert turn["failure"] == row["failure"]
            terminal = await _row(world, "SELECT payload FROM event WHERE kind='run.failed'")
            assert terminal["payload"]["publicMessage"]
            assert terminal["payload"]["nextSteps"]
            with pytest.raises(AgentRuntimeError):
                await journal.append(
                    _output_batch(
                        world, attempt_id=attempt, item_id=item, content="late old-owner output"
                    )
                )
            if content == "deleted":
                page = await SqlJournalReader(engine, world.schema).read_public(
                    actor(world=world),
                    StreamRequest(session_id=world.session_id, after_seq="0", last_event_id=None),
                )
                assert page.kind == "resync_required"
                assert page.reason == "authorization_changed"
        finally:
            await engine.dispose()

    run(scenario)
    assert count(world, "event", "kind='run.failed'") == 1
    assert count(world, "event", "kind='item.completed'") == 1
    assert count(world, "output_seal") == 1
    assert count(world, "outbox") == count(world, "event")


@pytest.mark.parametrize("terminal_kind", ("failed", "cancelled"))
def test_terminal_keeps_completed_work_and_unknown_effect_supervision(world, terminal_kind):  # noqa: F811
    async def scenario():
        engine = engine_for(world.schema, search_path=False)
        journal = SqlJournal(engine, world.schema, actor(world=world))
        try:
            completed = await _seed_item(world)
            prior_attempt = await _seed_attempt(world)
            await journal.append(
                _output_batch(
                    world, attempt_id=prior_attempt, item_id=completed, content="delivered work"
                )
            )
            await journal.seal_output(
                SealOutputRequest(
                    lease=lease(world),
                    attempt_id=prior_attempt,
                    item_id=completed,
                    proof_id=uuid.uuid4(),
                    sealed_revision="1",
                )
            )
            unfinished = await _seed_item(world)
            new_attempt = await _seed_attempt(world)
            await journal.append(
                _output_batch(
                    world, attempt_id=new_attempt, item_id=unfinished, content="unfinished work"
                )
            )
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f"UPDATE {world.schema}.timeline_item SET status='streaming' WHERE id=:id"
                    ),
                    {"id": unfinished},
                )
                await db.execute(
                    sa.text(f"UPDATE {world.schema}.turn SET primary_run_id=:id"),
                    {"id": world.run_id},
                )
            before = await _row(world, "SELECT * FROM timeline_item WHERE id=:id", id=completed)
            execution = supervisor = None
            if terminal_kind == "failed":
                supervisor = await _seed_work(world, kind="reconcile_execution")
                execution = await _seed_execution(world, supervisor_work_id=supervisor)
                claimed = ClaimedWork(
                    disposition="claimed",
                    lease=lease(world),
                    state_version="1",
                    checkpoint_id=world.checkpoint_id,
                    budget_allocation_id=uuid.uuid4(),
                )
                await SqlRecovery(engine, world.schema).handle_worker_failure(
                    claimed,
                    AgentRuntimeError(
                        FailureEnvelope(
                            code="TOOL_FAILED",
                            category="tool",
                            retryable=False,
                            public_message="工具执行未能完成。",
                        )
                    ),
                )
            else:
                receipt = await SqlStateUnitOfWork(
                    engine, world.schema, actor(world=world)
                ).record_control(
                    ControlRequest(
                        run_id=world.run_id,
                        kind="cancel",
                        idempotency_key="f04-cancel-retained",
                        expected_version="1",
                    )
                )
                row = await _row(world, "SELECT * FROM run WHERE id=:id", id=world.run_id)
                claimed = ControlClaimedWork(
                    disposition="control_claimed",
                    lease=lease(world),
                    state_version=str(row["state_version"]),
                    checkpoint_id=row["checkpoint_id"],
                    command_id=receipt.command_id,
                    desired_control="cancel",
                    status="cancelling",
                    cancel_version=str(row["cancel_version"]),
                )
                await SqlMainControlHandler(engine, world.schema, actor(world=world))(
                    claimed, LeaseHandle(claimed.lease)
                )
            assert before == await _row(
                world, "SELECT * FROM timeline_item WHERE id=:id", id=completed
            )
            terminal = await _row(
                world, "SELECT payload FROM event WHERE kind=:kind", kind=f"run.{terminal_kind}"
            )
            refs = terminal["payload"]["partialResultRefs"]
            assert {ref["resourceId"] for ref in refs} == {str(completed), str(unfinished)}
            assert terminal["payload"]["publicMessage"] and terminal["payload"]["nextSteps"]
            if execution is not None:
                row = await _row(world, "SELECT status FROM execution WHERE id=:id", id=execution)
                assert row["status"] == "running"
                row = await _row(world, "SELECT status FROM work_item WHERE id=:id", id=supervisor)
                assert row["status"] == "pending"
                assert terminal["payload"]["nextSteps"][0]["action"] == "manual_attention"
            turn = await _row(
                world, "SELECT outcome_run_id FROM turn WHERE id=:id", id=world.turn_id
            )
            assert turn["outcome_run_id"] == world.run_id
        finally:
            await engine.dispose()

    run(scenario)
    assert count(world, "outbox") == count(world, "event")

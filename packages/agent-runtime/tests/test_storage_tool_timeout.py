"""Real SQL observation/ledger checks; simulated handlers use real asyncio timers."""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import sqlalchemy as sa
from test_decision_validation import facts, tool_action
from test_storage_subagents import install_tool_revision
from test_storage_uow import (
    _row,
    _seed,
    _seed_attempt,
    actor,
    engine_for,
    guard,
    lease,
    migrate,
    run,
)
from test_tool_timeout_boundary import assert_timeout, grant_for, invocation_pair

from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.decisions import decision_adapter
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.tools import ToolCompleted, ToolFailed
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.decisions import SqlDecisionStore
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.tool_execution_gate import SqlToolExecutionGate
from agent_runtime.storage.tool_host import SqlToolHost, ToolEffect
from agent_runtime.storage.uow import SqlStateUnitOfWork, SqlSystemCapacityStore

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENT_RUNTIME_TEST_DSN"), reason="requires guarded real PostgreSQL"
)


@pytest.fixture(scope="module")
def timeout_schema():
    assert sa.make_url(os.environ["AGENT_RUNTIME_TEST_DSN"]).database == "aletai_wangxh"
    schema = f"r05_tool_timeout_{uuid4().hex[:12]}"
    try:
        assert migrate("upgrade", schema).status == "applied"
        yield schema
    finally:

        async def drop():
            engine = engine_for(schema, search_path=False)
            try:
                async with engine.begin() as db:
                    await db.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            finally:
                await engine.dispose()

        run(drop)


@pytest.mark.parametrize(
    "mode", ["readonly_timer", "external_timer", "external_native", "completed"]
)
def test_timeout_sql_observation_and_external_retention(timeout_schema, mode, monkeypatch):
    async def check():
        world = await _seed(timeout_schema, suffix=uuid4().hex[:8])
        engine = engine_for(world.schema, search_path=False)
        try:
            attempt_id = await _seed_attempt(world)
            snapshot_id = (
                await _row(
                    world,
                    "SELECT context_snapshot_id FROM model_attempt WHERE id=:id",
                    id=attempt_id,
                )
            )["context_snapshot_id"]
            external = mode.startswith("external")
            spec, invocation = invocation_pair(effect="external" if external else "none")
            # Separate capability and capacity-group identities for this shared random schema.
            ref = spec.ref.model_copy(
                update={"key": spec.ref.key.model_copy(update={"key": world.suffix})}
            )
            spec = spec.model_copy(update={"ref": ref, "concurrency_group": world.suffix})
            state, context, _ = facts()
            state = state.model_copy(
                update={
                    "scope_id": world.scope,
                    "session_id": world.session_id,
                    "run_id": world.run_id,
                    "state_version": str(world.state_version),
                    "checkpoint_ref": "state:2",
                }
            )
            context = context.model_copy(
                update={
                    "scope_id": world.scope,
                    "session_id": world.session_id,
                    "run_id": world.run_id,
                    "snapshot_id": snapshot_id,
                    "tools": (spec.ref,),
                }
            )
            frame = DecisionFrame(
                actor=actor(world=world),
                context=context,
                attempt_id=attempt_id,
                action=decision_adapter.validate_python(
                    tool_action(tool=spec.ref.model_dump(mode="json", by_alias=True))
                ),
            )
            journal = SqlJournal(engine, world.schema, frame.actor)
            store = SqlDecisionStore(
                engine,
                world.schema,
                actor=frame.actor,
                lease=lease(world),
                payloads=BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema)),
                context=SimpleNamespace(load=AsyncMock(return_value=(context, {}))),
                access=SimpleNamespace(verify=AsyncMock()),
                journal=journal,
            )
            await store.persist(state, frame)
            call = frame.action.payload.validated_calls[0]
            invocation = invocation.model_copy(
                update={
                    "actor": frame.actor,
                    "run_id": world.run_id,
                    "session_id": world.session_id,
                    "model_attempt_id": attempt_id,
                    "context_snapshot_id": snapshot_id,
                    "context_version": context.context_version,
                    "call": call,
                }
            )
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f'INSERT INTO "{world.schema}".budget_allocation '
                        "(id,scope_id,root_run_id,limits,reserved,unknown_reserved,settled,"
                        "usage_status,unsettled_count,version) VALUES "
                        "(:id,:scope,:run,CAST(:limits AS jsonb),'{}','{}','{}','complete',0,1)"
                    ),
                    {
                        "id": uuid4(),
                        "scope": world.scope,
                        "run": world.run_id,
                        "limits": '{"tool_calls":4}',
                    },
                )
            capacity_store = SqlSystemCapacityStore(engine, world.schema)
            capacity = SqlToolCapacity(capacity_store, deployment_id="timeout-sql")
            await capacity_store.seed(
                capacity.key(world.scope, spec), capacity=spec.max_concurrency
            )
            if external:
                await install_tool_revision(engine, world, spec)
            unit = SqlStateUnitOfWork(engine, world.schema, frame.actor)

            async def authorize(db, current, refs):
                assert current.actor == frame.actor and refs == ()
                await unit._lock_guard(db, guard(world))

            stopped = asyncio.Event()
            calls = []

            async def dispatch(current, *, key=None):
                calls.append(key)
                assert current == invocation
                if mode == "completed":
                    return ToolEffect(data={"value": 420}, summary="completed")
                if mode == "external_native":
                    raise TimeoutError("response timed out after send")
                try:
                    await asyncio.Event().wait()
                finally:
                    await asyncio.sleep(0.01)
                    stopped.set()

            lookup = AsyncMock()
            host = SqlToolHost(
                engine,
                world.schema,
                specs=SimpleNamespace(get=AsyncMock(return_value=spec)),
                resources=SimpleNamespace(resources=AsyncMock(return_value=())),
                schemas=SimpleNamespace(input=lambda *args: None, output=lambda *args: None),
                authorize=authorize,
                capacity=capacity,
                execution_gate=SqlToolExecutionGate(
                    world.schema, approval_required=lambda *args: False
                ),
                read_tools={spec.ref: SimpleNamespace(invoke=dispatch)} if not external else {},
                external_tools={spec.ref: SimpleNamespace(dispatch=dispatch, lookup=lookup)}
                if external
                else {},
            )
            # All fixture SQL has finished before this bounded end-to-end invocation starts.
            invocation = invocation.model_copy(
                update={"deadline_at": datetime.now(UTC) + timedelta(seconds=6)}
            )
            grant = grant_for(spec, invocation)
            release = AsyncMock(wraps=capacity.release)
            monkeypatch.setattr(capacity, "release", release)
            if mode == "external_timer":
                with pytest.raises(AgentRuntimeError) as caught:
                    await host.execute(invocation, grant)
                assert_timeout(caught.value.failure, invocation)
                assert isinstance(caught.value.__cause__, TimeoutError)
                assert stopped.is_set()
            else:
                outcome = await host.execute(invocation, grant)
                if mode == "external_native":
                    assert isinstance(outcome, ToolFailed) and outcome.effect == "unknown"
                    assert outcome.failure.code == "DEPENDENCY_UNAVAILABLE"
                elif mode == "completed":
                    assert isinstance(outcome, ToolCompleted) and outcome.result.data == {
                        "value": 420
                    }
                else:
                    assert isinstance(outcome, ToolFailed) and outcome.effect == "none"
                    assert_timeout(outcome.failure, invocation)
                    assert stopped.is_set()
                    original = journal.append_state_in_transaction

                    async def fail_event(*args):
                        raise RuntimeError("forced observation rollback")

                    monkeypatch.setattr(journal, "append_state_in_transaction", fail_event)
                    with pytest.raises(RuntimeError, match="forced observation rollback"):
                        await store.record(frame, outcome)
                    assert (await store.recover()).unresolved_call_ids == (call.tool_call_id,)
                    monkeypatch.setattr(journal, "append_state_in_transaction", original)
                if not external:
                    await store.record(frame, outcome)
                    await store.record(frame, outcome)
                    recovered = await store.recover()
                    assert recovered.unresolved_call_ids == () and recovered.outcomes == (outcome,)
                    row = await _row(
                        world, "SELECT status FROM tool_call WHERE id=:id", id=call.tool_call_id
                    )
                    assert row["status"] == ("completed" if mode == "completed" else "failed")
                    events = await _row(
                        world,
                        "SELECT count(*) AS n FROM event "
                        "WHERE run_id=:run AND kind='tool.finished'",
                        run=world.run_id,
                    )
                    assert events["n"] == 1
            assert len(calls) == 1
            lookup.assert_not_awaited()
            attempt = await _row(
                world, "SELECT status,usage_status FROM model_attempt WHERE id=:id", id=attempt_id
            )
            assert attempt == {"status": "completed", "usage_status": "complete"}
            if external:
                release.assert_not_awaited()
                operation = await _row(
                    world, "SELECT status FROM operation WHERE id=:id", id=invocation.operation_id
                )
                assert operation["status"] == (
                    "dispatching" if mode == "external_timer" else "unknown"
                )
                execution = await _row(
                    world,
                    "SELECT status,supervisor_work_id FROM execution WHERE operation_id=:id",
                    id=invocation.operation_id,
                )
                assert execution["status"] == (
                    "running" if mode == "external_timer" else "unknown"
                )
                work = await _row(
                    world,
                    "SELECT status FROM work_item WHERE id=:id",
                    id=execution["supervisor_work_id"],
                )
                assert work["status"] == "pending"
                reservation = await capacity_store.reservation_by_request(
                    capacity.key(world.scope, spec), f"tool:{invocation.operation_id}"
                )
                assert reservation["status"] == "reserved"
            else:
                release.assert_awaited_once()
        finally:
            await engine.dispose()

    run(check)

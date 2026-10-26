"""Real SQL input-rejection observations never acquire effect/capacity intent."""

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from test_decision_validation import facts, tool_action
from test_storage_tool_timeout import timeout_schema  # noqa: F401
from test_storage_uow import _row, _seed, _seed_attempt, actor, engine_for, lease, run
from test_tool_input_observation import rejecting_schemas
from test_tool_timeout_boundary import grant_for, invocation_pair

from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.decisions import decision_adapter
from agent_runtime.contracts.tools import ToolNotExecuted
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.decisions import SqlDecisionStore
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.tool_execution_gate import SqlToolExecutionGate
from agent_runtime.storage.tool_host import SqlToolHost
from agent_runtime.storage.uow import SqlSystemCapacityStore

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENT_RUNTIME_TEST_DSN"), reason="requires guarded real PostgreSQL"
)


@pytest.mark.parametrize("mode", ["raw_pydantic", "pinned"])
def test_invalid_input_observation_is_unique_and_has_no_dispatch_intent(timeout_schema, mode):  # noqa: F811
    async def check():
        world = await _seed(timeout_schema, suffix=uuid4().hex[:8])
        engine = engine_for(world.schema, search_path=False)
        try:
            attempt = await _seed_attempt(world)
            snapshot = (
                await _row(
                    world, "SELECT context_snapshot_id FROM model_attempt WHERE id=:id", id=attempt
                )
            )["context_snapshot_id"]
            spec, invocation = invocation_pair(effect="external", seconds=300)
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
                    "snapshot_id": snapshot,
                    "tools": (spec.ref,),
                }
            )
            action = tool_action(tool=spec.ref.model_dump(mode="json", by_alias=True))
            action["payload"]["validatedCalls"][0]["arguments"] = {
                "private": "PRIVATE_ARGUMENT_TEXT"
            }
            frame = DecisionFrame(
                actor=actor(world=world),
                context=context,
                attempt_id=attempt,
                action=decision_adapter.validate_python(action),
            )
            store = SqlDecisionStore(
                engine,
                world.schema,
                actor=frame.actor,
                lease=lease(world),
                payloads=BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema)),
                context=SimpleNamespace(load=AsyncMock(return_value=(context, {}))),
                access=SimpleNamespace(verify=AsyncMock()),
                journal=SqlJournal(engine, world.schema, frame.actor),
            )
            await store.persist(state, frame)
            call = frame.action.payload.validated_calls[0]
            before = await _row(
                world, "SELECT status FROM tool_call WHERE id=:id", id=call.tool_call_id
            )
            assert before["status"] == "pending"
            invocation = invocation.model_copy(
                update={
                    "actor": frame.actor,
                    "run_id": world.run_id,
                    "session_id": world.session_id,
                    "model_attempt_id": attempt,
                    "context_snapshot_id": snapshot,
                    "context_version": context.context_version,
                    "call": call,
                }
            )
            resources = SimpleNamespace(
                resources=AsyncMock(side_effect=AssertionError("resources reached"))
            )
            authorize = AsyncMock(side_effect=AssertionError("authority reached"))
            handler = SimpleNamespace(dispatch=AsyncMock(), lookup=AsyncMock())
            hook = SimpleNamespace(before=AsyncMock(), after=AsyncMock())
            host = SqlToolHost(
                engine,
                world.schema,
                specs=SimpleNamespace(get=AsyncMock(return_value=spec)),
                resources=resources,
                schemas=rejecting_schemas(spec, mode),
                authorize=authorize,
                capacity=SqlToolCapacity(
                    SqlSystemCapacityStore(engine, world.schema), deployment_id="input-sql"
                ),
                execution_gate=SqlToolExecutionGate(
                    world.schema, approval_required=lambda *args: False
                ),
                external_tools={spec.ref: handler},
                hooks=(hook,),
            )
            outcome = await host.execute(invocation, grant_for(spec, invocation))
            assert isinstance(outcome, ToolNotExecuted) and outcome.effect == "none"
            assert outcome.tool_call_id == call.tool_call_id and outcome.run_id == world.run_id
            assert outcome.operation_id == invocation.operation_id
            assert outcome.failure.code == "INVALID_INPUT" and not outcome.failure.retryable
            assert "PRIVATE_ARGUMENT_TEXT" not in outcome.model_dump_json()
            await store.record(frame, outcome)
            await store.record(frame, outcome)
            recovered = await store.recover()
            assert recovered.unresolved_call_ids == () and recovered.outcomes == (outcome,)
            assert len(recovered.require_next_model_sources()) == 1
            row = await _row(
                world,
                "SELECT status,failure,finished_at FROM tool_call WHERE id=:id",
                id=call.tool_call_id,
            )
            assert row["status"] == "not_executed" and row["finished_at"] is not None
            assert row["failure"]["code"] == "INVALID_INPUT"
            events = await _row(
                world,
                "SELECT count(*) AS n FROM event WHERE run_id=:run AND kind='tool.finished'",
                run=world.run_id,
            )
            assert events["n"] == 1
            for table, predicate in (
                ("operation", "task_id=:task"),
                ("execution", "run_id=:run"),
                ("system_capacity_reservation", "holder_ref=:holder"),
                ("work_item", "run_id=:run AND kind='reconcile_execution'"),
            ):
                row = await _row(
                    world,
                    f"SELECT count(*) AS n FROM {table} WHERE {predicate}",
                    run=world.run_id,
                    task=world.task_id,
                    holder=str(world.run_id),
                )
                assert row["n"] == 0, table
            audit = await _row(
                world, "SELECT status,usage_status FROM model_attempt WHERE id=:id", id=attempt
            )
            assert audit == {"status": "completed", "usage_status": "complete"}
            resources.resources.assert_not_awaited()
            authorize.assert_not_awaited()
            handler.dispatch.assert_not_awaited()
            handler.lookup.assert_not_awaited()
            hook.before.assert_not_awaited()
            hook.after.assert_not_awaited()
        finally:
            await engine.dispose()

    run(check)

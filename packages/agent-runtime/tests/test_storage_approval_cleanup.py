"""Real PostgreSQL approval cleanup and decision/event atomicity."""

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from uuid import uuid4

import pytest
import sqlalchemy as sa
from test_storage_approvals import Access
from test_storage_uow import actor, engine_for, run
from test_storage_uow import no_leftover_schemas as no_leftover_schemas
from test_storage_uow import schema as schema
from test_storage_uow import world as world

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.policy import PolicyRequest
from agent_runtime.contracts.requests import ControlRequest
from agent_runtime.contracts.tools import ApprovalDecision, ToolInvocation, ToolSpec
from agent_runtime.storage.approval_cleanup import SqlApprovalCleanup
from agent_runtime.storage.approvals import SqlApprovalService
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.uow import SqlStateUnitOfWork

pytestmark = pytest.mark.skipif(
    not os.getenv("AGENT_RUNTIME_TEST_DSN"), reason="requires real PostgreSQL"
)


async def issue(world, engine):
    data = json.loads(files("agent_runtime.contracts").joinpath("fixtures/tools.json").read_text())
    spec = ToolSpec.model_validate(data["spec"])
    now = datetime.now(UTC)
    invocation = ToolInvocation.model_validate(
        {
            **data["invocation"],
            "actor": actor(world=world).model_dump(mode="json", by_alias=True),
            "sessionId": str(world.session_id),
            "runId": str(world.run_id),
            "modelAttemptId": None,
            "operationId": str(uuid4()),
            "deadlineAt": (now + timedelta(seconds=60)).isoformat(),
        }
    )
    service = SqlApprovalService(engine, world.schema, access=Access())
    approval = await service.request(
        PolicyRequest(invocation=invocation, policy=spec.approval_policy, resources=()),
        spec,
        public_summary="Approve report",
        expires_at=now + timedelta(minutes=5),
    )
    async with engine.begin() as db:
        await db.execute(
            sa.text(f"UPDATE \"{world.schema}\".run SET status='waiting_user' WHERE id=:id"),
            {"id": world.run_id},
        )
    return service, approval


def decision(approval):
    return ApprovalDecision(
        approval_id=approval.approval_id,
        expected_version="0",
        decision="approved",
        idempotency_key=f"approval:{approval.approval_id}:0:approved",
    )


async def assert_terminal(world, engine, approval, status):
    async with engine.connect() as db:
        row = (
            (
                await db.execute(
                    sa.text(f'SELECT status,revision FROM "{world.schema}".approval WHERE id=:id'),
                    {"id": approval.approval_id},
                )
            )
            .mappings()
            .one()
        )
        events = (
            (
                await db.execute(
                    sa.text(
                        f'SELECT e.payload FROM "{world.schema}".event e '
                        f'JOIN "{world.schema}".outbox o ON o.event_id=e.id '
                        "WHERE e.kind='approval.updated'"
                    )
                )
            )
            .scalars()
            .all()
        )
    assert row["status"] == status and row["revision"] == 1
    assert len(events) == 1
    assert events[0]["status"] == status and events[0]["revision"] == "1"
    assert events[0]["approvalRef"]["resourceId"] == str(approval.approval_id)


@pytest.mark.parametrize(
    "cause", ["completed", "failed", "cancelled", "cancel", "expired", "auth", "deleted"]
)
def test_cleanup_closes_pending_with_one_durable_event(world, cause):
    async def scenario():
        engine = engine_for(world.schema)
        try:
            _, approval = await issue(world, engine)
            async with engine.begin() as db:
                if cause in {"completed", "failed", "cancelled"}:
                    table, assignments = "run", "status=:value,owner_id=NULL,lease_until=NULL"
                    if cause == "completed":
                        assignments += ",outcome='success'"
                    elif cause == "failed":
                        assignments += ",failure='{}'::jsonb"
                    value = cause
                elif cause == "cancel":
                    table, assignments, value = "run", "status=:value", "waiting_user"
                elif cause == "expired":
                    table, assignments, value = (
                        "approval",
                        "expires_at=:value",
                        datetime.now(UTC) - timedelta(seconds=1),
                    )
                elif cause == "auth":
                    table, assignments, value = "session", "auth_revision=:value", 2
                else:
                    table, assignments, value = "session", "deleted_at=:value", datetime.now(UTC)
                target = (
                    approval.approval_id
                    if table == "approval"
                    else world.session_id
                    if table == "session"
                    else world.run_id
                )
                await db.execute(
                    sa.text(f'UPDATE "{world.schema}".{table} SET {assignments} WHERE id=:id'),
                    {"value": value, "id": target},
                )
            if cause == "cancel":
                await SqlStateUnitOfWork(engine, world.schema, actor(world=world)).record_control(
                    ControlRequest(
                        run_id=world.run_id,
                        kind="cancel",
                        idempotency_key="cleanup-cancel",
                        expected_version="1",
                    )
                )
            cleanup = SqlApprovalCleanup(engine, world.schema)
            assert await cleanup.reconcile(world.scope) == 1
            assert await cleanup.reconcile(world.scope) == 0
            await assert_terminal(
                world, engine, approval, "expired" if cause == "expired" else "cancelled"
            )
        finally:
            await engine.dispose()

    run(scenario)


@pytest.mark.parametrize("path", ["decision", "cleanup"])
def test_approval_rolls_back_when_journal_cannot_commit(world, monkeypatch, path):
    async def scenario():
        engine = engine_for(world.schema)
        try:
            service, approval = await issue(world, engine)

            async def unavailable(*args, **kwargs):
                raise RuntimeError("injected Journal failure")

            with monkeypatch.context() as patch:
                patch.setattr(SqlJournal, "append_state_in_transaction", unavailable)
                if path == "cleanup":
                    async with engine.begin() as db:
                        await db.execute(
                            sa.text(
                                f'UPDATE "{world.schema}".approval '
                                "SET expires_at=CURRENT_TIMESTAMP-INTERVAL '1 second' WHERE id=:id"
                            ),
                            {"id": approval.approval_id},
                        )
                with pytest.raises(RuntimeError, match="injected Journal"):
                    if path == "decision":
                        await service.resolve(actor(world=world), decision(approval))
                    else:
                        await SqlApprovalCleanup(engine, world.schema).reconcile(world.scope)
            async with engine.connect() as db:
                row = (
                    await db.execute(
                        sa.text(
                            f'SELECT status,revision FROM "{world.schema}".approval WHERE id=:id'
                        ),
                        {"id": approval.approval_id},
                    )
                ).one()
                count = (
                    await db.execute(
                        sa.text(
                            f'SELECT count(*) FROM "{world.schema}".event '
                            "WHERE kind='approval.updated'"
                        )
                    )
                ).scalar_one()
            assert tuple(row) == ("pending", 0) and count == 0
            if path == "decision":
                await service.resolve(actor(world=world), decision(approval))
                await assert_terminal(world, engine, approval, "approved")
            else:
                assert await SqlApprovalCleanup(engine, world.schema).reconcile(world.scope) == 1
                await assert_terminal(world, engine, approval, "expired")
        finally:
            await engine.dispose()

    run(scenario)


def test_cleanup_racing_decision_never_overwrites_approved(world):
    async def scenario():
        engine = engine_for(world.schema)
        try:
            service, approval = await issue(world, engine)
            # Force both cleanup and resolve to observe an expired pending item.
            now = datetime.now(UTC) + timedelta(minutes=6)
            service = SqlApprovalService(engine, world.schema, access=Access(), clock=lambda: now)
            cleanup = SqlApprovalCleanup(engine, world.schema, clock=lambda: now)
            results = await asyncio.wait_for(
                asyncio.gather(
                    service.resolve(actor(world=world), decision(approval)),
                    cleanup.reconcile(world.scope),
                    return_exceptions=True,
                ),
                timeout=20,
            )
            assert all(
                not isinstance(item, Exception) or isinstance(item, AgentRuntimeError)
                for item in results
            )
            await assert_terminal(world, engine, approval, "expired")
        finally:
            await engine.dispose()

    run(scenario)

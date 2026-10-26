"""Pending approval validity and durable bounded-scan progress on real PostgreSQL."""

import asyncio
import json
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa
from test_storage_approval_cleanup import issue
from test_storage_uow import _seed, engine_for, migrate, run

from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.storage.approval_cleanup import SqlApprovalCleanup

pytestmark = pytest.mark.skipif(
    not os.getenv("AGENT_RUNTIME_TEST_DSN"), reason="requires real PostgreSQL"
)


@pytest.fixture(scope="module")
def schema(tmp_path_factory):
    name = "p05_validity_sql_" + uuid4().hex[:12]
    assert migrate("upgrade", name).status == "applied"
    record = tmp_path_factory.mktemp("validity-schema") / "cleanup.json"
    record.write_text(json.dumps({"schema": name, "status": "pending"}))
    try:
        yield name
    finally:

        async def drop():
            engine = engine_for(name)
            try:
                async with engine.begin() as db:
                    await db.execute(sa.schema.DropSchema(name, cascade=True, if_exists=True))
            finally:
                await engine.dispose()

        run(drop)
        record.write_text(json.dumps({"schema": name, "status": "cleaned"}))


@pytest.fixture
def world(schema):
    identity = uuid4().hex
    return run(lambda: _seed(schema, suffix=identity, scope="validity-" + identity))


async def assert_terminal(world, engine, approval, status):
    async with engine.connect() as db:
        row = (
            await db.execute(
                sa.text(
                    f'SELECT status,revision FROM "{world.schema}".approval '
                    'WHERE scope_id=:scope AND id=:id'
                ),
                {"scope": world.scope, "id": approval.approval_id},
            )
        ).one()
        events = (
            (
                await db.execute(
                    sa.text(
                        f'SELECT e.payload FROM "{world.schema}".event e '
                        f'JOIN "{world.schema}".outbox o '
                        "ON o.scope_id=e.scope_id AND o.event_id=e.id "
                        "WHERE e.scope_id=:scope AND e.kind='approval.updated'"
                    ),
                    {"scope": world.scope},
                )
            )
            .scalars()
            .all()
        )
    assert tuple(row) == (status, 1)
    assert len(events) == 1 and events[0]["status"] == status


@pytest.mark.parametrize("cause", ["grant_revoked", "grant_expired", "run_revision"])
def test_pending_core_authority_loss_is_cancelled(world, cause):
    async def scenario():
        engine = engine_for(world.schema)
        try:
            _, approval = await issue(world, engine)
            async with engine.begin() as db:
                if cause == "run_revision":
                    query = (
                        "UPDATE {s}.run SET authority_revision=authority_revision+1 WHERE id=:id"
                    )
                elif cause == "grant_revoked":
                    query = (
                        "UPDATE {s}.capability_grant SET revoked_at=CURRENT_TIMESTAMP "
                        "WHERE run_id=:id"
                    )
                else:
                    query = "UPDATE {s}.capability_grant SET expires_at=:past WHERE run_id=:id"
                await db.execute(
                    sa.text(query.format(s=f'"{world.schema}"')),
                    {
                        "id": world.run_id,
                        "past": datetime.now(UTC) - timedelta(seconds=1),
                    },
                )
            cleanup = SqlApprovalCleanup(engine, world.schema)
            assert await cleanup.reconcile(world.scope) == 1
            assert await cleanup.reconcile(world.scope) == 0
            await assert_terminal(world, engine, approval, "cancelled")
        finally:
            await engine.dispose()

    run(scenario)


@pytest.mark.parametrize(
    "code",
    [
        "FORBIDDEN",
        "VERSION_CONFLICT",
        "DELIVERY_EXPIRED",
        "DEPENDENCY_UNAVAILABLE",
        "runtime",
        "database",
    ],
)
def test_resource_validity_only_cancels_on_definitive_denial(world, code):
    async def scenario():
        engine = engine_for(world.schema)
        try:
            _, approval = await issue(world, engine)

            async def validity(db, actor, session_id, resources):
                assert actor.actor_id and session_id == world.session_id
                if code == "database":
                    await db.execute(sa.text("SELECT * FROM nonexistent_approval_validity_table"))
                if code == "runtime":
                    raise RuntimeError("temporary validity dependency failure")
                raise runtime_failure(
                    code, category="authorization", public_message="Validity check"
                )

            cleanup = SqlApprovalCleanup(engine, world.schema, resource_validity=validity)
            if code in {"FORBIDDEN", "VERSION_CONFLICT", "DELIVERY_EXPIRED"}:
                assert await cleanup.reconcile(world.scope) == 1
                assert await cleanup.reconcile(world.scope) == 0
                await assert_terminal(world, engine, approval, "cancelled")
            else:
                expected = (
                    RuntimeError
                    if code == "runtime"
                    else sa.exc.DBAPIError
                    if code == "database"
                    else AgentRuntimeError
                )
                with pytest.raises(expected):
                    await cleanup.reconcile(world.scope)
                async with engine.connect() as db:
                    row = (
                        await db.execute(
                            sa.text(
                                f'SELECT status,revision FROM "{world.schema}".approval '
                                "WHERE id=:id"
                            ),
                            {"id": approval.approval_id},
                        )
                    ).one()
                    count = (
                        await db.execute(
                            sa.text(
                                f'SELECT count(*) FROM "{world.schema}".event '
                                "WHERE scope_id=:scope AND kind='approval.updated'"
                            ),
                            {"scope": world.scope},
                        )
                    ).scalar_one()
                assert tuple(row) == ("pending", 0) and count == 0
        finally:
            await engine.dispose()

    run(scenario)


def test_valid_pending_rows_rotate_across_scanner_restarts(world):
    async def scenario():
        engine = engine_for(world.schema)
        try:
            approvals = []
            for _ in range(3):
                async with engine.begin() as db:
                    await db.execute(
                        sa.text(f"UPDATE \"{world.schema}\".run SET status='running' WHERE id=:id"),
                        {"id": world.run_id},
                    )
                _, approval = await issue(world, engine)
                approvals.append(approval)
                async with engine.begin() as db:
                    await db.execute(
                        sa.text(
                            f'UPDATE "{world.schema}".approval '
                            "SET resource_refs=CAST(:refs AS jsonb) WHERE id=:id"
                        ),
                        {
                            "id": approval.approval_id,
                            "refs": json.dumps(
                                [
                                    {
                                        "resourceType": "attachment",
                                        "resourceId": str(uuid4()),
                                        "resourceVersion": "1",
                                        "scope": {
                                            "scopeId": world.scope,
                                            "sessionId": str(world.session_id),
                                        },
                                    }
                                ]
                            ),
                        },
                    )
            async with engine.connect() as db:
                refs = (
                    (
                        await db.execute(
                            sa.text(
                                f'SELECT resource_refs FROM "{world.schema}".approval '
                                "WHERE scope_id=:scope ORDER BY updated_at,id"
                            ),
                            {"scope": world.scope},
                        )
                    )
                    .scalars()
                    .all()
                )
            seen = []

            async def validity(db, actor, session_id, resources):
                seen.append(resources[0].resource_id)
                if resources[0].resource_id == refs[-1][0]["resourceId"]:
                    raise runtime_failure(
                        "FORBIDDEN", category="authorization", public_message="Revoked"
                    )

            fixed = datetime.now(UTC)
            for expected in [0, 0, 1]:
                assert (
                    await SqlApprovalCleanup(
                        engine, world.schema, resource_validity=validity, clock=lambda: fixed
                    ).reconcile(world.scope, limit=1)
                    == expected
                )
            assert seen == [ref[0]["resourceId"] for ref in refs]
            await assert_terminal(world, engine, approvals[-1], "cancelled")
        finally:
            await engine.dispose()

    run(scenario)


def test_concurrent_resource_cleanup_has_one_terminal_event(world):
    async def scenario():
        engine = engine_for(world.schema)
        try:
            _, approval = await issue(world, engine)

            async def validity(db, actor, session_id, resources):
                raise runtime_failure(
                    "FORBIDDEN", category="authorization", public_message="Revoked"
                )

            counts = await asyncio.wait_for(
                asyncio.gather(
                    *[
                        SqlApprovalCleanup(
                            engine, world.schema, resource_validity=validity
                        ).reconcile(world.scope)
                        for _ in range(2)
                    ]
                ),
                timeout=20,
            )
            assert sum(counts) == 1
            await assert_terminal(world, engine, approval, "cancelled")
        finally:
            await engine.dispose()

    run(scenario)

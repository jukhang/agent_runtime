"""Spawn intersects requested, parent and pinned Agent time limits exactly once."""

import json
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa
from test_storage_subagents import claim_request
from test_storage_uow import _seed, actor, engine_for, guard, migrate, run

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.references import CapabilityKey, CapabilityRef
from agent_runtime.contracts.subagents import SpawnRequest
from agent_runtime.contracts.tasks import TaskPackage
from agent_runtime.contracts.work_claims import ChildClaimedWork
from agent_runtime.storage.delegation_types import DelegationPolicy
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.recovery import SqlRecovery
from agent_runtime.storage.scheduler import SqlScheduler
from agent_runtime.storage.subagents import SqlSubagentRuntime
from agent_runtime.storage.uow import SqlStateUnitOfWork

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENT_RUNTIME_TEST_DSN"), reason="requires guarded PostgreSQL"
)


@pytest.fixture(scope="module")
def deadline_schema():
    assert sa.make_url(os.environ["AGENT_RUNTIME_TEST_DSN"]).database == "aletai_wangxh"
    name = f"r05_spawn_deadline_{uuid4().hex[:12]}"
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


@pytest.mark.parametrize(
    "case,manifest,task_seconds,parent_seconds,expected_seconds",
    [
        ("task", {"budget": {"maxRuntimeSeconds": 300}}, 60, 900, 60),
        ("parent", {"budget": {"maxRuntimeSeconds": 300}}, 600, 60, 60),
        ("agent", {"budget": {"maxRuntimeSeconds": 300}}, 600, 900, 300),
        ("default_missing", {}, 3600, 7200, 1800),
        ("default_empty", {"budget": {}}, 3600, 7200, 1800),
        ("null", {"budget": None}, 600, 900, None),
        ("zero", {"budget": {"maxRuntimeSeconds": 0}}, 600, 900, None),
        ("negative", {"budget": {"maxRuntimeSeconds": -1}}, 600, 900, None),
        ("string", {"budget": {"maxRuntimeSeconds": "300"}}, 600, 900, None),
        ("bool", {"budget": {"maxRuntimeSeconds": True}}, 600, 900, None),
        ("unknown", {"budget": {"unknown": 300}}, 600, 900, None),
        ("expired", {}, -1, 900, None),
        ("parent_expired", {}, 600, -1, None),
    ],
)
def test_spawn_deadline_intersection_and_reclaim(
    deadline_schema, case, manifest, task_seconds, parent_seconds, expected_seconds
):
    world = run(lambda: _seed(deadline_schema, scope=f"deadline-{uuid4().hex}"))

    async def scenario():
        engine = engine_for(world.schema)
        now = datetime.now(UTC)
        target = CapabilityRef(key=CapabilityKey(namespace="test", key="child"), revision="1")
        acceptance = CapabilityRef(key=CapabilityKey(namespace="test", key="accept"), revision="1")
        allocation = uuid4()
        policy = DelegationPolicy(
            agent=target,
            agent_revision_id=world.agent_revision_id,
            parent_allocation_id=allocation,
            model_calls=2,
            tool_calls=0,
            tokens=100,
            tools=(),
            acceptance_spec=acceptance,
            output_schema_ref="test:text:1",
        )
        task = TaskPackage(
            task_id=uuid4(),
            target_agent=target,
            objective="Check supplied facts",
            constraints=(),
            acceptance_spec_ref="test:accept:1",
            output_schema_ref="test:text:1",
            resource_refs=(),
            budget_allocation_id=uuid4(),
            deadline_at=now + timedelta(seconds=task_seconds),
        )

        class Resources:
            async def authorize_session_in_transaction(self, db, current, session):
                assert current == actor(world=world) and session == world.session_id

        class Verifier:
            async def verify_in_transaction(self, *args):
                raise AssertionError("spawn does not verify a child result")

        async def deadlines(child):
            async with engine.connect() as db:
                result = await db.execute(
                    sa.text(
                        "SELECT 'run' AS kind,deadline_at AS deadline FROM run WHERE id=:run "
                        "UNION ALL SELECT 'task',deadline_at FROM task WHERE id=:task "
                        "UNION ALL SELECT 'work',deadline_at FROM work_item WHERE run_id=:run "
                        "UNION ALL SELECT 'assignment',deadline_at FROM agent_message "
                        "WHERE recipient_run_id=:run AND kind='task.assign' "
                        "UNION ALL SELECT 'grant',expires_at FROM capability_grant "
                        "WHERE run_id=:run"
                    ),
                    {"run": child.child_run_id, "task": child.task_id},
                )
                return dict(result.all())

        try:
            async with engine.begin() as db:
                await db.execute(
                    sa.text("UPDATE run SET deadline_at=:deadline WHERE id=:id"),
                    {"id": world.run_id, "deadline": now + timedelta(seconds=parent_seconds)},
                )
                await db.execute(
                    sa.text(
                        "UPDATE agent_revision SET manifest=CAST(:manifest AS jsonb) WHERE id=:id"
                    ),
                    {"id": world.agent_revision_id, "manifest": json.dumps(manifest)},
                )
                await db.execute(
                    sa.text(
                        "UPDATE capability_grant SET allowed_scope=CAST(:allowed AS jsonb),"
                        "expires_at=:expires WHERE run_id=:id"
                    ),
                    {
                        "id": world.run_id,
                        "expires": now + timedelta(days=1),
                        "allowed": json.dumps(
                            {
                                "agents": [target.model_dump(mode="json", by_alias=True)],
                                "tools": [],
                                "resources": [],
                            }
                        ),
                    },
                )
                await db.execute(
                    sa.text(
                        "INSERT INTO budget_allocation "
                        "(id,scope_id,root_run_id,limits,reserved,unknown_reserved,settled,"
                        "usage_status,unsettled_count,version) VALUES "
                        "(:id,:scope,:root,CAST(:limits AS jsonb),"
                        "'{}','{}','{}','complete',0,0)"
                    ),
                    {
                        "id": allocation,
                        "scope": world.scope,
                        "root": world.run_id,
                        "limits": json.dumps({"model_calls": 4, "tool_calls": 0, "tokens": 200}),
                    },
                )
            runtime = SqlSubagentRuntime(
                engine,
                world.schema,
                actor=actor(world=world),
                journal=SqlJournal(engine, world.schema, actor(world=world)),
                resources=Resources(),
                policies={target: policy},
                message_predicate=acceptance,
                verifier=Verifier(),
            )
            runtime._state = SqlStateUnitOfWork(
                engine, world.schema, actor(world=world), clock=lambda: now
            )
            request = SpawnRequest(
                parent_guard=guard(world), task=task, idempotency_key=f"spawn-{case}"
            )
            if expected_seconds is None:
                with pytest.raises(AgentRuntimeError) as exc:
                    await runtime.spawn(request)
                assert exc.value.failure.code == (
                    "INVALID_INPUT"
                    if case == "expired"
                    else "VERSION_CONFLICT"
                    if case == "parent_expired"
                    else "FORBIDDEN"
                )
                async with engine.connect() as db:
                    assert (
                        await db.execute(
                            sa.text("SELECT count(*) FROM run WHERE parent_run_id=:id"),
                            {"id": world.run_id},
                        )
                    ).scalar_one() == 0
                    assert (
                        await db.execute(
                            sa.text("SELECT checkpoint_id FROM run WHERE id=:id"),
                            {"id": world.run_id},
                        )
                    ).scalar_one() == world.checkpoint_id
                    assert (
                        await db.execute(
                            sa.text("SELECT count(*) FROM checkpoint WHERE run_id=:id"),
                            {"id": world.run_id},
                        )
                    ).scalar_one() == 1
                return
            child = await runtime.spawn(request)
            expected = now + timedelta(seconds=expected_seconds)
            before = await deadlines(child)
            assert before == dict.fromkeys(("run", "task", "work", "assignment", "grant"), expected)
            async with engine.connect() as db:
                audit = (
                    await db.execute(
                        sa.text(
                            "SELECT convert_from(b.content,'UTF8')::jsonb FROM run r "
                            "JOIN checkpoint c ON c.id=r.checkpoint_id AND c.scope_id=r.scope_id "
                            "JOIN context_blob b ON b.blob_key=c.state_ref "
                            "AND b.scope_id=c.scope_id "
                            "WHERE r.id=:id"
                        ),
                        {"id": world.run_id},
                    )
                ).scalar_one()
                assert datetime.fromisoformat(audit["task"]["deadlineAt"]) == task.deadline_at
            first = await SqlScheduler(
                engine, world.schema, clock=lambda: now + timedelta(seconds=1)
            ).claim_work(claim_request(world.scope, "child"))
            assert isinstance(first, ChildClaimedWork)
            async with engine.begin() as db:
                for table, identity in (
                    ("run", child.child_run_id),
                    ("work_item", first.lease.work_id),
                ):
                    await db.execute(
                        sa.text(f"UPDATE {table} SET lease_until=:expired WHERE id=:id"),
                        {"id": identity, "expired": now + timedelta(seconds=1)},
                    )
            report = await SqlRecovery(
                engine, world.schema, clock=lambda: now + timedelta(seconds=2)
            ).recover_expired_leases(world.scope)
            assert report.requeued == 1
            second = await SqlScheduler(
                engine, world.schema, clock=lambda: now + timedelta(seconds=10)
            ).claim_work(claim_request(world.scope, "child"))
            assert isinstance(second, ChildClaimedWork)
            assert second.lease.work_id == first.lease.work_id
            assert second.lease.work_epoch != first.lease.work_epoch
            assert await deadlines(child) == before
        finally:
            await engine.dispose()

    run(scenario)

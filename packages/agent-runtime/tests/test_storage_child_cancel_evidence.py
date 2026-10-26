"""Cancellation needs generation-bound stopped evidence, not a successful effect."""

import json
import os
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from test_storage_child_recovery import pair  # noqa: F401
from test_storage_uow import _seed_attempt, _seed_execution, _seed_work, engine_for, migrate, run

from agent_runtime.storage.child_recovery import unresolved_child_effects
from agent_runtime.storage.recovery import SqlRecovery


@pytest.fixture(scope="module")
def recovery_schema():
    assert sa.make_url(os.environ["AGENT_RUNTIME_TEST_DSN"]).database == "aletai_wangxh"
    schema = f"r05_cancel_evidence_{uuid4().hex[:12]}"
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


@pytest.mark.parametrize("cancelling", [False, True])
def test_query_keeps_explicit_cancellation_boundary(cancelling):
    class Database:
        async def execute(self, statement, parameters):
            assert parameters["cancelling"] is cancelling
            sql = str(statement)
            assert "w.subject_ref->>'resourceVersion'=CAST(e.generation AS text)" in sql
            assert "w.id=e.supervisor_work_id" in sql
            assert "w.status='completed'" in sql
            assert "COALESCE(o.failure->>'category','')" in sql
            assert not any(word in sql for word in ("UPDATE", "INSERT", "DELETE"))
            return self

        def scalar_one(self):
            return True

    assert (
        run(
            lambda: unresolved_child_effects(
                Database(), "offline", {"scope_id": "scope", "id": uuid4()}, cancelling=cancelling
            )
        )
        is True
    )


@pytest.mark.skipif(not os.environ.get("AGENT_RUNTIME_TEST_DSN"), reason="requires PostgreSQL")
def test_cancel_requires_current_stopped_evidence_without_refund(pair):  # noqa: F811
    parent, child, engine, now, execute, rows = pair
    supervisor = run(lambda: _seed_work(child, status="completed"))
    execution = run(
        lambda: _seed_execution(
            child, status="unknown", generation=7, supervisor_work_id=supervisor
        )
    )
    attempt = run(lambda: _seed_attempt(child))
    operation = run(
        lambda: rows("SELECT operation_id FROM {t}execution WHERE id=:id", id=execution)
    )[0]["operation_id"]
    tool, allocation, command = uuid4(), uuid4(), uuid4()
    facts = {"scope_id": child.scope, "id": child.run_id}

    async def prepare():
        async with engine.begin() as db:

            async def sql(statement, **params):
                await db.execute(sa.text(statement.replace("{t}", f'"{child.schema}".')), params)

            subject = {
                "resourceType": "execution",
                "resourceId": str(execution),
                "resourceVersion": "7",
                "scope": {"scopeId": child.scope},
            }
            await sql(
                "UPDATE {t}work_item SET subject_ref=CAST(:subject AS jsonb) WHERE id=:id",
                id=supervisor,
                subject=json.dumps(subject),
            )
            await sql(
                "UPDATE {t}operation SET status='unknown',result_ref=NULL,failure=NULL "
                "WHERE id=:id",
                id=operation,
            )
            await sql(
                "INSERT INTO {t}tool_call "
                "(id,scope_id,run_id,operation_id,spec_revision,input_hash,status) "
                "VALUES (:id,:scope,:run,:op,'{}',:hash,'failed')",
                id=tool,
                scope=child.scope,
                run=child.run_id,
                op=operation,
                hash="a" * 64,
            )
            await sql(
                "UPDATE {t}model_attempt SET status='running',finished_at=NULL WHERE id=:id",
                id=attempt,
            )
            await sql(
                "INSERT INTO {t}budget_allocation "
                "(id,scope_id,root_run_id,limits,reserved,unknown_reserved,settled,"
                "usage_status,unsettled_count,version) VALUES "
                "(:id,:scope,:root,'{}',:held,:held,'{}','unknown',1,1)",
                id=allocation,
                scope=child.scope,
                root=parent.run_id,
                held=json.dumps({"tokens": 123}),
            )
            await sql(
                "INSERT INTO {t}command "
                "(id,scope_id,run_id,actor_id,request_key,request_hash,kind,status,"
                "expected_state_version) VALUES (:id,:scope,:run,:actor,:key,:hash,"
                "'cancel','accepted',1)",
                id=command,
                scope=parent.scope,
                run=parent.run_id,
                actor=parent.actor_id,
                key=f"cancel:{command}",
                hash="b" * 64,
            )
            await sql(
                "UPDATE {t}run SET status='cancelling',desired_control='cancel',"
                "control_command_id=:command WHERE id=:id",
                id=parent.run_id,
                command=command,
            )

    run(prepare)
    budget_before = run(
        lambda: rows("SELECT * FROM {t}budget_allocation WHERE id=:id", id=allocation)
    )
    recovery = SqlRecovery(engine, child.schema, clock=lambda: now)
    assert run(lambda: recovery.recover_cancelled_children(child.scope)) == 0
    assert not run(lambda: rows("SELECT id FROM {t}agent_message"))

    async def matrix():
        async with AsyncSession(engine) as db, db.begin():

            async def sql(statement, **params):
                await db.execute(sa.text(statement.replace("{t}", f'"{child.schema}".')), params)

            marker = {"category": "reconciled_external_effect", "remoteStatus": "stopped"}
            await sql("UPDATE {t}execution SET status='cancelled' WHERE id=:id", id=execution)
            await sql(
                "UPDATE {t}operation SET failure=CAST(:marker AS jsonb) WHERE id=:id",
                id=operation,
                marker=json.dumps(marker),
            )
            mutations = [
                (execution, "execution SET status='running'"),
                (execution, "execution SET status='unknown'"),
                (execution, "execution SET status='failed'"),
                (execution, "execution SET generation=8"),
                (execution, "execution SET supervisor_work_id=NULL"),
                (supervisor, "work_item SET run_id=:parent"),
                (supervisor, "work_item SET kind='recover_scope'"),
                (supervisor, "work_item SET status='pending'"),
                (
                    supervisor,
                    "work_item SET subject_ref=jsonb_set(subject_ref,"
                    "'{resourceId}',to_jsonb('wrong'::text))",
                ),
                (
                    supervisor,
                    "work_item SET subject_ref=jsonb_set(subject_ref,"
                    "'{scope,scopeId}',to_jsonb('wrong'::text))",
                ),
                (operation, "operation SET failure=NULL"),
                (
                    operation,
                    "operation SET failure=jsonb_set(failure,"
                    "'{remoteStatus}',to_jsonb('completed'::text))",
                ),
                (operation, "operation SET status='dispatching'"),
                (operation, "operation SET status='prepared'"),
                (tool, "tool_call SET status='running'"),
            ]
            for identity, mutation in mutations:
                savepoint = await db.begin_nested()
                try:
                    await sql(
                        f"UPDATE {{t}}{mutation} WHERE id=:id", id=identity, parent=parent.run_id
                    )
                    assert await unresolved_child_effects(
                        db, child.schema, facts, cancelling=True
                    ), mutation
                finally:
                    await savepoint.rollback()
            for status, remote in (("completed", "completed"), ("cancelled", "stopped")):
                marker["remoteStatus"] = remote
                await sql(
                    "UPDATE {t}execution SET status=:status WHERE id=:id",
                    id=execution,
                    status=status,
                )
                await sql(
                    "UPDATE {t}operation SET failure=CAST(:marker AS jsonb) WHERE id=:id",
                    id=operation,
                    marker=json.dumps(marker),
                )
                assert await unresolved_child_effects(db, child.schema, facts)
                assert not await unresolved_child_effects(db, child.schema, facts, cancelling=True)

    run(matrix)
    other = run(lambda: _seed_execution(child, status="unknown"))
    assert run(lambda: recovery.recover_cancelled_children(child.scope)) == 0
    assert run(lambda: rows("SELECT status FROM {t}run WHERE id=:id", id=parent.run_id)) == [
        {"status": "cancelling"}
    ]
    run(lambda: execute("UPDATE {t}execution SET status='cancelled' WHERE id=:id", id=other))
    assert run(lambda: recovery.recover_cancelled_children(child.scope)) == 1
    assert run(lambda: recovery.recover_cancelled_children(child.scope)) == 0
    assert run(lambda: rows("SELECT status,outcome FROM {t}run WHERE id=:id", id=child.run_id)) == [
        {"status": "cancelled", "outcome": None}
    ]
    assert run(
        lambda: rows("SELECT status,result_ref FROM {t}operation WHERE id=:id", id=operation)
    ) == [{"status": "unknown", "result_ref": None}]
    assert (
        run(lambda: rows("SELECT * FROM {t}budget_allocation WHERE id=:id", id=allocation))
        == budget_before
    )
    assert not run(lambda: rows("SELECT id FROM {t}budget_entry WHERE kind='settle'"))
    assert run(
        lambda: rows("SELECT usage_status FROM {t}model_attempt WHERE id=:id", id=attempt)
    ) == [{"usage_status": "unknown"}]
    assert run(lambda: rows("SELECT kind FROM {t}agent_message")) == [{"kind": "task.failed"}]
    assert not run(lambda: rows("SELECT id FROM {t}event WHERE kind='run.completed'"))

"""Real PostgreSQL context content and owner ACL in an isolated Runtime schema."""

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy", reason="requires agent-runtime[storage]")

import sqlalchemy as sa  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker  # noqa: E402
from test_context_runtime import Estimator, request  # noqa: E402
from test_storage_uow import _seed, _seed_item, engine_for, migrate, run  # noqa: E402

from agent_runtime.context_payloads import BlobContextPayloadStore  # noqa: E402
from agent_runtime.contracts.canonical import content_digest  # noqa: E402
from agent_runtime.contracts.failure import AgentRuntimeError  # noqa: E402
from agent_runtime.contracts.references import ResourceRef  # noqa: E402
from agent_runtime.storage.context_blobs import SqlContextBlobBackend  # noqa: E402
from agent_runtime.storage.context_factory import build_sql_context_components  # noqa: E402
from agent_runtime.storage.context_sources import SqlRuntimeSourceReader  # noqa: E402

DSN = os.environ.get("AGENT_RUNTIME_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="AGENT_RUNTIME_TEST_DSN is not configured")


def test_shared_context_blob_and_authorized_sources() -> None:
    schema = f"p03_sources_{uuid.uuid4().hex[:12]}"
    assert migrate("upgrade", schema).status == "applied"
    world = run(lambda: _seed(schema))

    async def scenario() -> None:
        engine = engine_for(schema, search_path=False)
        another_engine = engine_for(schema, search_path=False)
        goal_id = uuid.uuid4()
        plan_id = uuid.uuid4()
        revision_id = uuid.uuid4()
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {schema}.goal_revision "
                        "(id, scope_id, task_id, revision, objective, constraints, focus_refs) "
                        "VALUES (:id, :scope, :task, 2, 'Improve yield', '{}', '[]')"
                    ),
                    {"id": goal_id, "scope": world.scope, "task": world.task_id},
                )
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {schema}.plan "
                        "(id, scope_id, task_id, current_revision) "
                        "VALUES (:id, :scope, :task, 3)"
                    ),
                    {"id": plan_id, "scope": world.scope, "task": world.task_id},
                )
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {schema}.plan_revision "
                        "(id, scope_id, plan_id, revision, steps, public_reason) "
                        "VALUES (:id, :scope, :plan, 3, '[{\"step\":\"analyze\"}]', 'initial')"
                    ),
                    {"id": revision_id, "scope": world.scope, "plan": plan_id},
                )

            refs = tuple(
                ResourceRef.model_validate(
                    {
                        "resourceType": kind,
                        "resourceId": str(identifier),
                        "resourceVersion": version,
                        "scope": {"scopeId": world.scope, "sessionId": str(world.session_id)},
                    }
                )
                for kind, identifier, version in (
                    ("goal_revision", goal_id, "2"),
                    ("plan_revision", revision_id, "3"),
                )
            )
            req = request(refs=0)
            req = req.model_copy(
                update={
                    "actor": req.actor.model_copy(
                        update={"scope_id": world.scope, "actor_id": world.actor_id}
                    ),
                    "session_id": world.session_id,
                    "run_id": world.run_id,
                    "resource_refs": refs,
                }
            )
            blobs = SqlContextBlobBackend(engine, schema)
            reader = SqlRuntimeSourceReader(engine, schema, blobs=blobs)
            sessions = async_sessionmaker(engine, expire_on_commit=False)
            async with sessions() as session, session.begin():
                await reader.authorize_session_in_transaction(session, req.actor, world.session_id)
                for ref in refs:
                    await reader.authorize_resource_in_transaction(
                        session, req.actor, ref, session_id=world.session_id
                    )
                with pytest.raises(AgentRuntimeError):
                    await reader.authorize_resource_in_transaction(
                        session, req.actor, refs[0], session_id=uuid.uuid4()
                    )
                with pytest.raises(AgentRuntimeError):
                    await reader.authorize_resource_in_transaction(
                        session,
                        req.actor,
                        refs[0].model_copy(update={"resource_version": "99"}),
                        session_id=world.session_id,
                    )
            payloads = BlobContextPayloadStore(blobs)
            bundle = build_sql_context_components(engine, schema, estimator=Estimator())
            prepared = await bundle.runtime.prepare(req)
            assert [item.disposition for item in prepared.sources] == ["included", "included"]

            other_bundle = build_sql_context_components(
                another_engine, schema, estimator=Estimator()
            )
            restored, payload = await other_bundle.runtime.load(world.scope, prepared.snapshot_id)
            assert restored == prepared
            assert "Improve yield" in str(payload["sources"])
            assert "analyze" in str(payload["sources"])
            with pytest.raises(AgentRuntimeError):
                await other_bundle.runtime.load("other-scope", prepared.snapshot_id)

            orphan = {"orphan": True}
            await payloads.put(world.scope, orphan, content_digest(orphan))
            answer = {"runId": str(world.run_id), "text": "Durable historical answer"}
            answer_ref = await payloads.put(world.scope, answer, content_digest(answer))
            item_id = await _seed_item(world)
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(f"UPDATE {schema}.timeline_item SET content_ref=:ref WHERE id=:id"),
                    {"ref": answer_ref, "id": item_id},
                )
                await connection.execute(
                    sa.text(f"UPDATE {schema}.context_blob SET created_at = :old"),
                    {"old": datetime.now(UTC) - timedelta(days=2)},
                )
            collected = await SqlContextBlobBackend(engine, schema).collect_unreferenced(
                before=datetime.now(UTC) - timedelta(days=1)
            )
            assert collected == 1
            assert await payloads.get(world.scope, answer_ref) == answer
            assert (await other_bundle.runtime.load(world.scope, prepared.snapshot_id))[
                0
            ] == prepared

            access = bundle.access
            await access.verify(req.actor, prepared)
            wrong_actor = req.actor.model_copy(update={"actor_id": "other-user"})
            with pytest.raises(AgentRuntimeError):
                await reader.read(req.model_copy(update={"actor": wrong_actor}), refs[0])
            stale = refs[0].model_copy(update={"resource_version": "1"})
            with pytest.raises(AgentRuntimeError):
                await reader.read(req, stale)
            with pytest.raises(AgentRuntimeError):
                await reader.authorize_resource(
                    req.actor, refs[0].model_copy(update={"resource_type": "attachment"})
                )

            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(f"UPDATE {schema}.session SET auth_revision = 2 WHERE id = :id"),
                    {"id": world.session_id},
                )
            with pytest.raises(AgentRuntimeError):
                await access.verify(req.actor, prepared)
        finally:
            await another_engine.dispose()
            await engine.dispose()

    try:
        run(scenario)
    finally:

        async def drop() -> None:
            engine = engine_for(schema)
            try:
                async with engine.begin() as connection:
                    await connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            finally:
                await engine.dispose()

        run(drop)

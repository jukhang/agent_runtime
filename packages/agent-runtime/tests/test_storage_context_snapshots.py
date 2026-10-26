"""Real PostgreSQL ContextSnapshot ownership and version tests."""

import asyncio
import os
import uuid

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy", reason="requires agent-runtime[storage]")

import sqlalchemy as sa  # noqa: E402
from test_context_runtime import request, runtime  # noqa: E402
from test_storage_uow import _seed, engine_for, migrate, run  # noqa: E402

from agent_runtime.context_payloads import BlobContextPayloadStore  # noqa: E402
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure  # noqa: E402
from agent_runtime.contracts.references import ResourceRef  # noqa: E402
from agent_runtime.contracts.transactions import ActorContext  # noqa: E402
from agent_runtime.storage.context_access import SqlContextAccessVerifier  # noqa: E402
from agent_runtime.storage.context_blobs import SqlContextBlobBackend  # noqa: E402
from agent_runtime.storage.context_snapshots import SqlContextSnapshotStore  # noqa: E402

DSN = os.environ.get("AGENT_RUNTIME_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="AGENT_RUNTIME_TEST_DSN is not configured")


def test_snapshot_versions_and_authorization_cas() -> None:
    schema = f"p03_context_{uuid.uuid4().hex[:12]}"
    assert migrate("upgrade", schema).status == "applied"
    world = run(lambda: _seed(schema))

    async def scenario() -> None:
        engine = engine_for(schema, search_path=False)
        try:
            store = SqlContextSnapshotStore(engine, schema)
            service, fake_payloads, _ = runtime()
            req = request()
            resource = req.resource_refs[0]
            resource = resource.model_copy(
                update={"scope": resource.scope.model_copy(update={"scope_id": world.scope})}
            )
            req = req.model_copy(
                update={
                    "session_id": world.session_id,
                    "run_id": world.run_id,
                    "actor": req.actor.model_copy(update={"scope_id": world.scope}),
                    "resource_refs": (resource,),
                }
            )
            draft = await service.prepare(req)
            with pytest.raises(AgentRuntimeError):
                await store.save(draft)
            blob_store = BlobContextPayloadStore(SqlContextBlobBackend(engine, schema))
            ref = await blob_store.put(
                draft.scope_id,
                fake_payloads.data[draft.payload_ref],
                draft.content_hash,
            )
            draft = draft.model_copy(update={"payload_ref": ref})
            first, second = await asyncio.gather(
                store.save(draft.model_copy(update={"snapshot_id": uuid.uuid4()})),
                store.save(draft.model_copy(update={"snapshot_id": uuid.uuid4()})),
            )
            assert {first.context_version, second.context_version} == {"1", "2"}
            assert await store.get(world.scope, first.snapshot_id) == first

            class Resources:
                denied = False
                checked = 0

                async def authorize_session(
                    self, actor: ActorContext, session_id: uuid.UUID
                ) -> None:
                    assert actor == req.actor
                    assert session_id == world.session_id

                async def authorize_resource(
                    self, actor: ActorContext, resource: ResourceRef
                ) -> None:
                    self.checked += 1
                    if self.denied:
                        raise runtime_failure(
                            "FORBIDDEN", category="authorization", public_message="Revoked."
                        )

            resources = Resources()
            access = SqlContextAccessVerifier(engine, schema, resources=resources)
            await access.verify(req.actor, first)
            assert resources.checked == 1
            resources.denied = True
            with pytest.raises(AgentRuntimeError):
                await access.verify(req.actor, first)
            resources.denied = False
            with pytest.raises(AgentRuntimeError):
                await store.get("another-scope", first.snapshot_id)
            with pytest.raises(AgentRuntimeError):
                await store.save(draft.model_copy(update={"session_id": uuid.uuid4()}))
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(f"UPDATE {schema}.session SET auth_revision = 2 WHERE id = :id"),
                    {"id": world.session_id},
                )
            with pytest.raises(AgentRuntimeError):
                await store.save(draft.model_copy(update={"snapshot_id": uuid.uuid4()}))
            with pytest.raises(AgentRuntimeError):
                await access.verify(req.actor, first)
        finally:
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

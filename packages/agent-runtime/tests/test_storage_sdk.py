"""Actual SQL SDK ingress, bounded snapshots, streaming authorization and pruning safety."""

from contextlib import aclosing

import pytest
from test_storage_uow import DSN, engine_for, run, schema  # noqa: F401

from agent_runtime import Agent, Runtime
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.references import CapabilityKey, CapabilityRef
from agent_runtime.contracts.requests import StreamRequest, SubmitRequest, TextInput
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.admission import AdmissionAgent, SqlAdmission
from agent_runtime.storage.agent_install import SqlAgentInstaller
from agent_runtime.storage.run_backend import SqlRunBackend
from agent_runtime.storage.sessions import SqlSessionStore
from agent_runtime.storage.uow.schema import scoped

pytestmark = pytest.mark.skipif(not DSN, reason="requires isolated PostgreSQL")


def test_independent_sdk_ingress_snapshot_scope_stream_and_retention(schema, monkeypatch):  # noqa: F811
    async def exercise():
        engine = engine_for(schema, search_path=False)
        actor = ActorContext(
            scope_id="sdk", actor_id="owner", auth_revision="1", grants_ref="owner"
        )
        capability = CapabilityRef(
            key=CapabilityKey(namespace="sdk", key="assistant"), revision="1"
        )
        agent = Agent(id="assistant", name="Assistant")
        revision = await SqlAgentInstaller(engine, schema, namespace="sdk").install(
            "sdk", capability, agent
        )
        installed = AdmissionAgent(capability, revision, agent, 4096)

        async def identity(db, current):
            assert current == actor

        async def authorize(db, current, request, binding):
            assert current == actor and binding == installed
            return True

        try:
            sessions = SqlSessionStore(engine, schema, authorize=identity)
            session = await sessions.create(actor, title="SDK acceptance")
            assert (await sessions.list(actor))[0].id == session.id
            backend = SqlRunBackend(engine, schema)
            assert (await backend.snapshot(actor, session.id)).snapshot_seq == "0"

            async def public_only(current, row, restricted):
                return not restricted

            denied_analysis = SqlRunBackend(
                engine, schema, include_analysis=True, authorizer=public_only
            )
            with pytest.raises(AgentRuntimeError) as denied:
                await denied_analysis.snapshot(actor, session.id)
            assert denied.value.failure.code == "FORBIDDEN"
            assert (
                await denied_analysis.read_page(
                    actor, StreamRequest(session_id=session.id, after_seq="0", last_event_id=None)
                )
            ).kind == "resync_required"
            runtime = Runtime(
                admission=SqlAdmission(
                    engine, schema, agents={("sdk", capability): installed}, authorize=authorize
                ),
                backend=backend,
            )
            request = SubmitRequest(
                session_id=session.id,
                agent=capability,
                idempotency_key="one",
                inputs=(TextInput(kind="text", text="question"),),
            )
            submitted = await runtime.submit(actor, request)
            assert (await runtime.submit(actor, request)).accepted == submitted.accepted
            snapshot = await backend.snapshot(actor, session.id)
            assert snapshot.items[0].channels[0].text == "question"
            assert snapshot.runs[0].status == "queued"
            with monkeypatch.context() as patch:

                async def reject_history(*args, **kwargs):
                    raise AssertionError("Waiting must not reconstruct Session history")

                patch.setattr(backend, "_load", reject_history)
                with pytest.raises(AgentRuntimeError) as waiting:
                    await submitted.handle.result(wait_timeout=0.3)
                assert waiting.value.failure.code == "WAIT_TIMEOUT"
            async with aclosing(
                backend.events(
                    actor, StreamRequest(session_id=session.id, after_seq="0", last_event_id=None)
                )
            ) as stream:
                assert (await anext(stream)).kind == "turn.accepted"
                assert (await anext(stream)).kind == "cursor.advanced"
            for other in (
                actor.model_copy(update={"actor_id": "other"}),
                actor.model_copy(update={"scope_id": "other"}),
                actor.model_copy(update={"auth_revision": "2"}),
            ):
                with pytest.raises(AgentRuntimeError) as error:
                    await backend.snapshot(other, session.id)
                assert error.value.failure.code == "FORBIDDEN"
            with pytest.raises(AgentRuntimeError) as error:
                await SqlRunBackend(engine, schema, max_snapshot_bytes=1).snapshot(
                    actor, session.id
                )
            assert error.value.failure.code == "VERSION_CONFLICT"
            archived = await sessions.create(actor, title="Archived session")
            archived_request = request.model_copy(update={"session_id": archived.id})
            await runtime.submit(actor, archived_request)
            before_pruning = await backend.materialize(actor, archived.id)
            async with engine.begin() as db:
                await db.execute(
                    scoped("DELETE FROM {t}outbox WHERE scope_id=:scope", schema), {"scope": "sdk"}
                )
                await db.execute(
                    scoped("DELETE FROM {t}event WHERE scope_id=:scope", schema), {"scope": "sdk"}
                )
            with pytest.raises(AgentRuntimeError) as error:
                await backend.snapshot(actor, session.id)
            assert error.value.failure.code == "VERSION_CONFLICT"
            restored = await backend.snapshot(actor, archived.id)
            assert restored.items == before_pruning.items
            assert restored.snapshot_seq == before_pruning.snapshot_seq
            assert restored.retained_from_seq == str(int(restored.snapshot_seq) + 1)
            await runtime.submit(
                actor,
                archived_request.model_copy(
                    update={
                        "idempotency_key": "tail",
                        "inputs": (TextInput(kind="text", text="follow-up"),),
                    }
                ),
            )
            combined = await backend.snapshot(actor, archived.id)
            assert [item.channels[0].text for item in combined.items] == ["question", "follow-up"]
            from datetime import UTC, datetime, timedelta

            from agent_runtime.storage.context_blobs import SqlContextBlobBackend

            await SqlContextBlobBackend(engine, schema).collect_unreferenced(
                before=datetime.now(UTC) + timedelta(minutes=1)
            )
            assert (await backend.snapshot(actor, archived.id)).items == combined.items
            from agent_runtime.contracts.requests import ControlRequest

            cancelled = await backend.control(
                actor,
                session.id,
                ControlRequest(
                    run_id=submitted.accepted.run_id,
                    kind="cancel",
                    idempotency_key="cancel-without-history",
                    expected_version="0",
                ),
            )
            assert cancelled.status == "accepted"
        finally:
            await engine.dispose()

    run(exercise)

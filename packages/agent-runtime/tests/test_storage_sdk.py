"""Actual SQL SDK ingress, bounded snapshots, streaming authorization and pruning safety."""

from contextlib import aclosing

import pytest
from test_storage_uow import (  # noqa: F401
    DSN,
    _seed_attempt,
    _seed_item,
    actor,
    engine_for,
    lease,
    make_request,
    run,
    schema,
    state_guard,
    world,
)

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


@pytest.mark.parametrize("page_events", [7, 256])
def test_snapshot_pages_large_journal_and_monotonic_cache(world, monkeypatch, page_events):  # noqa: F811
    from datetime import UTC, datetime

    from agent_runtime.contracts.payloads import ItemStarted
    from agent_runtime.contracts.provider import AnalysisDelta, ContentDelta
    from agent_runtime.contracts.state_events import (
        ItemStartedDraft,
        MainEventAuthority,
        StateEventBatch,
    )
    from agent_runtime.storage.journal import SqlJournal
    from agent_runtime.storage.model_events import SqlModelEventSink

    item = run(lambda: _seed_item(world))
    attempt = run(lambda: _seed_attempt(world))

    async def exercise():
        engine = engine_for(world.schema, search_path=False)
        current = actor(world=world)
        request = make_request().model_copy(
            update={
                "scope_id": world.scope,
                "session_id": world.session_id,
                "run_id": world.run_id,
                "attempt_id": attempt,
            }
        )
        sink = SqlModelEventSink(
            SqlJournal(engine, world.schema, current),
            lease=lease(world),
            turn_id=world.turn_id,
            attempt_id=attempt,
            item_id=item,
            item_revision="1",
            trace_id="snapshot-paging",
        )
        backend = SqlRunBackend(
            engine,
            world.schema,
            include_analysis=True,
            max_snapshot_events=page_events,
            max_snapshot_bytes=12000,
        )
        try:
            await SqlJournal(engine, world.schema, current).append_state(
                StateEventBatch(
                    authority=MainEventAuthority(kind="main", guard=state_guard(world)),
                    session_id=world.session_id,
                    trace_id="snapshot-start",
                    events=(
                        ItemStartedDraft(
                            kind="item.started",
                            producer_key=f"snapshot-start:{item}",
                            occurred_at=datetime.now(UTC),
                            turn_id=world.turn_id,
                            run_id=world.run_id,
                            parent_run_id=None,
                            step_id=None,
                            item_id=item,
                            model_attempt_id=attempt,
                            payload=ItemStarted(
                                item_type="message",
                                role="assistant",
                                channels=("content", "analysis"),
                                item_revision="1",
                            ),
                        ),
                    ),
                )
            )
            events = tuple(
                cls(attempt_id=attempt, sequence=str(i), kind=kind, text=text)
                for i in range(60)
                for cls, kind, text in [
                    (ContentDelta, "content.delta", "x")
                    if i % 2 == 0
                    else (AnalysisDelta, "analysis.delta", "y")
                ]
            )
            await sink.record_many(request, events)
            async with engine.connect() as db:
                cost = await db.scalar(
                    scoped(
                        "SELECT sum(octet_length(payload::text)+512) FROM {t}event "
                        "WHERE scope_id=:scope AND session_id=:session",
                        world.schema,
                    ),
                    {"scope": world.scope, "session": world.session_id},
                )
            assert cost > 12000
            with pytest.raises(AgentRuntimeError) as oversized_event:
                await SqlRunBackend(engine, world.schema, max_snapshot_bytes=200).snapshot(
                    current, world.session_id
                )
            assert oversized_event.value.failure.code == "SNAPSHOT_TOO_LARGE"
            public_without_cache = await SqlRunBackend(
                engine,
                world.schema,
                max_snapshot_events=1,
                max_snapshot_bytes=12000,
            ).snapshot(current, world.session_id)
            assert public_without_cache.items[0].channels[0].text == "x" * 30
            assert len(public_without_cache.items[0].channels) == 1
            first = await backend.snapshot(current, world.session_id)
            assert first.snapshot_seq == "61" and first.items
            assert {c.name: c.text for c in first.items[0].channels} == {
                "content": "x" * 30,
                "analysis": "y" * 30,
            }
            old_snapshot, old_projection, _ = await backend._load(current, world.session_id)

            async def cache_row():
                async with engine.connect() as db:
                    return (
                        await db.execute(
                            scoped(
                                "SELECT applied_seq,projection_revision FROM {t}projection "
                                "WHERE scope_id=:scope AND session_id=:session",
                                world.schema,
                            ),
                            {"scope": world.scope, "session": world.session_id},
                        )
                    ).one()

            before = await cache_row()
            observed = []
            original = backend._replay

            async def tracked(*args, **kwargs):
                observed.append((kwargs["after"], kwargs["watermark"]))
                await original(*args, **kwargs)

            monkeypatch.setattr(backend, "_replay", tracked)
            assert (await backend.snapshot(current, world.session_id)).items == first.items
            assert observed == [(61, 61)] and await cache_row() == before
            public = await SqlRunBackend(
                engine,
                world.schema,
                max_snapshot_events=3,
                max_snapshot_bytes=12000,
            ).snapshot(current, world.session_id)
            assert [c.name for c in public.items[0].channels] == ["content"]
            assert await cache_row() == before
            await sink.record(
                request,
                ContentDelta(
                    attempt_id=attempt,
                    sequence="60",
                    kind="content.delta",
                    text="z",
                ),
            )
            latest = await backend.snapshot(current, world.session_id)
            assert latest.snapshot_seq == "62"
            await backend._store_projection(current, old_snapshot, old_projection)
            assert (await cache_row()).applied_seq == 62
            assert (await backend.snapshot(current, world.session_id)).items == latest.items
            with pytest.raises(AgentRuntimeError) as error:
                await SqlRunBackend(engine, world.schema, max_snapshot_bytes=1).snapshot(
                    current, world.session_id
                )
            assert error.value.failure.code == "SNAPSHOT_TOO_LARGE"
            await sink.record(
                request,
                ContentDelta(
                    attempt_id=attempt, sequence="61", kind="content.delta", text="a" * 13000
                ),
            )
            with pytest.raises(AgentRuntimeError) as failed_refresh:
                await backend.snapshot(current, world.session_id)
            assert failed_refresh.value.failure.code == "SNAPSHOT_TOO_LARGE"
            assert (await cache_row()).applied_seq == 62
        finally:
            await engine.dispose()

    run(exercise)


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
            assert error.value.failure.code == "SNAPSHOT_TOO_LARGE"
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

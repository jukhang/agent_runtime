"""Real PostgreSQL admission transactions; no SQLite concurrency substitutes."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

import pytest

pytest.importorskip("sqlalchemy", reason="requires agent-runtime[storage]")

import sqlalchemy as sa
from test_storage_uow import DSN, engine_for, run, schema  # noqa: F401

from agent_runtime.agent import Agent
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.context import ContextBudget, ContextRequest
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.journal import JournalPage
from agent_runtime.contracts.references import (
    CapabilityKey,
    CapabilityRef,
    ResourceRef,
    ResourceScope,
)
from agent_runtime.contracts.requests import StreamRequest, SubmitRequest, TextInput
from agent_runtime.contracts.scheduler import ClaimedWork, ClaimRequest, WorkerCapabilities
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.admission import AdmissionAgent, SqlAdmission
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.context_sources import SqlRuntimeSourceReader
from agent_runtime.storage.journal import SqlJournal, SqlJournalReader, owner_read_authorizer
from agent_runtime.storage.main_run_loader import MainAgentBinding, SqlMainRunLoader
from agent_runtime.storage.scheduler import SqlScheduler
from agent_runtime.worker import LeaseHandle

pytestmark = pytest.mark.skipif(not DSN, reason="AGENT_RUNTIME_TEST_DSN is not configured")


@pytest.mark.parametrize(
    "case", ["replay", "parallel", "changed", "denied", "rollback", "claim", "read_race"]
)
def test_sql_admission_atomic_ingress(schema: str, case: str) -> None:  # noqa: F811
    async def exercise() -> None:
        engine = engine_for(schema, search_path=False)
        session_id, agent_id, revision_id = uuid4(), uuid4(), uuid4()
        actor = ActorContext(
            scope_id="scope-a", actor_id="user-a", auth_revision="1", grants_ref="host-auth:1"
        )
        ref = CapabilityRef(key=CapabilityKey(namespace="local", key="assistant"), revision="1")
        agent = Agent(id="assistant", name="Assistant")
        binding = AdmissionAgent(ref, revision_id, agent, 100000)

        async def authorize(*args: object) -> bool:
            return case != "denied"

        admission = SqlAdmission(
            engine, schema, agents={(actor.scope_id, ref): binding}, authorize=authorize
        )
        request = SubmitRequest(
            session_id=session_id,
            agent=ref,
            idempotency_key="request-a",
            inputs=(TextInput(kind="text", text="Explain rain."),),
        )
        try:
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f"INSERT INTO {schema}.session "
                        "(id,scope_id,owner_actor_id,title_source,event_seq,auth_revision) "
                        "VALUES (:id,'scope-a','user-a','none',0,1)"
                    ),
                    {"id": session_id},
                )
                await db.execute(
                    sa.text(
                        f"INSERT INTO {schema}.agent (id,scope_id,key) "
                        "VALUES (:id,'scope-a','assistant')"
                    ),
                    {"id": agent_id},
                )
                await db.execute(
                    sa.text(
                        f"INSERT INTO {schema}.agent_revision "
                        "(id,scope_id,agent_id,revision,manifest,content_hash) "
                        "VALUES (:id,'scope-a',:agent,1,CAST(:manifest AS jsonb),:hash)"
                    ),
                    {
                        "id": revision_id,
                        "agent": agent_id,
                        "manifest": agent.model_dump_json(by_alias=True),
                        "hash": content_digest(agent.model_dump(mode="json", by_alias=True)),
                    },
                )
            if case == "denied":
                with pytest.raises(AgentRuntimeError):
                    await admission.accept(actor, request)
                receipts = []
            elif case == "rollback":
                original = SqlJournal.append_state_in_transaction

                async def fail_after_publication(self, db, batch):
                    await original(self, db, batch)
                    raise RuntimeError("outbox unavailable")

                with patch.object(
                    SqlJournal,
                    "append_state_in_transaction",
                    new=fail_after_publication,
                ):
                    with pytest.raises(RuntimeError, match="outbox unavailable"):
                        await admission.accept(actor, request)
                receipts = []
            elif case == "parallel":
                first = await asyncio.gather(*(admission.accept(actor, request) for _ in range(6)))
                assert all(receipt == first[0] for receipt in first)
                rest = await asyncio.gather(
                    *(
                        admission.accept(actor, request.model_copy(update={"idempotency_key": key}))
                        for key in ("request-b", "request-c")
                    )
                )
                receipts = [first[0], *rest]
                assert sorted(int(receipt.accepted_seq) for receipt in receipts) == [1, 2, 3]
            else:
                receipt = await admission.accept(actor, request)
                receipts = [receipt]
                assert await admission.accept(actor, request) == receipt
                if case == "read_race":

                    async def append_after_watermark(current_actor, session, restricted):
                        second = await admission.accept(
                            actor, request.model_copy(update={"idempotency_key": "concurrent"})
                        )
                        receipts.append(second)
                        return await owner_read_authorizer(current_actor, session, restricted)

                    page = await SqlJournalReader(
                        engine, schema, authorizer=append_after_watermark
                    ).read_public(
                        actor,
                        StreamRequest(session_id=session_id, after_seq="0", last_event_id=None),
                    )
                    assert isinstance(page, JournalPage)
                    assert page.cursor == "1" and len(page.events) == 1
                    next_page = await SqlJournalReader(engine, schema).read_public(
                        actor,
                        StreamRequest(session_id=session_id, after_seq="1", last_event_id=None),
                    )
                    assert isinstance(next_page, JournalPage)
                    assert next_page.cursor == "2" and len(next_page.events) == 1
                if case == "changed":
                    with pytest.raises(AgentRuntimeError) as error:
                        await admission.accept(
                            actor,
                            request.model_copy(
                                update={
                                    "inputs": (TextInput(kind="text", text="Different request."),)
                                }
                            ),
                        )
                    assert error.value.failure.code == "VERSION_CONFLICT"
                    with pytest.raises(AgentRuntimeError):
                        await admission.accept(
                            actor.model_copy(update={"actor_id": "other"}), request
                        )
                    wrong = SqlAdmission(
                        engine,
                        schema,
                        authorize=authorize,
                        agents={
                            (actor.scope_id, ref): replace(
                                binding,
                                max_tokens=50,
                                agent=agent.model_copy(update={"instructions": ("changed",)}),
                            )
                        },
                    )
                    with pytest.raises(AgentRuntimeError):
                        await wrong.accept(
                            actor, request.model_copy(update={"idempotency_key": "new"})
                        )
                if case == "claim":
                    claim = await SqlScheduler(engine, schema).claim(
                        ClaimRequest(
                            scope_id=actor.scope_id,
                            lease_seconds=60,
                            worker=WorkerCapabilities(
                                worker_id="test-worker", runtime_revision=ref, capabilities=(ref,)
                            ),
                        )
                    )
                    assert isinstance(claim, ClaimedWork)
                    assert claim.lease.run_id == receipt.run_id

                    class Setup:
                        async def resolve(self, *args):
                            raise AssertionError("loading must not invoke the model")

                    loaded = await SqlMainRunLoader(
                        engine,
                        schema,
                        actor=actor,
                        agents={revision_id: MainAgentBinding(ref, Setup())},
                    ).load(claim, LeaseHandle(claim.lease))
                    assert loaded.state.state_version == "0"
                    assert loaded.inputs.applied_seq == "0"
                    source = ResourceRef(
                        resource_type="turn_input",
                        resource_id=str(receipt.turn_id),
                        resource_version="0",
                        scope=ResourceScope(
                            scope_id=actor.scope_id,
                            session_id=str(session_id),
                            owner_id=actor.actor_id,
                        ),
                    )
                    reader = SqlRuntimeSourceReader(
                        engine, schema, blobs=SqlContextBlobBackend(engine, schema)
                    )
                    context = ContextRequest(
                        actor=actor,
                        session_id=session_id,
                        run_id=receipt.run_id,
                        profile=ref,
                        model=ref,
                        prompt=ref,
                        tools=(),
                        resource_refs=(source,),
                        budget=ContextBudget(
                            allocation_id=claim.budget_allocation_id,
                            max_input_tokens=4096,
                            reserved_output_tokens=1024,
                        ),
                    )
                    actual = await reader.read(context, source)
                    assert "Explain rain." in actual.content
                    with pytest.raises(AgentRuntimeError):
                        await reader.read(
                            context.model_copy(update={"session_id": uuid4()}), source
                        )
                    with pytest.raises(AgentRuntimeError):
                        await reader.authorize_resource(
                            actor.model_copy(update={"auth_revision": "2"}), source
                        )
            async with engine.connect() as db:
                for table in (
                    "turn",
                    "run",
                    "task",
                    "checkpoint",
                    "budget_allocation",
                    "capability_grant",
                    "work_item",
                    "timeline_item",
                    "event",
                    "outbox",
                ):
                    count = (
                        await db.execute(sa.text(f"SELECT count(*) FROM {schema}.{table}"))
                    ).scalar_one()
                    assert count == len(receipts), (table, count)
                count = (
                    await db.execute(sa.text(f"SELECT count(*) FROM {schema}.context_blob"))
                ).scalar_one()
                assert count == 2 * len(receipts)
                if case != "claim":
                    owners = (
                        (
                            await db.execute(
                                sa.text(f"SELECT owner_run_id FROM {schema}.session_slot")
                            )
                        )
                        .scalars()
                        .all()
                    )
                    assert all(owner is None for owner in owners)
                watermark = (
                    await db.execute(
                        sa.text(f"SELECT event_seq FROM {schema}.session WHERE id=:id"),
                        {"id": session_id},
                    )
                ).scalar_one()
                assert watermark == len(receipts)
            assert (
                await SqlContextBlobBackend(engine, schema).collect_unreferenced(
                    before=datetime.now(UTC) + timedelta(days=1)
                )
                == 0
            )
        finally:
            await engine.dispose()

    run(exercise)

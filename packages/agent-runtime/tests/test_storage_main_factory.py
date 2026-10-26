"""Real Provider + installed configuration + SQL claim + durable replay integration."""

import asyncio
import json
import os
import time
from datetime import UTC, datetime
from uuid import uuid4

import pytest

pytest.importorskip("sqlalchemy")
pytest.importorskip("httpx")

import httpx
import sqlalchemy as sa
from test_storage_agent_install import capability
from test_storage_uow import DSN, engine_for, run, schema  # noqa: F401

from agent_runtime.agent import Agent
from agent_runtime.contracts.journal import JournalPage
from agent_runtime.contracts.model import ModelConfig, ProviderCapabilities
from agent_runtime.contracts.policy import PolicyAllow
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.requests import StreamRequest, SubmitRequest, TextInput
from agent_runtime.contracts.scheduler import WorkerCapabilities
from agent_runtime.contracts.tools import FullResult, ToolCompleted, ToolSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.work_claims import WorkClaimRequest
from agent_runtime.policy_validation import policy_input_digest
from agent_runtime.providers.openai_compatible import OpenAIAdapterProfile, ProviderConnection
from agent_runtime.storage.admission import AdmissionAgent, SqlAdmission
from agent_runtime.storage.agent_install import SqlAgentInstaller
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.context_sources import SqlRuntimeSourceReader
from agent_runtime.storage.host_worker import SqlClaimActorResolver, SqlClaimMainHandler
from agent_runtime.storage.journal import SqlJournal, SqlJournalReader
from agent_runtime.storage.main_factory import (
    MainAgentProfile,
    MainHostPorts,
    SqlMainHandlerFactory,
)
from agent_runtime.storage.recovery import SqlRecovery
from agent_runtime.storage.scheduler import SqlScheduler
from agent_runtime.storage.subagents import SqlSubagentRuntime
from agent_runtime.tool_names import provider_tool_name
from agent_runtime.worker import WorkerConfig, WorkerRunner

pytestmark = pytest.mark.skipif(
    not DSN or os.environ.get("AGENT_RUNTIME_LIVE_PROVIDER") != "1",
    reason="requires isolated PostgreSQL and explicit real Provider opt-in",
)


@pytest.mark.parametrize("with_tool", [False, True], ids=["conversation", "tool-observation"])
def test_main_factory_real_qwen_conversation(schema: str, with_tool: bool) -> None:  # noqa: F811
    async def exercise() -> None:
        engine = engine_for(schema, search_path=False)
        actor = ActorContext(
            scope_id="scope-a", actor_id="owner", auth_revision="1", grants_ref="host:owner"
        )
        ref, model_ref = capability("assistant"), capability("model")
        tool_ref = capability("read-counter")
        agent = Agent(
            id="assistant",
            name="Assistant",
            model=model_ref,
            instructions=("Answer the user's question accurately and concisely.",),
            tools=(tool_ref,) if with_tool else (),
        )
        model = ModelConfig(
            ref=model_ref,
            connection=capability("connection"),
            model_name=os.environ.get("MODEL_NAME", "qwen3.6-27b"),
            max_output_tokens=4096,
            thinking="disabled",
            provider_options={"chat_template_kwargs": {"enable_thinking": False}},
        )
        session_id = uuid4()
        reader = SqlRuntimeSourceReader(engine, schema, blobs=SqlContextBlobBackend(engine, schema))
        predicate = capability("message")
        tool_calls = []

        class ReadCounter:
            """Trusted read-only test port; not a substitute for the P04 ToolHost."""

            async def definition(self, pinned):
                assert pinned == tool_ref
                return {
                    "type": "function",
                    "function": {
                        "name": provider_tool_name(pinned),
                        "description": "Read the current counter value from the trusted registry.",
                        "parameters": {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": False,
                        },
                    },
                }

            async def get(self, call):
                assert call.tool == tool_ref and call.arguments == {}
                return ToolSpec(
                    ref=tool_ref,
                    description="Read counter",
                    source=tool_ref,
                    kind="read",
                    side_effect="none",
                    input_schema=tool_ref,
                    output_schema=tool_ref,
                    permissions=(),
                    approval_policy=tool_ref,
                    idempotency="read_only",
                    timeout_seconds=30,
                    concurrency_group="counter",
                    max_concurrency=1,
                    cancellation="stop",
                    public_result_policy="full",
                )

            async def resources(self, invocation, spec):
                assert (
                    invocation.actor.scope_id,
                    invocation.actor.actor_id,
                    invocation.actor.auth_revision,
                ) == (actor.scope_id, actor.actor_id, actor.auth_revision)
                assert spec.ref == tool_ref
                async with engine.connect() as db:
                    turn_id = (
                        await db.execute(
                            sa.text(f"SELECT id FROM {schema}.turn WHERE primary_run_id=:run"),
                            {"run": invocation.run_id},
                        )
                    ).scalar_one()
                return (
                    ResourceRef(
                        resource_type="turn_input",
                        resource_id=str(turn_id),
                        resource_version="0",
                        scope=ResourceScope(
                            scope_id=actor.scope_id,
                            session_id=str(session_id),
                            owner_id=actor.actor_id,
                        ),
                    ),
                )

            async def evaluate(self, request):
                return PolicyAllow(
                    decision="allow",
                    decision_id=uuid4(),
                    policy=request.policy,
                    input_digest=policy_input_digest(request),
                    evidence_refs=request.resources,
                    evaluated_at=datetime.now(UTC),
                    expires_at=request.invocation.deadline_at,
                    grant_ref="trusted-counter-read",
                )

            async def execute(self, invocation, grant):
                assert grant.decision == "allow" and invocation.call.arguments == {}
                tool_calls.append(invocation.call.tool_call_id)
                return ToolCompleted(
                    status="completed",
                    tool_call_id=invocation.call.tool_call_id,
                    run_id=invocation.run_id,
                    operation_id=None,
                    effect="none",
                    effect_refs=(),
                    result=FullResult(policy="full", data={"counter": 5837}),
                )

        tool = ReadCounter()

        class NoEffects:
            def __getattr__(self, name):
                raise AssertionError(f"tool-free Agent unexpectedly requested {name}")

        class Ports:
            async def open(self, current, lease):
                assert current.actor_id == actor.actor_id
                return MainHostPorts(
                    tool if with_tool else NoEffects(),
                    tool if with_tool else NoEffects(),
                    tool if with_tool else NoEffects(),
                    tool if with_tool else NoEffects(),
                    tool if with_tool else NoEffects(),
                    reader,
                    SqlSubagentRuntime(
                        engine,
                        schema,
                        actor=current,
                        journal=SqlJournal(engine, schema, current),
                        resources=reader,
                        policies={},
                        message_predicate=predicate,
                        verifier=NoEffects(),
                    ),
                )

        class Estimator:
            def count(self, text):
                return len(text.encode("utf-8"))  # conservative UTF-8 upper bound

        async def connections(request):
            return ProviderConnection(
                base_url=os.environ["BASE_URL"], api_key=os.environ["API_KEY"]
            )

        async def capabilities(request):
            return ProviderCapabilities(
                model=model_ref,
                streaming="supported",
                tool_calls="supported",
                structured_output="supported",
                thinking_control="supported",
                tools_with_structured_output="unknown",
                tools_with_thinking="supported",
                usage="supported",
                cancellation="supported",
                context_window=262144,
                max_output_tokens=65536,
                allowed_option_keys=("chat_template_kwargs",),
            )

        async def authorize(*args):
            return True

        async def unexpected(work):
            raise AssertionError("unexpected lane")

        errors = []

        async def fail(work, error):
            settlement = await SqlRecovery(engine, schema).handle_worker_failure(work, error)
            if settlement.disposition == "fenced":
                async with engine.connect() as db:
                    status = (
                        await db.execute(
                            sa.text(f"SELECT status FROM {schema}.work_item WHERE id=:id"),
                            {"id": work.lease.work_id},
                        )
                    ).scalar_one()
                if status == "completed":
                    return  # Renewal can race the handler's committed release and cleanup.
            errors.append(error)
            raise error

        try:
            revision_id = await SqlAgentInstaller(engine, schema, namespace="host").install(
                actor.scope_id, ref, agent
            )
            installed = AdmissionAgent(ref, revision_id, agent, 100000)
            admission = SqlAdmission(
                engine, schema, authorize=authorize, agents={(actor.scope_id, ref): installed}
            )
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f"INSERT INTO {schema}.session "
                        "(id,scope_id,owner_actor_id,title_source,event_seq,auth_revision) "
                        "VALUES (:id,'scope-a','owner','none',0,1)"
                    ),
                    {"id": session_id},
                )
            async with httpx.AsyncClient(trust_env=False, timeout=120) as client:
                factory = SqlMainHandlerFactory(
                    engine,
                    schema,
                    client=client,
                    estimator=Estimator(),
                    agents={
                        (actor.scope_id, revision_id): MainAgentProfile(
                            installed, model, capability("profile"), capability("prompt")
                        )
                    },
                    ports=Ports(),
                    connections=connections,
                    capabilities=capabilities,
                    adapter=OpenAIAdapterProfile.qwen(),
                    allowed_predicates=(predicate,),
                    message_predicate=predicate,
                )
                worker = WorkerRunner(
                    SqlScheduler(engine, schema),
                    WorkClaimRequest(
                        scope_id=actor.scope_id,
                        lease_seconds=60,
                        lanes=("main", "control"),
                        worker=WorkerCapabilities(
                            worker_id="real-host-worker", runtime_revision=ref, capabilities=(ref,)
                        ),
                    ),
                    unexpected,
                    config=WorkerConfig(concurrency=1, idle_sleep_seconds=0.05),
                    error_sink=fail,
                    main_handler=SqlClaimMainHandler(
                        SqlClaimActorResolver(engine, schema), factory
                    ),
                )
                queued = []
                questions = (
                    (
                        ("first", "What is 17 multiplied by 23? Give the number only.", "391"),
                        ("followup", "Add 9 to your previous result. Give the number only.", "400"),
                    )
                    if not with_tool
                    else (
                        (
                            "first",
                            "Read the current counter with your tool, then add 9. "
                            "Do not guess the counter. Give the resulting number only.",
                            "5846",
                        ),
                    )
                )
                for key, question, expected in questions:
                    receipt = await admission.accept(
                        actor,
                        SubmitRequest(
                            session_id=session_id,
                            agent=ref,
                            idempotency_key=key,
                            inputs=(TextInput(kind="text", text=question),),
                        ),
                    )
                    queued.append((receipt, expected))
                task = asyncio.create_task(worker.serve())
                try:
                    for receipt, expected in queued:
                        deadline = time.monotonic() + 180
                        saw_delta = False
                        cursor = "0"
                        while time.monotonic() < deadline:
                            if task.done():
                                task.result()
                                raise AssertionError("worker stopped before terminal")
                            page = await SqlJournalReader(engine, schema).read_public(
                                actor,
                                StreamRequest(
                                    session_id=session_id, after_seq=cursor, last_event_id=None
                                ),
                            )
                            assert isinstance(page, JournalPage)
                            cursor = page.cursor
                            saw_delta |= any(
                                e.kind == "assistant.delta" and e.run_id == receipt.run_id
                                for e in page.events
                            )
                            async with engine.connect() as db:
                                status = (
                                    await db.execute(
                                        sa.text(f"SELECT status FROM {schema}.run WHERE id=:id"),
                                        {"id": receipt.run_id},
                                    )
                                ).scalar_one()
                                if status in {"completed", "failed", "cancelled"}:
                                    assert status == "completed"
                                    final_ids = (
                                        await db.execute(
                                            sa.text(
                                                "SELECT payload->'finalItemIds' "
                                                f"FROM {schema}.event "
                                                "WHERE run_id=:run AND kind='run.completed'"
                                            ),
                                            {"run": receipt.run_id},
                                        )
                                    ).scalar_one()
                                    assert len(final_ids) == 1
                                    answer = (
                                        await db.execute(
                                            sa.text(
                                                f"SELECT b.content FROM {schema}.timeline_item i "
                                                f"JOIN {schema}.context_blob b "
                                                "ON b.scope_id=i.scope_id "
                                                "AND b.blob_key=i.content_ref "
                                                "WHERE i.id=:id"
                                            ),
                                            {"id": final_ids[0]},
                                        )
                                    ).scalar_one()
                                    stored = json.loads(bytes(answer))
                                    assert stored["runId"] == str(receipt.run_id)
                                    assert stored["text"].strip().strip("`") == expected
                                    assert saw_delta
                                    break
                            await asyncio.sleep(0.1)
                        else:
                            async with engine.connect() as db:
                                facts = (
                                    (
                                        await db.execute(
                                            sa.text(
                                                f"SELECT status,phase,waiting_spec,failure "
                                                f"FROM {schema}.run WHERE id=:run"
                                            ),
                                            {"run": receipt.run_id},
                                        )
                                    )
                                    .mappings()
                                    .one()
                                )
                                attempts = (
                                    await db.execute(
                                        sa.text(
                                            "SELECT status,finish_reason "
                                            f"FROM {schema}.model_attempt "
                                            "WHERE run_id=:run ORDER BY started_at"
                                        ),
                                        {"run": receipt.run_id},
                                    )
                                ).all()
                                output = (
                                    (
                                        await db.execute(
                                            sa.text(
                                                f"SELECT payload->>'text' FROM {schema}.event "
                                                "WHERE run_id=:run AND kind='assistant.delta' "
                                                "ORDER BY session_seq"
                                            ),
                                            {"run": receipt.run_id},
                                        )
                                    )
                                    .scalars()
                                    .all()
                                )
                            raise AssertionError(
                                f"real host did not reach terminal: {dict(facts)}; "
                                f"attempts={attempts}; calls={len(tool_calls)}; output={output}"
                            )
                    assert not errors
                    if with_tool:
                        assert len(tool_calls) == 1
                        async with engine.connect() as db:
                            assert (
                                await db.execute(
                                    sa.text(
                                        f"SELECT count(*) FROM {schema}.tool_call "
                                        "WHERE status='completed'"
                                    )
                                )
                            ).scalar_one() == 1
                finally:
                    worker.request_drain()
                    await task
        finally:
            await engine.dispose()

    run(exercise)

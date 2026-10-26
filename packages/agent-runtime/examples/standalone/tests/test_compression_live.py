"""Real Qwen + PostgreSQL compression probes; no synthetic Provider or SQLite."""

import asyncio
import json
import os
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
import sqlalchemy as sa
from centn_example.compression import ByteEstimator
from compression_faults import EmptyOutputTransport
from compression_semantics import answer
from runtime_provider_faults import InterruptingTransport
from sqlalchemy.ext.asyncio import create_async_engine
from test_context_runtime import request as context_request
from test_storage_uow import _seed, actor, lease

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.context_runtime import ContextRuntime
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.model import ModelConfig, ProviderCapabilities
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.requests import SubmitRequest, TextInput
from agent_runtime.providers.openai_compatible import OpenAIAdapterProfile, ProviderConnection
from agent_runtime.storage.compression import SqlContextCompression, SummaryResourceAccess
from agent_runtime.storage.context_access import SqlContextAccessVerifier
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.context_snapshots import SqlContextSnapshotStore
from agent_runtime.storage.context_sources import SqlRuntimeSourceReader
from agent_runtime.storage.migrate import execute

pytestmark = pytest.mark.skipif(
    os.environ.get("CENTN_LIVE") != "1", reason="Real services required"
)
FACTS = [
    "Alpha planned hours: 1000.",
    "Alpha completed hours: 800.",
    "Correction: completion is 80%, not 90%.",
    "Beta budget: 2300 CNY.",
    "Do not publish without legal approval.",
    "Pending: legal review and supplier signature.",
]


class NoTools:
    async def definition(self, ref):
        raise AssertionError("Summary must not request a tool")


@pytest.fixture(scope="module")
def compression_schema(tmp_path_factory):
    dsn = os.environ["CENTN_DATABASE_URL"]
    assert sa.make_url(dsn).database == "aletai_wangxh"
    schema = "p05_compress_" + uuid4().hex[:12]
    asyncio.run(execute("upgrade", dsn, schema, "base", 60))
    try:
        yield schema
    finally:

        async def cleanup():
            engine = create_async_engine(dsn)
            try:
                async with engine.begin() as db:
                    await db.execute(sa.schema.DropSchema(schema, cascade=True, if_exists=True))
            finally:
                await engine.dispose()

        asyncio.run(cleanup())
        (tmp_path_factory.getbasetemp() / "schema-cleanup.json").write_text(
            json.dumps(
                {
                    "database": "aletai_wangxh",
                    "schema": schema,
                    "status": "cleaned",
                }
            )
        )


async def scenario(tmp_path, mode, schema):
    dsn = os.environ["CENTN_DATABASE_URL"]
    assert sa.make_url(dsn).database == "aletai_wangxh"
    engine = create_async_engine(dsn)
    record = {
        "schema": schema,
        "database": "aletai_wangxh",
        "mode": mode,
        "startedAt": datetime.now(UTC).isoformat(),
        "layer": "SQL + real upstream",
    }
    evidence = tmp_path / f"{mode}.json"
    record["httpRequests"] = []
    record["providerErrors"] = []

    async def capture_request(http_request):
        body = json.loads(http_request.content)
        record["httpRequests"].append(
            {
                "messagesDigest": content_digest(body["messages"]),
                "bodyDigest": content_digest(body),
                "model": body["model"],
            }
        )

    async def capture_error(response):
        if response.status_code >= 400:
            raw = await response.aread()
            try:
                value = json.loads(raw)
            except ValueError:
                value = {}
            error = value.get("error", value) if isinstance(value, dict) else {}
            record["providerErrors"].append(
                {
                    "httpStatus": response.status_code,
                    "type": error.get("type") if isinstance(error, dict) else None,
                    "code": error.get("code") if isinstance(error, dict) else None,
                    "message": str(error.get("message", ""))[:2000]
                    if isinstance(error, dict)
                    else "",
                }
            )

    injected = []
    transport = (
        InterruptingTransport(httpx.AsyncHTTPTransport(), mode, injected)
        if mode in {"timeout", "disconnect"}
        else EmptyOutputTransport(injected)
        if mode == "empty"
        else None
    )
    client = httpx.AsyncClient(
        timeout=90,
        trust_env=False,
        transport=transport,
        event_hooks={"request": [capture_request], "response": [capture_error]},
    )
    try:
        world = await _seed(schema, suffix=mode)
        principal = actor(world=world)
        claim = lease(world)
        blobs = SqlContextBlobBackend(engine, schema)
        payloads = BlobContextPayloadStore(blobs)
        reader = SqlRuntimeSourceReader(engine, schema, blobs=blobs, optional_history=True)
        model_ref = context_request(refs=0).model
        original = SubmitRequest(
            session_id=world.session_id,
            agent=model_ref,
            idempotency_key="old-history",
            inputs=(
                TextInput(
                    kind="text",
                    text=(
                        "token " * 300000
                        if mode == "context_limit"
                        else "\n".join(
                            [
                                *FACTS,
                                *[
                            f"Archived Gamma note {i}: unrelated seating observation."
                                    for i in range(90)
                                ],
                            ]
                        )
                    ),
                ),
            ),
        )
        current = original.model_copy(
            update={
                "idempotency_key": "current",
                "inputs": (TextInput(kind="text", text="Recall facts"),),
            }
        )
        old_ref = await payloads.put(
            world.scope,
            original.model_dump(mode="json", by_alias=True),
            content_digest(original.model_dump(mode="json", by_alias=True)),
        )
        new_ref = await payloads.put(
            world.scope,
            current.model_dump(mode="json", by_alias=True),
            content_digest(current.model_dump(mode="json", by_alias=True)),
        )
        allocation = uuid4()
        async with engine.begin() as db:
            await db.execute(
                sa.text(
                    f"UPDATE {schema}.turn SET input_ref=:ref,primary_run_id=:run WHERE id=:id"
                ),
                {"ref": old_ref, "run": world.sender_run_id, "id": world.turn_id},
            )
            await db.execute(
                sa.text(
                    f"INSERT INTO {schema}.turn "
                    "(id,scope_id,session_id,request_key,request_hash,input_ref,primary_run_id) "
                    "VALUES (:id,:scope,:session,:key,:hash,:ref,:run)"
                ),
                {
                    "id": uuid4(),
                    "scope": world.scope,
                    "session": world.session_id,
                    "hash": "c" * 64,
                    "ref": new_ref,
                    "run": world.run_id,
                    "key": "current-" + mode,
                },
            )
            await db.execute(
                sa.text(
                    f"INSERT INTO {schema}.budget_allocation "
                    "(id,scope_id,root_run_id,limits,reserved,unknown_reserved,settled,"
                    "usage_status,unsettled_count,version) "
                    "VALUES (:id,:scope,:run,CAST(:limits AS jsonb),"
                    "'{}','{}','{}','complete',0,1)"
                ),
                {
                    "id": allocation,
                    "scope": world.scope,
                    "run": world.run_id,
                    "limits": json.dumps(
                        {
                            "model_calls": 1 if mode == "budget" else 16,
                            "tokens": 8000000 if mode == "context_limit" else 500000,
                            "tool_calls": 0,
                        }
                    ),
                },
            )
        resource = ResourceRef(
            resource_type="turn_input",
            resource_id=str(world.turn_id),
            resource_version="0",
            scope=ResourceScope(
                scope_id=world.scope, session_id=str(world.session_id), owner_id=world.actor_id
            ),
        )
        request = context_request(refs=0).model_copy(
            update={
                "actor": principal,
                "session_id": world.session_id,
                "run_id": world.run_id,
                "resource_refs": (resource,),
                "prompt_text": "Answer the user's question faithfully.",
                "user_input": (
                    "For active Alpha/Beta, state planned/completed hours, corrected percent, "
                    "Beta budget, publication restriction, and pending legal/supplier work. "
                    "Ignore unrelated archived Gamma projects."
                ),
                "budget": context_request(refs=0).budget.model_copy(
                    update={
                        "allocation_id": allocation,
                        "max_input_tokens": 4200,
                        "reserved_output_tokens": 2048,
                    }
                ),
            }
        )
        model = ModelConfig(
            ref=model_ref,
            connection=model_ref,
            model_name=os.environ["CENTN_MODEL"],
            max_output_tokens=1 if mode == "truncated" else 2048,
            thinking="disabled",
            provider_options={},
        )

        async def connection(call):
            return ProviderConnection(
                base_url=os.environ["CENTN_PROVIDER_URL"], api_key=os.environ["CENTN_PROVIDER_KEY"]
            )

        async def capabilities(call):
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
                context_window=8000000 if mode == "context_limit" else 262144,
                max_output_tokens=32768,
                allowed_option_keys=("chat_template_kwargs",),
            )

        def components():
            compression = SqlContextCompression(
                engine,
                schema,
                actor=principal,
                lease=lambda: claim,
                reader=reader,
                resources=reader,
                estimator=ByteEstimator(),
                model=model,
                client=client,
                connections=connection,
                capabilities=capabilities,
                adapter=OpenAIAdapterProfile.qwen(),
                max_input_tokens=8000000 if mode == "context_limit" else 200000,
            )
            runtime = ContextRuntime(
                reader=reader,
                estimator=ByteEstimator(),
                payloads=payloads,
                snapshots=SqlContextSnapshotStore(engine, schema),
                compression=compression,
            )
            access = SqlContextAccessVerifier(
                engine, schema, resources=SummaryResourceAccess(reader, compression)
            )
            return runtime, access

        runtime, access = components()
        source = await reader.read(request, resource)
        assert not source.required
        record["originalBytes"] = len(source.content.encode())
        if mode in {"timeout", "disconnect", "budget", "empty", "truncated", "context_limit"}:
            with pytest.raises(AgentRuntimeError) as error:
                await runtime.prepare(request)
            record["failure"] = error.value.failure.model_dump(mode="json", by_alias=True)
            if mode == "budget":
                assert error.value.failure.code == "BUDGET_EXHAUSTED"
            elif mode in {"timeout", "disconnect"}:
                assert injected == [mode]
                with pytest.raises(AgentRuntimeError) as unresolved:
                    await runtime.prepare(request)
                assert unresolved.value.failure.code == "USAGE_UNRESOLVED"
                assert injected == [mode]
                record["unknownUsageBlocksNextCall"] = True
            elif mode == "empty":
                assert injected and error.value.failure.code == "INCOMPLETE_MODEL_OUTPUT"
                assert "non-empty" in error.value.failure.public_message
            elif mode == "context_limit":
                assert error.value.failure.code == "CONTEXT_LIMIT"
            else:
                assert error.value.failure.code == "INCOMPLETE_MODEL_OUTPUT"
            record["injected"] = injected
        else:
            prepared = await runtime.prepare(request)
            assert prepared.sources[0].disposition == "summarized"
            assert prepared.sources[0].summary_ref is not None
            restored, restored_access = components()
            recovered, payload = await restored.load(world.scope, prepared.snapshot_id)
            await restored_access.verify(principal, recovered)
            text = payload["sources"][0]["content"]
            assert all(fact in text for fact in FACTS)
            record.update(
                {
                    "snapshotId": str(prepared.snapshot_id),
                    "summaryRef": prepared.sources[0].summary_ref.model_dump(
                        mode="json", by_alias=True
                    ),
                    "compressedBytes": len(text.encode()),
                    "factsPreserved": FACTS,
                    "freshInstanceRestored": True,
                }
            )
            original_runtime = ContextRuntime(
                reader=reader,
                estimator=ByteEstimator(),
                payloads=payloads,
                snapshots=SqlContextSnapshotStore(engine, schema),
            )
            original_snapshot = await original_runtime.prepare(
                request.model_copy(
                    update={
                        "budget": request.budget.model_copy(update={"max_input_tokens": 20000}),
                    }
                )
            )
            common = dict(
                engine=engine,
                schema=schema,
                actor=principal,
                lease=lambda: claim,
                model=model,
                client=client,
                connection=connection,
                capabilities=capabilities,
                tools=NoTools(),
                payloads=payloads,
            )
            record["originalAnswer"] = await answer(
                **common,
                runtime=original_runtime,
                access=SqlContextAccessVerifier(engine, schema, resources=reader),
                prepared=original_snapshot,
            )
            record["compressedAnswer"] = await answer(
                **common,
                runtime=restored,
                access=restored_access,
                prepared=recovered,
            )
            async with engine.begin() as db:
                await db.execute(
                    sa.text(f"UPDATE {schema}.turn SET input_ref=:ref WHERE id=:id"),
                    {"id": world.turn_id, "ref": new_ref},
                )
            with pytest.raises(AgentRuntimeError) as conflict:
                await restored.load(world.scope, prepared.snapshot_id)
            assert conflict.value.failure.code == "VERSION_CONFLICT"
            record["sourceContentConflict"] = "VERSION_CONFLICT"
            async with engine.begin() as db:
                await db.execute(
                    sa.text(f"UPDATE {schema}.turn SET input_ref=:ref WHERE id=:id"),
                    {"id": world.turn_id, "ref": old_ref},
                )
                await db.execute(
                    sa.text(f"UPDATE {schema}.session SET deleted_at=now() WHERE id=:id"),
                    {"id": world.session_id},
                )
            with pytest.raises(AgentRuntimeError) as deleted:
                await restored.load(world.scope, prepared.snapshot_id)
            assert deleted.value.failure.code == "FORBIDDEN"
            record["sourceDeletion"] = "FORBIDDEN"
            async with engine.begin() as db:
                await db.execute(
                    sa.text(f"UPDATE {schema}.session SET deleted_at=NULL WHERE id=:id"),
                    {"id": world.session_id},
                )
            # Both the old worker and a newly assembled worker must reject revoked originals.
            async with engine.begin() as db:
                await db.execute(
                    sa.text(f"UPDATE {schema}.session SET auth_revision=2 WHERE id=:id"),
                    {"id": world.session_id},
                )
            with pytest.raises(AgentRuntimeError) as error:
                await restored.load(world.scope, prepared.snapshot_id)
            assert error.value.failure.code == "VERSION_CONFLICT"
            record["revocationFailure"] = error.value.failure.code
        async with engine.connect() as db:
            rows = (
                (
                    await db.execute(
                        sa.text(
                            "SELECT id,context_snapshot_id,status,finish_reason,"
                            "usage_status,usage,"
                            f"failure,input_ref,output_ref FROM {schema}.model_attempt "
                            "WHERE run_id=:run ORDER BY started_at"
                        ),
                        {"run": world.run_id},
                    )
                )
                .mappings()
                .all()
            )
            record["modelAttempts"] = [dict(row) for row in rows]
        for row in rows:
            input_payload = await payloads.get(world.scope, row["input_ref"])
            if "renderedRequest" in input_payload:
                messages = input_payload["renderedRequest"]["messages"]
                if record["httpRequests"]:
                    assert content_digest(messages) == record["httpRequests"][0]["messagesDigest"]
                    assert messages[1]["content"] == input_payload["userInput"]
                    record["renderedMessagesMatchHTTP"] = True
        for row in rows:
            if row["output_ref"] is not None:
                record.setdefault("attemptOutputs", []).append(
                    await payloads.get(world.scope, row["output_ref"])
                )
        if mode in {"timeout", "disconnect"}:
            assert rows and rows[0]["usage_status"] == "unknown"
            assert record["attemptOutputs"][0]["kind"] == "compression-candidate-v1"
        record["passed"] = True
    except AgentRuntimeError as exc:
        record["unexpectedFailure"] = exc.failure.model_dump(mode="json", by_alias=True)
        async with engine.connect() as db:
            outputs = (
                (
                    await db.execute(
                        sa.text(
                            f"SELECT output_ref FROM {schema}.model_attempt "
                            "WHERE output_ref IS NOT NULL AND run_id=:run"
                        ),
                        {"run": world.run_id},
                    )
                )
                .scalars()
                .all()
            )
        record["rejectedOutputs"] = [await payloads.get(world.scope, ref) for ref in outputs]
        raise
    finally:
        if record.get("passed") is not True and "world" in locals():
            async with engine.connect() as db:
                record["failedAttemptAudit"] = [
                    dict(row)
                    for row in (
                        await db.execute(
                            sa.text(
                                "SELECT id,status,usage_status,usage,failure "
                                f"FROM {schema}.model_attempt "
                                "WHERE run_id=:run ORDER BY started_at"
                            ),
                            {"run": world.run_id},
                        )
                    )
                    .mappings()
                    .all()
                ]
        await client.aclose()
        await engine.dispose()
        record["cleanupRecord"] = "../schema-cleanup.json (module fixture)"
        evidence.write_text(json.dumps(record, default=str, ensure_ascii=False, indent=2))


@pytest.mark.parametrize(
    "mode", ["summary", "timeout", "disconnect", "budget", "empty", "truncated", "context_limit"]
)
def test_real_compression_sql(tmp_path, mode, compression_schema):
    asyncio.run(scenario(tmp_path, mode, compression_schema))

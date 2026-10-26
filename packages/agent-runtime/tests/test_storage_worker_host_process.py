"""R02 acceptance: installed Agents execute in distinct SQL Worker processes."""

import asyncio
import json
import os
import re
import signal
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa
from test_mcp_broker_integration import mcp_server as mcp_server  # noqa: F401
from test_storage_uow import actor, engine_for, lease, schema, world  # noqa: F401

from agent_runtime.agent import Agent
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.capacity import CapacityObserveRequest
from agent_runtime.contracts.decisions import ToolCallRequest
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.model import ModelConfig
from agent_runtime.contracts.references import CapabilityKey, CapabilityRef
from agent_runtime.contracts.requests import SubmitRequest, TextInput
from agent_runtime.contracts.subagents import SpawnRequest
from agent_runtime.contracts.tasks import TaskPackage
from agent_runtime.contracts.tools import McpToolBinding, ToolInvocation, ToolSpec
from agent_runtime.contracts.transactions import CommitGuard
from agent_runtime.storage.admission import AdmissionAgent, SqlAdmission
from agent_runtime.storage.agent_install import SqlAgentInstaller
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.context_sources import SqlRuntimeSourceReader
from agent_runtime.storage.host_tool_access import SqlHostToolAccess
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.subagents import SqlSubagentRuntime
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.uow.capacity import SqlSystemCapacityStore

pytestmark = pytest.mark.skipif(
    not os.getenv("AGENT_RUNTIME_TEST_DSN"), reason="requires isolated PostgreSQL"
)


def ref(key: str) -> CapabilityRef:
    return CapabilityRef(key=CapabilityKey(namespace="builtin", key=key), revision="1")


def sse(*chunks: dict) -> bytes:
    return (
        "".join(f"data: {json.dumps(chunk)}\n\n" for chunk in chunks) + "data: [DONE]\n\n"
    ).encode()


def test_root_tool_access_fences_stale_claim(world) -> None:  # noqa: F811
    async def exercise() -> None:
        engine = engine_for(world.schema, search_path=False)
        current = actor(world=world)
        reader = SqlRuntimeSourceReader(
            engine, world.schema, blobs=SqlContextBlobBackend(engine, world.schema)
        )
        access = SqlHostToolAccess(
            world.schema,
            current,
            reader,
            lease=lambda: lease(world, slot_version="99"),
        )
        invocation = ToolInvocation(
            actor=current,
            session_id=world.session_id,
            run_id=world.run_id,
            model_attempt_id=None,
            context_snapshot_id=uuid4(),
            context_version="1",
            call=ToolCallRequest(tool_call_id=uuid4(), tool=ref("read-counter"), arguments={}),
            operation_id=None,
            deadline_at=datetime.now(UTC) + timedelta(minutes=1),
        )
        try:
            async with engine.begin() as db:
                with pytest.raises(AgentRuntimeError) as failure:
                    await access.authorize(db, invocation, ())
            assert failure.value.failure.code == "FENCED"
        finally:
            await engine.dispose()

    asyncio.run(exercise())


@pytest.mark.parametrize("mode", ["local", "mcp"])
def test_installed_main_child_and_maintenance_workers(world, mcp_server, mode) -> None:  # noqa: F811
    async def exercise() -> None:
        engine = engine_for(world.schema, search_path=False)
        current = actor(world=world)
        main_ref, child_ref, tool_ref = ref("host-main"), ref("host-child"), ref("read-counter")
        model_ref, connection_ref = ref("host-model"), ref("host-connection")
        tool = ToolSpec(
            ref=tool_ref,
            description="Read the trusted counter",
            source=tool_ref,
            kind="read",
            side_effect="none",
            input_schema=tool_ref,
            output_schema=tool_ref,
            permissions=(),
            approval_policy=ref("host-policy"),
            idempotency="read_only",
            timeout_seconds=30,
            concurrency_group="host-counter",
            max_concurrency=1,
            cancellation="stop",
            public_result_policy="full",
        )
        if mode == "mcp":
            input_schema = {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
                "additionalProperties": False,
            }
            output_schema = {
                "type": "object",
                "required": ["resultType", "content"],
                "properties": {"resultType": {"const": "complete"}},
            }
            tool = ToolSpec.model_validate(
                {
                    **tool.model_dump(mode="json", by_alias=True),
                    "source": ref("mcp-server").model_dump(mode="json", by_alias=True),
                    "timeoutSeconds": 180,
                    "inputSchema": {
                        "key": {"namespace": "mcp.input", "key": content_digest(input_schema)},
                        "revision": "1",
                    },
                    "outputSchema": {
                        "key": {"namespace": "mcp.output", "key": content_digest(output_schema)},
                        "revision": "1",
                    },
                    "mcp": McpToolBinding(
                        protocol_version="2026-07-28",
                        tool_name="search",
                        destination=ref("mcp-search"),
                        input_schema=input_schema,
                        output_schema=output_schema,
                    ).model_dump(mode="json", by_alias=True),
                }
            )
        main_agent = Agent(
            id=main_ref.key.key,
            name="Host Main",
            model=model_ref,
            instructions=("Read the counter with your tool, then add nine.",),
            tools=(tool_ref,),
        )
        child_agent = Agent(
            id=child_ref.key.key,
            name="Host Child",
            model=model_ref,
            instructions=("Complete the assigned analysis in one sentence.",),
        )
        main_model = ModelConfig(
            ref=model_ref,
            connection=connection_ref,
            model_name="host-main-test",
            max_output_tokens=128,
            thinking="disabled",
            provider_options={"chat_template_kwargs": {"enable_thinking": False}},
        )
        child_model = main_model.model_copy(update={"model_name": "host-child-test"})
        installer = SqlAgentInstaller(engine, world.schema, namespace="builtin")
        main_revision = await installer.install(world.scope, main_ref, main_agent)
        child_revision = await installer.install(world.scope, child_ref, child_agent)
        main_installed = AdmissionAgent(main_ref, main_revision, main_agent, 100000)
        session_id = uuid4()
        async with engine.begin() as db:
            await db.execute(
                sa.text(
                    f'INSERT INTO "{world.schema}".session '
                    "(id,scope_id,owner_actor_id,title_source,event_seq,auth_revision) "
                    "VALUES (:id,:scope,:actor,'none',0,:auth)"
                ),
                {
                    "id": session_id,
                    "scope": world.scope,
                    "actor": current.actor_id,
                    "auth": int(current.auth_revision),
                },
            )

        async def admission_authorize(db, claimed_actor, request, installed):
            return claimed_actor == current and installed == main_installed

        receipt = await SqlAdmission(
            engine,
            world.schema,
            authorize=admission_authorize,
            agents={(world.scope, main_ref): main_installed},
        ).accept(
            current,
            SubmitRequest(
                session_id=session_id,
                agent=main_ref,
                idempotency_key="r02-host-main",
                inputs=(TextInput(kind="text", text="Read the counter and add nine."),),
            ),
        )
        capacity_store = SqlSystemCapacityStore(engine, world.schema)
        capacity = SqlToolCapacity(capacity_store, deployment_id="r02-host-process")
        await capacity_store.seed(capacity.key(world.scope, tool), capacity=1)

        parent_allocation, child_allocation = uuid4(), uuid4()
        async with engine.begin() as db:
            await db.execute(
                sa.text(
                    f'INSERT INTO "{world.schema}".budget_allocation '
                    "(id,scope_id,root_run_id,limits,reserved,unknown_reserved,settled,"
                    "usage_status,unsettled_count,version) VALUES "
                    "(:id,:scope,:run,CAST(:limits AS jsonb),'{}','{}','{}','complete',0,0)"
                ),
                {
                    "id": parent_allocation,
                    "scope": world.scope,
                    "run": world.run_id,
                    "limits": json.dumps({"model_calls": 10, "tool_calls": 10, "tokens": 4000}),
                },
            )
            await db.execute(
                sa.text(
                    f'UPDATE "{world.schema}".capability_grant '
                    "SET allowed_scope=CAST(:allowed AS jsonb) WHERE run_id=:run"
                ),
                {
                    "run": world.run_id,
                    "allowed": json.dumps(
                        {
                            "agents": [child_ref.model_dump(mode="json", by_alias=True)],
                            "tools": [],
                            "resources": [],
                        }
                    ),
                },
            )
        reader = SqlRuntimeSourceReader(
            engine, world.schema, blobs=SqlContextBlobBackend(engine, world.schema)
        )
        acceptance = ref("host-acceptance")
        from agent_runtime.storage.delegation_types import DelegationPolicy

        policy = DelegationPolicy(
            agent=child_ref,
            agent_revision_id=child_revision,
            parent_allocation_id=parent_allocation,
            model_calls=4,
            tool_calls=2,
            tokens=1200,
            tools=(),
            acceptance_spec=acceptance,
            output_schema_ref="builtin:report:1",
        )

        class Verifier:
            async def verify_in_transaction(self, db, request, proposal):
                raise AssertionError("child result must not be collected by this test")

        child_runtime = SqlSubagentRuntime(
            engine,
            world.schema,
            actor=current,
            journal=SqlJournal(engine, world.schema, current),
            resources=reader,
            policies={child_ref: policy},
            message_predicate=ref("message"),
            verifier=Verifier(),
        )
        child = await child_runtime.spawn(
            SpawnRequest(
                parent_guard=CommitGuard(
                    actor=current,
                    lease=lease(world),
                    expected_state_version=str(world.state_version),
                ),
                task=TaskPackage(
                    task_id=uuid4(),
                    target_agent=child_ref,
                    objective="State one finding about the assigned evidence.",
                    constraints=(),
                    acceptance_spec_ref="builtin:host-acceptance:1",
                    output_schema_ref="builtin:report:1",
                    resource_refs=(),
                    budget_allocation_id=child_allocation,
                    deadline_at=datetime.now(UTC) + timedelta(minutes=5),
                ),
                idempotency_key="r02-host-child",
            )
        )

        calls: list[str] = []

        class ModelEndpoint(BaseHTTPRequestHandler):
            def do_POST(self):
                size = int(self.headers["Content-Length"])
                payload = json.loads(self.rfile.read(size))
                model = payload["model"]
                calls.append(model)
                if payload.get("response_format"):
                    body = json.dumps(payload)
                    match = re.search(
                        r"Only these sealed item IDs may be used as finalItemIds:.*?"
                        r"([0-9a-f]{8}-[0-9a-f-]{27})",
                        body,
                    )
                    assert match, body[-1000:]
                    content = json.dumps(
                        {
                            "kind": "finish",
                            "payload": {
                                "outcome": "success",
                                "evidenceRefs": [],
                                "finalItemIds": [match.group(1)],
                            },
                        }
                    )
                    chunks = (
                        {"choices": [{"delta": {"content": content}}]},
                        {"choices": [{"delta": {}, "finish_reason": "stop"}]},
                    )
                elif model == "host-main-test" and calls.count(model) == 1:
                    name = payload["tools"][0]["function"]["name"]
                    arguments = "{}" if mode == "local" else '{"query":"fixture"}'
                    chunks = (
                        {
                            "choices": [
                                {
                                    "delta": {
                                        "tool_calls": [
                                            {
                                                "index": 0,
                                                "id": "counter-1",
                                                "type": "function",
                                                "function": {"name": name, "arguments": arguments},
                                            }
                                        ]
                                    }
                                }
                            ]
                        },
                        {"choices": [{"delta": {}, "finish_reason": "tool_calls"}]},
                    )
                else:
                    answer = (
                        "5846"
                        if model == "host-main-test"
                        else "The assigned evidence supports the delegated finding."
                    )
                    chunks = (
                        {"choices": [{"delta": {"content": answer}}]},
                        {"choices": [{"delta": {}, "finish_reason": "stop"}]},
                    )
                response = sse(
                    *chunks,
                    {
                        "choices": [],
                        "usage": {
                            "prompt_tokens": 10,
                            "completion_tokens": 10,
                            "total_tokens": 20,
                        },
                    },
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)

            def log_message(self, format, *args):
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), ModelEndpoint)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        environment = {
            **os.environ,
            "RUNTIME_TEST_SCHEMA": world.schema,
            "RUNTIME_TEST_SCOPE": world.scope,
            "RUNTIME_TEST_DEPLOYMENT": "r02-host-process",
            "RUNTIME_TEST_PROVIDER_URL": f"http://127.0.0.1:{server.server_port}/v1",
            "RUNTIME_TEST_MAIN_AGENT": main_agent.model_dump_json(by_alias=True),
            "RUNTIME_TEST_MAIN_REVISION": str(main_revision),
            "RUNTIME_TEST_MAIN_REF": main_ref.model_dump_json(by_alias=True),
            "RUNTIME_TEST_MAIN_MODEL": main_model.model_dump_json(by_alias=True),
            "RUNTIME_TEST_CHILD_AGENT": child_agent.model_dump_json(by_alias=True),
            "RUNTIME_TEST_CHILD_REVISION": str(child_revision),
            "RUNTIME_TEST_CHILD_REF": child_ref.model_dump_json(by_alias=True),
            "RUNTIME_TEST_CHILD_MODEL": child_model.model_dump_json(by_alias=True),
            "RUNTIME_TEST_TOOL_SPEC": tool.model_dump_json(by_alias=True),
        }
        if mode == "mcp":
            environment.update(
                {
                    "RUNTIME_TEST_MCP_PORT": str(mcp_server[0]),
                    "RUNTIME_TEST_MCP_CERT": mcp_server[2]["cert_file"],
                    "RUNTIME_TEST_MCP_TOKEN": mcp_server[2]["token"],
                }
            )
            seen_before = len(mcp_server[2]["seen"])
        root = Path(__file__).resolve().parents[3]
        processes = []
        logs: dict[str, str] = {}
        try:
            for role in ("main", "child", "maintenance"):
                process = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "scripts.runtime_worker",
                    "--factory",
                    "scripts.verify_runtime_p04_host_worker:bootstrap",
                    cwd=root,
                    env={**environment, "RUNTIME_TEST_ROLE": role},
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                processes.append((role, process))
            deadline = time.monotonic() + (210 if mode == "mcp" else 90)
            while time.monotonic() < deadline:
                for role, process in processes:
                    if process.returncode is not None:
                        out, err = await process.communicate()
                        pytest.fail(f"{role} exited: {out.decode()} {err.decode()}")
                async with engine.connect() as db:
                    states = (
                        (
                            await db.execute(
                                sa.text(
                                    f'SELECT id,status FROM "{world.schema}".run '
                                    "WHERE id IN (:main,:child)"
                                ),
                                {"main": receipt.run_id, "child": child.child_run_id},
                            )
                        )
                        .mappings()
                        .all()
                    )
                if {row["id"]: row["status"] for row in states} == {
                    receipt.run_id: "completed",
                    child.child_run_id: "completed",
                }:
                    break
                await asyncio.sleep(0.1)
            else:
                async with engine.connect() as db:
                    runs = (
                        (
                            await db.execute(
                                sa.text(
                                    f'SELECT id,status,phase,failure FROM "{world.schema}".run '
                                    "WHERE id IN (:main,:child)"
                                ),
                                {"main": receipt.run_id, "child": child.child_run_id},
                            )
                        )
                        .mappings()
                        .all()
                    )
                    works = (
                        (
                            await db.execute(
                                sa.text(
                                    "SELECT run_id,status,kind,lease_until "
                                    f'FROM "{world.schema}".work_item '
                                    "WHERE run_id IN (:main,:child)"
                                ),
                                {"main": receipt.run_id, "child": child.child_run_id},
                            )
                        )
                        .mappings()
                        .all()
                    )
                    attempts = (
                        (
                            await db.execute(
                                sa.text(
                                    "SELECT run_id,status,failure FROM "
                                    f'"{world.schema}".model_attempt '
                                    "WHERE run_id IN (:main,:child)"
                                ),
                                {"main": receipt.run_id, "child": child.child_run_id},
                            )
                        )
                        .mappings()
                        .all()
                    )
                    tool_calls = (
                        (
                            await db.execute(
                                sa.text(
                                    f"SELECT run_id,status,provider_call_id,result_ref "
                                    f'FROM "{world.schema}".tool_call WHERE run_id=:run'
                                ),
                                {"run": receipt.run_id},
                            )
                        )
                        .mappings()
                        .all()
                    )
                    credential_grants = (
                        (
                            await db.execute(
                                sa.text(
                                    f'SELECT issued_at,expires_at,revoked_at FROM "{world.schema}".'
                                    "capability_grant WHERE "
                                    "allowed_scope->>'kind'='credential_proxy'"
                                )
                            )
                        )
                        .mappings()
                        .all()
                    )
                    credential_requests = (
                        (
                            await db.execute(
                                sa.text(
                                    f'SELECT status,created_at FROM "{world.schema}".'
                                    "credential_request"
                                )
                            )
                        )
                        .mappings()
                        .all()
                    )
                    db_now = (await db.execute(sa.text("SELECT clock_timestamp()"))).scalar_one()
                pytest.fail(
                    f"main/child did not finish: runs={list(map(dict, runs))}; "
                    f"works={list(map(dict, works))}; attempts={list(map(dict, attempts))}; "
                    f"tools={list(map(dict, tool_calls))}; calls={calls}; "
                    f"grants={list(map(dict, credential_grants))}; "
                    f"requests={list(map(dict, credential_requests))}; "
                    f"db_now={db_now}; app_now={datetime.now(UTC)}"
                )
            async with engine.connect() as db:
                tool_states = (
                    (
                        await db.execute(
                            sa.text(
                                f'SELECT status FROM "{world.schema}".tool_call WHERE run_id=:run'
                            ),
                            {"run": receipt.run_id},
                        )
                    )
                    .scalars()
                    .all()
                )
                child_messages = (
                    await db.execute(
                        sa.text(
                            f'SELECT count(*) FROM "{world.schema}".agent_message '
                            "WHERE sender_run_id=:run AND kind='result.proposed'"
                        ),
                        {"run": child.child_run_id},
                    )
                ).scalar_one()
                active_works = (
                    await db.execute(
                        sa.text(
                            f'SELECT count(*) FROM "{world.schema}".work_item '
                            "WHERE run_id IN (:main,:child) AND status='running'"
                        ),
                        {"main": receipt.run_id, "child": child.child_run_id},
                    )
                ).scalar_one()
                events = (
                    (
                        await db.execute(
                            sa.text(
                                f'SELECT run_id,count(*) AS total FROM "{world.schema}".event '
                                "WHERE run_id IN (:main,:child) GROUP BY run_id"
                            ),
                            {"main": receipt.run_id, "child": child.child_run_id},
                        )
                    )
                    .mappings()
                    .all()
                )
                active_leases = (
                    await db.execute(
                        sa.text(
                            f'SELECT count(*) FROM "{world.schema}".run '
                            "WHERE id IN (:main,:child) AND owner_id IS NOT NULL"
                        ),
                        {"main": receipt.run_id, "child": child.child_run_id},
                    )
                ).scalar_one()
            assert tool_states == ["completed"]
            if mode == "mcp":
                assert mcp_server[2]["seen"][seen_before:] == [("tools/call", True, "search")]
            assert child_messages == 1
            assert active_works == 0
            assert active_leases == 0
            assert {row["run_id"] for row in events} == {receipt.run_id, child.child_run_id}
            assert all(row["total"] > 0 for row in events)
            if mode == "mcp":
                async with engine.connect() as db:
                    response_refs = (
                        await db.execute(sa.text(
                            f'SELECT response->>\'bodyRef\' FROM "{world.schema}".'
                            "credential_request WHERE status='completed'"
                        ))
                    ).scalars().all()
                    payloads = (
                        await db.execute(sa.text(
                            f'SELECT content FROM "{world.schema}".context_blob'
                        ))
                    ).scalars().all()
                assert len(response_refs) == 1 and response_refs[0]
                assert all(mcp_server[2]["token"].encode() not in bytes(row)
                           for row in payloads)
                await SqlContextBlobBackend(engine, world.schema).collect_unreferenced(
                    before=datetime.now(UTC) + timedelta(days=1)
                )
                assert await SqlContextBlobBackend(engine, world.schema).get(
                    world.scope, response_refs[0]
                )
            assert (
                await capacity_store.observe(
                    CapacityObserveRequest(key=capacity.key(world.scope, tool))
                )
            ).reserved == "0"
            assert calls.count("host-main-test") >= 3
            assert calls.count("host-child-test") == 1
        finally:
            for role, process in processes:
                if process.returncode is None:
                    process.send_signal(signal.SIGTERM)
                try:
                    out, err = await asyncio.wait_for(process.communicate(), timeout=10)
                except TimeoutError:
                    process.kill()
                    out, err = await process.communicate()
                logs[role] = out.decode() + err.decode()
            evidence = root / "output/p04-03"
            evidence.mkdir(parents=True, exist_ok=True)
            for role, content in logs.items():
                (evidence / f"r04-{mode}-host-{role}.log").write_text(content, encoding="utf-8")
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            await engine.dispose()
        for role, content in logs.items():
            assert f"r02-host role={role}" in content
            assert "started" in content and "stopped" in content
            assert "Exception terminating connection" not in content

    asyncio.run(exercise())

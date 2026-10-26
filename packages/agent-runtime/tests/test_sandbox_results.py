"""Public projection and private output sealing; no remote execution or database."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from test_context_payloads import Backend as Blobs
from test_decision_validation import tool_action

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.decisions import decision_adapter
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.tools import ToolInvocation, ToolSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.sandbox_results import SqlSandboxResultCollector
from agent_runtime.storage.tool_host import ToolEffect


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "policy,status,invalid,reason",
    [
        ("full", "completed", False, None),
        ("summary", "completed", False, None),
        ("reference_only", "completed", False, None),
        ("full", "failed", False, None),
        ("reference_only", "failed", False, None),
        ("full", "running", False, None),
        ("full", "completed", True, None),
        ("full", "failed", False, "quota_exceeded"),
        ("full", "failed", False, "execution_deadline"),
        ("full", "failed", False, "execution_cancelled"),
        ("full", "failed", False, "quota_inspection_unavailable"),
    ],
)
async def test_output_is_sealed_and_public_policy_enforced(policy, status, invalid, reason):
    engine = create_async_engine("postgresql+asyncpg://unused:unused@127.0.0.1/unused")
    call = decision_adapter.validate_python(tool_action()).payload.validated_calls[0]
    actor = ActorContext(scope_id="test", actor_id="owner", auth_revision="1", grants_ref="grant:1")
    invocation = ToolInvocation(
        actor=actor,
        session_id=uuid4(),
        run_id=uuid4(),
        model_attempt_id=uuid4(),
        context_snapshot_id=uuid4(),
        context_version="1",
        call=call,
        operation_id=uuid4(),
        deadline_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    spec = ToolSpec(
        ref=call.tool,
        description="sandbox",
        source=call.tool,
        kind="background",
        side_effect="external",
        input_schema=call.tool,
        output_schema=call.tool,
        permissions=(),
        approval_policy=call.tool,
        idempotency="reconcile",
        timeout_seconds=60,
        concurrency_group="sandbox",
        max_concurrency=1,
        cancellation="stop",
        public_result_policy=policy,
    )
    payload = {
        "invocation": invocation.model_dump(mode="json", by_alias=True),
        "spec": spec.model_dump(mode="json", by_alias=True),
        "call": call.model_dump(mode="json", by_alias=True),
    }
    row = {
        "run_id": invocation.run_id,
        "operation_id": invocation.operation_id,
        "status": status,
        "id": uuid4(),
        "generation": 1,
        "failure": {"category": reason, "exitCode": 0} if reason else None,
        "remote_execution_id": "command-1",
        "log_cursor": "0",
    }
    blobs = BlobContextPayloadStore(Blobs())

    class Schema:
        def output(self, spec, data):
            assert data == {"values": [1, 2, 3]}
            if invalid:
                raise ValueError("invalid output schema")

    async def read(backend, payload):
        assert payload["remoteExecutionId"] == "command-1" and payload["logCursor"] == "0"
        refs = (
            (
                ResourceRef(
                    resource_type="artifact",
                    resource_id=str(uuid4()),
                    resource_version="1",
                    scope=ResourceScope(scope_id="test", session_id=str(invocation.session_id)),
                ),
            )
            if policy == "reference_only"
            else ()
        )
        return ToolEffect({"values": [1, 2, 3]}, "Three values generated.", refs)

    async def authorize(*args):
        raise AssertionError("read does not grant SQL commit authority")

    collector = SqlSandboxResultCollector(
        engine,
        "agent_runtime",
        actor=actor,
        payloads=blobs,
        journal=None,
        schemas=Schema(),
        reader=read,
        authorize=authorize,
        waker=None,
    )
    try:
        if status == "running":
            with pytest.raises(AgentRuntimeError):
                await collector.read(None, row, payload)
            return
        result = await collector.read(None, row, payload)
        detail = await blobs.get("test", result.collection_ref)
        assert detail["data"] == {"values": [1, 2, 3]}
        observation = await blobs.get("test", result.ref)
        from agent_runtime.tool_transcript import ToolTranscript

        assert ToolTranscript.model_validate(observation).call == invocation.call
        assert set(observation) == {"call", "outcome"}
        assert detail["executionId"] == str(row["id"])
        assert detail["generation"] == row["generation"]
        assert "data" not in observation and "collectedData" not in observation
        if status == "completed" and not invalid:
            assert result.outcome.result.policy == policy
            if policy == "summary":
                assert observation["outcome"]["result"] == {
                    "policy": "summary",
                    "text": "Three values generated.",
                }
            if policy == "reference_only":
                public = observation["outcome"]["result"]
                assert set(public) == {"policy", "refs"}
                assert public["refs"] == observation["outcome"]["effectRefs"]
                assert public["refs"][0]["scope"]["scopeId"] == "test"
                assert public["refs"][0]["resourceVersion"] == "1"
        else:
            assert result.outcome.failure.detail_ref == result.collection_ref
            assert result.outcome.effect == "committed" and not result.outcome.failure.retryable
            if invalid:
                assert result.outcome.failure.code == "INVALID_INPUT"
            if reason in {"quota_exceeded", "execution_deadline"}:
                assert result.outcome.failure.code == "BUDGET_EXHAUSTED"
            if reason:
                assert detail["exitEvidence"]["category"] == reason
                assert "Inspect the saved output" in result.outcome.failure.public_message
    finally:
        await engine.dispose()

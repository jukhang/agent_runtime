"""Windowing is explicit and reversible; required evidence is never silently discarded."""

from uuid import uuid4

import pytest
from test_context_runtime import Estimator, Payloads, Snapshots, request

from agent_runtime.context_runtime import ContextRuntime, LoadedSource
from agent_runtime.context_window import ToolResultWindow
from agent_runtime.contracts.canonical import canonical_json
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.references import CapabilityRef, ResourceRef

TOOL = CapabilityRef.model_validate({"key": {"namespace": "test", "key": "read"}, "revision": "1"})


def source(req, *, size=5000):
    identity = uuid4()
    transcript = {
        "call": {
            "toolCallId": str(identity),
            "tool": TOOL.model_dump(mode="json", by_alias=True),
            "arguments": {"page": 0},
        },
        "outcome": {
            "status": "completed",
            "toolCallId": str(identity),
            "runId": str(req.run_id),
            "operationId": None,
            "effect": "none",
            "effectRefs": [],
            "result": {"policy": "full", "data": {"text": "A" * size}},
        },
    }
    return LoadedSource(
        resource=ResourceRef(
            resource_type="tool_call_result",
            resource_id=str(identity),
            resource_version="1",
            scope={"scopeId": req.actor.scope_id},
        ),
        content=canonical_json(transcript),
        required=True,
    )


def test_only_older_registered_success_is_offloadable_and_original_is_unchanged():
    req = request(refs=0)
    sources = (source(req), source(req))
    window = ToolResultWindow({TOOL: lambda data: {"characters": len(data["text"])}})
    result = window(sources)
    assert result[0].content == sources[0].content
    assert result[1].offloaded_content is None
    assert '"reread"' in result[0].offloaded_content
    assert '"originalResource"' in result[0].offloaded_content
    assert "AAAA" not in result[0].offloaded_content
    assert ToolResultWindow({})(sources) == sources


@pytest.mark.asyncio
async def test_explicitly_retired_successes_do_not_make_required_working_set_unbounded():
    req = request(refs=0)
    sources = tuple(source(req) for _ in range(20))
    req = req.model_copy(
        update={
            "resource_refs": tuple(s.resource for s in sources),
            "budget": req.budget.model_copy(update={"max_input_tokens": 8500}),
        }
    )

    class Reader:
        async def read(self, request, resource):
            return next(s for s in sources if s.resource == resource)

    service = ContextRuntime(
        reader=Reader(),
        estimator=Estimator(),
        payloads=Payloads(),
        snapshots=Snapshots(),
        source_window=ToolResultWindow({TOOL: lambda _: {}}, retire_successful=True),
    )
    result = await service.prepare(req)
    assert result.estimated_input_tokens <= 8500
    assert result.sources[-1].required and result.sources[-1].disposition == "included"
    assert any(s.reason == "retired_tool_result_budget" for s in result.sources)
    assert all(s.required or s.disposition in {"included", "omitted"} for s in result.sources)


@pytest.mark.asyncio
async def test_required_aggregate_adapts_without_exceeding_budget_or_changing_storage():
    req = request(refs=0)
    sources = (source(req), source(req))
    req = req.model_copy(update={"resource_refs": tuple(s.resource for s in sources)})

    class Reader:
        async def read(self, request, resource):
            return next(s for s in sources if s.resource == resource)

    payloads = Payloads()
    runtime = ContextRuntime(
        reader=Reader(),
        estimator=Estimator(),
        payloads=payloads,
        snapshots=Snapshots(),
        source_window=ToolResultWindow({TOOL: lambda _: {}}),
    )
    req = req.model_copy(
        update={"budget": req.budget.model_copy(update={"max_input_tokens": 8500})}
    )
    snapshot = await runtime.prepare(req)
    _, payload = await runtime.load(snapshot.scope_id, snapshot.snapshot_id)
    assert snapshot.estimated_input_tokens <= 8500
    assert snapshot.sources[0].reason == "offloaded_retrievable"
    assert snapshot.sources[1].reason is None
    assert payload["sources"][0]["offloaded"] is True
    assert payload["sources"][1]["content"] == sources[1].content
    assert sources[0].offloaded_content is None
    # No alternative can make the latest essential result disappear.
    req = req.model_copy(
        update={"budget": req.budget.model_copy(update={"max_input_tokens": 2000})}
    )
    with pytest.raises(AgentRuntimeError):
        await runtime.prepare(req)


@pytest.mark.asyncio
@pytest.mark.parametrize("summary", ["x" * 5000, "x" * 5001, "x" * 4990])
async def test_full_summary_wrapper_must_have_actual_gain(summary):
    from agent_runtime.context_runtime import CompressedSource

    req = request(max_input_tokens=1800)

    class Reader:
        async def read(self, req, resource):
            return LoadedSource(resource=resource, content="x" * 5000)

    class Compression:
        async def compress(self, req, source):
            return CompressedSource(content=summary, summary_ref=source.resource)

    runtime = ContextRuntime(
        reader=Reader(),
        estimator=Estimator(),
        payloads=Payloads(),
        snapshots=Snapshots(),
        compression=Compression(),
    )
    result = await runtime.prepare(req)
    assert result.sources[0].reason == "optional_compression_no_gain"

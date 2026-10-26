"""Offline safety tests, not live Provider release evidence."""

import json

import pytest
from test_context_runtime import Estimator, Payloads, Reader, Snapshots, request

from agent_runtime.compression import source_lines, verified_extraction
from agent_runtime.context_runtime import CompressedSource, ContextRuntime
from agent_runtime.contracts.failure import AgentRuntimeError


@pytest.mark.parametrize(
    "output",
    [
        "",
        "{}",
        '{"quotes":["A=100"]}',
        '{"quotes":["A=100","B=900"]}',
        '{"quotes":["A=100","B!=900"',
    ],
)
def test_invalid_or_incomplete_summary_never_replaces_facts(output):
    with pytest.raises(AgentRuntimeError):
        verified_extraction("A=100\nB!=900\nA=100\n", output)


def test_preserves_corrections_negation_pending_steps_and_all_topics():
    lines = [
        "Alpha: 1000",
        "Beta: 800",
        "Correction: 80%, not 90%",
        "Do not publish",
        "Pending: legal review",
    ]
    original = "\n".join(lines * 20)
    result = verified_extraction(original, json.dumps({"quotes": lines}))
    assert "\n".join(lines) in result
    assert json.loads(result.split("indices): ")[1]) == list(range(5)) * 20
    assert len(result) < len(original)


def test_structured_history_keeps_every_leaf_and_decodes_escaped_lines():
    original = json.dumps({"text": "A=1\nNot approved\nA=1\n" * 20, "version": 3})
    lines = list(dict.fromkeys(source_lines(original)))
    result = verified_extraction(original, json.dumps({"quotes": lines}))
    assert '$/"version"=3' in result and "Not approved" in result


def test_unique_text_is_not_claimed_as_compressed():
    with pytest.raises(AgentRuntimeError):
        verified_extraction("A=1", '{"quotes":["A=1"]}')


def test_model_selected_literal_subset_can_compress_unique_irrelevant_history():
    original = "\n".join(
        ["Active budget: 2300", *[f"Archived note {i}: unrelated" for i in range(80)]]
    )
    summary = verified_extraction(original, '{"keep":[0]}')
    assert "Active budget: 2300" in summary
    assert "Archived note" not in summary
    assert "facts may be missing" in summary


@pytest.mark.parametrize("ids", [[], [-1], [999], [True], [0, 0], [1, 0]])
def test_subset_rejects_fabricated_reordered_or_ambiguous_indices(ids):
    with pytest.raises(AgentRuntimeError):
        verified_extraction("A=1\nB=2\n" * 100, json.dumps({"keep": ids}))


def test_repeated_corrections_preserve_the_final_occurrence():
    original = "\n".join(["A=100", "A=200", "A=100"] * 30)
    text = verified_extraction(original, '{"quotes":["A=100","A=200"]}')
    order = json.loads(text.split("indices): ")[1])
    assert ["A=100", "A=200"][order[-1]] == "A=100"
    assert len(order) == 90


@pytest.mark.asyncio
async def test_runtime_records_actual_summary_and_revalidates_restoration():
    class Compression:
        checked = 0

        async def compress(self, req, source):
            assert not source.required
            return CompressedSource(content="verified", summary_ref=source.resource)

        async def validate(self, snapshot, payload):
            self.checked += 1

    class LongReader(Reader):
        async def read(self, req, ref):
            return (await super().read(req, ref)).model_copy(update={"content": "x" * 5000})

    compression = Compression()
    runtime = ContextRuntime(
        reader=LongReader(required=False),
        estimator=Estimator(),
        payloads=Payloads(),
        snapshots=Snapshots(),
        compression=compression,
    )
    req = request(max_input_tokens=1800)
    snapshot = await runtime.prepare(req)
    assert snapshot.sources[0].disposition == "summarized"
    assert snapshot.sources[0].summary_status == "completed"
    _, payload = await runtime.load(snapshot.scope_id, snapshot.snapshot_id)
    assert payload["sources"][0]["content"] == "verified"
    assert compression.checked == 1


@pytest.mark.asyncio
async def test_runtime_never_compresses_required_overflow():
    class ForbiddenCompression:
        async def compress(self, req, source):
            raise AssertionError("Must never compress mandatory facts")

        async def validate(self, snapshot, payload):
            pass

    runtime = ContextRuntime(
        reader=Reader(required=True),
        estimator=Estimator(),
        payloads=Payloads(),
        snapshots=Snapshots(),
        compression=ForbiddenCompression(),
    )
    with pytest.raises(AgentRuntimeError):
        await runtime.prepare(request(max_input_tokens=5))

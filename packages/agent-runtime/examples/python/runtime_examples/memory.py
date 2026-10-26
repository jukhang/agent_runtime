"""History as source data and verifiable compression, not a silent global memory."""

from agent_runtime.compression import render_source, verified_extraction
from agent_runtime.context_runtime import LoadedSource
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from runtime_examples.context import actor, build_context, request


async def demo() -> dict[str, object]:
    context, reader = build_context()
    history = ResourceRef(
        resource_type="timeline_item",
        resource_id="previous-turn",
        resource_version="1",
        scope=ResourceScope(scope_id=actor().scope_id),
    )
    reader.sources[history] = LoadedSource(
        resource=history, content="User corrected actual hours to 800, not 900.", required=True
    )
    prepared = await context.prepare(request("What was the corrected actual hours?", (history,)))
    # A teaching-only extractor output. Production uses SqlContextCompression + real Provider.
    original = "Actual hours: 800, not 900.\nPending: verify workshop B.\n" + "Noise.\n" * 200
    compressed = verified_extraction(original, '{"keep":[0,1]}')
    return {
        "snapshotId": str(prepared.snapshot_id),
        "summary": compressed,
        "compressionRequest": render_source(
            original, task="Preserve corrected hours and pending work"
        ),
    }


if __name__ == "__main__":
    import asyncio
    import json

    print(json.dumps(asyncio.run(demo()), indent=2))

"""A durable no-gain observation is reused across compressor instances, not invalid output."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from test_context_runtime import Reader, request

from agent_runtime.compression import COMPRESSION_PROMPT, CompressionNoGain
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.model import ModelConfig
from agent_runtime.storage.compression import SqlContextCompression


@pytest.mark.asyncio
async def test_durable_no_gain_is_reused_without_model_and_still_revalidates_source():
    req = request()
    reader = Reader(required=False)
    source = await reader.read(req, req.resource_refs[0])
    model = ModelConfig(
        ref=req.model,
        connection=req.model,
        model_name="test",
        max_output_tokens=100,
        thinking="disabled",
        provider_options={},
    )
    key = content_digest(
        {
            "source": source.resource.model_dump(mode="json", by_alias=True),
            "content": source.content,
            "task": req.user_input,
            "prompt": req.prompt_text,
            "model": model.model_dump(mode="json", by_alias=True),
            "compressionPrompt": COMPRESSION_PROMPT,
        }
    )
    persisted = json.dumps({"compressionKey": key, "noGain": True, "text": '{"keep":[0]}'})

    class Db:
        async def execute(self, *args):
            return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [persisted]))

    @asynccontextmanager
    async def sessions():
        yield Db()

    # Reconstruct independent instances as a Worker restart would. Neither has a
    # Provider: reaching the model path is a test failure, not a hidden mock success.
    for _ in range(2):
        compressor = object.__new__(SqlContextCompression)
        compressor._actor, compressor._model = req.actor, model
        compressor._lock, compressor._sessions = asyncio.Lock(), sessions
        compressor._schema, compressor._reader = "test_schema", reader
        compressor._estimator = SimpleNamespace(count=len)
        with pytest.raises(CompressionNoGain):
            await compressor.compress(req, source)
    compressor._reader = Reader(required=False, changed=True)
    with pytest.raises(AgentRuntimeError):
        await compressor.compress(req, source)

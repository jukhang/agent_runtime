"""Real PostgreSQL no-gain marker publication and reuse across compressor instances."""

import asyncio
from types import SimpleNamespace

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker
from test_compression_no_gain_cache import COMPRESSION_PROMPT
from test_context_runtime import Reader, request
from test_storage_uow import DSN, _seed_attempt, actor, engine_for, run
from test_storage_uow import schema as schema  # noqa: F401
from test_storage_uow import world as world  # noqa: F401

from agent_runtime.compression import CompressionNoGain
from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.model import ModelConfig
from agent_runtime.storage.compression import SqlContextCompression
from agent_runtime.storage.context_blobs import SqlContextBlobBackend

pytestmark = pytest.mark.skipif(not DSN, reason="requires guarded real PostgreSQL")


def test_no_gain_marker_survives_reconstruction_without_new_model_attempt(world):
    async def exercise():
        engine = engine_for(world.schema, search_path=False)
        try:
            attempt = await _seed_attempt(world)
            req = request().model_copy(
                update={
                    "actor": actor(world=world),
                    "session_id": world.session_id,
                    "run_id": world.run_id,
                }
            )
            source_ref = req.resource_refs[0].model_copy(
                update={
                    "scope": req.resource_refs[0].scope.model_copy(update={"scope_id": world.scope})
                }
            )
            req = req.model_copy(update={"resource_refs": (source_ref,)})
            source = await Reader(required=False).read(req, source_ref)
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
                    "source": source_ref.model_dump(mode="json", by_alias=True),
                    "content": source.content,
                    "task": req.user_input,
                    "prompt": req.prompt_text,
                    "model": model.model_dump(mode="json", by_alias=True),
                    "compressionPrompt": COMPRESSION_PROMPT,
                }
            )

            def compressor():
                value = object.__new__(SqlContextCompression)
                value._actor, value._model = req.actor, model
                value._lock = asyncio.Lock()
                value._sessions = async_sessionmaker(engine, expire_on_commit=False)
                value._schema, value._reader = world.schema, Reader(required=False)
                value._estimator = SimpleNamespace(count=len)
                value._payloads = BlobContextPayloadStore(
                    SqlContextBlobBackend(engine, world.schema)
                )
                return value

            call = SimpleNamespace(
                scope_id=world.scope,
                run_id=world.run_id,
                attempt_id=attempt,
                model=model,
                input_ref="input:1",
            )
            first = compressor()
            await first._record_candidate(call, '{"keep":[0]}')
            await first._record_candidate(call, '{"keep":[0]}', no_gain_key=key)
            with pytest.raises(CompressionNoGain):
                await compressor().compress(req, source)
        finally:
            await engine.dispose()

    run(exercise)

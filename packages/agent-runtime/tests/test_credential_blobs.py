"""Private Broker refs are actor-scoped even when the backing Blob is shared."""

from uuid import uuid4

import pytest

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.credential_blobs import SqlBrokerBlobs


@pytest.mark.asyncio
async def test_broker_blob_actor_and_size_boundaries():
    actor = ActorContext(
        scope_id="test-scope", actor_id="owner", auth_revision="1", grants_ref="grant"
    )
    other = actor.model_copy(update={"actor_id": "other"})

    class Backend:
        data = {}

        async def put_immutable(self, scope_id, key, value):
            self.data[(scope_id, key)] = value

        async def get(self, scope_id, key):
            return self.data[(scope_id, key)]

    blob = SqlBrokerBlobs(Backend(), actor=actor)
    ref = await blob.put_private(actor, uuid4(), b"private-response")
    assert await blob.read(actor, ref, 32) == b"private-response"
    with pytest.raises(AgentRuntimeError):
        await blob.read(other, ref, 32)
    with pytest.raises(AgentRuntimeError):
        await blob.put_private(other, uuid4(), b"other")
    with pytest.raises(AgentRuntimeError):
        await blob.read(actor, ref, 1)

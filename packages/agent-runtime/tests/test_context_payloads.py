"""Shared-blob adapter must scope and verify durable context bytes."""

import pytest

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.failure import AgentRuntimeError


class Backend:
    def __init__(self) -> None:
        self.data: dict[tuple[str, str], bytes] = {}

    async def put_immutable(self, scope_id: str, key: str, data: bytes) -> None:
        if (scope_id, key) in self.data and self.data[scope_id, key] != data:
            raise ValueError("immutable blob conflict")
        self.data[scope_id, key] = data

    async def get(self, scope_id: str, key: str) -> bytes:
        return self.data[scope_id, key]


@pytest.mark.asyncio
async def test_content_addressed_roundtrip_and_scope_isolation() -> None:
    backend = Backend()
    store = BlobContextPayloadStore(backend)
    payload: dict[str, object] = {"sources": [{"content": "测试"}]}
    digest = content_digest(payload)
    ref = await store.put("enterprise-a", payload, digest)
    assert await store.get("enterprise-a", ref) == payload
    assert await store.put("enterprise-a", payload, digest) == ref
    assert len(backend.data) == 1
    with pytest.raises(AgentRuntimeError):
        await store.get("enterprise-b", ref)
    backend.data["enterprise-a", ref] = b'{"sources":[]}'
    with pytest.raises(AgentRuntimeError):
        await store.get("enterprise-a", ref)


@pytest.mark.asyncio
async def test_rejects_wrong_digest_and_oversize_without_write() -> None:
    backend = Backend()
    store = BlobContextPayloadStore(backend, max_bytes=20)
    payload: dict[str, object] = {"content": "a" * 50}
    with pytest.raises(AgentRuntimeError):
        await store.put("enterprise-a", payload, content_digest(payload))
    with pytest.raises(AgentRuntimeError):
        await BlobContextPayloadStore(backend).put("enterprise-a", {"ok": True}, "a" * 64)
    assert not backend.data

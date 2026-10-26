"""Shared private Broker blobs scoped to one authenticated Runtime actor."""

import hashlib
from uuid import UUID

from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.uow import errors


class SqlBrokerBlobs:
    """Content-addressed storage; opaque refs are never published to model events."""

    def __init__(self, backend: SqlContextBlobBackend, *, actor: ActorContext) -> None:
        self._backend, self._bound_actor = backend, actor

    def _require_actor(self, caller: ActorContext) -> None:
        if caller != self._bound_actor:
            errors.forbidden("private credential blob belongs to another actor")

    async def read(self, caller: ActorContext, ref: str, max_bytes: int) -> bytes:
        self._require_actor(caller)
        if max_bytes < 1:
            errors.invalid_input("private credential blob limit must be positive")
        content = await self._backend.get(caller.scope_id, ref)
        if len(content) > max_bytes:
            errors.invalid_input("private credential blob exceeds the requested limit")
        return content

    async def put_private(self, caller: ActorContext, request_id: UUID, data: bytes) -> str:
        self._require_actor(caller)
        if not data:
            errors.invalid_input("private credential blob cannot be empty")
        scope_hash = hashlib.sha256(caller.scope_id.encode("utf-8")).hexdigest()
        digest = hashlib.sha256(data).hexdigest()
        key = f"context-{scope_hash}-{digest}"
        await self._backend.put_immutable(caller.scope_id, key, data)
        return key


__all__ = ["SqlBrokerBlobs"]

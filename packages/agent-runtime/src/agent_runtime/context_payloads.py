"""Content-addressed context payloads over an injected durable shared blob backend."""

import hashlib
import json
from typing import Protocol

from agent_runtime.contracts.canonical import (
    CanonicalJSONError,
    canonical_bytes,
    content_digest,
)
from agent_runtime.contracts.failure import runtime_failure


def context_blob_key(scope_id: str, digest: str) -> str:
    scope = hashlib.sha256(scope_id.encode("utf-8")).hexdigest()
    return f"context-{scope}-{digest}"


class ContextBlobBackend(Protocol):
    async def put_immutable(self, scope_id: str, key: str, data: bytes) -> None:
        """Create once or verify identical bytes; return only after durable visibility."""
        ...

    async def get(self, scope_id: str, key: str) -> bytes: ...


class BlobContextPayloadStore:
    def __init__(self, backend: ContextBlobBackend, *, max_bytes: int = 16_777_216) -> None:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self._backend = backend
        self._max_bytes = max_bytes

    async def put(self, scope_id: str, payload: dict[str, object], digest: str) -> str:
        data = canonical_bytes(payload)
        if len(data) > self._max_bytes:
            raise runtime_failure(
                "BUDGET_EXHAUSTED",
                category="context",
                public_message="Context payload exceeds the storage limit.",
            )
        if content_digest(payload) != digest:
            raise runtime_failure(
                "INVALID_INPUT", category="context", public_message="Context digest mismatch."
            )
        ref = self._ref(scope_id, digest)
        await self._backend.put_immutable(scope_id, ref, data)
        return ref

    async def get(self, scope_id: str, ref: str) -> dict[str, object]:
        prefix = self._ref(scope_id, "")
        if not ref.startswith(prefix) or len(ref) != len(prefix) + 64:
            raise runtime_failure(
                "FORBIDDEN",
                category="authorization",
                public_message="Context payload scope mismatch.",
            )
        data = await self._backend.get(scope_id, ref)
        if len(data) > self._max_bytes or hashlib.sha256(data).hexdigest() != ref[len(prefix) :]:
            raise runtime_failure(
                "INVALID_INPUT", category="context", public_message="Context payload is corrupt."
            )
        try:
            value = json.loads(data)
            valid = isinstance(value, dict) and canonical_bytes(value) == data
        except (json.JSONDecodeError, UnicodeDecodeError, CanonicalJSONError):
            valid = False
            value = None
        if not valid:
            raise runtime_failure(
                "INVALID_INPUT", category="context", public_message="Context payload is invalid."
            )
        assert isinstance(value, dict)
        return value

    @staticmethod
    def _ref(scope_id: str, digest: str) -> str:
        return context_blob_key(scope_id, digest)

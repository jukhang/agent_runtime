"""Durable shared context blobs in the Runtime-owned PostgreSQL schema."""

import hashlib
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.storage.uow.schema import require_schema, scoped

MAX_CONTEXT_BLOB_BYTES = 16_777_216


class SqlContextBlobBackend:
    def __init__(self, engine: AsyncEngine, schema: str) -> None:
        schema = require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._insert = scoped(
            "INSERT INTO {t}context_blob "
            "(scope_id, blob_key, digest, size_bytes, content) "
            "VALUES (:scope_id, :blob_key, :digest, :size_bytes, :content) "
            "ON CONFLICT (scope_id, blob_key) DO NOTHING",
            schema,
        )
        self._select = scoped(
            "SELECT digest, size_bytes, content FROM {t}context_blob "
            "WHERE scope_id = :scope_id AND blob_key = :blob_key",
            schema,
        )
        self._lock = scoped(
            "SELECT digest, size_bytes, content FROM {t}context_blob "
            "WHERE scope_id = :scope_id AND blob_key = :blob_key FOR KEY SHARE",
            schema,
        )
        self._collect = scoped(
            "WITH candidates AS ("
            "SELECT b.scope_id, b.blob_key FROM {t}context_blob b "
            "WHERE b.created_at < :before AND NOT EXISTS ("
            "SELECT 1 FROM {t}context_snapshot s "
            "WHERE s.scope_id = b.scope_id AND s.payload_ref = b.blob_key) "
            "AND NOT EXISTS (SELECT 1 FROM {t}checkpoint c "
            "WHERE c.scope_id = b.scope_id AND c.state_ref = b.blob_key) "
            "AND NOT EXISTS (SELECT 1 FROM {t}projection p "
            "WHERE p.scope_id = b.scope_id AND p.state_ref = b.blob_key) "
            "AND NOT EXISTS (SELECT 1 FROM {t}tool_call t "
            "WHERE t.scope_id = b.scope_id AND t.result_ref = b.blob_key) "
            "AND NOT EXISTS (SELECT 1 FROM {t}timeline_item i "
            "WHERE i.scope_id = b.scope_id AND i.content_ref = b.blob_key) "
            "AND NOT EXISTS (SELECT 1 FROM {t}turn t "
            "WHERE t.scope_id = b.scope_id AND t.input_ref = b.blob_key) "
            "AND NOT EXISTS (SELECT 1 FROM {t}task t "
            "WHERE t.scope_id = b.scope_id AND t.acceptance_spec_ref = b.blob_key) "
            "AND NOT EXISTS (SELECT 1 FROM {t}model_attempt m "
            "WHERE m.scope_id = b.scope_id "
            "AND (m.input_ref = b.blob_key OR m.output_ref = b.blob_key)) "
            "AND NOT EXISTS (SELECT 1 FROM {t}credential_request r "
            "WHERE r.scope_id = b.scope_id "
            "AND r.response->>'bodyRef' = b.blob_key) "
            "AND NOT EXISTS (SELECT 1 FROM {t}capability_revision r "
            "WHERE r.scope_id = b.scope_id AND r.status IN ('draft', 'active', 'superseded') "
            "AND r.manifest->'spec'->>'kind' = 'skill' "
            "AND EXISTS (SELECT 1 FROM jsonb_array_elements("
            "r.manifest->'spec'->'spec'->'files') AS f "
            "WHERE f->>'blobRef' = b.blob_key)) "
            "ORDER BY b.created_at LIMIT :limit FOR UPDATE OF b SKIP LOCKED) "
            "DELETE FROM {t}context_blob b USING candidates c "
            "WHERE b.scope_id = c.scope_id AND b.blob_key = c.blob_key "
            "RETURNING b.blob_key",
            schema,
        )

    async def put_immutable(self, scope_id: str, key: str, data: bytes) -> None:
        async with self._sessions() as session, session.begin():
            await self.put_in_transaction(session, scope_id, key, data)

    async def put_in_transaction(
        self, session: AsyncSession, scope_id: str, key: str, data: bytes
    ) -> None:
        """Keep a new resource and its immutable payload in the same transaction."""
        if not session.in_transaction():
            raise ValueError("blob writes require an active transaction")
        digest = self._check_key(scope_id, key)
        if len(data) > MAX_CONTEXT_BLOB_BYTES or hashlib.sha256(data).hexdigest() != digest:
            raise runtime_failure(
                "INVALID_INPUT", category="context", public_message="Context blob is invalid."
            )
        await session.execute(
            self._insert,
            {
                "scope_id": scope_id,
                "blob_key": key,
                "digest": digest,
                "size_bytes": len(data),
                "content": data,
            },
        )
        row = (
            (await session.execute(self._lock, {"scope_id": scope_id, "blob_key": key}))
            .mappings()
            .one()
        )
        if (
            row["digest"] != digest
            or row["size_bytes"] != len(data)
            or bytes(row["content"]) != data
        ):
            raise runtime_failure(
                "VERSION_CONFLICT",
                category="context",
                public_message="Context blob identity conflicts with stored content.",
            )

    async def get(self, scope_id: str, key: str) -> bytes:
        async with self._sessions() as session:
            return await self.get_in_transaction(session, scope_id, key)

    async def get_in_transaction(self, session: AsyncSession, scope_id: str, key: str) -> bytes:
        digest = self._check_key(scope_id, key)
        row = (
            (await session.execute(self._select, {"scope_id": scope_id, "blob_key": key}))
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise runtime_failure(
                "INVALID_INPUT", category="context", public_message="Context blob was not found."
            )
        content = bytes(row["content"])
        if (
            row["digest"] != digest
            or row["size_bytes"] != len(content)
            or hashlib.sha256(content).hexdigest() != digest
        ):
            raise runtime_failure(
                "INVALID_INPUT", category="context", public_message="Context blob is corrupt."
            )
        return content

    async def collect_unreferenced(self, *, before: datetime, limit: int = 100) -> int:
        """Delete only old, unreferenced blobs; concurrent snapshot commits hold a key lock."""

        if before.tzinfo is None or before.utcoffset() is None:
            raise ValueError("before must be timezone-aware")
        if not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        async with self._sessions() as session, session.begin():
            rows = (await session.execute(self._collect, {"before": before, "limit": limit})).all()
            return len(rows)

    @staticmethod
    def _check_key(scope_id: str, key: str) -> str:
        scope_hash = hashlib.sha256(scope_id.encode("utf-8")).hexdigest()
        prefix = f"context-{scope_hash}-"
        digest = key[len(prefix) :] if key.startswith(prefix) else ""
        if len(digest) != 64 or any(ch not in "0123456789abcdef" for ch in digest):
            raise runtime_failure(
                "FORBIDDEN", category="authorization", public_message="Context blob scope mismatch."
            )
        return digest


__all__ = ["SqlContextBlobBackend"]

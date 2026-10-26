"""PostgreSQL persistence for immutable, run-scoped context snapshots."""

from __future__ import annotations

import json
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.contracts.context import PreparedContext
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlContextSnapshotStore:
    def __init__(self, engine: AsyncEngine, schema: str) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        schema = require_schema(schema)
        self._lock = scoped(
            "SELECT r.session_id, s.auth_revision, s.deleted_at FROM {t}run r "
            "JOIN {t}session s ON s.scope_id = r.scope_id AND s.id = r.session_id "
            "WHERE r.scope_id = :scope_id AND r.id = :run_id FOR UPDATE OF r, s",
            schema,
        )
        self._version = scoped(
            "SELECT COALESCE(MAX(context_version), 0) FROM {t}context_snapshot "
            "WHERE scope_id = :scope_id AND run_id = :run_id",
            schema,
        )
        self._blob = scoped(
            "SELECT digest FROM {t}context_blob "
            "WHERE scope_id = :scope_id AND blob_key = :blob_key FOR KEY SHARE",
            schema,
        )
        self._insert = scoped(
            "INSERT INTO {t}context_snapshot "
            "(id, scope_id, session_id, run_id, auth_revision, context_version, "
            "estimated_input_tokens, schema_revision, task_type, payload_ref, content_hash, "
            "source_refs, degradation, model_ref, prompt_ref, tool_refs, budget) "
            "VALUES (:id, :scope_id, :session_id, :run_id, :auth_revision, :context_version, "
            ":estimated_input_tokens, 1, 'model', :payload_ref, :content_hash, "
            "CAST(:source_refs AS jsonb), CAST(:degradation AS jsonb), "
            "CAST(:model_ref AS jsonb), CAST(:prompt_ref AS jsonb), "
            "CAST(:tool_refs AS jsonb), CAST(:budget AS jsonb))",
            schema,
        )
        self._get = scoped(
            "SELECT * FROM {t}context_snapshot WHERE scope_id = :scope_id AND id = :id",
            schema,
        )

    async def save(self, draft: PreparedContext) -> PreparedContext:
        async with self._sessions() as session, session.begin():
            owner = (
                (
                    await session.execute(
                        self._lock, {"scope_id": draft.scope_id, "run_id": draft.run_id}
                    )
                )
                .mappings()
                .one_or_none()
            )
            if (
                owner is None
                or owner["session_id"] != draft.session_id
                or owner["deleted_at"] is not None
            ):
                raise runtime_failure(
                    "FORBIDDEN",
                    category="authorization",
                    public_message="Context run is unavailable in this session.",
                )
            if owner["auth_revision"] != int(draft.auth_revision):
                raise runtime_failure(
                    "VERSION_CONFLICT",
                    category="authorization",
                    public_message="Context authorization changed before snapshot commit.",
                )
            blob_digest = (
                await session.execute(
                    self._blob,
                    {"scope_id": draft.scope_id, "blob_key": draft.payload_ref},
                )
            ).scalar_one_or_none()
            if blob_digest != draft.content_hash:
                raise runtime_failure(
                    "INVALID_INPUT",
                    category="context",
                    public_message="Context payload is not durably available.",
                )
            version = (
                int(
                    (
                        await session.execute(
                            self._version, {"scope_id": draft.scope_id, "run_id": draft.run_id}
                        )
                    ).scalar_one()
                )
                + 1
            )
            snapshot = draft.model_copy(update={"context_version": str(version)})
            await session.execute(
                self._insert,
                {
                    "id": snapshot.snapshot_id,
                    "scope_id": snapshot.scope_id,
                    "session_id": snapshot.session_id,
                    "run_id": snapshot.run_id,
                    "auth_revision": int(snapshot.auth_revision),
                    "context_version": version,
                    "estimated_input_tokens": snapshot.estimated_input_tokens,
                    "payload_ref": snapshot.payload_ref,
                    "content_hash": snapshot.content_hash,
                    "source_refs": _json(
                        [s.model_dump(mode="json", by_alias=True) for s in snapshot.sources]
                    ),
                    "degradation": _json(
                        [s.source_id for s in snapshot.sources if s.disposition != "included"]
                    ),
                    "model_ref": _json(snapshot.model.model_dump(mode="json", by_alias=True)),
                    "prompt_ref": _json(snapshot.prompt.model_dump(mode="json", by_alias=True)),
                    "tool_refs": _json(
                        [t.model_dump(mode="json", by_alias=True) for t in snapshot.tools]
                    ),
                    "budget": _json(snapshot.budget.model_dump(mode="json", by_alias=True)),
                },
            )
            return snapshot

    async def get(self, scope_id: str, snapshot_id: UUID) -> PreparedContext:
        async with self._sessions() as session:
            row = (
                (await session.execute(self._get, {"scope_id": scope_id, "id": snapshot_id}))
                .mappings()
                .one_or_none()
            )
        if row is None:
            raise runtime_failure(
                "INVALID_INPUT",
                category="context",
                public_message="Context snapshot was not found.",
            )
        if row["schema_revision"] != 1:
            raise runtime_failure(
                "INVALID_INPUT",
                category="context",
                public_message="Context snapshot representation is unsupported.",
            )
        return PreparedContext.model_validate(
            {
                "scope_id": row["scope_id"],
                "session_id": row["session_id"],
                "run_id": row["run_id"],
                "auth_revision": str(row["auth_revision"]),
                "snapshot_id": row["id"],
                "context_version": str(row["context_version"]),
                "payload_ref": row["payload_ref"],
                "content_hash": row["content_hash"],
                "estimated_input_tokens": row["estimated_input_tokens"],
                "sources": row["source_refs"],
                "model": row["model_ref"],
                "prompt": row["prompt_ref"],
                "tools": row["tool_refs"],
                "budget": row["budget"],
            }
        )


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


__all__ = ["SqlContextSnapshotStore"]

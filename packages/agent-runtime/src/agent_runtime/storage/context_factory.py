"""Explicit SQL Context assembly; the host still owns its model tokenizer."""

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.context_runtime import ContextRuntime, TokenEstimator
from agent_runtime.storage.context_access import SqlContextAccessVerifier
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.context_snapshots import SqlContextSnapshotStore
from agent_runtime.storage.context_sources import SqlRuntimeSourceReader


@dataclass(frozen=True, slots=True)
class SqlContextComponents:
    runtime: ContextRuntime
    access: SqlContextAccessVerifier


def build_sql_context_components(
    engine: AsyncEngine, schema: str, *, estimator: TokenEstimator, optional_history: bool = False
) -> SqlContextComponents:
    """Compose the owner-only Runtime sources and shared PostgreSQL payload store."""

    blobs = SqlContextBlobBackend(engine, schema)
    reader = SqlRuntimeSourceReader(engine, schema, blobs=blobs, optional_history=optional_history)
    return SqlContextComponents(
        runtime=ContextRuntime(
            reader=reader,
            estimator=estimator,
            payloads=BlobContextPayloadStore(blobs),
            snapshots=SqlContextSnapshotStore(engine, schema),
        ),
        access=SqlContextAccessVerifier(engine, schema, resources=reader),
    )


__all__ = ["SqlContextComponents", "build_sql_context_components"]

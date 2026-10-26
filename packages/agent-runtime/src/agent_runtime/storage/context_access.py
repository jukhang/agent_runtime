"""Preflight live authority for a frozen context before any model request."""

from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.contracts.context import PreparedContext
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.uow.schema import require_schema, scoped


class ContextResourceAccess(Protocol):
    async def authorize_session(self, actor: ActorContext, session_id: UUID) -> None: ...

    async def authorize_resource(self, actor: ActorContext, resource: ResourceRef) -> None:
        """Deny deleted, stale-version or no-longer-readable resources."""
        ...


class TransactionalResourceAccess(Protocol):
    async def authorize_session_in_transaction(
        self, session: AsyncSession, actor: ActorContext, session_id: UUID
    ) -> None:
        """Use the caller's transaction and hold relevant authorization locks."""
        ...

    async def authorize_resource_in_transaction(
        self,
        session: AsyncSession,
        actor: ActorContext,
        resource: ResourceRef,
        *,
        session_id: UUID,
    ) -> None: ...


class SqlContextAccessVerifier:
    """Check DB authority revision; delegate host-specific ACL and resource versions."""

    def __init__(
        self, engine: AsyncEngine, schema: str, *, resources: ContextResourceAccess
    ) -> None:
        schema = require_schema(schema)
        self._schema = schema
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._resources = resources
        self._current = scoped(
            "SELECT r.session_id, s.auth_revision, s.deleted_at, r.status, r.desired_control "
            "FROM {t}run r JOIN {t}session s "
            "ON s.scope_id = r.scope_id AND s.id = r.session_id "
            "WHERE r.scope_id = :scope_id AND r.id = :run_id",
            schema,
        )

    async def verify(self, actor: ActorContext, snapshot: PreparedContext) -> None:
        if actor.scope_id != snapshot.scope_id:
            raise runtime_failure(
                "FORBIDDEN", category="authorization", public_message="Context scope changed."
            )
        async with self._sessions() as session:
            row = (
                (
                    await session.execute(
                        self._current,
                        {"scope_id": snapshot.scope_id, "run_id": snapshot.run_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            from agent_runtime.storage.child_access import require_child_resources

            refs = tuple(
                ref
                for source in snapshot.sources
                if source.disposition in {"included", "summarized"}
                for ref in (source.resource, source.summary_ref)
                if ref is not None
            )
            await require_child_resources(
                session, self._schema, actor, snapshot.run_id, refs, snapshot.tools
            )
        if row is None or row["session_id"] != snapshot.session_id or row["deleted_at"] is not None:
            raise runtime_failure(
                "FORBIDDEN",
                category="authorization",
                public_message="Context run is unavailable.",
            )
        if row["status"] != "running" or row["desired_control"] is not None:
            raise runtime_failure(
                "FENCED", category="concurrency", public_message="Context run is not active."
            )
        if (
            row["auth_revision"] != int(snapshot.auth_revision)
            or actor.auth_revision != snapshot.auth_revision
        ):
            raise runtime_failure(
                "VERSION_CONFLICT",
                category="authorization",
                public_message="Context authorization changed.",
            )
        await self._resources.authorize_session(actor, snapshot.session_id)
        for source in snapshot.sources:
            if source.disposition in {"included", "summarized"}:
                await self._resources.authorize_resource(actor, source.resource)
                if source.summary_ref is not None:
                    await self._resources.authorize_resource(actor, source.summary_ref)


__all__ = ["ContextResourceAccess", "SqlContextAccessVerifier", "TransactionalResourceAccess"]

"""Explicit session lifecycle for independent hosts, using Runtime's standard table."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.uow.schema import require_schema, scoped

type SessionAuthorizer = Callable[[AsyncSession, ActorContext], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class SessionInfo:
    id: UUID
    title: str | None
    created_at: datetime
    updated_at: datetime


class SqlSessionStore:
    def __init__(self, engine: AsyncEngine, schema: str, *, authorize: SessionAuthorizer) -> None:
        self._schema = require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._authorize = authorize

    async def create(self, actor: ActorContext, *, title: str | None = None) -> SessionInfo:
        if title is not None and (not title.strip() or len(title) > 255):
            raise ValueError("Session title must contain 1–255 characters")
        identity = uuid4()
        async with self._sessions() as db, db.begin():
            await self._authorize(db, actor)
            row = (
                (
                    await db.execute(
                        scoped(
                            "INSERT INTO {t}session "
                            "(id,scope_id,owner_actor_id,title_source,title,event_seq,"
                            "auth_revision) "
                            "VALUES (:id,:scope,:actor,:source,:title,0,:revision) "
                            "RETURNING id,title,created_at,updated_at",
                            self._schema,
                        ),
                        {
                            "id": identity,
                            "scope": actor.scope_id,
                            "actor": actor.actor_id,
                            "source": "none" if title is None else "manual",
                            "title": title,
                            "revision": int(actor.auth_revision),
                        },
                    )
                )
                .mappings()
                .one()
            )
            return SessionInfo(**dict(row))

    async def list(self, actor: ActorContext, *, limit: int = 100) -> tuple[SessionInfo, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("Session list limit must be 1–1000")
        async with self._sessions() as db, db.begin():
            await self._authorize(db, actor)
            rows = (
                await db.execute(
                    scoped(
                        "SELECT id,title,created_at,updated_at FROM {t}session "
                        "WHERE scope_id=:scope AND owner_actor_id=:actor AND deleted_at IS NULL "
                        "AND auth_revision=:revision ORDER BY updated_at DESC,id DESC LIMIT :limit",
                        self._schema,
                    ),
                    {
                        "scope": actor.scope_id,
                        "actor": actor.actor_id,
                        "revision": int(actor.auth_revision),
                        "limit": limit,
                    },
                )
            ).mappings()
            return tuple(SessionInfo(**dict(row)) for row in rows)


__all__ = ["SessionInfo", "SqlSessionStore"]

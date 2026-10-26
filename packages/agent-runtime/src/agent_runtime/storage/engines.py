"""Async engine and session factory for the standard PostgreSQL adapter.

Every value is an explicit constructor input: the module never reads ``.env``, the host
settings object or any global getter, and importing it opens no connection. A host keeps
the engine and sessionmaker for the lifetime of its application context and closes them
through :meth:`StorageContext.dispose` (DS-P02-01 requirement 1).
"""

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")

#: Migrations and DDL are executed through the same async driver, never at import time.
POSTGRES_DRIVER = "postgresql+asyncpg"


@dataclass(frozen=True, slots=True)
class StorageConfig:
    """Explicit connection and pool configuration for one Runtime deployment."""

    dsn: str
    schema: str = "agent_runtime"
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout_seconds: float = 30.0
    pool_recycle_seconds: int = 1800
    statement_timeout_ms: int = 15_000
    application_name: str = "agent-runtime"
    #: Convenience only: the UoW renders every statement with an explicit ``schema``
    #: prefix (review §10.2 P1-3), so unqualified ad-hoc SQL is the only thing this
    #: affects. It stays opt-in and is never required for correctness.
    set_search_path: bool = False

    def __post_init__(self) -> None:
        if not self.dsn.startswith(POSTGRES_DRIVER + "://"):
            raise ValueError(f"the standard storage adapter requires a {POSTGRES_DRIVER} DSN")
        if not IDENTIFIER.match(self.schema):
            raise ValueError("schema must be a lowercase PostgreSQL identifier")
        if self.pool_size < 1 or self.max_overflow < 0:
            raise ValueError("pool_size must be positive and max_overflow non-negative")
        if self.pool_timeout_seconds <= 0 or self.pool_recycle_seconds <= 0:
            raise ValueError("pool timeouts must be positive")
        if self.statement_timeout_ms <= 0:
            raise ValueError("statement_timeout_ms must be positive")

    def qualified(self, relation: str) -> str:
        """Return ``schema.relation`` for explicit SQL and reflection."""
        return f"{self.schema}.{relation}"

    def safe_summary(self) -> dict[str, Any]:
        """Diagnostics without credentials, host or database names."""
        return {
            "driver": POSTGRES_DRIVER,
            "schema": self.schema,
            "poolSize": self.pool_size,
            "maxOverflow": self.max_overflow,
            "statementTimeoutMs": self.statement_timeout_ms,
            "searchPathSet": self.set_search_path,
        }


def create_storage_engine(config: StorageConfig) -> AsyncEngine:
    """Build the async engine. No connection is opened until first use.

    ``statement_timeout`` is applied per connection through asyncpg ``server_settings``,
    so the value advertised by :meth:`StorageConfig.safe_summary` is the value PostgreSQL
    enforces (review §8.2 P2-1). ``search_path`` stays opt-in: the standard adapter
    qualifies every table with :attr:`StorageConfig.schema`, so the default configuration
    is safe without it (review §10.2 P1-3).
    """
    server_settings = {
        "application_name": config.application_name,
        "statement_timeout": f"{config.statement_timeout_ms}ms",
    }
    if config.set_search_path:
        server_settings["search_path"] = f"{config.schema},public"
    return create_async_engine(
        config.dsn,
        pool_size=config.pool_size,
        max_overflow=config.max_overflow,
        pool_timeout=config.pool_timeout_seconds,
        pool_recycle=config.pool_recycle_seconds,
        pool_pre_ping=True,
        connect_args={
            "timeout": config.pool_timeout_seconds,
            "server_settings": server_settings,
        },
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Build the sessionmaker for an engine owned by the caller."""
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


@dataclass(slots=True)
class StorageContext:
    """Engine plus sessionmaker with an explicit close boundary."""

    config: StorageConfig
    engine: AsyncEngine = field(init=False)
    session_factory: async_sessionmaker[AsyncSession] = field(init=False)

    def __post_init__(self) -> None:
        self.engine = create_storage_engine(self.config)
        self.session_factory = create_session_factory(self.engine)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Yield a session: this is a *session lifetime* boundary only.

        No transaction is started, committed or rolled back here; the caller stays in
        SQLAlchemy's autobegin semantics and owns every commit. Use
        :meth:`transaction` when an explicit all-or-nothing boundary is wanted. Business
        unit-of-work semantics belong to DS-P02-02, not to this adapter.
        """
        async with self.session_factory() as session:
            yield session

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncSession]:
        """Yield a session inside one explicit transaction: commit, or roll back on error.

        This is a transport-level boundary (``BEGIN``/``COMMIT``/``ROLLBACK``) only. It
        deliberately contains no business unit-of-work logic such as epoch, lease or
        authority checks; those arrive with DS-P02-02.
        """
        async with self.session_factory() as session:
            async with session.begin():
                yield session

    async def dispose(self) -> None:
        await self.engine.dispose()

    def safe_summary(self) -> dict[str, Any]:
        return self.config.safe_summary()


__all__ = [
    "POSTGRES_DRIVER",
    "StorageConfig",
    "StorageContext",
    "create_session_factory",
    "create_storage_engine",
]

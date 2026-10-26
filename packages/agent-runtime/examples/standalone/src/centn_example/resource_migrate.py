"""Explicit host-only Alembic entry; never adds application tables to Runtime schema."""

import hashlib
from pathlib import Path

import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import create_async_engine

from agent_runtime.storage.uow.schema import require_schema


def resource_schema(runtime_schema: str) -> str:
    value = require_schema(f"{runtime_schema}_host")
    if len(value.encode("ascii")) > 63:
        raise ValueError("Host resource schema exceeds PostgreSQL identifier bound")
    return value


def config(schema: str) -> Config:
    value = Config()
    value.set_main_option("script_location", str(Path(__file__).parent / "resource_migrations"))
    value.attributes["resource_schema"] = require_schema(schema)
    return value


async def upgrade(dsn: str, runtime_schema: str) -> None:
    schema = resource_schema(runtime_schema)
    engine = create_async_engine(dsn)
    try:
        async with engine.begin() as db:
            lock = int.from_bytes(
                hashlib.sha256(f"host-resource:{schema}".encode()).digest()[:8], "big", signed=True
            )
            await db.execute(sa.text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock})
            owner = (
                await db.execute(
                    sa.text(
                        "SELECT pg_get_userbyid(nspowner)=current_user "
                        "FROM pg_namespace WHERE nspname=:schema"
                    ),
                    {"schema": schema},
                )
            ).scalar_one_or_none()
            if owner is False:
                raise ValueError("Host resource schema is owned by another role")
            if owner is not None:
                objects = (
                    (
                        await db.execute(
                            sa.text(
                                "SELECT c.relname FROM pg_class c "
                                "JOIN pg_namespace n ON n.oid=c.relnamespace "
                                "WHERE n.nspname=:schema AND c.relkind IN ('r','p','v','m','f','S')"
                            ),
                            {"schema": schema},
                        )
                    )
                    .scalars()
                    .all()
                )
                if set(objects) - {"resource", "sandbox_allocation", "alembic_version"}:
                    raise ValueError("Host resource schema contains unrelated objects")
            await db.execute(sa.schema.CreateSchema(schema, if_not_exists=True))

            def migrate(connection):
                current = config(schema)
                current.attributes["connection"] = connection
                command.upgrade(current, "head")

            await db.run_sync(migrate)
    finally:
        await engine.dispose()

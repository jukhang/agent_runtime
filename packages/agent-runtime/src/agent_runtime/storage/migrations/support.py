"""Shared helpers for the Runtime migration chain."""

from pathlib import Path
from typing import Any

import sqlalchemy as sa
from alembic.config import Config
from sqlalchemy.dialects import postgresql
from sqlalchemy.types import TypeEngine

#: Config main option / CLI flag carrying the target schema.
SCHEMA_OPTION = "agent_runtime_schema"

#: Production schema owned by the Runtime chain (Q40).
DEFAULT_SCHEMA = "agent_runtime"

#: ``alembic_version`` always lives inside the Runtime schema so the host chain and the
#: public schema stay untouched.
VERSION_TABLE = "alembic_version"

#: Catalog ``SqlType`` -> DDL type. PostgreSQL is the only supported standard target.
SQL_TYPES: dict[str, TypeEngine[Any]] = {
    "UUID": sa.Uuid(),
    "TEXT": sa.Text(),
    "BIGINT": sa.BigInteger(),
    "INTEGER": sa.Integer(),
    "TIMESTAMPTZ": sa.DateTime(timezone=True),
    "JSONB": postgresql.JSONB(),
    "BYTEA": postgresql.BYTEA(),
}


def configured_schema(config: Config) -> str:
    """Resolve the target schema from attributes, main options or the default."""
    schema = config.attributes.get("schema") or config.get_main_option(SCHEMA_OPTION)
    return (schema or DEFAULT_SCHEMA).strip()


def script_location() -> str:
    """Filesystem location of the packaged chain (works from a wheel install)."""
    return str(Path(__file__).resolve().parent)


__all__ = [
    "DEFAULT_SCHEMA",
    "SCHEMA_OPTION",
    "SQL_TYPES",
    "VERSION_TABLE",
    "configured_schema",
    "script_location",
]

"""Explicit schema binding for every UoW statement (review §10.2 P1-3).

The standard :class:`~agent_runtime.storage.engines.StorageConfig` does not force a
``search_path``, so unqualified table names would silently resolve to ``public`` (or fail).
Every statement in :mod:`agent_runtime.storage.uow` is therefore rendered from a template
that carries an explicit, validated ``<schema>.`` prefix; the search path only stays a
convenience and can never decide which ``run`` table is written.
"""

import re

import sqlalchemy as sa

SCHEMA_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")

#: Placeholder for the schema-qualified prefix inside a statement template.
PREFIX = "{t}"


def require_schema(schema: str) -> str:
    """Return a schema usable as a SQL identifier prefix, or raise ``ValueError``."""
    if not SCHEMA_PATTERN.match(schema):
        raise ValueError(f"schema must be a lowercase PostgreSQL identifier: {schema!r}")
    return schema


def scoped(template: str, schema: str) -> sa.TextClause:
    """Render one statement template with the schema-qualified table prefix."""
    return sa.text(template.format(t=f"{require_schema(schema)}."))


__all__ = ["PREFIX", "SCHEMA_PATTERN", "require_schema", "scoped"]

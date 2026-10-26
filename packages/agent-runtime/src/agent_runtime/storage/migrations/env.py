"""Alembic environment for the Runtime-owned chain (Q40).

Runs only through :mod:`agent_runtime.storage.migrate` (or an explicit ``alembic -c`` with
``script_location`` set): the environment never reads host settings, never creates an
engine at import time, and always keeps ``alembic_version`` inside the target schema.
"""

from alembic import context
from sqlalchemy.engine import Connection

from agent_runtime.storage.base import Base
from agent_runtime.storage.migrations.support import (
    VERSION_TABLE,
    configured_schema,
)

config = context.config
target_metadata = Base.metadata


def _configure(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=config.attributes.get("target_metadata", target_metadata),
        include_name=config.attributes.get("include_name"),
        version_table=VERSION_TABLE,
        version_table_schema=configured_schema(config),
        include_schemas=True,
        compare_type=True,
        compare_server_default=True,
    )


def run_migrations_offline() -> None:
    """Offline (``--sql``) generation is deliberately unsupported.

    The Runtime chain must run under the deployment migration role against the real
    schema; emitting unverified SQL would bypass the owner/privilege preflight (Q40).
    """
    raise RuntimeError("the Runtime migration chain requires a live connection")


def run_migrations_online() -> None:
    """Run inside a caller-provided connection, keeping DDL transactional."""
    connection = config.attributes.get("connection")
    if connection is None:
        raise RuntimeError("the Runtime chain requires a caller-provided connection")
    _configure(connection)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

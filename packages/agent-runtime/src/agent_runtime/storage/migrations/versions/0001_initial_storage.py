"""Initial Runtime storage revision: create the schema and the frozen 40 tables.

Revision ID: 0001_initial_storage
Create Date: 2026-09-22

The DDL is driven by this revision's own immutable catalog snapshot
(``catalogs/0001_initial_storage.json``), never by the live
:mod:`agent_runtime.storage_spec` catalog: a later field or revision must not rewrite (or
break) history on a new database (review §8.2 P1-1). :data:`SNAPSHOT_DIGEST` pins the
snapshot bytes this revision was cut from.

Order is tables -> indexes -> foreign keys, because composite scope foreign keys target
unique indexes, not primary keys (prototype finding F1).
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.schema import CreateSchema

from agent_runtime.storage.migrations.snapshot import (
    SnapshotTable,
    load_snapshot,
    snapshot_digest,
)
from agent_runtime.storage.migrations.support import SQL_TYPES, configured_schema

revision = "0001_initial_storage"
down_revision = None
branch_labels = None
depends_on = None

#: Name of the immutable snapshot this revision owns.
SNAPSHOT = "0001_initial_storage"

#: sha256 of ``catalogs/0001_initial_storage.json`` at the moment this revision was cut.
SNAPSHOT_DIGEST = "b76052a18995462c789a649dc6a12771ef598a095b10a8461521eff81f81d1b9"
EXPECTED_TABLES = 40
EXPECTED_COLUMNS = 476


def constraint_name(prefix: str, table: str, offset: int) -> str:
    """Deterministic constraint name shared with the runtime storage probe."""
    return f"{prefix}_{table}_{offset}"


def schema_name() -> str:
    config = op.get_context().config
    if config is None:  # pragma: no cover - Alembic always provides a configuration
        raise RuntimeError("migration context has no configuration")
    return configured_schema(config)


def frozen_tables() -> tuple[SnapshotTable, ...]:
    """The revision's own snapshot, verified against the pinned digest."""
    digest = snapshot_digest(SNAPSHOT)
    if digest != SNAPSHOT_DIGEST:
        raise RuntimeError(
            f"snapshot {SNAPSHOT} was modified (sha256 {digest}); revisions are immutable"
        )
    tables = load_snapshot(SNAPSHOT)
    columns = sum(len(table.columns) for table in tables)
    if len(tables) != EXPECTED_TABLES or columns != EXPECTED_COLUMNS:
        raise RuntimeError(f"snapshot {SNAPSHOT} no longer describes the frozen catalog")
    return tables


def upgrade() -> None:
    schema = schema_name()
    tables = frozen_tables()
    op.execute(CreateSchema(schema, if_not_exists=True))
    for spec in tables:
        op.create_table(
            spec.name,
            *(
                sa.Column(
                    column.name,
                    SQL_TYPES[column.sql_type],
                    nullable=column.nullable,
                    server_default=(
                        sa.text(column.default) if column.default is not None else None
                    ),
                    comment=column.comment,
                )
                for column in spec.columns
            ),
            sa.PrimaryKeyConstraint(*spec.primary_key, name=f"pk_{spec.name}"),
            *(
                sa.CheckConstraint(check.expression, name=constraint_name("ck", spec.name, offset))
                for offset, check in enumerate(spec.checks)
            ),
            schema=schema,
        )
    for spec in tables:
        for offset, index in enumerate(spec.indexes):
            op.create_index(
                constraint_name("ix", spec.name, offset),
                spec.name,
                list(index.columns),
                unique=index.unique,
                schema=schema,
                postgresql_where=sa.text(index.predicate) if index.predicate else None,
            )
    for spec in tables:
        for offset, foreign_key in enumerate(spec.foreign_keys):
            op.create_foreign_key(
                constraint_name("fk", spec.name, offset),
                spec.name,
                foreign_key.target,
                list(foreign_key.columns),
                list(foreign_key.target_columns),
                source_schema=schema,
                referent_schema=schema,
                ondelete=foreign_key.on_delete,
                deferrable=foreign_key.deferrable,
                initially="DEFERRED" if foreign_key.initially_deferred else "IMMEDIATE",
            )


def downgrade() -> None:
    """Drop the Runtime tables, including cycles, leaving a clean schema.

    ``alembic_version`` and the schema itself stay in place: the schema may have been
    pre-created by the deployment and the version row must survive a downgrade-to-base so
    the chain stays repeatable. Dropping the schema is a deployment decision, not a
    migration side effect.
    """
    schema = schema_name()
    tables = frozen_tables()
    for spec in tables:
        for offset, _foreign_key in enumerate(spec.foreign_keys):
            op.drop_constraint(
                constraint_name("fk", spec.name, offset),
                spec.name,
                schema=schema,
                type_="foreignkey",
            )
    for spec in reversed(tables):
        op.drop_table(spec.name, schema=schema)


__all__ = [
    "EXPECTED_COLUMNS",
    "EXPECTED_TABLES",
    "SNAPSHOT",
    "SNAPSHOT_DIGEST",
    "branch_labels",
    "depends_on",
    "downgrade",
    "down_revision",
    "frozen_tables",
    "revision",
    "upgrade",
]

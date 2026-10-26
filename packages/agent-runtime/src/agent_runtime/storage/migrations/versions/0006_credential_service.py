"""Immutable mTLS service-to-grant binding.

Revision ID: 0006_credential_service
Revises: 0005_credential_ledger
"""

import sqlalchemy as sa
from alembic import op

from agent_runtime.storage.migrations.snapshot import load_snapshot, snapshot_digest
from agent_runtime.storage.migrations.support import SQL_TYPES, configured_schema

revision = "0006_credential_service"
down_revision = "0005_credential_ledger"
branch_labels = None
depends_on = None
SNAPSHOT = "0006_credential_service"
SNAPSHOT_DIGEST = "b9741d4869dd47a288457131b124d2fb0fcb48dba503f25a0381474aafeb4afb"
EXPECTED_TABLES = 45
EXPECTED_COLUMNS = 528


def upgrade() -> None:
    if snapshot_digest(SNAPSHOT) != SNAPSHOT_DIGEST:
        raise RuntimeError("service catalog changed after revision freeze")
    tables = load_snapshot(SNAPSHOT)
    if len(tables) != EXPECTED_TABLES or sum(len(t.columns) for t in tables) != EXPECTED_COLUMNS:
        raise RuntimeError("unexpected service catalog shape")
    config = op.get_context().config
    if config is None:
        raise RuntimeError("migration context has no configuration")
    schema = configured_schema(config)
    spec = next(t for t in tables if t.name == "credential_service_binding")
    op.create_table(
        spec.name,
        *(
            sa.Column(
                c.name,
                SQL_TYPES[c.sql_type],
                nullable=c.nullable,
                server_default=sa.text(c.default) if c.default else None,
                comment=c.comment,
            )
            for c in spec.columns
        ),
        sa.PrimaryKeyConstraint(*spec.primary_key, name=f"pk_{spec.name}"),
        *(
            sa.CheckConstraint(c.expression, name=f"ck_{spec.name}_{i}")
            for i, c in enumerate(spec.checks)
        ),
        schema=schema,
    )
    for i, index in enumerate(spec.indexes):
        op.create_index(
            f"ix_{spec.name}_{i}",
            spec.name,
            list(index.columns),
            unique=index.unique,
            schema=schema,
        )
    for i, fk in enumerate(spec.foreign_keys):
        op.create_foreign_key(
            f"fk_{spec.name}_{i}",
            spec.name,
            fk.target,
            list(fk.columns),
            list(fk.target_columns),
            source_schema=schema,
            referent_schema=schema,
            ondelete=fk.on_delete,
            deferrable=fk.deferrable,
            initially="DEFERRED" if fk.initially_deferred else "IMMEDIATE",
        )


def downgrade() -> None:
    config = op.get_context().config
    if config is None:
        raise RuntimeError("migration context has no configuration")
    op.drop_table("credential_service_binding", schema=configured_schema(config))

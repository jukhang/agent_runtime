"""Private credential reservations and audit, isolated from public events.

Revision ID: 0005_credential_ledger
Revises: 0004_input_required
"""

import sqlalchemy as sa
from alembic import op

from agent_runtime.storage.migrations.snapshot import load_snapshot, snapshot_digest
from agent_runtime.storage.migrations.support import SQL_TYPES, configured_schema

revision = "0005_credential_ledger"
down_revision = "0004_input_required"
branch_labels = None
depends_on = None
SNAPSHOT = "0005_credential_ledger"
SNAPSHOT_DIGEST = "3e6a404ffcd0f4971654e81ec075265dca08fe1ecdadddd0d23c2e218f7ed478"
EXPECTED_TABLES = 44
EXPECTED_COLUMNS = 522


def upgrade() -> None:
    if snapshot_digest(SNAPSHOT) != SNAPSHOT_DIGEST:
        raise RuntimeError("credential catalog changed after revision freeze")
    tables = load_snapshot(SNAPSHOT)
    if (
        len(tables) != EXPECTED_TABLES
        or sum(len(table.columns) for table in tables) != EXPECTED_COLUMNS
    ):
        raise RuntimeError("unexpected credential catalog shape")
    config = op.get_context().config
    if config is None:
        raise RuntimeError("migration context has no configuration")
    schema = configured_schema(config)
    added = [
        next(table for table in tables if table.name == name)
        for name in ("credential_request", "credential_audit")
    ]
    for spec in added:
        op.create_table(
            spec.name,
            *(
                sa.Column(
                    column.name,
                    SQL_TYPES[column.sql_type],
                    nullable=column.nullable,
                    server_default=sa.text(column.default) if column.default else None,
                    comment=column.comment,
                )
                for column in spec.columns
            ),
            sa.PrimaryKeyConstraint(*spec.primary_key, name=f"pk_{spec.name}"),
            *(
                sa.CheckConstraint(check.expression, name=f"ck_{spec.name}_{offset}")
                for offset, check in enumerate(spec.checks)
            ),
            schema=schema,
        )
    for spec in added:
        for offset, index in enumerate(spec.indexes):
            op.create_index(
                f"ix_{spec.name}_{offset}",
                spec.name,
                list(index.columns),
                unique=index.unique,
                schema=schema,
            )
    for spec in added:
        for offset, fk in enumerate(spec.foreign_keys):
            op.create_foreign_key(
                f"fk_{spec.name}_{offset}",
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
    schema = configured_schema(config)
    op.drop_table("credential_audit", schema=schema)
    op.drop_table("credential_request", schema=schema)

"""Shared, scope-isolated context payloads for multi-worker recovery.

Revision ID: 0003_context_blob
Revises: 0002_evidence_ownership
"""

import sqlalchemy as sa
from alembic import op

from agent_runtime.storage.migrations.snapshot import load_snapshot, snapshot_digest
from agent_runtime.storage.migrations.support import SQL_TYPES, configured_schema

revision = "0003_context_blob"
down_revision = "0002_evidence_ownership"
branch_labels = None
depends_on = None

SNAPSHOT = "0003_context_blob"
SNAPSHOT_DIGEST = "74e3aa5137ef3c0226e36449bfab46dc05913c52d7c3935125a8e8c6d8856a67"
EXPECTED_TABLES = 42
EXPECTED_COLUMNS = 504


def upgrade() -> None:
    if snapshot_digest(SNAPSHOT) != SNAPSHOT_DIGEST:
        raise RuntimeError("context blob catalog snapshot changed after revision freeze")
    tables = load_snapshot(SNAPSHOT)
    if (
        len(tables) != EXPECTED_TABLES
        or sum(len(table.columns) for table in tables) != EXPECTED_COLUMNS
    ):
        raise RuntimeError("context blob catalog has an unexpected shape")
    table = next(table for table in tables if table.name == "context_blob")
    config = op.get_context().config
    if config is None:
        raise RuntimeError("migration context has no configuration")
    schema = configured_schema(config)
    op.create_table(
        table.name,
        *(
            sa.Column(
                column.name,
                SQL_TYPES[column.sql_type],
                nullable=column.nullable,
                server_default=sa.text(column.default) if column.default is not None else None,
                comment=column.comment,
            )
            for column in table.columns
        ),
        sa.PrimaryKeyConstraint(*table.primary_key, name="pk_context_blob"),
        *(
            sa.CheckConstraint(check.expression, name=f"ck_context_blob_{offset}")
            for offset, check in enumerate(table.checks)
        ),
        schema=schema,
    )


def downgrade() -> None:
    config = op.get_context().config
    if config is None:
        raise RuntimeError("migration context has no configuration")
    op.drop_table("context_blob", schema=configured_schema(config))

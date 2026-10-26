"""Publish durable user-input requests from AskUser transitions.

Revision ID: 0004_input_required
Revises: 0003_context_blob
"""

from alembic import op

from agent_runtime.storage.migrations.snapshot import load_snapshot, snapshot_digest
from agent_runtime.storage.migrations.support import configured_schema

revision = "0004_input_required"
down_revision = "0003_context_blob"
branch_labels = None
depends_on = None

SNAPSHOT = "0004_input_required"
SNAPSHOT_DIGEST = "0b8bc975d34b6da177f5534c8b0640bec35748d1da75b11ee4d86fc5b4806351"
EXPECTED_TABLES = 42
EXPECTED_COLUMNS = 504
PREVIOUS_DIGEST = "74e3aa5137ef3c0226e36449bfab46dc05913c52d7c3935125a8e8c6d8856a67"


def _kind_check(snapshot: str, digest: str) -> str:
    if snapshot_digest(snapshot) != digest:
        raise RuntimeError("event catalog snapshot changed after revision freeze")
    tables = load_snapshot(snapshot)
    if len(tables) != EXPECTED_TABLES or sum(len(t.columns) for t in tables) != EXPECTED_COLUMNS:
        raise RuntimeError("event catalog has an unexpected shape")
    event = next(table for table in tables if table.name == "event")
    if event.checks[4].columns != ("kind",):
        raise RuntimeError("event kind check moved in the frozen catalog")
    return event.checks[4].expression


def _replace_kind_check(expression: str) -> None:
    config = op.get_context().config
    if config is None:
        raise RuntimeError("migration context has no configuration")
    schema = configured_schema(config)
    op.drop_constraint("ck_event_4", "event", schema=schema, type_="check")
    op.create_check_constraint("ck_event_4", "event", expression, schema=schema)


def upgrade() -> None:
    _replace_kind_check(_kind_check(SNAPSHOT, SNAPSHOT_DIGEST))


def downgrade() -> None:
    _replace_kind_check(_kind_check("0003_context_blob", PREVIOUS_DIGEST))

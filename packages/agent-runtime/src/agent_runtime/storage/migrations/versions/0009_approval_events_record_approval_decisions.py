"""Record approval decisions; reviewed CHECK-only change after autogeneration.

Revision ID: 0009_approval_events
Revises: 0008_human_input
"""

from alembic import op

from agent_runtime.storage.migrations.snapshot import load_snapshot, snapshot_digest
from agent_runtime.storage.migrations.support import configured_schema

revision = "0009_approval_events"
down_revision = "0008_human_input"
branch_labels = None
depends_on = None
SNAPSHOT = "0009_approval_events"
SNAPSHOT_DIGEST = "439440abe3eb345a05ed1e7cbc3166a83f08df1b6e5840e67786e9d13138baa6"
EXPECTED_TABLES = 45
EXPECTED_COLUMNS = 528


def _replace(snapshot: str) -> None:
    config = op.get_context().config
    if config is None:
        raise RuntimeError("Migration configuration is missing")
    schema = configured_schema(config)
    event = next(table for table in load_snapshot(snapshot) if table.name == "event")
    expression = next(check.expression for check in event.checks if check.columns == ("kind",))
    op.drop_constraint("ck_event_4", "event", schema=schema, type_="check")
    op.create_check_constraint("ck_event_4", "event", expression, schema=schema)


def upgrade() -> None:
    if snapshot_digest(SNAPSHOT) != SNAPSHOT_DIGEST:
        raise RuntimeError("Approval events catalog snapshot changed")
    _replace(SNAPSHOT)


def downgrade() -> None:
    config = op.get_context().config
    if config is None:
        raise RuntimeError("Migration configuration is missing")
    schema = configured_schema(config)
    if op.get_bind().exec_driver_sql(
        f"SELECT 1 FROM \"{schema}\".event WHERE kind='approval.updated' LIMIT 1"
    ).first():
        raise RuntimeError("Cannot downgrade while approval decision events exist")
    _replace("0008_human_input")

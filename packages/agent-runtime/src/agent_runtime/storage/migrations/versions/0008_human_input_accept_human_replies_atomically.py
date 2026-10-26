"""accept human replies atomically

Revision ID: 0008_human_input
Revises: 0007_capability_superseded
Create Date: 2026-10-19 14:55:15.481609

Runtime chain revisions are forward-only in production; ``downgrade`` exists for isolated
CI and controlled recovery only. Tables are created in the order tables -> indexes ->
foreign keys because composite scope foreign keys target unique indexes (finding F1).
"""

from alembic import op

from agent_runtime.storage.migrations.snapshot import load_snapshot, snapshot_digest
from agent_runtime.storage.migrations.support import configured_schema

revision = "0008_human_input"
down_revision = "0007_capability_superseded"
branch_labels = None
depends_on = None
SNAPSHOT = "0008_human_input"
SNAPSHOT_DIGEST = "209e39e7ec762a3b35794912bdd211597d776557558c7c4ce9871579e670c71b"
EXPECTED_TABLES = 45
EXPECTED_COLUMNS = 528


def _replace(snapshot: str, digest: str) -> None:
    if snapshot_digest(snapshot) != digest:
        raise RuntimeError("human-input catalog snapshot changed")
    tables = load_snapshot(snapshot)
    if len(tables) != EXPECTED_TABLES or sum(len(t.columns) for t in tables) != EXPECTED_COLUMNS:
        raise RuntimeError("human-input catalog shape changed")
    event = next(table for table in tables if table.name == "event")
    checks = [check.expression for check in event.checks if check.columns == ("kind",)]
    if len(checks) != 1:
        raise RuntimeError("event kind check is not unique")
    config = op.get_context().config
    if config is None:
        raise RuntimeError("migration context has no configuration")
    schema = configured_schema(config)
    op.drop_constraint("ck_event_4", "event", schema=schema, type_="check")
    op.create_check_constraint("ck_event_4", "event", checks[0], schema=schema)


def upgrade() -> None:
    # Alembic does not detect CHECK changes. Keep generated index renames out of
    # this migration and apply only the reviewed, immutable event-kind change.
    _replace(SNAPSHOT, SNAPSHOT_DIGEST)


def downgrade() -> None:
    config = op.get_context().config
    if config is None:
        raise RuntimeError("migration context has no configuration")
    schema = configured_schema(config)
    if (
        op.get_bind()
        .exec_driver_sql(f"SELECT 1 FROM \"{schema}\".event WHERE kind='input.accepted' LIMIT 1")
        .first()
    ):
        raise RuntimeError("cannot downgrade while human replies exist")
    _replace(
        "0007_capability_superseded",
        "bf6fa23e65033983a5985f2e881b14fc4c3d5adacf3325444efe518a1adfe658",
    )

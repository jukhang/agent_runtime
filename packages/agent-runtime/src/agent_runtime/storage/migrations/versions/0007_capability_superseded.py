"""Keep published but superseded revisions distinct from revoked revisions.

Revision ID: 0007_capability_superseded
Revises: 0006_credential_service
"""

from alembic import op

from agent_runtime.storage.migrations.snapshot import load_snapshot, snapshot_digest
from agent_runtime.storage.migrations.support import configured_schema

revision = "0007_capability_superseded"
down_revision = "0006_credential_service"
branch_labels = None
depends_on = None

SNAPSHOT = "0007_capability_superseded"
SNAPSHOT_DIGEST = "bf6fa23e65033983a5985f2e881b14fc4c3d5adacf3325444efe518a1adfe658"
PREVIOUS_DIGEST = "b9741d4869dd47a288457131b124d2fb0fcb48dba503f25a0381474aafeb4afb"
EXPECTED_TABLES = 45
EXPECTED_COLUMNS = 528


def _expression(snapshot: str, digest: str) -> str:
    if snapshot_digest(snapshot) != digest:
        raise RuntimeError("capability catalog changed after revision freeze")
    tables = load_snapshot(snapshot)
    if (
        len(tables) != EXPECTED_TABLES
        or sum(len(table.columns) for table in tables) != EXPECTED_COLUMNS
    ):
        raise RuntimeError("unexpected capability catalog shape")
    revisions = next(table for table in tables if table.name == "capability_revision")
    checks = [check.expression for check in revisions.checks if check.columns == ("status",)]
    if len(checks) != 1:
        raise RuntimeError("capability status check is not unique")
    return checks[0]


def _replace(expression: str) -> None:
    config = op.get_context().config
    if config is None:
        raise RuntimeError("migration context has no configuration")
    schema = configured_schema(config)
    op.drop_constraint(
        "ck_capability_revision_1", "capability_revision", schema=schema, type_="check"
    )
    op.create_check_constraint(
        "ck_capability_revision_1", "capability_revision", expression, schema=schema
    )


def upgrade() -> None:
    _replace(_expression(SNAPSHOT, SNAPSHOT_DIGEST))


def downgrade() -> None:
    config = op.get_context().config
    if config is None:
        raise RuntimeError("migration context has no configuration")
    schema = configured_schema(config)
    bind = op.get_bind()
    existing = bind.exec_driver_sql(
        f'SELECT 1 FROM "{schema}"."capability_revision" WHERE status = \'superseded\' LIMIT 1'
    ).first()
    if existing is not None:
        raise RuntimeError("cannot downgrade while superseded capability revisions exist")
    _replace(_expression("0006_credential_service", PREVIOUS_DIGEST))

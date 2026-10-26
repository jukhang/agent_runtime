"""Second Runtime storage revision: durable evidence and ownership columns.

Revision ID: 0002_evidence_ownership
Revises: 0001_initial_storage
Create Date: 2026-09-23

Review §11.2 / Q44–Q47 authorized exactly one successor migration for facts that 0001
cannot express. The 0001 snapshot and revision stay byte-for-byte untouched; this revision
adds only the delta:

* ``output_seal`` - one immutable row per sealed producer with both channels' cumulative
  digests, both channel heads and the durable journal watermark (Q45);
* ``run.authority_revision`` - the per-run authority fence revision (Q46);
* ``execution.run_id`` - explicit execution ownership, so sibling runs sharing a task are
  never conflated (review P1-4);
* ``system_capacity_reservation.release_request_hash`` - the canonical release payload
  digest, so a replay with a different ``holder_state`` is refused (Q44).

DDL order follows revision 0001: tables, then indexes, then foreign keys.
"""

import sqlalchemy as sa
from alembic import op

from agent_runtime.storage.migrations.snapshot import load_snapshot, snapshot_digest
from agent_runtime.storage.migrations.support import SQL_TYPES, configured_schema

revision = "0002_evidence_ownership"
down_revision = "0001_initial_storage"
branch_labels = None
depends_on = None

#: Name of the immutable snapshot describing the *resulting* catalog of this revision.
SNAPSHOT = "0002_evidence_ownership"

#: sha256 of ``catalogs/0002_evidence_ownership.json`` when it was cut.
#: Updated only while this unreleased revision is under DS-P02-02 review.
SNAPSHOT_DIGEST = "ea8bfc17c23faaad9026852992dcaf36f074305d77e48a939495847d143d3080"

EXPECTED_TABLES = 41
EXPECTED_COLUMNS = 497


def constraint_name(prefix: str, table: str, offset: int) -> str:
    return f"{prefix}_{table}_{offset}"


def schema_name() -> str:
    config = op.get_context().config
    if config is None:  # pragma: no cover - Alembic always provides a configuration
        raise RuntimeError("migration context has no configuration")
    return configured_schema(config)


def verify_snapshot() -> None:
    digest = snapshot_digest(SNAPSHOT)
    if digest != SNAPSHOT_DIGEST:
        raise RuntimeError(
            f"snapshot {SNAPSHOT} was modified (sha256 {digest}); revisions are immutable"
        )
    tables = load_snapshot(SNAPSHOT)
    if len(tables) != EXPECTED_TABLES or sum(len(table.columns) for table in tables) != (
        EXPECTED_COLUMNS
    ):
        raise RuntimeError(f"snapshot {SNAPSHOT} no longer describes the frozen catalog")


def upgrade() -> None:
    verify_snapshot()
    schema = schema_name()
    tables = {table.name: table for table in load_snapshot(SNAPSHOT)}
    seal = tables["output_seal"]
    assert any(check.expression == "authority_revision >= 0" for check in tables["run"].checks)
    op.create_table(
        seal.name,
        *(
            sa.Column(
                column.name,
                SQL_TYPES[column.sql_type],
                nullable=column.nullable,
                server_default=sa.text(column.default) if column.default is not None else None,
                comment=column.comment,
            )
            for column in seal.columns
        ),
        sa.PrimaryKeyConstraint(*seal.primary_key, name=f"pk_{seal.name}"),
        *(
            sa.CheckConstraint(check.expression, name=constraint_name("ck", seal.name, offset))
            for offset, check in enumerate(seal.checks)
        ),
        schema=schema,
    )
    for offset, index in enumerate(seal.indexes):
        op.create_index(
            constraint_name("ix", seal.name, offset),
            seal.name,
            list(index.columns),
            unique=index.unique,
            schema=schema,
            postgresql_where=sa.text(index.predicate) if index.predicate else None,
        )
    for offset, foreign_key in enumerate(seal.foreign_keys):
        op.create_foreign_key(
            constraint_name("fk", seal.name, offset),
            seal.name,
            foreign_key.target,
            list(foreign_key.columns),
            list(foreign_key.target_columns),
            source_schema=schema,
            referent_schema=schema,
            ondelete=foreign_key.on_delete,
            deferrable=foreign_key.deferrable,
            initially="DEFERRED" if foreign_key.initially_deferred else "IMMEDIATE",
        )
    op.add_column(
        "run",
        sa.Column(
            "authority_revision",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
            comment="Per-run authority fence revision; grant writers bump it under the run row",
        ),
        schema=schema,
    )
    op.create_check_constraint(
        "ck_run_authority_revision",
        "run",
        "authority_revision >= 0",
        schema=schema,
    )
    # Existing 0001 runs already have capability grants. A zero-only default would fence
    # every request whose latest grant revision is non-zero immediately after deployment.
    op.execute(
        sa.text(
            f'UPDATE "{schema}"."run" AS r SET authority_revision = COALESCE('
            f'(SELECT MAX(g.revision) FROM "{schema}"."capability_grant" AS g '
            "WHERE g.scope_id = r.scope_id AND g.run_id = r.id), 0)"
        )
    )
    op.add_column(
        "execution",
        sa.Column(
            "run_id",
            sa.Uuid(),
            nullable=True,
            comment="run id",
        ),
        schema=schema,
    )
    # Revision 0001 related an execution only to Operation -> Task. Preserve a row when that
    # task has exactly one run; a shared task is ambiguous and must fail migration rather than
    # silently attributing a subagent execution to the root run (the bug this column fixes).
    op.execute(
        sa.text(
            "WITH owner AS ("
            f"SELECT e.scope_id, e.id AS execution_id, "
            "(array_agg(r.id ORDER BY r.id))[1] AS run_id "
            f'FROM "{schema}"."execution" AS e '
            f'JOIN "{schema}"."operation" AS o ON o.scope_id = e.scope_id '
            "AND o.id = e.operation_id "
            f'JOIN "{schema}"."run" AS r ON r.scope_id = o.scope_id AND r.task_id = o.task_id '
            "GROUP BY e.scope_id, e.id HAVING count(*) = 1) "
            f'UPDATE "{schema}"."execution" AS e SET run_id = owner.run_id FROM owner '
            "WHERE e.scope_id = owner.scope_id AND e.id = owner.execution_id"
        )
    )
    bind = op.get_bind()
    unresolved = bind.execute(
        sa.text(f'SELECT count(*) FROM "{schema}"."execution" WHERE run_id IS NULL')
    ).scalar_one()
    if int(unresolved):
        raise RuntimeError(
            f"cannot migrate {unresolved} execution row(s) without unambiguous Run ownership"
        )
    op.alter_column("execution", "run_id", nullable=False, schema=schema)
    op.create_index(
        "ix_execution_run_id",
        "execution",
        ["scope_id", "run_id", "generation"],
        schema=schema,
    )
    op.create_foreign_key(
        "fk_execution_run_id",
        "execution",
        "run",
        ["scope_id", "run_id"],
        ["scope_id", "id"],
        source_schema=schema,
        referent_schema=schema,
        ondelete="NO ACTION",
        deferrable=True,
        initially="DEFERRED",
    )
    op.add_column(
        "system_capacity_reservation",
        sa.Column(
            "release_request_hash",
            sa.Text(),
            nullable=True,
            comment="release request hash",
        ),
        schema=schema,
    )
    op.create_check_constraint(
        "ck_reservation_release_request_hash",
        "system_capacity_reservation",
        "release_request_hash IS NULL OR length(release_request_hash) = 64",
        schema=schema,
    )


def downgrade() -> None:
    schema = schema_name()

    op.drop_constraint(
        "ck_reservation_release_request_hash",
        "system_capacity_reservation",
        schema=schema,
        type_="check",
    )
    op.drop_column("system_capacity_reservation", "release_request_hash", schema=schema)
    op.drop_constraint("fk_execution_run_id", "execution", schema=schema, type_="foreignkey")
    op.drop_index("ix_execution_run_id", "execution", schema=schema)
    op.drop_column("execution", "run_id", schema=schema)
    op.drop_constraint("ck_run_authority_revision", "run", schema=schema, type_="check")
    op.drop_column("run", "authority_revision", schema=schema)
    for offset, _foreign_key in enumerate(
        next(table for table in load_snapshot(SNAPSHOT) if table.name == "output_seal").foreign_keys
    ):
        op.drop_constraint(
            constraint_name("fk", "output_seal", offset),
            "output_seal",
            schema=schema,
            type_="foreignkey",
        )
    op.drop_table("output_seal", schema=schema)


__all__ = [
    "EXPECTED_COLUMNS",
    "EXPECTED_TABLES",
    "SNAPSHOT",
    "SNAPSHOT_DIGEST",
    "branch_labels",
    "depends_on",
    "downgrade",
    "down_revision",
    "revision",
    "upgrade",
]

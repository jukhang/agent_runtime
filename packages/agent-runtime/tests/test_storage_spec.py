import json
from dataclasses import replace
from importlib.resources import files

import pytest

from agent_runtime.storage_spec import (
    TABLES,
    TRANSACTION_RECORDS,
    Check,
    ForeignKey,
    storage_document,
    validate_storage,
)


def test_all_storage_constraints_resolve_and_spec_ships_in_wheel():
    validate_storage(TABLES)
    # 36 P01 tables + DS-P02-00 additions (system_capacity,
    # system_capacity_reservation, capability, capability_revision) + the DS-P02-02
    # second-repair successor table (output_seal).
    assert len(TABLES) == 45
    assert (
        files("agent_runtime.contracts").joinpath("storage.json").read_text() == storage_document()
    )
    catalog = json.loads(files("agent_runtime.contracts").joinpath("catalog.json").read_text())
    aliases = {
        "Item": "timeline_item",
        "SessionSlot": "session_slot",
        "WorkItem": "work_item",
        "BudgetAllocation": "budget_allocation",
        "ModelAttempt": "model_attempt",
        "ContextSnapshot": "context_snapshot",
        "AgentMessage": "agent_message",
        "ToolCall": "tool_call",
    }
    for transaction in catalog["transactions"]:
        mapped = {aliases.get(name, name.lower()) for name in transaction["records"]}
        assert mapped == set(TRANSACTION_RECORDS[transaction["id"]])
    assert all(
        set(names) <= {table.name for table in TABLES} for names in TRANSACTION_RECORDS.values()
    )


def test_storage_rejects_dangling_or_unscoped_foreign_keys():
    session = TABLES[0]
    for fk in [ForeignKey(("scope_id", "id"), "missing"), ForeignKey(("id",), "session", ("id",))]:
        with pytest.raises(ValueError):
            validate_storage((replace(session, foreign_keys=(fk,)), *TABLES[1:]))


def test_scope_less_foreign_keys_must_cover_the_full_target_primary_key():
    """C4: a reservation may only reference an aggregate through its whole key."""
    reservation = next(table for table in TABLES if table.name == "system_capacity_reservation")
    partial = ForeignKey(
        ("deployment_id", "resource_kind"),
        "system_capacity",
        ("deployment_id", "resource_kind"),
    )
    others = tuple(table for table in TABLES if table.name != reservation.name)
    with pytest.raises(ValueError):
        validate_storage((*others, replace(reservation, foreign_keys=(partial,))))
    key = ("deployment_id", "resource_kind", "resource_version", "resource_key")
    assert reservation.foreign_keys == (ForeignKey(key, "system_capacity", key),)


def test_reservation_release_facts_are_frozen_in_storage():
    """C4: release replay identity and status/reason pairing are database invariants."""
    reservation = next(table for table in TABLES if table.name == "system_capacity_reservation")
    columns = {column.name: column for column in reservation.columns}
    assert columns["release_request_id"].nullable
    assert columns["release_reason"].nullable
    checks = {check.columns: check.expression for check in reservation.checks}
    assert checks[("status", "released_at")] == "(status = 'reserved') = (released_at IS NULL)"
    expression = checks[("status", "release_request_id", "release_reason")]
    assert "((status = 'reserved') = (release_request_id IS NULL))" in expression
    assert "((release_request_id IS NULL) = (release_reason IS NULL))" in expression
    assert "((status = 'released') = (release_reason IN ('settled','cancelled')))" in expression
    assert "((status = 'expired') = (release_reason = 'expired'))" in expression
    assert "((status = 'reclaimed') = (release_reason = 'reclaimed'))" in expression


def test_storage_rejects_constraint_on_missing_column():
    with pytest.raises(ValueError):
        validate_storage(
            (replace(TABLES[0], checks=(Check(("missing",), "missing > 0"),)), *TABLES[1:])
        )


def test_circular_foreign_keys_are_deferred_and_never_cascade_business_deletion():
    for table in TABLES:
        for fk in table.foreign_keys:
            assert fk.deferrable and fk.initially_deferred
            assert fk.on_delete == "NO ACTION"


def test_active_work_unique_fence_and_nullable_model_tool_source_are_explicit():
    tables = {table.name: table for table in TABLES}
    assert any(
        index.unique and index.columns == ("scope_id", "run_id") and index.predicate
        for index in tables["work_item"].indexes
    )
    columns = {column.name: column for column in tables["tool_call"].columns}
    assert columns["attempt_id"].nullable and columns["provider_call_id"].nullable
    assert "wake_generation" in {column.name for column in tables["run"].columns}


def test_run_control_lifecycle_is_a_database_invariant():
    """A4: the run table must refuse the same control lifecycles RunState refuses."""
    run = next(table for table in TABLES if table.name == "run")
    control = [
        check.expression
        for check in run.checks
        if check.columns == ("desired_control", "status")
    ]
    assert "(desired_control = 'cancel') = (status = 'cancelling')" in control
    assert (
        "desired_control IS NULL OR status IN "
        "('queued', 'running', 'waiting_tool', 'waiting_user', 'cancelling')"
    ) in control


def test_every_dedup_key_field_exists_in_the_catalog():
    """DS-P01-03 / §29.1: a dedup key may only reference stored fields."""
    by_table = {table.name: {column.name for column in table.columns} for table in TABLES}
    required = {
        "advance_run": {"run": ("wake_generation",)},
        "reconcile_execution": {"execution": ("generation", "reconciliation_round")},
        "rebuild_projection": {"projection": ("projection_revision", "rebuild_generation")},
        # source_checkpoint_id is nullable because an original checkpoint has no source.
        "migrate_checkpoint": {"checkpoint": ("source_checkpoint_id", "schema_revision")},
    }
    for kind, tables in required.items():
        for table_name, columns in tables.items():
            missing = sorted(set(columns) - by_table[table_name])
            assert missing == [], f"{kind}: {table_name} missing {missing}"


def test_dedup_generation_counters_are_non_null_with_declared_defaults():
    tables = {table.name: {c.name: c for c in table.columns} for table in TABLES}
    for table_name, column_name, default in (
        # Allocated by the wake transaction, so it has no server default.
        ("run", "wake_generation", None),
        # Supervision/projection rounds start at zero and are incremented per round.
        ("execution", "reconciliation_round", "0"),
        ("projection", "rebuild_generation", "0"),
    ):
        column = tables[table_name][column_name]
        assert column.sql_type == "BIGINT"
        assert column.nullable is False
        assert column.default == default

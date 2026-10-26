"""Catalog <-> ORM model consistency and storage boundary tests (DS-P02-01).

These tests need the ``storage`` extra but no database: they compare the committed
declarative models with the frozen catalog and assert that importing the core package
never pulls in the storage dependencies (Q38).
"""

import importlib
import importlib.util
import json
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy", reason="requires agent-runtime[storage]")

from agent_runtime.storage import Base  # noqa: E402
from agent_runtime.storage.consistency import (  # noqa: E402
    TYPE_MAP,
    catalog_shapes,
    compare_catalog_to_models,
    compare_models_to_catalog_sizes,
    model_shapes,
    normalize_default,
    normalize_sql,
    sql_type_of,
)
from agent_runtime.storage.engines import (  # noqa: E402
    StorageConfig,
    StorageContext,
    create_session_factory,
    create_storage_engine,
)
from agent_runtime.storage.migrations.snapshot import (  # noqa: E402
    load_snapshot,
    snapshot_digest,
    snapshot_names,
    snapshot_path,
)
from agent_runtime.storage_spec import TABLES  # noqa: E402

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
IMMUTABLE_TABLES = {
    "credential_audit",
    "credential_service_binding",
    "context_blob",
    "checkpoint",
    "output_seal",
    "event",
    "budget_entry",
    "agent_revision",
    "goal_revision",
    "plan_revision",
    "context_snapshot",
    "artifact_revision",
}


def test_catalog_and_models_agree_completely():
    assert compare_models_to_catalog_sizes() == ()
    assert compare_catalog_to_models() == ()
    assert len(catalog_shapes()) == len(model_shapes()) == len(TABLES) == 45
    assert sum(len(shape.columns) for shape in model_shapes().values()) == 528


def test_every_catalog_table_has_one_explicit_row_class():
    modules = importlib.import_module("agent_runtime.storage.models")
    modules_by_table = {}
    for name in modules.__all__:
        row_class = getattr(modules, name)
        modules_by_table[row_class.__tablename__] = row_class
    assert set(modules_by_table) == {spec.name for spec in TABLES}
    for name, row_class in modules_by_table.items():
        assert row_class.__name__.endswith("Row"), name
        assert row_class.__module__.startswith("agent_runtime.storage.models."), name
        assert row_class.__table__ is Base.metadata.tables[name]


def test_constraint_names_are_deterministic_and_unique():
    names: list[str] = []
    for table in Base.metadata.tables.values():
        names.extend(constraint.name or "" for constraint in table.constraints)
        names.extend(index.name or "" for index in table.indexes)
        assert all(name for name in names[-len(table.indexes) - len(table.constraints) :]), (
            table.name
        )
    assert len(names) == len(set(names))
    assert all(name.startswith(("pk_", "ix_", "ck_", "fk_")) for name in names)


def test_immutable_tables_are_declared_in_metadata():
    declared = {name for name, table in Base.metadata.tables.items() if table.info["immutable"]}
    assert declared == IMMUTABLE_TABLES
    assert {spec.name for spec in TABLES if spec.immutable} == IMMUTABLE_TABLES


def test_catalog_columns_are_declared_with_comments_and_explicit_nullability():
    for spec in TABLES:
        table = Base.metadata.tables[spec.name]
        for column in spec.columns:
            actual = table.columns[column.name]
            assert actual.comment == column.comment, f"{spec.name}.{column.name}"
            assert bool(actual.nullable) == column.nullable, f"{spec.name}.{column.name}"
            assert sql_type_of(actual.type) == column.sql_type, f"{spec.name}.{column.name}"
            assert sql_type_of(TYPE_MAP[column.sql_type]) == column.sql_type


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("CURRENT_TIMESTAMP", "current_timestamp"),
        ("'0'::bigint", "0"),
        ("now()", "current_timestamp"),
        (None, None),
    ],
)
def test_default_normalisation(raw, expected):
    assert normalize_default(raw) == expected


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("status IN ('a','b')", "status = ANY (ARRAY['a'::text, 'b'::text])"),
        ("(status = 'x')", "status = 'x'"),
        ("a IN ('x','y')", "(a = ANY (ARRAY['x'::text, 'y'::text]))"),
    ],
)
def test_predicate_normalisation_matches_postgres_spelling(left, right):
    from agent_runtime.storage.consistency import expressions_match

    assert expressions_match(left, right)


def test_storage_config_validates_inputs_and_never_exposes_the_dsn():
    config = StorageConfig(
        dsn="postgresql+asyncpg://user:secret@host:5432/db", schema="agent_runtime"
    )
    assert "secret" not in repr(config.safe_summary())
    assert "dsn" not in config.safe_summary()
    assert config.qualified("session") == "agent_runtime.session"
    with pytest.raises(ValueError):
        StorageConfig(dsn="postgresql://user@host/db")
    with pytest.raises(ValueError):
        StorageConfig(dsn="postgresql+asyncpg://user@host/db", schema="AgentRuntime")
    with pytest.raises(ValueError):
        StorageConfig(dsn="postgresql+asyncpg://user@host/db", pool_size=0)
    with pytest.raises(ValueError):
        StorageConfig(dsn="postgresql+asyncpg://user@host/db", statement_timeout_ms=0)


def test_engine_and_session_factory_construction_is_lazy():
    config = StorageConfig(dsn="postgresql+asyncpg://user:secret@127.0.0.1:1/absent")
    engine = create_storage_engine(config)
    try:
        assert engine.url.drivername == "postgresql+asyncpg"
        assert engine.pool.size() == config.pool_size
        factory = create_session_factory(engine)
        assert factory.kw["expire_on_commit"] is False
    finally:
        import asyncio

        asyncio.run(engine.dispose())


def test_storage_context_owns_and_closes_its_engine():
    config = StorageConfig(dsn="postgresql+asyncpg://user:secret@127.0.0.1:1/absent")
    context = StorageContext(config)
    assert context.engine.url.drivername == "postgresql+asyncpg"
    assert context.safe_summary()["schema"] == "agent_runtime"
    import asyncio

    asyncio.run(context.dispose())


def test_core_package_import_never_pulls_in_storage_dependencies():
    """Q38: the base wheel must import without SQLAlchemy/Alembic/asyncpg installed."""
    script = (
        "import sys, agent_runtime;"
        "leaked = sorted(name for name in sys.modules if name.split('.')[0] in "
        "{'sqlalchemy', 'alembic', 'asyncpg'});"
        "assert not leaked, leaked;"
        "assert not hasattr(agent_runtime, 'storage');"
        "print('core-import-clean')"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=PACKAGE_ROOT,
        capture_output=True,
        text=True,
        env={"PYTHONPATH": "src", "PATH": "/usr/bin:/bin"},
    )
    assert result.returncode == 0, result.stderr
    assert "core-import-clean" in result.stdout


def test_normalize_sql_is_idempotent():
    for text in ("a IN ('x','y')", "(status = 'reserved') = (released_at IS NULL)"):
        assert normalize_sql(normalize_sql(text)) == normalize_sql(text)

# --- Frozen revision inputs (review §8.2 P1-1) ---------------------------------------


def revision_modules() -> list[tuple[Path, object]]:
    versions = PACKAGE_ROOT / "src/agent_runtime/storage/migrations/versions"
    modules = []
    for path in sorted(versions.glob("[0-9]*.py")):
        spec = importlib.util.spec_from_file_location(f"frozen_revision_{path.stem}", path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        modules.append((path, module))
    assert modules, "no migration revisions found"
    return modules


def test_migration_chain_never_imports_the_live_catalog():
    """A historical revision must read its own snapshot, not the evolving catalog."""
    chain = [
        PACKAGE_ROOT / "src/agent_runtime/storage/migrations/snapshot.py",
        PACKAGE_ROOT / "src/agent_runtime/storage/migrations/support.py",
        PACKAGE_ROOT / "src/agent_runtime/storage/migrations/env.py",
        PACKAGE_ROOT / "src/agent_runtime/storage/migrate.py",
        *(path for path, _module in revision_modules()),
    ]
    for path in chain:
        text = path.read_text(encoding="utf-8")
        assert "from agent_runtime.storage_spec" not in text, path
        assert "import agent_runtime.storage_spec" not in text, path
        assert "storage_document" not in text, path


def test_every_revision_pins_its_own_snapshot_digest():
    for path, module in revision_modules():
        assert hasattr(module, "SNAPSHOT"), path
        assert snapshot_digest(module.SNAPSHOT) == module.SNAPSHOT_DIGEST, path
        tables = load_snapshot(module.SNAPSHOT)
        assert len(tables) == module.EXPECTED_TABLES, path
        assert sum(len(table.columns) for table in tables) == module.EXPECTED_COLUMNS, path


def test_snapshot_is_a_shipped_package_resource():
    names = snapshot_names()
    assert names, "no catalog snapshot is packaged"
    for name in names:
        path = snapshot_path(name)
        assert path.is_file() and path.stat().st_size > 0
        document = json.loads(path.read_text(encoding="utf-8"))
        assert document["schema"] == "agent_runtime"
        assert len(document["tables"]) > 0


def test_latest_revision_snapshot_matches_the_live_catalog():
    """A catalog change without a new revision must fail here, not in history."""
    latest = snapshot_names()[-1]
    snapshot = {table.name: table for table in load_snapshot(latest)}
    live = {spec.name: spec for spec in TABLES}
    assert set(snapshot) == set(live)
    for name, table in snapshot.items():
        expected = live[name]
        assert [
            (column.name, column.sql_type, column.nullable, column.default, column.comment)
            for column in table.columns
        ] == [
            (column.name, column.sql_type, column.nullable, column.default, column.comment)
            for column in expected.columns
        ], name
        assert table.primary_key == expected.primary_key, name
        assert [(index.columns, index.unique, index.predicate) for index in table.indexes] == [
            (index.columns, index.unique, index.predicate) for index in expected.indexes
        ], name
        assert [(check.columns, check.expression) for check in table.checks] == [
            (check.columns, check.expression) for check in expected.checks
        ], name
        assert [
            (
                foreign_key.columns,
                foreign_key.target,
                foreign_key.target_columns,
                foreign_key.on_delete,
                foreign_key.deferrable,
                foreign_key.initially_deferred,
            )
            for foreign_key in table.foreign_keys
        ] == [
            (
                foreign_key.columns,
                foreign_key.target,
                foreign_key.target_columns,
                foreign_key.on_delete,
                foreign_key.deferrable,
                foreign_key.initially_deferred,
            )
            for foreign_key in expected.foreign_keys
        ], name
        assert table.immutable == expected.immutable, name


# --- Base-wheel CLI entry point (review §8.2 P2-3) -----------------------------------


def test_console_script_targets_the_lazy_wrapper():
    pyproject = tomllib.loads((PACKAGE_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert (
        pyproject["project"]["scripts"]["agent-runtime-migrate"]
        == "agent_runtime.migrate_cli:main"
    )
    wrapper = (PACKAGE_ROOT / "src/agent_runtime/migrate_cli.py").read_text(encoding="utf-8")
    assert "storage.migrate import main" in wrapper


def test_migrate_cli_reports_a_missing_storage_extra(monkeypatch, capsys):
    from agent_runtime import migrate_cli

    monkeypatch.setattr(migrate_cli, "storage_package_available", lambda: False)
    monkeypatch.setitem(sys.modules, "agent_runtime.storage.migrate", None)
    assert migrate_cli.main([]) == migrate_cli.MISSING_EXTRA_EXIT
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["errorType"] == "MissingStorageExtra"
    assert "agent-runtime[storage]" in payload["errorMessage"]


def test_migrate_cli_preserves_internal_import_errors(monkeypatch):
    """A broken adapter inside an installed extra must keep its real failure category."""
    from agent_runtime import migrate_cli

    monkeypatch.setitem(sys.modules, "agent_runtime.storage.migrate", None)
    with pytest.raises(ImportError):
        migrate_cli.main([])


@pytest.mark.parametrize(
    ("name", "package_available", "expected"),
    [
        ("sqlalchemy", True, True),
        ("asyncpg", True, True),
        ("agent_runtime.storage", False, True),
        ("agent_runtime.storage.migrate", False, True),
        ("agent_runtime.storage.migrate", True, False),
        ("agent_runtime.contracts.nope", True, False),
        ("psycopg", True, False),
        (None, True, False),
        (None, False, True),
    ],
)
def test_migrate_cli_import_error_classification(name, package_available, expected):
    from agent_runtime import migrate_cli

    error = ModuleNotFoundError(f"No module named {name!r}", name=name)
    assert (
        migrate_cli.classify_import_error(
            error, storage_package_available=package_available
        )
        is expected
    )


def test_statement_timeout_is_part_of_the_public_config_contract():
    config = StorageConfig(
        dsn="postgresql+asyncpg://user@host:5432/db", statement_timeout_ms=4321
    )
    assert config.safe_summary()["statementTimeoutMs"] == 4321
    with pytest.raises(ValueError):
        StorageConfig(dsn="postgresql+asyncpg://user@host:5432/db", statement_timeout_ms=0)

"""Real PostgreSQL migration and privilege suite (DS-P02-01 requirements 5-7).

Opt-in: set ``AGENT_RUNTIME_TEST_DSN`` to a disposable database or account that may create
schemas. Every test uses a random ``p02_test_*`` schema and the module teardown fails if any
schema is left behind; SQLite and mocked repositories cannot substitute for this suite.
"""

import asyncio
import os
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy", reason="requires agent-runtime[storage]")

import sqlalchemy as sa  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine  # noqa: E402

from agent_runtime.storage.consistency import (  # noqa: E402
    compare_catalog_to_database,
    compare_catalog_to_models,
)
from agent_runtime.storage.engines import StorageConfig, StorageContext  # noqa: E402
from agent_runtime.storage.migrate import (  # noqa: E402
    MigrationLockError,
    MigrationPreflightError,
    MigrationResult,
    MigrationStateError,
    advisory_key,
    execute,
)
from agent_runtime.storage.migrations.snapshot import (  # noqa: E402
    load_snapshot,
    snapshot_names,
)
from agent_runtime.storage_spec import TABLES  # noqa: E402

DSN = os.environ.get("AGENT_RUNTIME_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="AGENT_RUNTIME_TEST_DSN is not configured")
_DATABASE_VERIFIED = False

_LOOP: Any = None


def run[T](coro: Callable[[], Awaitable[T]]) -> T:
    """Drive one coroutine on a persistent loop so the shared engine stays usable.

    asyncpg connections belong to the loop that created them, and ``asyncio.run`` builds
    a fresh loop per call; keeping one loop for the module lets the helpers reuse a single
    engine instead of reconnecting (which dominates the runtime against a remote server).
    """
    global _LOOP
    if _LOOP is None:
        _LOOP = asyncio.new_event_loop()
    return _LOOP.run_until_complete(coro())


def migrate(
    command: str,
    schema: str,
    *,
    revision: str = "base",
    lock_timeout_seconds: float = 30.0,
) -> MigrationResult:
    global _DATABASE_VERIFIED
    assert DSN is not None
    if not _DATABASE_VERIFIED:
        expected = os.environ.get("AGENT_RUNTIME_EXPECTED_DATABASE", "aletai_wangxh")
        actual = str(scalar("SELECT current_database()"))
        if actual != expected:
            raise RuntimeError(
                f"database guard rejected {actual!r}; expected {expected!r}"
            )
        _DATABASE_VERIFIED = True
    return run(lambda: execute(command, DSN, schema, revision, lock_timeout_seconds))


_ENGINE: Any = None


def engine() -> Any:
    """One shared engine for helper statements; the module teardown disposes it."""
    global _ENGINE
    if _ENGINE is None:
        assert DSN is not None
        _ENGINE = create_async_engine(DSN, pool_size=2, max_overflow=0)
    return _ENGINE


def scalar(sql: str, **params: Any) -> Any:
    async def main() -> Any:
        async with engine().connect() as connection:
            return (await connection.execute(sa.text(sql), params)).scalar()

    return run(main)


def execute_sql(*statements: str) -> None:
    async def main() -> None:
        async with engine().begin() as connection:
            for statement in statements:
                await connection.execute(sa.text(statement))

    run(main)


def insert(schema: str, table: str, **values: Any) -> tuple[str, dict[str, Any]]:
    columns = ", ".join(values)
    placeholders = ", ".join(f":{name}" for name in values)
    statement = f'INSERT INTO "{schema}"."{table}" ({columns}) VALUES ({placeholders})'
    return statement, dict(values)


def insert_row(schema: str, table: str, **values: Any) -> None:
    statement, params = insert(schema, table, **values)

    async def main() -> None:
        async with engine().begin() as connection:
            await connection.execute(sa.text(statement), params)

    run(main)


def rejected(statement: str, params: dict[str, Any], sqlstate: str) -> None:
    """Assert that a statement fails with a specific SQLSTATE inside its own transaction."""

    async def main() -> None:
        with pytest.raises(sa.exc.IntegrityError) as error:
            async with engine().begin() as connection:
                await connection.execute(sa.text(statement), params)
        assert getattr(error.value.orig, "sqlstate", None) == sqlstate

    run(main)


def rejected_row(schema: str, table: str, sqlstate: str, **values: Any) -> None:
    statement, params = insert(schema, table, **values)
    rejected(statement, params, sqlstate)


def relation_snapshot(schema: str) -> set[str]:
    """Relations (tables/views/sequences), excluding the indexes behind constraints."""
    rows = scalar(
        "SELECT coalesce(string_agg(c.relname, ',' ORDER BY c.relname), '') "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = :schema AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')",
        schema=schema,
    )
    return {name for name in str(rows).split(",") if name}


def schema_exists(schema: str) -> bool:
    return bool(
        scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = :schema)", schema=schema)
    )


def drop_schema(schema: str) -> None:
    execute_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def seed_0001_upgrade_graph(schema: str) -> dict[str, Any]:
    """Insert a connected 0001 graph that exercises every 0002 backfill."""
    names = (
        "agent",
        "revision",
        "session",
        "turn",
        "run",
        "task",
        "operation",
        "execution",
        "grant",
        "reservation",
    )
    ids = {name: uuid.uuid4() for name in names}
    now = datetime.now(UTC)
    deadline = now + timedelta(days=1)
    async with engine().begin() as connection:
        await connection.execute(sa.text("SET CONSTRAINTS ALL DEFERRED"))
        statements = (
            ("agent", "id, scope_id, key", ":agent, 'scope-upgrade', 'upgrade-agent'"),
            (
                "agent_revision",
                "id, scope_id, agent_id, revision, manifest, content_hash",
                ":revision, 'scope-upgrade', :agent, 3, '{}'::jsonb, :digest",
            ),
            (
                "session",
                "id, scope_id, owner_actor_id, title_source, event_seq, auth_revision",
                ":session, 'scope-upgrade', 'actor-upgrade', 'none', 0, 3",
            ),
            (
                "turn",
                "id, scope_id, session_id, request_key, request_hash, input_ref",
                ":turn, 'scope-upgrade', :session, 'upgrade-turn', :digest, 'input:upgrade'",
            ),
            (
                "run",
                "id, scope_id, session_id, turn_id, root_run_id, agent_revision_id, task_id, "
                "status, phase, state_version, owner_epoch, cancel_version, wake_generation, "
                "pending_input_version, applied_input_version, phase_started_at, deadline_at",
                ":run, 'scope-upgrade', :session, :turn, :run, :revision, :task, 'queued', "
                "'queued', 1, 0, 0, 0, 0, 0, :now, :deadline",
            ),
            (
                "task",
                "id, scope_id, root_run_id, acceptance_spec_ref, deadline_at",
                ":task, 'scope-upgrade', :run, 'acceptance:upgrade', :deadline",
            ),
            (
                "operation",
                "id, scope_id, task_id, idempotency_key, input_hash, status",
                ":operation, 'scope-upgrade', :task, 'operation-upgrade', :digest, 'prepared'",
            ),
            (
                "execution",
                "id, scope_id, operation_id, remote_request_key, status, log_cursor, "
                "cleanup_status, generation, deadline_at",
                ":execution, 'scope-upgrade', :operation, 'execution-upgrade', 'pending', "
                "'cursor:0', 'pending', 1, :deadline",
            ),
            (
                "capability_grant",
                "id, scope_id, run_id, actor_id, revision, allowed_scope, issued_at, expires_at",
                ":grant, 'scope-upgrade', :run, 'actor-upgrade', 3, '{}'::jsonb, :now, :deadline",
            ),
            (
                "system_capacity",
                "deployment_id, resource_kind, resource_key, resource_version, capacity, "
                "reserved, version",
                "'deployment-upgrade', 'run_slot', 'default', 1, 4, 0, 2",
            ),
            (
                "system_capacity_reservation",
                "id, deployment_id, resource_kind, resource_key, status, holder_ref, request_id, "
                "resource_version, amount, version, lease_until, released_at, "
                "release_request_id, release_reason",
                ":reservation, 'deployment-upgrade', 'run_slot', 'default', 'released', "
                "'run:upgrade', 'reserve-upgrade', 1, 1, 2, :deadline, :now, "
                "'release-upgrade', 'settled'",
            ),
        )
        params = {**ids, "now": now, "deadline": deadline, "digest": "a" * 64}
        for table, columns, values in statements:
            await connection.execute(
                sa.text(f'INSERT INTO "{schema}"."{table}" ({columns}) VALUES ({values})'),
                params,
            )
    return ids


@pytest.fixture
def schema() -> Any:
    name = f"p02_test_{uuid.uuid4().hex[:12]}"
    yield name
    drop_schema(name)


@pytest.fixture(scope="module", autouse=True)
def shared_engine_and_clean_teardown() -> Any:
    yield
    leftovers = scalar(
        "SELECT coalesce(string_agg(nspname, ','), '') FROM pg_namespace "
        "WHERE nspname LIKE 'p02_test_%'"
    )
    if _ENGINE is not None:
        run(lambda: _ENGINE.dispose())
    if _LOOP is not None:
        _LOOP.close()
    assert not leftovers, f"test schemas were left behind: {leftovers}"


def public_version_state() -> tuple[set[str], str | None]:
    """Host/public version-table state, which this chain must never touch."""
    exists = scalar("SELECT to_regclass('public.alembic_version') IS NOT NULL")
    revision = (
        scalar(
            "SELECT string_agg(version_num, ',' ORDER BY version_num) FROM public.alembic_version"
        )
        if exists
        else None
    )
    return relation_snapshot("public"), revision


def test_upgrade_creates_the_frozen_schema_without_touching_host_schemas(schema):
    public_before = public_version_state()
    host_runtime_before = (
        relation_snapshot("agent_runtime") if schema_exists("agent_runtime") else set()
    )
    result = migrate("upgrade", schema)
    assert result.status == "applied"
    assert result.current_revision == "0009_approval_events"
    assert result.lock == "transaction-scoped"
    assert (
        scalar(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_type = 'BASE TABLE'",
            schema=schema,
        )
        == len(TABLES) + 1
    )
    assert (
        scalar(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = :schema",
            schema=schema,
        )
        == sum(len(spec.columns) for spec in TABLES) + 1
    )
    # The host chain (``public.alembic_version``) and the default Runtime schema are
    # untouched: this run only ever wrote inside the random test schema.
    assert public_version_state() == public_before
    if schema_exists("agent_runtime"):
        assert relation_snapshot("agent_runtime") == host_runtime_before
    assert relation_snapshot(schema) == {spec.name for spec in TABLES} | {"alembic_version"}
    assert migrate("check", schema).status == "clean"


def test_repeated_upgrade_is_idempotent(schema):
    assert migrate("upgrade", schema).status == "applied"
    second = migrate("upgrade", schema)
    assert second.status == "up-to-date"
    assert second.applied_revisions == ()
    assert second.current_revision == "0009_approval_events"
    assert migrate("check", schema).status == "clean"


def test_input_required_constraint_steps_back_and_forward(schema):
    assert migrate("upgrade", schema).status == "applied"

    def event_check():
        return scalar(
            "SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c "
            "JOIN pg_class t ON t.oid=c.conrelid "
            "JOIN pg_namespace n ON n.oid=t.relnamespace "
            "WHERE n.nspname=:schema AND t.relname='event' AND c.conname='ck_event_4'",
            schema=schema,
        )

    assert "input.required" in event_check()
    assert migrate("downgrade", schema, revision="0003_context_blob").current_revision == (
        "0003_context_blob"
    )
    assert "input.required" not in event_check()
    assert migrate("upgrade", schema).current_revision == "0009_approval_events"
    assert "input.required" in event_check()
    assert migrate("check", schema).status == "clean"


def test_downgrade_then_upgrade_repeats_in_an_isolated_schema(schema):
    assert migrate("upgrade", schema).status == "applied"
    downgraded = migrate("downgrade", schema)
    assert downgraded.status == "downgraded"
    assert downgraded.current_revision is None
    assert relation_snapshot(schema) == {"alembic_version"}
    assert migrate("current", schema).current_revision is None
    assert migrate("upgrade", schema).status == "applied"
    assert migrate("check", schema).status == "clean"


def test_successor_revision_steps_back_to_the_0001_snapshot_and_forward_again(schema):
    """0002 is reversible and leaves the frozen 0001 schema exactly as sealed."""
    assert migrate("upgrade", schema).status == "applied"
    assert migrate("check", schema).status == "clean"
    stepped = migrate("downgrade", schema, revision="0001_initial_storage")
    assert stepped.status == "downgraded"
    assert stepped.current_revision == "0001_initial_storage"
    frozen_0001 = {table.name for table in load_snapshot("0001_initial_storage")}
    assert relation_snapshot(schema) == frozen_0001 | {"alembic_version"}
    assert (
        scalar(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = :schema",
            schema=schema,
        )
        == sum(len(table.columns) for table in load_snapshot("0001_initial_storage")) + 1
    )
    # The 0001 revision itself still verifies once more from its own snapshot.
    assert migrate("upgrade", schema).status == "applied"
    assert migrate("current", schema).current_revision == "0009_approval_events"
    assert migrate("check", schema).status == "clean"


def test_successor_migrates_populated_0001_ownership_and_authority(schema):
    """A production upgrade must work with existing runs, grants and executions."""
    # The deployment API intentionally upgrades to head. Step back to the immutable 0001
    # snapshot to construct the exact database shape an older installation would contain.
    assert migrate("upgrade", schema).status == "applied"
    assert (
        migrate("downgrade", schema, revision="0001_initial_storage").current_revision
        == "0001_initial_storage"
    )
    ids = run(lambda: seed_0001_upgrade_graph(schema))
    upgraded = migrate("upgrade", schema)
    assert upgraded.current_revision == "0009_approval_events"
    assert (
        scalar(
            f'SELECT run_id FROM "{schema}"."execution" WHERE id = :id',
            id=ids["execution"],
        )
        == ids["run"]
    )
    assert (
        scalar(
            f'SELECT authority_revision FROM "{schema}"."run" WHERE id = :id',
            id=ids["run"],
        )
        == 3
    )
    # The old schema did not retain holder_state, so migration must not invent a replay hash.
    assert (
        scalar(
            f'SELECT release_request_hash FROM "{schema}".'
            '"system_capacity_reservation" WHERE id = :id',
            id=ids["reservation"],
        )
        is None
    )
    assert migrate("check", schema).status == "clean"


def test_precreated_schema_is_idempotent_but_foreign_objects_are_refused(schema):
    execute_sql(f'CREATE SCHEMA "{schema}"')
    assert migrate("upgrade", schema).status == "applied"
    assert migrate("check", schema).status == "clean"

    other = f"{schema}_foreign"
    execute_sql(f'CREATE SCHEMA "{other}"', f'CREATE TABLE "{other}".host_owned (id int)')
    try:
        with pytest.raises(MigrationPreflightError):
            migrate("upgrade", other)
        assert relation_snapshot(other) == {"host_owned"}
    finally:
        drop_schema(other)


def test_version_table_with_an_unknown_revision_is_refused(schema):
    execute_sql(
        f'CREATE SCHEMA "{schema}"',
        f'CREATE TABLE "{schema}".alembic_version (version_num varchar(32) primary key)',
        f"INSERT INTO \"{schema}\".alembic_version VALUES ('not-a-runtime-revision')",
    )
    with pytest.raises(MigrationStateError):
        migrate("upgrade", schema)
    assert relation_snapshot(schema) == {"alembic_version"}


def test_advisory_lock_serialises_concurrent_deployments(schema):
    execute_sql(f'CREATE SCHEMA "{schema}"')

    async def hold_and_migrate() -> MigrationResult:
        async with engine().connect() as connection:
            await connection.execute(
                sa.text("SELECT pg_advisory_lock(:key)"), {"key": advisory_key(schema)}
            )
            try:
                with pytest.raises(MigrationLockError):
                    await execute("upgrade", DSN or "", schema, "base", 0.5)
            finally:
                await connection.execute(
                    sa.text("SELECT pg_advisory_unlock(:key)"), {"key": advisory_key(schema)}
                )
        return await execute("upgrade", DSN or "", schema, "base", 5.0)

    result = run(hold_and_migrate)
    assert result.status == "applied"
    assert migrate("check", schema).status == "clean"


def test_three_way_consistency_matches_catalog_models_and_database(schema):
    assert compare_catalog_to_models() == ()
    assert migrate("upgrade", schema).status == "applied"

    async def compare() -> tuple[Any, ...]:
        return await compare_catalog_to_database(engine(), schema)

    assert run(compare) == ()


NOW = datetime(2026, 9, 22, tzinfo=UTC)


def test_database_rejects_invalid_rows(schema):
    assert migrate("upgrade", schema).status == "applied"
    agent_id = str(uuid.uuid4())
    insert_row(schema, "agent", id=agent_id, scope_id="a", key="sample")

    # Cross-scope composite foreign key (23503).
    rejected_row(
        schema,
        "agent_revision",
        "23503",
        id=str(uuid.uuid4()),
        scope_id="b",
        agent_id=agent_id,
        revision=1,
        manifest="{}",
        content_hash="a" * 64,
    )
    # Aggregate CHECK, then unique index (23514 / 23505).
    rejected_row(
        schema,
        "resource_pool",
        "23514",
        id=str(uuid.uuid4()),
        scope_id="a",
        pool_key="cpu",
        capacity=2,
        reserved=3,
        version=0,
    )
    insert_row(
        schema,
        "resource_pool",
        id=str(uuid.uuid4()),
        scope_id="a",
        pool_key="cpu",
        capacity=2,
        reserved=0,
        version=0,
    )
    rejected_row(
        schema,
        "resource_pool",
        "23505",
        id=str(uuid.uuid4()),
        scope_id="a",
        pool_key="cpu",
        capacity=2,
        reserved=0,
        version=0,
    )
    # domain_job handler identity is all-or-nothing.
    rejected_row(
        schema,
        "work_item",
        "23514",
        id=str(uuid.uuid4()),
        scope_id="a",
        subject_ref='{"kind": "thing"}',
        kind="domain_job",
        dedup_key="dedup-1",
        status="pending",
        payload_ref="payload:1",
        priority=0,
        lease_epoch=0,
        attempt_count=0,
        available_at=NOW,
        deadline_at=NOW + timedelta(hours=1),
    )
    # Platform capacity facts: aggregate CHECK, composite FK, request uniqueness and
    # release-fact pairing (23514 / 23503 / 23505 / 23514).
    insert_row(
        schema,
        "system_capacity",
        deployment_id="d1",
        resource_kind="run_slot",
        resource_key="group-a",
        resource_version=1,
        capacity=2,
        reserved=1,
        version=1,
    )
    rejected_row(
        schema,
        "system_capacity",
        "23514",
        deployment_id="d1",
        resource_kind="run_slot",
        resource_key="group-b",
        resource_version=1,
        capacity=1,
        reserved=2,
        version=1,
    )
    reservation = dict(
        deployment_id="d1",
        resource_kind="run_slot",
        resource_key="group-a",
        resource_version=1,
        status="reserved",
        holder_ref="run:1",
        request_id="req-1",
        amount=1,
        version=1,
        lease_until=NOW + timedelta(hours=1),
    )
    insert_row(schema, "system_capacity_reservation", id=str(uuid.uuid4()), **reservation)
    rejected_row(
        schema,
        "system_capacity_reservation",
        "23503",
        id=str(uuid.uuid4()),
        **{**reservation, "resource_key": "group-missing"},
    )
    rejected_row(
        schema, "system_capacity_reservation", "23505", id=str(uuid.uuid4()), **reservation
    )
    rejected_row(
        schema,
        "system_capacity_reservation",
        "23514",
        id=str(uuid.uuid4()),
        **{**reservation, "request_id": "req-2", "release_request_id": "req-2"},
    )
    # Partial unique index: at most one active revision per capability.
    capability_id = str(uuid.uuid4())
    insert_row(
        schema,
        "capability",
        id=capability_id,
        scope_id="a",
        namespace="builtin",
        key="search",
        kind="tool",
    )
    active = dict(
        scope_id="a",
        capability_id=capability_id,
        revision=1,
        content_digest="b" * 64,
        status="active",
        manifest="{}",
    )
    insert_row(schema, "capability_revision", id=str(uuid.uuid4()), **active)
    rejected_row(
        schema, "capability_revision", "23505", id=str(uuid.uuid4()), **{**active, "revision": 2}
    )


def test_immutability_is_declared_not_ddl_enforced(schema):
    """Documented boundary: no trigger/rule silently enforces the declared immutable set."""
    assert migrate("upgrade", schema).status == "applied"
    triggers = scalar(
        "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = :schema AND NOT t.tgisinternal",
        schema=schema,
    )
    assert triggers == 0


def test_application_role_has_no_ddl_but_can_read_and_write(schema):
    assert migrate("upgrade", schema).status == "applied"
    role = f"p02_app_{uuid.uuid4().hex[:10]}"
    password = uuid.uuid4().hex
    try:
        execute_sql(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
    except sa.exc.DBAPIError as exc:  # pragma: no cover - only without CREATEROLE
        pytest.skip(f"cannot create a test role: {type(exc).__name__}")
    try:
        execute_sql(
            f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"',
            f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"',
        )
        assert DSN is not None
        app_dsn = (
            sa.engine.make_url(DSN)
            .set(username=role, password=password)
            .render_as_string(hide_password=False)
        )
        statement, params = insert(
            schema,
            "resource_pool",
            id=str(uuid.uuid4()),
            scope_id="a",
            pool_key="app",
            capacity=1,
            reserved=0,
            version=0,
        )

        async def app_role_checks() -> None:
            engine = create_async_engine(app_dsn, pool_size=1, max_overflow=0)
            try:
                async with engine.begin() as connection:
                    await connection.execute(sa.text(statement), params)
                async with engine.connect() as connection:
                    count = await connection.execute(
                        sa.text(f'SELECT count(*) FROM "{schema}"."resource_pool"')
                    )
                    assert count.scalar() == 1
                for forbidden in (
                    f'CREATE TABLE "{schema}".nope (id int)',
                    f'DROP TABLE "{schema}"."resource_pool"',
                    f'ALTER TABLE "{schema}"."resource_pool" ADD COLUMN nope int',
                ):
                    with pytest.raises(sa.exc.ProgrammingError) as error:
                        async with engine.begin() as connection:
                            await connection.execute(sa.text(forbidden))
                    assert getattr(error.value.orig, "sqlstate", None) == "42501"
            finally:
                await engine.dispose()

        run(app_role_checks)
    finally:
        execute_sql(f'DROP OWNED BY "{role}"')
        execute_sql(f'DROP ROLE "{role}"')

# --- Review §8.2 P2-1/P2-2: engine settings and session boundaries -------------------


def test_statement_timeout_reaches_real_connections(schema):
    assert migrate("upgrade", schema).status == "applied"
    context = StorageContext(StorageConfig(dsn=DSN or "", schema=schema, statement_timeout_ms=1234))

    async def read_setting() -> Any:
        async with context.session() as session:
            return (
                await session.execute(
                    sa.text("SELECT setting::int FROM pg_settings WHERE name = 'statement_timeout'")
                )
            ).scalar()

    try:
        assert run(read_setting) == 1234
    finally:
        run(context.dispose)


def test_statement_timeout_cancels_a_slow_statement(schema):
    context = StorageContext(StorageConfig(dsn=DSN or "", schema=schema, statement_timeout_ms=1))

    async def slow() -> None:
        async with context.session() as session:
            await session.execute(sa.text("SELECT pg_sleep(0.5)"))

    try:
        with pytest.raises(sa.exc.DBAPIError) as error:
            run(slow)
        assert getattr(error.value.orig, "sqlstate", None) == "57014"
    finally:
        run(context.dispose)


def test_session_does_not_commit_while_transaction_does(schema):
    assert migrate("upgrade", schema).status == "applied"
    context = StorageContext(StorageConfig(dsn=DSN or "", schema=schema))
    uncommitted = str(uuid.uuid4())
    committed = str(uuid.uuid4())
    rolled_back = str(uuid.uuid4())

    def row(identifier: str, key: str) -> tuple[Any, dict[str, Any]]:
        return (
            sa.text(
                f'INSERT INTO "{schema}".agent (id, scope_id, key) '
                "VALUES (:id, :scope, :key)"
            ),
            {"id": identifier, "scope": "a", "key": key},
        )

    async def count(identifier: str) -> Any:
        async with context.session() as session:
            return (
                await session.execute(
                    sa.text(f'SELECT count(*) FROM "{schema}".agent WHERE id = :id'),
                    {"id": identifier},
                )
            ).scalar()

    async def exercise() -> None:
        # session() is a lifetime boundary: closing it without commit leaves nothing behind.
        async with context.session() as session:
            await session.execute(*row(uncommitted, "uncommitted"))
        assert await count(uncommitted) == 0
        # transaction() commits.
        async with context.transaction() as session:
            await session.execute(*row(committed, "committed"))
        assert await count(committed) == 1
        # transaction() rolls back on error.
        with pytest.raises(RuntimeError):
            async with context.transaction() as session:
                await session.execute(*row(rolled_back, "rolled-back"))
                raise RuntimeError("boom")
        assert await count(rolled_back) == 0

    try:
        run(exercise)
    finally:
        run(context.dispose)


# --- Review §8.2 P1-2: the lock must cover the uncommitted DDL window ----------------


def test_advisory_lock_covers_the_uncommitted_ddl_window(schema, monkeypatch):
    """Freeze the window between 'DDL executed' and 'transaction committed'."""
    import threading

    from agent_runtime.storage import migrate as migrate_module

    real_dispatch = migrate_module.dispatch
    ddl_done = threading.Event()
    release = threading.Event()

    async def paused_dispatch(
        connection: Any, command: str, schema_name: str, revision: str
    ) -> Any:
        result = await real_dispatch(connection, command, schema_name, revision)
        ddl_done.set()
        await asyncio.get_running_loop().run_in_executor(None, release.wait, 60)
        return result

    monkeypatch.setattr(migrate_module, "dispatch", paused_dispatch)
    outcome: dict[str, Any] = {}

    def first_deployment() -> None:
        # Its own loop: the shared test loop belongs to the main thread.
        try:
            outcome["result"] = asyncio.run(
                migrate_module.execute("upgrade", DSN or "", schema, "base", 60.0)
            )
        except Exception as exc:  # pragma: no cover - reported below
            outcome["error"] = exc

    worker = threading.Thread(target=first_deployment)
    worker.start()
    try:
        assert ddl_done.wait(120), "the first deployment never reached the post-DDL window"
        # The DDL exists in the uncommitted transaction but is invisible to other sessions.
        assert (
            scalar(
                "SELECT to_regclass(:relation) IS NULL",
                relation=f"{schema}.session",
            )
            is True
        )
        # A second deployment cannot take the lock in that window.
        with pytest.raises(MigrationLockError):
            migrate("upgrade", schema, lock_timeout_seconds=1.0)
    finally:
        release.set()
        worker.join(180)
    assert "error" not in outcome, outcome
    assert outcome["result"].status == "applied"
    assert (
        scalar("SELECT to_regclass(:relation) IS NOT NULL", relation=f"{schema}.session") is True
    )
    # The lock is released by the commit itself: the next deployment proceeds normally.
    assert migrate("upgrade", schema).status == "up-to-date"


# --- Review §8.2 P1-1: a changed live catalog must not rewrite history ---------------


def test_head_revision_ignores_a_changed_live_catalog(schema, monkeypatch):
    import dataclasses

    import agent_runtime.storage_spec as storage_spec

    head = snapshot_names()[-1]
    assert head == "0009_approval_events"
    future_table = dataclasses.replace(storage_spec.TABLES[0], name="future_table")
    monkeypatch.setattr(storage_spec, "TABLES", (*storage_spec.TABLES, future_table))
    monkeypatch.setattr(storage_spec, "storage_document", lambda: '{"mutated": true}')
    result = migrate("upgrade", schema)
    assert result.status == "applied"
    # The head schema is the head snapshot, byte-frozen: a live-catalog change is invisible.
    frozen = {table.name for table in load_snapshot(head)}
    assert relation_snapshot(schema) == frozen | {"alembic_version"}
    assert "future_table" not in relation_snapshot(schema)
    # 0001's own snapshot is untouched by the successor revision too.
    initial = {table.name for table in load_snapshot("0001_initial_storage")}
    assert initial | {
        "output_seal",
        "context_blob",
        "credential_request",
        "credential_audit",
        "credential_service_binding",
    } == frozen
    assert (
        scalar(
            "SELECT count(*) FROM information_schema.columns WHERE table_schema = :schema",
            schema=schema,
        )
        == sum(len(table.columns) for table in load_snapshot(head)) + 1
    )

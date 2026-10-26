"""Migration CLI for the Runtime-owned chain (DS-P02-01 requirements 4-6, Q40).

* the chain owns ``agent_runtime.alembic_version`` and never touches the host or public
  version table;
* concurrent deployments are serialised with a PostgreSQL advisory lock, so a second
  ``upgrade`` either waits for the configured timeout or fails - it never runs DDL in
  parallel;
* every command returns a structured JSON result, and failures keep the original
  exception chain without echoing the DSN or any credential.

Usage::

    python -m agent_runtime.storage.migrate upgrade --dsn postgresql+asyncpg://…
    python -m agent_runtime.storage.migrate current --dsn …
    python -m agent_runtime.storage.migrate check   --dsn …
"""

import argparse
import asyncio
import hashlib
import json
import os
import re
import time
from dataclasses import asdict, dataclass, field, replace
from typing import Any, Literal

import sqlalchemy as sa
from alembic import command as alembic_command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from agent_runtime.storage.consistency import compare_catalog_to_database
from agent_runtime.storage.migrations.snapshot import known_relations
from agent_runtime.storage.migrations.support import (
    DEFAULT_SCHEMA,
    SCHEMA_OPTION,
    VERSION_TABLE,
    script_location,
)

DSN_ENV = "AGENT_RUNTIME_DSN"
SCHEMA_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")
_DSN_CREDENTIALS = re.compile(r"://[^/@\s]*@")


class MigrationError(RuntimeError):
    """Base class for structured migration failures."""


class MigrationPreflightError(MigrationError):
    """The target schema or version table cannot be taken over safely (Q40)."""


class MigrationLockError(MigrationError):
    """Another deployment holds the migration advisory lock."""


class MigrationStateError(MigrationError):
    """The recorded revision is not part of this chain."""


@dataclass(frozen=True, slots=True)
class MigrationResult:
    """Structured, credential-free outcome of one CLI invocation."""

    status: Literal["applied", "up-to-date", "downgraded", "inspected", "clean", "differences"]
    command: str
    schema: str
    current_revision: str | None = None
    target_revision: str | None = None
    applied_revisions: tuple[str, ...] = ()
    lock: str = "not-requested"
    duration_ms: int = 0
    differences: tuple[dict[str, str], ...] = ()
    roles: dict[str, str] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["appliedRevisions"] = list(self.applied_revisions)
        payload["differences"] = list(self.differences)
        payload.pop("applied_revisions")
        return payload


def sanitize(message: str) -> str:
    """Never leak a DSN or password through an error message."""
    return _DSN_CREDENTIALS.sub("://***@", message)[:500]


def build_config(schema: str) -> Config:
    config = Config()
    config.set_main_option("script_location", script_location())
    config.set_main_option(SCHEMA_OPTION, schema)
    config.attributes["schema"] = schema
    return config


def advisory_key(schema: str) -> int:
    """Stable positive 63-bit advisory lock key for one target schema."""
    digest = hashlib.sha256(f"agent_runtime.migrate:{schema}".encode()).digest()
    return int.from_bytes(digest[:8], "big") >> 1


def known_revisions(schema: str) -> tuple[str, ...]:
    return tuple(
        script.revision
        for script in ScriptDirectory.from_config(build_config(schema)).walk_revisions()
    )


async def _scalar(connection: AsyncConnection, statement: sa.TextClause, **params: Any) -> Any:
    return (await connection.execute(statement, params)).scalar()


async def acquire_lock(connection: AsyncConnection, key: int, timeout_seconds: float) -> str:
    """Take a *transaction-scoped* advisory lock, retrying until the timeout expires.

    ``pg_try_advisory_xact_lock`` is released by PostgreSQL itself when the surrounding
    transaction commits or rolls back, so the lock necessarily covers the DDL **and** the
    version-table commit. A session lock released by hand before ``engine.begin()`` exits
    would leave a window in which a second deployment could enter preflight/DDL between
    the unlock and the commit (review §8.2 P1-2).
    """
    deadline = time.monotonic() + timeout_seconds
    while True:
        acquired = await _scalar(
            connection, sa.text("SELECT pg_try_advisory_xact_lock(:key)"), key=key
        )
        if acquired:
            return "transaction-scoped"
        if time.monotonic() >= deadline:
            raise MigrationLockError(
                "another migration holds the advisory lock for this schema "
                f"(waited {timeout_seconds:g}s)"
            )
        await asyncio.sleep(0.25)


async def preflight(connection: AsyncConnection, schema: str) -> dict[str, str]:
    """Verify owner/privilege and refuse to take over foreign objects (Q40)."""
    exists = await _scalar(
        connection,
        sa.text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = :schema)"),
        schema=schema,
    )
    current_user = str(await _scalar(connection, sa.text("SELECT current_user")))
    is_superuser = bool(
        await _scalar(
            connection,
            sa.text("SELECT rolsuper FROM pg_roles WHERE rolname = current_user"),
        )
    )
    if not exists:
        allowed = bool(
            await _scalar(
                connection,
                sa.text(
                    "SELECT has_database_privilege(current_user, current_database(), 'CREATE')"
                ),
            )
        )
        if not allowed and not is_superuser:
            raise MigrationPreflightError(f"role {current_user} may not create schema {schema}")
        return {"role": current_user, "schema": "absent", "privilege": "database CREATE"}

    owner = str(
        await _scalar(
            connection,
            sa.text("SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = :schema"),
            schema=schema,
        )
    )
    can_create = bool(
        await _scalar(
            connection,
            sa.text("SELECT has_schema_privilege(current_user, :schema, 'CREATE')"),
            schema=schema,
        )
    )
    rows = (
        await connection.execute(
            sa.text(
                "SELECT c.relname FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = :schema AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')"
            ),
            {"schema": schema},
        )
    ).all()
    allowed_relations = known_relations()
    unexpected = sorted(name for (name,) in rows if name not in allowed_relations)
    if unexpected:
        raise MigrationPreflightError(
            f"schema {schema} contains objects this chain does not own: "
            + ", ".join(unexpected[:8])
        )
    if owner != current_user and not can_create and not is_superuser:
        raise MigrationPreflightError(
            f"role {current_user} neither owns schema {schema} nor has CREATE on it"
        )
    has_version_table = bool(
        await _scalar(
            connection,
            sa.text("SELECT to_regclass(:relation) IS NOT NULL"),
            relation=f"{schema}.{VERSION_TABLE}",
        )
    )
    if has_version_table:
        recorded = tuple(
            (
                await connection.execute(
                    sa.text(f'SELECT version_num FROM "{schema}"."{VERSION_TABLE}"')
                )
            ).scalars()
        )
        known = known_revisions(schema)
        # An empty version table is the legitimate post-``downgrade base`` state.
        if len(recorded) > 1 or (recorded and recorded[0] not in known):
            raise MigrationStateError(
                f"{schema}.{VERSION_TABLE} records {list(recorded)!r}, which is not a "
                "revision of the Runtime chain"
            )
    privilege = "CREATE" if can_create else ("owner" if owner == current_user else "unknown")
    return {"role": current_user, "schema": owner, "privilege": privilege}


async def current_revision(connection: AsyncConnection, schema: str) -> str | None:
    def read(sync_connection: sa.Connection) -> str | None:
        context = MigrationContext.configure(
            sync_connection,
            opts={"version_table_schema": schema, "version_table": VERSION_TABLE},
        )
        return context.get_current_revision()

    return await connection.run_sync(read)


def run_chain(config: Config, command: str, revision: str) -> None:
    """Invoke Alembic on the sync connection stored in ``config.attributes``."""
    if command == "upgrade":
        alembic_command.upgrade(config, revision)
    else:
        alembic_command.downgrade(config, revision)


async def dispatch(
    connection: AsyncConnection,
    command: str,
    schema: str,
    revision: str,
) -> MigrationResult:
    """Run one command on an already-locked connection."""
    roles = await preflight(connection, schema)
    if command == "upgrade":
        # Alembic bootstraps ``<schema>.alembic_version`` before the first revision runs,
        # so the schema itself must exist already. This is still DDL performed by the
        # migration role, inside the same transaction as the migration: a failure rolls
        # both back.
        await connection.execute(sa.text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    before = await current_revision(connection, schema)
    if command == "current":
        return MigrationResult(
            status="inspected",
            command=command,
            schema=schema,
            current_revision=before,
            target_revision=revision,
            roles=roles,
        )
    if command == "check":
        differences = await compare_catalog_to_database(connection, schema)
        return MigrationResult(
            status="clean" if not differences else "differences",
            command=command,
            schema=schema,
            current_revision=before,
            differences=tuple(item.as_dict() for item in differences),
            roles=roles,
        )
    config = build_config(schema)
    head = ScriptDirectory.from_config(config).get_current_head()
    target = revision if command == "downgrade" else head
    if target is None:  # pragma: no cover - the Runtime chain always has a head revision
        raise MigrationError("the Runtime chain has no head revision to upgrade to")

    def run(sync_connection: sa.Connection) -> None:
        config.attributes["connection"] = sync_connection
        run_chain(config, command, target)

    await connection.run_sync(run)
    after = await current_revision(connection, schema)
    status: Literal["applied", "up-to-date", "downgraded"] = (
        "up-to-date"
        if command == "upgrade" and before == after
        else "downgraded"
        if command == "downgrade"
        else "applied"
    )
    return MigrationResult(
        status=status,
        command=command,
        schema=schema,
        current_revision=after,
        target_revision=target,
        applied_revisions=(after,) if after and after != before else (),
        roles=roles,
    )


async def execute(
    command: str,
    dsn: str,
    schema: str,
    revision: str,
    lock_timeout_seconds: float,
) -> MigrationResult:
    started = time.monotonic()
    engine: AsyncEngine = create_async_engine(dsn, pool_size=2, max_overflow=0)
    key = advisory_key(schema)
    lock_state = "not-requested"
    try:
        # One explicit transaction per invocation: the schema bootstrap, the DDL chain and
        # the version-table update commit or roll back together (transactional DDL), and
        # the transaction-scoped advisory lock is held until that commit/rollback finishes.
        async with engine.begin() as connection:
            lock_state = await acquire_lock(connection, key, lock_timeout_seconds)
            result = await dispatch(connection, command, schema, revision)
    finally:
        await engine.dispose()
    return replace(
        result,
        lock=lock_state,
        duration_ms=int((time.monotonic() - started) * 1000),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["upgrade", "downgrade", "current", "check"])
    parser.add_argument("--dsn", default=None, help=f"defaults to ${DSN_ENV}")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--revision", default="base")
    parser.add_argument("--lock-timeout-seconds", type=float, default=30.0)
    args = parser.parse_args(argv)

    if not SCHEMA_PATTERN.match(args.schema):
        print(
            json.dumps(
                {
                    "status": "failed",
                    "command": args.command,
                    "schema": args.schema,
                    "errorType": "UsageError",
                    "errorMessage": "schema must be a lowercase PostgreSQL identifier",
                }
            )
        )
        return 2
    dsn = args.dsn or os.environ.get(DSN_ENV)
    if not dsn:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "command": args.command,
                    "schema": args.schema,
                    "errorType": "UsageError",
                    "errorMessage": f"--dsn or ${DSN_ENV} is required",
                }
            )
        )
        return 2
    try:
        result = asyncio.run(
            execute(args.command, dsn, args.schema, args.revision, args.lock_timeout_seconds)
        )
    except Exception as exc:  # the failure class is reported, never swallowed
        print(
            json.dumps(
                {
                    "status": "failed",
                    "command": args.command,
                    "schema": args.schema,
                    "errorType": type(exc).__name__,
                    "errorMessage": sanitize(str(exc)),
                }
            )
        )
        return 1
    print(json.dumps(result.as_dict(), default=str))
    return (
        0
        if result.status
        in {
            "applied",
            "up-to-date",
            "downgraded",
            "inspected",
            "clean",
        }
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())

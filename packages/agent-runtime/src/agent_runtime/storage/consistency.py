"""Three-way structural consistency: catalog <-> ORM models <-> live PostgreSQL.

``agent_runtime.contracts.storage.json`` (derived from :mod:`agent_runtime.storage_spec`)
is the specification source; this module turns it, the committed declarative models and a
reflected database into the same normalised :class:`TableShape` and reports **every**
difference - extra objects on either side fail too (DS-P02-01 requirement 3).

Normalisation is required because PostgreSQL re-renders constraint text and reflection
loses some spelling (verified with the DS-P02-01 prototype, F4-F6):

* comments come from ``pg_description``: reflection reports ``None``;
* types are compared as catalog ``SqlType`` buckets, not dialect class names;
* casts, quoting and whitespace in CHECK text/predicates are folded away;
* ``x IN (a, b)`` and ``x = ANY (ARRAY[a, b])`` are the same predicate (F5);
* ``ondelete`` reflecting as ``None`` means ``NO ACTION`` (F6).
"""

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa
from sqlalchemy import CheckConstraint, ForeignKeyConstraint, Index
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.types import TypeEngine

from agent_runtime.storage_spec import TABLES
from agent_runtime.storage_spec import Table as TableSpec

#: Catalog ``SqlType`` -> declarative type. JSONB stays PostgreSQL-specific: the standard
#: implementation never degrades to SQLite-compatible types (DS-P02-01 decision).
TYPE_MAP: dict[str, TypeEngine[Any]] = {
    "UUID": sa.Uuid(),
    "TEXT": sa.Text(),
    "BIGINT": sa.BigInteger(),
    "INTEGER": sa.Integer(),
    "TIMESTAMPTZ": sa.DateTime(timezone=True),
    "JSONB": postgresql.JSONB(),
    "BYTEA": postgresql.BYTEA(),
}

#: The Runtime chain owns this relation inside its schema; it is not part of the catalog.
RUNTIME_VERSION_TABLE = "alembic_version"

_CAST = re.compile(
    r"::\s*(?:text|character varying|bigint|integer|uuid|timestamp(?: with(?:out)? time zone)?"
    r"|numeric|boolean|jsonb|smallint)"
)
_ARRAY_ANY = re.compile(r"([a-z_][a-z0-9_.]*)\s*=\s*any\s*\(\s*array\s*\[([^\]]*)\]\s*\)")
_WHITESPACE = re.compile(r"\s+")
_COMMA = re.compile(r"\s*,\s*")


@dataclass(frozen=True, slots=True)
class ColumnShape:
    sql_type: str | None
    nullable: bool
    default: str | None
    comment: str


@dataclass(frozen=True, slots=True)
class IndexShape:
    columns: tuple[str, ...]
    unique: bool
    predicate: str


@dataclass(frozen=True, slots=True)
class ForeignKeyShape:
    columns: tuple[str, ...]
    target: str
    target_columns: tuple[str, ...]
    on_delete: str
    deferrable: bool
    initially_deferred: bool


@dataclass(frozen=True, slots=True)
class TableShape:
    columns: Mapping[str, ColumnShape]
    primary_key: tuple[str, ...]
    indexes: tuple[IndexShape, ...]
    checks: tuple[str, ...]
    foreign_keys: tuple[ForeignKeyShape, ...]
    immutable: bool


@dataclass(frozen=True, slots=True)
class Difference:
    table: str
    aspect: str
    expected: object
    actual: object

    def describe(self) -> str:
        return f"{self.table}.{self.aspect}: expected {self.expected!r}, found {self.actual!r}"

    def as_dict(self) -> dict[str, str]:
        return {
            "table": self.table,
            "aspect": self.aspect,
            "expected": str(self.expected),
            "actual": str(self.actual),
        }


def normalize_sql(text: str) -> str:
    """Fold PostgreSQL's re-rendering of a predicate into a comparable canonical text."""
    lowered = _CAST.sub("", text.lower()).replace('"', "")
    lowered = _ARRAY_ANY.sub(lambda match: f"{match.group(1)} in ({match.group(2)})", lowered)
    lowered = _COMMA.sub(",", lowered)
    return _WHITESPACE.sub(" ", lowered).strip()


def initially_deferred(options: Mapping[str, Any]) -> bool:
    """Read SQLAlchemy's inspector spelling of ``INITIALLY DEFERRED`` (finding F6)."""
    marker = options.get("initially_deferred", options.get("initially"))
    return str(marker or "").upper() == "DEFERRED"


def normalize_default(text: str | None) -> str | None:
    if text is None:
        return None
    folded = normalize_sql(text).strip("'")
    return "current_timestamp" if folded in {"now()", "current_timestamp"} else folded


def expressions_match(expected: str, actual: str) -> bool:
    """Compare CHECK texts, tolerating PostgreSQL's redundant parentheses."""
    if normalize_sql(expected) == normalize_sql(actual):
        return True
    return strip_parentheses(normalize_sql(expected)) == strip_parentheses(normalize_sql(actual))


def strip_parentheses(text: str) -> str:
    return _WHITESPACE.sub(" ", text.replace("(", " ").replace(")", " ")).strip()


def sql_type_of(type_: TypeEngine[Any]) -> str | None:
    """Map an ORM or reflected type onto the catalog ``SqlType`` buckets."""
    if isinstance(type_, postgresql.BYTEA) or type_.__class__.__name__.upper() == "BYTEA":
        return "BYTEA"
    if isinstance(type_, postgresql.JSONB) or type_.__class__.__name__.upper() == "JSONB":
        return "JSONB"
    if isinstance(type_, sa.Uuid) or type_.__class__.__name__.upper() == "UUID":
        return "UUID"
    if isinstance(type_, sa.Text):
        return "TEXT"
    if isinstance(type_, sa.BigInteger):
        return "BIGINT"
    if isinstance(type_, sa.Integer):
        return "INTEGER"
    if isinstance(type_, sa.DateTime):
        return "TIMESTAMPTZ" if type_.timezone else None
    return None


def _predicate(index: Index) -> str | None:
    """Partial-index predicate of a declarative index, if any."""
    where = index.dialect_options["postgresql"].get("where", None)
    return None if where is None else str(where)


def _column_shape(column: sa.Column[Any], comment: str | None = None) -> ColumnShape:
    return ColumnShape(
        sql_type=sql_type_of(column.type),
        nullable=bool(column.nullable),
        default=normalize_default(
            None
            if column.server_default is None
            else str(getattr(column.server_default, "arg", column.server_default))
        ),
        comment=column.comment or comment or "",
    )


def catalog_shapes(tables: Iterable[TableSpec] = TABLES) -> dict[str, TableShape]:
    """Normalise the frozen catalog."""
    shapes: dict[str, TableShape] = {}
    for spec in tables:
        shapes[spec.name] = TableShape(
            columns={
                column.name: ColumnShape(
                    sql_type=column.sql_type,
                    nullable=column.nullable,
                    default=normalize_default(column.default),
                    comment=column.comment,
                )
                for column in spec.columns
            },
            primary_key=spec.primary_key,
            indexes=tuple(
                sorted(
                    (
                        IndexShape(
                            columns=index.columns,
                            unique=index.unique,
                            predicate=normalize_sql(index.predicate or ""),
                        )
                        for index in spec.indexes
                    ),
                    key=lambda shape: (shape.columns, shape.unique, shape.predicate),
                )
            ),
            checks=tuple(sorted(normalize_sql(check.expression) for check in spec.checks)),
            foreign_keys=tuple(
                sorted(
                    (
                        ForeignKeyShape(
                            columns=fk.columns,
                            target=fk.target,
                            target_columns=fk.target_columns,
                            on_delete=fk.on_delete or "NO ACTION",
                            deferrable=fk.deferrable,
                            initially_deferred=fk.initially_deferred,
                        )
                        for fk in spec.foreign_keys
                    ),
                    key=lambda shape: (shape.columns, shape.target, shape.target_columns),
                )
            ),
            immutable=spec.immutable,
        )
    return shapes


def model_shapes() -> dict[str, TableShape]:
    """Normalise the committed declarative models (importing the model package)."""
    from agent_runtime.storage import Base  # noqa: PLC0415 - avoids a package import cycle

    shapes: dict[str, TableShape] = {}
    for key, table in Base.metadata.tables.items():
        name = key.split(".")[-1]
        shapes[name] = TableShape(
            columns={column.name: _column_shape(column) for column in table.columns},
            primary_key=tuple(column.name for column in table.primary_key.columns),
            indexes=tuple(
                sorted(
                    (
                        IndexShape(
                            columns=tuple(column.name for column in index.columns),
                            unique=bool(index.unique),
                            predicate=normalize_sql(str(_predicate(index) or "")),
                        )
                        for index in table.indexes
                    ),
                    key=lambda shape: (shape.columns, shape.unique, shape.predicate),
                )
            ),
            checks=tuple(
                sorted(
                    normalize_sql(str(constraint.sqltext))
                    for constraint in table.constraints
                    if isinstance(constraint, CheckConstraint)
                )
            ),
            foreign_keys=tuple(
                sorted(
                    (
                        ForeignKeyShape(
                            columns=tuple(element.parent.name for element in constraint.elements),
                            target=next(iter(constraint.elements)).column.table.name,
                            target_columns=tuple(
                                element.column.name for element in constraint.elements
                            ),
                            on_delete=constraint.ondelete or "NO ACTION",
                            deferrable=bool(constraint.deferrable),
                            initially_deferred=constraint.initially == "DEFERRED",
                        )
                        for constraint in table.constraints
                        if isinstance(constraint, ForeignKeyConstraint)
                    ),
                    key=lambda shape: (shape.columns, shape.target, shape.target_columns),
                )
            ),
            immutable=bool(table.info.get("immutable", False)),
        )
    return shapes


def database_shapes(
    inspector: sa.Inspector,
    comments: Mapping[tuple[str, str], str],
    schema: str,
    *,
    tables: Iterable[TableSpec] = TABLES,
) -> dict[str, TableShape]:
    """Normalise a live database through the SQLAlchemy inspector."""
    shapes: dict[str, TableShape] = {}
    for spec in tables:
        columns = {
            column["name"]: ColumnShape(
                sql_type=sql_type_of(column["type"]),
                nullable=bool(column["nullable"]),
                default=normalize_default(column.get("default")),
                comment=comments.get((spec.name, column["name"]), ""),
            )
            for column in inspector.get_columns(spec.name, schema=schema)
        }
        pk = inspector.get_pk_constraint(spec.name, schema=schema)
        pk_columns = tuple(str(name) for name in pk["constrained_columns"] or ())
        indexes = tuple(
            sorted(
                (
                    IndexShape(
                        columns=tuple(str(name) for name in index["column_names"] or ()),
                        unique=bool(index["unique"]),
                        predicate=normalize_sql(
                            str(
                                (index.get("dialect_options") or {}).get("postgresql_where", "")
                                or ""
                            )
                        ),
                    )
                    for index in inspector.get_indexes(spec.name, schema=schema)
                    if index.get("column_names")
                ),
                key=lambda shape: (shape.columns, shape.unique, shape.predicate),
            )
        )
        checks = tuple(
            sorted(
                normalize_sql(str(check.get("sqltext", "")))
                for check in inspector.get_check_constraints(spec.name, schema=schema)
            )
        )
        foreign_keys = tuple(
            sorted(
                (
                    ForeignKeyShape(
                        columns=tuple(str(name) for name in fk["constrained_columns"] or ()),
                        target=fk["referred_table"],
                        target_columns=tuple(str(name) for name in fk["referred_columns"] or ()),
                        on_delete=(fk.get("options") or {}).get("ondelete") or "NO ACTION",
                        deferrable=bool((fk.get("options") or {}).get("deferrable")),
                        initially_deferred=initially_deferred(fk.get("options") or {}),
                    )
                    for fk in inspector.get_foreign_keys(spec.name, schema=schema)
                ),
                key=lambda shape: (shape.columns, shape.target, shape.target_columns),
            )
        )
        shapes[spec.name] = TableShape(
            columns=columns,
            primary_key=pk_columns,
            indexes=indexes,
            checks=checks,
            foreign_keys=foreign_keys,
            immutable=False,
        )
    return shapes


def column_comments(connection: sa.Connection, schema: str) -> dict[tuple[str, str], str]:
    """Read column comments from the catalog: reflection reports ``None`` (F4)."""
    rows = connection.execute(
        sa.text(
            "SELECT c.relname, a.attname, COALESCE(d.description, '') "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 "
            "AND NOT a.attisdropped "
            "LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum "
            "WHERE n.nspname = :schema"
        ),
        {"schema": schema},
    )
    return {(row[0], row[1]): row[2] for row in rows}


def compare_shapes(
    expected: Mapping[str, TableShape],
    actual: Mapping[str, TableShape],
    *,
    compare_immutable: bool = True,
) -> tuple[Difference, ...]:
    """Report every missing, extra or different structure on either side."""
    differences: list[Difference] = []
    for name in sorted(set(expected) - set(actual)):
        differences.append(Difference(name, "table", "present", "missing"))
    for name in sorted(set(actual) - set(expected)):
        differences.append(Difference(name, "table", "absent", "unexpected table"))
    for name in sorted(set(expected) & set(actual)):
        want = expected[name]
        have = actual[name]
        differences.extend(_compare_table(name, want, have, compare_immutable))
    return tuple(differences)


def _compare_table(
    name: str,
    want: TableShape,
    have: TableShape,
    compare_immutable: bool,
) -> list[Difference]:
    differences: list[Difference] = []
    for column in sorted(set(want.columns) - set(have.columns)):
        differences.append(Difference(name, f"column:{column}", "present", "missing"))
    for column in sorted(set(have.columns) - set(want.columns)):
        differences.append(Difference(name, f"column:{column}", "absent", "unexpected column"))
    for column in sorted(set(want.columns) & set(have.columns)):
        expected = want.columns[column]
        actual = have.columns[column]
        for aspect, left, right in (
            ("sql_type", expected.sql_type, actual.sql_type),
            ("nullable", expected.nullable, actual.nullable),
            ("default", expected.default, actual.default),
            ("comment", expected.comment, actual.comment),
        ):
            if left != right:
                differences.append(Difference(name, f"{column}.{aspect}", left, right))
    if tuple(want.primary_key) != tuple(have.primary_key):
        differences.append(Difference(name, "primary_key", want.primary_key, have.primary_key))
    for index in _index_differences(want, have):
        differences.append(Difference(name, "index", index[0], index[1]))
    expected_checks = set(want.checks)
    for check in sorted(expected_checks - set(have.checks)):
        if not any(expressions_match(check, other) for other in have.checks):
            differences.append(Difference(name, "check", check, "missing"))
    for check in sorted(set(have.checks) - expected_checks):
        if not any(expressions_match(check, other) for other in want.checks):
            differences.append(Difference(name, "check", "absent", check))
    for fk in _fk_differences(want, have):
        differences.append(Difference(name, "foreign_key", fk[0], fk[1]))
    if compare_immutable and want.immutable != have.immutable:
        differences.append(Difference(name, "immutable", want.immutable, have.immutable))
    return differences


def _index_differences(want: TableShape, have: TableShape) -> list[tuple[IndexShape, object]]:
    differences: list[tuple[IndexShape, object]] = []
    remaining = list(have.indexes)
    for index in want.indexes:
        for candidate in remaining:
            if (
                index.columns == candidate.columns
                and index.unique == candidate.unique
                and expressions_match(index.predicate, candidate.predicate)
            ):
                remaining.remove(candidate)
                break
        else:
            differences.append((index, "missing"))
    for index in remaining:
        differences.append((index, "unexpected index"))
    return differences


def _fk_differences(want: TableShape, have: TableShape) -> list[tuple[ForeignKeyShape, object]]:
    differences: list[tuple[ForeignKeyShape, object]] = []
    remaining = list(have.foreign_keys)
    for foreign_key in want.foreign_keys:
        for candidate in remaining:
            if foreign_key == candidate:
                remaining.remove(candidate)
                break
        else:
            differences.append((foreign_key, "missing"))
    for foreign_key in remaining:
        differences.append((foreign_key, "unexpected foreign key"))
    return differences


def compare_catalog_to_models() -> tuple[Difference, ...]:
    """Catalog <-> committed ORM models (no database required)."""
    return compare_shapes(catalog_shapes(), model_shapes())


def compare_models_to_catalog_sizes() -> tuple[Difference, ...]:
    """Table and column counts must match the frozen catalog exactly."""
    expected = catalog_shapes()
    actual = model_shapes()
    differences: list[Difference] = []
    if len(expected) != len(actual):
        differences.append(Difference("*", "tables", len(expected), len(actual)))
    expected_columns = sum(len(shape.columns) for shape in expected.values())
    actual_columns = sum(len(shape.columns) for shape in actual.values())
    if expected_columns != actual_columns:
        differences.append(Difference("*", "columns", expected_columns, actual_columns))
    return tuple(differences)


async def compare_catalog_to_database(
    source: AsyncEngine | AsyncConnection,
    schema: str,
    *,
    tables: Iterable[TableSpec] = TABLES,
) -> tuple[Difference, ...]:
    """Catalog <-> live database, including comments read from ``pg_description``."""
    if isinstance(source, AsyncEngine):
        async with source.connect() as connection:
            return await compare_catalog_to_database(connection, schema, tables=tables)
    connection = source
    relation_names = await connection.run_sync(
        lambda sync: set(sa.inspect(sync).get_table_names(schema=schema))
    )
    comments = await connection.run_sync(lambda sync: column_comments(sync, schema))
    expected = catalog_shapes(tables)
    present = tuple(spec for spec in tables if spec.name in relation_names)
    shapes = await connection.run_sync(
        lambda sync: database_shapes(sa.inspect(sync), comments, schema, tables=present)
    )
    differences = list(compare_shapes(expected, shapes, compare_immutable=False))
    unexpected = sorted(relation_names - set(expected) - {RUNTIME_VERSION_TABLE})
    for name in unexpected:
        differences.append(Difference(name, "table", "absent", "unexpected table"))
    return tuple(differences)


__all__ = [
    "ColumnShape",
    "Difference",
    "ForeignKeyShape",
    "IndexShape",
    "TableShape",
    "TYPE_MAP",
    "catalog_shapes",
    "column_comments",
    "compare_catalog_to_database",
    "compare_catalog_to_models",
    "compare_models_to_catalog_sizes",
    "compare_shapes",
    "database_shapes",
    "expressions_match",
    "initially_deferred",
    "model_shapes",
    "normalize_default",
    "normalize_sql",
    "sql_type_of",
]

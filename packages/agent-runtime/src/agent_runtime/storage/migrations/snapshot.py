"""Immutable catalog snapshots consumed by the Runtime migration chain.

A historical revision must never read the *live* catalog: if it did, adding a field or a
later revision would silently rewrite (or break) DDL that was already applied elsewhere
(review §8.2 P1-1). Each revision therefore owns an immutable snapshot JSON under
``catalogs/`` that ships with the wheel, and this module is the only loader.

Deliberately imports nothing from :mod:`agent_runtime.storage_spec` (no live catalog) and
no database driver.
"""

import hashlib
import json
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

SNAPSHOT_PACKAGE = "agent_runtime.storage.migrations.catalogs"


@dataclass(frozen=True, slots=True)
class SnapshotColumn:
    name: str
    sql_type: str
    nullable: bool
    default: str | None
    comment: str


@dataclass(frozen=True, slots=True)
class SnapshotIndex:
    columns: tuple[str, ...]
    unique: bool
    predicate: str | None


@dataclass(frozen=True, slots=True)
class SnapshotCheck:
    columns: tuple[str, ...]
    expression: str


@dataclass(frozen=True, slots=True)
class SnapshotForeignKey:
    columns: tuple[str, ...]
    target: str
    target_columns: tuple[str, ...]
    on_delete: str
    deferrable: bool
    initially_deferred: bool


@dataclass(frozen=True, slots=True)
class SnapshotTable:
    name: str
    columns: tuple[SnapshotColumn, ...]
    primary_key: tuple[str, ...]
    indexes: tuple[SnapshotIndex, ...]
    checks: tuple[SnapshotCheck, ...]
    foreign_keys: tuple[SnapshotForeignKey, ...]
    immutable: bool


def snapshot_path(name: str) -> Path:
    """Filesystem path of one snapshot (works from an installed wheel)."""
    return Path(str(files(SNAPSHOT_PACKAGE).joinpath(f"{name}.json")))


def snapshot_digest(name: str) -> str:
    """sha256 of the raw snapshot bytes, compared against the revision's constant."""
    return hashlib.sha256(snapshot_path(name).read_bytes()).hexdigest()


def load_snapshot(name: str) -> tuple[SnapshotTable, ...]:
    """Parse an immutable snapshot into plain frozen structures."""
    document = json.loads(snapshot_path(name).read_text(encoding="utf-8"))
    return tuple(_table(entry) for entry in document["tables"])


def snapshot_names() -> tuple[str, ...]:
    """Every snapshot shipped with the wheel, sorted by revision name."""
    return tuple(
        sorted(
            entry.name[: -len(".json")]
            for entry in files(SNAPSHOT_PACKAGE).iterdir()
            if entry.name.endswith(".json") and not entry.name.startswith("_")
        )
    )


def known_relations() -> frozenset[str]:
    """Union of every relation the chain can create, across all revisions.

    The CLI preflight uses this instead of the live catalog so that taking over an
    existing schema stays correct after the catalog evolves.
    """
    names = {"alembic_version"}
    for name in snapshot_names():
        names.update(table.name for table in load_snapshot(name))
    return frozenset(names)


def _table(entry: dict[str, object]) -> SnapshotTable:
    return SnapshotTable(
        name=str(entry["name"]),
        columns=tuple(
            SnapshotColumn(
                name=str(column["name"]),
                sql_type=str(column["sql_type"]),
                nullable=bool(column["nullable"]),
                default=None if column["default"] is None else str(column["default"]),
                comment=str(column["comment"]),
            )
            for column in _entries(entry["columns"])
        ),
        primary_key=_strings(entry["primary_key"]),
        indexes=tuple(
            SnapshotIndex(
                columns=_strings(index["columns"]),
                unique=bool(index["unique"]),
                predicate=None if index["predicate"] is None else str(index["predicate"]),
            )
            for index in _entries(entry["indexes"])
        ),
        checks=tuple(
            SnapshotCheck(
                columns=_strings(check["columns"]),
                expression=str(check["expression"]),
            )
            for check in _entries(entry["checks"])
        ),
        foreign_keys=tuple(
            SnapshotForeignKey(
                columns=_strings(foreign_key["columns"]),
                target=str(foreign_key["target"]),
                target_columns=_strings(foreign_key["target_columns"]),
                on_delete=str(foreign_key["on_delete"]),
                deferrable=bool(foreign_key["deferrable"]),
                initially_deferred=bool(foreign_key["initially_deferred"]),
            )
            for foreign_key in _entries(entry["foreign_keys"])
        ),
        immutable=bool(entry["immutable"]),
    )


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):  # pragma: no cover - snapshots are validated
        raise TypeError("snapshot name list expected")
    return tuple(str(name) for name in value)


def _entries(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):  # pragma: no cover - snapshots are validated
        raise TypeError("snapshot entry must be a list")
    return [dict(entry) for entry in value if isinstance(entry, dict)]


__all__ = [
    "SNAPSHOT_PACKAGE",
    "SnapshotCheck",
    "SnapshotColumn",
    "SnapshotForeignKey",
    "SnapshotIndex",
    "SnapshotTable",
    "known_relations",
    "load_snapshot",
    "snapshot_digest",
    "snapshot_names",
    "snapshot_path",
]

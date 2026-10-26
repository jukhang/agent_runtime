"""Console entry point for the Runtime migration CLI.

The console script ships in the *base* wheel, while the migration implementation lives
behind the ``storage`` extra. This module therefore imports nothing heavy: it defers the
storage import, and when the extra is genuinely absent it prints a stable,
traceback-free JSON error with an installation hint and a non-zero exit code.

Only a *confirmed* missing storage dependency is classified as ``MissingStorageExtra``
(Codex review §9.3): an ``ImportError`` raised from inside an installed adapter keeps its
real failure category instead of being misreported as "not installed".
"""

import importlib.util
import json
import sys
from typing import Any

#: Exit code used when the storage extra is not installed.
MISSING_EXTRA_EXIT = 3

#: Third-party packages the ``storage`` extra provides.
STORAGE_DEPENDENCIES = frozenset({"sqlalchemy", "alembic", "asyncpg", "greenlet"})

#: The adapter package itself; absent exactly when the extra was never installed.
STORAGE_PACKAGE = "agent_runtime.storage"

#: Stable, scriptable message: no traceback, no credentials, one actionable command.
INSTALL_HINT = (
    "the standard PostgreSQL storage adapter is not installed; "
    "install it with `pip install 'agent-runtime[storage]'` "
    "(dependencies: SQLAlchemy, Alembic, asyncpg)"
)


def missing_extra_payload(module: str | None = None) -> dict[str, Any]:
    """Structured failure payload for a base-wheel install without the extra."""
    return {
        "status": "failed",
        "command": None,
        "schema": None,
        "errorType": "MissingStorageExtra",
        "errorMessage": INSTALL_HINT,
        "missingModule": module,
    }


def storage_package_available() -> bool:
    """True when the adapter package itself is importable in this interpreter."""
    try:
        return importlib.util.find_spec(STORAGE_PACKAGE) is not None
    except (ImportError, ValueError):  # pragma: no cover - broken/partial installation
        return False


def classify_import_error(exc: ImportError, *, storage_package_available: bool) -> bool:
    """Decide whether ``exc`` means "the storage extra is not installed"."""
    missing = getattr(exc, "name", None)
    if missing:
        top_level = missing.split(".")[0]
        if top_level in STORAGE_DEPENDENCIES:
            return True
        if missing == STORAGE_PACKAGE or missing.startswith(f"{STORAGE_PACKAGE}."):
            # The adapter package is only "missing" when it truly cannot be found; a
            # submodule failing inside an installed adapter is an internal defect.
            return not storage_package_available
        return False
    # No module name: only a confirmed absent adapter package is a missing extra.
    return not storage_package_available


def is_missing_storage_extra(exc: ImportError) -> bool:
    return classify_import_error(exc, storage_package_available=storage_package_available())


def main(argv: list[str] | None = None) -> int:
    """Delegate to the storage CLI, or fail clearly when the extra is absent."""
    try:
        from agent_runtime.storage.migrate import main as storage_main
    except ImportError as exc:
        if not is_missing_storage_extra(exc):
            # Preserve the real failure category: an internal import defect or a broken
            # installation must not be reported as a missing extra.
            raise
        print(json.dumps(missing_extra_payload(getattr(exc, "name", None))))
        return MISSING_EXTRA_EXIT
    return storage_main(argv if argv is not None else sys.argv[1:])


__all__ = [
    "INSTALL_HINT",
    "MISSING_EXTRA_EXIT",
    "STORAGE_DEPENDENCIES",
    "STORAGE_PACKAGE",
    "classify_import_error",
    "is_missing_storage_extra",
    "main",
    "missing_extra_payload",
    "storage_package_available",
]


if __name__ == "__main__":
    raise SystemExit(main())

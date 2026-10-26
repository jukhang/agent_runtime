"""Declarative base and metadata for the standard PostgreSQL storage adapter.

The models in :mod:`agent_runtime.storage.models` are explicit, reviewed artifacts;
``agent_runtime.contracts.storage.json`` remains the specification source and the
catalog/ORM consistency check in :mod:`agent_runtime.storage.consistency` is a hard gate.

This module imports no host application code, opens no connection and reads no global
configuration, so importing :mod:`agent_runtime.storage` stays side-effect free (Q38/Q40).
"""

from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

#: Deterministic constraint naming. Every constraint in the models is named explicitly
#: (``pk_``/``ix_``/``fk_``/``ck_`` plus a per-table offset), so the convention only
#: backstops objects a future revision adds without a name. Names are part of the
#: migration surface: they must stay stable across revisions for ``downgrade``.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Declarative base for ``agent_runtime`` rows.

    The metadata is deliberately *schema-free*: the physical schema is a deployment and
    migration input (``agent_runtime`` in production, a random schema in tests), passed to
    DDL and reflection rather than baked into the models.
    """

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


__all__ = ["Base", "NAMING_CONVENTION"]

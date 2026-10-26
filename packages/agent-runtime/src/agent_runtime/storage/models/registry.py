"""Agent and capability registries with immutable revisions. (DS-P02-01).

Explicit declarative models; ``contracts/storage.json`` is the specification source.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    PrimaryKeyConstraint,
    Text,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from agent_runtime.storage.base import Base


class AgentRow(Base):
    """Row model for ``agent``."""

    __tablename__ = "agent"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_agent"),
        Index("ix_agent_0", "scope_id", "id", unique=True),
        Index("ix_agent_1", "scope_id", "key", unique=True),
        ForeignKeyConstraint(
            ["scope_id", "published_revision_id"],
            ["agent_revision.scope_id", "agent_revision.id"],
            name="fk_agent_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 6}},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="Application allocated stable identity"
    )
    scope_id: Mapped[str] = mapped_column(
        Text(), nullable=False, comment="Trusted host isolation scope"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Creation time",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Last mutation time",
    )
    key: Mapped[str] = mapped_column(Text(), nullable=False, comment="key")
    published_revision_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="published revision id"
    )


class AgentRevisionRow(Base):
    """Row model for ``agent_revision``."""

    __tablename__ = "agent_revision"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_agent_revision"),
        Index("ix_agent_revision_0", "scope_id", "id", unique=True),
        Index("ix_agent_revision_1", "scope_id", "agent_id", "revision", unique=True),
        CheckConstraint("revision >= 0", name="ck_agent_revision_0"),
        ForeignKeyConstraint(
            ["scope_id", "agent_id"],
            ["agent.scope_id", "agent.id"],
            name="fk_agent_revision_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": True, "catalog_columns": 8}},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="Application allocated stable identity"
    )
    scope_id: Mapped[str] = mapped_column(
        Text(), nullable=False, comment="Trusted host isolation scope"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Creation time",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Last mutation time",
    )
    agent_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="agent id")
    revision: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="revision")
    manifest: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="manifest")
    content_hash: Mapped[str] = mapped_column(Text(), nullable=False, comment="content hash")


class CapabilityRow(Base):
    """Row model for ``capability``."""

    __tablename__ = "capability"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_capability"),
        Index("ix_capability_0", "scope_id", "id", unique=True),
        Index("ix_capability_1", "scope_id", "namespace", "key", unique=True),
        CheckConstraint(
            "kind IN ('model', 'tool', 'skill', 'prompt', 'hook', 'knowledge', 'agent')",
            name="ck_capability_0",
        ),
        {"info": {"immutable": False, "catalog_columns": 7}},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="Application allocated stable identity"
    )
    scope_id: Mapped[str] = mapped_column(
        Text(), nullable=False, comment="Trusted host isolation scope"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Creation time",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Last mutation time",
    )
    namespace: Mapped[str] = mapped_column(Text(), nullable=False, comment="namespace")
    key: Mapped[str] = mapped_column(Text(), nullable=False, comment="key")
    kind: Mapped[str] = mapped_column(Text(), nullable=False, comment="kind")


class CapabilityRevisionRow(Base):
    """Row model for ``capability_revision``."""

    __tablename__ = "capability_revision"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_capability_revision"),
        Index("ix_capability_revision_0", "scope_id", "id", unique=True),
        Index("ix_capability_revision_1", "scope_id", "capability_id", "revision", unique=True),
        Index(
            "ix_capability_revision_2",
            "scope_id",
            "capability_id",
            unique=True,
            postgresql_where=text("status = 'active'"),
        ),
        CheckConstraint("revision >= 0", name="ck_capability_revision_0"),
        CheckConstraint(
            "status IN ('draft', 'active', 'superseded', 'revoked')",
            name="ck_capability_revision_1",
        ),
        CheckConstraint("length(content_digest) = 64", name="ck_capability_revision_2"),
        ForeignKeyConstraint(
            ["scope_id", "capability_id"],
            ["capability.scope_id", "capability.id"],
            name="fk_capability_revision_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 9}},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="Application allocated stable identity"
    )
    scope_id: Mapped[str] = mapped_column(
        Text(), nullable=False, comment="Trusted host isolation scope"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Creation time",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Last mutation time",
    )
    capability_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="capability id"
    )
    revision: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="revision")
    content_digest: Mapped[str] = mapped_column(Text(), nullable=False, comment="content digest")
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    manifest: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="manifest")

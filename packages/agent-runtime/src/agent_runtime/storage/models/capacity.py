"""Platform capacity outside the user scope. (DS-P02-01).

Explicit declarative models; ``contracts/storage.json`` is the specification source.
"""

import uuid
from datetime import datetime

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
from sqlalchemy.orm import Mapped, mapped_column

from agent_runtime.storage.base import Base


class ResourcePoolRow(Base):
    """Row model for ``resource_pool``."""

    __tablename__ = "resource_pool"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_resource_pool"),
        Index("ix_resource_pool_0", "scope_id", "id", unique=True),
        Index("ix_resource_pool_1", "scope_id", "pool_key", unique=True),
        CheckConstraint("capacity >= 0", name="ck_resource_pool_0"),
        CheckConstraint("reserved >= 0", name="ck_resource_pool_1"),
        CheckConstraint("version >= 0", name="ck_resource_pool_2"),
        CheckConstraint("reserved <= capacity", name="ck_resource_pool_3"),
        {"info": {"immutable": False, "catalog_columns": 8}},
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
    pool_key: Mapped[str] = mapped_column(Text(), nullable=False, comment="pool key")
    capacity: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="capacity")
    reserved: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="reserved")
    version: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="version")


class ResourcePermitRow(Base):
    """Row model for ``resource_permit``."""

    __tablename__ = "resource_permit"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_resource_permit"),
        Index("ix_resource_permit_0", "scope_id", "id", unique=True),
        Index("ix_resource_permit_1", "scope_id", "pool_id", "holder_key", unique=True),
        Index("ix_resource_permit_2", "scope_id", "pool_id", "status"),
        Index("ix_resource_permit_3", "scope_id", "lease_until"),
        CheckConstraint("capacity_units >= 0", name="ck_resource_permit_0"),
        CheckConstraint(
            "status IN ('reserved', 'released', 'uncertain')",
            name="ck_resource_permit_1",
        ),
        CheckConstraint("capacity_units > 0", name="ck_resource_permit_2"),
        ForeignKeyConstraint(
            ["scope_id", "pool_id"],
            ["resource_pool.scope_id", "resource_pool.id"],
            name="fk_resource_permit_0",
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
    pool_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="pool id")
    holder_key: Mapped[str] = mapped_column(Text(), nullable=False, comment="holder key")
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    capacity_units: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="capacity units"
    )
    lease_until: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="lease until"
    )


class SystemCapacityRow(Base):
    """Row model for ``system_capacity``."""

    __tablename__ = "system_capacity"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint(
            "deployment_id",
            "resource_kind",
            "resource_version",
            "resource_key",
            name="pk_system_capacity",
        ),
        CheckConstraint("resource_version >= 0", name="ck_system_capacity_0"),
        CheckConstraint("capacity >= 0", name="ck_system_capacity_1"),
        CheckConstraint("reserved >= 0", name="ck_system_capacity_2"),
        CheckConstraint("version >= 0", name="ck_system_capacity_3"),
        CheckConstraint(
            "resource_kind IN ('model_slot', 'tool_slot', 'run_slot', 'sandbox_slot', "
            "'egress_slot')",
            name="ck_system_capacity_4",
        ),
        CheckConstraint("reserved <= capacity", name="ck_system_capacity_5"),
        {"info": {"immutable": False, "catalog_columns": 9}},
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
    deployment_id: Mapped[str] = mapped_column(Text(), nullable=False, comment="deployment id")
    resource_kind: Mapped[str] = mapped_column(Text(), nullable=False, comment="resource kind")
    resource_key: Mapped[str] = mapped_column(Text(), nullable=False, comment="resource key")
    resource_version: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="resource version"
    )
    capacity: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="capacity")
    reserved: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="reserved")
    version: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="version")


class SystemCapacityReservationRow(Base):
    """Row model for ``system_capacity_reservation``."""

    __tablename__ = "system_capacity_reservation"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_system_capacity_reservation"),
        Index(
            "ix_system_capacity_reservation_0",
            "deployment_id",
            "resource_kind",
            "resource_version",
            "resource_key",
            "request_id",
            unique=True,
        ),
        Index(
            "ix_system_capacity_reservation_1",
            "deployment_id",
            "resource_kind",
            "resource_version",
            "resource_key",
            "status",
        ),
        Index("ix_system_capacity_reservation_2", "status", "lease_until"),
        CheckConstraint("resource_version >= 0", name="ck_system_capacity_reservation_0"),
        CheckConstraint("amount >= 0", name="ck_system_capacity_reservation_1"),
        CheckConstraint("version >= 0", name="ck_system_capacity_reservation_2"),
        CheckConstraint(
            "resource_kind IN ('model_slot', 'tool_slot', 'run_slot', 'sandbox_slot', "
            "'egress_slot')",
            name="ck_system_capacity_reservation_3",
        ),
        CheckConstraint(
            "status IN ('reserved', 'released', 'expired', 'reclaimed')",
            name="ck_system_capacity_reservation_4",
        ),
        CheckConstraint("amount > 0", name="ck_system_capacity_reservation_5"),
        CheckConstraint(
            "(status = 'reserved') = (released_at IS NULL)",
            name="ck_system_capacity_reservation_6",
        ),
        CheckConstraint(
            "release_reason IN ('settled', 'cancelled', 'expired', 'reclaimed')",
            name="ck_system_capacity_reservation_7",
        ),
        CheckConstraint(
            "release_request_hash IS NULL OR length(release_request_hash) = 64",
            name="ck_system_capacity_reservation_8",
        ),
        CheckConstraint(
            "((status = 'reserved') = (release_request_id IS NULL)) AND ((release_request_id IS "
            "NULL) = (release_reason IS NULL)) AND ((status = 'released') = (release_reason IN "
            "('settled','cancelled'))) AND ((status = 'expired') = (release_reason = 'expired')) "
            "AND ((status = 'reclaimed') = (release_reason = 'reclaimed'))",
            name="ck_system_capacity_reservation_9",
        ),
        ForeignKeyConstraint(
            ["deployment_id", "resource_kind", "resource_version", "resource_key"],
            [
                "system_capacity.deployment_id",
                "system_capacity.resource_kind",
                "system_capacity.resource_version",
                "system_capacity.resource_key",
            ],
            name="fk_system_capacity_reservation_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 17}},
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="Application allocated stable identity"
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
    deployment_id: Mapped[str] = mapped_column(Text(), nullable=False, comment="deployment id")
    resource_kind: Mapped[str] = mapped_column(Text(), nullable=False, comment="resource kind")
    resource_key: Mapped[str] = mapped_column(Text(), nullable=False, comment="resource key")
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    holder_ref: Mapped[str] = mapped_column(Text(), nullable=False, comment="holder ref")
    request_id: Mapped[str] = mapped_column(Text(), nullable=False, comment="request id")
    release_request_id: Mapped[str | None] = mapped_column(
        Text(), nullable=True, comment="release request id"
    )
    release_reason: Mapped[str | None] = mapped_column(
        Text(), nullable=True, comment="release reason"
    )
    release_request_hash: Mapped[str | None] = mapped_column(
        Text(), nullable=True, comment="release request hash"
    )
    resource_version: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="resource version"
    )
    amount: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="amount")
    version: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="version")
    lease_until: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="lease until"
    )
    released_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="released at"
    )

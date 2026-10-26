"""Private credential request ledger and append-only audit. Never stores raw secrets."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
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


class CredentialServiceBindingRow(Base):
    __tablename__ = "credential_service_binding"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_credential_service_binding"),
        Index("ix_credential_service_binding_0", "scope_id", "id", unique=True),
        Index("ix_credential_service_binding_1", "scope_id", "grant_id", unique=True),
        CheckConstraint("peer_sha256 ~ '^[0-9a-f]{64}$'", name="ck_credential_service_binding_0"),
        ForeignKeyConstraint(
            ["scope_id", "grant_id"],
            ["capability_grant.scope_id", "capability_grant.id"],
            name="fk_credential_service_binding_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": True, "catalog_columns": 6}},
    )
    id: Mapped[UUID] = mapped_column(
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
    grant_id: Mapped[UUID] = mapped_column(Uuid(), nullable=False, comment="grant id")
    peer_sha256: Mapped[str] = mapped_column(Text(), nullable=False, comment="peer sha256")


class CredentialRequestRow(Base):
    __tablename__ = "credential_request"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_credential_request"),
        Index("ix_credential_request_0", "scope_id", "id", unique=True),
        Index("ix_credential_request_1", "scope_id", "grant_id", "status"),
        CheckConstraint(
            "status IN ('reserved', 'unknown', 'completed')", name="ck_credential_request_0"
        ),
        CheckConstraint("length(request_hash) = 64", name="ck_credential_request_1"),
        CheckConstraint(
            "(status = 'completed') = (response IS NOT NULL)", name="ck_credential_request_2"
        ),
        CheckConstraint(
            "(status = 'completed') = (finished_at IS NOT NULL)", name="ck_credential_request_3"
        ),
        ForeignKeyConstraint(
            ["scope_id", "grant_id"],
            ["capability_grant.scope_id", "capability_grant.id"],
            name="fk_credential_request_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 9}},
    )
    id: Mapped[UUID] = mapped_column(
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
    grant_id: Mapped[UUID] = mapped_column(Uuid(), nullable=False, comment="grant id")
    request_hash: Mapped[str] = mapped_column(Text(), nullable=False, comment="request hash")
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    response: Mapped[Any | None] = mapped_column(JSONB(), nullable=True, comment="response")
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="finished at"
    )


class CredentialAuditRow(Base):
    __tablename__ = "credential_audit"
    __table_args__ = (
        PrimaryKeyConstraint("id", name="pk_credential_audit"),
        Index("ix_credential_audit_0", "scope_id", "id", unique=True),
        Index("ix_credential_audit_1", "scope_id", "grant_id", "created_at"),
        CheckConstraint(
            "action IN ('issued', 'reserved', 'completed', 'unknown', 'revoked')",
            name="ck_credential_audit_0",
        ),
        ForeignKeyConstraint(
            ["scope_id", "grant_id"],
            ["capability_grant.scope_id", "capability_grant.id"],
            name="fk_credential_audit_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "request_id"],
            ["credential_request.scope_id", "credential_request.id"],
            name="fk_credential_audit_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": True, "catalog_columns": 9}},
    )
    id: Mapped[UUID] = mapped_column(
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
    grant_id: Mapped[UUID] = mapped_column(Uuid(), nullable=False, comment="grant id")
    request_id: Mapped[UUID | None] = mapped_column(Uuid(), nullable=True, comment="request id")
    actor_id: Mapped[str] = mapped_column(Text(), nullable=False, comment="actor id")
    action: Mapped[str] = mapped_column(Text(), nullable=False, comment="action")
    evidence: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="evidence")

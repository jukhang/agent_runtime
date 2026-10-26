"""Host resources are not Runtime artifacts and have an independent migration chain."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    LargeBinary,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class ResourceBase(DeclarativeBase):
    pass


class ResourceRow(ResourceBase):
    __tablename__ = "resource"
    __table_args__ = (
        Index("ix_resource_owner", "scope_id", "owner_actor_id", "session_id", "created_at"),
        CheckConstraint("kind IN ('attachment','knowledge','report')", name="ck_resource_kind"),
        CheckConstraint("version = 1", name="ck_resource_version"),
        CheckConstraint("size_bytes BETWEEN 1 AND 1048576", name="ck_resource_size"),
        CheckConstraint("octet_length(content) = size_bytes", name="ck_resource_content_size"),
        CheckConstraint("digest ~ '^[a-f0-9]{64}$'", name="ck_resource_digest"),
    )
    scope_id: Mapped[str] = mapped_column(Text, primary_key=True, comment="Trusted host scope")
    id: Mapped[UUID] = mapped_column(Uuid, primary_key=True, comment="Idempotent resource identity")
    owner_actor_id: Mapped[str] = mapped_column(Text, comment="Authenticated resource owner")
    session_id: Mapped[UUID] = mapped_column(
        Uuid, comment="Owning Runtime Session; service checked"
    )
    version: Mapped[int] = mapped_column(BigInteger, comment="Immutable resource version")
    kind: Mapped[str] = mapped_column(
        Text, comment="Attachment, explicit knowledge source, or report"
    )
    title: Mapped[str] = mapped_column(Text, comment="Display title, never a filesystem path")
    mime: Mapped[str] = mapped_column(Text, comment="Server-selected safe MIME")
    content: Mapped[bytes] = mapped_column(LargeBinary, comment="Immutable bounded UTF-8 content")
    digest: Mapped[str] = mapped_column(Text, comment="SHA-256 of exact downloadable bytes")
    request_hash: Mapped[str] = mapped_column(Text, comment="Immutable idempotency payload hash")
    size_bytes: Mapped[int] = mapped_column(BigInteger, comment="Exact stored byte length")
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), comment="Revocation time"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), comment="Resource creation time"
    )


class SandboxAllocationRow(ResourceBase):
    __tablename__ = "sandbox_allocation"
    __table_args__ = (
        UniqueConstraint("scope_id", "operation_id", name="uq_sandbox_allocation_operation"),
        Index("ix_sandbox_allocation_cleanup", "deployment_id", "status", "cleanup_until"),
        CheckConstraint(
            "status IN ('reserved','created','bound','cleaning','cleaned')",
            name="ck_sandbox_allocation_status",
        ),
    )
    scope_id: Mapped[str] = mapped_column(
        Text, primary_key=True, comment="Trusted allocation scope"
    )
    id: Mapped[UUID] = mapped_column(
        Uuid, primary_key=True, comment="Immutable allocation identity"
    )
    deployment_id: Mapped[str] = mapped_column(
        Text, comment="Host-controlled remote inventory deployment"
    )
    owner_actor_id: Mapped[str] = mapped_column(Text, comment="Authenticated allocating actor")
    session_id: Mapped[UUID] = mapped_column(Uuid, comment="Original Session identity")
    run_id: Mapped[UUID] = mapped_column(Uuid, comment="Original Run identity")
    operation_id: Mapped[UUID] = mapped_column(Uuid, comment="One remote allocation per operation")
    work_id: Mapped[UUID] = mapped_column(Uuid, comment="Original SQL Work claim")
    claim_owner: Mapped[str] = mapped_column(Text, comment="Original claimed Worker owner")
    run_epoch: Mapped[int] = mapped_column(BigInteger, comment="Original Run fencing epoch")
    work_epoch: Mapped[int] = mapped_column(BigInteger, comment="Original Work fencing epoch")
    invocation_hash: Mapped[str] = mapped_column(Text, comment="Exact allocating invocation digest")
    remote_metadata: Mapped[dict] = mapped_column(
        JSONB, comment="Immutable remote ownership metadata"
    )
    status: Mapped[str] = mapped_column(Text, comment="Allocation and cleanup state")
    sandbox_id: Mapped[str | None] = mapped_column(
        Text, comment="Acknowledged or inventoried remote ID"
    )
    execution_id: Mapped[UUID | None] = mapped_column(
        Uuid, comment="Bound Runtime execution; same transaction"
    )
    cleanup_owner: Mapped[UUID | None] = mapped_column(Uuid, comment="Fenced maintenance owner")
    cleanup_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), comment="Maintenance lease expiry"
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), comment="Latest authorized dispatch time"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), comment="Intent persistence time"
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), comment="Last ledger transition time"
    )

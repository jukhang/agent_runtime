"""Model attempts, context snapshots and budget accounting. (DS-P02-01).

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
    Integer,
    PrimaryKeyConstraint,
    Text,
    Uuid,
    text,
)
from sqlalchemy.dialects.postgresql import BYTEA, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from agent_runtime.storage.base import Base


class ModelAttemptRow(Base):
    """Row model for ``model_attempt``."""

    __tablename__ = "model_attempt"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_model_attempt"),
        Index("ix_model_attempt_0", "scope_id", "id", unique=True),
        Index("ix_model_attempt_1", "scope_id", "run_id", "request_key", unique=True),
        Index("ix_model_attempt_2", "scope_id", "run_id", "started_at"),
        Index("ix_model_attempt_3", "scope_id", "status", "started_at"),
        CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed', 'interrupted', 'cancelled')",
            name="ck_model_attempt_0",
        ),
        CheckConstraint(
            "usage_status IN ('pending', 'complete', 'partial', 'unknown')",
            name="ck_model_attempt_1",
        ),
        CheckConstraint(
            "usage_reason IN ('pending', 'provider_reported', 'provider_not_reported', "
            "'collection_failed', 'estimated')",
            name="ck_model_attempt_2",
        ),
        CheckConstraint(
            "(usage_status = 'pending' AND usage_reason = 'pending') OR (usage_status = "
            "'complete' AND usage_reason = 'provider_reported') OR (usage_status = 'partial' AND "
            "usage_reason IN ('provider_reported','estimated')) OR (usage_status = 'unknown' AND "
            "usage_reason IN ('provider_not_reported','collection_failed','estimated'))",
            name="ck_model_attempt_3",
        ),
        CheckConstraint(
            "(status IN ('completed','failed','interrupted','cancelled')) = (finished_at IS NOT "
            "NULL)",
            name="ck_model_attempt_4",
        ),
        ForeignKeyConstraint(
            ["scope_id", "run_id"],
            ["run.scope_id", "run.id"],
            name="fk_model_attempt_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "context_snapshot_id"],
            ["context_snapshot.scope_id", "context_snapshot.id"],
            name="fk_model_attempt_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 18}},
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
    run_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="run id")
    context_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="context snapshot id"
    )
    request_key: Mapped[str] = mapped_column(Text(), nullable=False, comment="request key")
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    input_ref: Mapped[str] = mapped_column(Text(), nullable=False, comment="input ref")
    usage_status: Mapped[str] = mapped_column(Text(), nullable=False, comment="usage status")
    usage_reason: Mapped[str] = mapped_column(Text(), nullable=False, comment="usage reason")
    provider_request_id: Mapped[str | None] = mapped_column(
        Text(), nullable=True, comment="provider request id"
    )
    output_ref: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="output ref")
    finish_reason: Mapped[str | None] = mapped_column(
        Text(), nullable=True, comment="finish reason"
    )
    usage: Mapped[Any | None] = mapped_column(JSONB(), nullable=True, comment="usage")
    failure: Mapped[Any | None] = mapped_column(JSONB(), nullable=True, comment="failure")
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="started at"
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="finished at"
    )


class BudgetAllocationRow(Base):
    """Row model for ``budget_allocation``."""

    __tablename__ = "budget_allocation"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_budget_allocation"),
        Index("ix_budget_allocation_0", "scope_id", "id", unique=True),
        Index("ix_budget_allocation_1", "scope_id", "root_run_id"),
        CheckConstraint("unsettled_count >= 0", name="ck_budget_allocation_0"),
        CheckConstraint("version >= 0", name="ck_budget_allocation_1"),
        CheckConstraint(
            "usage_status IN ('complete', 'partial', 'unknown')",
            name="ck_budget_allocation_2",
        ),
        ForeignKeyConstraint(
            ["scope_id", "root_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_budget_allocation_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "parent_id"],
            ["budget_allocation.scope_id", "budget_allocation.id"],
            name="fk_budget_allocation_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 13}},
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
    root_run_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="root run id")
    parent_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(), nullable=True, comment="parent id")
    limits: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="limits")
    reserved: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="reserved")
    unknown_reserved: Mapped[Any] = mapped_column(
        JSONB(), nullable=False, comment="unknown reserved"
    )
    settled: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="settled")
    usage_status: Mapped[str] = mapped_column(Text(), nullable=False, comment="usage status")
    unsettled_count: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="unsettled count"
    )
    version: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="version")


class BudgetEntryRow(Base):
    """Row model for ``budget_entry``."""

    __tablename__ = "budget_entry"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_budget_entry"),
        Index("ix_budget_entry_0", "scope_id", "id", unique=True),
        Index(
            "ix_budget_entry_1", "scope_id", "allocation_id", "operation_key", "kind", unique=True
        ),
        CheckConstraint(
            "kind IN ('reserve', 'settle', 'release', 'adjust')",
            name="ck_budget_entry_0",
        ),
        CheckConstraint(
            "usage_status IN ('complete', 'partial', 'unknown')",
            name="ck_budget_entry_1",
        ),
        CheckConstraint(
            "usage_reason IN ('pending', 'provider_reported', 'provider_not_reported', "
            "'collection_failed', 'estimated')",
            name="ck_budget_entry_2",
        ),
        CheckConstraint(
            "((kind = 'settle') = (usage_reason IS NOT NULL)) AND (usage_reason IS NULL OR "
            "((usage_status = 'complete' AND usage_reason = 'provider_reported') OR "
            "(usage_status = 'partial' AND usage_reason IN ('provider_reported','estimated')) OR "
            "(usage_status = 'unknown' AND usage_reason IN "
            "('provider_not_reported','collection_failed','estimated'))))",
            name="ck_budget_entry_3",
        ),
        ForeignKeyConstraint(
            ["scope_id", "allocation_id"],
            ["budget_allocation.scope_id", "budget_allocation.id"],
            name="fk_budget_entry_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": True, "catalog_columns": 11}},
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
    allocation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="allocation id"
    )
    operation_key: Mapped[str] = mapped_column(Text(), nullable=False, comment="operation key")
    kind: Mapped[str] = mapped_column(Text(), nullable=False, comment="kind")
    usage_status: Mapped[str] = mapped_column(Text(), nullable=False, comment="usage status")
    usage_reason: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="usage reason")
    quantities: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="quantities")
    reconciliation_ref: Mapped[str | None] = mapped_column(
        Text(), nullable=True, comment="reconciliation ref"
    )


class ContextSnapshotRow(Base):
    """Row model for ``context_snapshot``."""

    __tablename__ = "context_snapshot"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_context_snapshot"),
        Index("ix_context_snapshot_0", "scope_id", "id", unique=True),
        Index("ix_context_snapshot_1", "scope_id", "run_id", "created_at"),
        CheckConstraint("auth_revision >= 0", name="ck_context_snapshot_0"),
        CheckConstraint("context_version >= 0", name="ck_context_snapshot_1"),
        CheckConstraint("estimated_input_tokens >= 0", name="ck_context_snapshot_2"),
        CheckConstraint("schema_revision > 0", name="ck_context_snapshot_3"),
        ForeignKeyConstraint(
            ["scope_id", "session_id"],
            ["session.scope_id", "session.id"],
            name="fk_context_snapshot_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "run_id"],
            ["run.scope_id", "run.id"],
            name="fk_context_snapshot_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": True, "catalog_columns": 19}},
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
    session_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="session id")
    run_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(), nullable=True, comment="run id")
    auth_revision: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="auth revision"
    )
    context_version: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="context version"
    )
    estimated_input_tokens: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="estimated input tokens"
    )
    schema_revision: Mapped[int] = mapped_column(
        Integer(), nullable=False, comment="schema revision"
    )
    task_type: Mapped[str] = mapped_column(Text(), nullable=False, comment="task type")
    payload_ref: Mapped[str] = mapped_column(Text(), nullable=False, comment="payload ref")
    content_hash: Mapped[str] = mapped_column(Text(), nullable=False, comment="content hash")
    source_refs: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="source refs")
    degradation: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="degradation")
    model_ref: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="model ref")
    prompt_ref: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="prompt ref")
    tool_refs: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="tool refs")
    budget: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="budget")


class ContextBlobRow(Base):
    """Immutable, scope-isolated shared context bytes."""

    __tablename__ = "context_blob"
    __table_args__ = (
        PrimaryKeyConstraint("scope_id", "blob_key", name="pk_context_blob"),
        CheckConstraint("size_bytes >= 0", name="ck_context_blob_0"),
        CheckConstraint("length(digest) = 64", name="ck_context_blob_1"),
        CheckConstraint("size_bytes = octet_length(content)", name="ck_context_blob_2"),
        CheckConstraint("size_bytes <= 16777216", name="ck_context_blob_3"),
        {"info": {"immutable": True, "catalog_columns": 7}},
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
    blob_key: Mapped[str] = mapped_column(Text(), nullable=False, comment="blob key")
    digest: Mapped[str] = mapped_column(Text(), nullable=False, comment="digest")
    size_bytes: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="size bytes")
    content: Mapped[bytes] = mapped_column(BYTEA(), nullable=False, comment="content")

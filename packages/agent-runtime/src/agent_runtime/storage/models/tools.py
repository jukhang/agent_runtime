"""Tool calls, approvals, artifacts and workspaces. (DS-P02-01).

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


class ToolCallRow(Base):
    """Row model for ``tool_call``."""

    __tablename__ = "tool_call"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_tool_call"),
        Index("ix_tool_call_0", "scope_id", "id", unique=True),
        Index(
            "ix_tool_call_1",
            "scope_id",
            "attempt_id",
            "provider_call_id",
            unique=True,
            postgresql_where=text("attempt_id IS NOT NULL"),
        ),
        Index("ix_tool_call_2", "scope_id", "run_id", "status"),
        CheckConstraint(
            "status IN ('pending', 'running', 'waiting_approval', 'completed', 'failed', "
            "'cancelled', 'not_executed')",
            name="ck_tool_call_0",
        ),
        CheckConstraint("(attempt_id IS NULL) = (provider_call_id IS NULL)", name="ck_tool_call_1"),
        ForeignKeyConstraint(
            ["scope_id", "run_id"],
            ["run.scope_id", "run.id"],
            name="fk_tool_call_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "attempt_id"],
            ["model_attempt.scope_id", "model_attempt.id"],
            name="fk_tool_call_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "operation_id"],
            ["operation.scope_id", "operation.id"],
            name="fk_tool_call_2",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 15}},
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
    attempt_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="attempt id"
    )
    operation_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="operation id"
    )
    provider_call_id: Mapped[str | None] = mapped_column(
        Text(), nullable=True, comment="provider call id"
    )
    result_ref: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="result ref")
    spec_revision: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="spec revision")
    input_hash: Mapped[str] = mapped_column(Text(), nullable=False, comment="input hash")
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="started at"
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="finished at"
    )
    failure: Mapped[Any | None] = mapped_column(JSONB(), nullable=True, comment="failure")


class ApprovalRow(Base):
    """Row model for ``approval``."""

    __tablename__ = "approval"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_approval"),
        Index("ix_approval_0", "scope_id", "id", unique=True),
        Index("ix_approval_1", "scope_id", "request_key", unique=True),
        Index("ix_approval_2", "scope_id", "status", "expires_at"),
        CheckConstraint("revision >= 0", name="ck_approval_0"),
        CheckConstraint("auth_revision >= 0", name="ck_approval_1"),
        CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'expired', 'cancelled')",
            name="ck_approval_2",
        ),
        CheckConstraint("decision IN ('approved', 'rejected', 'cancelled')", name="ck_approval_3"),
        ForeignKeyConstraint(
            ["scope_id", "run_id"],
            ["run.scope_id", "run.id"],
            name="fk_approval_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "operation_id"],
            ["operation.scope_id", "operation.id"],
            name="fk_approval_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 19}},
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
    operation_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="operation id"
    )
    actor_id: Mapped[str] = mapped_column(Text(), nullable=False, comment="actor id")
    request_key: Mapped[str] = mapped_column(Text(), nullable=False, comment="request key")
    input_hash: Mapped[str] = mapped_column(Text(), nullable=False, comment="input hash")
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    decision: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="decision")
    decision_actor_id: Mapped[str | None] = mapped_column(
        Text(), nullable=True, comment="decision actor id"
    )
    revision: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="revision")
    auth_revision: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="auth revision"
    )
    policy_ref: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="policy ref")
    tool_revision: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="tool revision")
    resource_refs: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="resource refs")
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="expires at"
    )
    approved_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="approved at"
    )


class ArtifactRow(Base):
    """Row model for ``artifact``."""

    __tablename__ = "artifact"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_artifact"),
        Index("ix_artifact_0", "scope_id", "id", unique=True),
        Index("ix_artifact_1", "scope_id", "task_id", "updated_at"),
        ForeignKeyConstraint(
            ["scope_id", "task_id"],
            ["task.scope_id", "task.id"],
            name="fk_artifact_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "latest_revision_id"],
            ["artifact_revision.scope_id", "artifact_revision.id"],
            name="fk_artifact_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
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
    owner_actor_id: Mapped[str] = mapped_column(Text(), nullable=False, comment="owner actor id")
    task_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="task id")
    latest_revision_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="latest revision id"
    )


class ArtifactRevisionRow(Base):
    """Row model for ``artifact_revision``."""

    __tablename__ = "artifact_revision"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_artifact_revision"),
        Index("ix_artifact_revision_0", "scope_id", "id", unique=True),
        Index("ix_artifact_revision_1", "scope_id", "artifact_id", "revision", unique=True),
        CheckConstraint("revision >= 0", name="ck_artifact_revision_0"),
        ForeignKeyConstraint(
            ["scope_id", "artifact_id"],
            ["artifact.scope_id", "artifact.id"],
            name="fk_artifact_revision_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "source_execution_id"],
            ["execution.scope_id", "execution.id"],
            name="fk_artifact_revision_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": True, "catalog_columns": 10}},
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
    artifact_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="artifact id")
    source_execution_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="source execution id"
    )
    revision: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="revision")
    blob_ref: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="blob ref")
    input_refs: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="input refs")
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")


class WorkspaceRow(Base):
    """Row model for ``workspace``."""

    __tablename__ = "workspace"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_workspace"),
        Index("ix_workspace_0", "scope_id", "id", unique=True),
        Index("ix_workspace_1", "scope_id", "task_id"),
        CheckConstraint("generation >= 0", name="ck_workspace_0"),
        CheckConstraint("revision >= 0", name="ck_workspace_1"),
        ForeignKeyConstraint(
            ["scope_id", "task_id"],
            ["task.scope_id", "task.id"],
            name="fk_workspace_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "owner_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_workspace_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "latest_checkpoint_id"],
            ["workspace_checkpoint.scope_id", "workspace_checkpoint.id"],
            name="fk_workspace_2",
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
    task_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="task id")
    owner_run_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="owner run id")
    latest_checkpoint_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="latest checkpoint id"
    )
    generation: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="generation")
    revision: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="revision")


class WorkspaceCheckpointRow(Base):
    """Row model for ``workspace_checkpoint``."""

    __tablename__ = "workspace_checkpoint"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_workspace_checkpoint"),
        Index("ix_workspace_checkpoint_0", "scope_id", "id", unique=True),
        Index("ix_workspace_checkpoint_1", "scope_id", "workspace_id", "revision", unique=True),
        CheckConstraint("generation >= 0", name="ck_workspace_checkpoint_0"),
        CheckConstraint("revision >= 0", name="ck_workspace_checkpoint_1"),
        CheckConstraint("total_bytes >= 0", name="ck_workspace_checkpoint_2"),
        CheckConstraint(
            "state IN ('preparing', 'ready', 'failed')",
            name="ck_workspace_checkpoint_3",
        ),
        ForeignKeyConstraint(
            ["scope_id", "workspace_id"],
            ["workspace.scope_id", "workspace.id"],
            name="fk_workspace_checkpoint_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "owner_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_workspace_checkpoint_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 12}},
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
    workspace_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="workspace id")
    owner_run_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="owner run id")
    generation: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="generation")
    revision: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="revision")
    total_bytes: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="total bytes")
    state: Mapped[str] = mapped_column(Text(), nullable=False, comment="state")
    manifest_ref: Mapped[str] = mapped_column(Text(), nullable=False, comment="manifest ref")
    content_hash: Mapped[str] = mapped_column(Text(), nullable=False, comment="content hash")


class CapabilityGrantRow(Base):
    """Row model for ``capability_grant``."""

    __tablename__ = "capability_grant"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_capability_grant"),
        Index("ix_capability_grant_0", "scope_id", "id", unique=True),
        Index(
            "ix_capability_grant_1",
            "scope_id",
            "token_hash",
            unique=True,
            postgresql_where=text("token_hash IS NOT NULL"),
        ),
        Index("ix_capability_grant_2", "scope_id", "run_id", "expires_at"),
        CheckConstraint("execution_generation >= 0", name="ck_capability_grant_0"),
        CheckConstraint("revision >= 0", name="ck_capability_grant_1"),
        CheckConstraint("expires_at > issued_at", name="ck_capability_grant_2"),
        CheckConstraint(
            "(execution_id IS NULL) = (execution_generation IS NULL)",
            name="ck_capability_grant_3",
        ),
        ForeignKeyConstraint(
            ["scope_id", "run_id"],
            ["run.scope_id", "run.id"],
            name="fk_capability_grant_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "execution_id"],
            ["execution.scope_id", "execution.id"],
            name="fk_capability_grant_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 14}},
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
    execution_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="execution id"
    )
    execution_generation: Mapped[int | None] = mapped_column(
        BigInteger(), nullable=True, comment="execution generation"
    )
    actor_id: Mapped[str] = mapped_column(Text(), nullable=False, comment="actor id")
    revision: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="revision")
    token_hash: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="token hash")
    allowed_scope: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="allowed scope")
    issued_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="issued at"
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="expires at"
    )
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="revoked at"
    )

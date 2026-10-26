"""Work claiming, checkpoints, execution supervision and plans. (DS-P02-01).

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
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from agent_runtime.storage.base import Base


class WorkItemRow(Base):
    """Row model for ``work_item``."""

    __tablename__ = "work_item"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_work_item"),
        Index("ix_work_item_0", "scope_id", "id", unique=True),
        Index("ix_work_item_1", "scope_id", "dedup_key", unique=True),
        Index(
            "ix_work_item_2",
            "scope_id",
            "run_id",
            unique=True,
            postgresql_where=text("kind = 'advance_run' AND status IN ('pending','leased')"),
        ),
        Index(
            "ix_work_item_3",
            "scope_id",
            "available_at",
            "priority",
            "id",
            postgresql_where=text("status = 'pending'"),
        ),
        Index(
            "ix_work_item_4", "scope_id", "lease_until", postgresql_where=text("status = 'leased'")
        ),
        Index(
            "ix_work_item_5",
            "scope_id",
            "handler_namespace",
            "handler_key",
            "handler_revision",
            "status",
            postgresql_where=text("kind = 'domain_job'"),
        ),
        CheckConstraint("handler_revision >= 0", name="ck_work_item_0"),
        CheckConstraint("lease_epoch >= 0", name="ck_work_item_1"),
        CheckConstraint("attempt_count >= 0", name="ck_work_item_2"),
        CheckConstraint(
            "kind IN ('advance_run', 'reconcile_execution', 'migrate_checkpoint', "
            "'rebuild_projection', 'recover_scope', 'collect_garbage', 'evaluate', 'domain_job')",
            name="ck_work_item_3",
        ),
        CheckConstraint(
            "status IN ('pending', 'leased', 'completed', 'failed', 'cancelled')",
            name="ck_work_item_4",
        ),
        CheckConstraint(
            "wake_reason IN ('start', 'tool_result', 'message', 'resume', 'handoff', 'recovery', "
            "'control')",
            name="ck_work_item_5",
        ),
        CheckConstraint(
            "(kind = 'advance_run' AND run_id IS NOT NULL AND wake_reason IS NOT NULL) OR (kind "
            "<> 'advance_run' AND wake_reason IS NULL)",
            name="ck_work_item_6",
        ),
        CheckConstraint(
            "(kind = 'domain_job') = (handler_namespace IS NOT NULL AND handler_key IS NOT NULL "
            "AND handler_revision IS NOT NULL)",
            name="ck_work_item_7",
        ),
        CheckConstraint(
            "(status = 'leased' AND owner_id IS NOT NULL AND lease_until IS NOT NULL) OR (status "
            "<> 'leased' AND owner_id IS NULL AND lease_until IS NULL)",
            name="ck_work_item_8",
        ),
        ForeignKeyConstraint(
            ["scope_id", "run_id"],
            ["run.scope_id", "run.id"],
            name="fk_work_item_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 22}},
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
    run_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(), nullable=True, comment="run id")
    subject_ref: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="subject ref")
    kind: Mapped[str] = mapped_column(Text(), nullable=False, comment="kind")
    dedup_key: Mapped[str] = mapped_column(Text(), nullable=False, comment="dedup key")
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    payload_ref: Mapped[str] = mapped_column(Text(), nullable=False, comment="payload ref")
    handler_namespace: Mapped[str | None] = mapped_column(
        Text(), nullable=True, comment="handler namespace"
    )
    handler_key: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="handler key")
    handler_revision: Mapped[int | None] = mapped_column(
        BigInteger(), nullable=True, comment="handler revision"
    )
    wake_reason: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="wake reason")
    owner_id: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="owner id")
    priority: Mapped[int] = mapped_column(Integer(), nullable=False, comment="priority")
    lease_epoch: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="lease epoch")
    attempt_count: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="attempt count"
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="available at"
    )
    deadline_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="deadline at"
    )
    lease_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="lease until"
    )
    failure: Mapped[Any | None] = mapped_column(JSONB(), nullable=True, comment="failure")


class CheckpointRow(Base):
    """Row model for ``checkpoint``."""

    __tablename__ = "checkpoint"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_checkpoint"),
        Index("ix_checkpoint_0", "scope_id", "id", unique=True),
        Index(
            "ix_checkpoint_1",
            "scope_id",
            "run_id",
            "state_version",
            "representation_revision",
            unique=True,
        ),
        CheckConstraint("state_version >= 0", name="ck_checkpoint_0"),
        CheckConstraint("representation_revision >= 0", name="ck_checkpoint_1"),
        CheckConstraint("event_seq >= 0", name="ck_checkpoint_2"),
        CheckConstraint("schema_revision > 0", name="ck_checkpoint_3"),
        ForeignKeyConstraint(
            ["scope_id", "run_id"],
            ["run.scope_id", "run.id"],
            name="fk_checkpoint_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "source_checkpoint_id"],
            ["checkpoint.scope_id", "checkpoint.id"],
            name="fk_checkpoint_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": True, "catalog_columns": 13}},
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
    source_checkpoint_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="source checkpoint id"
    )
    state_version: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="state version"
    )
    representation_revision: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="representation revision"
    )
    event_seq: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="event seq")
    schema_revision: Mapped[int] = mapped_column(
        Integer(), nullable=False, comment="schema revision"
    )
    runtime_build: Mapped[str] = mapped_column(Text(), nullable=False, comment="runtime build")
    state_ref: Mapped[str] = mapped_column(Text(), nullable=False, comment="state ref")
    content_hash: Mapped[str] = mapped_column(Text(), nullable=False, comment="content hash")


class OperationRow(Base):
    """Row model for ``operation``."""

    __tablename__ = "operation"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_operation"),
        Index("ix_operation_0", "scope_id", "id", unique=True),
        Index("ix_operation_1", "scope_id", "task_id", "idempotency_key", unique=True),
        Index("ix_operation_2", "scope_id", "status", "updated_at"),
        CheckConstraint(
            "status IN ('prepared', 'dispatching', 'committed', 'failed_safe', 'unknown')",
            name="ck_operation_0",
        ),
        CheckConstraint("status <> 'committed' OR result_ref IS NOT NULL", name="ck_operation_1"),
        ForeignKeyConstraint(
            ["scope_id", "task_id"],
            ["task.scope_id", "task.id"],
            name="fk_operation_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 10}},
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
    idempotency_key: Mapped[str] = mapped_column(Text(), nullable=False, comment="idempotency key")
    input_hash: Mapped[str] = mapped_column(Text(), nullable=False, comment="input hash")
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    result_ref: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="result ref")
    failure: Mapped[Any | None] = mapped_column(JSONB(), nullable=True, comment="failure")


class ExecutionRow(Base):
    """Row model for ``execution``."""

    __tablename__ = "execution"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_execution"),
        Index("ix_execution_0", "scope_id", "id", unique=True),
        Index("ix_execution_1", "scope_id", "remote_request_key", unique=True),
        Index(
            "ix_execution_2",
            "scope_id",
            "sandbox_id",
            "remote_execution_id",
            unique=True,
            postgresql_where=text("sandbox_id IS NOT NULL AND remote_execution_id IS NOT NULL"),
        ),
        Index("ix_execution_3", "scope_id", "status", "deadline_at"),
        Index("ix_execution_4", "scope_id", "run_id", "generation"),
        Index("ix_execution_5", "scope_id", "cleanup_status"),
        CheckConstraint("generation >= 0", name="ck_execution_0"),
        CheckConstraint("reconciliation_round >= 0", name="ck_execution_1"),
        CheckConstraint(
            "status IN ('pending', 'starting', 'running', 'cancelling', 'completed', 'failed', "
            "'cancelled', 'unknown')",
            name="ck_execution_2",
        ),
        CheckConstraint(
            "cleanup_status IN ('pending', 'running', 'completed', 'failed')",
            name="ck_execution_3",
        ),
        ForeignKeyConstraint(
            ["scope_id", "operation_id"],
            ["operation.scope_id", "operation.id"],
            name="fk_execution_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "run_id"],
            ["run.scope_id", "run.id"],
            name="fk_execution_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "supervisor_work_id"],
            ["work_item.scope_id", "work_item.id"],
            name="fk_execution_2",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 17}},
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
    operation_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="operation id")
    run_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="run id")
    supervisor_work_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="supervisor work id"
    )
    remote_request_key: Mapped[str] = mapped_column(
        Text(), nullable=False, comment="remote request key"
    )
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    log_cursor: Mapped[str] = mapped_column(Text(), nullable=False, comment="log cursor")
    cleanup_status: Mapped[str] = mapped_column(Text(), nullable=False, comment="cleanup status")
    sandbox_id: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="sandbox id")
    remote_execution_id: Mapped[str | None] = mapped_column(
        Text(), nullable=True, comment="remote execution id"
    )
    generation: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="generation")
    reconciliation_round: Mapped[int] = mapped_column(
        BigInteger(),
        nullable=False,
        server_default=text("0"),
        comment="Supervision round used by the reconcile_execution dedup key",
    )
    deadline_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="deadline at"
    )
    failure: Mapped[Any | None] = mapped_column(JSONB(), nullable=True, comment="failure")


class TaskRow(Base):
    """Row model for ``task``."""

    __tablename__ = "task"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_task"),
        Index("ix_task_0", "scope_id", "id", unique=True),
        Index("ix_task_1", "scope_id", "root_run_id"),
        ForeignKeyConstraint(
            ["scope_id", "root_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_task_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "parent_task_id"],
            ["task.scope_id", "task.id"],
            name="fk_task_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "current_goal_revision_id"],
            ["goal_revision.scope_id", "goal_revision.id"],
            name="fk_task_2",
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
    root_run_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="root run id")
    parent_task_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="parent task id"
    )
    current_goal_revision_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="current goal revision id"
    )
    acceptance_spec_ref: Mapped[str] = mapped_column(
        Text(), nullable=False, comment="acceptance spec ref"
    )
    deadline_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="deadline at"
    )


class GoalRevisionRow(Base):
    """Row model for ``goal_revision``."""

    __tablename__ = "goal_revision"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_goal_revision"),
        Index("ix_goal_revision_0", "scope_id", "id", unique=True),
        Index("ix_goal_revision_1", "scope_id", "task_id", "revision", unique=True),
        CheckConstraint("revision >= 0", name="ck_goal_revision_0"),
        ForeignKeyConstraint(
            ["scope_id", "task_id"],
            ["task.scope_id", "task.id"],
            name="fk_goal_revision_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": True, "catalog_columns": 9}},
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
    revision: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="revision")
    objective: Mapped[str] = mapped_column(Text(), nullable=False, comment="objective")
    constraints: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="constraints")
    focus_refs: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="focus refs")


class PlanRow(Base):
    """Row model for ``plan``."""

    __tablename__ = "plan"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_plan"),
        Index("ix_plan_0", "scope_id", "id", unique=True),
        Index("ix_plan_1", "scope_id", "task_id", unique=True),
        CheckConstraint("current_revision >= 0", name="ck_plan_0"),
        ForeignKeyConstraint(
            ["scope_id", "task_id"],
            ["task.scope_id", "task.id"],
            name="fk_plan_0",
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
    task_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="task id")
    current_revision: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="current revision"
    )


class PlanRevisionRow(Base):
    """Row model for ``plan_revision``."""

    __tablename__ = "plan_revision"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_plan_revision"),
        Index("ix_plan_revision_0", "scope_id", "id", unique=True),
        Index("ix_plan_revision_1", "scope_id", "plan_id", "revision", unique=True),
        CheckConstraint("revision >= 0", name="ck_plan_revision_0"),
        ForeignKeyConstraint(
            ["scope_id", "plan_id"],
            ["plan.scope_id", "plan.id"],
            name="fk_plan_revision_0",
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
    plan_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="plan id")
    revision: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="revision")
    steps: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="steps")
    public_reason: Mapped[str] = mapped_column(Text(), nullable=False, comment="public reason")


class TaskDependencyRow(Base):
    """Row model for ``task_dependency``."""

    __tablename__ = "task_dependency"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint(
            "scope_id", "task_id", "depends_on_task_id", name="pk_task_dependency"
        ),
        CheckConstraint("task_id <> depends_on_task_id", name="ck_task_dependency_0"),
        ForeignKeyConstraint(
            ["scope_id", "task_id"],
            ["task.scope_id", "task.id"],
            name="fk_task_dependency_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "depends_on_task_id"],
            ["task.scope_id", "task.id"],
            name="fk_task_dependency_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 5}},
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
    depends_on_task_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="depends on task id"
    )


class HandoffRow(Base):
    """Row model for ``handoff``."""

    __tablename__ = "handoff"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_handoff"),
        Index("ix_handoff_0", "scope_id", "id", unique=True),
        Index(
            "ix_handoff_1",
            "scope_id",
            "session_id",
            unique=True,
            postgresql_where=text("status IN ('preparing','accepted')"),
        ),
        CheckConstraint("expected_slot_version >= 0", name="ck_handoff_0"),
        CheckConstraint(
            "status IN ('preparing', 'accepted', 'committed', 'rejected', 'expired')",
            name="ck_handoff_1",
        ),
        CheckConstraint("source_run_id <> target_run_id", name="ck_handoff_2"),
        ForeignKeyConstraint(
            ["scope_id", "session_id"],
            ["session.scope_id", "session.id"],
            name="fk_handoff_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "source_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_handoff_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "target_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_handoff_2",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 11}},
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
    source_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="source run id"
    )
    target_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="target run id"
    )
    expected_slot_version: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="expected slot version"
    )
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    manifest_ref: Mapped[str] = mapped_column(Text(), nullable=False, comment="manifest ref")
    deadline_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="deadline at"
    )

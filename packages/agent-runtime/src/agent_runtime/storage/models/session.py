"""Sessions, turns, runs and their timeline projections. (DS-P02-01).

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


class SessionRow(Base):
    """Row model for ``session``."""

    __tablename__ = "session"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_session"),
        Index("ix_session_0", "scope_id", "id", unique=True),
        Index("ix_session_1", "scope_id", "owner_actor_id", "updated_at"),
        CheckConstraint("event_seq >= 0", name="ck_session_0"),
        CheckConstraint("auth_revision >= 0", name="ck_session_1"),
        CheckConstraint(
            "title_source IN ('none', 'generated', 'document', 'manual')",
            name="ck_session_2",
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
    owner_actor_id: Mapped[str] = mapped_column(Text(), nullable=False, comment="owner actor id")
    title_source: Mapped[str] = mapped_column(Text(), nullable=False, comment="title source")
    title: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="title")
    event_seq: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="event seq")
    auth_revision: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="auth revision"
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="deleted at"
    )


class TurnRow(Base):
    """Row model for ``turn``."""

    __tablename__ = "turn"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_turn"),
        Index("ix_turn_0", "scope_id", "id", unique=True),
        Index("ix_turn_1", "scope_id", "session_id", "request_key", unique=True),
        Index("ix_turn_2", "scope_id", "session_id", "created_at"),
        ForeignKeyConstraint(
            ["scope_id", "session_id"],
            ["session.scope_id", "session.id"],
            name="fk_turn_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "primary_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_turn_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "outcome_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_turn_2",
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
    session_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="session id")
    request_key: Mapped[str] = mapped_column(Text(), nullable=False, comment="request key")
    request_hash: Mapped[str] = mapped_column(Text(), nullable=False, comment="request hash")
    input_ref: Mapped[str] = mapped_column(Text(), nullable=False, comment="input ref")
    primary_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="primary run id"
    )
    outcome_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="outcome run id"
    )
    outcome: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="outcome")
    failure: Mapped[Any | None] = mapped_column(JSONB(), nullable=True, comment="failure")


class RunRow(Base):
    """Row model for ``run``."""

    __tablename__ = "run"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_run"),
        Index("ix_run_0", "scope_id", "id", unique=True),
        Index("ix_run_1", "scope_id", "status", "lease_until"),
        Index("ix_run_2", "scope_id", "root_run_id"),
        Index("ix_run_3", "scope_id", "turn_id"),
        CheckConstraint("state_version >= 0", name="ck_run_0"),
        CheckConstraint("owner_epoch >= 0", name="ck_run_1"),
        CheckConstraint("cancel_version >= 0", name="ck_run_2"),
        CheckConstraint("wake_generation >= 0", name="ck_run_3"),
        CheckConstraint("pending_input_version >= 0", name="ck_run_4"),
        CheckConstraint("applied_input_version >= 0", name="ck_run_5"),
        CheckConstraint("authority_revision >= 0", name="ck_run_6"),
        CheckConstraint(
            "status IN ('queued', 'running', 'waiting_tool', 'waiting_user', 'paused', "
            "'cancelling', 'completed', 'failed', 'cancelled')",
            name="ck_run_7",
        ),
        CheckConstraint("desired_control IN ('pause', 'cancel')", name="ck_run_8"),
        CheckConstraint(
            "phase IN ('queued', 'context', 'model', 'tool', 'waiting', 'paused', 'cancelling', "
            "'terminal')",
            name="ck_run_9",
        ),
        CheckConstraint("(owner_id IS NULL) = (lease_until IS NULL)", name="ck_run_10"),
        CheckConstraint(
            "(desired_control IS NULL) = (control_command_id IS NULL)",
            name="ck_run_11",
        ),
        CheckConstraint("(desired_control = 'cancel') = (status = 'cancelling')", name="ck_run_12"),
        CheckConstraint(
            "desired_control IS NULL OR status IN ('queued', 'running', 'waiting_tool', "
            "'waiting_user', 'cancelling')",
            name="ck_run_13",
        ),
        CheckConstraint("applied_input_version <= pending_input_version", name="ck_run_14"),
        CheckConstraint(
            "(status = 'completed' AND outcome IS NOT NULL AND outcome IN ('success','partial')) "
            "OR (status <> 'completed' AND outcome IS NULL)",
            name="ck_run_15",
        ),
        CheckConstraint("(status = 'failed') = (failure IS NOT NULL)", name="ck_run_16"),
        ForeignKeyConstraint(
            ["scope_id", "session_id"],
            ["session.scope_id", "session.id"],
            name="fk_run_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "turn_id"],
            ["turn.scope_id", "turn.id"],
            name="fk_run_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "root_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_run_2",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "parent_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_run_3",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "agent_revision_id"],
            ["agent_revision.scope_id", "agent_revision.id"],
            name="fk_run_4",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "task_id"],
            ["task.scope_id", "task.id"],
            name="fk_run_5",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "checkpoint_id"],
            ["checkpoint.scope_id", "checkpoint.id"],
            name="fk_run_6",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "control_command_id"],
            ["command.scope_id", "command.id"],
            name="fk_run_7",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 30}},
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
    turn_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="turn id")
    root_run_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="root run id")
    agent_revision_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="agent revision id"
    )
    task_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="task id")
    parent_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="parent run id"
    )
    checkpoint_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="checkpoint id"
    )
    control_command_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="control command id"
    )
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    phase: Mapped[str] = mapped_column(Text(), nullable=False, comment="phase")
    outcome: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="outcome")
    owner_id: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="owner id")
    desired_control: Mapped[str | None] = mapped_column(
        Text(), nullable=True, comment="desired control"
    )
    pause_reason: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="pause reason")
    failure: Mapped[Any | None] = mapped_column(JSONB(), nullable=True, comment="failure")
    waiting_spec: Mapped[Any | None] = mapped_column(JSONB(), nullable=True, comment="waiting spec")
    state_version: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="state version"
    )
    owner_epoch: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="owner epoch")
    cancel_version: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="cancel version"
    )
    wake_generation: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="wake generation"
    )
    pending_input_version: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="pending input version"
    )
    applied_input_version: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="applied input version"
    )
    authority_revision: Mapped[int] = mapped_column(
        BigInteger(),
        nullable=False,
        server_default=text("0"),
        comment="Per-run authority fence revision; grant writers bump it under the run row",
    )
    lease_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="lease until"
    )
    phase_started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="phase started at"
    )
    deadline_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="deadline at"
    )


class SessionSlotRow(Base):
    """Row model for ``session_slot``."""

    __tablename__ = "session_slot"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("scope_id", "session_id", name="pk_session_slot"),
        CheckConstraint("slot_version >= 0", name="ck_session_slot_0"),
        ForeignKeyConstraint(
            ["scope_id", "session_id"],
            ["session.scope_id", "session.id"],
            name="fk_session_slot_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "owner_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_session_slot_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 6}},
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
    owner_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="owner run id"
    )
    slot_version: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="slot version")


class ProjectionRow(Base):
    """Row model for ``projection``."""

    __tablename__ = "projection"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("scope_id", "session_id", name="pk_projection"),
        CheckConstraint("projection_revision >= 0", name="ck_projection_0"),
        CheckConstraint("applied_seq >= 0", name="ck_projection_1"),
        CheckConstraint("rebuild_generation >= 0", name="ck_projection_2"),
        ForeignKeyConstraint(
            ["scope_id", "session_id"],
            ["session.scope_id", "session.id"],
            name="fk_projection_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 9}},
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
    projection_revision: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="projection revision"
    )
    applied_seq: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="applied seq")
    rebuild_generation: Mapped[int] = mapped_column(
        BigInteger(),
        nullable=False,
        server_default=text("0"),
        comment="Projection rebuild generation used by the rebuild_projection dedup key",
    )
    state_ref: Mapped[str] = mapped_column(Text(), nullable=False, comment="state ref")
    content_hash: Mapped[str] = mapped_column(Text(), nullable=False, comment="content hash")


class TimelineItemRow(Base):
    """Row model for ``timeline_item``."""

    __tablename__ = "timeline_item"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_timeline_item"),
        Index("ix_timeline_item_0", "scope_id", "id", unique=True),
        Index("ix_timeline_item_1", "scope_id", "session_id", "source_seq"),
        CheckConstraint("final_revision >= 0", name="ck_timeline_item_0"),
        CheckConstraint("source_seq >= 0", name="ck_timeline_item_1"),
        CheckConstraint(
            "status IN ('pending', 'streaming', 'completed', 'failed', 'cancelled')",
            name="ck_timeline_item_2",
        ),
        ForeignKeyConstraint(
            ["scope_id", "session_id"],
            ["session.scope_id", "session.id"],
            name="fk_timeline_item_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "run_id"],
            ["run.scope_id", "run.id"],
            name="fk_timeline_item_1",
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
    run_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(), nullable=True, comment="run id")
    item_type: Mapped[str] = mapped_column(Text(), nullable=False, comment="item type")
    content_ref: Mapped[str] = mapped_column(Text(), nullable=False, comment="content ref")
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    final_revision: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="final revision"
    )
    source_seq: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="source seq")

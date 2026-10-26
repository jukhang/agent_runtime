"""Append-only journal, outbox, mailbox and command records. (DS-P02-01).

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


class CommandRow(Base):
    """Row model for ``command``."""

    __tablename__ = "command"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_command"),
        Index("ix_command_0", "scope_id", "id", unique=True),
        Index("ix_command_1", "scope_id", "run_id", "actor_id", "request_key", unique=True),
        Index("ix_command_2", "scope_id", "run_id", "status", "created_at"),
        CheckConstraint("expected_state_version >= 0", name="ck_command_0"),
        CheckConstraint("kind IN ('pause', 'resume', 'cancel')", name="ck_command_1"),
        CheckConstraint("status IN ('accepted', 'applied', 'rejected')", name="ck_command_2"),
        ForeignKeyConstraint(
            ["scope_id", "run_id"],
            ["run.scope_id", "run.id"],
            name="fk_command_0",
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
    run_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="run id")
    actor_id: Mapped[str] = mapped_column(Text(), nullable=False, comment="actor id")
    request_key: Mapped[str] = mapped_column(Text(), nullable=False, comment="request key")
    request_hash: Mapped[str] = mapped_column(Text(), nullable=False, comment="request hash")
    kind: Mapped[str] = mapped_column(Text(), nullable=False, comment="kind")
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    expected_state_version: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="expected state version"
    )
    result_ref: Mapped[str | None] = mapped_column(Text(), nullable=True, comment="result ref")
    failure: Mapped[Any | None] = mapped_column(JSONB(), nullable=True, comment="failure")


class AgentMessageRow(Base):
    """Row model for ``agent_message``."""

    __tablename__ = "agent_message"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_agent_message"),
        Index("ix_agent_message_0", "scope_id", "id", unique=True),
        Index("ix_agent_message_1", "scope_id", "recipient_run_id", "recipient_seq", unique=True),
        Index("ix_agent_message_2", "scope_id", "recipient_run_id", "status", "recipient_seq"),
        Index(
            "ix_agent_message_3",
            "scope_id",
            "lease_until",
            postgresql_where=text("status = 'claimed'"),
        ),
        CheckConstraint("recipient_seq >= 0", name="ck_agent_message_0"),
        CheckConstraint(
            "status IN ('pending', 'claimed', 'applied', 'rejected', 'expired')",
            name="ck_agent_message_1",
        ),
        CheckConstraint("recipient_seq > 0", name="ck_agent_message_2"),
        CheckConstraint("sender_run_id <> recipient_run_id", name="ck_agent_message_3"),
        CheckConstraint(
            "(status = 'claimed' AND claim_token IS NOT NULL AND lease_until IS NOT NULL) OR "
            "(status <> 'claimed' AND claim_token IS NULL AND lease_until IS NULL)",
            name="ck_agent_message_4",
        ),
        CheckConstraint("protocol_revision > 0", name="ck_agent_message_5"),
        CheckConstraint(
            "kind IN ('clarification.request', 'control.cancel', 'handoff.accepted', "
            "'handoff.committed', 'handoff.rejected', 'handoff.request', 'input.append', "
            "'progress.updated', 'result.accepted', 'result.proposed', 'result.rejected', "
            "'task.accepted', 'task.assign', 'task.failed', 'task.rejected')",
            name="ck_agent_message_6",
        ),
        ForeignKeyConstraint(
            ["scope_id", "sender_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_agent_message_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "recipient_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_agent_message_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "task_id"],
            ["task.scope_id", "task.id"],
            name="fk_agent_message_2",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "applied_checkpoint_id"],
            ["checkpoint.scope_id", "checkpoint.id"],
            name="fk_agent_message_3",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": False, "catalog_columns": 20}},
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
    sender_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="sender run id"
    )
    recipient_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="recipient run id"
    )
    task_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="task id")
    correlation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="correlation id"
    )
    causation_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="causation id"
    )
    claim_token: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="claim token"
    )
    applied_checkpoint_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="applied checkpoint id"
    )
    recipient_seq: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="recipient seq"
    )
    protocol_revision: Mapped[int] = mapped_column(
        Integer(), nullable=False, comment="protocol revision"
    )
    payload: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="payload")
    kind: Mapped[str] = mapped_column(Text(), nullable=False, comment="kind")
    payload_hash: Mapped[str] = mapped_column(Text(), nullable=False, comment="payload hash")
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    trace_id: Mapped[str] = mapped_column(Text(), nullable=False, comment="trace id")
    deadline_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="deadline at"
    )
    lease_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="lease until"
    )


class MailboxCursorRow(Base):
    """Row model for ``mailbox_cursor``."""

    __tablename__ = "mailbox_cursor"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("scope_id", "recipient_run_id", name="pk_mailbox_cursor"),
        CheckConstraint("allocated_seq >= 0", name="ck_mailbox_cursor_0"),
        CheckConstraint("applied_seq >= 0", name="ck_mailbox_cursor_1"),
        CheckConstraint("applied_seq <= allocated_seq", name="ck_mailbox_cursor_2"),
        ForeignKeyConstraint(
            ["scope_id", "recipient_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_mailbox_cursor_0",
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
    recipient_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="recipient run id"
    )
    allocated_seq: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="allocated seq"
    )
    applied_seq: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="applied seq")


class OutputSealRow(Base):
    """Row model for ``output_seal``."""

    __tablename__ = "output_seal"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_output_seal"),
        Index("ix_output_seal_0", "scope_id", "id", unique=True),
        Index("ix_output_seal_1", "scope_id", "run_id", "model_attempt_id", "item_id", unique=True),
        Index("ix_output_seal_2", "scope_id", "run_id", "sealed_revision"),
        Index("ix_output_seal_3", "scope_id", "proof_id", unique=True),
        CheckConstraint("content_last_delta_index >= 0", name="ck_output_seal_0"),
        CheckConstraint("content_last_session_seq >= 0", name="ck_output_seal_1"),
        CheckConstraint("analysis_last_delta_index >= 0", name="ck_output_seal_2"),
        CheckConstraint("analysis_last_session_seq >= 0", name="ck_output_seal_3"),
        CheckConstraint("durable_seq >= 0", name="ck_output_seal_4"),
        CheckConstraint("sealed_revision >= 0", name="ck_output_seal_5"),
        CheckConstraint("length(content_digest) = 64", name="ck_output_seal_6"),
        CheckConstraint("length(analysis_digest) = 64", name="ck_output_seal_7"),
        CheckConstraint("sealed_revision >= 1", name="ck_output_seal_8"),
        CheckConstraint("durable_seq >= 1", name="ck_output_seal_9"),
        CheckConstraint(
            "(content_last_session_seq IS NULL) = (content_last_delta_index IS NULL)",
            name="ck_output_seal_10",
        ),
        CheckConstraint(
            "(analysis_last_session_seq IS NULL) = (analysis_last_delta_index IS NULL)",
            name="ck_output_seal_11",
        ),
        ForeignKeyConstraint(
            ["scope_id", "session_id"],
            ["session.scope_id", "session.id"],
            name="fk_output_seal_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "run_id"],
            ["run.scope_id", "run.id"],
            name="fk_output_seal_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "model_attempt_id"],
            ["model_attempt.scope_id", "model_attempt.id"],
            name="fk_output_seal_2",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "item_id"],
            ["timeline_item.scope_id", "timeline_item.id"],
            name="fk_output_seal_3",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": True, "catalog_columns": 18}},
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
    proof_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="proof id")
    session_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="session id")
    run_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="run id")
    model_attempt_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(), nullable=False, comment="model attempt id"
    )
    item_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="item id")
    content_digest: Mapped[str] = mapped_column(Text(), nullable=False, comment="content digest")
    analysis_digest: Mapped[str] = mapped_column(Text(), nullable=False, comment="analysis digest")
    content_last_delta_index: Mapped[int | None] = mapped_column(
        BigInteger(), nullable=True, comment="content last delta index"
    )
    content_last_session_seq: Mapped[int | None] = mapped_column(
        BigInteger(), nullable=True, comment="content last session seq"
    )
    analysis_last_delta_index: Mapped[int | None] = mapped_column(
        BigInteger(), nullable=True, comment="analysis last delta index"
    )
    analysis_last_session_seq: Mapped[int | None] = mapped_column(
        BigInteger(), nullable=True, comment="analysis last session seq"
    )
    durable_seq: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="durable seq")
    sealed_revision: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="sealed revision"
    )
    sealed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="sealed at"
    )


class EventRow(Base):
    """Row model for ``event``."""

    __tablename__ = "event"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_event"),
        Index("ix_event_0", "scope_id", "id", unique=True),
        Index("ix_event_1", "scope_id", "session_id", "session_seq", unique=True),
        Index("ix_event_2", "scope_id", "producer_key", unique=True),
        Index("ix_event_3", "scope_id", "run_id", "session_seq"),
        CheckConstraint("session_seq >= 0", name="ck_event_0"),
        CheckConstraint("session_seq > 0", name="ck_event_1"),
        CheckConstraint("visibility IN ('public', 'restricted')", name="ck_event_2"),
        CheckConstraint("schema_revision > 0", name="ck_event_3"),
        CheckConstraint(
            "kind IN ('analysis.delta', 'approval.required', 'approval.updated', "
            "'artifact.created', "
            "'assistant.delta', 'checkpoint.migrated', 'command.updated', 'handoff.committed', "
            "'input.accepted', 'input.required', 'item.completed', 'item.started', "
            "'mailbox.applied', "
            "'model.prepared', "
            "'plan.updated', 'resource.revoked', 'run.cancelled', 'run.completed', 'run.failed', "
            "'run.status', 'tool.finished', 'tool.progress', 'tool.started', 'turn.accepted')",
            name="ck_event_4",
        ),
        ForeignKeyConstraint(
            ["scope_id", "session_id"],
            ["session.scope_id", "session.id"],
            name="fk_event_0",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "turn_id"],
            ["turn.scope_id", "turn.id"],
            name="fk_event_1",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "run_id"],
            ["run.scope_id", "run.id"],
            name="fk_event_2",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "parent_run_id"],
            ["run.scope_id", "run.id"],
            name="fk_event_3",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "item_id"],
            ["timeline_item.scope_id", "timeline_item.id"],
            name="fk_event_4",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        ForeignKeyConstraint(
            ["scope_id", "model_attempt_id"],
            ["model_attempt.scope_id", "model_attempt.id"],
            name="fk_event_5",
            ondelete="NO ACTION",
            deferrable=True,
            initially="DEFERRED",
        ),
        {"info": {"immutable": True, "catalog_columns": 21}},
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
    turn_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(), nullable=True, comment="turn id")
    run_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(), nullable=True, comment="run id")
    parent_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="parent run id"
    )
    step_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(), nullable=True, comment="step id")
    item_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(), nullable=True, comment="item id")
    model_attempt_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="model attempt id"
    )
    session_seq: Mapped[int] = mapped_column(BigInteger(), nullable=False, comment="session seq")
    schema_revision: Mapped[int] = mapped_column(
        Integer(), nullable=False, comment="schema revision"
    )
    producer_key: Mapped[str] = mapped_column(Text(), nullable=False, comment="producer key")
    producer_digest: Mapped[str] = mapped_column(Text(), nullable=False, comment="producer digest")
    kind: Mapped[str] = mapped_column(Text(), nullable=False, comment="kind")
    visibility: Mapped[str] = mapped_column(Text(), nullable=False, comment="visibility")
    trace_id: Mapped[str] = mapped_column(Text(), nullable=False, comment="trace id")
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="occurred at"
    )
    recorded_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="recorded at"
    )
    payload: Mapped[Any] = mapped_column(JSONB(), nullable=False, comment="payload")


class OutboxRow(Base):
    """Row model for ``outbox``."""

    __tablename__ = "outbox"
    __table_args__ = (
        # Primary key and constraints mirror contracts/storage.json exactly.
        PrimaryKeyConstraint("id", name="pk_outbox"),
        Index("ix_outbox_0", "scope_id", "id", unique=True),
        Index("ix_outbox_1", "scope_id", "event_id", unique=True),
        Index(
            "ix_outbox_2", "scope_id", "available_at", postgresql_where=text("status = 'pending'")
        ),
        Index(
            "ix_outbox_3", "scope_id", "lease_until", postgresql_where=text("status = 'claimed'")
        ),
        CheckConstraint("attempt_count >= 0", name="ck_outbox_0"),
        CheckConstraint("status IN ('pending', 'claimed', 'delivered')", name="ck_outbox_1"),
        CheckConstraint(
            "(status = 'claimed' AND claim_token IS NOT NULL AND lease_until IS NOT NULL) OR "
            "(status <> 'claimed' AND claim_token IS NULL AND lease_until IS NULL)",
            name="ck_outbox_2",
        ),
        ForeignKeyConstraint(
            ["scope_id", "event_id"],
            ["event.scope_id", "event.id"],
            name="fk_outbox_0",
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
    event_id: Mapped[uuid.UUID] = mapped_column(Uuid(), nullable=False, comment="event id")
    claim_token: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(), nullable=True, comment="claim token"
    )
    status: Mapped[str] = mapped_column(Text(), nullable=False, comment="status")
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="available at"
    )
    lease_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="lease until"
    )
    delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, comment="delivered at"
    )
    attempt_count: Mapped[int] = mapped_column(
        BigInteger(), nullable=False, comment="attempt count"
    )

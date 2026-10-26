"""Portable PostgreSQL storage specification, not an ORM or DDL executor."""

import argparse
import json
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Literal

from agent_runtime.contracts.events import event_adapter
from agent_runtime.contracts.messages import message_adapter
from agent_runtime.contracts.references import CAPABILITY_KINDS

type SqlType = Literal["UUID", "TEXT", "BIGINT", "INTEGER", "TIMESTAMPTZ", "JSONB", "BYTEA"]


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: SqlType
    nullable: bool = False
    default: str | None = None
    comment: str = ""


@dataclass(frozen=True)
class Index:
    columns: tuple[str, ...]
    unique: bool = False
    predicate: str | None = None


@dataclass(frozen=True)
class Check:
    columns: tuple[str, ...]
    expression: str


@dataclass(frozen=True)
class ForeignKey:
    columns: tuple[str, ...]
    target: str
    target_columns: tuple[str, ...] = ("scope_id", "id")
    on_delete: Literal["NO ACTION"] = "NO ACTION"
    deferrable: bool = True
    initially_deferred: bool = True


@dataclass(frozen=True)
class Table:
    name: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]
    indexes: tuple[Index, ...]
    checks: tuple[Check, ...]
    foreign_keys: tuple[ForeignKey, ...]
    immutable: bool = False


def cols(sql_type: SqlType, *names: str, nullable: bool = False) -> tuple[Column, ...]:
    return tuple(Column(name, sql_type, nullable, comment=name.replace("_", " ")) for name in names)


def enum(column: str, *values: str) -> Check:
    literals = ", ".join("'" + value + "'" for value in values)
    return Check((column,), f"{column} IN ({literals})")


def table(
    name: str,
    *fields: Column,
    primary: tuple[str, ...] = ("id",),
    unique: tuple[tuple[str, ...], ...] = (),
    indexes: tuple[Index, ...] = (),
    checks: tuple[Check, ...] = (),
    refs: tuple[tuple[str | tuple[str, ...], str], ...] = (),
    immutable: bool = False,
    scoped: bool = True,
) -> Table:
    # ``refs`` entries are either a single column (meaning the composite scope key
    # ``("scope_id", field)``) or an explicit column tuple for a scope-less composite
    # reference that targets the whole primary key of another scope-less table.
    foreign_keys = tuple(
        ForeignKey(("scope_id", field), target)
        if isinstance(field, str)
        else ForeignKey(field, target, target_columns=field)
        for field, target in refs
    )
    common = (
        *(
            (Column("scope_id", "TEXT", comment="Trusted host isolation scope"),)
            if scoped
            else ()
        ),
        Column("created_at", "TIMESTAMPTZ", default="CURRENT_TIMESTAMP", comment="Creation time"),
        Column(
            "updated_at", "TIMESTAMPTZ", default="CURRENT_TIMESTAMP", comment="Last mutation time"
        ),
    )
    identity = (
        (Column("id", "UUID", comment="Application allocated stable identity"),)
        if primary == ("id",)
        else ()
    )
    all_columns = (*identity, *common, *fields)
    scoped_key = (Index(("scope_id", "id"), unique=True),) if identity and scoped else ()
    nonnegative = tuple(
        Check((c.name,), f"{c.name} >= 0") for c in fields if c.sql_type == "BIGINT"
    )
    return Table(
        name,
        all_columns,
        primary,
        (*scoped_key, *(Index(key, unique=True) for key in unique), *indexes),
        (*nonnegative, *checks),
        foreign_keys,
        immutable,
    )


USAGE = ("complete", "partial", "unknown")
RUN_STATES = (
    "queued",
    "running",
    "waiting_tool",
    "waiting_user",
    "paused",
    "cancelling",
    "completed",
    "failed",
    "cancelled",
)
RESOURCE_KINDS = ("model_slot", "tool_slot", "run_slot", "sandbox_slot", "egress_slot")
CAPABILITY_STATUSES = ("draft", "active", "superseded", "revoked")
USAGE_REASONS = (
    "pending",
    "provider_reported",
    "provider_not_reported",
    "collection_failed",
    "estimated",
)
# Frozen usage completeness matrix (DS-P02-00): a reported reason must match the status.
USAGE_REASON_MATRIX = (
    "(usage_status = 'complete' AND usage_reason = 'provider_reported') "
    "OR (usage_status = 'partial' AND usage_reason IN ('provider_reported','estimated')) "
    "OR (usage_status = 'unknown' AND usage_reason IN "
    "('provider_not_reported','collection_failed','estimated'))"
)

WORK_KINDS = (
    "advance_run",
    "reconcile_execution",
    "migrate_checkpoint",
    "rebuild_projection",
    "recover_scope",
    "collect_garbage",
    "evaluate",
    "domain_job",
)

# Columns are explicitly grouped by SQL type, never inferred from a suffix.
_TABLES = (
    table(
        "credential_service_binding",
        *cols("UUID", "grant_id"),
        *cols("TEXT", "peer_sha256"),
        unique=(("scope_id", "grant_id"),),
        checks=(Check(("peer_sha256",), "peer_sha256 ~ '^[0-9a-f]{64}$'"),),
        refs=(("grant_id", "capability_grant"),),
        immutable=True,
    ),
    table(
        "credential_request",
        *cols("UUID", "grant_id"),
        *cols("TEXT", "request_hash", "status"),
        *cols("JSONB", "response", nullable=True),
        *cols("TIMESTAMPTZ", "finished_at", nullable=True),
        indexes=(Index(("scope_id", "grant_id", "status")),),
        checks=(
            enum("status", "reserved", "unknown", "completed"),
            Check(("request_hash",), "length(request_hash) = 64"),
            Check(("status", "response"), "(status = 'completed') = (response IS NOT NULL)"),
            Check(("status", "finished_at"), "(status = 'completed') = (finished_at IS NOT NULL)"),
        ),
        refs=(("grant_id", "capability_grant"),),
    ),
    table(
        "credential_audit",
        *cols("UUID", "grant_id"),
        *cols("UUID", "request_id", nullable=True),
        *cols("TEXT", "actor_id", "action"),
        *cols("JSONB", "evidence"),
        indexes=(Index(("scope_id", "grant_id", "created_at")),),
        checks=(enum("action", "issued", "reserved", "completed", "unknown", "revoked"),),
        refs=(("grant_id", "capability_grant"), ("request_id", "credential_request")),
        immutable=True,
    ),
    table(
        "session",
        *cols("TEXT", "owner_actor_id", "title_source"),
        *cols("TEXT", "title", nullable=True),
        *cols("BIGINT", "event_seq", "auth_revision"),
        *cols("TIMESTAMPTZ", "deleted_at", nullable=True),
        indexes=(Index(("scope_id", "owner_actor_id", "updated_at")),),
        checks=(enum("title_source", "none", "generated", "document", "manual"),),
    ),
    table(
        "turn",
        *cols("UUID", "session_id"),
        *cols("TEXT", "request_key", "request_hash", "input_ref"),
        *cols("UUID", "primary_run_id", "outcome_run_id", nullable=True),
        *cols("TEXT", "outcome", nullable=True),
        *cols("JSONB", "failure", nullable=True),
        unique=(("scope_id", "session_id", "request_key"),),
        indexes=(Index(("scope_id", "session_id", "created_at")),),
        refs=(("session_id", "session"), ("primary_run_id", "run"), ("outcome_run_id", "run")),
    ),
    table(
        "run",
        *cols("UUID", "session_id", "turn_id", "root_run_id", "agent_revision_id", "task_id"),
        *cols("UUID", "parent_run_id", "checkpoint_id", "control_command_id", nullable=True),
        *cols("TEXT", "status", "phase"),
        *cols("TEXT", "outcome", "owner_id", "desired_control", "pause_reason", nullable=True),
        *cols("JSONB", "failure", "waiting_spec", nullable=True),
        *cols(
            "BIGINT",
            "state_version",
            "owner_epoch",
            "cancel_version",
            "wake_generation",
            "pending_input_version",
            "applied_input_version",
        ),
        Column(
            "authority_revision",
            "BIGINT",
            default="0",
            comment=(
                "Per-run authority fence revision; grant writers bump it under the run row"
            ),
        ),
        *cols("TIMESTAMPTZ", "lease_until", nullable=True),
        *cols("TIMESTAMPTZ", "phase_started_at", "deadline_at"),
        indexes=(
            Index(("scope_id", "status", "lease_until")),
            Index(("scope_id", "root_run_id")),
            Index(("scope_id", "turn_id")),
        ),
        checks=(
            enum("status", *RUN_STATES),
            enum("desired_control", "pause", "cancel"),
            enum(
                "phase",
                "queued",
                "context",
                "model",
                "tool",
                "waiting",
                "paused",
                "cancelling",
                "terminal",
            ),
            Check(("owner_id", "lease_until"), "(owner_id IS NULL) = (lease_until IS NULL)"),
            Check(
                ("desired_control", "control_command_id"),
                "(desired_control IS NULL) = (control_command_id IS NULL)",
            ),
            Check(
                ("desired_control", "status"),
                "(desired_control = 'cancel') = (status = 'cancelling')",
            ),
            Check(
                ("desired_control", "status"),
                "desired_control IS NULL OR status IN "
                "('queued', 'running', 'waiting_tool', 'waiting_user', 'cancelling')",
            ),
            Check(
                ("pending_input_version", "applied_input_version"),
                "applied_input_version <= pending_input_version",
            ),
            Check(
                ("status", "outcome"),
                "(status = 'completed' AND outcome IS NOT NULL "
                "AND outcome IN ('success','partial')) "
                "OR (status <> 'completed' AND outcome IS NULL)",
            ),
            Check(("status", "failure"), "(status = 'failed') = (failure IS NOT NULL)"),
        ),
        refs=(
            ("session_id", "session"),
            ("turn_id", "turn"),
            ("root_run_id", "run"),
            ("parent_run_id", "run"),
            ("agent_revision_id", "agent_revision"),
            ("task_id", "task"),
            ("checkpoint_id", "checkpoint"),
            ("control_command_id", "command"),
        ),
    ),
    table(
        "session_slot",
        *cols("UUID", "session_id"),
        *cols("UUID", "owner_run_id", nullable=True),
        *cols("BIGINT", "slot_version"),
        primary=("scope_id", "session_id"),
        refs=(("session_id", "session"), ("owner_run_id", "run")),
    ),
    table(
        "work_item",
        *cols("UUID", "run_id", nullable=True),
        *cols("JSONB", "subject_ref"),
        *cols("TEXT", "kind", "dedup_key", "status", "payload_ref"),
        *cols("TEXT", "handler_namespace", nullable=True),
        *cols("TEXT", "handler_key", nullable=True),
        *cols("BIGINT", "handler_revision", nullable=True),
        *cols("TEXT", "wake_reason", "owner_id", nullable=True),
        *cols("INTEGER", "priority"),
        *cols("BIGINT", "lease_epoch", "attempt_count"),
        *cols("TIMESTAMPTZ", "available_at", "deadline_at"),
        *cols("TIMESTAMPTZ", "lease_until", nullable=True),
        *cols("JSONB", "failure", nullable=True),
        unique=(("scope_id", "dedup_key"),),
        indexes=(
            Index(
                ("scope_id", "run_id"),
                True,
                "kind = 'advance_run' AND status IN ('pending','leased')",
            ),
            Index(("scope_id", "available_at", "priority", "id"), predicate="status = 'pending'"),
            Index(("scope_id", "lease_until"), predicate="status = 'leased'"),
            Index(
                ("scope_id", "handler_namespace", "handler_key", "handler_revision", "status"),
                predicate="kind = 'domain_job'",
            ),
        ),
        checks=(
            enum("kind", *WORK_KINDS),
            enum("status", "pending", "leased", "completed", "failed", "cancelled"),
            enum(
                "wake_reason",
                "start",
                "tool_result",
                "message",
                "resume",
                "handoff",
                "recovery",
                "control",
            ),
            Check(
                ("kind", "run_id", "wake_reason"),
                "(kind = 'advance_run' AND run_id IS NOT NULL AND wake_reason IS NOT NULL) "
                "OR (kind <> 'advance_run' AND wake_reason IS NULL)",
            ),
            Check(
                ("kind", "handler_namespace", "handler_key", "handler_revision"),
                "(kind = 'domain_job') = (handler_namespace IS NOT NULL "
                "AND handler_key IS NOT NULL AND handler_revision IS NOT NULL)",
            ),
            Check(
                ("status", "owner_id", "lease_until"),
                "(status = 'leased' AND owner_id IS NOT NULL AND lease_until IS NOT NULL) "
                "OR (status <> 'leased' AND owner_id IS NULL AND lease_until IS NULL)",
            ),
        ),
        refs=(("run_id", "run"),),
    ),
    table(
        "checkpoint",
        *cols("UUID", "run_id"),
        *cols("UUID", "source_checkpoint_id", nullable=True),
        *cols("BIGINT", "state_version", "representation_revision", "event_seq"),
        *cols("INTEGER", "schema_revision"),
        *cols("TEXT", "runtime_build", "state_ref", "content_hash"),
        unique=(("scope_id", "run_id", "state_version", "representation_revision"),),
        refs=(("run_id", "run"), ("source_checkpoint_id", "checkpoint")),
        immutable=True,
    ),
    table(
        "command",
        *cols("UUID", "run_id"),
        *cols("TEXT", "actor_id", "request_key", "request_hash", "kind", "status"),
        *cols("BIGINT", "expected_state_version"),
        *cols("TEXT", "result_ref", nullable=True),
        *cols("JSONB", "failure", nullable=True),
        unique=(("scope_id", "run_id", "actor_id", "request_key"),),
        indexes=(Index(("scope_id", "run_id", "status", "created_at")),),
        checks=(
            enum("kind", "pause", "resume", "cancel"),
            enum("status", "accepted", "applied", "rejected"),
        ),
        refs=(("run_id", "run"),),
    ),
    table(
        "agent_message",
        *cols("UUID", "sender_run_id", "recipient_run_id", "task_id", "correlation_id"),
        *cols("UUID", "causation_id", "claim_token", "applied_checkpoint_id", nullable=True),
        *cols("BIGINT", "recipient_seq"),
        *cols("INTEGER", "protocol_revision"),
        *cols("JSONB", "payload"),
        *cols("TEXT", "kind", "payload_hash", "status", "trace_id"),
        *cols("TIMESTAMPTZ", "deadline_at"),
        *cols("TIMESTAMPTZ", "lease_until", nullable=True),
        unique=(("scope_id", "recipient_run_id", "recipient_seq"),),
        indexes=(
            Index(("scope_id", "recipient_run_id", "status", "recipient_seq")),
            Index(("scope_id", "lease_until"), predicate="status = 'claimed'"),
        ),
        checks=(
            enum("status", "pending", "claimed", "applied", "rejected", "expired"),
            Check(("recipient_seq",), "recipient_seq > 0"),
            Check(("sender_run_id", "recipient_run_id"), "sender_run_id <> recipient_run_id"),
            Check(
                ("status", "claim_token", "lease_until"),
                "(status = 'claimed' AND claim_token IS NOT NULL AND lease_until IS NOT NULL) "
                "OR (status <> 'claimed' AND claim_token IS NULL AND lease_until IS NULL)",
            ),
        ),
        refs=(
            ("sender_run_id", "run"),
            ("recipient_run_id", "run"),
            ("task_id", "task"),
            ("applied_checkpoint_id", "checkpoint"),
        ),
    ),
    table(
        "mailbox_cursor",
        *cols("UUID", "recipient_run_id"),
        *cols("BIGINT", "allocated_seq", "applied_seq"),
        primary=("scope_id", "recipient_run_id"),
        checks=(Check(("applied_seq", "allocated_seq"), "applied_seq <= allocated_seq"),),
        refs=(("recipient_run_id", "run"),),
    ),
    table(
        "model_attempt",
        *cols("UUID", "run_id", "context_snapshot_id"),
        *cols("TEXT", "request_key", "status", "input_ref", "usage_status"),
        *cols("TEXT", "usage_reason"),
        *cols("TEXT", "provider_request_id", "output_ref", "finish_reason", nullable=True),
        *cols("JSONB", "usage", "failure", nullable=True),
        *cols("TIMESTAMPTZ", "started_at", "finished_at", nullable=True),
        unique=(("scope_id", "run_id", "request_key"),),
        indexes=(
            Index(("scope_id", "run_id", "started_at")),
            Index(("scope_id", "status", "started_at")),
        ),
        checks=(
            enum("status", "pending", "running", "completed", "failed", "interrupted", "cancelled"),
            enum("usage_status", "pending", *USAGE),
            enum("usage_reason", *USAGE_REASONS),
            Check(
                ("usage_status", "usage_reason"),
                "(usage_status = 'pending' AND usage_reason = 'pending') OR "
                + USAGE_REASON_MATRIX,
            ),
            Check(
                ("status", "finished_at"),
                "(status IN ('completed','failed','interrupted','cancelled')) "
                "= (finished_at IS NOT NULL)",
            ),
        ),
        refs=(("run_id", "run"), ("context_snapshot_id", "context_snapshot")),
    ),
    table(
        "tool_call",
        *cols("UUID", "run_id"),
        *cols("UUID", "attempt_id", "operation_id", nullable=True),
        *cols("TEXT", "provider_call_id", "result_ref", nullable=True),
        *cols("JSONB", "spec_revision"),
        *cols("TEXT", "input_hash", "status"),
        *cols("TIMESTAMPTZ", "started_at", "finished_at", nullable=True),
        *cols("JSONB", "failure", nullable=True),
        indexes=(
            Index(("scope_id", "attempt_id", "provider_call_id"), True, "attempt_id IS NOT NULL"),
            Index(("scope_id", "run_id", "status")),
        ),
        checks=(
            enum(
                "status",
                "pending",
                "running",
                "waiting_approval",
                "completed",
                "failed",
                "cancelled",
                "not_executed",
            ),
            Check(
                ("attempt_id", "provider_call_id"),
                "(attempt_id IS NULL) = (provider_call_id IS NULL)",
            ),
        ),
        refs=(("run_id", "run"), ("attempt_id", "model_attempt"), ("operation_id", "operation")),
    ),
    table(
        "operation",
        *cols("UUID", "task_id"),
        *cols("TEXT", "idempotency_key", "input_hash", "status"),
        *cols("TEXT", "result_ref", nullable=True),
        *cols("JSONB", "failure", nullable=True),
        unique=(("scope_id", "task_id", "idempotency_key"),),
        indexes=(Index(("scope_id", "status", "updated_at")),),
        checks=(
            enum("status", "prepared", "dispatching", "committed", "failed_safe", "unknown"),
            Check(("status", "result_ref"), "status <> 'committed' OR result_ref IS NOT NULL"),
        ),
        refs=(("task_id", "task"),),
    ),
    table(
        "execution",
        *cols("UUID", "operation_id"),
        *cols("UUID", "run_id"),
        *cols("UUID", "supervisor_work_id", nullable=True),
        *cols("TEXT", "remote_request_key", "status", "log_cursor", "cleanup_status"),
        *cols("TEXT", "sandbox_id", "remote_execution_id", nullable=True),
        *cols("BIGINT", "generation"),
        Column(
            "reconciliation_round",
            "BIGINT",
            default="0",
            comment="Supervision round used by the reconcile_execution dedup key",
        ),
        *cols("TIMESTAMPTZ", "deadline_at"),
        *cols("JSONB", "failure", nullable=True),
        unique=(("scope_id", "remote_request_key"),),
        indexes=(
            Index(
                ("scope_id", "sandbox_id", "remote_execution_id"),
                True,
                "sandbox_id IS NOT NULL AND remote_execution_id IS NOT NULL",
            ),
            Index(("scope_id", "status", "deadline_at")),
            Index(("scope_id", "run_id", "generation")),
            Index(("scope_id", "cleanup_status")),
        ),
        checks=(
            enum(
                "status",
                "pending",
                "starting",
                "running",
                "cancelling",
                "completed",
                "failed",
                "cancelled",
                "unknown",
            ),
            enum("cleanup_status", "pending", "running", "completed", "failed"),
        ),
        refs=(
            ("operation_id", "operation"),
            ("run_id", "run"),
            ("supervisor_work_id", "work_item"),
        ),
    ),
    # Sealed producer facts (DS-P02-02 second repair, Q45): ``event.producer_digest`` is a
    # single-event digest and cannot prove that a producer finished flushing both output
    # channels. One row per sealed producer identity records the *cumulative* channel
    # digests, both channel heads and the durable journal watermark, so the State UoW can
    # lock and re-enumerate seals instead of inferring durability from event existence.
    table(
        "output_seal",
        *cols("UUID", "proof_id", "session_id", "run_id", "model_attempt_id", "item_id"),
        *cols("TEXT", "content_digest", "analysis_digest"),
        *cols(
            "BIGINT", "content_last_delta_index", "content_last_session_seq", nullable=True
        ),
        *cols(
            "BIGINT", "analysis_last_delta_index", "analysis_last_session_seq", nullable=True
        ),
        *cols("BIGINT", "durable_seq", "sealed_revision"),
        *cols("TIMESTAMPTZ", "sealed_at"),
        immutable=True,
        unique=(("scope_id", "run_id", "model_attempt_id", "item_id"),),
        indexes=(
            Index(("scope_id", "run_id", "sealed_revision")),
            Index(("scope_id", "proof_id"), True),
        ),
        checks=(
            Check(("content_digest",), "length(content_digest) = 64"),
            Check(("analysis_digest",), "length(analysis_digest) = 64"),
            Check(("sealed_revision",), "sealed_revision >= 1"),
            Check(("durable_seq",), "durable_seq >= 1"),
            Check(
                ("content_last_session_seq", "content_last_delta_index"),
                "(content_last_session_seq IS NULL) = (content_last_delta_index IS NULL)",
            ),
            Check(
                ("analysis_last_session_seq", "analysis_last_delta_index"),
                "(analysis_last_session_seq IS NULL) = (analysis_last_delta_index IS NULL)",
            ),
        ),
        refs=(
            ("session_id", "session"),
            ("run_id", "run"),
            ("model_attempt_id", "model_attempt"),
            ("item_id", "timeline_item"),
        ),
    ),
    table(
        "event",
        *cols("UUID", "session_id"),
        *cols(
            "UUID",
            "turn_id",
            "run_id",
            "parent_run_id",
            "step_id",
            "item_id",
            "model_attempt_id",
            nullable=True,
        ),
        *cols("BIGINT", "session_seq"),
        *cols("INTEGER", "schema_revision"),
        *cols("TEXT", "producer_key", "producer_digest", "kind", "visibility", "trace_id"),
        *cols("TIMESTAMPTZ", "occurred_at", "recorded_at"),
        *cols("JSONB", "payload"),
        unique=(("scope_id", "session_id", "session_seq"), ("scope_id", "producer_key")),
        indexes=(Index(("scope_id", "run_id", "session_seq")),),
        checks=(
            Check(("session_seq",), "session_seq > 0"),
            enum("visibility", "public", "restricted"),
        ),
        refs=(
            ("session_id", "session"),
            ("turn_id", "turn"),
            ("run_id", "run"),
            ("parent_run_id", "run"),
            ("item_id", "timeline_item"),
            ("model_attempt_id", "model_attempt"),
        ),
        immutable=True,
    ),
    table(
        "outbox",
        *cols("UUID", "event_id"),
        *cols("UUID", "claim_token", nullable=True),
        *cols("TEXT", "status"),
        *cols("TIMESTAMPTZ", "available_at"),
        *cols("TIMESTAMPTZ", "lease_until", "delivered_at", nullable=True),
        *cols("BIGINT", "attempt_count"),
        unique=(("scope_id", "event_id"),),
        indexes=(
            Index(("scope_id", "available_at"), predicate="status = 'pending'"),
            Index(("scope_id", "lease_until"), predicate="status = 'claimed'"),
        ),
        checks=(
            enum("status", "pending", "claimed", "delivered"),
            Check(
                ("status", "claim_token", "lease_until"),
                "(status = 'claimed' AND claim_token IS NOT NULL AND lease_until IS NOT NULL) "
                "OR (status <> 'claimed' AND claim_token IS NULL AND lease_until IS NULL)",
            ),
        ),
        refs=(("event_id", "event"),),
    ),
    table(
        "projection",
        *cols("UUID", "session_id"),
        *cols("BIGINT", "projection_revision", "applied_seq"),
        Column(
            "rebuild_generation",
            "BIGINT",
            default="0",
            comment="Projection rebuild generation used by the rebuild_projection dedup key",
        ),
        *cols("TEXT", "state_ref", "content_hash"),
        primary=("scope_id", "session_id"),
        refs=(("session_id", "session"),),
    ),
    table(
        "budget_allocation",
        *cols("UUID", "root_run_id"),
        *cols("UUID", "parent_id", nullable=True),
        *cols("JSONB", "limits", "reserved", "unknown_reserved", "settled"),
        *cols("TEXT", "usage_status"),
        *cols("BIGINT", "unsettled_count", "version"),
        indexes=(Index(("scope_id", "root_run_id")),),
        checks=(enum("usage_status", *USAGE),),
        refs=(("root_run_id", "run"), ("parent_id", "budget_allocation")),
    ),
    table(
        "budget_entry",
        *cols("UUID", "allocation_id"),
        *cols("TEXT", "operation_key", "kind", "usage_status"),
        *cols("TEXT", "usage_reason", nullable=True),
        *cols("JSONB", "quantities"),
        *cols("TEXT", "reconciliation_ref", nullable=True),
        unique=(("scope_id", "allocation_id", "operation_key", "kind"),),
        checks=(
            enum("kind", "reserve", "settle", "release", "adjust"),
            enum("usage_status", *USAGE),
            enum("usage_reason", *USAGE_REASONS),
            Check(
                ("kind", "usage_status", "usage_reason"),
                "((kind = 'settle') = (usage_reason IS NOT NULL)) AND "
                "(usage_reason IS NULL OR (" + USAGE_REASON_MATRIX + "))",
            ),
        ),
        refs=(("allocation_id", "budget_allocation"),),
        immutable=True,
    ),
    table(
        "resource_pool",
        *cols("TEXT", "pool_key"),
        *cols("BIGINT", "capacity", "reserved", "version"),
        unique=(("scope_id", "pool_key"),),
        checks=(Check(("reserved", "capacity"), "reserved <= capacity"),),
    ),
    table(
        "resource_permit",
        *cols("UUID", "pool_id"),
        *cols("TEXT", "holder_key", "status"),
        *cols("BIGINT", "capacity_units"),
        *cols("TIMESTAMPTZ", "lease_until"),
        unique=(("scope_id", "pool_id", "holder_key"),),
        indexes=(Index(("scope_id", "pool_id", "status")), Index(("scope_id", "lease_until"))),
        checks=(
            enum("status", "reserved", "released", "uncertain"),
            Check(("capacity_units",), "capacity_units > 0"),
        ),
        refs=(("pool_id", "resource_pool"),),
    ),
    table(
        "agent",
        *cols("TEXT", "key"),
        *cols("UUID", "published_revision_id", nullable=True),
        unique=(("scope_id", "key"),),
        refs=(("published_revision_id", "agent_revision"),),
    ),
    table(
        "agent_revision",
        *cols("UUID", "agent_id"),
        *cols("BIGINT", "revision"),
        *cols("JSONB", "manifest"),
        *cols("TEXT", "content_hash"),
        unique=(("scope_id", "agent_id", "revision"),),
        refs=(("agent_id", "agent"),),
        immutable=True,
    ),
    table(
        "task",
        *cols("UUID", "root_run_id"),
        *cols("UUID", "parent_task_id", "current_goal_revision_id", nullable=True),
        *cols("TEXT", "acceptance_spec_ref"),
        *cols("TIMESTAMPTZ", "deadline_at"),
        indexes=(Index(("scope_id", "root_run_id")),),
        refs=(
            ("root_run_id", "run"),
            ("parent_task_id", "task"),
            ("current_goal_revision_id", "goal_revision"),
        ),
    ),
    table(
        "goal_revision",
        *cols("UUID", "task_id"),
        *cols("BIGINT", "revision"),
        *cols("TEXT", "objective"),
        *cols("JSONB", "constraints", "focus_refs"),
        unique=(("scope_id", "task_id", "revision"),),
        refs=(("task_id", "task"),),
        immutable=True,
    ),
    table(
        "plan",
        *cols("UUID", "task_id"),
        *cols("BIGINT", "current_revision"),
        unique=(("scope_id", "task_id"),),
        refs=(("task_id", "task"),),
    ),
    table(
        "plan_revision",
        *cols("UUID", "plan_id"),
        *cols("BIGINT", "revision"),
        *cols("JSONB", "steps"),
        *cols("TEXT", "public_reason"),
        unique=(("scope_id", "plan_id", "revision"),),
        refs=(("plan_id", "plan"),),
        immutable=True,
    ),
    table(
        "task_dependency",
        *cols("UUID", "task_id", "depends_on_task_id"),
        primary=("scope_id", "task_id", "depends_on_task_id"),
        checks=(Check(("task_id", "depends_on_task_id"), "task_id <> depends_on_task_id"),),
        refs=(("task_id", "task"), ("depends_on_task_id", "task")),
    ),
    table(
        "context_snapshot",
        *cols("UUID", "session_id"),
        *cols("UUID", "run_id", nullable=True),
        *cols("BIGINT", "auth_revision", "context_version", "estimated_input_tokens"),
        *cols("INTEGER", "schema_revision"),
        *cols("TEXT", "task_type", "payload_ref", "content_hash"),
        *cols(
            "JSONB", "source_refs", "degradation", "model_ref", "prompt_ref", "tool_refs", "budget"
        ),
        indexes=(Index(("scope_id", "run_id", "created_at")),),
        refs=(("session_id", "session"), ("run_id", "run")),
        immutable=True,
    ),
    table(
        "context_blob",
        *cols("TEXT", "blob_key", "digest"),
        *cols("BIGINT", "size_bytes"),
        *cols("BYTEA", "content"),
        primary=("scope_id", "blob_key"),
        checks=(
            Check(("digest",), "length(digest) = 64"),
            Check(("size_bytes", "content"), "size_bytes = octet_length(content)"),
            Check(("size_bytes",), "size_bytes <= 16777216"),
        ),
        immutable=True,
    ),
    table(
        "approval",
        *cols("UUID", "run_id"),
        *cols("UUID", "operation_id", nullable=True),
        *cols("TEXT", "actor_id", "request_key", "input_hash", "status"),
        *cols("TEXT", "decision", "decision_actor_id", nullable=True),
        *cols("BIGINT", "revision", "auth_revision"),
        *cols("JSONB", "policy_ref", "tool_revision", "resource_refs"),
        *cols("TIMESTAMPTZ", "expires_at"),
        *cols("TIMESTAMPTZ", "approved_at", nullable=True),
        unique=(("scope_id", "request_key"),),
        indexes=(Index(("scope_id", "status", "expires_at")),),
        checks=(
            enum("status", "pending", "approved", "rejected", "expired", "cancelled"),
            enum("decision", "approved", "rejected", "cancelled"),
        ),
        refs=(("run_id", "run"), ("operation_id", "operation")),
    ),
    table(
        "artifact",
        *cols("TEXT", "owner_actor_id"),
        *cols("UUID", "task_id"),
        *cols("UUID", "latest_revision_id", nullable=True),
        indexes=(Index(("scope_id", "task_id", "updated_at")),),
        refs=(("task_id", "task"), ("latest_revision_id", "artifact_revision")),
    ),
    table(
        "artifact_revision",
        *cols("UUID", "artifact_id"),
        *cols("UUID", "source_execution_id", nullable=True),
        *cols("BIGINT", "revision"),
        *cols("JSONB", "blob_ref", "input_refs"),
        *cols("TEXT", "status"),
        unique=(("scope_id", "artifact_id", "revision"),),
        refs=(("artifact_id", "artifact"), ("source_execution_id", "execution")),
        immutable=True,
    ),
    table(
        "workspace",
        *cols("UUID", "task_id", "owner_run_id"),
        *cols("UUID", "latest_checkpoint_id", nullable=True),
        *cols("BIGINT", "generation", "revision"),
        indexes=(Index(("scope_id", "task_id")),),
        refs=(
            ("task_id", "task"),
            ("owner_run_id", "run"),
            ("latest_checkpoint_id", "workspace_checkpoint"),
        ),
    ),
    table(
        "workspace_checkpoint",
        *cols("UUID", "workspace_id", "owner_run_id"),
        *cols("BIGINT", "generation", "revision", "total_bytes"),
        *cols("TEXT", "state", "manifest_ref", "content_hash"),
        unique=(("scope_id", "workspace_id", "revision"),),
        checks=(enum("state", "preparing", "ready", "failed"),),
        refs=(("workspace_id", "workspace"), ("owner_run_id", "run")),
    ),
    table(
        "handoff",
        *cols("UUID", "session_id", "source_run_id", "target_run_id"),
        *cols("BIGINT", "expected_slot_version"),
        *cols("TEXT", "status", "manifest_ref"),
        *cols("TIMESTAMPTZ", "deadline_at"),
        indexes=(Index(("scope_id", "session_id"), True, "status IN ('preparing','accepted')"),),
        checks=(
            enum("status", "preparing", "accepted", "committed", "rejected", "expired"),
            Check(("source_run_id", "target_run_id"), "source_run_id <> target_run_id"),
        ),
        refs=(("session_id", "session"), ("source_run_id", "run"), ("target_run_id", "run")),
    ),
    table(
        "capability_grant",
        *cols("UUID", "run_id"),
        *cols("UUID", "execution_id", nullable=True),
        *cols("BIGINT", "execution_generation", nullable=True),
        *cols("TEXT", "actor_id"),
        *cols("BIGINT", "revision"),
        *cols("TEXT", "token_hash", nullable=True),
        *cols("JSONB", "allowed_scope"),
        *cols("TIMESTAMPTZ", "issued_at", "expires_at"),
        *cols("TIMESTAMPTZ", "revoked_at", nullable=True),
        indexes=(
            Index(("scope_id", "token_hash"), True, "token_hash IS NOT NULL"),
            Index(("scope_id", "run_id", "expires_at")),
        ),
        checks=(
            Check(("issued_at", "expires_at"), "expires_at > issued_at"),
            Check(
                ("execution_id", "execution_generation"),
                "(execution_id IS NULL) = (execution_generation IS NULL)",
            ),
        ),
        refs=(("run_id", "run"), ("execution_id", "execution")),
    ),
    table(
        "timeline_item",
        *cols("UUID", "session_id"),
        *cols("UUID", "run_id", nullable=True),
        *cols("TEXT", "item_type", "content_ref", "status"),
        *cols("BIGINT", "final_revision", "source_seq"),
        indexes=(Index(("scope_id", "session_id", "source_seq")),),
        checks=(enum("status", "pending", "streaming", "completed", "failed", "cancelled"),),
        refs=(("session_id", "session"), ("run_id", "run")),
    ),
    # --- DS-P02-00 frozen additions -------------------------------------------------
    # Platform capacity lives outside the user scope: keyed by deployment, resource kind
    # and the capacity specification revision, never by a fabricated scope. Concurrency
    # is CAS on `version`; callers raise CAPACITY_UNAVAILABLE when the reservation would
    # exceed `capacity`.
    table(
        "system_capacity",
        *cols("TEXT", "deployment_id", "resource_kind", "resource_key"),
        *cols("BIGINT", "resource_version", "capacity", "reserved", "version"),
        scoped=False,
        primary=("deployment_id", "resource_kind", "resource_version", "resource_key"),
        checks=(
            enum("resource_kind", *RESOURCE_KINDS),
            Check(("reserved", "capacity"), "reserved <= capacity"),
        ),
    ),
    # Reservation facts: the aggregate counter alone cannot prove idempotent release,
    # crash recovery or expiry reclamation, so every reservation is a durable row with
    # its holder, request identity, amount, status and lease. Aggregates are only ever
    # changed in the same transaction that changes the matching reservation row.
    table(
        "system_capacity_reservation",
        *cols("TEXT", "deployment_id", "resource_kind", "resource_key"),
        *cols("TEXT", "status", "holder_ref", "request_id"),
        *cols("TEXT", "release_request_id", nullable=True),
        *cols("TEXT", "release_reason", nullable=True),
        *cols("TEXT", "release_request_hash", nullable=True),
        *cols("BIGINT", "resource_version", "amount", "version"),
        *cols("TIMESTAMPTZ", "lease_until"),
        *cols("TIMESTAMPTZ", "released_at", nullable=True),
        scoped=False,
        unique=(
            ("deployment_id", "resource_kind", "resource_version", "resource_key", "request_id"),
        ),
        indexes=(
            Index(
                ("deployment_id", "resource_kind", "resource_version", "resource_key", "status")
            ),
            Index(("status", "lease_until")),
        ),
        checks=(
            enum("resource_kind", *RESOURCE_KINDS),
            enum("status", "reserved", "released", "expired", "reclaimed"),
            Check(("amount",), "amount > 0"),
            Check(("status", "released_at"), "(status = 'reserved') = (released_at IS NULL)"),
            # Release facts: the first release request is persisted so a replay of the
            # same request is `reused` while a different request on a terminal
            # reservation is `rejected` (review C4).
            enum("release_reason", "settled", "cancelled", "expired", "reclaimed"),
            Check(
                ("release_request_hash",),
                "release_request_hash IS NULL OR length(release_request_hash) = 64",
            ),
            Check(
                ("status", "release_request_id", "release_reason"),
                "((status = 'reserved') = (release_request_id IS NULL)) AND "
                "((release_request_id IS NULL) = (release_reason IS NULL)) AND "
                "((status = 'released') = (release_reason IN ('settled','cancelled'))) AND "
                "((status = 'expired') = (release_reason = 'expired')) AND "
                "((status = 'reclaimed') = (release_reason = 'reclaimed'))",
            ),
        ),
        refs=(
            (
                ("deployment_id", "resource_kind", "resource_version", "resource_key"),
                "system_capacity",
            ),
        ),
    ),
    # Capability registry: a stable namespace+key identity plus immutable revisions.
    # Display names never participate in persistence or idempotency (they live in the
    # revision manifest), and at most one revision per capability may be `active`.
    table(
        "capability",
        *cols("TEXT", "namespace", "key", "kind"),
        unique=(("scope_id", "namespace", "key"),),
        checks=(enum("kind", *CAPABILITY_KINDS),),
    ),
    table(
        "capability_revision",
        *cols("UUID", "capability_id"),
        *cols("BIGINT", "revision"),
        *cols("TEXT", "content_digest", "status"),
        *cols("JSONB", "manifest"),
        unique=(("scope_id", "capability_id", "revision"),),
        indexes=(
            Index(
                ("scope_id", "capability_id"),
                True,
                "status = 'active'",
            ),
        ),
        checks=(
            enum("status", *CAPABILITY_STATUSES),
            Check(
                ("status", "content_digest"),
                "length(content_digest) = 64",
            ),
        ),
        refs=(("capability_id", "capability"),),
    ),
)

TABLES = tuple(
    replace(
        item,
        checks=(
            *item.checks,
            *(
                Check((column.name,), f"{column.name} > 0")
                for column in item.columns
                if column.name in {"schema_revision", "protocol_revision"}
            ),
            *(
                (enum("kind", *sorted(event_adapter.json_schema()["discriminator"]["mapping"])),)
                if item.name == "event"
                else ()
            ),
            *(
                (enum("kind", *sorted(message_adapter.json_schema()["discriminator"]["mapping"])),)
                if item.name == "agent_message"
                else ()
            ),
        ),
    )
    for item in _TABLES
)

TRANSACTION_RECORDS = {
    "T1": ("session", "turn", "run", "timeline_item", "work_item", "event", "outbox"),
    "T2": ("session_slot", "run", "work_item", "budget_allocation"),
    "T3": ("run", "model_attempt", "context_snapshot", "budget_allocation", "event", "outbox"),
    "T4": ("run", "event", "outbox"),
    "T5": ("run", "agent_message", "checkpoint", "event", "outbox"),
    "T6": ("run", "operation", "tool_call", "artifact", "checkpoint", "event", "outbox"),
    "T7": ("run", "work_item", "checkpoint", "budget_allocation", "event", "outbox"),
    "T8": ("run", "work_item", "agent_message", "event", "outbox"),
    "T9": ("run", "work_item", "timeline_item", "event", "outbox"),
    "T10": ("session_slot", "run", "checkpoint", "handoff", "work_item", "event", "outbox"),
}


def validate_storage(tables: tuple[Table, ...]) -> None:
    lookup = {item.name: item for item in tables}
    if len(lookup) != len(tables):
        raise ValueError("duplicate table")
    for item in tables:
        columns = {column.name: column for column in item.columns}
        required = {"created_at", "updated_at"}
        if "scope_id" in columns:
            required.add("scope_id")
        # Scope-less tables may only reference another scope-less aggregate through an
        # explicit composite key that matches its full primary key (checked below).
        if len(columns) != len(item.columns) or not required <= columns.keys():
            raise ValueError(f"duplicate or missing common column: {item.name}")
        if not item.primary_key or any(
            name not in columns or columns[name].nullable for name in item.primary_key
        ):
            raise ValueError(f"invalid primary key: {item.name}")
        for column in item.columns:
            if not column.comment:
                raise ValueError(f"missing column comment: {item.name}.{column.name}")
        constraints: tuple[Index | Check | ForeignKey, ...] = (
            *item.indexes,
            *item.checks,
            *item.foreign_keys,
        )
        for constraint in constraints:
            if not constraint.columns or not set(constraint.columns) <= columns.keys():
                raise ValueError(f"constraint references absent column: {item.name}")
        for fk in item.foreign_keys:
            target = lookup.get(fk.target)
            if target is None:
                raise ValueError(f"unresolved foreign key: {item.name}")
            if "scope_id" in columns:
                if fk.columns[0] != "scope_id" or fk.target_columns[0] != "scope_id":
                    raise ValueError(f"unscoped foreign key: {item.name}")
            elif fk.columns != tuple(target.primary_key) or fk.target_columns != tuple(
                target.primary_key
            ):
                raise ValueError(
                    f"scope-less foreign key must reference the full target primary key: "
                    f"{item.name}"
                )
            unique_keys = {
                target.primary_key,
                *(
                    index.columns
                    for index in target.indexes
                    if index.unique and index.predicate is None
                ),
            }
            if fk.target_columns not in unique_keys or len(fk.columns) != len(fk.target_columns):
                raise ValueError(f"foreign key lacks target unique key: {item.name}")
            target_cols = {column.name: column for column in target.columns}
            if any(
                columns[a].sql_type != target_cols[b].sql_type
                for a, b in zip(fk.columns, fk.target_columns, strict=True)
            ):
                raise ValueError(f"foreign key type mismatch: {item.name}")


def storage_document() -> str:
    validate_storage(TABLES)
    return (
        json.dumps(
            {
                "schema": "agent_runtime",
                "tables": [asdict(item) for item in TABLES],
                "transactions": TRANSACTION_RECORDS,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    content = storage_document()
    if args.check:
        if not args.path.exists() or args.path.read_text(encoding="utf-8") != content:
            parser.exit(1, "Storage specification missing or stale\n")
    else:
        args.path.parent.mkdir(parents=True, exist_ok=True)
        args.path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()

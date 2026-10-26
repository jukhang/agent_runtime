"""The one place that expresses the UoW lock order (DS-P02-02 requirement 2).

Fixed order, always:

    SessionSlot -> Run -> Authority -> WorkItem / Command / Mailbox
    -> Budget / Capacity -> Domain Resource

Every method in :mod:`agent_runtime.storage.uow` locks rows only through these helpers, so
no two transactions can pick up the same rows in opposite order. ``FOR UPDATE`` appears in
this module (plus the operation claim) only, and a package test asserts that.

The authority fence is **per run**, not per table (review §11.2 P1-2, Q46): the ``run`` row
already carried by the lock order holds ``authority_revision``, and the grant write port
must bump it while holding that same run row. No global ``LOCK TABLE`` is used, so two
unrelated scopes/runs never serialize against each other.

All statements are rendered against an explicit schema (review §10.2 P1-3).
"""

from dataclasses import dataclass
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.capacity import CapacityKey
from agent_runtime.storage.uow.schema import scoped

#: Documented order; refusals name this list when a caller is out of order.
LOCK_ORDER = (
    "session_slot",
    "run",
    "authority",
    "work_item",
    "command",
    "agent_message",
    "mailbox_cursor",
    "budget_allocation",
    "system_capacity",
    "system_capacity_reservation",
    "domain_resource",
)

#: Same-database tables this adapter may lock for a domain commit. The registry in
#: :mod:`agent_runtime.storage.uow.domain` adds the version column per resource type.
DOMAIN_TABLES: tuple[str, ...] = ("workspace", "projection", "budget_allocation")


@dataclass(frozen=True, slots=True)
class LockStatements:
    slot: sa.TextClause
    run: sa.TextClause
    run_session: sa.TextClause
    current_grant: sa.TextClause
    work: sa.TextClause
    live_works: sa.TextClause
    command: sa.TextClause
    command_by_key: sa.TextClause
    messages: sa.TextClause
    cursor: sa.TextClause
    capacity: sa.TextClause
    reservation: sa.TextClause
    reservation_by_request: sa.TextClause
    reserved_rows: sa.TextClause
    seals: sa.TextClause
    producer_deltas: sa.TextClause
    seal_by_producer: sa.TextClause
    seal_by_proof: sa.TextClause
    model_attempt_owner: sa.TextClause
    timeline_item_owner: sa.TextClause
    seal_insert: sa.TextClause
    work_finish_id: sa.TextClause
    work_finish_kind: sa.TextClause
    work_insert: sa.TextClause
    run_authority: sa.TextClause


def build_lock_statements(schema: str) -> LockStatements:
    """Render the row-lock statements for one explicit schema."""
    return LockStatements(
        slot=scoped(
            "SELECT session_id, owner_run_id, slot_version FROM {t}session_slot "
            "WHERE scope_id = :scope_id AND session_id = :session_id FOR UPDATE",
            schema,
        ),
        run=scoped(
            "SELECT * FROM {t}run WHERE scope_id = :scope_id AND id = :run_id FOR UPDATE",
            schema,
        ),
        run_session=scoped(
            "SELECT session_id FROM {t}run WHERE scope_id = :scope_id AND id = :run_id",
            schema,
        ),
        current_grant=scoped(
            "SELECT revision, expires_at, revoked_at, allowed_scope FROM {t}capability_grant "
            "WHERE scope_id = :scope_id AND run_id = :run_id AND actor_id = :actor_id "
            "AND execution_id IS NULL "
            "AND allowed_scope->>'kind' IS DISTINCT FROM 'credential_proxy' "
            "ORDER BY revision DESC, issued_at DESC, id LIMIT 1",
            schema,
        ),
        work=scoped(
            "SELECT * FROM {t}work_item WHERE scope_id = :scope_id AND id = :work_id FOR UPDATE",
            schema,
        ),
        live_works=scoped(
            "SELECT * FROM {t}work_item WHERE scope_id = :scope_id AND run_id = :run_id "
            "AND status IN ('pending', 'leased') FOR UPDATE",
            schema,
        ),
        command=scoped(
            "SELECT * FROM {t}command WHERE scope_id = :scope_id AND id = :command_id FOR UPDATE",
            schema,
        ),
        command_by_key=scoped(
            "SELECT * FROM {t}command WHERE scope_id = :scope_id AND run_id = :run_id "
            "AND actor_id = :actor_id AND request_key = :request_key FOR UPDATE",
            schema,
        ),
        messages=scoped(
            "SELECT * FROM {t}agent_message WHERE scope_id = :scope_id "
            "AND recipient_run_id = :run_id AND id = ANY(:message_ids) "
            "ORDER BY recipient_seq FOR UPDATE",
            schema,
        ),
        cursor=scoped(
            "SELECT * FROM {t}mailbox_cursor WHERE scope_id = :scope_id "
            "AND recipient_run_id = :run_id FOR UPDATE",
            schema,
        ),
        capacity=scoped(
            "SELECT * FROM {t}system_capacity WHERE deployment_id = :deployment_id "
            "AND resource_kind = :resource_kind AND resource_version = :resource_version "
            "AND resource_key = :resource_key FOR UPDATE",
            schema,
        ),
        reservation=scoped(
            "SELECT * FROM {t}system_capacity_reservation WHERE id = :reservation_id FOR UPDATE",
            schema,
        ),
        reservation_by_request=scoped(
            "SELECT * FROM {t}system_capacity_reservation WHERE deployment_id = :deployment_id "
            "AND resource_kind = :resource_kind AND resource_version = :resource_version "
            "AND resource_key = :resource_key AND request_id = :request_id FOR UPDATE",
            schema,
        ),
        reserved_rows=scoped(
            "SELECT id, amount, status FROM {t}system_capacity_reservation "
            "WHERE deployment_id = :deployment_id AND resource_kind = :resource_kind "
            "AND resource_version = :resource_version AND resource_key = :resource_key "
            "AND status = 'reserved' FOR UPDATE",
            schema,
        ),
        seals=scoped(
            "SELECT * FROM {t}output_seal WHERE scope_id = :scope_id AND run_id = :run_id "
            "ORDER BY sealed_revision, model_attempt_id, item_id",
            schema,
        ),
        producer_deltas=scoped(
            "SELECT count(*) FROM {t}event WHERE scope_id = :scope_id AND run_id = :run_id",
            schema,
        ),
        seal_by_producer=scoped(
            "SELECT * FROM {t}output_seal WHERE scope_id = :scope_id AND run_id = :run_id "
            "AND model_attempt_id = :model_attempt_id AND item_id = :item_id",
            schema,
        ),
        seal_by_proof=scoped(
            "SELECT * FROM {t}output_seal WHERE scope_id = :scope_id AND proof_id = :proof_id",
            schema,
        ),
        model_attempt_owner=scoped(
            "SELECT run_id FROM {t}model_attempt WHERE scope_id = :scope_id "
            "AND id = :model_attempt_id",
            schema,
        ),
        timeline_item_owner=scoped(
            "SELECT session_id, run_id FROM {t}timeline_item WHERE scope_id = :scope_id "
            "AND id = :item_id",
            schema,
        ),
        seal_insert=scoped(
            "INSERT INTO {t}output_seal (id, scope_id, session_id, run_id, model_attempt_id, "
            "item_id, proof_id, content_digest, analysis_digest, content_last_delta_index, "
            "content_last_session_seq, analysis_last_delta_index, analysis_last_session_seq, "
            "durable_seq, sealed_revision, sealed_at) VALUES (:id, :scope_id, :session_id, "
            ":run_id, :model_attempt_id, :item_id, :proof_id, :content_digest, "
            ":analysis_digest, :content_last_delta_index, :content_last_session_seq, "
            ":analysis_last_delta_index, :analysis_last_session_seq, :durable_seq, "
            ":sealed_revision, :sealed_at)",
            schema,
        ),
        work_finish_id=scoped(
            "UPDATE {t}work_item SET status = :status, owner_id = NULL, lease_until = NULL, "
            "lease_epoch = lease_epoch + 1, updated_at = :now "
            "WHERE scope_id = :scope_id AND id = :work_id",
            schema,
        ),
        work_finish_kind=scoped(
            "UPDATE {t}work_item SET status = :status, owner_id = NULL, lease_until = NULL, "
            "lease_epoch = lease_epoch + 1, updated_at = :now "
            "WHERE scope_id = :scope_id AND run_id = :run_id AND kind = :kind "
            "AND status IN ('pending', 'leased')",
            schema,
        ),
        work_insert=scoped(
            "INSERT INTO {t}work_item (id, scope_id, run_id, subject_ref, kind, dedup_key, "
            "status, payload_ref, wake_reason, priority, lease_epoch, attempt_count, "
            "available_at, deadline_at) VALUES (:id, :scope_id, :run_id, "
            "CAST(:subject_ref AS jsonb), :kind, :dedup_key, 'pending', :payload_ref, "
            ":wake_reason, :priority, 0, 0, :now, :deadline)",
            schema,
        ),
        run_authority=scoped(
            "SELECT authority_revision FROM {t}run WHERE scope_id = :scope_id AND id = :run_id",
            schema,
        ),
    )


def capacity_key_params(key: CapacityKey) -> dict[str, object]:
    return {
        "deployment_id": key.deployment_id,
        "resource_kind": key.resource_kind,
        "resource_version": int(key.resource_version),
        "resource_key": key.resource_key,
    }


async def lock_slot(
    session: AsyncSession, sql: LockStatements, scope_id: str, session_id: UUID
) -> dict[str, Any] | None:
    result = await session.execute(sql.slot, {"scope_id": scope_id, "session_id": session_id})
    row = result.mappings().one_or_none()
    return None if row is None else dict(row)


async def lock_run(
    session: AsyncSession, sql: LockStatements, scope_id: str, run_id: UUID
) -> dict[str, Any] | None:
    result = await session.execute(sql.run, {"scope_id": scope_id, "run_id": run_id})
    row = result.mappings().one_or_none()
    return None if row is None else dict(row)


async def run_session(
    session: AsyncSession, sql: LockStatements, scope_id: str, run_id: UUID
) -> UUID | None:
    """Non-locking read of the run's session, used to resolve the slot *before* locking it.

    The fixed order is Slot -> Run, so a caller that only knows the run id resolves the
    session here, locks that slot, then locks the run and verifies the run still names the
    same session - the run row is the authority for a concurrent session move.
    """
    result = await session.execute(sql.run_session, {"scope_id": scope_id, "run_id": run_id})
    return result.scalar_one_or_none()


async def current_grant(
    session: AsyncSession, sql: LockStatements, scope_id: str, run_id: UUID, actor_id: str
) -> dict[str, Any] | None:
    result = await session.execute(
        sql.current_grant, {"scope_id": scope_id, "run_id": run_id, "actor_id": actor_id}
    )
    row = result.mappings().one_or_none()
    return None if row is None else dict(row)


async def lock_work_item(
    session: AsyncSession, sql: LockStatements, scope_id: str, work_id: UUID
) -> dict[str, Any] | None:
    result = await session.execute(sql.work, {"scope_id": scope_id, "work_id": work_id})
    row = result.mappings().one_or_none()
    return None if row is None else dict(row)


async def lock_live_works(
    session: AsyncSession, sql: LockStatements, scope_id: str, run_id: UUID
) -> list[dict[str, Any]]:
    """Every live work item of one run, in the lock order."""
    result = await session.execute(sql.live_works, {"scope_id": scope_id, "run_id": run_id})
    return [dict(row) for row in result.mappings()]


async def lock_command(
    session: AsyncSession, sql: LockStatements, scope_id: str, command_id: UUID
) -> dict[str, Any] | None:
    result = await session.execute(sql.command, {"scope_id": scope_id, "command_id": command_id})
    row = result.mappings().one_or_none()
    return None if row is None else dict(row)


async def lock_command_by_key(
    session: AsyncSession,
    sql: LockStatements,
    scope_id: str,
    run_id: UUID,
    actor_id: str,
    request_key: str,
) -> dict[str, Any] | None:
    result = await session.execute(
        sql.command_by_key,
        {
            "scope_id": scope_id,
            "run_id": run_id,
            "actor_id": actor_id,
            "request_key": request_key,
        },
    )
    row = result.mappings().one_or_none()
    return None if row is None else dict(row)


async def lock_messages(
    session: AsyncSession,
    sql: LockStatements,
    scope_id: str,
    run_id: UUID,
    message_ids: tuple[UUID, ...],
) -> list[dict[str, Any]]:
    result = await session.execute(
        sql.messages,
        {"scope_id": scope_id, "run_id": run_id, "message_ids": list(message_ids)},
    )
    return [dict(row) for row in result.mappings()]


async def lock_mailbox_cursor(
    session: AsyncSession, sql: LockStatements, scope_id: str, run_id: UUID
) -> dict[str, Any] | None:
    result = await session.execute(sql.cursor, {"scope_id": scope_id, "run_id": run_id})
    row = result.mappings().one_or_none()
    return None if row is None else dict(row)


async def lock_capacity(
    session: AsyncSession, sql: LockStatements, key: CapacityKey
) -> dict[str, Any] | None:
    result = await session.execute(sql.capacity, capacity_key_params(key))
    row = result.mappings().one_or_none()
    return None if row is None else dict(row)


async def lock_reservation(
    session: AsyncSession, sql: LockStatements, reservation_id: UUID
) -> dict[str, Any] | None:
    result = await session.execute(sql.reservation, {"reservation_id": reservation_id})
    row = result.mappings().one_or_none()
    return None if row is None else dict(row)


async def lock_reservation_by_request(
    session: AsyncSession, sql: LockStatements, key: CapacityKey, request_id: str
) -> dict[str, Any] | None:
    result = await session.execute(
        sql.reservation_by_request, {**capacity_key_params(key), "request_id": request_id}
    )
    row = result.mappings().one_or_none()
    return None if row is None else dict(row)


async def lock_reserved_rows(
    session: AsyncSession, sql: LockStatements, key: CapacityKey
) -> list[dict[str, Any]]:
    result = await session.execute(sql.reserved_rows, capacity_key_params(key))
    return [dict(row) for row in result.mappings()]


async def lock_domain_resource(
    session: AsyncSession, schema: str, table: str, scope_id: str, resource_id: str
) -> dict[str, Any] | None:
    """Lock one registered same-database resource row (table names are whitelisted)."""
    if table not in DOMAIN_TABLES:
        raise AssertionError(f"domain table is not whitelisted: {table}")
    statement = scoped(
        f"SELECT * FROM {{t}}{table} WHERE scope_id = :scope_id AND id = :resource_id FOR UPDATE",
        schema,
    )
    result = await session.execute(statement, {"scope_id": scope_id, "resource_id": resource_id})
    row = result.mappings().one_or_none()
    return None if row is None else dict(row)


__all__ = [
    "DOMAIN_TABLES",
    "LOCK_ORDER",
    "LockStatements",
    "build_lock_statements",
    "capacity_key_params",
    "current_grant",
    "lock_capacity",
    "lock_command",
    "lock_command_by_key",
    "lock_domain_resource",
    "lock_live_works",
    "lock_mailbox_cursor",
    "lock_messages",
    "lock_reservation",
    "lock_reservation_by_request",
    "lock_reserved_rows",
    "lock_run",
    "lock_slot",
    "lock_work_item",
]

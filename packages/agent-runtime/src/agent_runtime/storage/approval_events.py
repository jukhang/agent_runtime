"""Approval row transitions and their Journal/Outbox records share one transaction."""

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from agent_runtime.contracts.payloads import ApprovalUpdated
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.state_events import (
    ApprovalUpdatedDraft,
    ServiceEventAuthority,
    StateEventBatch,
)
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.journal import SqlJournal


async def append_approval_update(
    db: AsyncSession,
    engine: AsyncEngine,
    schema: str,
    actor: ActorContext,
    run: Mapping[str, Any],
    row: Mapping[str, Any],
    *,
    status: str,
    revision: int,
    now: datetime,
    reason: str,
) -> None:
    source = ResourceRef(
        resource_type="approval",
        resource_id=str(row["id"]),
        resource_version=str(revision),
        scope=ResourceScope(scope_id=actor.scope_id, session_id=str(run["session_id"])),
    )
    payload = ApprovalUpdated.model_validate(
        {
            "approval_ref": source,
            "revision": str(revision),
            "status": status,
            "expires_at": row["expires_at"],
            "public_reason": reason,
        }
    )
    await SqlJournal(engine, schema, actor).append_state_in_transaction(
        db,
        StateEventBatch(
            authority=ServiceEventAuthority(
                kind="service",
                actor=actor,
                purpose="approval",
                source=source,
                transaction_id=uuid4(),
            ),
            session_id=run["session_id"],
            trace_id=f"approval:{row['id']}:{revision}",
            events=(
                ApprovalUpdatedDraft(
                    kind="approval.updated",
                    producer_key=f"approval:{row['id']}:{revision}",
                    occurred_at=now,
                    turn_id=run["turn_id"],
                    run_id=run["id"],
                    parent_run_id=run["parent_run_id"],
                    step_id=None,
                    item_id=None,
                    model_attempt_id=None,
                    payload=payload,
                ),
            ),
        ),
    )

"""Shared child execution fence; locking a slot does not confer slot ownership."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.ownership import ChildCommitGuard
from agent_runtime.storage.uow import errors, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import scoped


async def require_child(
    db: AsyncSession,
    schema: str,
    guard: ChildCommitGuard,
    *,
    now: datetime,
    allow_control: bool = False,
) -> dict[str, Any]:
    lease = guard.lease
    statements = locks.build_lock_statements(schema)
    if min(lease.run_lease_until, lease.work_lease_until) <= now:
        errors.fenced("child lease expired")
    await locks.lock_slot(db, statements, lease.scope_id, lease.session_id)
    rows = {}
    for identity in sorted((lease.run_id, guard.root_run_id), key=str):
        rows[identity] = await locks.lock_run(db, statements, lease.scope_id, identity)
    child, root = rows[lease.run_id], rows[guard.root_run_id]
    if child is None or root is None:
        errors.fenced("child or root disappeared")
    if (
        child["root_run_id"] != root["id"]
        or child["parent_run_id"] != root["id"]
        or root["parent_run_id"] is not None
        or root["root_run_id"] != root["id"]
        or child["session_id"] != lease.session_id
        or root["session_id"] != lease.session_id
        or child["owner_id"] != lease.owner_id
        or child["owner_epoch"] != int(lease.run_epoch)
        or child["lease_until"] is None
        or child["lease_until"] <= now
    ):
        errors.fenced("child execution identity changed")
    if child["state_version"] != int(guard.expected_state_version):
        errors.version_conflict("child state changed")
    if root["cancel_version"] != int(guard.expected_root_cancel_version):
        errors.control_pending("root cancellation changed")
    if child["status"] in {"completed", "failed", "cancelled"}:
        errors.version_conflict("child is terminal")
    if not allow_control and (
        child["status"] != "running"
        or child["desired_control"] is not None
        or root["status"] in {"completed", "failed", "cancelled", "cancelling", "paused"}
        or root["desired_control"] == "cancel"
        or child["pending_input_version"] != child["applied_input_version"]
    ):
        errors.control_pending("child execution cannot advance")
    if not allow_control and min(child["deadline_at"], root["deadline_at"]) <= now:
        raise runtime_failure(
            "BUDGET_EXHAUSTED",
            category="budget",
            public_message="Delegated Run deadline expired.",
            deadline_at=min(child["deadline_at"], root["deadline_at"]),
        )
    await revalidate_authority(db, statements, guard.actor, run=child, now=now)
    await revalidate_authority(db, statements, guard.actor, run=root, now=now)
    work = await locks.lock_work_item(db, statements, lease.scope_id, lease.work_id)
    if (
        work is None
        or work["run_id"] != lease.run_id
        or work["status"] != "leased"
        or work["owner_id"] != lease.owner_id
        or work["lease_epoch"] != int(lease.work_epoch)
        or work["lease_until"] is None
        or work["lease_until"] <= now
    ):
        errors.fenced("child work lease changed")
    try:
        workspace_id = UUID(guard.workspace_ref.resource_id)
    except ValueError:
        errors.invalid_input("child workspace identity must be a UUID")
    row = (
        (
            await db.execute(
                scoped(
                    "SELECT owner_run_id,generation,revision FROM {t}workspace "
                    "WHERE scope_id=:scope AND id=:id FOR UPDATE",
                    schema,
                ),
                {"scope": lease.scope_id, "id": workspace_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        row is None
        or row["owner_run_id"] != lease.run_id
        or row["generation"] != int(guard.workspace_generation)
        or row["revision"] != int(guard.workspace_ref.resource_version or "0")
    ):
        errors.version_conflict("child workspace changed")
    return dict(child)


__all__ = ["require_child"]

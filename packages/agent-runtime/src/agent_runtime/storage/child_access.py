"""Apply a child grant before reading any versioned source bytes."""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.references import CapabilityRef, ResourceRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import scoped


async def require_child_resources(
    session: AsyncSession,
    schema: str,
    actor: ActorContext,
    run_id: UUID,
    refs: tuple[ResourceRef, ...],
    tools: tuple[CapabilityRef, ...] = (),
) -> None:
    row = (
        (
            await session.execute(
                scoped(
                    "SELECT r.parent_run_id,r.authority_revision,g.revision,"
                    "g.allowed_scope,g.revoked_at,"
                    "g.expires_at,CURRENT_TIMESTAMP AS now FROM {t}run r "
                    "LEFT JOIN {t}capability_grant g ON g.scope_id=r.scope_id AND g.run_id=r.id "
                    "AND g.actor_id=:actor AND g.revision=r.authority_revision "
                    "AND g.execution_id IS NULL "
                    "AND g.allowed_scope->>'kind' IS DISTINCT FROM 'credential_proxy' "
                    "WHERE r.scope_id=:scope AND r.id=:run ORDER BY g.issued_at DESC,g.id LIMIT 1",
                    schema,
                ),
                {"actor": actor.actor_id, "scope": actor.scope_id, "run": run_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        errors.forbidden("source run does not exist")
    if row["parent_run_id"] is None:
        return
    allowed = row["allowed_scope"]
    if (
        row["revision"] != int(actor.auth_revision)
        or row["revoked_at"] is not None
        or row["expires_at"] <= row["now"]
        or not isinstance(allowed, dict)
        or any(
            tool.model_dump(mode="json", by_alias=True) not in allowed.get("tools", [])
            for tool in tools
        )
    ):
        errors.forbidden("source is outside the child capability grant")
    for ref in refs:
        if ref.model_dump(mode="json", by_alias=True) in allowed.get("resources", []):
            continue
        if ref.scope.scope_id != actor.scope_id or ref.resource_version != "1":
            errors.forbidden("child output resource has another scope or version")
        try:
            identity = UUID(ref.resource_id)
        except ValueError:
            errors.forbidden("child output resource has no stable identity")
        if ref.resource_type == "tool_call_result":
            sql = (
                "SELECT id FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run "
                "AND id=:id AND result_ref IS NOT NULL "
                "AND status IN ('completed','failed','cancelled','not_executed')"
            )
        elif ref.resource_type == "timeline_item":
            sql = (
                "SELECT i.id FROM {t}timeline_item i JOIN {t}output_seal s "
                "ON s.scope_id=i.scope_id AND s.run_id=i.run_id AND s.item_id=i.id "
                "JOIN {t}model_attempt m ON m.scope_id=s.scope_id AND m.id=s.model_attempt_id "
                "WHERE i.scope_id=:scope AND i.run_id=:run AND i.id=:id "
                "AND i.status='completed' AND i.final_revision=1 AND s.sealed_revision=1 "
                "AND m.run_id=i.run_id AND m.status='completed'"
            )
        else:
            errors.forbidden("source is outside the child capability grant")
        owned = await session.execute(
            scoped(sql, schema), {"scope": actor.scope_id, "run": run_id, "id": identity}
        )
        if owned.first() is None:
            errors.forbidden("output is not owned and finalized by this child")


__all__ = ["require_child_resources"]

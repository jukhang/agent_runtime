"""Authority revalidation against stored grants (Q46: per-run fence, no table lock).

The host issues a grant revision per ``(scope, run, actor)``. A request only carries an
*expected* ``auth_revision``; the transaction re-reads the run's ``authority_revision``
and the current grant, and refuses when either moved.

The fence is the **run row**, which every state transaction already locks in the fixed
order (SessionSlot -> Run -> Authority -> ...). Grant writers must therefore update grants
while holding that same run row and bump ``run.authority_revision`` in the same
transaction; a direct administrative ``UPDATE`` that bypasses the port is outside the
Runtime concurrency guarantee (Q46). This replaces the previous table-wide
``SHARE ROW EXCLUSIVE`` lock, which serialized unrelated scopes and runs.
"""

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.uow import errors, locks


async def revalidate_authority(
    session: AsyncSession,
    sql: locks.LockStatements,
    actor: ActorContext,
    *,
    run: Mapping[str, Any],
    now: datetime,
) -> int:
    """Return the effective authority revision after proving the caller's claim is current."""
    fence = int(run["authority_revision"])
    if fence != int(actor.auth_revision):
        errors.forbidden("the run authority revision advanced past the request")
    row = await locks.current_grant(session, sql, actor.scope_id, run["id"], actor.actor_id)
    if row is None:
        errors.forbidden("no capability grant is registered for this actor and run")
    if row["revoked_at"] is not None:
        errors.forbidden("the capability grant was revoked")
    if row["expires_at"] <= now:
        errors.forbidden("the capability grant expired")
    if int(row["revision"]) != fence:
        errors.forbidden("the capability grant no longer matches the run authority fence")
    return fence


__all__ = ["revalidate_authority"]

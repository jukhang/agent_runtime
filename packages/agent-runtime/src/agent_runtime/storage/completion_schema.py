"""Candidate reference menus derived from current SQL facts and delivered context only."""

from collections.abc import Mapping
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.context import PreparedContext
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.storage.tool_recovery_evidence import UNSUCCESSFUL_TOOL_STATES
from agent_runtime.storage.uow.schema import scoped


async def completion_recovery_choices(
    db: AsyncSession, schema: str, request: ProviderRequest, snapshot: PreparedContext
) -> dict[str, str]:
    visible = sorted(
        {
            source.resource.resource_id
            for source in snapshot.sources
            if source.resource.resource_type == "tool_call_result"
            and source.disposition in {"included", "summarized"}
        }
    )
    if not visible:
        return {}
    from uuid import UUID

    identities = [UUID(identity) for identity in visible]
    rows = await db.execute(
        scoped(
            "SELECT id,status FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run "
            "AND id=ANY(:ids)",
            schema,
        ),
        {"scope": request.scope_id, "run": request.run_id, "ids": identities},
    )
    return {str(row["id"]): row["status"] for row in rows.mappings()}


def bind_completion_recoveries(schema: dict[str, Any], calls: Mapping[str, str]) -> None:
    """Narrow proposals, without certifying semantic recovery, ordering or effect safety."""
    failed = sorted(
        identity for identity, status in calls.items() if status in UNSUCCESSFUL_TOOL_STATES
    )
    completed = sorted(identity for identity, status in calls.items() if status == "completed")
    payload = schema["$defs"]["FinishPayload"]["properties"]
    assessment = schema["$defs"]["RecoveredToolFailure"]["properties"]
    payload["recoveredToolFailures"]["maxItems"] = min(len(failed), 128)
    assessment["failedToolCallId"] = {"type": "string", "enum": failed} if failed else False
    assessment["recoveryToolCallIds"]["items"] = (
        {"type": "string", "enum": completed} if completed else False
    )

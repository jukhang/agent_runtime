"""Final SQL budget and approval checks for a pinned tool invocation."""

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.policy import PolicyAllow
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.tools import ToolInvocation, ToolSpec
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlToolExecutionGate:
    """Recheck limits and approval inside every tool effect transaction.

    The host supplies the pinned policy requirement. A grant reference is only
    evidence; an approval must still be live and bound to this exact operation.
    """

    def __init__(
        self,
        schema: str,
        *,
        approval_required: Callable[
            [ToolInvocation, ToolSpec, tuple[ResourceRef, ...]], bool
        ],
    ) -> None:
        self._schema = require_schema(schema)
        self._approval_required = approval_required

    @property
    def schema(self) -> str:
        return self._schema

    async def verify(
        self,
        db: AsyncSession,
        invocation: ToolInvocation,
        grant: PolicyAllow,
        spec: ToolSpec,
        resources: tuple[ResourceRef, ...],
    ) -> bool:
        params = {"scope": invocation.actor.scope_id, "run": invocation.run_id}
        run = (
            (
                await db.execute(
                    scoped(
                        "SELECT parent_run_id,deadline_at FROM {t}run "
                        "WHERE scope_id=:scope AND id=:run FOR UPDATE",
                        self._schema,
                    ),
                    params,
                )
            )
            .mappings()
            .one_or_none()
        )
        if run is None or run["deadline_at"] <= datetime.now(UTC):
            errors.forbidden("tool run is missing or past its deadline")
        if run["parent_run_id"] is None:
            budget_query = (
                "SELECT b.limits,b.settled,b.reserved FROM {t}budget_allocation b "
                "WHERE b.scope_id=:scope AND b.root_run_id=:run AND b.parent_id IS NULL "
                "FOR UPDATE"
            )
        else:
            budget_query = (
                "SELECT b.limits,b.settled,b.reserved FROM {t}budget_allocation b "
                "WHERE b.scope_id=:scope AND b.root_run_id=:root AND b.parent_id IS NOT NULL "
                "AND EXISTS (SELECT 1 FROM {t}agent_message m "
                "WHERE m.scope_id=b.scope_id AND m.recipient_run_id=:run "
                "AND m.sender_run_id=:root AND m.kind='task.assign' "
                "AND m.payload->'payload'->>'budgetAllocationId'=CAST(b.id AS text)) "
                "FOR UPDATE"
            )
            params["root"] = run["parent_run_id"]
        allocation = (
            (await db.execute(scoped(budget_query, self._schema), params))
            .mappings()
            .one_or_none()
        )
        if allocation is None:
            errors.forbidden("tool has no assigned budget")
        count = int(
            (
                await db.execute(
                    scoped(
                        "SELECT count(*) FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run",
                        self._schema,
                    ),
                    params,
                )
            ).scalar_one()
        )
        limit = int(allocation["limits"].get("tool_calls", 0))
        spent = int(allocation["settled"].get("tool_calls", 0))
        held = int(allocation["reserved"].get("tool_calls", 0))
        consumed = count + spent if run["parent_run_id"] is None else max(count, spent)
        if consumed + held > limit:
            raise runtime_failure(
                "BUDGET_EXHAUSTED",
                category="budget",
                public_message="The tool call budget was exhausted before execution.",
            )

        if not self._approval_required(invocation, spec, resources):
            return False
        if invocation.operation_id is None:
            errors.forbidden("approval-required tool has no operation identity")
        parts = str(grant.grant_ref).split(":")
        if len(parts) != 3 or parts[0] != "approval":
            errors.forbidden("tool requires a persisted approval grant")
        try:
            approval_id, revision = UUID(parts[1]), int(parts[2])
        except (ValueError, OverflowError):
            errors.forbidden("approval grant identity is invalid")
        approval = (
            (
                await db.execute(
                    scoped(
                        "SELECT * FROM {t}approval WHERE scope_id=:scope AND id=:id FOR UPDATE",
                        self._schema,
                    ),
                    {"scope": invocation.actor.scope_id, "id": approval_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if (
            approval is None
            or approval["status"] != "approved"
            or approval["decision"] != "approved"
            or approval["expires_at"] <= datetime.now(UTC)
            or approval["revision"] != revision
            or approval["run_id"] != invocation.run_id
            or (
                approval["operation_id"] is not None
                and approval["operation_id"] != invocation.operation_id
            )
            or approval["request_key"] != f"tool:{invocation.operation_id}"
            or approval["actor_id"] != invocation.actor.actor_id
            or str(approval["auth_revision"]) != invocation.actor.auth_revision
            or approval["input_hash"] != content_digest(invocation.call.arguments)
            or approval["tool_revision"] != spec.ref.model_dump(mode="json", by_alias=True)
            or approval["policy_ref"] != spec.approval_policy.model_dump(mode="json", by_alias=True)
            or approval["resource_refs"]
            != [ref.model_dump(mode="json", by_alias=True) for ref in resources]
        ):
            errors.forbidden("approval is stale or differs from the tool operation")
        return True


__all__ = ["SqlToolExecutionGate"]

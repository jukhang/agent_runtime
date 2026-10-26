"""Host approval reads use current SQL authority, not stale projected buttons."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from agent_runtime.contracts.tools import ApprovalDecision
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.approvals import SqlApprovalService
from agent_runtime.storage.host_tool_access import SqlHostToolAccess
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import scoped
from centn_example.resources import Resources, Sources


class Approvals:
    def __init__(self, resources: Resources, sources: Sources) -> None:
        self.resources, self.sources = resources, sources

    async def list(self, actor: ActorContext, session_id: UUID) -> list[dict[str, Any]]:
        return await self._read(actor, session_id)

    async def _read(
        self, actor: ActorContext, session_id: UUID, approval_id: UUID | None = None
    ) -> list[dict[str, Any]]:
        async with self.resources.sessions() as db, db.begin():
            await self.sources.authorize_session_in_transaction(db, actor, session_id)
            rows = (
                (
                    await db.execute(
                        scoped(
                            "SELECT a.id,a.status,a.revision,a.run_id,a.expires_at,a.tool_revision "
                            "FROM {t}approval a JOIN {t}run r "
                            "ON r.scope_id=a.scope_id AND r.id=a.run_id "
                            "WHERE a.scope_id=:scope AND a.actor_id=:actor "
                            "AND r.session_id=:session "
                            + ("AND a.id=:approval " if approval_id is not None else "")
                            + "ORDER BY (a.status='pending') DESC,"
                            "a.created_at DESC,a.id DESC LIMIT 100",
                            self.resources.runtime_schema,
                        ),
                        {
                            "scope": actor.scope_id,
                            "actor": actor.actor_id,
                            "session": session_id,
                            "approval": approval_id,
                        },
                    )
                )
                .mappings()
                .all()
            )
            return [
                {
                    "id": str(row["id"]),
                    "runId": str(row["run_id"]),
                    "status": row["status"],
                    "revision": str(row["revision"]),
                    "expiresAt": row["expires_at"].isoformat(),
                    "tool": row["tool_revision"],
                }
                for row in rows
            ]

    async def resolve(
        self, actor: ActorContext, session_id: UUID, decision: ApprovalDecision
    ) -> dict[str, Any]:
        rows = await self._read(actor, session_id, decision.approval_id)
        row = next((row for row in rows if row["id"] == str(decision.approval_id)), None)
        if row is None:
            errors.forbidden("Approval is not in this Session")
        canonical = (
            f"approval:{decision.approval_id}:{decision.expected_version}:{decision.decision}"
        )
        if decision.idempotency_key != canonical:
            errors.invalid_input("Approval request key is not canonical")
        if (
            row["status"] == decision.decision
            and int(row["revision"]) == int(decision.expected_version) + 1
        ):
            # Only the request actor can decide approvals in this single-owner host.
            return {key: row[key] for key in ("id", "runId", "status", "revision")}
        service = SqlApprovalService(
            self.resources.engine,
            self.resources.runtime_schema,
            access=SqlHostToolAccess(self.resources.runtime_schema, actor, self.sources),
        )
        result = await service.resolve(actor, decision)
        return {
            "id": str(result.approval_id),
            "runId": str(result.run_id),
            "status": result.status,
            "revision": str(result.revision),
        }

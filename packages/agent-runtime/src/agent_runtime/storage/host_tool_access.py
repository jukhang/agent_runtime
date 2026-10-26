"""Transactional host authorization shared by ToolHost and approval requests."""

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.policy import PolicyRequest
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.tools import ToolInvocation, ToolSpec
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.storage.context_access import TransactionalResourceAccess
from agent_runtime.storage.uow import errors, facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema


class SqlHostToolAccess:
    """Recheck the active root run, current grant and exact resource versions."""

    def __init__(
        self,
        schema: str,
        actor: ActorContext,
        resources: TransactionalResourceAccess,
        *,
        lease: Callable[[], LeaseToken] | None = None,
    ) -> None:
        self._schema = require_schema(schema)
        self._locks = locks.build_lock_statements(self._schema)
        self._actor, self._resources = actor, resources
        self._lease = lease

    async def authorize(
        self,
        db: AsyncSession,
        invocation: ToolInvocation,
        resources: tuple[ResourceRef, ...],
    ) -> None:
        now = datetime.now(UTC)
        if invocation.actor != self._actor or invocation.deadline_at <= now:
            errors.forbidden("tool actor or deadline changed")
        slot = await locks.lock_slot(
            db, self._locks, self._actor.scope_id, invocation.session_id
        )
        run = await locks.lock_run(
            db, self._locks, self._actor.scope_id, invocation.run_id
        )
        if (
            slot is None or run is None
            or slot["owner_run_id"] != invocation.run_id
            or run["session_id"] != invocation.session_id
            or run["parent_run_id"] is not None
            or run["status"] != "running"
            or run["desired_control"] is not None
            or run["deadline_at"] <= now
            or run["lease_until"] is None
            or run["lease_until"] <= now
        ):
            errors.forbidden("tool root run is not current and active")
        await revalidate_authority(db, self._locks, self._actor, run=run, now=now)
        if self._lease is not None:
            current = self._lease()
            if (
                current.scope_id != self._actor.scope_id
                or current.run_id != invocation.run_id
                or current.session_id != invocation.session_id
            ):
                errors.fenced("tool invocation differs from its claimed lease")
            facts.verify_lease(current, now=now)
            facts.verify_slot(slot, current)
            facts.verify_run(run, self._actor, current, now=now)
            facts.verify_work(
                await locks.lock_work_item(
                    db, self._locks, self._actor.scope_id, current.work_id
                ),
                current,
                now=now,
            )
        grant = await locks.current_grant(
            db, self._locks, self._actor.scope_id, invocation.run_id, self._actor.actor_id
        )
        allowed = None if grant is None else grant["allowed_scope"]
        if (
            not isinstance(allowed, dict)
            or invocation.call.tool.model_dump(mode="json", by_alias=True)
            not in allowed.get("tools", [])
        ):
            errors.forbidden("tool is not in the active capability grant")
        await self._resources.authorize_session_in_transaction(
            db, self._actor, invocation.session_id
        )
        for ref in resources:
            await self._resources.authorize_resource_in_transaction(
                db, self._actor, ref, session_id=invocation.session_id
            )

    async def require_request(
        self, db: AsyncSession, request: PolicyRequest, spec: ToolSpec
    ) -> None:
        if spec.ref != request.invocation.call.tool:
            errors.forbidden("approval tool differs from the installed specification")
        await self.authorize(db, request.invocation, request.resources)

    async def require_decision(
        self,
        db: AsyncSession,
        approver: ActorContext,
        request_actor: str,
        run_id: UUID,
        session_id: UUID,
        resources: tuple[ResourceRef, ...],
    ) -> None:
        if approver != self._actor or approver.actor_id != request_actor:
            errors.forbidden("approval belongs to another actor")
        run = await locks.lock_run(db, self._locks, approver.scope_id, run_id)
        if (
            run is None
            or run["session_id"] != session_id
            or run["status"] != "waiting_user"
            or run["desired_control"] is not None
            or run["deadline_at"] <= datetime.now(UTC)
        ):
            errors.forbidden("approval run is no longer waiting for this actor")
        await revalidate_authority(
            db, self._locks, approver, run=run, now=datetime.now(UTC)
        )
        await self._resources.authorize_session_in_transaction(
            db, approver, session_id
        )
        for ref in resources:
            await self._resources.authorize_resource_in_transaction(
                db, approver, ref, session_id=session_id
            )


__all__ = ["SqlHostToolAccess"]

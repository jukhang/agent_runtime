"""Lease-fenced live-run check for model decisions and tool dispatch."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.context_runtime import ContextAccessVerifier, ContextRuntime
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.loop import RunState
from agent_runtime.contracts.ownership import ChildCommitGuard, ChildLeaseToken
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.storage.child_authority import require_child
from agent_runtime.storage.uow import errors, facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped

Clock = Callable[[], datetime]


class SqlLiveRunGuard:
    """Recheck a claimed worker and its decision without releasing execution ownership.

    This is a preflight fence, not a substitute for the Tool Runtime's transactional
    grant, resource-version and idempotency checks at the effect boundary.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        lease: LeaseToken | ChildLeaseToken | Callable[[], LeaseToken | ChildLeaseToken],
        context: ContextRuntime,
        access: ContextAccessVerifier,
        clock: Clock = lambda: datetime.now(UTC),
        child_guard: Callable[[], ChildCommitGuard] | None = None,
    ) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._locks = locks.build_lock_statements(require_schema(schema))
        self._checkpoint = scoped(
            "SELECT state_ref FROM {t}checkpoint WHERE scope_id = :scope_id "
            "AND id = :checkpoint_id AND run_id = :run_id",
            schema,
        )
        self._actor = actor
        self._lease_source = lease if callable(lease) else lambda: lease
        self._context = context
        self._access = access
        self._clock = clock
        self._child_guard = child_guard
        self._schema = schema

    async def require_running(self, state: RunState, frame: DecisionFrame) -> None:
        lease = self._lease_source()
        if (
            frame.actor != self._actor
            or state.scope_id != lease.scope_id
            or state.session_id != lease.session_id
            or state.run_id != lease.run_id
            or frame.context.scope_id != lease.scope_id
            or frame.context.session_id != lease.session_id
            or frame.context.run_id != lease.run_id
        ):
            errors.forbidden("decision, actor and worker claim do not share an owner")
        now = self._clock()
        async with self._sessions() as session, session.begin():
            if isinstance(lease, ChildLeaseToken):
                if self._child_guard is None:
                    errors.forbidden("child live check requires explicit authority")
                guard = self._child_guard()
                if guard.actor != self._actor or guard.lease != lease:
                    errors.forbidden("child live guard changed owner or lease")
                run = await require_child(session, self._schema, guard, now=now)
            else:
                facts.verify_lease(lease, now=now)
                slot = await locks.lock_slot(session, self._locks, lease.scope_id, lease.session_id)
                facts.verify_slot(slot, lease)
                run = dict(
                    facts.verify_run(
                        await locks.lock_run(session, self._locks, lease.scope_id, lease.run_id),
                        self._actor,
                        lease,
                        now=now,
                    )
                )
                await revalidate_authority(session, self._locks, self._actor, run=run, now=now)
                facts.verify_work(
                    await locks.lock_work_item(session, self._locks, lease.scope_id, lease.work_id),
                    lease,
                    now=now,
                )
            if (
                run["status"] != "running"
                or run["desired_control"] is not None
                or int(run["state_version"]) != int(state.state_version)
                or int(run["cancel_version"]) != int(state.cancel_version)
                or int(run["applied_input_version"]) != int(state.applied_input_seq)
                or int(run["pending_input_version"]) != int(state.pending_input_seq)
            ):
                errors.version_conflict("run changed since the decision was prepared")
            checkpoint_id = run["checkpoint_id"]
            if checkpoint_id is None:
                errors.version_conflict("run has no committed checkpoint")
            checkpoint_ref = (
                await session.execute(
                    self._checkpoint,
                    {
                        "scope_id": lease.scope_id,
                        "run_id": lease.run_id,
                        "checkpoint_id": checkpoint_id,
                    },
                )
            ).scalar_one_or_none()
            if checkpoint_ref != state.checkpoint_ref:
                errors.version_conflict("checkpoint changed since the decision was prepared")

        persisted, _ = await self._context.load(lease.scope_id, frame.context.snapshot_id)
        if persisted != frame.context:
            raise runtime_failure(
                "VERSION_CONFLICT",
                category="context",
                public_message="Decision context is not the persisted snapshot.",
            )
        await self._access.verify(self._actor, persisted)


__all__ = ["SqlLiveRunGuard"]

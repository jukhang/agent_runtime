"""Per-claim host identity and supervised Worker/Recovery assembly."""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.contracts.ownership import ChildCommitGuard
from agent_runtime.contracts.scheduler import ClaimedWork
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.contracts.work_claims import (
    ChildClaimedWork,
    ControlClaimedWork,
    WorkClaimResult,
)
from agent_runtime.storage.child_authority import require_child
from agent_runtime.storage.recovery import SqlRecovery
from agent_runtime.storage.uow import errors, facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.worker import (
    ChildLeaseHandle,
    ChildWorkHandler,
    LeaseHandle,
    MainWorkHandler,
    WorkerRunner,
)


class MainHandlerFactory(Protocol):
    async def open(
        self, actor: ActorContext, work: WorkClaimResult, lease: LeaseHandle
    ) -> MainWorkHandler:
        """Bind all user-scoped ports; shared clients remain host-owned."""
        ...


class ChildHandlerFactory(Protocol):
    async def open(
        self, actor: ActorContext, work: ChildClaimedWork, lease: ChildLeaseHandle
    ) -> ChildWorkHandler:
        """Bind the child's current grants and resource access for this claim."""
        ...


class ApprovalWakeScanner(Protocol):
    async def wake_ready(self, scope_id: str, *, limit: int = 100) -> int: ...


class SqlClaimActorResolver:
    """Resolve a root owner from trusted rows, never from work payloads."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._locks = locks.build_lock_statements(require_schema(schema))
        self._owner = scoped(
            "SELECT owner_actor_id,auth_revision,deleted_at FROM {t}session "
            "WHERE scope_id=:scope AND id=:session",
            schema,
        )
        self._clock = clock

    async def resolve(self, lease: LeaseToken) -> ActorContext:
        now = self._clock()
        facts.verify_lease(lease, now=now)
        async with self._sessions() as db, db.begin():
            facts.verify_slot(
                await locks.lock_slot(db, self._locks, lease.scope_id, lease.session_id), lease
            )
            row = await locks.lock_run(db, self._locks, lease.scope_id, lease.run_id)
            if row is None or row["parent_run_id"] is not None:
                errors.forbidden("root claim identity cannot be resolved")
            owner = (
                (
                    await db.execute(
                        self._owner,
                        {
                            "scope": lease.scope_id,
                            "session": lease.session_id,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if owner is None or owner["deleted_at"] is not None:
                errors.forbidden("claim session is missing or deleted")
            if int(owner["auth_revision"]) != int(row["authority_revision"]):
                errors.forbidden("session authorization changed")
            actor = ActorContext(
                scope_id=lease.scope_id,
                actor_id=owner["owner_actor_id"],
                auth_revision=str(row["authority_revision"]),
                grants_ref=f"run-grant:{lease.run_id}:{row['authority_revision']}",
            )
            facts.verify_run(row, actor, lease, now=now)
            await revalidate_authority(db, self._locks, actor, run=row, now=now)
            facts.verify_work(
                await locks.lock_work_item(db, self._locks, lease.scope_id, lease.work_id),
                lease,
                now=now,
            )
            return actor


class SqlClaimMainHandler:
    def __init__(self, resolver: SqlClaimActorResolver, factory: MainHandlerFactory) -> None:
        self._resolver = resolver
        self._factory = factory

    async def __call__(self, work: WorkClaimResult, lease: LeaseHandle) -> None:
        if not isinstance(work, (ClaimedWork, ControlClaimedWork)) or not isinstance(
            work.lease, LeaseToken
        ):
            errors.invalid_input("root host received another work lane")
        if work.lease.model_dump(exclude={"run_lease_until", "work_lease_until"}) != (
            lease.current().model_dump(exclude={"run_lease_until", "work_lease_until"})
        ):
            errors.fenced("host claim identity changed")
        actor = await self._resolver.resolve(lease.current())
        handler = await self._factory.open(actor, work, lease)
        await handler(work, lease)


class SqlClaimChildHandler:
    """Resolve child identity under the current SQL fence before opening host ports."""

    def __init__(
        self, engine: AsyncEngine, schema: str, factory: ChildHandlerFactory
    ) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._schema = require_schema(schema)
        self._factory = factory
        self._owner = scoped(
            "SELECT owner_actor_id,auth_revision,deleted_at FROM {t}session "
            "WHERE scope_id=:scope AND id=:session",
            schema,
        )

    async def __call__(self, work: ChildClaimedWork, lease: ChildLeaseHandle) -> None:
        current = lease.current()
        if work.lease.model_dump(exclude={"run_lease_until", "work_lease_until"}) != (
            current.model_dump(exclude={"run_lease_until", "work_lease_until"})
        ):
            errors.fenced("child claim identity changed")
        async with self._sessions() as db, db.begin():
            owner = (
                (
                    await db.execute(
                        self._owner,
                        {"scope": current.scope_id, "session": current.session_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if owner is None or owner["deleted_at"] is not None:
                errors.forbidden("child session is missing or deleted")
            actor = ActorContext(
                scope_id=current.scope_id,
                actor_id=owner["owner_actor_id"],
                auth_revision=str(owner["auth_revision"]),
                grants_ref=f"run-grant:{current.run_id}:{owner['auth_revision']}",
            )
            guard = ChildCommitGuard(
                actor=actor,
                lease=current,
                expected_state_version=work.state_version,
                root_run_id=work.root_run_id,
                expected_root_cancel_version=work.root_cancel_version,
                workspace_ref=work.workspace_ref,
                workspace_owner_run_id=current.run_id,
                workspace_generation=work.workspace_generation,
            )
            await require_child(db, self._schema, guard, now=datetime.now(UTC))
        handler = await self._factory.open(actor, work, lease)
        await handler(work, lease)


class SqlWorkerRecoverySink:
    def __init__(self, recovery: SqlRecovery) -> None:
        self._recovery = recovery

    async def __call__(self, work: WorkClaimResult, error: BaseException) -> None:
        await self._recovery.handle_worker_failure(work, error)


class SqlWorkerService:
    """Own the Runner and periodic recovery; either failing stops its sibling."""

    def __init__(
        self,
        runner: WorkerRunner,
        recovery: SqlRecovery,
        *,
        scope_id: str,
        approval_wakes: ApprovalWakeScanner | None = None,
        recovery_interval_seconds: float = 30.0,
    ) -> None:
        if recovery_interval_seconds <= 0:
            raise ValueError("recovery interval must be positive")
        self._runner = runner
        self._recovery = recovery
        self._scope = scope_id
        self._approval_wakes = approval_wakes
        self._interval = recovery_interval_seconds
        self._stop = asyncio.Event()

    def request_drain(self) -> None:
        self._stop.set()
        self._runner.request_drain()

    async def serve(self) -> None:
        worker = asyncio.create_task(self._runner.serve())
        recovery = asyncio.create_task(self._recover())
        try:
            done, _ = await asyncio.wait({worker, recovery}, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            self.request_drain()
            try:
                await worker
            finally:
                if not recovery.done():
                    try:
                        await asyncio.wait_for(recovery, timeout=10)
                    except TimeoutError:
                        recovery.cancel()
                await asyncio.gather(recovery, return_exceptions=True)

    async def _recover(self) -> None:
        while not self._stop.is_set():
            await self._recovery.reconcile_scope(self._scope)
            if self._approval_wakes is not None:
                await self._approval_wakes.wake_ready(self._scope)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
            except TimeoutError:
                pass

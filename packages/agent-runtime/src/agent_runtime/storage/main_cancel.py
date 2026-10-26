"""Claim-scoped SQL control watcher; never poll on the token publication path."""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.storage.host_worker import SqlClaimActorResolver
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlMainCancelSignal:
    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        lease: Callable[[], LeaseToken],
        *,
        actor: ActorContext,
        interval_seconds: float = 0.25,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("control polling interval must be positive")
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._schema, self._lease = require_schema(schema), lease
        self._actor = actor
        self._actors = SqlClaimActorResolver(engine, schema)
        self._interval = interval_seconds
        self._cancelled = asyncio.Event()
        self._deadline_at: datetime | None = None

    def _check_deadline(self) -> None:
        if self._deadline_at is not None and datetime.now(UTC) >= self._deadline_at:
            raise AgentRuntimeError(
                FailureEnvelope(
                    code="BUDGET_EXHAUSTED",
                    category="budget",
                    retryable=False,
                    public_message="Run deadline expired.",
                    deadline_at=self._deadline_at,
                )
            )

    def is_cancelled(self) -> bool:
        self._check_deadline()
        return self._cancelled.is_set()

    async def wait(self) -> None:
        self._check_deadline()
        remaining = (
            None
            if self._deadline_at is None
            else (self._deadline_at - datetime.now(UTC)).total_seconds()
        )
        try:
            async with asyncio.timeout(remaining):
                await self._cancelled.wait()
        except TimeoutError:
            self._check_deadline()
            raise
        self._check_deadline()

    async def watch(self) -> None:
        # A failed authority read is fail-closed. The supervisor owns and awaits this
        # task so neither disconnects nor repeated cancellation leave a poller alive.
        try:
            while not self.is_cancelled():
                lease = self._lease()
                current_actor = await self._actors.resolve(lease)
                if (
                    current_actor.scope_id,
                    current_actor.actor_id,
                    current_actor.auth_revision,
                ) != (self._actor.scope_id, self._actor.actor_id, self._actor.auth_revision):
                    self._cancelled.set()
                    return
                async with self._sessions() as db:
                    row = (
                        (
                            await db.execute(
                                scoped(
                                    "SELECT r.status,r.desired_control,r.owner_epoch,r.owner_id,"
                                    "r.lease_until,r.deadline_at,s.auth_revision,s.deleted_at "
                                    "FROM {t}run r JOIN {t}session s ON s.scope_id=r.scope_id "
                                    "AND s.id=r.session_id WHERE r.scope_id=:scope AND r.id=:run",
                                    self._schema,
                                ),
                                {"scope": lease.scope_id, "run": lease.run_id},
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                if row is not None:
                    self._deadline_at = row["deadline_at"]
                    self._check_deadline()
                if (
                    row is None
                    or row["status"] != "running"
                    or row["desired_control"] is not None
                    or row["owner_epoch"] != int(lease.run_epoch)
                    or row["owner_id"] != lease.owner_id
                    or row["lease_until"] is None
                    or row["lease_until"] <= datetime.now(UTC)
                    or row["deleted_at"] is not None
                    or row["auth_revision"] != int(self._actor.auth_revision)
                ):
                    self._cancelled.set()
                    return
                assert self._deadline_at is not None
                await asyncio.sleep(
                    min(
                        self._interval,
                        max(0, (self._deadline_at - datetime.now(UTC)).total_seconds()),
                    )
                )
        finally:
            self._cancelled.set()

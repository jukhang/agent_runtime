"""Claim-scoped child model interruption when its root control or authority changes."""

import asyncio
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.ownership import ChildCommitGuard
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.work_claims import ChildClaimedWork
from agent_runtime.storage.child_authority import require_child
from agent_runtime.storage.uow.schema import require_schema
from agent_runtime.worker import ChildLeaseHandle


class SqlChildCancelSignal:
    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        actor: ActorContext,
        work: ChildClaimedWork,
        lease: ChildLeaseHandle,
    ) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._schema = require_schema(schema)
        self._actor, self._work, self._lease = actor, work, lease
        self._cancelled = asyncio.Event()
        self._failure: AgentRuntimeError | None = None

    def is_cancelled(self) -> bool:
        if self._failure is not None:
            raise self._failure
        return self._cancelled.is_set()

    async def wait(self) -> None:
        if self._failure is not None:
            raise self._failure
        await self._cancelled.wait()
        if self._failure is not None:
            raise self._failure

    async def watch(self) -> None:
        try:
            while not self.is_cancelled():
                work, lease = self._work, self._lease.current()
                guard = ChildCommitGuard(
                    actor=self._actor,
                    lease=lease,
                    expected_state_version=work.state_version,
                    root_run_id=work.root_run_id,
                    expected_root_cancel_version=work.root_cancel_version,
                    workspace_ref=work.workspace_ref,
                    workspace_owner_run_id=lease.run_id,
                    workspace_generation=work.workspace_generation,
                )
                async with self._sessions() as db, db.begin():
                    await require_child(db, self._schema, guard, now=datetime.now(UTC))
                await asyncio.sleep(0.5)
        except AgentRuntimeError as exc:
            if exc.failure.code != "BUDGET_EXHAUSTED":
                raise
            self._failure = exc
        finally:
            self._cancelled.set()

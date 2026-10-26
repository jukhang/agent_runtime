"""Recover terminal approval waits after a resolver or worker crash."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.approval_cleanup import ApprovalResourceValidity, SqlApprovalCleanup
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.recovery import SqlRecovery
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.storage.wake import SqlWaitWaker, WakeRequest


class SqlApprovalWakeScanner:
    """Scan terminal approvals; the waker owns the final authorization and CAS."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        resource_validity: ApprovalResourceValidity | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._engine = engine
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._schema = require_schema(schema)
        self._clock = clock
        self._recovery = SqlRecovery(engine, schema, clock=clock)
        self._cleanup = SqlApprovalCleanup(
            engine, schema, resource_validity=resource_validity, clock=clock
        )
        self._ready = scoped(
            "SELECT r.id AS run_id,r.session_id,r.state_version,r.authority_revision,"
            "s.owner_actor_id,a.id AS approval_id FROM {t}run r "
            "JOIN {t}session s ON s.scope_id=r.scope_id AND s.id=r.session_id "
            "JOIN {t}approval a ON a.scope_id=r.scope_id AND a.run_id=r.id "
            "WHERE r.scope_id=:scope AND r.status='waiting_user' "
            "AND r.desired_control IS NULL AND r.deadline_at>:now "
            "AND s.deleted_at IS NULL AND a.status IN "
            "('approved','rejected','expired','cancelled') "
            "AND r.waiting_spec->>'kind'='approval' "
            "AND r.waiting_spec->>'subjectId'=CAST(a.id AS text) "
            "ORDER BY a.updated_at,a.id LIMIT :limit",
            schema,
        )

    async def wake_ready(self, scope_id: str, *, limit: int = 100) -> int:
        if limit <= 0 or limit > 1000:
            raise ValueError("approval wake scan limit must be between 1 and 1000")
        now = self._clock()
        await self._cleanup.reconcile(scope_id, limit=limit)
        async with self._sessions() as db, db.begin():
            rows = (
                (
                    await db.execute(
                        self._ready,
                        {"scope": scope_id, "now": now, "limit": limit},
                    )
                )
                .mappings()
                .all()
            )
        payloads = BlobContextPayloadStore(SqlContextBlobBackend(self._engine, self._schema))
        woke = 0
        for row in rows:
            actor = ActorContext(
                scope_id=scope_id,
                actor_id=row["owner_actor_id"],
                auth_revision=str(row["authority_revision"]),
                grants_ref=f"run-grant:{row['run_id']}:{row['authority_revision']}",
            )
            waker = SqlWaitWaker(
                self._engine,
                self._schema,
                actor=actor,
                journal=SqlJournal(self._engine, self._schema, actor),
                payloads=payloads,
                approval_resumer_enabled=True,
                clock=self._clock,
            )
            try:
                await waker.wake(
                    WakeRequest(
                        actor=actor,
                        session_id=UUID(str(row["session_id"])),
                        run_id=UUID(str(row["run_id"])),
                        expected_state_version=int(row["state_version"]),
                        kind="approval",
                        subject_id=str(row["approval_id"]),
                    )
                )
            except AgentRuntimeError as exc:
                if exc.failure.code == "FORBIDDEN":
                    await self._recovery.fail_waiting_approval(
                        scope_id,
                        UUID(str(row["run_id"])),
                        UUID(str(row["approval_id"])),
                        int(row["state_version"]),
                        exc.failure,
                    )
                    continue
                if exc.failure.code not in {"VERSION_CONFLICT", "FENCED", "CONTROL_PENDING"}:
                    raise
                continue
            woke += 1
        return woke


__all__ = ["SqlApprovalWakeScanner"]

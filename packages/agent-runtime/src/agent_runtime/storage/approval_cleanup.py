"""Bounded maintenance convergence; never overwrites an already-decided approval."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.approval_events import append_approval_update
from agent_runtime.storage.uow import locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped


class ApprovalResourceValidity(Protocol):
    async def __call__(
        self,
        db: AsyncSession,
        actor: ActorContext,
        session_id: UUID,
        resources: tuple[ResourceRef, ...],
    ) -> None:
        """Check current resource ACL/versions without granting a human decision."""


class SqlApprovalCleanup:
    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        resource_validity: ApprovalResourceValidity | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._engine, self._schema = engine, require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._locks, self._clock = locks.build_lock_statements(schema), clock
        self._resource_validity = resource_validity

    async def reconcile(self, scope_id: str, *, limit: int = 100) -> int:
        if not 1 <= limit <= 1000:
            raise ValueError("Approval cleanup limit must be 1..1000")
        now = self._clock()
        async with self._sessions() as db:
            candidates = (
                (
                    await db.execute(
                        scoped(
                            "SELECT a.id,r.id AS run_id,r.session_id FROM {t}approval a "
                            "JOIN {t}run r ON r.scope_id=a.scope_id AND r.id=a.run_id "
                            "JOIN {t}session s ON s.scope_id=r.scope_id AND s.id=r.session_id "
                            "WHERE a.scope_id=:scope AND a.status='pending' "
                            "ORDER BY a.updated_at,a.id LIMIT :limit",
                            self._schema,
                        ),
                        {"scope": scope_id, "limit": limit},
                    )
                )
                .mappings()
                .all()
            )
        changed = 0
        for candidate in candidates:
            async with self._sessions() as db, db.begin():
                await locks.lock_slot(db, self._locks, scope_id, candidate["session_id"])
                run = await locks.lock_run(db, self._locks, scope_id, candidate["run_id"])
                row = (
                    (
                        await db.execute(
                            scoped(
                                "SELECT * FROM {t}approval "
                                "WHERE scope_id=:scope AND id=:id FOR UPDATE",
                                self._schema,
                            ),
                            {"scope": scope_id, "id": candidate["id"]},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None or run is None or row["status"] != "pending":
                    continue
                session = (
                    (
                        await db.execute(
                            scoped(
                                "SELECT * FROM {t}session "
                                "WHERE scope_id=:scope AND id=:id FOR UPDATE",
                                self._schema,
                            ),
                            {"scope": scope_id, "id": run["session_id"]},
                        )
                    )
                    .mappings()
                    .one()
                )
                closed = (
                    run["status"] in {"completed", "failed", "cancelled"}
                    or run["desired_control"] == "cancel"
                    or session["deleted_at"] is not None
                    or row["auth_revision"] != session["auth_revision"]
                    or row["auth_revision"] != run["authority_revision"]
                )
                expired = row["expires_at"] <= now or run["deadline_at"] <= now
                request_actor = ActorContext(
                    scope_id=scope_id,
                    actor_id=row["actor_id"],
                    auth_revision=str(row["auth_revision"]),
                    grants_ref="approval-validity",
                )
                if not closed:
                    try:
                        await revalidate_authority(db, self._locks, request_actor, run=run, now=now)
                        if self._resource_validity is not None:
                            await self._resource_validity(
                                db,
                                request_actor,
                                run["session_id"],
                                tuple(
                                    ResourceRef.model_validate(ref) for ref in row["resource_refs"]
                                ),
                            )
                    except AgentRuntimeError as exc:
                        if exc.failure.code not in {
                            "FORBIDDEN",
                            "VERSION_CONFLICT",
                            "DELIVERY_EXPIRED",
                        }:
                            raise
                        closed = True
                if not closed and not expired:
                    # Persist scan progress even across maintenance restarts/fixed test clocks.
                    await db.execute(
                        scoped(
                            "UPDATE {t}approval SET updated_at=clock_timestamp() "
                            "WHERE scope_id=:scope AND id=:id AND status='pending' "
                            "AND revision=:revision",
                            self._schema,
                        ),
                        {"scope": scope_id, "id": row["id"], "revision": row["revision"]},
                    )
                    continue
                status = "cancelled" if closed else "expired"
                revised = (
                    await db.execute(
                        scoped(
                            "UPDATE {t}approval SET status=:status,"
                            "revision=revision+1,updated_at=:now "
                            "WHERE scope_id=:scope AND id=:id "
                            "AND status='pending' AND revision=:revision "
                            "RETURNING revision",
                            self._schema,
                        ),
                        {
                            "scope": scope_id,
                            "id": row["id"],
                            "revision": row["revision"],
                            "status": status,
                            "now": now,
                        },
                    )
                ).scalar_one_or_none()
                if revised is None:
                    continue
                actor = ActorContext(
                    scope_id=scope_id,
                    actor_id=session["owner_actor_id"],
                    auth_revision=str(session["auth_revision"]),
                    grants_ref="approval-cleanup",
                )
                await append_approval_update(
                    db,
                    self._engine,
                    self._schema,
                    actor,
                    run,
                    dict(row),
                    status=status,
                    revision=int(revised),
                    now=now,
                    reason="任务或授权已结束，待审批操作已关闭。"
                    if closed
                    else "审批已过期，操作未执行。",
                )
                changed += 1
        return changed

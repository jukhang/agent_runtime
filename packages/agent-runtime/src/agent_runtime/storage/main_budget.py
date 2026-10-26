"""Conservative root budget view from persisted allocations and audit facts."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.contracts.loop import BudgetCounter, BudgetView, RunState
from agent_runtime.contracts.results import Usage
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.storage.uow import errors, facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlMainBudgetReader:
    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        lease: LeaseToken | Callable[[], LeaseToken],
        allocation_id: UUID,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._schema = require_schema(schema)
        self._locks = locks.build_lock_statements(self._schema)
        self._actor = actor
        self._lease = lease if callable(lease) else lambda: lease
        self._allocation_id = allocation_id
        self._clock = clock

    async def read(self, state: RunState) -> BudgetView:
        lease = self._lease()
        if (state.scope_id, state.session_id, state.run_id) != (
            lease.scope_id,
            lease.session_id,
            lease.run_id,
        ) or state.scope_id != self._actor.scope_id:
            errors.forbidden("root budget state belongs to another claim")
        now = self._clock()
        facts.verify_lease(lease, now=now)
        params = {"scope": lease.scope_id, "run": lease.run_id, "id": self._allocation_id}
        async with self._sessions() as db, db.begin():
            slot = await locks.lock_slot(db, self._locks, lease.scope_id, lease.session_id)
            facts.verify_slot(slot, lease)
            run = dict(
                facts.verify_run(
                    await locks.lock_run(db, self._locks, lease.scope_id, lease.run_id),
                    self._actor,
                    lease,
                    now=now,
                )
            )
            await revalidate_authority(db, self._locks, self._actor, run=run, now=now)
            work = facts.verify_work(
                await locks.lock_work_item(db, self._locks, lease.scope_id, lease.work_id),
                lease,
                now=now,
            )
            if work["kind"] != "advance_run" or run["parent_run_id"] is not None:
                errors.forbidden("root budget requires a main advance_run")
            if (
                run["status"] != "running"
                or run["desired_control"] is not None
                or now >= run["deadline_at"]
                or int(run["state_version"]) != int(state.state_version)
                or int(run["cancel_version"]) != int(state.cancel_version)
                or int(run["applied_input_version"]) != int(state.applied_input_seq)
                or int(run["pending_input_version"]) != int(state.pending_input_seq)
            ):
                errors.version_conflict("root budget state changed since it was loaded")
            checkpoint_ref = (
                await db.execute(
                    scoped(
                        "SELECT state_ref FROM {t}checkpoint WHERE scope_id=:scope "
                        "AND id=:id AND run_id=:run",
                        self._schema,
                    ),
                    {**params, "id": run["checkpoint_id"]},
                )
            ).scalar_one_or_none()
            if checkpoint_ref != state.checkpoint_ref:
                errors.version_conflict("root budget checkpoint changed")
            allocation = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}budget_allocation WHERE scope_id=:scope "
                            "AND id=:id AND root_run_id=:run AND parent_id IS NULL FOR UPDATE",
                            self._schema,
                        ),
                        params,
                    )
                )
                .mappings()
                .one_or_none()
            )
            if allocation is None:
                errors.forbidden("root budget allocation is not installed")
            attempts = (
                (
                    await db.execute(
                        scoped(
                            "SELECT status,usage FROM {t}model_attempt "
                            "WHERE scope_id=:scope AND run_id=:run",
                            self._schema,
                        ),
                        params,
                    )
                )
                .mappings()
                .all()
            )
            tool_count = (
                await db.execute(
                    scoped(
                        "SELECT count(*) FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run",
                        self._schema,
                    ),
                    params,
                )
            ).scalar_one()
            own_tokens, unknown = 0, False
            for attempt in attempts:
                if attempt["status"] == "running" or attempt["usage"] is None:
                    unknown = True
                    continue
                usage = Usage.model_validate(attempt["usage"])
                own_tokens += int(usage.total_tokens or "0")
                unknown |= usage.status != "complete"
            own = {
                "model_calls": len(attempts),
                "tool_calls": int(tool_count),
                "tokens": own_tokens,
            }
            limits = allocation["limits"]
            settled = allocation["settled"]
            reserved = allocation["reserved"]
            unknown_reserved = allocation["unknown_reserved"]
            counters: dict[str, BudgetCounter] = {}
            for key, own_spent in own.items():
                limit = int(limits[key])
                spent = int(settled.get(key, 0)) + own_spent
                held = int(reserved.get(key, 0))
                unknown_held = int(unknown_reserved.get(key, 0))
                if unknown and key in {"model_calls", "tokens"}:
                    conservative = max(0, limit - spent - held)
                    held += conservative
                    unknown_held += conservative
                counters[key] = BudgetCounter(
                    limit=str(limit),
                    spent=str(spent),
                    reserved=str(held),
                    unknown_reserved=str(unknown_held),
                )
            return BudgetView(
                allocation_id=self._allocation_id,
                revision=str(allocation["version"]),
                model_calls=counters["model_calls"],
                tool_calls=counters["tool_calls"],
                tokens=counters["tokens"],
                deadline_at=run["deadline_at"],
            )


__all__ = ["SqlMainBudgetReader"]

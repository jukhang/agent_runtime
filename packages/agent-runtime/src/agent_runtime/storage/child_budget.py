"""Child budget views derived from durable model/tool audit, not model self-reports."""

import json
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.contracts.loop import BudgetCounter, BudgetView, RunState
from agent_runtime.contracts.ownership import ChildCommitGuard
from agent_runtime.contracts.results import Usage
from agent_runtime.storage.child_authority import require_child
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlChildBudgetReader:
    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        allocation_id: UUID,
        guard: Callable[[], ChildCommitGuard],
    ) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._schema = require_schema(schema)
        self._allocation, self._guard = allocation_id, guard

    async def read(self, state: RunState) -> BudgetView:
        guard = self._guard()
        if (state.scope_id, state.session_id, state.run_id) != (
            guard.lease.scope_id,
            guard.lease.session_id,
            guard.lease.run_id,
        ):
            errors.forbidden("budget state changed child identity")
        async with self._sessions() as db, db.begin():
            row = await require_child(db, self._schema, guard, now=datetime.now(UTC))
            params = {
                "scope": guard.actor.scope_id,
                "run": guard.lease.run_id,
                "allocation": self._allocation,
                "root": guard.root_run_id,
            }
            allocation = (
                (
                    await db.execute(
                        scoped(
                            "SELECT b.* FROM {t}budget_allocation b WHERE b.scope_id=:scope "
                            "AND b.id=:allocation AND b.root_run_id=:root "
                            "AND b.parent_id IS NOT NULL "
                            "AND EXISTS (SELECT 1 FROM {t}agent_message a "
                            "WHERE a.scope_id=b.scope_id AND a.recipient_run_id=:run "
                            "AND a.sender_run_id=:root AND a.kind='task.assign' "
                            "AND a.payload->'payload'->>'budgetAllocationId'=CAST(b.id AS text)) "
                            "FOR UPDATE",
                            self._schema,
                        ),
                        params,
                    )
                )
                .mappings()
                .one_or_none()
            )
            if allocation is None:
                errors.forbidden("budget is not the child's assigned allocation")
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
            tokens, unknown = 0, 0
            for attempt in attempts:
                if attempt["status"] == "running" or attempt["usage"] is None:
                    unknown += 1
                    continue
                usage = Usage.model_validate(attempt["usage"])
                if usage.status != "complete":
                    unknown += 1
                tokens += int(usage.total_tokens or "0")
            tools = (
                await db.execute(
                    scoped(
                        "SELECT count(*) FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run",
                        self._schema,
                    ),
                    params,
                )
            ).scalar_one()
            spent = {"model_calls": len(attempts), "tool_calls": tools, "tokens": tokens}
            limits = allocation["limits"]
            held_tokens = max(0, limits["tokens"] - tokens) if unknown else 0
            held_calls = max(0, limits["model_calls"] - len(attempts)) if unknown else 0
            held = {"tokens": held_tokens, "model_calls": held_calls}
            # These are audit projections. Parent settlement still has its own unique ledger key.
            await db.execute(
                scoped(
                    "UPDATE {t}budget_allocation SET settled=CAST(:spent AS jsonb),"
                    "reserved=CAST(:held AS jsonb),unknown_reserved=CAST(:held AS jsonb),"
                    "usage_status=:status,unsettled_count=:unknown,version=version+1,"
                    "updated_at=CURRENT_TIMESTAMP WHERE scope_id=:scope AND id=:allocation",
                    self._schema,
                ),
                {
                    **params,
                    "spent": json.dumps(spent),
                    "held": json.dumps(held),
                    "status": "unknown" if unknown else "complete",
                    "unknown": unknown,
                },
            )
            counters = {
                key: BudgetCounter(
                    limit=str(limits[key]),
                    spent=str(spent[key]),
                    reserved=str(held.get(key, 0)),
                    unknown_reserved=str(held.get(key, 0)),
                )
                for key in spent
            }
            return BudgetView(
                allocation_id=self._allocation,
                revision=str(allocation["version"] + 1),
                model_calls=counters["model_calls"],
                tool_calls=counters["tool_calls"],
                tokens=counters["tokens"],
                deadline_at=row["deadline_at"],
            )


__all__ = ["SqlChildBudgetReader"]

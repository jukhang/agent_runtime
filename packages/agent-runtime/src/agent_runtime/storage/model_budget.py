"""Purpose-scoped closing holds in the existing immutable budget ledger.

Caller owns Run/Work fencing and locks allocation after those owners. Model usage
is counted from ModelAttempt exactly once; ledger settlement must not add it again.
"""

import json
from collections.abc import Mapping
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.contracts.results import Usage
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import scoped


def purpose_key(run_id: UUID) -> str:
    return f"runtime.finalization:{run_id}"


async def entry(
    db: AsyncSession,
    schema: str,
    *,
    scope: str,
    allocation: UUID,
    run: UUID,
    kind: str,
    quantities: Mapping[str, int],
    receipt: str | None = None,
    usage: Usage | None = None,
) -> None:
    await db.execute(
        scoped(
            "INSERT INTO {t}budget_entry (id,scope_id,allocation_id,operation_key,kind,"
            "quantities,reconciliation_ref,usage_status,usage_reason) VALUES "
            "(:id,:scope,:allocation,:key,:kind,CAST(:quantities AS jsonb),"
            ":receipt,:status,:reason)",
            schema,
        ),
        {
            "id": uuid5(NAMESPACE_URL, f"{scope}:{allocation}:{purpose_key(run)}:{kind}"),
            "scope": scope,
            "allocation": allocation,
            "key": purpose_key(run),
            "kind": kind,
            "quantities": json.dumps(dict(quantities)),
            "receipt": receipt,
            "status": usage.status if usage else "complete",
            "reason": usage.reason if usage else None,
        },
    )


async def closing_entries(
    db: AsyncSession, schema: str, scope: str, allocation: UUID, run: UUID
) -> dict[str, Any]:
    rows = (
        await db.execute(
            scoped(
                "SELECT kind,quantities,reconciliation_ref FROM {t}budget_entry "
                "WHERE scope_id=:scope AND allocation_id=:allocation AND operation_key=:key",
                schema,
            ),
            {"scope": scope, "allocation": allocation, "key": purpose_key(run)},
        )
    ).mappings()
    return {r["kind"]: dict(r) for r in rows}


async def reserve_closing(
    db: AsyncSession, schema: str, *, scope: str, allocation: UUID, run: UUID, tokens: int
) -> None:
    if tokens == 0:
        return
    row = (
        (
            await db.execute(
                scoped(
                    "SELECT * FROM {t}budget_allocation "
                    "WHERE scope_id=:scope AND id=:id FOR UPDATE",
                    schema,
                ),
                {"scope": scope, "id": allocation},
            )
        )
        .mappings()
        .one()
    )
    if await closing_entries(db, schema, scope, allocation, run):
        errors.version_conflict("closing hold already exists")
    quantities = {"model_calls": 1, "tokens": tokens}
    reserved = dict(row["reserved"])
    for key, amount in quantities.items():
        if (
            int(row["limits"][key]) - int(row["settled"].get(key, 0)) - int(reserved.get(key, 0))
            < amount
        ):
            exhausted("The allocation cannot reserve its closing window.")
        reserved[key] = int(reserved.get(key, 0)) + amount
    await db.execute(
        scoped(
            "UPDATE {t}budget_allocation SET reserved=CAST(:held AS jsonb),version=version+1 "
            "WHERE scope_id=:scope AND id=:id",
            schema,
        ),
        {"scope": scope, "id": allocation, "held": json.dumps(reserved)},
    )
    await entry(
        db,
        schema,
        scope=scope,
        allocation=allocation,
        run=run,
        kind="reserve",
        quantities=quantities,
    )


def exhausted(message: str) -> None:
    raise runtime_failure("BUDGET_EXHAUSTED", category="budget", public_message=message)


async def own_spend(
    db: AsyncSession, schema: str, scope: str, run: UUID
) -> tuple[dict[str, int], bool]:
    rows = (
        (
            await db.execute(
                scoped(
                    "SELECT status,usage FROM {t}model_attempt "
                    "WHERE scope_id=:scope AND run_id=:run",
                    schema,
                ),
                {"scope": scope, "run": run},
            )
        )
        .mappings()
        .all()
    )
    total, unknown = 0, False
    for row in rows:
        if row["usage"] is None or row["status"] in {"pending", "running"}:
            unknown = True
            continue
        usage = Usage.model_validate(row["usage"])
        total += int(usage.total_tokens or "0")
        unknown |= usage.status != "complete"
    tools = (
        await db.execute(
            scoped(
                "SELECT count(*) FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run",
                schema,
            ),
            {"scope": scope, "run": run},
        )
    ).scalar_one()
    return {"model_calls": len(rows), "tokens": total, "tool_calls": int(tools)}, unknown


async def authorize_attempt(
    db: AsyncSession, schema: str, request: ProviderRequest, *, fenced: bool
) -> None:
    admitted = (
        await db.execute(
            scoped(
                "SELECT allocation_id FROM {t}budget_entry WHERE scope_id=:scope "
                "AND operation_key=:key AND kind='reserve'",
                schema,
            ),
            {"scope": request.scope_id, "key": purpose_key(request.run_id)},
        )
    ).scalars().all()
    if admitted and request.budget_allocation_id not in admitted:
        errors.forbidden("model call cannot substitute its admitted closing allocation")
    row = (
        (
            await db.execute(
                scoped(
                    "SELECT * FROM {t}budget_allocation "
                    "WHERE scope_id=:scope AND id=:id FOR UPDATE",
                    schema,
                ),
                {"scope": request.scope_id, "id": request.budget_allocation_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        if fenced or request.purpose == "closing":
            errors.forbidden("the execution's model allocation is unavailable")
        return  # Old unfunded audit-only integrations retain their previous protocol.
    if row["parent_id"] is None and row["root_run_id"] != request.run_id:
        errors.forbidden("model allocation belongs to another Run")
    history = await closing_entries(db, schema, request.scope_id, row["id"], request.run_id)
    if row["parent_id"] is not None:
        assigned = (
            await db.execute(
                scoped(
                    "SELECT 1 FROM {t}agent_message "
                    "WHERE scope_id=:scope AND recipient_run_id=:run "
                    "AND sender_run_id=:root AND kind='task.assign' "
                    "AND payload->'payload'->>'budgetAllocationId'=:allocation LIMIT 1",
                    schema,
                ),
                {
                    "scope": request.scope_id,
                    "run": request.run_id,
                    "root": row["root_run_id"],
                    "allocation": str(row["id"]),
                },
            )
        ).scalar_one_or_none()
        if assigned is None:
            errors.forbidden("model allocation is not assigned to this child")
    if "reserve" not in history:
        if request.purpose == "closing":
            errors.forbidden("no closing purpose was admitted")
        if row["parent_id"] is None:
            return
        parent_history = await closing_entries(
            db, schema, request.scope_id, row["parent_id"], row["root_run_id"]
        )
        if "reserve" not in parent_history:
            return
    if not fenced:
        errors.forbidden("purpose-budgeted model calls require a live execution fence")
    context = (
        (
            await db.execute(
                scoped(
                    "SELECT payload_ref,content_hash,estimated_input_tokens,task_type "
                    "FROM {t}context_snapshot WHERE scope_id=:scope AND run_id=:run "
                    "AND session_id=:session AND id=:id",
                    schema,
                ),
                {
                    "scope": request.scope_id,
                    "run": request.run_id,
                    "session": request.session_id,
                    "id": request.context_snapshot_id,
                },
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        context is None
        or context["payload_ref"] != request.input_ref
        or context["content_hash"] != request.input_digest
        or context["estimated_input_tokens"] != request.estimated_input_tokens
        or (context["task_type"] == "model.closing") != (request.purpose == "closing")
    ):
        errors.forbidden("model purpose or input differs from its admitted snapshot")
    if request.purpose == "ordinary" and "adjust" in history:
        exhausted("The closing call has already started; no new paid work is admitted.")
    spend, unknown = await own_spend(db, schema, request.scope_id, request.run_id)
    if unknown:
        raise runtime_failure(
            "USAGE_UNRESOLVED",
            category="budget",
            public_message="Previous model usage must be reconciled before another call.",
        )
    # Child allocation.settled is a projection of its own audit, unlike a root's
    # settled child costs. Never count the child's own usage twice.
    settled = row["settled"] if row["parent_id"] is None else {}
    held = dict(row["reserved"])
    need = {
        "model_calls": 1,
        "tokens": request.estimated_input_tokens + request.model.max_output_tokens,
    }
    if request.purpose == "closing":
        if "adjust" in history or "release" in history:
            errors.version_conflict("closing purpose was already consumed or released")
        quantities = history["reserve"]["quantities"]
        if need["tokens"] > quantities["tokens"] or request.tools or request.output_schema:
            errors.forbidden("closing request exceeds its admitted purpose")
        for key, amount in quantities.items():
            if int(held.get(key, 0)) - int(row["unknown_reserved"].get(key, 0)) < amount:
                errors.version_conflict("closing purpose hold is unavailable")
            held[key] -= amount
    for key, amount in need.items():
        if (
            int(row["limits"][key]) - int(settled.get(key, 0)) - spend[key] - int(held.get(key, 0))
            < amount
        ):
            exhausted(
                "Insufficient budget for the proposed model call; closing funds remain protected."
            )
    if request.purpose == "closing":
        await entry(
            db,
            schema,
            scope=request.scope_id,
            allocation=row["id"],
            run=request.run_id,
            kind="adjust",
            quantities=need,
            receipt=str(request.attempt_id),
        )
        await db.execute(
            scoped(
                "UPDATE {t}budget_allocation SET reserved=CAST(:held AS jsonb),version=version+1 "
                "WHERE scope_id=:scope AND id=:id",
                schema,
            ),
            {"scope": request.scope_id, "id": row["id"], "held": json.dumps(held)},
        )


async def release_closing(db: AsyncSession, schema: str, scope: str, run: UUID) -> None:
    # Only the unclaimed, known hold is released. A running/unknown closing call
    # is represented by its ModelAttempt; this function never settles it as zero.
    rows = (
        (
            await db.execute(
                scoped(
                    "SELECT b.* FROM {t}budget_allocation b JOIN {t}budget_entry e "
                    "ON e.scope_id=b.scope_id AND e.allocation_id=b.id "
                    "WHERE b.scope_id=:scope AND e.operation_key=:key AND e.kind='reserve' "
                    "ORDER BY b.id FOR UPDATE OF b",
                    schema,
                ),
                {"scope": scope, "key": purpose_key(run)},
            )
        )
        .mappings()
        .all()
    )
    for row in rows:
        history = await closing_entries(db, schema, scope, row["id"], run)
        if "release" in history or "adjust" in history:
            continue
        held = dict(row["reserved"])
        for key, amount in history["reserve"]["quantities"].items():
            if int(held.get(key, 0)) - int(row["unknown_reserved"].get(key, 0)) < amount:
                errors.version_conflict("unclaimed closing hold differs from ledger")
            held[key] -= amount
        await entry(
            db,
            schema,
            scope=scope,
            allocation=row["id"],
            run=run,
            kind="release",
            quantities=history["reserve"]["quantities"],
        )
        await db.execute(
            scoped(
                "UPDATE {t}budget_allocation SET reserved=CAST(:held AS jsonb),version=version+1 "
                "WHERE scope_id=:scope AND id=:id",
                schema,
            ),
            {"scope": scope, "id": row["id"], "held": json.dumps(held)},
        )

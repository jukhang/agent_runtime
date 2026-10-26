"""Trusted model retry evidence read inside the recovery transaction."""

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.results import Usage
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.transactions import LeaseToken
from agent_runtime.recovery_policy import RecoveryChoice, RecoveryContext, propose_recovery
from agent_runtime.storage.model_attempts import claim_attempt_suffix
from agent_runtime.storage.uow import locks
from agent_runtime.storage.uow.schema import scoped


async def propose_model_retry(
    db: AsyncSession,
    schema: str,
    lease: LeaseToken,
    run: Mapping[str, Any],
    work: Mapping[str, Any],
    failure: FailureEnvelope,
    *,
    budget_id: UUID | None,
    clock: Callable[[], datetime],
    maximum_attempts: int,
    base_backoff: timedelta,
    max_backoff: timedelta,
) -> RecoveryChoice:
    """Caller holds Slot/Run/Work; the candidate does not grant execution authority."""

    def stop(reason: str) -> RecoveryChoice:
        return RecoveryChoice(kind="stop", reason=reason, failure=failure)

    now = clock()
    if work["kind"] != "advance_run" or budget_id is None or failure.attempt_id is None:
        return stop("unproven_model_failure")
    try:
        attempt_id = UUID(failure.attempt_id)
    except ValueError:
        return stop("unproven_model_failure")
    params = {"scope": lease.scope_id, "run": lease.run_id, "id": attempt_id}
    attempt = (
        (
            await db.execute(
                scoped(
                    "SELECT m.*,c.auth_revision FROM {t}model_attempt m JOIN {t}context_snapshot c "
                    "ON c.scope_id=m.scope_id AND c.id=m.context_snapshot_id AND c.run_id=m.run_id "
                    "WHERE m.scope_id=:scope AND m.run_id=:run AND m.id=:id",
                    schema,
                ),
                params,
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        attempt is None
        or attempt["status"] != "failed"
        or not attempt["request_key"].endswith(claim_attempt_suffix(lease))
        or attempt["failure"] != failure.model_dump(mode="json", by_alias=True)
    ):
        return stop("unproven_model_failure")
    owner = (
        (
            await db.execute(
                scoped(
                    "SELECT owner_actor_id,auth_revision FROM {t}session "
                    "WHERE scope_id=:scope AND id=:session "
                    "AND deleted_at IS NULL",
                    schema,
                ),
                {**params, "session": lease.session_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if owner is None or int(owner["auth_revision"]) != int(run["authority_revision"]):
        return stop("authority_unavailable")
    grant = await locks.current_grant(
        db,
        locks.build_lock_statements(schema),
        lease.scope_id,
        lease.run_id,
        owner["owner_actor_id"],
    )
    if (
        grant is None
        or grant["revoked_at"] is not None
        or grant["expires_at"] <= now
        or int(grant["revision"]) != int(run["authority_revision"])
        or int(attempt["auth_revision"]) != int(run["authority_revision"])
    ):
        return stop("authority_unavailable")
    allocation = (
        (
            await db.execute(
                scoped(
                    "SELECT * FROM {t}budget_allocation WHERE scope_id=:scope AND id=:id "
                    "AND root_run_id=:run AND parent_id IS NULL FOR UPDATE",
                    schema,
                ),
                {**params, "id": budget_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if allocation is None:
        return stop("budget_unavailable")
    attempts = (
        (
            await db.execute(
                scoped(
                    "SELECT status,usage FROM {t}model_attempt "
                    "WHERE scope_id=:scope AND run_id=:run",
                    schema,
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    tokens = 0
    for item in attempts:
        if item["status"] in {"pending", "running"} or item["usage"] is None:
            return stop("usage_unresolved")
        usage = Usage.model_validate(item["usage"])
        if usage.status != "complete":
            return stop("usage_unresolved")
        tokens += int(usage.total_tokens or "0")
    available: dict[str, int] = {}
    for key, own in (("model_calls", len(attempts)), ("tokens", tokens)):
        limit = allocation["limits"].get(key)
        if limit is None:
            return stop("budget_unavailable")
        available[key] = max(
            0,
            int(limit)
            - own
            - int(allocation["settled"].get(key, 0))
            - int(allocation["reserved"].get(key, 0)),
        )
    if available["tokens"] == 0:
        return stop("token_budget_exhausted")
    # No tool should have been dispatched from a failed decision. Previously
    # unresolved external effects must also be reconciled before progression.
    effects = (
        await db.execute(
            scoped(
                "SELECT EXISTS(SELECT 1 FROM {t}tool_call WHERE scope_id=:scope "
                "AND run_id=:run AND attempt_id=:id) OR EXISTS(SELECT 1 FROM {t}operation o "
                "JOIN {t}tool_call t ON t.scope_id=o.scope_id AND t.operation_id=o.id "
                "WHERE t.scope_id=:scope AND t.run_id=:run "
                "AND o.status IN ('prepared','dispatching','unknown')) OR EXISTS(SELECT 1 "
                "FROM {t}execution WHERE scope_id=:scope AND run_id=:run "
                "AND status IN ('pending','starting','running','cancelling','unknown'))",
                schema,
            ),
            params,
        )
    ).scalar_one()
    if effects:
        return RecoveryChoice(kind="reconcile", reason="unresolved_effect", failure=failure)
    task_deadline = (
        await db.execute(
            scoped("SELECT deadline_at FROM {t}task WHERE scope_id=:scope AND id=:task", schema),
            {**params, "task": run["task_id"]},
        )
    ).scalar_one_or_none()
    if task_deadline is None:
        return stop("deadline_unavailable")
    now = clock()
    if grant["expires_at"] <= now:
        return stop("authority_unavailable")
    return propose_recovery(
        failure,
        RecoveryContext(
            stage="model",
            effect="none",
            attempt_count=int(work["attempt_count"]),
            max_attempts=maximum_attempts,
            model_calls_remaining=available["model_calls"],
            now=now,
            run_deadline=min(run["deadline_at"], work["deadline_at"], task_deadline),
            run_status=RunStatus(run["status"]),
            desired_control=run["desired_control"],
            base_backoff=base_backoff,
            max_backoff=max_backoff,
        ),
    )

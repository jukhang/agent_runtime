"""PostgreSQL ModelAttempt lifecycle store.

One attempt can move from absent to running and then to exactly one terminal status.
The store is intentionally separate from retry policy: a retry must use a new attempt id.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.provider import AttemptStart, AttemptTerminal, ModelAttemptStore
from agent_runtime.storage.uow import facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped

if TYPE_CHECKING:
    from sqlalchemy.engine import CursorResult, RowMapping

    from agent_runtime.contracts.model import ProviderRequest


class SqlModelAttemptStore(ModelAttemptStore):
    """Persist one provider attempt with idempotent begin and terminal CAS."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext | None = None,
        lease: Callable[[], LeaseToken] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if (actor is None) != (lease is None):
            raise ValueError("claim-bound attempts require both actor and lease")
        self._schema = require_schema(schema)
        self._actor, self._lease = actor, lease
        self._clock = clock
        self._locks = locks.build_lock_statements(self._schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._select = scoped(
            "SELECT * FROM {t}model_attempt WHERE id = :id FOR UPDATE",
            self._schema,
        )
        # Both the primary key and scoped identity index must arbitrate duplicate starts.
        self._insert = scoped(
            "INSERT INTO {t}model_attempt (id, scope_id, created_at, updated_at, run_id, "
            "context_snapshot_id, request_key, status, input_ref, usage_status, "
            "usage_reason, provider_request_id, output_ref, finish_reason, usage, failure, "
            "started_at, finished_at) VALUES (:id, :scope_id, :now, :now, :run_id, "
            ":context_snapshot_id, :request_key, 'running', :input_ref, 'pending', "
            "'pending', NULL, NULL, NULL, NULL, NULL, :now, NULL) ON CONFLICT DO NOTHING",
            self._schema,
        )
        self._finish = scoped(
            "UPDATE {t}model_attempt SET status = :status, usage_status = :usage_status, "
            "usage_reason = :usage_reason, finish_reason = :finish_reason, "
            "usage = CAST(:usage AS jsonb), failure = CAST(:failure AS jsonb), "
            "finished_at = :finished_at, updated_at = :finished_at "
            "WHERE scope_id = :scope_id AND id = :id AND status = 'running'",
            self._schema,
        )

    async def begin(self, request: ProviderRequest, *, started_at: datetime) -> AttemptStart:
        _aware(started_at, "started_at")
        async with self._sessions() as session, session.begin():
            if self._lease is not None and self._actor is not None:
                lease = self._lease()
                if (request.scope_id, request.session_id, request.run_id) != (
                    lease.scope_id,
                    lease.session_id,
                    lease.run_id,
                ):
                    raise runtime_failure(
                        "IDENTITY_MISMATCH",
                        category="concurrency",
                        public_message="Model attempt belongs to another claim.",
                    )
                facts.verify_lease(lease, now=started_at)
                facts.verify_slot(
                    await locks.lock_slot(session, self._locks, lease.scope_id, lease.session_id),
                    lease,
                )
                run = facts.verify_run(
                    await locks.lock_run(session, self._locks, lease.scope_id, lease.run_id),
                    self._actor,
                    lease,
                    now=started_at,
                )
                await revalidate_authority(
                    session, self._locks, self._actor, run=run, now=started_at
                )
                work = facts.verify_work(
                    await locks.lock_work_item(session, self._locks, lease.scope_id, lease.work_id),
                    lease,
                    now=started_at,
                )
                now = self._clock()
                _aware(now, "clock")
                # A slow lock acquisition must not authorize a paid model call
                # using the timestamp from before the wait.
                facts.verify_lease(lease, now=now)
                facts.verify_run(run, self._actor, lease, now=now)
                facts.verify_work(work, lease, now=now)
                await revalidate_authority(session, self._locks, self._actor, run=run, now=now)
                if (
                    run["status"] != "running"
                    or run["desired_control"] is not None
                    or run["deadline_at"] <= now
                ):
                    raise runtime_failure(
                        "CONTROL_PENDING",
                        category="concurrency",
                        public_message="Model execution is no longer active.",
                    )
            inserted = cast(
                "CursorResult[Any]",
                await session.execute(
                    self._insert,
                    {
                        "id": request.attempt_id,
                        "scope_id": request.scope_id,
                        "now": started_at,
                        "run_id": request.run_id,
                        "context_snapshot_id": request.context_snapshot_id,
                        "request_key": self._key(request),
                        "input_ref": request.input_ref,
                    },
                ),
            )
            if inserted.rowcount == 1:
                return "started"
            row = (await session.execute(self._select, {"id": request.attempt_id})).mappings().one()
            self._verify_existing(request, row)
            if row["status"] != "running":
                raise runtime_failure(
                    "VERSION_CONFLICT",
                    category="concurrency",
                    public_message="The model attempt is already terminal.",
                    attempt_id=str(request.attempt_id),
                )
            return "already_running"

    async def finish(self, request: ProviderRequest, terminal: AttemptTerminal) -> None:
        _validate_terminal(terminal)
        async with self._sessions() as session, session.begin():
            row = (
                (
                    await session.execute(
                        self._select,
                        {"id": request.attempt_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise runtime_failure(
                    "VERSION_CONFLICT",
                    category="concurrency",
                    public_message="The model attempt does not exist.",
                    attempt_id=str(request.attempt_id),
                )
            self._verify_existing(request, row)
            usage = terminal.usage.model_dump(mode="json", by_alias=True)
            failure = (
                terminal.failure.model_dump(mode="json", by_alias=True)
                if terminal.failure is not None
                else None
            )
            if row["status"] != "running":
                if _same_terminal(row, terminal, usage, failure):
                    return
                raise runtime_failure(
                    "VERSION_CONFLICT",
                    category="concurrency",
                    public_message="The model attempt terminal result has already been recorded.",
                    attempt_id=str(request.attempt_id),
                )
            result = cast(
                "CursorResult[Any]",
                await session.execute(
                    self._finish,
                    {
                        "status": terminal.status,
                        "usage_status": terminal.usage.status,
                        "usage_reason": terminal.usage.reason,
                        "finish_reason": terminal.finish_reason,
                        "usage": json.dumps(usage, separators=(",", ":"), ensure_ascii=False),
                        "failure": (
                            json.dumps(failure, separators=(",", ":"), ensure_ascii=False)
                            if failure is not None
                            else None
                        ),
                        "finished_at": terminal.finished_at,
                        "scope_id": request.scope_id,
                        "id": request.attempt_id,
                    },
                ),
            )
            if result.rowcount != 1:
                raise runtime_failure(
                    "FENCED",
                    category="concurrency",
                    public_message="The model attempt lost its terminal write race.",
                    attempt_id=str(request.attempt_id),
                )

    def _key(self, request: ProviderRequest) -> str:
        key = _request_key(request)
        return key if self._lease is None else key + claim_attempt_suffix(self._lease())

    def _verify_existing(self, request: ProviderRequest, row: RowMapping) -> None:
        expected = {
            "scope_id": request.scope_id,
            "run_id": request.run_id,
            "context_snapshot_id": request.context_snapshot_id,
            "request_key": self._key(request),
            "input_ref": request.input_ref,
        }
        if any(row[key] != value for key, value in expected.items()):
            raise runtime_failure(
                "IDENTITY_MISMATCH",
                category="concurrency",
                public_message="The model attempt identity conflicts with persisted facts.",
                attempt_id=str(request.attempt_id),
            )


def claim_attempt_suffix(lease: LeaseToken) -> str:
    """Persist the dispatch fence without changing the provider request identity."""
    return f":claim:{lease.work_id}:{lease.work_epoch}:{lease.run_epoch}"


def _request_key(request: ProviderRequest) -> str:
    payload = request.model_dump(mode="json", by_alias=True)
    return f"model-attempt:{request.attempt_id}:{content_digest(payload)}"


def _aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def _validate_terminal(terminal: AttemptTerminal) -> None:
    _aware(terminal.finished_at, "finished_at")
    if terminal.status not in {"completed", "failed", "cancelled", "interrupted"}:
        raise ValueError("invalid model attempt terminal status")
    if terminal.status == "completed" and (
        terminal.failure is not None or terminal.finish_reason not in {"stop", "tool_calls"}
    ):
        raise ValueError("completed attempt requires a successful finish without failure")
    if terminal.status == "failed" and terminal.failure is None:
        raise ValueError("failed attempt requires a failure envelope")
    if terminal.status in {"cancelled", "interrupted"} and terminal.failure is not None:
        raise ValueError("cancelled or interrupted attempt cannot carry failure")


def _json_value(value: object) -> object:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _same_terminal(
    row: RowMapping,
    terminal: AttemptTerminal,
    usage: dict[str, Any],
    failure: dict[str, Any] | None,
) -> bool:
    return (
        row["status"] == terminal.status
        and row["finish_reason"] == terminal.finish_reason
        and _json_value(row["usage"]) == usage
        and _json_value(row["failure"]) == failure
    )


__all__ = ["SqlModelAttemptStore"]

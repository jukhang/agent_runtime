"""``SqlSystemCapacityStore``: platform capacity outside any user scope (requirement 8).

Aggregate and reservation rows always change in one transaction, under a single row lock
on the aggregate (taken in :mod:`agent_runtime.storage.uow.locks`), so concurrent reserves
cannot oversell. Replays are decided by persisted facts, and the *payload* of a replayed
request is checked as well (review §10.2 P2-1), so a reused request id with a different
amount, holder, lease or release reason is refused instead of reported as success:

* first release of a ``reserved`` reservation -> ``applied``;
* same ``request_id`` **and same payload** on an already terminal reservation -> ``reused``;
* a different request, or the same request with a different payload -> ``rejected``/refused;
* ``expired``/``reclaimed`` reclamation requires a holder confirmed ``stopped``/``terminal``.

All statements are rendered with an explicit schema prefix (review §10.2 P1-3).
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, TypedDict, cast
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.capacity import (
    CapacityKey,
    CapacityObserveRequest,
    CapacityReleaseApplied,
    CapacityReleaseRejected,
    CapacityReleaseRequest,
    CapacityReleaseResult,
    CapacityReleaseReused,
    CapacityReservation,
    CapacityReserveRequest,
    CapacityView,
)
from agent_runtime.storage.uow import errors, locks
from agent_runtime.storage.uow.schema import require_schema, scoped

Clock = Callable[[], datetime]
type ReleaseStatus = Literal["released", "expired", "reclaimed"]

TERMINAL_RESERVATION_STATUSES = frozenset({"released", "expired", "reclaimed"})


class CapacityReservationRecord(TypedDict):
    id: UUID
    status: str
    amount: int
    version: int
    lease_until: datetime
    holder_ref: str


_REASON_STATUS = {
    "settled": "released",
    "cancelled": "released",
    "expired": "expired",
    "reclaimed": "reclaimed",
}


def utcnow() -> datetime:
    return datetime.now(UTC)


def release_request_digest(request: CapacityReleaseRequest) -> str:
    """Canonical digest of the whole release payload (Q44).

    ``holder_state`` is part of the request identity, so it is part of the digest instead
    of being unrepresentable in the reservation row.
    """
    key = request.key
    return content_digest(
        {
            "deploymentId": key.deployment_id,
            "resourceKind": key.resource_kind,
            "resourceVersion": str(key.resource_version),
            "resourceKey": key.resource_key,
            "reservationId": str(request.reservation_id),
            "requestId": request.request_id,
            "reason": request.reason,
            "holderState": request.holder_state,
        }
    )


@dataclass(frozen=True, slots=True)
class _Statements:
    count: sa.TextClause
    reservation_insert: sa.TextClause
    reservation_release: sa.TextClause
    capacity_insert: sa.TextClause
    capacity_consume: sa.TextClause
    capacity_return: sa.TextClause
    view: sa.TextClause
    snapshot: sa.TextClause


def build_statements(schema: str) -> _Statements:
    require_schema(schema)
    return _Statements(
        count=scoped(
            "SELECT capacity, reserved, version FROM {t}system_capacity "
            "WHERE deployment_id = :deployment_id AND resource_kind = :resource_kind "
            "AND resource_version = :resource_version AND resource_key = :resource_key",
            schema,
        ),
        reservation_insert=scoped(
            "INSERT INTO {t}system_capacity_reservation (id, deployment_id, resource_kind, "
            "resource_key, resource_version, status, holder_ref, request_id, amount, version, "
            "lease_until) VALUES (:id, :deployment_id, :resource_kind, :resource_key, "
            ":resource_version, 'reserved', :holder_ref, :request_id, :amount, 1, :lease_until)",
            schema,
        ),
        reservation_release=scoped(
            "UPDATE {t}system_capacity_reservation SET status = :status, released_at = :now, "
            "release_request_id = :request_id, release_reason = :reason, "
            "release_request_hash = :request_hash, version = version + 1 "
            "WHERE id = :reservation_id",
            schema,
        ),
        capacity_insert=scoped(
            "INSERT INTO {t}system_capacity (deployment_id, resource_kind, resource_key, "
            "resource_version, capacity, reserved, version) VALUES "
            "(:deployment_id, :resource_kind, :resource_key, :resource_version, :capacity, "
            "0, 0) ON CONFLICT (deployment_id, resource_kind, resource_version, resource_key) "
            "DO UPDATE SET capacity = EXCLUDED.capacity",
            schema,
        ),
        capacity_consume=scoped(
            "UPDATE {t}system_capacity SET reserved = reserved + :amount, "
            "version = version + 1, updated_at = :now WHERE deployment_id = :deployment_id "
            "AND resource_kind = :resource_kind AND resource_version = :resource_version "
            "AND resource_key = :resource_key",
            schema,
        ),
        capacity_return=scoped(
            "UPDATE {t}system_capacity SET reserved = reserved - :amount, "
            "version = version + 1, updated_at = :now WHERE deployment_id = :deployment_id "
            "AND resource_kind = :resource_kind AND resource_version = :resource_version "
            "AND resource_key = :resource_key",
            schema,
        ),
        view=scoped(
            "SELECT capacity, reserved, version FROM {t}system_capacity "
            "WHERE deployment_id = :deployment_id AND resource_kind = :resource_kind "
            "AND resource_version = :resource_version AND resource_key = :resource_key",
            schema,
        ),
        snapshot=scoped(
            "SELECT id, status, amount, request_id, release_request_id, release_reason, "
            "release_request_hash "
            "FROM {t}system_capacity_reservation WHERE deployment_id = :deployment_id "
            "AND resource_kind = :resource_kind AND resource_version = :resource_version "
            "AND resource_key = :resource_key ORDER BY created_at, id",
            schema,
        ),
    )


class SqlSystemCapacityStore:
    """Reserve / release / observe over ``system_capacity[_reservation]``."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        clock: Clock = utcnow,
    ) -> None:
        self._engine = engine
        self._schema = require_schema(schema)
        self._clock = clock
        self._locks = locks.build_lock_statements(self._schema)
        self._sql = build_statements(self._schema)
        self._session_factory = async_sessionmaker(engine, expire_on_commit=False)

    def _session(self) -> AsyncSession:
        return self._session_factory()

    @property
    def schema(self) -> str:
        return self._schema

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    async def reserve(self, request: CapacityReserveRequest) -> CapacityReservation:
        async with self._session() as session, session.begin():
            return await self.reserve_in_transaction(session, request)

    async def reserve_in_transaction(
        self,
        session: AsyncSession,
        request: CapacityReserveRequest,
        *,
        expected_capacity: int | None = None,
    ) -> CapacityReservation:
        """Reserve with a durable dispatch intent; transaction rollback refunds both."""
        if not session.in_transaction():
            raise ValueError("capacity reservation requires an active transaction")
        key = request.key
        aggregate = await locks.lock_capacity(session, self._locks, key)
        if aggregate is None:
            errors.capacity_unavailable("the capacity key is not declared")
        if expected_capacity is not None and int(aggregate["capacity"]) != expected_capacity:
            errors.version_conflict("declared capacity differs from the pinned specification")
        replay = await locks.lock_reservation_by_request(
            session, self._locks, key, request.request_id
        )
        if replay is not None:
            if (
                int(replay["amount"]) != int(request.amount)
                or replay["holder_ref"] != request.holder_ref
                or replay["lease_until"] != request.lease_until
            ):
                errors.invalid_input("the reserve request id was already used with another payload")
            if replay["status"] != "reserved":
                errors.version_conflict("the reserve request was already released")
            return CapacityReservation(
                key=key,
                reservation_id=replay["id"],
                amount=int(replay["amount"]),
                status="reserved",
                version=str(replay["version"]),
                lease_until=replay["lease_until"],
                holder_ref=replay["holder_ref"],
            )
        if int(aggregate["reserved"]) + request.amount > int(aggregate["capacity"]):
            errors.capacity_unavailable("declared capacity would be exceeded")
        reservation_id = uuid4()
        await session.execute(
            self._sql.reservation_insert,
            {
                "id": reservation_id,
                **locks.capacity_key_params(key),
                "holder_ref": request.holder_ref,
                "request_id": request.request_id,
                "amount": request.amount,
                "lease_until": request.lease_until,
            },
        )
        await session.execute(
            self._sql.capacity_consume,
            {
                **locks.capacity_key_params(key),
                "amount": request.amount,
                "now": self._clock(),
            },
        )
        return CapacityReservation(
            key=key,
            reservation_id=reservation_id,
            amount=request.amount,
            status="reserved",
            version="1",
            lease_until=request.lease_until,
            holder_ref=request.holder_ref,
        )

    async def release(self, request: CapacityReleaseRequest) -> CapacityReleaseResult:
        async with self._session() as session:
            async with session.begin():
                return await self.release_in_transaction(session, request)

    async def release_in_transaction(
        self, session: AsyncSession, request: CapacityReleaseRequest
    ) -> CapacityReleaseResult:
        """Release with the caller's transaction, so a sealed result and slot agree."""
        key = request.key
        now = self._clock()
        aggregate = await locks.lock_capacity(session, self._locks, key)
        reservation = await locks.lock_reservation(session, self._locks, request.reservation_id)
        if aggregate is None or reservation is None:
            errors.invalid_input("reservation or capacity key does not exist")
        stored_key = (
            reservation["deployment_id"],
            reservation["resource_kind"],
            int(reservation["resource_version"]),
            reservation["resource_key"],
        )
        claimed_key = (
            key.deployment_id,
            key.resource_kind,
            int(key.resource_version),
            key.resource_key,
        )
        if stored_key != claimed_key:
            errors.invalid_input("reservation belongs to another capacity key")
        status = str(reservation["status"])
        request_hash = release_request_digest(request)
        if status in TERMINAL_RESERVATION_STATUSES:
            if reservation["release_request_id"] != request.request_id:
                return CapacityReleaseRejected(
                    outcome="rejected",
                    reservation_id=reservation["id"],
                    failure=errors.conflict_failure(
                        "a terminal reservation was released by another request"
                    ),
                )
            if reservation["release_request_hash"] is None:
                return CapacityReleaseRejected(
                    outcome="rejected",
                    reservation_id=reservation["id"],
                    failure=errors.conflict_failure(
                        "legacy terminal reservation has no verifiable release payload"
                    ),
                )
            if reservation["release_request_hash"] != request_hash:
                return CapacityReleaseRejected(
                    outcome="rejected",
                    reservation_id=reservation["id"],
                    failure=errors.conflict_failure(
                        "the release request id was already used with another payload"
                    ),
                )
            return CapacityReleaseReused(
                outcome="reused",
                reservation_id=reservation["id"],
                status=cast(ReleaseStatus, status),
                version=str(reservation["version"]),
                released_at=reservation["released_at"],
            )
        if request.reason in {"expired", "reclaimed"} and request.holder_state == "unknown":
            return CapacityReleaseRejected(
                outcome="rejected",
                reservation_id=reservation["id"],
                failure=errors.conflict_failure(
                    "reclamation requires a confirmed stopped or terminal holder"
                ),
            )
        target = _REASON_STATUS[request.reason]
        amount = int(reservation["amount"])
        await session.execute(
            self._sql.reservation_release,
            {
                "status": target,
                "now": now,
                "request_id": request.request_id,
                "reason": request.reason,
                "request_hash": request_hash,
                "reservation_id": reservation["id"],
            },
        )
        await session.execute(
            self._sql.capacity_return,
            {**locks.capacity_key_params(key), "amount": amount, "now": now},
        )
        return CapacityReleaseApplied(
            outcome="applied",
            reservation_id=reservation["id"],
            status=cast(ReleaseStatus, target),
            version=str(int(reservation["version"]) + 1),
            released_at=now,
        )

    async def observe(self, request: CapacityObserveRequest) -> CapacityView:
        key = request.key
        async with self._session() as session:
            result = await session.execute(self._sql.view, locks.capacity_key_params(key))
            row = result.mappings().one_or_none()
            if row is None:
                errors.capacity_unavailable("the capacity key is not declared")
            return CapacityView(
                key=key,
                capacity=str(row["capacity"]),
                reserved=str(row["reserved"]),
                version=str(row["version"]),
            )

    async def reservation_by_request(
        self, key: CapacityKey, request_id: str
    ) -> CapacityReservationRecord | None:
        """Read one exact reservation for idempotent post-commit cleanup."""
        async with self._session() as session:
            return await self.reservation_by_request_in_transaction(session, key, request_id)

    async def reservation_by_request_in_transaction(
        self, session: AsyncSession, key: CapacityKey, request_id: str
    ) -> CapacityReservationRecord | None:
        row = (
            (
                await session.execute(
                    scoped(
                        "SELECT id,status,amount,version,lease_until,holder_ref "
                        "FROM {t}system_capacity_reservation "
                        "WHERE deployment_id=:deployment_id AND resource_kind=:resource_kind "
                        "AND resource_version=:resource_version "
                        "AND resource_key=:resource_key AND request_id=:request_id",
                        self._schema,
                    ),
                    {**locks.capacity_key_params(key), "request_id": request_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        return None if row is None else cast(CapacityReservationRecord, dict(row))

    async def seed(self, key: CapacityKey, *, capacity: int) -> None:
        """Declare one capacity counter (deployment bootstrap, not a runtime operation)."""
        async with self._session() as session:
            async with session.begin():
                await session.execute(
                    self._sql.capacity_insert,
                    {**locks.capacity_key_params(key), "capacity": capacity},
                )

    async def snapshot(self, key: CapacityKey) -> dict[str, object]:
        """Raw aggregate/reservation facts for evidence reports."""
        async with self._session() as session:
            aggregate = await session.execute(self._sql.count, locks.capacity_key_params(key))
            reservations = await session.execute(self._sql.snapshot, locks.capacity_key_params(key))
            row = aggregate.mappings().one_or_none()
            return {
                "aggregate": None if row is None else dict(row),
                "reservations": [dict(item) for item in reservations.mappings()],
            }


__all__ = [
    "TERMINAL_RESERVATION_STATUSES",
    "release_request_digest",
    "Clock",
    "SqlSystemCapacityStore",
    "utcnow",
]

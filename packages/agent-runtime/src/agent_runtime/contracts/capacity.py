"""Platform capacity outside the user scope (DS-P02-00, review P1-1).

``SystemCapacity`` is keyed by deployment, resource kind, the capacity specification
revision and a stable ``resource_key`` (supplier, concurrency group, pool or
destination). It never fabricates a user scope: the effective permission for an
operation is the intersection of this platform capacity and the host Policy decision.

A reservation is a durable fact, not just an aggregate decrement: the storage catalog
keeps ``system_capacity_reservation`` rows with holder, request identity, amount,
status and lease, and the aggregate counter changes only in the same transaction that
changes the reservation row. Release therefore returns a discriminated outcome that can
express a first release, an idempotent replay and a refusal, and expiry reclamation
must confirm the holder really stopped instead of trusting local time.

Replay is decided by persisted release facts, not by caller memory (review C4): the first
release writes ``release_request_id`` and ``release_reason`` on the terminal reservation
while ``status``/``released_at`` move together, so

* an un-released reservation plus a new request id yields ``applied``,
* an already released reservation whose stored ``release_request_id`` equals the request
  id yields ``reused``,
* an already terminal reservation with a different request id yields ``rejected``.

The reservation row is a child of ``system_capacity`` through a deferred composite
foreign key on the full capacity key, so a reservation can never outlive the aggregate
it reserves against.
"""

from typing import Annotated, Literal, NoReturn, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, TypeAdapter, model_validator

from agent_runtime.contracts.base import Contract, Identifier, MachineKey, Revision
from agent_runtime.contracts.failure import FailureEnvelope, RecoveryOption, runtime_failure

type ResourceKind = Literal["model_slot", "tool_slot", "run_slot", "sandbox_slot", "egress_slot"]
type ReleaseReason = Literal["settled", "cancelled", "expired", "reclaimed"]
type ReservationStatus = Literal["reserved", "released", "expired", "reclaimed"]
type HolderState = Literal["stopped", "terminal", "unknown"]

#: Hard ceiling on a single reservation; keeps one caller from exhausting a deployment.
MAX_RESERVATION = 1024


class CapacityKey(Contract):
    """Identity of one platform capacity counter; there is no user scope here."""

    deployment_id: Identifier
    resource_kind: ResourceKind
    resource_version: Revision
    resource_key: MachineKey


class CapacityReserveRequest(Contract):
    key: CapacityKey
    amount: Annotated[int, Field(strict=True, ge=1, le=MAX_RESERVATION)]
    holder_ref: Identifier
    request_id: MachineKey
    lease_until: AwareDatetime
    deadline_at: AwareDatetime

    @model_validator(mode="after")
    def lease_within_deadline(self) -> Self:
        if self.lease_until > self.deadline_at:
            raise ValueError("a reservation lease cannot outlive the request deadline")
        return self


class CapacityReleaseRequest(Contract):
    key: CapacityKey
    reservation_id: UUID
    request_id: MachineKey
    reason: ReleaseReason
    holder_state: HolderState

    @model_validator(mode="after")
    def never_reclaim_on_unknown_holder(self) -> Self:
        # Expiry scanning may only return capacity after the holder is confirmed gone.
        if self.reason == "reclaimed" and self.holder_state == "unknown":
            raise ValueError("reclamation requires a confirmed stopped or terminal holder")
        return self


class CapacityObserveRequest(Contract):
    key: CapacityKey


class CapacityReservation(Contract):
    key: CapacityKey
    reservation_id: UUID
    amount: Annotated[int, Field(strict=True, ge=1, le=MAX_RESERVATION)]
    status: ReservationStatus
    version: Revision
    lease_until: AwareDatetime
    holder_ref: Identifier

    @model_validator(mode="after")
    def live_reservation_is_reserved(self) -> Self:
        if self.status != "reserved":
            raise ValueError("a returned reservation must still be held")
        return self


class CapacityReleaseApplied(Contract):
    """The first release of this reservation by the current request."""

    outcome: Literal["applied"]
    reservation_id: UUID
    status: Literal["released", "expired", "reclaimed"]
    version: Revision
    released_at: AwareDatetime


class CapacityReleaseReused(Contract):
    """Idempotent replay: the same request already released this reservation."""

    outcome: Literal["reused"]
    reservation_id: UUID
    status: Literal["released", "expired", "reclaimed"]
    version: Revision
    released_at: AwareDatetime


class CapacityReleaseRejected(Contract):
    outcome: Literal["rejected"]
    reservation_id: UUID
    failure: FailureEnvelope


type CapacityReleaseResult = Annotated[
    CapacityReleaseApplied | CapacityReleaseReused | CapacityReleaseRejected,
    Field(discriminator="outcome"),
]
capacity_release_adapter: TypeAdapter[CapacityReleaseResult] = TypeAdapter(CapacityReleaseResult)


class CapacityView(Contract):
    """Observed counter for one key; callers never receive another key's numbers."""

    key: CapacityKey
    capacity: Revision
    reserved: Revision
    version: Revision

    @model_validator(mode="after")
    def consistent_counter(self) -> Self:
        if int(self.reserved) > int(self.capacity):
            raise ValueError("reserved capacity exceeds the declared capacity")
        return self


def capacity_unavailable(
    key: CapacityKey,
    *,
    requested: int,
    retry_after_seconds: int | None = None,
) -> NoReturn:
    """Raise the frozen public capacity failure: kind, retryability, suggested backoff.

    The message names the caller's own request and resource kind only. The resource key,
    available totals and reserved totals are never included, so a rejection cannot be
    used to infer another tenant's capacity.
    """
    recovery = (
        RecoveryOption(action="retry_same", label=f"Retry after {retry_after_seconds}s")
        if retry_after_seconds is not None
        else RecoveryOption(action="retry_same", label="Retry when capacity is available")
    )
    raise runtime_failure(
        "CAPACITY_UNAVAILABLE",
        category="budget",
        retryable=True,
        public_message=(
            f"Platform {key.resource_kind} capacity is unavailable for a request of {requested}."
        ),
        recovery_options=(recovery,),
    )

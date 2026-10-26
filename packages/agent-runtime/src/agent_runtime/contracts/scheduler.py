"""Main-slot scheduler contracts; claims must be verified in database transactions."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, TypeAdapter, model_validator

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.payloads import InputRequired
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.tasks import WaitSpec
from agent_runtime.contracts.transactions import CommitGuard, LeaseToken
from agent_runtime.contracts.views import Digest


class WorkerCapabilities(Contract):
    worker_id: Identifier
    runtime_revision: CapabilityRef
    capabilities: tuple[CapabilityRef, ...]

    @model_validator(mode="after")
    def unique_capabilities(self) -> Self:
        keys = [cap.key for cap in self.capabilities]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate worker capability")
        return self


class ClaimRequest(Contract):
    scope_id: Identifier
    worker: WorkerCapabilities
    lease_seconds: Annotated[int, Field(strict=True, ge=1, le=3600)]


class ClaimedWork(Contract):
    disposition: Literal["claimed"]
    lease: LeaseToken
    state_version: Revision
    checkpoint_id: UUID | None
    budget_allocation_id: UUID


class IdleWork(Contract):
    disposition: Literal["idle"]
    reason: Literal["no_work", "capacity", "capability_mismatch"]
    retry_after_ms: Annotated[int, Field(strict=True, ge=1, le=60000)]


type ClaimResult = Annotated[ClaimedWork | IdleWork, Field(discriminator="disposition")]
claim_adapter: TypeAdapter[ClaimResult] = TypeAdapter(ClaimResult)


class RenewRequest(Contract):
    lease: LeaseToken
    lease_seconds: Annotated[int, Field(strict=True, ge=1, le=3600)]


class RenewedLease(Contract):
    disposition: Literal["renewed"]
    lease: LeaseToken


class LostLease(Contract):
    disposition: Literal["lost"]
    failure: FailureEnvelope


type RenewResult = Annotated[RenewedLease | LostLease, Field(discriminator="disposition")]
renew_adapter: TypeAdapter[RenewResult] = TypeAdapter(RenewResult)


class InputWatermark(Contract):
    pending_version: Revision
    applied_version: Revision

    @model_validator(mode="after")
    def monotonic(self) -> Self:
        if int(self.applied_version) > int(self.pending_version):
            raise ValueError("applied input version exceeds pending version")
        return self


class YieldRequest(Contract):
    guard: CommitGuard
    checkpoint_id: UUID
    wait_spec: WaitSpec
    inputs: InputWatermark
    input_request: InputRequired | None = None

    @model_validator(mode="after")
    def matching_input_request(self) -> Self:
        if self.input_request is not None and (
            self.wait_spec.kind != "message"
            or self.wait_spec.subject_id != str(self.guard.lease.run_id)
            or self.input_request.reply_to_run_id != self.guard.lease.run_id
            or int(self.input_request.state_version) != int(self.guard.expected_state_version) + 1
        ):
            raise ValueError("input request must wait for a message to this run")
        return self


class YieldBase(Contract):
    run_id: UUID
    state_version: Revision
    checkpoint_id: UUID
    inputs: InputWatermark


class WaitingRun(YieldBase):
    disposition: Literal["waiting"]
    status: Literal["waiting_tool", "waiting_user"]

    @model_validator(mode="after")
    def no_unhandled_inputs(self) -> Self:
        if self.inputs.pending_version != self.inputs.applied_version:
            raise ValueError("waiting would lose pending inputs")
        return self


class RequeuedRun(YieldBase):
    disposition: Literal["requeued"]
    work_id: UUID

    @model_validator(mode="after")
    def pending_input_exists(self) -> Self:
        if self.inputs.pending_version == self.inputs.applied_version:
            raise ValueError("input-triggered requeue requires pending input")
        return self


type YieldResult = Annotated[WaitingRun | RequeuedRun, Field(discriminator="disposition")]
yield_adapter: TypeAdapter[YieldResult] = TypeAdapter(YieldResult)


class WakeInputRequest(Contract):
    scope_id: Identifier
    session_id: UUID
    run_id: UUID
    result_key: Identifier
    result_digest: Digest
    result_ref: Identifier


class WakeBase(Contract):
    run_id: UUID
    inputs: InputWatermark


class WakePending(WakeBase):
    @model_validator(mode="after")
    def pending_input_exists(self) -> Self:
        if self.inputs.pending_version == self.inputs.applied_version:
            raise ValueError("new wake input must advance pending watermark")
        return self


class WakeScheduled(WakePending):
    disposition: Literal["scheduled"]
    work_id: UUID


class WakeDeferred(WakePending):
    disposition: Literal["deferred"]


class WakeDuplicate(WakeBase):
    disposition: Literal["duplicate"]


class WakeTerminal(WakeBase):
    disposition: Literal["terminal"]
    status: Literal["completed", "failed", "cancelled"]


type WakeResult = Annotated[
    WakeScheduled | WakeDeferred | WakeDuplicate | WakeTerminal, Field(discriminator="disposition")
]
wake_adapter: TypeAdapter[WakeResult] = TypeAdapter(WakeResult)

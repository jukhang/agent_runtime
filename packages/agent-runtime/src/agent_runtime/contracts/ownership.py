"""Separate child and maintenance authority shapes from the main session slot."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, TypeAdapter, model_validator

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.message_payloads import ResultProposed
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.transactions import ActorContext, MaintenanceLease


class ChildLeaseToken(Contract):
    scope_id: Identifier
    session_id: UUID
    run_id: UUID
    work_id: UUID
    owner_id: Identifier
    run_epoch: Revision
    work_epoch: Revision
    run_lease_until: AwareDatetime
    work_lease_until: AwareDatetime


class ChildCommitGuard(Contract):
    actor: ActorContext
    lease: ChildLeaseToken
    expected_state_version: Revision
    root_run_id: UUID
    expected_root_cancel_version: Revision
    workspace_ref: ResourceRef
    workspace_owner_run_id: UUID
    workspace_generation: Revision

    @model_validator(mode="after")
    def own_workspace(self) -> Self:
        if self.actor.scope_id != self.lease.scope_id:
            raise ValueError("actor and child lease scopes disagree")
        if self.root_run_id == self.lease.run_id:
            raise ValueError("child run must be distinct from its root")
        if self.workspace_owner_run_id != self.lease.run_id:
            raise ValueError("child cannot submit another run's workspace")
        if (
            self.workspace_ref.resource_type != "workspace"
            or self.workspace_ref.scope.scope_id != self.lease.scope_id
        ):
            raise ValueError("workspace reference type or scope mismatch")
        if self.workspace_ref.resource_version is None:
            # A floating reference would let a child submit against whatever revision is
            # current at commit time instead of the revision it validated (F-B5).
            raise ValueError("child workspace reference must pin its resource version")
        if int(self.workspace_generation) < 1:
            raise ValueError("workspace generation starts at 1")
        return self


class ChildProposalRequest(Contract):
    guard: ChildCommitGuard
    task_id: UUID
    proposal: ResultProposed


class ExecutionReconcileRequest(Contract):
    actor: ActorContext
    lease: MaintenanceLease
    execution_ref: ResourceRef
    generation: Revision

    @model_validator(mode="after")
    def execution_scope(self) -> Self:
        if self.actor.scope_id != self.lease.scope_id:
            raise ValueError("maintenance actor and lease scopes disagree")
        if (
            self.execution_ref.resource_type != "execution"
            or self.execution_ref.scope.scope_id != self.lease.scope_id
        ):
            raise ValueError("execution reference type or scope mismatch")
        return self


class ExecutionObservation(Contract):
    execution_ref: ResourceRef
    generation: Revision
    observed_at: AwareDatetime
    state: Literal[
        "pending",
        "starting",
        "running",
        "cancelling",
        "completed",
        "failed",
        "cancelled",
        "unknown",
    ]
    evidence_ref: Identifier
    failure: FailureEnvelope | None

    @model_validator(mode="after")
    def honest_observation(self) -> Self:
        if self.execution_ref.resource_type != "execution":
            raise ValueError("observation requires an execution reference")
        if self.state == "failed" and self.failure is None:
            raise ValueError("failed execution requires failure information")
        if self.state not in {"failed", "unknown"} and self.failure is not None:
            raise ValueError("failure must be recorded as failed or unknown execution")
        return self


class ExecutionCancelRequest(ExecutionReconcileRequest):
    command_id: UUID
    root_run_id: UUID
    expected_cancel_version: Revision
    deadline_at: AwareDatetime


class ExecutionCancelResult(Contract):
    command_id: UUID
    observation: ExecutionObservation
    disposition: Literal["requested", "stopped", "unknown"]
    supervision_work_id: UUID | None

    @model_validator(mode="after")
    def require_stop_evidence(self) -> Self:
        if self.disposition == "stopped" and self.observation.state not in {
            "completed",
            "failed",
            "cancelled",
        }:
            raise ValueError("cancellation acknowledgement is not proof of a stopped process")
        if self.disposition != "stopped" and self.supervision_work_id is None:
            raise ValueError("unresolved cancellation requires durable supervision")
        return self


class StaleRunRef(Contract):
    scope_id: Identifier
    session_id: UUID
    run_id: UUID
    expected_state_version: Revision
    expected_owner_epoch: Revision
    owner_id: Identifier | None
    lease_until: AwareDatetime | None

    @model_validator(mode="after")
    def observed_lease_pair(self) -> Self:
        if (self.owner_id is None) != (self.lease_until is None):
            raise ValueError("observed owner and lease must both be present or null")
        return self


class RecoveryRequest(Contract):
    actor: ActorContext
    lease: MaintenanceLease
    target: StaleRunRef

    @model_validator(mode="after")
    def recovery_scope(self) -> Self:
        if (
            self.actor.scope_id != self.lease.scope_id
            or self.target.scope_id != self.lease.scope_id
        ):
            raise ValueError("recovery target, actor and lease scopes must match")
        return self


class RecoveryScheduled(Contract):
    disposition: Literal["scheduled"]
    run_id: UUID
    work_id: UUID
    state_version: Revision
    status: Literal["queued", "cancelling"]
    wake_reason: Literal["recovery", "control"]

    @model_validator(mode="after")
    def preserve_cancellation(self) -> Self:
        if (self.status == "cancelling") != (self.wake_reason == "control"):
            raise ValueError("cancel recovery must remain in control branch")
        return self


class RecoveryDeferred(Contract):
    disposition: Literal["deferred"]
    run_id: UUID
    reason: Literal["active_lease", "paused", "capacity", "incompatible", "state_changed"]
    recheck_at: AwareDatetime | None


class RecoveryTerminal(Contract):
    disposition: Literal["terminal"]
    run_id: UUID
    status: Literal["completed", "failed", "cancelled"]
    state_version: Revision


type RecoveryDisposition = Annotated[
    RecoveryScheduled | RecoveryDeferred | RecoveryTerminal, Field(discriminator="disposition")
]
recovery_adapter: TypeAdapter[RecoveryDisposition] = TypeAdapter(RecoveryDisposition)

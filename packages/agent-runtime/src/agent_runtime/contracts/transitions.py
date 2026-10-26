"""State transition requests; observed state and proofs require transactional revalidation.

DS-P01-08 (F4/F5): terminal commits carry an explicit flush-evidence union instead of a
possibly-empty proof tuple, and the state service returns a typed control outcome for
pause/resume/cancel without invoking the model.
"""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, TypeAdapter, model_validator

from agent_runtime.contracts.base import Contract, Identifier, MachineKey, Revision
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.journal import FlushProof
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.results import RunResult
from agent_runtime.contracts.state import DesiredControl, RunStatus
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.contracts.views import Control

TERMINAL = {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}


class StateWriteGuard(Contract):
    actor: ActorContext
    lease: LeaseToken
    expected_state_version: Revision
    status: RunStatus
    desired_control: DesiredControl | None
    control_command_id: UUID | None
    cancel_version: Revision

    @model_validator(mode="after")
    def consistent_control(self) -> Self:
        if self.actor.scope_id != self.lease.scope_id:
            raise ValueError("actor and lease scope mismatch")
        if (self.desired_control is None) != (self.control_command_id is None):
            raise ValueError("control and command identity must be present together")
        if (self.status == RunStatus.CANCELLING) != (self.desired_control == DesiredControl.CANCEL):
            raise ValueError("cancelling state and cancel command must match")
        if self.status in TERMINAL:
            raise ValueError("terminal run cannot acquire a state transition guard")
        return self


class ExecutionResolution(Contract):
    execution_ref: ResourceRef
    generation: Revision
    state: Literal["stopped", "isolated", "unknown"]
    evidence_ref: Identifier
    supervision_work_id: UUID | None

    @model_validator(mode="after")
    def supervised_unresolved_execution(self) -> Self:
        if self.execution_ref.resource_type != "execution":
            raise ValueError("execution resolution requires an execution reference")
        if self.state != "stopped" and self.supervision_work_id is None:
            raise ValueError("isolated or unknown execution requires persistent supervision")
        return self


class VerifiedEvidence(Contract):
    """Output flush proofs enumerated by the storage transaction for this run."""

    kind: Literal["verified"]
    proofs: Annotated[tuple[FlushProof, ...], Field(min_length=1)]
    enumerated_session_seq: Revision


class NoProducersConfirmed(Contract):
    """The storage transaction enumerated this run's output producers and found none.

    Only the storage UnitOfWork may issue this value. T9 must still re-enumerate the
    database facts before completing a run: the union prevents *omitting* evidence, it
    does not prevent *forging* it.
    """

    kind: Literal["no_producers"]
    run_id: UUID
    enumerated_session_seq: Revision


type FlushEvidence = Annotated[VerifiedEvidence | NoProducersConfirmed, Field(discriminator="kind")]


class StateCommitBase(Contract):
    guard: StateWriteGuard
    checkpoint_id: UUID
    evidence: FlushEvidence
    executions: tuple[ExecutionResolution, ...]

    @model_validator(mode="after")
    def owned_evidence(self) -> Self:
        lease = self.guard.lease
        if int(self.evidence.enumerated_session_seq) < 1:
            raise ValueError("enumerated session watermark must be positive")
        if isinstance(self.evidence, VerifiedEvidence):
            proofs = [(proof.attempt_id, proof.item_id) for proof in self.evidence.proofs]
            ids = [proof.proof_id for proof in self.evidence.proofs]
            if len(proofs) != len(set(proofs)) or len(ids) != len(set(ids)):
                raise ValueError("duplicate flush proof or output producer")
            for proof in self.evidence.proofs:
                if (proof.scope_id, proof.session_id, proof.run_id) != (
                    lease.scope_id,
                    lease.session_id,
                    lease.run_id,
                ):
                    raise ValueError("flush proof belongs to a different run or scope")
            # Enumerating cannot report a watermark behind evidence it just enumerated.
            durable = max(int(proof.durable_seq) for proof in self.evidence.proofs)
            if int(self.evidence.enumerated_session_seq) < durable:
                raise ValueError("enumerated session watermark is behind a durable proof")
        elif self.evidence.run_id != lease.run_id:
            raise ValueError("no-producer confirmation belongs to a different run")
        execution_ids = [item.execution_ref.resource_id for item in self.executions]
        if len(execution_ids) != len(set(execution_ids)):
            raise ValueError("duplicate execution resolution")
        if any(item.execution_ref.scope.scope_id != lease.scope_id for item in self.executions):
            raise ValueError("execution evidence belongs to another scope")
        return self


class PauseCommitRequest(StateCommitBase):
    @model_validator(mode="after")
    def pause_at_safe_point(self) -> Self:
        if self.guard.desired_control != DesiredControl.PAUSE or self.guard.status not in {
            RunStatus.RUNNING,
            RunStatus.QUEUED,
            RunStatus.WAITING_TOOL,
            RunStatus.WAITING_USER,
        }:
            raise ValueError("pause requires an active run and its pending pause command")
        if any(item.state == "unknown" for item in self.executions):
            raise ValueError("cannot pause while execution isolation is unknown")
        return self


class TerminalCommitRequest(StateCommitBase):
    result: RunResult

    @model_validator(mode="after")
    def terminal_conditions(self) -> Self:
        if self.result.run_id != self.guard.lease.run_id:
            raise ValueError("terminal result targets a different run")
        if self.result.status == "completed":
            if self.guard.status != RunStatus.RUNNING or self.guard.desired_control is not None:
                raise ValueError("completion requires running without pending control")
            if any(item.state == "unknown" for item in self.executions):
                raise ValueError("cannot complete with unknown external execution")
        if self.result.status == "cancelled":
            if self.guard.status != RunStatus.CANCELLING:
                raise ValueError("cancellation requires the current cancel command")
            if any(item.state != "stopped" for item in self.executions):
                raise ValueError("cancellation requires confirmed stopped executions")
        return self


class CancelCommitRequest(StateCommitBase):
    """Safe-point cancellation commit (DS-P02-02 second repair, Q47).

    Cancellation may only be *applied* with a full safe-point bundle: the worker lease
    guard, the new checkpoint, flush evidence and execution resolutions. The generic
    ``apply_control`` path that revoked execution rights without evidence is removed; the
    trusted supervisor uses this request at a safe point, and ``commit_terminal`` finishes
    the run once every execution is confirmed stopped.
    """

    @model_validator(mode="after")
    def cancelling_with_pending_command(self) -> Self:
        if self.guard.desired_control != DesiredControl.CANCEL:
            raise ValueError("cancel commit requires the persisted cancel command")
        if self.guard.status != RunStatus.CANCELLING:
            raise ValueError("cancel commit requires the cancelling state")
        return self


class UpdatedStateRef(Contract):
    scope_id: Identifier
    session_id: UUID
    run_id: UUID
    state_version: Revision
    checkpoint_id: UUID
    status: RunStatus
    committed_seq: Revision


class AdvanceCommitRequest(StateCommitBase):
    next_action_ref: Identifier

    @model_validator(mode="after")
    def active_without_control(self) -> Self:
        if self.guard.status != RunStatus.RUNNING or self.guard.desired_control is not None:
            raise ValueError("normal advancement requires running without pending control")
        return self


class ResumeCommitRequest(Contract):
    actor: ActorContext
    session_id: UUID
    run_id: UUID
    command_id: UUID
    expected_state_version: Revision
    expected_cancel_version: Revision
    checkpoint_id: UUID
    idempotency_key: MachineKey
    # No fabricated worker lease: resume is an authenticated control transaction.


class ResumedRun(Contract):
    scope_id: Identifier
    session_id: UUID
    run_id: UUID
    command_id: UUID
    work_id: UUID
    state_version: Revision
    status: Literal["queued"]
    desired_control: None
    control_command_id: None
    wake_reason: Literal["resume"]


class ControlApplied(Contract):
    """A persisted pause/resume/cancel command reached its safe-point effect.

    The control branch runs as trusted program code: it never calls a model and never
    consumes model budget. `work_id` is set only when the transaction created the
    deduplicated resume work for this command.
    """

    outcome: Literal["applied"]
    command_id: UUID
    run_id: UUID
    kind: Control
    status: RunStatus
    state_version: Revision
    desired_control: DesiredControl | None
    work_id: UUID | None

    @model_validator(mode="after")
    def applied_consistency(self) -> Self:
        expected: dict[str, set[RunStatus]] = {
            "pause": {RunStatus.PAUSED},
            "resume": {RunStatus.QUEUED, RunStatus.WAITING_TOOL, RunStatus.WAITING_USER},
            "cancel": {RunStatus.CANCELLING, RunStatus.CANCELLED},
        }
        if self.status not in expected[self.kind]:
            raise ValueError("control outcome status does not match the command")
        if self.kind == "cancel":
            # Cancelling still carries the cancel command; the terminal state clears it.
            expected_control = (
                DesiredControl.CANCEL if self.status == RunStatus.CANCELLING else None
            )
            if self.desired_control != expected_control:
                raise ValueError("cancel application must match the control lifecycle")
        elif self.desired_control is not None:
            raise ValueError("non-cancel control application must clear the control fields")
        if self.kind == "resume":
            if self.work_id is None:
                raise ValueError("resume application must reference its resume work")
        elif self.work_id is not None:
            # Only resume creates deduplicated advance work; a pause or cancel receipt
            # must not imply that a new work item exists (F-B1).
            raise ValueError("only resume application may create resume work")
        return self


class ControlRejected(Contract):
    """A persisted control command was refused with a structured, public reason."""

    outcome: Literal["rejected"]
    command_id: UUID
    run_id: UUID
    failure: FailureEnvelope


type ControlOutcome = Annotated[ControlApplied | ControlRejected, Field(discriminator="outcome")]
control_outcome_adapter: TypeAdapter[ControlOutcome] = TypeAdapter(ControlOutcome)

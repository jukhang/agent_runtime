"""Transaction request shapes; the database must validate all claimed authority."""

from typing import Annotated, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from agent_runtime.contracts.base import Contract, Identifier, MachineKey, Revision
from agent_runtime.contracts.references import CapabilityRef, ResourceRef


class ActorContext(Contract):
    scope_id: Identifier
    actor_id: Identifier
    auth_revision: Revision
    grants_ref: Identifier


class LeaseToken(Contract):
    scope_id: Identifier
    session_id: UUID
    run_id: UUID
    work_id: UUID
    owner_id: Identifier
    run_epoch: Revision
    work_epoch: Revision
    slot_version: Revision
    run_lease_until: AwareDatetime
    work_lease_until: AwareDatetime


class CommitGuard(Contract):
    actor: ActorContext
    lease: LeaseToken
    expected_state_version: Revision

    @model_validator(mode="after")
    def same_scope(self) -> Self:
        if self.actor.scope_id != self.lease.scope_id:
            raise ValueError("actor/lease scope mismatch")
        return self


class PrepareModelRequest(Contract):
    guard: CommitGuard
    attempt_id: UUID
    context_snapshot_id: UUID
    request_key: MachineKey
    request_ref: Identifier
    budget_allocation_id: UUID


class ClaimedInput(Contract):
    message_id: UUID
    claim_token: UUID
    recipient_seq: Revision


class ApplyInputRequest(Contract):
    guard: CommitGuard
    inputs: Annotated[tuple[ClaimedInput, ...], Field(min_length=1, max_length=64)]
    expected_applied_seq: Revision
    checkpoint_ref: Identifier

    @model_validator(mode="after")
    def ordered_claims(self) -> Self:
        ids = [entry.message_id for entry in self.inputs]
        seqs = [int(entry.recipient_seq) for entry in self.inputs]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate claimed input")
        # T5 applies the mailbox in order: a gap would silently drop or defer messages
        # while ``AppliedInputs`` later demands an unbroken range (DS-P01-05).
        start = int(self.expected_applied_seq)
        if seqs != list(range(start + 1, start + 1 + len(seqs))):
            raise ValueError("claimed inputs must be contiguous from the applied watermark")
        return self


class DomainCommitRequest(Contract):
    guard: CommitGuard
    operation_id: UUID
    resource: ResourceRef
    action: Identifier
    change_ref: Identifier

    @model_validator(mode="after")
    def resource_scope(self) -> Self:
        if self.resource.scope.scope_id != self.guard.actor.scope_id:
            raise ValueError("cross-scope domain commit is not supported")
        if self.resource.resource_version is None:
            raise ValueError("existing resource commit requires expected resource version")
        return self


class PreparedTarget(Contract):
    scope_id: Identifier
    session_id: UUID
    run_id: UUID
    expected_state_version: Revision
    checkpoint_id: UUID


class HandoffRequest(Contract):
    source_guard: CommitGuard
    target: PreparedTarget
    handoff_id: UUID
    transfer_manifest_ref: Identifier

    @model_validator(mode="after")
    def compatible_participants(self) -> Self:
        source, target = self.source_guard, self.target
        if source.lease.scope_id != target.scope_id:
            raise ValueError("handoff requires same scope")
        if source.lease.session_id != target.session_id:
            raise ValueError("handoff requires same session")
        if source.lease.run_id == target.run_id:
            raise ValueError("handoff requires distinct runs")
        return self


class MaintenanceLease(Contract):
    scope_id: Identifier
    work_id: UUID
    owner_id: Identifier
    work_epoch: Revision
    work_lease_until: AwareDatetime


class CheckpointMigrationRequest(Contract):
    actor: ActorContext
    lease: MaintenanceLease
    run_id: UUID
    expected_state_version: Revision
    source_checkpoint_id: UUID
    target_checkpoint_id: UUID
    expected_representation_revision: Revision
    target_schema_revision: Annotated[int, Field(strict=True, ge=1)]
    migrator: CapabilityRef

    @model_validator(mode="after")
    def preserve_source(self) -> Self:
        if self.actor.scope_id != self.lease.scope_id:
            raise ValueError("actor/maintenance lease scope mismatch")
        if self.source_checkpoint_id == self.target_checkpoint_id:
            raise ValueError("migration must preserve the source checkpoint")
        return self

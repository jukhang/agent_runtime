"""Internal messages describe work and observations, never authority."""

from uuid import UUID

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.references import CapabilityRef, ResourceRef
from agent_runtime.contracts.tasks import NonblankText, ValidationEvidence


class TaskAssign(Contract):
    objective: NonblankText
    constraints: tuple[str, ...]
    acceptance_spec_ref: Identifier
    output_schema_ref: Identifier
    budget_allocation_id: UUID


class TaskAccepted(Contract):
    assignment_message_id: UUID


class TaskRejected(TaskAccepted):
    code: Identifier
    public_reason: NonblankText


class InputAppend(Contract):
    text: NonblankText
    expected_task_revision: Revision


class ClarificationRequest(Contract):
    question: NonblankText
    required_facts: tuple[NonblankText, ...]
    reply_to_run_id: UUID


class ProgressUpdated(Contract):
    step_ref: UUID | None
    public_summary: NonblankText
    evidence_refs: tuple[ResourceRef, ...]


class ResultProposed(Contract):
    result_id: UUID
    summary: NonblankText
    artifact_refs: tuple[ResourceRef, ...]
    evidence_refs: tuple[ResourceRef, ...]
    validation: ValidationEvidence
    remaining_issues: tuple[str, ...]


class ResultVerification(Contract):
    result_id: UUID
    verification_ref: Identifier
    public_reason: NonblankText


class TaskFailed(Contract):
    failure: FailureEnvelope
    partial_result_refs: tuple[ResourceRef, ...]


class ControlCancel(Contract):
    command_id: UUID
    reason: NonblankText


class HandoffRequest(Contract):
    handoff_id: UUID
    target_agent_revision: CapabilityRef
    transfer_manifest_ref: Identifier
    expected_slot_version: Revision


class HandoffResponse(Contract):
    handoff_id: UUID
    public_reason: NonblankText


class HandoffCommitted(Contract):
    handoff_id: UUID
    new_owner_run_id: UUID
    slot_version: Revision

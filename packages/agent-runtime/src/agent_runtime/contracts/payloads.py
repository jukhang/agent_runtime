"""Closed event payload shapes; no untyped event catch-all."""

from typing import Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, JsonValue, model_validator

from agent_runtime.contracts.base import Contract, Identifier, MachineKey, Revision
from agent_runtime.contracts.failure import FailureEnvelope, RecoveryOption
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.tasks import NonblankText
from agent_runtime.contracts.views import (
    Channel,
    Digest,
    ItemType,
    Phase,
    PlanStep,
    Role,
    TimelineItem,
    validate_phase,
    validate_steps,
)


class TurnAccepted(Contract):
    turn_id: UUID
    run_id: UUID
    user_item: TimelineItem
    assistant_item_id: UUID

    @model_validator(mode="after")
    def user_message(self) -> Self:
        if self.user_item.role != "user" or self.user_item.type != "message":
            raise ValueError("accepted input must be a user message")
        if self.assistant_item_id == self.user_item.id:
            raise ValueError("user and assistant item IDs must differ")
        return self


class ItemStarted(Contract):
    item_type: ItemType
    role: Role
    channels: tuple[Channel, ...]
    item_revision: Revision

    @model_validator(mode="after")
    def distinct_channels(self) -> Self:
        if len(self.channels) != len(set(self.channels)):
            raise ValueError("duplicate channel")
        return self


class ContentDelta(Contract):
    channel: Literal["content"]
    delta_index: Revision
    item_revision: Revision
    text: str


class AnalysisDelta(Contract):
    channel: Literal["analysis"]
    delta_index: Revision
    item_revision: Revision
    text: str


class ChannelDigest(Contract):
    channel: Channel
    digest: Digest


class ChannelIndex(Contract):
    channel: Channel
    last_delta_index: Revision | None


class ItemCompleted(Contract):
    item_revision: Revision
    status: Literal["completed", "failed", "cancelled"]
    channel_digests: tuple[ChannelDigest, ...]
    last_delta_indexes: tuple[ChannelIndex, ...]

    @model_validator(mode="after")
    def matching_channels(self) -> Self:
        digests = [entry.channel for entry in self.channel_digests]
        indexes = [entry.channel for entry in self.last_delta_indexes]
        if len(set(digests)) != len(digests) or len(set(indexes)) != len(indexes):
            raise ValueError("duplicate completed channel")
        if set(digests) != set(indexes):
            raise ValueError("digest/index channels must match")
        return self


class RunStatusPayload(Contract):
    state_version: Revision
    status: RunStatus
    phase: Phase
    phase_started_at: AwareDatetime
    public_reason: str

    @model_validator(mode="after")
    def valid_phase(self) -> Self:
        validate_phase(self.status, self.phase)
        return self


class InputRequired(Contract):
    state_version: Revision
    question: NonblankText
    required_facts: tuple[NonblankText, ...]
    reply_to_run_id: UUID


class InputAccepted(Contract):
    state_version: Revision
    reply_to_run_id: UUID
    user_item: TimelineItem

    @model_validator(mode="after")
    def user_reply(self) -> Self:
        if (
            self.user_item.role != "user"
            or self.user_item.type != "message"
            or self.user_item.status != "completed"
        ):
            raise ValueError("accepted reply requires a completed user message")
        return self


class PlanUpdated(Contract):
    plan_id: UUID
    revision: Revision
    steps: tuple[PlanStep, ...]
    public_reason: str

    @model_validator(mode="after")
    def valid_steps(self) -> Self:
        validate_steps(self.steps)
        return self


class ToolStarted(Contract):
    tool_call_id: UUID
    name: Identifier
    public_arguments: dict[str, JsonValue]
    started_at: AwareDatetime


class ToolProgress(Contract):
    tool_call_id: UUID
    progress_revision: Revision
    summary: str
    execution_ref: ResourceRef | None


class ToolFinished(Contract):
    tool_call_id: UUID
    status: Literal["completed", "failed", "cancelled", "not_executed"]
    result_refs: tuple[ResourceRef, ...]
    failure: FailureEnvelope | None
    finished_at: AwareDatetime

    @model_validator(mode="after")
    def valid_failure(self) -> Self:
        if self.status == "completed" and self.failure is not None:
            raise ValueError("successful tool cannot have failure")
        if self.status in {"failed", "not_executed"} and self.failure is None:
            raise ValueError("failed or unexecuted tool requires failure")
        return self


class ArtifactCreated(Contract):
    resource_ref: ResourceRef
    title: str
    mime: Identifier
    preview_status: Literal["pending", "ready", "failed", "unavailable"]


class ApprovalRequired(Contract):
    approval_ref: ResourceRef
    summary: str
    expires_at: AwareDatetime
    allowed_decisions: tuple[Literal["approved", "rejected", "cancelled"], ...]


class ApprovalUpdated(Contract):
    approval_ref: ResourceRef
    revision: Revision
    status: Literal["approved", "rejected", "expired", "cancelled"]
    expires_at: AwareDatetime
    public_reason: NonblankText

    @model_validator(mode="after")
    def pinned_revision(self) -> Self:
        if self.approval_ref.resource_type != "approval" or (
            self.approval_ref.resource_version != self.revision or int(self.revision) < 1
        ):
            raise ValueError("approval update requires its current pinned revision")
        return self


class CommandUpdated(Contract):
    command_id: UUID
    status: Literal["accepted", "applied", "rejected"]
    public_reason: str


class TerminalPayload(Contract):
    state_version: Revision
    final_item_ids: tuple[UUID, ...]
    partial_result_refs: tuple[ResourceRef, ...]
    # Optional only for replay of previously persisted events.
    public_message: NonblankText | None = None
    next_steps: tuple[RecoveryOption, ...] = ()


class RunCompleted(TerminalPayload):
    outcome: Literal["success", "partial"]
    failure: None


class RunFailed(TerminalPayload):
    outcome: None
    failure: FailureEnvelope


class RunCancelled(TerminalPayload):
    outcome: None
    failure: None


class ResourceRevoked(Contract):
    resource_id: Identifier
    auth_revision: Revision


class ModelPrepared(Contract):
    attempt_id: UUID
    context_snapshot_id: UUID
    request_key: MachineKey


class MailboxApplied(Contract):
    input_ids: tuple[UUID, ...]
    applied_input_version: Revision
    checkpoint_id: UUID


class HandoffCommitted(Contract):
    handoff_id: UUID
    source_run_id: UUID
    target_run_id: UUID
    slot_version: Revision


class CheckpointMigrated(Contract):
    source_checkpoint_id: UUID
    target_checkpoint_id: UUID
    representation_revision: Revision
    state_version: Revision

"""Snapshots and typed projections shared by events and run results."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, JsonValue, StringConstraints, model_validator

from agent_runtime.contracts.base import Contract, Revision
from agent_runtime.contracts.failure import FailureEnvelope, RecoveryOption
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.state import RunStatus

type Channel = Literal["content", "analysis"]
type ItemStatus = Literal["pending", "streaming", "completed", "failed", "cancelled"]
type ItemType = Literal["message", "tool", "approval", "artifact", "run"]
type Role = Literal["user", "assistant", "tool", "system"]
type Phase = Literal[
    "queued",
    "context",
    "model",
    "tool",
    "waiting",
    "paused",
    "cancelling",
    "terminal",
]
type Control = Literal["pause", "resume", "cancel"]
Digest = Annotated[str, StringConstraints(strict=True, pattern=r"^[a-f0-9]{64}$")]


def validate_phase(status: RunStatus, phase: Phase) -> None:
    expected: set[str]
    if status == RunStatus.RUNNING:
        expected = {"context", "model", "tool"}
    elif status in {RunStatus.WAITING_TOOL, RunStatus.WAITING_USER}:
        expected = {"waiting"}
    elif status in {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}:
        expected = {"terminal"}
    else:
        expected = {status.value}
    if phase not in expected:
        raise ValueError("phase disagrees with run status")


class ChannelState(Contract):
    name: Channel
    text: str
    last_delta_index: Revision | None
    content_hash: Digest


class TimelineItem(Contract):
    id: UUID
    type: ItemType
    role: Role
    created_seq: Revision
    revision: Revision
    status: ItemStatus
    channels: tuple[ChannelState, ...]
    resource_refs: tuple[ResourceRef, ...]

    @model_validator(mode="after")
    def unique_channels(self) -> Self:
        names = [channel.name for channel in self.channels]
        if len(names) != len(set(names)):
            raise ValueError("duplicate channel")
        return self


class RunView(Contract):
    id: UUID
    parent_run_id: UUID | None = None
    status: RunStatus
    state_version: Revision
    phase: Phase
    phase_started_at: AwareDatetime
    allowed_controls: tuple[Control, ...]
    failure: FailureEnvelope | None = None
    public_message: str | None = None
    next_steps: tuple[RecoveryOption, ...] = ()
    partial_result_refs: tuple[ResourceRef, ...] = ()

    @model_validator(mode="after")
    def consistent_controls(self) -> Self:
        validate_phase(self.status, self.phase)
        if len(self.allowed_controls) != len(set(self.allowed_controls)):
            raise ValueError("duplicate control")
        if self.status in {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}:
            if self.phase != "terminal" or self.allowed_controls:
                raise ValueError("terminal run must have terminal phase and no controls")
        elif self.phase == "terminal":
            raise ValueError("active run cannot have terminal phase")
        if "resume" in self.allowed_controls and self.status != RunStatus.PAUSED:
            raise ValueError("only paused runs may expose resume")
        if self.status == RunStatus.CANCELLING and self.allowed_controls:
            raise ValueError("cancelling run exposes no controls")
        if "pause" in self.allowed_controls and self.status not in {
            RunStatus.RUNNING,
            RunStatus.QUEUED,
        }:
            raise ValueError("pause control is only exposed for queued/running runs")
        return self


class PlanStep(Contract):
    id: UUID
    parent_step_id: UUID | None
    label: Annotated[str, StringConstraints(min_length=1)]
    status: Literal["pending", "running", "waiting", "completed", "failed", "cancelled"]
    evidence_refs: tuple[ResourceRef, ...]
    run_refs: tuple[UUID, ...]


def validate_steps(steps: tuple[PlanStep, ...]) -> None:
    parents = {step.id: step.parent_step_id for step in steps}
    if len(parents) != len(steps):
        raise ValueError("duplicate step ID")
    resolved: set[UUID] = set()
    for start in parents:
        path: set[UUID] = set()
        current: UUID | None = start
        while current is not None and current not in resolved:
            if current not in parents:
                raise ValueError("unknown parent step")
            if current in path:
                raise ValueError("cyclic step hierarchy")
            path.add(current)
            current = parents[current]
        resolved.update(path)


class PlanView(Contract):
    id: UUID
    revision: Revision
    steps: tuple[PlanStep, ...]

    @model_validator(mode="after")
    def valid_steps(self) -> Self:
        validate_steps(self.steps)
        return self


class ToolActivity(Contract):
    id: UUID
    run_id: UUID
    name: str
    public_arguments: dict[str, JsonValue]
    status: Literal["running", "completed", "failed", "cancelled", "not_executed"]
    summary: str
    result_refs: tuple[ResourceRef, ...]
    failure: FailureEnvelope | None
    created_seq: Revision


class ArtifactView(Contract):
    run_id: UUID | None
    resource_ref: ResourceRef
    title: str
    mime: str
    preview_status: Literal["pending", "ready", "failed", "unavailable"]


class InputRequestView(Contract):
    id: UUID
    run_id: UUID
    state_version: Revision
    question: str
    required_facts: tuple[str, ...]


class ApprovalView(Contract):
    run_id: UUID
    approval_ref: ResourceRef
    summary: str
    expires_at: AwareDatetime
    allowed_decisions: tuple[Literal["approved", "rejected", "cancelled"], ...]
    status: Literal["pending", "approved", "rejected", "expired", "cancelled"] = "pending"
    revision: Revision = "0"
    public_reason: str | None = None


class SnapshotEnvelope(Contract):
    schema_revision: Literal[1]
    projection_version: Annotated[int, Field(strict=True, ge=1)]
    session_id: UUID
    snapshot_seq: Revision
    auth_revision: Revision
    retained_from_seq: Revision
    items: tuple[TimelineItem, ...]
    runs: tuple[RunView, ...]
    plans: tuple[PlanView, ...]
    resource_refs: tuple[ResourceRef, ...]
    tools: tuple[ToolActivity, ...] = ()
    artifacts: tuple[ArtifactView, ...] = ()
    input_requests: tuple[InputRequestView, ...] = ()
    approvals: tuple[ApprovalView, ...] = ()

    @model_validator(mode="after")
    def consistent_snapshot(self) -> Self:
        if int(self.retained_from_seq) > int(self.snapshot_seq) + 1:
            raise ValueError("retention watermark is ahead of snapshot")
        for collection in (self.items, self.runs, self.plans, self.tools, self.input_requests):
            ids = [entry.id for entry in collection]
            if len(ids) != len(set(ids)):
                raise ValueError("duplicate snapshot identity")
        if len({artifact.resource_ref for artifact in self.artifacts}) != len(self.artifacts):
            raise ValueError("duplicate snapshot artifact")
        if len({approval.approval_ref for approval in self.approvals}) != len(self.approvals):
            raise ValueError("duplicate snapshot approval")
        if len({question.run_id for question in self.input_requests}) != len(self.input_requests):
            raise ValueError("duplicate waiting question for a run")
        if any(int(item.created_seq) > int(self.snapshot_seq) for item in self.items):
            raise ValueError("item is ahead of snapshot")
        if any(int(tool.created_seq) > int(self.snapshot_seq) for tool in self.tools):
            raise ValueError("tool is ahead of snapshot")
        return self

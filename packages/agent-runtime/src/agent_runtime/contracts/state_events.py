"""Uncommitted state events: journal identities and visibility are not producer inputs."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, TypeAdapter, model_validator

from agent_runtime.contracts import payloads
from agent_runtime.contracts.base import Contract, Identifier, MachineKey
from agent_runtime.contracts.events import check_event_identity
from agent_runtime.contracts.journal import CommittedEntry
from agent_runtime.contracts.ownership import ChildCommitGuard
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.transitions import StateWriteGuard


class StateDraftBase[T: Contract](Contract):
    producer_key: MachineKey
    occurred_at: AwareDatetime
    turn_id: UUID | None
    run_id: UUID | None
    parent_run_id: UUID | None
    step_id: UUID | None
    item_id: UUID | None
    model_attempt_id: UUID | None
    payload: T


class TurnAcceptedDraft(StateDraftBase[payloads.TurnAccepted]):
    kind: Literal["turn.accepted"]


class ItemStartedDraft(StateDraftBase[payloads.ItemStarted]):
    kind: Literal["item.started"]


class ItemCompletedDraft(StateDraftBase[payloads.ItemCompleted]):
    kind: Literal["item.completed"]


class RunStatusDraft(StateDraftBase[payloads.RunStatusPayload]):
    kind: Literal["run.status"]


class InputRequiredDraft(StateDraftBase[payloads.InputRequired]):
    kind: Literal["input.required"]


class InputAcceptedDraft(StateDraftBase[payloads.InputAccepted]):
    kind: Literal["input.accepted"]


class PlanUpdatedDraft(StateDraftBase[payloads.PlanUpdated]):
    kind: Literal["plan.updated"]


class ToolStartedDraft(StateDraftBase[payloads.ToolStarted]):
    kind: Literal["tool.started"]


class ToolProgressDraft(StateDraftBase[payloads.ToolProgress]):
    kind: Literal["tool.progress"]


class ToolFinishedDraft(StateDraftBase[payloads.ToolFinished]):
    kind: Literal["tool.finished"]


class ArtifactCreatedDraft(StateDraftBase[payloads.ArtifactCreated]):
    kind: Literal["artifact.created"]


class ApprovalRequiredDraft(StateDraftBase[payloads.ApprovalRequired]):
    kind: Literal["approval.required"]


class ApprovalUpdatedDraft(StateDraftBase[payloads.ApprovalUpdated]):
    kind: Literal["approval.updated"]


class CommandUpdatedDraft(StateDraftBase[payloads.CommandUpdated]):
    kind: Literal["command.updated"]


class RunCompletedDraft(StateDraftBase[payloads.RunCompleted]):
    kind: Literal["run.completed"]


class RunFailedDraft(StateDraftBase[payloads.RunFailed]):
    kind: Literal["run.failed"]


class RunCancelledDraft(StateDraftBase[payloads.RunCancelled]):
    kind: Literal["run.cancelled"]


class ResourceRevokedDraft(StateDraftBase[payloads.ResourceRevoked]):
    kind: Literal["resource.revoked"]


class ModelPreparedDraft(StateDraftBase[payloads.ModelPrepared]):
    kind: Literal["model.prepared"]


class MailboxAppliedDraft(StateDraftBase[payloads.MailboxApplied]):
    kind: Literal["mailbox.applied"]


class HandoffCommittedDraft(StateDraftBase[payloads.HandoffCommitted]):
    kind: Literal["handoff.committed"]


class CheckpointMigratedDraft(StateDraftBase[payloads.CheckpointMigrated]):
    kind: Literal["checkpoint.migrated"]


type StateEventDraft = Annotated[
    TurnAcceptedDraft
    | ItemStartedDraft
    | ItemCompletedDraft
    | RunStatusDraft
    | InputRequiredDraft
    | InputAcceptedDraft
    | PlanUpdatedDraft
    | ToolStartedDraft
    | ToolProgressDraft
    | ToolFinishedDraft
    | ArtifactCreatedDraft
    | ApprovalRequiredDraft
    | ApprovalUpdatedDraft
    | CommandUpdatedDraft
    | RunCompletedDraft
    | RunFailedDraft
    | RunCancelledDraft
    | ResourceRevokedDraft
    | ModelPreparedDraft
    | MailboxAppliedDraft
    | HandoffCommittedDraft
    | CheckpointMigratedDraft,
    Field(discriminator="kind"),
]
state_draft_adapter: TypeAdapter[StateEventDraft] = TypeAdapter(StateEventDraft)


class MainEventAuthority(Contract):
    kind: Literal["main"]
    guard: StateWriteGuard


class ChildEventAuthority(Contract):
    kind: Literal["child"]
    guard: ChildCommitGuard


class ServiceEventAuthority(Contract):
    kind: Literal["service"]
    actor: ActorContext
    purpose: Literal[
        "admission",
        "control",
        "recovery",
        "wake",
        "checkpoint",
        "tool_result",
        "resource",
        "approval",
    ]
    source: ResourceRef
    transaction_id: UUID

    @model_validator(mode="after")
    def pinned_source(self) -> Self:
        if (
            self.source.scope.scope_id != self.actor.scope_id
            or self.source.resource_version is None
        ):
            raise ValueError("service events need a same-scope pinned source")
        expected = {
            "admission": "turn",
            "control": "command",
            "recovery": "run",
            "wake": "run",
            "checkpoint": "checkpoint",
            "tool_result": "execution",
            "approval": "approval",
        }.get(self.purpose)
        if expected is not None and self.source.resource_type != expected:
            raise ValueError("service event source type does not match its purpose")
        return self


type EventAuthority = Annotated[
    MainEventAuthority | ChildEventAuthority | ServiceEventAuthority, Field(discriminator="kind")
]

SERVICE_KINDS = {
    "admission": {"turn.accepted", "item.started"},
    "control": {"command.updated", "run.status"},
    "recovery": {"run.status", "run.failed", "run.cancelled", "item.completed"},
    "wake": {"run.status", "input.accepted"},
    "checkpoint": {"checkpoint.migrated"},
    "tool_result": {"tool.progress", "tool.finished", "artifact.created", "run.status"},
    "resource": {"resource.revoked"},
    "approval": {"approval.updated"},
}


class StateEventBatch(Contract):
    authority: EventAuthority
    session_id: UUID
    trace_id: Identifier
    events: Annotated[tuple[StateEventDraft, ...], Field(min_length=1, max_length=64)]

    @model_validator(mode="after")
    def bounded_owned_events(self) -> Self:
        keys = [event.producer_key for event in self.events]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate state event producer key")
        if len(self.model_dump_json().encode("utf-8")) > 65536:
            raise ValueError("state event batch exceeds 64 KiB; use resource references")
        for event in self.events:
            # Reuse the committed-envelope identity rule so a draft the journal could
            # never materialize is refused here instead of on the next hop (F-B3).
            check_event_identity(
                event.payload,
                turn_id=event.turn_id,
                run_id=event.run_id,
                parent_run_id=event.parent_run_id,
                step_id=event.step_id,
                item_id=event.item_id,
                model_attempt_id=event.model_attempt_id,
            )
        if isinstance(self.authority, ServiceEventAuthority):
            if any(
                event.kind not in SERVICE_KINDS[self.authority.purpose] for event in self.events
            ):
                raise ValueError("service purpose cannot produce this event kind")
            source = self.authority.source
            # Session-bound services must name the session; a scope-level resource may
            # legitimately have no session (F-B4).
            if self.authority.purpose != "resource" and source.scope.session_id is None:
                raise ValueError("service source must carry the session it belongs to")
            if source.scope.session_id is not None and source.scope.session_id != str(
                self.session_id
            ):
                raise ValueError("service source belongs to another session")
            if self.authority.purpose == "recovery" and any(
                str(event.run_id) != source.resource_id for event in self.events
            ):
                raise ValueError("recovery event must belong to the observed run")
            if self.authority.purpose == "admission" and any(
                str(event.turn_id) != source.resource_id for event in self.events
            ):
                raise ValueError("admission event must belong to the accepted turn")
            if self.authority.purpose == "approval" and any(
                not isinstance(event, ApprovalUpdatedDraft) or event.payload.approval_ref != source
                or event.producer_key != f"approval:{source.resource_id}:{source.resource_version}"
                for event in self.events
            ):
                raise ValueError("approval event must match its service source")
        else:
            lease = self.authority.guard.lease
            if self.session_id != lease.session_id or any(
                event.run_id != lease.run_id for event in self.events
            ):
                raise ValueError("worker cannot write another session or run's events")
            forbidden = {
                "turn.accepted",
                "input.accepted",
                "resource.revoked",
                "checkpoint.migrated",
                "approval.updated",
            }
            if isinstance(self.authority, ChildEventAuthority):
                forbidden |= {"handoff.committed"}
            if any(event.kind in forbidden for event in self.events):
                raise ValueError("worker does not own this state transition")
        return self


class StateEventReceipt(Contract):
    scope_id: Identifier
    session_id: UUID
    entries: Annotated[tuple[CommittedEntry, ...], Field(min_length=1, max_length=64)]

    @model_validator(mode="after")
    def ordered_receipt(self) -> Self:
        seqs = [int(entry.session_seq) for entry in self.entries]
        ids = [entry.event_id for entry in self.entries]
        keys = [entry.producer_key for entry in self.entries]
        if len(set(ids)) != len(ids) or len(set(keys)) != len(keys):
            raise ValueError("receipt identities disagree")
        if seqs[0] < 1 or seqs != sorted(set(seqs)):
            raise ValueError("receipt sequences must be positive and ordered")
        if all(entry.disposition == "created" for entry in self.entries) and seqs[-1] - seqs[
            0
        ] + 1 != len(seqs):
            raise ValueError("new batch sequences must be contiguous")
        return self

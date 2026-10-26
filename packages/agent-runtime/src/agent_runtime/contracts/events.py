"""Typed journal events and transport control frames."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, TypeAdapter, model_validator

from agent_runtime.contracts import payloads
from agent_runtime.contracts.base import Contract, Identifier, Revision

ITEM_IDENTITY_PAYLOADS = (
    payloads.ItemStarted,
    payloads.ContentDelta,
    payloads.AnalysisDelta,
    payloads.ItemCompleted,
    payloads.InputAccepted,
)
RUN_IDENTITY_PAYLOADS = (
    payloads.TerminalPayload,
    payloads.RunStatusPayload,
    payloads.InputRequired,
    payloads.PlanUpdated,
    payloads.ToolStarted,
    payloads.ToolProgress,
    payloads.ToolFinished,
    payloads.ApprovalRequired,
    payloads.ApprovalUpdated,
    payloads.CommandUpdated,
    payloads.ModelPrepared,
    payloads.MailboxApplied,
    payloads.HandoffCommitted,
    payloads.CheckpointMigrated,
)


def check_event_identity(
    payload: Contract,
    *,
    turn_id: UUID | None,
    run_id: UUID | None,
    parent_run_id: UUID | None,
    step_id: UUID | None,
    item_id: UUID | None,
    model_attempt_id: UUID | None,
) -> None:
    """Identity rules shared by committed envelopes and uncommitted drafts (F-B3).

    A draft that cannot satisfy these rules can never be materialized, so validating
    them only at materialization time would accept a batch and fail on the next hop.
    """
    if run_id is None and (parent_run_id is not None or step_id is not None):
        raise ValueError("parent/step requires run identity")
    if isinstance(payload, ITEM_IDENTITY_PAYLOADS) and (item_id is None or run_id is None):
        raise ValueError("item event requires item and run identity")
    if isinstance(payload, RUN_IDENTITY_PAYLOADS) and run_id is None:
        raise ValueError("run event requires run identity")
    if isinstance(payload, payloads.TurnAccepted) and (
        turn_id != payload.turn_id or run_id != payload.run_id
    ):
        raise ValueError("accepted identity mismatch")
    if isinstance(payload, payloads.ModelPrepared) and model_attempt_id != payload.attempt_id:
        raise ValueError("model attempt identity mismatch")
    if isinstance(payload, payloads.HandoffCommitted) and run_id != payload.source_run_id:
        raise ValueError("handoff event must belong to source run")
    if isinstance(payload, payloads.InputRequired) and run_id != payload.reply_to_run_id:
        raise ValueError("input request must belong to its recipient run")
    if isinstance(payload, payloads.InputAccepted) and (
        run_id != payload.reply_to_run_id or item_id != payload.user_item.id
    ):
        raise ValueError("accepted reply identity mismatch")


class EventBase[T: Contract](Contract):
    schema_revision: Literal[1]
    event_id: UUID
    session_id: UUID
    session_seq: Revision
    turn_id: UUID | None
    run_id: UUID | None
    parent_run_id: UUID | None
    step_id: UUID | None
    item_id: UUID | None
    model_attempt_id: UUID | None
    occurred_at: AwareDatetime
    recorded_at: AwareDatetime
    visibility: Literal["public", "restricted"]
    trace_id: Identifier
    payload: T

    @model_validator(mode="after")
    def consistent_identity(self) -> Self:
        if int(self.session_seq) < 1:
            raise ValueError("journal sequence starts at 1")
        check_event_identity(
            self.payload,
            turn_id=self.turn_id,
            run_id=self.run_id,
            parent_run_id=self.parent_run_id,
            step_id=self.step_id,
            item_id=self.item_id,
            model_attempt_id=self.model_attempt_id,
        )
        if isinstance(self.payload, payloads.TurnAccepted | payloads.InputAccepted) and (
            int(self.payload.user_item.created_seq) > int(self.session_seq)
        ):
            raise ValueError("accepted item is ahead of journal")
        return self


class TurnAcceptedEvent(EventBase[payloads.TurnAccepted]):
    kind: Literal["turn.accepted"]


class ItemStartedEvent(EventBase[payloads.ItemStarted]):
    kind: Literal["item.started"]


class ContentDeltaEvent(EventBase[payloads.ContentDelta]):
    kind: Literal["assistant.delta"]


class AnalysisDeltaEvent(EventBase[payloads.AnalysisDelta]):
    kind: Literal["analysis.delta"]


class ItemCompletedEvent(EventBase[payloads.ItemCompleted]):
    kind: Literal["item.completed"]


class RunStatusEvent(EventBase[payloads.RunStatusPayload]):
    kind: Literal["run.status"]


class InputRequiredEvent(EventBase[payloads.InputRequired]):
    kind: Literal["input.required"]


class InputAcceptedEvent(EventBase[payloads.InputAccepted]):
    kind: Literal["input.accepted"]


class PlanUpdatedEvent(EventBase[payloads.PlanUpdated]):
    kind: Literal["plan.updated"]


class ToolStartedEvent(EventBase[payloads.ToolStarted]):
    kind: Literal["tool.started"]


class ToolProgressEvent(EventBase[payloads.ToolProgress]):
    kind: Literal["tool.progress"]


class ToolFinishedEvent(EventBase[payloads.ToolFinished]):
    kind: Literal["tool.finished"]


class ArtifactCreatedEvent(EventBase[payloads.ArtifactCreated]):
    kind: Literal["artifact.created"]


class ApprovalRequiredEvent(EventBase[payloads.ApprovalRequired]):
    kind: Literal["approval.required"]


class ApprovalUpdatedEvent(EventBase[payloads.ApprovalUpdated]):
    kind: Literal["approval.updated"]


class CommandUpdatedEvent(EventBase[payloads.CommandUpdated]):
    kind: Literal["command.updated"]


class RunCompletedEvent(EventBase[payloads.RunCompleted]):
    kind: Literal["run.completed"]


class RunFailedEvent(EventBase[payloads.RunFailed]):
    kind: Literal["run.failed"]


class RunCancelledEvent(EventBase[payloads.RunCancelled]):
    kind: Literal["run.cancelled"]


class ResourceRevokedEvent(EventBase[payloads.ResourceRevoked]):
    kind: Literal["resource.revoked"]


class ModelPreparedEvent(EventBase[payloads.ModelPrepared]):
    kind: Literal["model.prepared"]
    visibility: Literal["restricted"]


class MailboxAppliedEvent(EventBase[payloads.MailboxApplied]):
    kind: Literal["mailbox.applied"]
    visibility: Literal["restricted"]


class HandoffCommittedEvent(EventBase[payloads.HandoffCommitted]):
    kind: Literal["handoff.committed"]
    visibility: Literal["restricted"]


class CheckpointMigratedEvent(EventBase[payloads.CheckpointMigrated]):
    kind: Literal["checkpoint.migrated"]
    visibility: Literal["restricted"]


type EventEnvelope = Annotated[
    TurnAcceptedEvent
    | ItemStartedEvent
    | ContentDeltaEvent
    | AnalysisDeltaEvent
    | ItemCompletedEvent
    | RunStatusEvent
    | InputRequiredEvent
    | InputAcceptedEvent
    | PlanUpdatedEvent
    | ToolStartedEvent
    | ToolProgressEvent
    | ToolFinishedEvent
    | ArtifactCreatedEvent
    | ApprovalRequiredEvent
    | ApprovalUpdatedEvent
    | CommandUpdatedEvent
    | RunCompletedEvent
    | RunFailedEvent
    | RunCancelledEvent
    | ResourceRevokedEvent
    | ModelPreparedEvent
    | MailboxAppliedEvent
    | HandoffCommittedEvent
    | CheckpointMigratedEvent,
    Field(discriminator="kind"),
]


class CursorFrame(Contract):
    kind: Literal["cursor.advanced"]
    cursor: Revision
    auth_revision: Revision


class ResyncFrame(Contract):
    kind: Literal["resync_required"]
    reason: Literal["cursor_expired", "authorization_changed", "projection_changed"]
    snapshot_url: str

    @model_validator(mode="after")
    def same_origin_path(self) -> Self:
        if (
            not self.snapshot_url.startswith("/")
            or self.snapshot_url.startswith("//")
            or "\\" in self.snapshot_url
            or any(ord(char) <= 32 for char in self.snapshot_url)
        ):
            raise ValueError("snapshotUrl must be a same-origin absolute path")
        return self


event_adapter: TypeAdapter[EventEnvelope] = TypeAdapter(EventEnvelope)

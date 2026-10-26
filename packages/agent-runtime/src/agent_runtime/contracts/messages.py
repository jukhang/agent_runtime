"""Internal run-to-run messages, separate from public timeline events."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, TypeAdapter, model_validator

from agent_runtime.contracts import message_payloads
from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.references import ResourceRef


class MessageBase[T: Contract](Contract):
    protocol_revision: Literal[1]
    message_id: UUID
    correlation_id: UUID
    causation_id: UUID | None
    sender_run_id: UUID
    recipient_run_id: UUID
    task_id: UUID
    recipient_seq: Revision
    created_at: AwareDatetime
    deadline_at: AwareDatetime
    resource_refs: tuple[ResourceRef, ...]
    trace_id: Identifier
    payload: T

    @model_validator(mode="after")
    def consistent_delivery(self) -> Self:
        if self.sender_run_id == self.recipient_run_id:
            raise ValueError("inter-agent message must target a different run")
        if int(self.recipient_seq) == 0:
            raise ValueError("recipient sequence starts at 1")
        if self.deadline_at <= self.created_at:
            raise ValueError("deadline must be after creation")
        if self.causation_id == self.message_id:
            raise ValueError("message cannot cause itself")
        return self


class TaskAssignMessage(MessageBase[message_payloads.TaskAssign]):
    kind: Literal["task.assign"]


class TaskAcceptedMessage(MessageBase[message_payloads.TaskAccepted]):
    kind: Literal["task.accepted"]


class TaskRejectedMessage(MessageBase[message_payloads.TaskRejected]):
    kind: Literal["task.rejected"]


class InputAppendMessage(MessageBase[message_payloads.InputAppend]):
    kind: Literal["input.append"]


class ClarificationRequestMessage(MessageBase[message_payloads.ClarificationRequest]):
    kind: Literal["clarification.request"]


class ProgressUpdatedMessage(MessageBase[message_payloads.ProgressUpdated]):
    kind: Literal["progress.updated"]


class ResultProposedMessage(MessageBase[message_payloads.ResultProposed]):
    kind: Literal["result.proposed"]


class ResultAcceptedMessage(MessageBase[message_payloads.ResultVerification]):
    kind: Literal["result.accepted"]


class ResultRejectedMessage(MessageBase[message_payloads.ResultVerification]):
    kind: Literal["result.rejected"]


class TaskFailedMessage(MessageBase[message_payloads.TaskFailed]):
    kind: Literal["task.failed"]


class ControlCancelMessage(MessageBase[message_payloads.ControlCancel]):
    kind: Literal["control.cancel"]


class HandoffRequestMessage(MessageBase[message_payloads.HandoffRequest]):
    kind: Literal["handoff.request"]


class HandoffAcceptedMessage(MessageBase[message_payloads.HandoffResponse]):
    kind: Literal["handoff.accepted"]


class HandoffRejectedMessage(MessageBase[message_payloads.HandoffResponse]):
    kind: Literal["handoff.rejected"]


class HandoffCommittedMessage(MessageBase[message_payloads.HandoffCommitted]):
    kind: Literal["handoff.committed"]


type AgentEnvelope = Annotated[
    TaskAssignMessage
    | TaskAcceptedMessage
    | TaskRejectedMessage
    | InputAppendMessage
    | ClarificationRequestMessage
    | ProgressUpdatedMessage
    | ResultProposedMessage
    | ResultAcceptedMessage
    | ResultRejectedMessage
    | TaskFailedMessage
    | ControlCancelMessage
    | HandoffRequestMessage
    | HandoffAcceptedMessage
    | HandoffRejectedMessage
    | HandoffCommittedMessage,
    Field(discriminator="kind"),
]
message_adapter: TypeAdapter[AgentEnvelope] = TypeAdapter(AgentEnvelope)

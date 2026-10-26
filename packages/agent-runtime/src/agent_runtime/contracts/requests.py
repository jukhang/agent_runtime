"""Public request shapes. Actor identity comes from the host, never this body."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, StringConstraints, model_validator

from agent_runtime.contracts.base import Contract, MachineKey, Revision
from agent_runtime.contracts.references import CapabilityRef, ResourceRef
from agent_runtime.contracts.views import Control


class TextInput(Contract):
    kind: Literal["text"]
    text: Annotated[str, StringConstraints(min_length=1)]

    @model_validator(mode="after")
    def nonblank(self) -> Self:
        if not self.text.strip():
            raise ValueError("input cannot be blank")
        return self


class ResourceInput(Contract):
    kind: Literal["resource"]
    resource_ref: ResourceRef


type InputPart = Annotated[TextInput | ResourceInput, Field(discriminator="kind")]


class SubmitRequest(Contract):
    session_id: UUID
    agent: CapabilityRef
    idempotency_key: MachineKey
    inputs: Annotated[tuple[InputPart, ...], Field(min_length=1)]


class AcceptedTurn(Contract):
    session_id: UUID
    turn_id: UUID
    run_id: UUID
    assistant_item_id: UUID
    accepted_seq: Revision


class ControlRequest(Contract):
    run_id: UUID
    kind: Control
    idempotency_key: MachineKey
    expected_version: Revision


class CommandReceipt(Contract):
    command_id: UUID
    run_id: UUID
    status: Literal["accepted", "applied", "rejected"]
    state_version: Revision
    public_reason: str


class InteractionRequest(Contract):
    session_id: UUID
    run_id: UUID
    expected_version: Revision
    idempotency_key: MachineKey
    text: Annotated[str, StringConstraints(min_length=1, max_length=4096)]

    @model_validator(mode="after")
    def nonblank(self) -> Self:
        if not self.text.strip():
            raise ValueError("reply cannot be blank")
        return self


class InteractionReceipt(Contract):
    session_id: UUID
    run_id: UUID
    input_id: UUID
    state_version: Revision
    accepted_seq: Revision


class StreamRequest(Contract):
    session_id: UUID
    after_seq: Revision
    last_event_id: Revision | None

    @model_validator(mode="after")
    def matching_cursor(self) -> Self:
        if self.last_event_id is not None and self.last_event_id != self.after_seq:
            raise ValueError("afterSeq and Last-Event-ID disagree")
        return self

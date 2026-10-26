"""Mailbox receipt, claim and apply shapes retain distinct durability watermarks."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.messages import AgentEnvelope
from agent_runtime.contracts.transactions import CommitGuard


class MailboxReceipt(Contract):
    scope_id: Identifier
    message_id: UUID
    recipient_run_id: UUID
    recipient_seq: Revision
    disposition: Literal["accepted", "duplicate"]

    @model_validator(mode="after")
    def allocated_sequence(self) -> Self:
        if int(self.recipient_seq) < 1:
            raise ValueError("mailbox sequence starts at one")
        return self


class MailboxClaimRequest(Contract):
    guard: CommitGuard
    expected_applied_seq: Revision
    limit: Annotated[int, Field(strict=True, ge=1, le=256)]


class LeasedInput(Contract):
    message: AgentEnvelope
    claim_token: UUID
    lease_until: AwareDatetime


class ClaimedBatch(Contract):
    scope_id: Identifier
    recipient_run_id: UUID
    previous_applied_seq: Revision
    entries: Annotated[tuple[LeasedInput, ...], Field(max_length=256)]

    @model_validator(mode="after")
    def contiguous_claim(self) -> Self:
        seen: set[UUID] = set()
        for offset, entry in enumerate(self.entries, start=1):
            message = entry.message
            if message.recipient_run_id != self.recipient_run_id:
                raise ValueError("mailbox claim targets another run")
            if int(message.recipient_seq) != int(self.previous_applied_seq) + offset:
                raise ValueError("claim cannot silently skip an input")
            if message.message_id in seen:
                raise ValueError("duplicate message claim")
            seen.add(message.message_id)
        return self


class AppliedInputRef(Contract):
    scope_id: Identifier
    run_id: UUID
    applied_seq: Revision
    checkpoint_id: UUID
    state_version: Revision
    committed_seq: Revision

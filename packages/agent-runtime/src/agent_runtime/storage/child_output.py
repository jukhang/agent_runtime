"""In-process child output requests, separate from main-slot wire contracts."""

from typing import Annotated, Self
from uuid import UUID

from pydantic import Field, model_validator

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.canonical import canonical_bytes
from agent_runtime.contracts.journal import OutputDraft
from agent_runtime.contracts.ownership import ChildCommitGuard, ChildLeaseToken


class ChildProducerBatch(Contract):
    guard: ChildCommitGuard
    turn_id: UUID
    attempt_id: UUID
    item_id: UUID
    trace_id: Identifier
    events: Annotated[tuple[OutputDraft, ...], Field(min_length=1, max_length=64)]

    @property
    def lease(self) -> ChildLeaseToken:
        return self.guard.lease

    @model_validator(mode="after")
    def bounded_output(self) -> Self:
        previous: dict[str, int] = {}
        revision = self.events[0].payload.item_revision
        for event in self.events:
            payload = event.payload
            index = int(payload.delta_index)
            if payload.channel in previous and index != previous[payload.channel] + 1:
                raise ValueError("child output channel must be contiguous")
            previous[payload.channel] = index
            if payload.item_revision != revision:
                raise ValueError("child output cannot mix revisions")
        if len(canonical_bytes(self.model_dump(mode="json", by_alias=True))) > 65536:
            raise ValueError("child output batch exceeds 64 KiB")
        return self


class ChildSealOutputRequest(Contract):
    guard: ChildCommitGuard
    attempt_id: UUID
    item_id: UUID
    proof_id: UUID
    sealed_revision: Revision

    @property
    def lease(self) -> ChildLeaseToken:
        return self.guard.lease


__all__ = ["ChildProducerBatch", "ChildSealOutputRequest"]

"""Output drafts and storage receipts; only the journal assigns committed identities."""

import hashlib
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from agent_runtime.contracts.base import Contract, Identifier, MachineKey, Revision
from agent_runtime.contracts.canonical import CanonicalJSONError, canonical_bytes, content_digest
from agent_runtime.contracts.events import EventEnvelope
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.payloads import AnalysisDelta, ContentDelta
from agent_runtime.contracts.transactions import LeaseToken
from agent_runtime.contracts.views import Channel, Digest


class ContentDraft(Contract):
    kind: Literal["assistant.delta"]
    occurred_at: AwareDatetime
    payload: ContentDelta


class AnalysisDraft(Contract):
    kind: Literal["analysis.delta"]
    occurred_at: AwareDatetime
    payload: AnalysisDelta


type OutputDraft = Annotated[ContentDraft | AnalysisDraft, Field(discriminator="kind")]


def output_producer_key(attempt_id: UUID, channel: Channel, delta_index: Revision) -> MachineKey:
    return f"{attempt_id}/{channel}/{delta_index}"


def output_digest(event: OutputDraft) -> str:
    try:
        return content_digest(event.model_dump(mode="json", by_alias=True))
    except CanonicalJSONError as exc:
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="The output draft has no canonical JSON encoding.",
        ) from exc


class ProducerBatch(Contract):
    lease: LeaseToken
    turn_id: UUID
    attempt_id: UUID
    item_id: UUID
    trace_id: Identifier
    events: Annotated[tuple[OutputDraft, ...], Field(min_length=1, max_length=64)]

    @model_validator(mode="after")
    def bounded_ordered_output(self) -> Self:
        previous: dict[str, int] = {}
        revision = self.events[0].payload.item_revision
        for event in self.events:
            payload = event.payload
            index = int(payload.delta_index)
            if payload.channel in previous and index != previous[payload.channel] + 1:
                raise ValueError("duplicate, missing or reordered channel delta")
            previous[payload.channel] = index
            if payload.item_revision != revision:
                raise ValueError("batch cannot mix item revisions")
        try:
            encoded = canonical_bytes(self.model_dump(mode="json", by_alias=True))
        except (ValueError, UnicodeError) as exc:
            raise ValueError("batch must contain valid UTF-8, finite JSON text") from exc
        if len(encoded) > 65536:
            raise ValueError("journal output batch exceeds 64 KiB")
        return self


class CommittedEntry(Contract):
    producer_key: MachineKey
    content_digest: Digest
    event_id: UUID
    session_seq: Revision
    disposition: Literal["created", "reused"]


class CommittedBatch(Contract):
    scope_id: Identifier
    session_id: UUID
    run_id: UUID
    attempt_id: UUID
    item_id: UUID
    entries: Annotated[tuple[CommittedEntry, ...], Field(min_length=1, max_length=64)]

    @model_validator(mode="after")
    def consistent_receipt(self) -> Self:
        seqs = [int(entry.session_seq) for entry in self.entries]
        ids = [entry.event_id for entry in self.entries]
        keys = [entry.producer_key for entry in self.entries]
        if seqs[0] < 1 or seqs != sorted(set(seqs)):
            raise ValueError("receipt sequences must be positive, unique and ordered")
        if len(ids) != len(set(ids)) or len(keys) != len(set(keys)):
            raise ValueError("duplicate event or producer identity")
        if all(entry.disposition == "created" for entry in self.entries) and seqs[-1] - seqs[
            0
        ] + 1 != len(seqs):
            raise ValueError("new transaction must allocate contiguous sequences")
        return self


class FlushedChannel(Contract):
    channel: Channel
    last_delta_index: Revision | None
    last_session_seq: Revision | None
    digest: Digest

    @model_validator(mode="after")
    def empty_channel(self) -> Self:
        if (self.last_delta_index is None) != (self.last_session_seq is None):
            raise ValueError("channel delta and session watermark must both be present or null")
        if self.last_session_seq is not None and int(self.last_session_seq) < 1:
            raise ValueError("nonempty channel requires positive journal sequence")
        if self.last_delta_index is None and self.digest != hashlib.sha256(b"").hexdigest():
            raise ValueError("empty channel must carry empty content digest")
        return self


class FlushProof(Contract):
    proof_id: UUID
    scope_id: Identifier
    session_id: UUID
    run_id: UUID
    attempt_id: UUID
    item_id: UUID
    durable_seq: Revision
    channels: tuple[FlushedChannel, ...]

    @model_validator(mode="after")
    def all_channels_durable(self) -> Self:
        if len(self.channels) != 2 or {c.channel for c in self.channels} != {"content", "analysis"}:
            raise ValueError("flush proof requires both distinct output channels")
        for channel in self.channels:
            if int(channel.last_session_seq or "0") > int(self.durable_seq):
                raise ValueError("channel is ahead of durable journal watermark")
        return self


class SealOutputRequest(Contract):
    """Idempotent request for storage to derive and persist one output seal."""

    lease: LeaseToken
    attempt_id: UUID
    item_id: UUID
    proof_id: UUID
    sealed_revision: Revision


class JournalPage(Contract):
    """Authoritative replay page; ``cursor`` may cross filtered restricted events."""

    session_id: UUID
    after_seq: Revision
    cursor: Revision
    auth_revision: Revision
    events: tuple[EventEnvelope, ...]

    @model_validator(mode="after")
    def ordered_events(self) -> Self:
        if int(self.cursor) < int(self.after_seq):
            raise ValueError("journal cursor cannot move backwards")
        seqs = [int(event.session_seq) for event in self.events]
        if seqs != sorted(set(seqs)):
            raise ValueError("journal page events must be ordered and unique")
        if any(seq <= int(self.after_seq) or seq > int(self.cursor) for seq in seqs):
            raise ValueError("journal event is outside the page cursor range")
        return self


class OutboxNotice(Contract):
    """Payload sent to a wake-up transport; event content stays in the Journal."""

    scope_id: Identifier
    session_id: UUID
    event_id: UUID
    session_seq: Revision


class OutboxClaimRequest(Contract):
    limit: Annotated[int, Field(strict=True, ge=1, le=256)] = 64
    lease_seconds: Annotated[int, Field(strict=True, ge=1, le=300)] = 30


class OutboxClaim(Contract):
    claim_token: UUID
    lease_until: AwareDatetime
    notices: Annotated[tuple[OutboxNotice, ...], Field(min_length=1, max_length=256)]


class OutboxDelivery(Contract):
    claim_token: UUID
    event_ids: Annotated[tuple[UUID, ...], Field(min_length=1, max_length=256)]

    @model_validator(mode="after")
    def unique_events(self) -> Self:
        if len(self.event_ids) != len(set(self.event_ids)):
            raise ValueError("outbox delivery contains duplicate event IDs")
        return self

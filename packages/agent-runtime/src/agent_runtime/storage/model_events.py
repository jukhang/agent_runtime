"""Persist public model output channels through the authoritative Journal."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.journal import (
    AnalysisDraft,
    CommittedBatch,
    ContentDraft,
    OutputDraft,
    ProducerBatch,
    output_digest,
    output_producer_key,
)
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.contracts.ownership import ChildCommitGuard, ChildLeaseToken
from agent_runtime.contracts.payloads import AnalysisDelta as PublicAnalysisDelta
from agent_runtime.contracts.payloads import ContentDelta as PublicContentDelta
from agent_runtime.contracts.provider import (
    AnalysisDelta,
    ContentDelta,
    ProviderEvent,
)
from agent_runtime.contracts.transactions import LeaseToken
from agent_runtime.storage.child_output import ChildProducerBatch


class OutputJournal(Protocol):
    async def append(self, batch: ProducerBatch | ChildProducerBatch) -> CommittedBatch: ...


class SqlModelEventSink:
    """Journal content/analysis before the loop consumes each provider event.

    The sink is scoped to one attempt and one pre-created assistant Timeline Item.
    Tool candidates and usage/finish remain private until DecisionStore and
    ModelAttemptStore commit their complete facts.
    """

    def __init__(
        self,
        journal: OutputJournal,
        *,
        lease: LeaseToken | ChildLeaseToken | Callable[[], LeaseToken | ChildLeaseToken],
        turn_id: UUID,
        attempt_id: UUID,
        item_id: UUID,
        item_revision: str,
        trace_id: str,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        child_guard: Callable[[], ChildCommitGuard] | None = None,
    ) -> None:
        self._journal = journal
        self._lease = lease if callable(lease) else lambda: lease
        self._turn_id = turn_id
        self._attempt_id = attempt_id
        self._item_id = item_id
        self._item_revision = item_revision
        self._trace_id = trace_id
        self._clock = clock
        self._child_guard = child_guard
        self._indexes = {"content": 0, "analysis": 0}
        self._last_sequence = -1
        self._lock = asyncio.Lock()
        self._retry_events: tuple[ProviderEvent, ...] | None = None
        self._retry_times: dict[int, datetime] = {}

    async def record(self, request: ProviderRequest, event: ProviderEvent) -> None:
        await self.record_many(request, (event,))

    async def record_many(
        self, request: ProviderRequest, events: tuple[ProviderEvent, ...]
    ) -> None:
        if not events or len(events) > 64:
            raise ValueError("Model output batch requires 1–64 events")
        lease = self._lease()
        if (
            request.attempt_id != self._attempt_id
            or any(event.attempt_id != self._attempt_id for event in events)
            or request.scope_id != lease.scope_id
            or request.session_id != lease.session_id
            or request.run_id != lease.run_id
        ):
            raise runtime_failure(
                "IDENTITY_MISMATCH",
                category="storage",
                public_message="Model event belongs to another attempt or run.",
            )
        async with self._lock:
            if self._retry_events is not None and events != self._retry_events:
                raise runtime_failure(
                    "AUDIT_UNAVAILABLE",
                    category="storage",
                    public_message="Previous output append is unacknowledged; reconcile it first.",
                )
            indexes = dict(self._indexes)
            times = dict(self._retry_times)
            sequence = self._last_sequence
            drafts: list[OutputDraft] = []
            for event in events:
                current = int(event.sequence)
                if current <= sequence:
                    raise runtime_failure(
                        "VERSION_CONFLICT",
                        category="provider",
                        public_message="Model event sequence moved backwards.",
                    )
                sequence = current
                occurred_at = times.get(current)
                if occurred_at is None:
                    occurred_at = self._clock()
                    times[current] = occurred_at
                if isinstance(event, ContentDelta):
                    channel = "content"
                    drafts.append(
                        ContentDraft(
                            kind="assistant.delta",
                            occurred_at=occurred_at,
                            payload=PublicContentDelta(
                                channel="content",
                                delta_index=str(indexes[channel]),
                                item_revision=self._item_revision,
                                text=event.text,
                            ),
                        )
                    )
                elif isinstance(event, AnalysisDelta):
                    channel = "analysis"
                    drafts.append(
                        AnalysisDraft(
                            kind="analysis.delta",
                            occurred_at=occurred_at,
                            payload=PublicAnalysisDelta(
                                channel="analysis",
                                delta_index=str(indexes[channel]),
                                item_revision=self._item_revision,
                                text=event.text,
                            ),
                        )
                    )
                else:
                    continue
                indexes[channel] += 1
            if not drafts:
                self._last_sequence = sequence
                return
            batch: ProducerBatch | ChildProducerBatch
            if isinstance(lease, ChildLeaseToken):
                if self._child_guard is None:
                    raise runtime_failure(
                        "FORBIDDEN",
                        category="authorization",
                        public_message="Child output requires explicit child authority.",
                    )
                guard = self._child_guard()
                if guard.lease != lease:
                    raise runtime_failure(
                        "IDENTITY_MISMATCH",
                        category="authorization",
                        public_message="Child output guard and lease disagree.",
                    )
                batch = ChildProducerBatch(
                    guard=guard,
                    turn_id=self._turn_id,
                    attempt_id=self._attempt_id,
                    item_id=self._item_id,
                    trace_id=self._trace_id,
                    events=tuple(drafts),
                )
            else:
                batch = ProducerBatch(
                    lease=lease,
                    turn_id=self._turn_id,
                    attempt_id=self._attempt_id,
                    item_id=self._item_id,
                    trace_id=self._trace_id,
                    events=tuple(drafts),
                )
            # Preserve draft identity if commit succeeded but its acknowledgement
            # was lost. Re-timestamping a retry would conflict with producer keys.
            self._retry_events, self._retry_times = events, times
            receipt = await self._journal.append(batch)
            if (
                receipt.attempt_id != self._attempt_id
                or receipt.item_id != self._item_id
                or receipt.run_id != lease.run_id
                or receipt.scope_id != lease.scope_id
                or receipt.session_id != lease.session_id
                or len(receipt.entries) != len(drafts)
                or any(
                    entry.producer_key
                    != output_producer_key(
                        self._attempt_id, draft.payload.channel, draft.payload.delta_index
                    )
                    or entry.content_digest != output_digest(draft)
                    for entry, draft in zip(receipt.entries, drafts, strict=False)
                )
            ):
                raise runtime_failure(
                    "AUDIT_UNAVAILABLE",
                    category="storage",
                    public_message="Model output was not durably recorded.",
                )
            self._indexes = indexes
            self._last_sequence = sequence
            self._retry_events, self._retry_times = None, {}


__all__ = ["SqlModelEventSink"]

"""Batching does not expose uncommitted output or advance indexes on failed writes."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from test_provider_invoker import make_request
from test_storage_model_events import lease_for

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.journal import (
    CommittedBatch,
    CommittedEntry,
    output_digest,
    output_producer_key,
)
from agent_runtime.contracts.provider import AnalysisDelta, ContentDelta, ProviderFinish
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.main_stream import SqlMainStream
from agent_runtime.storage.model_events import SqlModelEventSink
from agent_runtime.worker import LeaseHandle


class BatchJournal:
    def __init__(self):
        self.batches = []
        self.seq = 0
        self.fail = False

    async def append(self, batch):
        if self.fail:
            raise RuntimeError("injected rollback")
        self.batches.append(batch)
        entries = []
        for event in batch.events:
            self.seq += 1
            entries.append(
                CommittedEntry(
                    producer_key=output_producer_key(
                        batch.attempt_id, event.payload.channel, event.payload.delta_index
                    ),
                    content_digest=output_digest(event),
                    event_id=uuid4(),
                    session_seq=str(self.seq),
                    disposition="created",
                )
            )
        return CommittedBatch(
            scope_id=batch.lease.scope_id,
            session_id=batch.lease.session_id,
            run_id=batch.lease.run_id,
            attempt_id=batch.attempt_id,
            item_id=batch.item_id,
            entries=tuple(entries),
        )


@pytest.mark.asyncio
async def test_batch_validates_all_events_before_append_and_indexes_after_commit():
    request = make_request()
    journal = BatchJournal()
    sink = SqlModelEventSink(
        journal,
        lease=lease_for(request),
        turn_id=uuid4(),
        attempt_id=request.attempt_id,
        item_id=uuid4(),
        item_revision="1",
        trace_id="batch",
    )
    events = (
        ContentDelta(attempt_id=request.attempt_id, sequence="0", kind="content.delta", text="A"),
        AnalysisDelta(
            attempt_id=request.attempt_id, sequence="1", kind="analysis.delta", text="why"
        ),
        ContentDelta(attempt_id=request.attempt_id, sequence="2", kind="content.delta", text="B"),
    )
    with pytest.raises(AgentRuntimeError):
        await sink.record_many(request, (events[1], events[0]))
    assert journal.batches == []
    journal.fail = True
    with pytest.raises(RuntimeError):
        await sink.record_many(request, events)
    journal.fail = False
    await sink.record_many(request, events)
    assert len(journal.batches) == 1
    assert [event.payload.delta_index for event in journal.batches[0].events] == ["0", "0", "1"]


@pytest.mark.asyncio
async def test_main_stream_persists_first_delta_then_batches_and_flushes_before_finish(monkeypatch):
    import agent_runtime.storage.main_stream as module

    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: 0.0))
    request = make_request()
    token = lease_for(request)
    actor = ActorContext(
        scope_id=request.scope_id, actor_id="owner", auth_revision="1", grants_ref="g"
    )
    engine = create_async_engine("postgresql+asyncpg://unused:unused@127.0.0.1/unused")
    stream = SqlMainStream(
        engine, "agent_runtime", actor=actor, loaded=SimpleNamespace(), lease=LeaseHandle(token)
    )
    journal = BatchJournal()
    stream._sinks[request.attempt_id] = SqlModelEventSink(
        journal,
        lease=token,
        turn_id=uuid4(),
        attempt_id=request.attempt_id,
        item_id=uuid4(),
        item_revision="1",
        trace_id="batch",
    )
    stream._requests[request.attempt_id] = request
    for seq in range(34):
        await stream.record(
            request,
            ContentDelta(
                attempt_id=request.attempt_id, sequence=str(seq), kind="content.delta", text="x"
            ),
        )
        if seq == 0:
            assert len(journal.batches) == 1
    assert [len(batch.events) for batch in journal.batches] == [1, 32]
    await stream.record(
        request,
        ProviderFinish(
            attempt_id=request.attempt_id,
            sequence="34",
            kind="finish",
            reason="stop",
            raw_reason=None,
        ),
    )
    assert [len(batch.events) for batch in journal.batches] == [1, 32, 1]
    assert not stream._pending[request.attempt_id]
    assert [event.payload.delta_index for batch in journal.batches for event in batch.events] == [
        str(i) for i in range(34)
    ]
    await engine.dispose()


@pytest.mark.asyncio
async def test_lost_append_ack_reuses_timestamp_and_rejects_skipping_unacknowledged_batch():
    request = make_request()

    class LostAck(BatchJournal):
        async def append(self, batch):
            hashes = [output_digest(event) for event in batch.events]
            if not self.batches:
                result = await super().append(batch)
                self.hashes, self.receipt = hashes, result
                raise TimeoutError("ack lost after commit")
            assert hashes == self.hashes
            return self.receipt

    now = datetime.now(UTC)
    ticks = iter((now, now + timedelta(seconds=1), now + timedelta(seconds=2)))
    journal = LostAck()
    sink = SqlModelEventSink(
        journal,
        lease=lease_for(request),
        turn_id=uuid4(),
        attempt_id=request.attempt_id,
        item_id=uuid4(),
        item_revision="1",
        trace_id="batch",
        clock=lambda: next(ticks),
    )
    first = ContentDelta(
        attempt_id=request.attempt_id, sequence="0", kind="content.delta", text="A"
    )
    with pytest.raises(TimeoutError):
        await sink.record(request, first)
    with pytest.raises(AgentRuntimeError, match="unacknowledged"):
        await sink.record(request, first.model_copy(update={"sequence": "1", "text": "B"}))
    await sink.record(request, first)
    assert len(journal.batches) == 1 and sink._indexes["content"] == 1

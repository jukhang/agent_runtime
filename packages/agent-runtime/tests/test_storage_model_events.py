"""Public model deltas are accepted only after durable Journal append."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from test_provider_invoker import make_request

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.journal import (
    CommittedBatch,
    CommittedEntry,
    output_digest,
    output_producer_key,
)
from agent_runtime.contracts.ownership import ChildCommitGuard, ChildLeaseToken
from agent_runtime.contracts.provider import AnalysisDelta, ContentDelta, ProviderFinish
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.storage.child_output import ChildProducerBatch
from agent_runtime.storage.model_events import SqlModelEventSink


def lease_for(request):
    deadline = datetime.now(UTC) + timedelta(minutes=5)
    return LeaseToken(
        scope_id=request.scope_id,
        session_id=request.session_id,
        run_id=request.run_id,
        work_id=uuid4(),
        owner_id="worker",
        run_epoch="1",
        work_epoch="1",
        slot_version="1",
        run_lease_until=deadline,
        work_lease_until=deadline,
    )


class Journal:
    def __init__(self):
        self.batches = []
        self.fail = False

    async def append(self, batch):
        if self.fail:
            raise RuntimeError("journal unavailable")
        self.batches.append(batch)
        return CommittedBatch(
            scope_id=batch.lease.scope_id,
            session_id=batch.lease.session_id,
            run_id=batch.lease.run_id,
            attempt_id=batch.attempt_id,
            item_id=batch.item_id,
            entries=(
                CommittedEntry(
                    producer_key=output_producer_key(
                        batch.attempt_id,
                        batch.events[0].payload.channel,
                        batch.events[0].payload.delta_index,
                    ),
                    content_digest=output_digest(batch.events[0]),
                    event_id=uuid4(),
                    session_seq=str(len(self.batches)),
                    disposition="created",
                ),
            ),
        )


@pytest.mark.asyncio
async def test_model_output_channels_have_independent_indexes_and_finish_is_private():
    request = make_request()
    journal = Journal()
    sink = SqlModelEventSink(
        journal,
        lease=lease_for(request),
        turn_id=uuid4(),
        attempt_id=request.attempt_id,
        item_id=uuid4(),
        item_revision="1",
        trace_id="trace",
    )
    await sink.record(
        request,
        ContentDelta(attempt_id=request.attempt_id, sequence="0", kind="content.delta", text="A"),
    )
    await sink.record(
        request,
        AnalysisDelta(
            attempt_id=request.attempt_id, sequence="1", kind="analysis.delta", text="why"
        ),
    )
    await sink.record(
        request,
        ContentDelta(attempt_id=request.attempt_id, sequence="2", kind="content.delta", text="B"),
    )
    await sink.record(
        request,
        ProviderFinish(
            attempt_id=request.attempt_id,
            sequence="3",
            kind="finish",
            reason="stop",
            raw_reason=None,
        ),
    )
    assert [
        (batch.events[0].kind, batch.events[0].payload.delta_index) for batch in journal.batches
    ] == [("assistant.delta", "0"), ("analysis.delta", "0"), ("assistant.delta", "1")]


@pytest.mark.asyncio
async def test_failed_journal_append_does_not_advance_model_delta_index():
    request = make_request()
    journal = Journal()
    sink = SqlModelEventSink(
        journal,
        lease=lease_for(request),
        turn_id=uuid4(),
        attempt_id=request.attempt_id,
        item_id=uuid4(),
        item_revision="1",
        trace_id="trace",
    )
    event = ContentDelta(
        attempt_id=request.attempt_id, sequence="0", kind="content.delta", text="A"
    )
    journal.fail = True
    with pytest.raises(RuntimeError, match="unavailable"):
        await sink.record(request, event)
    journal.fail = False
    await sink.record(request, event)
    assert journal.batches[0].events[0].payload.delta_index == "0"
    with pytest.raises(AgentRuntimeError) as error:
        await sink.record(request, event)
    assert error.value.failure.code == "VERSION_CONFLICT"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["valid", "missing", "mismatch", "foreign_session"])
async def test_child_sink_requires_explicit_matching_authority(mode):
    request = make_request()
    lease = ChildLeaseToken.model_validate(lease_for(request).model_dump(exclude={"slot_version"}))
    guard = ChildCommitGuard(
        actor=ActorContext(
            scope_id=lease.scope_id, actor_id="user", auth_revision="1", grants_ref="grant:1"
        ),
        lease=lease,
        expected_state_version="1",
        root_run_id=uuid4(),
        expected_root_cancel_version="0",
        workspace_ref=ResourceRef(
            resource_type="workspace",
            resource_id=str(uuid4()),
            resource_version="1",
            scope=ResourceScope(scope_id=lease.scope_id),
        ),
        workspace_owner_run_id=lease.run_id,
        workspace_generation="1",
    )
    journal = Journal()
    supplied = (
        guard.model_copy(update={"lease": lease.model_copy(update={"run_epoch": "2"})})
        if mode == "mismatch"
        else guard
    )
    sink = SqlModelEventSink(
        journal,
        lease=lease,
        child_guard=None if mode == "missing" else lambda: supplied,
        turn_id=uuid4(),
        attempt_id=request.attempt_id,
        item_id=uuid4(),
        item_revision="1",
        trace_id="child",
    )
    if mode == "foreign_session":
        request = request.model_copy(update={"session_id": uuid4()})
    event = ContentDelta(
        attempt_id=request.attempt_id, sequence="0", kind="content.delta", text="child"
    )
    if mode != "valid":
        with pytest.raises(AgentRuntimeError):
            await sink.record(request, event)
        assert journal.batches == []
        return
    journal.fail = True
    with pytest.raises(RuntimeError):
        await sink.record(request, event)
    journal.fail = False
    await sink.record(request, event)
    batch = journal.batches[0]
    assert isinstance(batch, ChildProducerBatch) and batch.guard == guard
    assert batch.events[0].payload.delta_index == "0"
    with pytest.raises(AgentRuntimeError):
        await sink.record(request, event)

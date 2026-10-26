import copy
import hashlib
import json
from importlib.resources import files

import pytest

from agent_runtime.contracts.events import event_adapter
from agent_runtime.projection import SessionProjection, display_event


def event(kind, seq, **payload):
    fixtures = json.loads(
        files("agent_runtime.contracts").joinpath("fixtures/events.json").read_text()
    )
    raw = copy.deepcopy(next(row for row in fixtures["events"] if row["kind"] == kind))
    raw["sessionSeq"] = str(seq)
    raw["payload"].update(payload)
    return event_adapter.validate_python(raw)


def test_projection_channels_duplicates_and_digest_validation():
    started = event("item.started", 1, channels=["content", "analysis"])
    view = SessionProjection(started.session_id)
    assert view.apply(started)
    delta = event("assistant.delta", 2, text="answer", deltaIndex="0")
    thought = event("analysis.delta", 3, text="thinking", deltaIndex="0")
    assert view.apply(delta)
    assert not view.apply(delta)
    assert view.apply(thought)
    item = view.items[started.item_id]
    assert {c.name: c.text for c in item.channels} == {"content": "answer", "analysis": "thinking"}
    with pytest.raises(ValueError, match="gap"):
        view.apply(event("assistant.delta", 4, deltaIndex="9"))
    assert view.cursor == 3
    completed = event(
        "item.completed",
        4,
        channelDigests=[
            {"channel": name, "digest": hashlib.sha256(text.encode()).hexdigest()}
            for name, text in (("content", "answer"), ("analysis", "thinking"))
        ],
        lastDeltaIndexes=[
            {"channel": name, "lastDeltaIndex": "0"} for name in ("content", "analysis")
        ],
    )
    assert view.apply(completed)
    assert view.items[started.item_id].status == "completed"
    assert view.apply(completed.model_copy(update={"session_seq": "5"}))
    with pytest.raises(ValueError, match="terminal item"):
        view.apply(
            completed.model_copy(
                update={
                    "session_seq": "6",
                    "payload": completed.payload.model_copy(update={"status": "failed"}),
                }
            )
        )
    assert view.cursor == 5


def test_projection_rejects_foreign_private_or_unknown_item():
    started = event("item.started", 1)
    view = SessionProjection(started.session_id)
    with pytest.raises(ValueError, match="existing item"):
        view.apply(event("assistant.delta", 2))
    with pytest.raises(ValueError, match="public"):
        view.apply(event("model.prepared", 2))
    assert not view.items and view.cursor == 0


def test_completion_cannot_omit_a_channel_or_invent_success():
    started = event("item.started", 1, channels=["content", "analysis"])
    view = SessionProjection(started.session_id)
    view.apply(started)
    with pytest.raises(ValueError, match="unfinished"):
        view.apply(event("run.completed", 2))
    assert not view.partial_results and view.cursor == 1


def test_aggregated_usage_never_turns_unknown_into_zero():
    from agent_runtime.storage.run_backend import SqlRunBackend

    assert SqlRunBackend._usage([]).status == "unknown"
    assert SqlRunBackend._usage([{"usage": None}]).total_tokens is None
    usage = {
        "status": "complete",
        "reason": "provider_reported",
        "inputTokens": "2",
        "outputTokens": "3",
        "totalTokens": "5",
    }
    assert SqlRunBackend._usage([{"usage": usage}, {"usage": usage}]).total_tokens == "10"


def test_terminal_results_only_follow_declared_output_references():
    from uuid import uuid4

    started = event("item.started", 1)
    view = SessionProjection(started.session_id)
    view.apply(started)
    with pytest.raises(ValueError, match="unknown or non-output"):
        view.apply(event("run.cancelled", 2, finalItemIds=[str(uuid4())]))
    assert view.cursor == 1 and not view.final_items
    view.apply(event("run.cancelled", 2, finalItemIds=[str(started.item_id)]))
    assert view.final_items[started.run_id] == (started.item_id,)
    assert view.items[started.item_id].status == "cancelled"


@pytest.mark.parametrize("include_analysis", [False, True])
def test_display_projection_hides_restricted_channel_metadata_consistently(include_analysis):
    started = event("item.started", 1, channels=["content", "analysis"])
    events = (
        started,
        event("analysis.delta", 2, text="private", deltaIndex="0").model_copy(
            update={"visibility": "restricted"}
        ),
        event("assistant.delta", 3, text="public", deltaIndex="0"),
        event(
            "item.completed",
            4,
            channelDigests=[
                {"channel": c, "digest": hashlib.sha256(text.encode()).hexdigest()}
                for c, text in (("content", "public"), ("analysis", "private"))
            ],
            lastDeltaIndexes=[
                {"channel": c, "lastDeltaIndex": "0"} for c in ("content", "analysis")
            ],
        ),
    )
    view = SessionProjection(started.session_id, include_analysis=include_analysis)
    for raw in events:
        projected = display_event(raw, include_analysis=include_analysis)
        if projected is not None:
            view.apply(projected)
    channels = {part.name: part.text for part in view.items[started.item_id].channels}
    assert channels == (
        {"content": "public", "analysis": "private"} if include_analysis else {"content": "public"}
    )
    assert started.payload.channels == ("content", "analysis")
    assert len(events[-1].payload.channel_digests) == 2
    assert display_event(event("model.prepared", 5), include_analysis=include_analysis) is None


def test_cached_projection_preserves_terminal_ownership_and_rejects_corruption():
    from uuid import uuid4

    from agent_runtime.contracts.views import SnapshotEnvelope
    from agent_runtime.projection import ProjectionState

    started = event("item.started", 1, channels=["content"])
    view = SessionProjection(started.session_id)
    view.apply(started)
    view.apply(event("assistant.delta", 2, text="partial", deltaIndex="0"))
    view.apply(event("run.cancelled", 3, finalItemIds=[str(started.item_id)]))
    snapshot = SnapshotEnvelope(
        schema_revision=1,
        projection_version=1,
        session_id=view.session_id,
        snapshot_seq="3",
        retained_from_seq="4",
        auth_revision="1",
        items=tuple(view.items.values()),
        runs=(),
        plans=(),
        resource_refs=(),
    )
    state = ProjectionState(
        snapshot=snapshot,
        item_runs=view.item_runs,
        terminal_events=tuple(view.terminal_events.values()),
    )
    restored = SessionProjection.restore(state, include_analysis=False)
    assert restored.final_items == view.final_items and restored.terminals == view.terminals
    assert restored.items == view.items
    with pytest.raises(ValueError, match="ownership"):
        SessionProjection.restore(
            state.model_copy(update={"item_runs": {started.item_id: uuid4()}}),
            include_analysis=False,
        )
    with pytest.raises(ValueError, match="invalid"):
        SessionProjection.restore(
            state.model_copy(
                update={"terminal_events": (*state.terminal_events, *state.terminal_events)}
            ),
            include_analysis=False,
        )
    with pytest.raises(ValueError, match="version"):
        SessionProjection.restore(
            state.model_copy(
                update={"snapshot": snapshot.model_copy(update={"projection_version": 2})}
            ),
            include_analysis=False,
        )

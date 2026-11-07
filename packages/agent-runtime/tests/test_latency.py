import json
from datetime import timedelta
from importlib.resources import files
from uuid import uuid4

import pytest

from agent_runtime.contracts.events import event_adapter
from agent_runtime.latency import ModelAttemptTiming, measure_turn, percentiles


def test_latency_replay_deduplicates_and_separates_completion():
    fixtures = json.loads(
        files("agent_runtime.contracts").joinpath("fixtures/events.json").read_text()
    )
    by_kind = {entry["kind"]: entry for entry in fixtures["events"]}
    base = event_adapter.validate_python(by_kind["turn.accepted"])
    sequence = [
        ("turn.accepted", 0),
        ("analysis.delta", 1),
        ("assistant.delta", 2),
        ("item.completed", 3),
        ("run.completed", 4),
    ]
    events = [
        event_adapter.validate_python(by_kind[kind]).model_copy(
            update={
                "event_id": uuid4(),
                "run_id": base.run_id,
                "session_seq": str(index + 1),
                "occurred_at": base.occurred_at + timedelta(seconds=seconds),
            }
        )
        for index, (kind, seconds) in enumerate(sequence)
    ]
    result = measure_turn([*reversed(events), events[2]], base.run_id)
    assert result.milliseconds["first_content_ms"] == 2000
    assert result.milliseconds["terminal_tail_ms"] == 1000
    assert result.milliseconds["queue_ms"] is None
    assert result.queued_after_last_content is False
    assert measure_turn(events, uuid4()).terminal_kind is None
    attempt = ModelAttemptTiming(
        base.run_id,
        uuid4(),
        base.occurred_at + timedelta(milliseconds=500),
        base.occurred_at + timedelta(milliseconds=200),
    )
    foreign = ModelAttemptTiming(uuid4(), uuid4(), base.occurred_at, base.occurred_at)
    measured = measure_turn(events, base.run_id, [attempt, attempt, foreign])
    assert measured.model_attempts == 1
    assert measured.milliseconds["model_first_output_ms"] == 500


def test_percentile_counts_and_invalid_samples():
    assert percentiles(list(range(1, 21))) == {"n": 20, "p50": 10, "p95": 19}
    assert percentiles([]) == {"n": 0, "p50": None, "p95": None}
    for sample in (-1, float("nan"), float("inf")):
        with pytest.raises(ValueError):
            percentiles([sample])

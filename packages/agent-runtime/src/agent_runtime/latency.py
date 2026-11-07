"""Read-only Turn latency projection from authoritative Journal events.

This is not tracing on the critical path. Missing milestones remain None, and
timestamps never mix server occurrence time with browser receipt time.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from math import ceil
from uuid import UUID

from agent_runtime.contracts.events import EventEnvelope


@dataclass(frozen=True, slots=True)
class TurnLatency:
    run_id: UUID
    terminal_kind: str | None
    milliseconds: dict[str, float | None]
    model_attempts: int
    tool_calls: int
    queued_after_last_content: bool


@dataclass(frozen=True, slots=True)
class ModelAttemptTiming:
    run_id: UUID
    attempt_id: UUID
    started_at: datetime | None
    context_prepared_at: datetime


def measure_turn(
    events: Sequence[EventEnvelope],
    run_id: UUID,
    model_timings: Sequence[ModelAttemptTiming] = (),
) -> TurnLatency:
    """Replay-safe projection; include restricted model.prepared for call counts."""
    unique = {event.event_id: event for event in events if event.run_id == run_id}
    ordered = sorted(unique.values(), key=lambda event: int(event.session_seq))
    marks: dict[str, datetime] = {}
    terminal_kind = None
    last_content_seq = None
    queued_sequences: list[int] = []
    attempts: set[UUID] = set()
    tools: set[UUID] = set()
    selected_timings = [timing for timing in model_timings if timing.run_id == run_id]
    if selected_timings:
        marks["model_prepared"] = min(t.context_prepared_at for t in selected_timings)
        started = [t.started_at for t in selected_timings if t.started_at is not None]
        if started:
            marks["model_started"] = min(started)
        attempts.update(t.attempt_id for t in selected_timings)
    for event in ordered:
        stamp = event.occurred_at
        if event.kind == "turn.accepted":
            marks.setdefault("accepted", stamp)
        elif event.kind == "run.status":
            if event.payload.status.value == "running":
                marks.setdefault("running", stamp)
            if event.payload.status.value == "queued":
                queued_sequences.append(int(event.session_seq))
        elif event.kind == "model.prepared":
            marks.setdefault("model_prepared", stamp)
            attempts.add(event.payload.attempt_id)
        elif event.kind == "analysis.delta" and event.payload.text:
            marks.setdefault("first_analysis", stamp)
            marks.setdefault("first_output", stamp)
        elif event.kind == "assistant.delta" and event.payload.text:
            marks.setdefault("first_content", stamp)
            marks.setdefault("first_output", stamp)
            marks["last_content"] = stamp
            last_content_seq = int(event.session_seq)
        elif event.kind == "item.completed":
            marks["message_completed"] = stamp
        elif event.kind == "tool.started":
            tools.add(event.payload.tool_call_id)
        elif event.kind in {"run.completed", "run.failed", "run.cancelled"}:
            marks["terminal"] = stamp
            terminal_kind = event.kind

    def elapsed(start: str, end: str) -> float | None:
        if start not in marks or end not in marks:
            return None
        return (marks[end] - marks[start]).total_seconds() * 1000

    pairs = {
        "queue_ms": ("accepted", "running"),
        "prepare_ms": ("running", "model_prepared"),
        "first_output_ms": ("accepted", "first_output"),
        "first_analysis_ms": ("accepted", "first_analysis"),
        "first_content_ms": ("accepted", "first_content"),
        "model_first_output_ms": ("model_started", "first_output"),
        "output_flush_ms": ("last_content", "message_completed"),
        "terminal_tail_ms": ("message_completed", "terminal"),
        "last_content_to_terminal_ms": ("last_content", "terminal"),
        "total_ms": ("accepted", "terminal"),
    }
    return TurnLatency(
        run_id=run_id,
        terminal_kind=terminal_kind,
        milliseconds={key: elapsed(*pair) for key, pair in pairs.items()},
        model_attempts=len(attempts),
        tool_calls=len(tools),
        queued_after_last_content=last_content_seq is not None
        and any(sequence > last_content_seq for sequence in queued_sequences),
    )


def percentiles(samples: Sequence[float]) -> dict[str, float | int | None]:
    """Nearest-rank percentiles. Always expose n; small n is not a release SLO."""
    from math import isfinite

    if any(not isfinite(sample) or sample < 0 for sample in samples):
        raise ValueError("latency samples must be finite and nonnegative")
    values = sorted(samples)
    return {
        "n": len(values),
        "p50": values[ceil(len(values) * 0.50) - 1] if values else None,
        "p95": values[ceil(len(values) * 0.95) - 1] if values else None,
    }

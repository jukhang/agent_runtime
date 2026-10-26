import json
from importlib.resources import files
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.provider import provider_event_adapter
from agent_runtime.provider_integrity import ProviderIntegrity

PROVIDER_FIXTURE = json.loads(
    files("agent_runtime.contracts").joinpath("fixtures/provider-integrity.json").read_text()
)


def gate(**kwargs):
    return ProviderIntegrity(
        uuid4(),
        max_argument_bytes=kwargs.get("size", 4096),
        max_content_bytes=kwargs.get("content", 4096),
        max_analysis_bytes=kwargs.get("analysis", 4096),
        max_calls=kwargs.get("calls", 4),
    )


def feed(g, sequence, kind, **kwargs):
    event = provider_event_adapter.validate_python(
        {
            "attemptId": str(g.attempt_id),
            "sequence": str(sequence),
            "kind": kind,
            **kwargs,
        }
    )
    g.feed(event)


def test_tool_arguments_released_only_after_terminal():
    g = gate()
    feed(g, 0, "tool.arguments.delta", callId="c", text='{"x":')
    feed(g, 1, "tool.arguments.delta", callId="c", text="1}")
    feed(g, 2, "tool.complete", callId="c", name="search", arguments={"x": 1})
    feed(g, 3, "finish", reason="tool_calls", rawReason="tool_calls")
    calls = g.completed_calls()
    assert calls[0].arguments == {"x": 1}
    calls[0].arguments["x"] = 2
    assert g.completed_calls()[0].arguments == {"x": 1}


@pytest.mark.parametrize("reason", ["length", "content_filter", "cancelled", "unknown"])
def test_bad_terminal(reason):
    g = gate()
    with pytest.raises(AgentRuntimeError):
        feed(g, 0, "finish", reason=reason, rawReason=reason)
    with pytest.raises(AgentRuntimeError):
        g.completed_calls()


def test_missing_terminal_never_releases_calls():
    g = gate()
    feed(g, 0, "tool.complete", callId="c", name="search", arguments={})
    with pytest.raises(AgentRuntimeError):
        g.completed_calls()


@pytest.mark.parametrize("raw", ['{"x":1,"x":1}', '{"x":NaN}', "[]", "{", '{"x":2}'])
def test_invalid_or_inconsistent_json(raw):
    g = gate()
    feed(g, 0, "tool.arguments.delta", callId="c", text=raw)
    with pytest.raises(AgentRuntimeError):
        feed(g, 1, "tool.complete", callId="c", name="search", arguments={"x": 1})


def test_incomplete_call_and_wrong_finish():
    for reason in ("stop", "tool_calls"):
        g = gate()
        feed(g, 0, "tool.arguments.delta", callId="c", text="{")
        with pytest.raises(AgentRuntimeError):
            feed(g, 1, "finish", reason=reason, rawReason=reason)


def test_sequence_and_attempt_isolation():
    for patch in ({"sequence": "1"}, {"attemptId": str(uuid4())}):
        g = gate()
        event = provider_event_adapter.validate_python(
            {
                "kind": "content.delta",
                "attemptId": str(g.attempt_id),
                "sequence": "0",
                "text": "hello",
                **patch,
            }
        )
        with pytest.raises(AgentRuntimeError):
            g.feed(event)


def test_bounds_and_duplicate_call():
    g = gate(size=1)
    with pytest.raises(AgentRuntimeError):
        feed(g, 0, "tool.arguments.delta", callId="c", text="ab")
    for call in ("c", "d"):
        g = gate(calls=1)
        feed(g, 0, "tool.complete", callId="c", name="search", arguments={})
        with pytest.raises(AgentRuntimeError):
            feed(g, 1, "tool.complete", callId=call, name="search", arguments={})


def test_text_analysis_usage_and_stop():
    g = gate()
    feed(g, 0, "analysis.delta", text="Checking sources")
    feed(g, 1, "content.delta", text="Answer")
    feed(
        g,
        2,
        "usage",
        usage={
            "status": "unknown",
            "reason": "provider_not_reported",
            "inputTokens": None,
            "outputTokens": None,
            "totalTokens": None,
        },
    )
    feed(g, 3, "finish", reason="stop", rawReason="stop")
    assert not g.completed_calls()
    with pytest.raises(AgentRuntimeError):
        feed(g, 4, "content.delta", text="late")


def test_error_and_unknown_events():
    g = gate()
    with pytest.raises(AgentRuntimeError) as exc:
        feed(
            g,
            0,
            "error",
            failure={
                "code": "PROVIDER_TIMEOUT",
                "category": "provider",
                "retryable": True,
                "publicMessage": "Timed out",
            },
        )
    assert exc.value.failure.code == "PROVIDER_TIMEOUT"
    with pytest.raises(ValidationError):
        feed(g, 1, "unknown")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"size": 0},
        {"size": -1},
        {"size": True},
        {"size": 1.5},
        {"content": 0},
        {"analysis": -2},
        {"calls": 1.5},
    ],
)
def test_invalid_limits(kwargs):
    with pytest.raises(ValueError):
        gate(**kwargs)


@pytest.mark.parametrize(
    ("streamed", "complete"),
    [
        ('{"x":true}', {"x": 1}),
        ('{"x":1}', {"x": 1.0}),
        ('{"x":1}', {"x": True}),
        ('{"x":[1]}', {"x": [True]}),
    ],
)
def test_streamed_and_complete_arguments_compare_by_json_type(streamed, complete):
    g = gate()
    feed(g, 0, "tool.arguments.delta", callId="c", text=streamed)
    with pytest.raises(AgentRuntimeError):
        feed(g, 1, "tool.complete", callId="c", name="search", arguments=complete)


def test_key_order_is_not_a_mismatch():
    g = gate()
    feed(g, 0, "tool.arguments.delta", callId="c", text='{"a": 1, "b": 2}')
    feed(g, 1, "tool.complete", callId="c", name="search", arguments={"b": 2, "a": 1})
    feed(g, 2, "finish", reason="tool_calls", rawReason="tool_calls")
    assert g.completed_calls()[0].arguments == {"b": 2, "a": 1}


def test_arguments_counted_once_per_attempt():
    # '{"x": 1}' is 8 bytes; the completion must not count the same content again.
    g = gate(size=8)
    feed(g, 0, "tool.arguments.delta", callId="c", text='{"x": 1}')
    feed(g, 1, "tool.complete", callId="c", name="search", arguments={"x": 1})
    feed(g, 2, "finish", reason="tool_calls", rawReason="tool_calls")
    assert len(g.completed_calls()) == 1


def test_complete_only_call_counts_canonical_bytes():
    g = gate(size=7)  # canonical '{"x":1}'
    feed(g, 0, "tool.complete", callId="c", name="search", arguments={"x": 1})
    feed(g, 1, "finish", reason="tool_calls", rawReason="tool_calls")
    assert len(g.completed_calls()) == 1
    g = gate(size=6)
    with pytest.raises(AgentRuntimeError):
        feed(g, 0, "tool.complete", callId="c", name="search", arguments={"x": 1})


def test_argument_limit_is_per_attempt_across_calls():
    g = gate(size=10)
    feed(g, 0, "tool.complete", callId="a", name="t", arguments={"x": 1})
    with pytest.raises(AgentRuntimeError):
        feed(g, 1, "tool.complete", callId="b", name="t", arguments={"y": 2})


def test_content_and_analysis_limits_are_independent():
    g = gate(content=4, analysis=1)
    feed(g, 0, "content.delta", text="abcd")
    with pytest.raises(AgentRuntimeError):
        feed(g, 1, "analysis.delta", text="ab")


def test_channel_limits_accumulate_across_deltas():
    g = gate(content=3)
    feed(g, 0, "content.delta", text="ab")
    with pytest.raises(AgentRuntimeError):
        feed(g, 1, "content.delta", text="cd")


def test_rejections_use_the_registered_integrity_code():
    g = gate(content=1)
    with pytest.raises(AgentRuntimeError) as exc:
        feed(g, 0, "content.delta", text="ab")
    assert exc.value.failure.code == "INCOMPLETE_MODEL_OUTPUT"
    assert exc.value.failure.category == "provider"


@pytest.mark.parametrize(
    ("kind", "kwargs"),
    [
        ("content.delta", {"text": "bad\ud800"}),
        ("analysis.delta", {"text": "bad\ud800"}),
        ("tool.arguments.delta", {"callId": "c", "text": "bad\ud800"}),
    ],
)
def test_unencodable_text_is_a_registered_integrity_failure(kind, kwargs):
    g = gate()
    with pytest.raises(AgentRuntimeError) as exc:
        feed(g, 0, kind, **kwargs)
    assert exc.value.failure.code == "INCOMPLETE_MODEL_OUTPUT"
    assert exc.value.failure.category == "provider"
    with pytest.raises(AgentRuntimeError):
        g.completed_calls()


def test_unencodable_complete_only_arguments_are_rejected():
    g = gate()
    with pytest.raises(AgentRuntimeError) as exc:
        feed(g, 0, "tool.complete", callId="c", name="search", arguments={"x": "bad\ud800"})
    assert exc.value.failure.code == "INCOMPLETE_MODEL_OUTPUT"
    with pytest.raises(AgentRuntimeError):
        g.completed_calls()


def test_escaped_surrogate_arguments_never_reach_completed_calls():
    g = gate()
    feed(g, 0, "tool.arguments.delta", callId="c", text='{"x":"\\ud800"}')
    with pytest.raises(AgentRuntimeError) as exc:
        feed(g, 1, "tool.complete", callId="c", name="search", arguments={"x": "\ud800"})
    assert exc.value.failure.code == "INCOMPLETE_MODEL_OUTPUT"
    with pytest.raises(AgentRuntimeError):
        g.completed_calls()


@pytest.mark.parametrize("case", PROVIDER_FIXTURE["cases"], ids=lambda case: case["name"])
def test_shared_provider_integrity_fixtures(case):
    limits = PROVIDER_FIXTURE["limits"]
    g = ProviderIntegrity(
        uuid4(),
        max_argument_bytes=limits["argument"],
        max_content_bytes=limits["content"],
        max_analysis_bytes=limits["analysis"],
        max_calls=limits["calls"],
    )
    events = []
    for index, event in enumerate(case["events"]):
        payload = {"attemptId": str(g.attempt_id), "sequence": str(index), "kind": event["kind"]}
        payload.update({key: value for key, value in event.items() if key != "kind"})
        events.append(provider_event_adapter.validate_python(payload))
    if case["expect"] == "accept":
        for event in events:
            g.feed(event)
        g.completed_calls()
        return
    if case["expect"] == "reject_feed":
        with pytest.raises(AgentRuntimeError):
            for event in events:
                g.feed(event)
        return
    if case["expect"] == "reject_terminal":
        for event in events:
            g.feed(event)
        with pytest.raises(AgentRuntimeError):
            g.completed_calls()
        return
    raise AssertionError(f"unknown expectation: {case['expect']}")

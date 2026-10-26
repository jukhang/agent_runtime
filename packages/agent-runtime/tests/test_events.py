import json
from copy import deepcopy
from importlib.resources import files

import pytest
from pydantic import ValidationError

from agent_runtime.contracts.events import CursorFrame, ResyncFrame, event_adapter
from agent_runtime.contracts.payloads import ItemCompleted, PlanUpdated
from agent_runtime.contracts.requests import StreamRequest, SubmitRequest
from agent_runtime.contracts.results import result_adapter
from agent_runtime.contracts.schema import schema_documents
from agent_runtime.contracts.views import RunView, SnapshotEnvelope

FIXTURE = json.loads(files("agent_runtime.contracts").joinpath("fixtures/events.json").read_text())
EVENTS = {event["kind"]: event for event in FIXTURE["events"]}
IDS = EVENTS["turn.accepted"]
INVALID_EVENTS = json.loads(
    files("agent_runtime.contracts").joinpath("fixtures/invalid-events.json").read_text()
)


@pytest.mark.parametrize("case", INVALID_EVENTS)
def test_shared_invalid_fixtures(case: dict) -> None:
    event = deepcopy(EVENTS[case["base"]])
    target = event
    for key in case["path"][:-1]:
        target = target[key]
    target[case["path"][-1]] = case["value"]
    with pytest.raises(ValidationError):
        event_adapter.validate_python(event)


@pytest.mark.parametrize("kind", list(EVENTS))
def test_all_events_roundtrip(kind: str) -> None:
    event = event_adapter.validate_python(EVENTS[kind])
    encoded = event_adapter.dump_json(event, by_alias=True)
    assert event_adapter.validate_json(encoded) == event


def test_fixture_covers_entire_discriminator() -> None:
    mapping = event_adapter.json_schema()["discriminator"]["mapping"]
    assert set(mapping) == set(EVENTS)
    assert "input.accepted" in mapping


def test_input_request_cannot_target_another_run() -> None:
    event = deepcopy(EVENTS["input.required"])
    event["payload"]["replyToRunId"] = "00000000-0000-4000-8000-000000000099"
    with pytest.raises(ValidationError):
        event_adapter.validate_python(event)


@pytest.mark.parametrize("kind", list(EVENTS))
def test_event_payloads_reject_unknown_fields(kind: str) -> None:
    event = deepcopy(EVENTS[kind])
    event["payload"]["undocumented"] = True
    with pytest.raises(ValidationError):
        event_adapter.validate_python(event)


@pytest.mark.parametrize(
    "mutation",
    [
        {"kind": "unknown"},
        {"schemaRevision": 2},
        {"schemaRevision": True},
        {"schemaRevision": 1.0},
        {"eventId": "invalid"},
        {"sessionSeq": "0"},
        {"sessionSeq": "01"},
        {"sessionSeq": 1},
        {"itemId": None},
        {"runId": None},
        {"recordedAt": "2026-09-14T10:00:00"},
    ],
)
def test_invalid_event_envelope(mutation: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        event_adapter.validate_python({**EVENTS["assistant.delta"], **mutation})


def test_channels_cannot_be_interchanged() -> None:
    event = deepcopy(EVENTS["assistant.delta"])
    event["payload"] = EVENTS["analysis.delta"]["payload"]
    with pytest.raises(ValidationError):
        event_adapter.validate_python(event)


@pytest.mark.parametrize(
    "kind",
    [
        "model.prepared",
        "mailbox.applied",
        "handoff.committed",
        "checkpoint.migrated",
    ],
)
def test_internal_events_cannot_be_public(kind: str) -> None:
    with pytest.raises(ValidationError):
        event_adapter.validate_python({**EVENTS[kind], "visibility": "public"})


def test_required_nullable_identity_cannot_be_omitted() -> None:
    event = deepcopy(EVENTS["assistant.delta"])
    del event["parentRunId"]
    with pytest.raises(ValidationError):
        event_adapter.validate_python(event)


def test_terminal_payload_requires_correct_failure_and_outcome() -> None:
    for kind, field, value in (
        ("run.completed", "outcome", None),
        ("run.failed", "failure", None),
        ("run.cancelled", "outcome", "success"),
    ):
        event = deepcopy(EVENTS[kind])
        event["payload"][field] = value
        with pytest.raises(ValidationError):
            event_adapter.validate_python(event)


def test_duplicate_delivery_keeps_identity() -> None:
    first = event_adapter.validate_python(EVENTS["assistant.delta"])
    second = event_adapter.validate_python(deepcopy(EVENTS["assistant.delta"]))
    assert first.event_id == second.event_id
    assert first.session_seq == second.session_seq


def test_snapshot_watermarks_and_duplicate_items() -> None:
    snapshot = {
        "schemaRevision": 1,
        "projectionVersion": 1,
        "sessionId": IDS["sessionId"],
        "snapshotSeq": "1",
        "authRevision": "1",
        "retainedFromSeq": "2",
        "items": [IDS["payload"]["userItem"]],
        "runs": [],
        "plans": [],
        "resourceRefs": [],
    }
    assert SnapshotEnvelope.model_validate(snapshot).snapshot_seq == "1"
    for patch in ({"snapshotSeq": "0"}, {"retainedFromSeq": "3"}, {"items": snapshot["items"] * 2}):
        with pytest.raises(ValidationError):
            SnapshotEnvelope.model_validate({**snapshot, **patch})


def test_plan_cycles_and_missing_parents() -> None:
    step = {
        "id": IDS["runId"],
        "parentStepId": IDS["runId"],
        "label": "A",
        "status": "pending",
        "evidenceRefs": [],
        "runRefs": [],
    }
    for parent in (IDS["runId"], IDS["turnId"]):
        with pytest.raises(ValidationError):
            PlanUpdated.model_validate(
                {
                    "planId": IDS["runId"],
                    "revision": "1",
                    "steps": [{**step, "parentStepId": parent}],
                    "publicReason": "Planning",
                }
            )


def test_item_completion_channel_integrity() -> None:
    with pytest.raises(ValidationError):
        ItemCompleted.model_validate(
            {
                "itemRevision": "1",
                "status": "completed",
                "channelDigests": [{"channel": "content", "digest": "a" * 64}],
                "lastDeltaIndexes": [],
            }
        )


def test_submit_no_actor_override_and_nonempty_input() -> None:
    request = {
        "sessionId": IDS["sessionId"],
        "agent": {"key": {"namespace": "builtin", "key": "a"}, "revision": "1"},
        "idempotencyKey": "req",
        "inputs": [{"kind": "text", "text": "Hello"}],
    }
    assert SubmitRequest.model_validate(request).inputs[0].kind == "text"
    for patch in (
        {"actorId": "admin"},
        {"scopeId": "admin"},
        {"inputs": []},
        {"inputs": [{"kind": "text", "text": "  "}]},
    ):
        with pytest.raises(ValidationError):
            SubmitRequest.model_validate({**request, **patch})


def test_cursor_reconciliation_and_control_not_business_event() -> None:
    request = {"sessionId": IDS["sessionId"], "afterSeq": "4", "lastEventId": "4"}
    assert StreamRequest.model_validate(request).after_seq == "4"
    with pytest.raises(ValidationError):
        StreamRequest.model_validate({**request, "lastEventId": "3"})
    cursor = CursorFrame(kind="cursor.advanced", cursor="4", auth_revision="1")
    with pytest.raises(ValidationError):
        event_adapter.validate_python(cursor.model_dump())


@pytest.mark.parametrize(
    "url", ["https://other.example/s", "//other/s", "/\\other/s", "javascript:alert(1)", "/ bad"]
)
def test_resync_is_not_an_external_navigation(url: str) -> None:
    with pytest.raises(ValidationError):
        ResyncFrame(kind="resync_required", reason="cursor_expired", snapshot_url=url)


def test_run_result_cannot_be_empty_failed_success() -> None:
    result = {
        "runId": IDS["runId"],
        "status": "failed",
        "outcome": None,
        "failure": EVENTS["run.failed"]["payload"]["failure"],
        "finalItems": [],
        "artifactRefs": [],
        "partialResults": [],
        "usage": {
            "status": "unknown",
            "reason": "provider_not_reported",
            "inputTokens": None,
            "outputTokens": None,
            "totalTokens": None,
        },
    }
    assert result_adapter.validate_python(result).status == "failed"
    for patch in ({"failure": None}, {"status": "running"}, {"outcome": "success"}):
        with pytest.raises(ValidationError):
            result_adapter.validate_python({**result, **patch})


def test_run_phase_and_controls() -> None:
    view = {
        "id": IDS["runId"],
        "status": "completed",
        "stateVersion": "2",
        "phase": "terminal",
        "phaseStartedAt": IDS["recordedAt"],
        "allowedControls": [],
    }
    assert RunView.model_validate(view).phase == "terminal"
    with pytest.raises(ValidationError):
        RunView.model_validate({**view, "allowedControls": ["resume"]})


def test_exported_schemas_match_source() -> None:
    for name, expected in schema_documents().items():
        actual = files("agent_runtime.contracts").joinpath("schemas", name).read_text()
        assert actual == expected

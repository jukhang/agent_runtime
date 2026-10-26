import pytest
from pydantic import ValidationError
from test_events import EVENTS

from agent_runtime.contracts.events import event_adapter
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.state_events import (
    StateEventBatch,
    StateEventReceipt,
    state_draft_adapter,
)
from agent_runtime.state_event_validation import (
    materialize_state_event,
    state_event_digest,
    validate_state_receipt,
)

KINDS = set(EVENTS) - {"assistant.delta", "analysis.delta"}


def draft(kind):
    event = event_adapter.validate_python(EVENTS[kind])
    data = event.model_dump(
        exclude={
            "schema_revision",
            "event_id",
            "session_id",
            "session_seq",
            "recorded_at",
            "visibility",
            "trace_id",
        }
    )
    return state_draft_adapter.validate_python({**data, "producer_key": f"state/{event.event_id}"})


@pytest.mark.parametrize("kind", sorted(KINDS))
def test_all_nontext_drafts_materialize_the_same_wire_event(kind):
    event = event_adapter.validate_python(EVENTS[kind])
    output = materialize_state_event(
        draft(kind),
        event_id=event.event_id,
        session_id=event.session_id,
        session_seq=event.session_seq,
        recorded_at=event.recorded_at,
        trace_id=event.trace_id,
    )
    assert output == event


def test_every_published_nontext_kind_has_a_draft():
    assert set(state_draft_adapter.json_schema()["discriminator"]["mapping"]) == KINDS


@pytest.mark.parametrize(
    "key,value", [("event_id", "fake"), ("session_seq", "9"), ("visibility", "public")]
)
def test_producer_cannot_choose_journal_identity_or_publicize_private_event(key, value):
    with pytest.raises(ValidationError):
        state_draft_adapter.validate_python({**draft("model.prepared").model_dump(), key: value})


def test_resource_service_cannot_forge_model_or_run_completion_and_receipt_is_bound():
    event = event_adapter.validate_python(EVENTS["resource.revoked"])
    batch_data = {
        "sessionId": str(event.session_id),
        "traceId": event.trace_id,
        "authority": {
            "kind": "service",
            "purpose": "resource",
            "transactionId": str(event.event_id),
            "actor": {
                "scopeId": "scope",
                "actorId": "user",
                "authRevision": "1",
                "grantsRef": "grant",
            },
            "source": {
                "resourceType": "artifact",
                "resourceId": "artifact",
                "resourceVersion": "1",
                "scope": {"scopeId": "scope"},
            },
        },
        "events": [draft("resource.revoked").model_dump()],
    }
    batch = StateEventBatch.model_validate(batch_data)
    authority = batch_data["authority"]
    with pytest.raises(ValidationError, match="session"):
        StateEventBatch.model_validate(
            {
                **batch_data,
                "authority": {
                    **authority,
                    "source": {
                        **authority["source"],
                        "scope": {"scopeId": "scope", "sessionId": "another-session"},
                    },
                },
            }
        )
    for kind in ("model.prepared", "run.completed"):
        with pytest.raises(ValidationError):
            StateEventBatch.model_validate({**batch_data, "events": [draft(kind).model_dump()]})
    entry = {
        "producerKey": batch.events[0].producer_key,
        "contentDigest": state_event_digest(batch.events[0]),
        "eventId": str(event.event_id),
        "sessionSeq": event.session_seq,
        "disposition": "created",
    }
    receipt = {"scopeId": "scope", "sessionId": str(event.session_id), "entries": [entry]}
    validate_state_receipt(batch, StateEventReceipt.model_validate(receipt))
    with pytest.raises(AgentRuntimeError):
        validate_state_receipt(
            batch,
            StateEventReceipt.model_validate(
                {**receipt, "entries": [{**entry, "contentDigest": "0" * 64}]}
            ),
        )


def service_authority(event, purpose, resource_type, *, resource_id, session):
    scope = {"scopeId": "scope"}
    if session:
        scope["sessionId"] = str(event.session_id)
    return {
        "kind": "service",
        "purpose": purpose,
        "transactionId": str(event.event_id),
        "actor": {
            "scopeId": "scope",
            "actorId": "worker",
            "authRevision": "1",
            "grantsRef": "grant",
        },
        "source": {
            "resourceType": resource_type,
            "resourceId": resource_id,
            "resourceVersion": "1",
            "scope": scope,
        },
    }


def service_batch(
    kind, purpose, resource_type, *, resource_id=None, draft_changes=None, session=True
):
    event = event_adapter.validate_python(EVENTS[kind])
    entry = draft(kind).model_dump()
    entry.update(draft_changes or {})
    return {
        "sessionId": str(event.session_id),
        "traceId": event.trace_id,
        "authority": service_authority(
            event,
            purpose,
            resource_type,
            resource_id=resource_id or str(event.run_id),
            session=session,
        ),
        "events": [entry],
    }


@pytest.mark.parametrize(
    ("kind", "purpose", "resource", "changes"),
    [
        ("tool.finished", "tool_result", "execution", {"run_id": None}),
        ("item.started", "admission", "turn", {"item_id": None}),
    ],
)
def test_draft_identity_rules_match_the_committed_envelope(kind, purpose, resource, changes):
    """F-B3: a batch the journal could never materialize must be refused up front."""
    event = event_adapter.validate_python(EVENTS[kind])
    resource_id = str(event.turn_id) if purpose == "admission" else str(event.run_id)
    payload = service_batch(
        kind, purpose, resource, resource_id=resource_id, draft_changes=changes
    )
    with pytest.raises(ValidationError):
        StateEventBatch.model_validate(payload)
    # The same draft is accepted once the missing identity is restored.
    StateEventBatch.model_validate(
        service_batch(kind, purpose, resource, resource_id=resource_id)
    )


@pytest.mark.parametrize(
    ("purpose", "kind", "resource"),
    [
        ("tool_result", "tool.finished", "execution"),
        ("recovery", "run.failed", "run"),
        ("admission", "turn.accepted", "turn"),
        ("control", "run.status", "command"),
    ],
)
def test_session_bound_service_source_must_name_the_session(purpose, kind, resource):
    event = event_adapter.validate_python(EVENTS[kind])
    resource_id = str(event.turn_id) if purpose == "admission" else str(event.run_id)
    payload = service_batch(kind, purpose, resource, resource_id=resource_id, session=False)
    with pytest.raises(ValidationError, match="session"):
        StateEventBatch.model_validate(payload)
    payload["authority"]["source"]["scope"]["sessionId"] = payload["sessionId"]
    StateEventBatch.model_validate(payload)


def test_scope_level_resource_service_needs_no_session():
    StateEventBatch.model_validate(
        service_batch("resource.revoked", "resource", "artifact", session=False)
    )

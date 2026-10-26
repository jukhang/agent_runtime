import json
from copy import deepcopy
from importlib.resources import files
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.contracts.decisions import decision_adapter
from agent_runtime.contracts.messages import message_adapter
from agent_runtime.contracts.transactions import (
    ApplyInputRequest,
    CheckpointMigrationRequest,
    CommitGuard,
    DomainCommitRequest,
    HandoffRequest,
)

DATA = json.loads(files("agent_runtime.contracts").joinpath("fixtures/internal.json").read_text())
MESSAGE = DATA["messages"][0]
ID = MESSAGE["senderRunId"]
OTHER = MESSAGE["recipientRunId"]
GUARD = {
    "actor": {"scopeId": "scope", "actorId": "user", "authRevision": "1", "grantsRef": "grants"},
    "lease": {
        "scopeId": "scope",
        "sessionId": ID,
        "runId": ID,
        "workId": ID,
        "ownerId": "worker",
        "runEpoch": "1",
        "workEpoch": "1",
        "slotVersion": "1",
        "runLeaseUntil": MESSAGE["deadlineAt"],
        "workLeaseUntil": MESSAGE["deadlineAt"],
    },
    "expectedStateVersion": "1",
}


@pytest.mark.parametrize(
    "family,adapter", [("messages", message_adapter), ("actions", decision_adapter)]
)
def test_exhaustive_fixtures(family, adapter):
    assert set(adapter.json_schema()["discriminator"]["mapping"]) == {
        item["kind"] for item in DATA[family]
    }


@pytest.mark.parametrize("item", DATA["messages"])
def test_message_roundtrip(item):
    model = message_adapter.validate_python(item)
    assert message_adapter.validate_json(message_adapter.dump_json(model, by_alias=True)) == model


@pytest.mark.parametrize("item", DATA["actions"])
def test_action_roundtrip(item):
    model = decision_adapter.validate_python(item)
    assert decision_adapter.validate_json(decision_adapter.dump_json(model, by_alias=True)) == model


@pytest.mark.parametrize("item", DATA["messages"] + DATA["actions"])
def test_unknown_payload_fields_rejected(item):
    adapter = message_adapter if "protocolRevision" in item else decision_adapter
    changed = deepcopy(item)
    changed["payload"]["executionAuthorized"] = True
    with pytest.raises(ValidationError):
        adapter.validate_python(changed)


@pytest.mark.parametrize(
    "patch",
    [
        {"protocolRevision": True},
        {"protocolRevision": 1.0},
        {"protocolRevision": 2},
        {"recipientSeq": "0"},
        {"recipientSeq": 1},
        {"recipientRunId": ID},
        {"deadlineAt": MESSAGE["createdAt"]},
        {"causationId": MESSAGE["messageId"]},
        {"kind": "unknown"},
        {"createdAt": "2026-09-14T10:00:00"},
    ],
)
def test_invalid_delivery(patch):
    with pytest.raises(ValidationError):
        message_adapter.validate_python({**MESSAGE, **patch})


def test_duplicate_tool_calls():
    item = deepcopy(DATA["actions"][1])
    item["payload"]["validatedCalls"] *= 2
    with pytest.raises(ValidationError):
        decision_adapter.validate_python(item)


def test_guard_requires_scope_and_epochs():
    assert CommitGuard.model_validate(GUARD).lease.run_epoch == "1"
    for key in ("runEpoch", "workEpoch", "slotVersion"):
        data = deepcopy(GUARD)
        del data["lease"][key]
        with pytest.raises(ValidationError):
            CommitGuard.model_validate(data)
    data = deepcopy(GUARD)
    data["actor"]["scopeId"] = "other"
    with pytest.raises(ValidationError):
        CommitGuard.model_validate(data)


def test_handoff_target_does_not_claim_source_slot():
    data = {
        "sourceGuard": GUARD,
        "handoffId": ID,
        "transferManifestRef": "manifest",
        "target": {
            "scopeId": "scope",
            "sessionId": ID,
            "runId": OTHER,
            "expectedStateVersion": "1",
            "checkpointId": OTHER,
        },
    }
    assert (
        HandoffRequest.model_validate(data).target.run_id
        != HandoffRequest.model_validate(data).source_guard.lease.run_id
    )
    for patch in ({"runId": ID}, {"scopeId": "other"}, {"sessionId": OTHER}):
        with pytest.raises(ValidationError):
            HandoffRequest.model_validate({**data, "target": {**data["target"], **patch}})


def test_migration_preserves_source_without_active_run_lease():
    data = {
        "actor": GUARD["actor"],
        "lease": {
            "scopeId": "scope",
            "workId": ID,
            "ownerId": "maintenance",
            "workEpoch": "1",
            "workLeaseUntil": MESSAGE["deadlineAt"],
        },
        "runId": ID,
        "expectedStateVersion": "1",
        "sourceCheckpointId": ID,
        "targetCheckpointId": OTHER,
        "expectedRepresentationRevision": "0",
        "targetSchemaRevision": 2,
        "migrator": {"key": {"namespace": "builtin", "key": "migrate"}, "revision": "1"},
    }
    assert CheckpointMigrationRequest.model_validate(data).target_schema_revision == 2
    for patch in ({"targetCheckpointId": ID}, {"targetSchemaRevision": True}):
        with pytest.raises(ValidationError):
            CheckpointMigrationRequest.model_validate({**data, **patch})


def test_input_claims_order_and_watermark():
    entry = {"messageId": ID, "claimToken": OTHER, "recipientSeq": "1"}
    data = {"guard": GUARD, "inputs": [entry], "expectedAppliedSeq": "0", "checkpointRef": "cp"}
    assert ApplyInputRequest.model_validate(data).inputs[0].recipient_seq == "1"
    for patch in ({"inputs": []}, {"inputs": [entry, entry]}, {"expectedAppliedSeq": "1"}):
        with pytest.raises(ValidationError):
            ApplyInputRequest.model_validate({**data, **patch})


def test_input_claims_must_be_contiguous_from_the_watermark():
    def entry(seq, message_id=None):
        return {"messageId": message_id or str(uuid4()), "claimToken": OTHER, "recipientSeq": seq}

    data = {"guard": GUARD, "inputs": [], "expectedAppliedSeq": "1", "checkpointRef": "cp"}
    contiguous = [entry("2"), entry("3"), entry("4")]
    parsed = ApplyInputRequest.model_validate({**data, "inputs": contiguous})
    assert [item.recipient_seq for item in parsed.inputs] == ["2", "3", "4"]
    for inputs in (
        [entry("2"), entry("4")],  # gap
        [entry("3")],  # starts past the watermark
        [entry("1")],  # already applied
        [entry("3"), entry("2")],  # reordered
        [entry("2"), entry("2")],  # repeated sequence with a distinct message
        [entry(str(seq)) for seq in range(2, 67)],  # over the cap
    ):
        with pytest.raises(ValidationError):
            ApplyInputRequest.model_validate({**data, "inputs": inputs})


def test_domain_existing_resource_version_and_scope():
    resource = {
        "resourceType": "artifact",
        "resourceId": ID,
        "resourceVersion": "hash",
        "scope": {"scopeId": "scope"},
    }
    data = {
        "guard": GUARD,
        "operationId": ID,
        "resource": resource,
        "action": "revise",
        "changeRef": "change",
    }
    assert DomainCommitRequest.model_validate(data).resource.resource_version == "hash"
    for patch in ({"resourceVersion": None}, {"scope": {"scopeId": "other"}}):
        with pytest.raises(ValidationError):
            DomainCommitRequest.model_validate({**data, "resource": {**resource, **patch}})

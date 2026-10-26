import hashlib
from copy import deepcopy
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.journal import CommittedBatch, FlushProof, ProducerBatch, output_digest
from agent_runtime.journal_validation import validate_committed_batch

ID = str(uuid4())
LEASE = {
    "scopeId": "scope",
    "sessionId": ID,
    "runId": ID,
    "workId": ID,
    "ownerId": "worker",
    "runEpoch": "1",
    "workEpoch": "2",
    "slotVersion": "1",
    "runLeaseUntil": "2026-09-14T11:00:00Z",
    "workLeaseUntil": "2026-09-14T11:00:00Z",
}
DRAFT = {
    "kind": "assistant.delta",
    "occurredAt": "2026-09-14T10:00:00Z",
    "payload": {"channel": "content", "deltaIndex": "0", "itemRevision": "1", "text": "Hello"},
}
BATCH = {
    "lease": LEASE,
    "turnId": ID,
    "attemptId": ID,
    "itemId": ID,
    "traceId": "trace",
    "events": [DRAFT],
}
RECEIPT = {
    "scopeId": "scope",
    "sessionId": ID,
    "runId": ID,
    "attemptId": ID,
    "itemId": ID,
    "entries": [
        {
            "producerKey": f"{ID}/content/0",
            "contentDigest": output_digest(ProducerBatch.model_validate(BATCH).events[0]),
            "eventId": ID,
            "sessionSeq": "1",
            "disposition": "created",
        }
    ],
}
EMPTY = hashlib.sha256(b"").hexdigest()
PROOF = {
    "proofId": ID,
    "scopeId": "scope",
    "sessionId": ID,
    "runId": ID,
    "attemptId": ID,
    "itemId": ID,
    "durableSeq": "1",
    "channels": [
        {"channel": "content", "lastDeltaIndex": "0", "lastSessionSeq": "1", "digest": "a" * 64},
        {"channel": "analysis", "lastDeltaIndex": None, "lastSessionSeq": None, "digest": EMPTY},
    ],
}


def test_batch_and_receipt_roundtrip():
    request = ProducerBatch.model_validate(BATCH)
    receipt = CommittedBatch.model_validate(RECEIPT)
    validate_committed_batch(request, receipt)
    for value in (request, receipt, FlushProof.model_validate(PROOF)):
        assert type(value).model_validate_json(value.model_dump_json(by_alias=True)) == value


@pytest.mark.parametrize(
    "key,value", [("sessionSeq", "1"), ("eventId", ID), ("recordedAt", "2026-09-14T10:00:00Z")]
)
def test_producer_cannot_assign_journal_fields(key, value):
    with pytest.raises(ValidationError):
        ProducerBatch.model_validate({**BATCH, "events": [{**DRAFT, key: value}]})


@pytest.mark.parametrize(
    "patch", [{"deltaIndex": "0"}, {"deltaIndex": "2"}, {"deltaIndex": "1", "itemRevision": "2"}]
)
def test_duplicate_gap_or_mixed_revision(patch):
    second = {**DRAFT, "payload": {**DRAFT["payload"], **patch}}
    with pytest.raises(ValidationError):
        ProducerBatch.model_validate({**BATCH, "events": [DRAFT, second]})


def test_interleaved_channels_are_independently_ordered():
    analysis = {
        **DRAFT,
        "kind": "analysis.delta",
        "payload": {**DRAFT["payload"], "channel": "analysis"},
    }
    second = {**DRAFT, "payload": {**DRAFT["payload"], "deltaIndex": "1"}}
    assert (
        len(ProducerBatch.model_validate({**BATCH, "events": [DRAFT, analysis, second]}).events)
        == 3
    )


@pytest.mark.parametrize("text", ["x" * 65536, "\ud800"])
def test_byte_limit_and_utf8(text):
    draft = {**DRAFT, "payload": {**DRAFT["payload"], "text": text}}
    with pytest.raises(ValidationError):
        ProducerBatch.model_validate({**BATCH, "events": [draft]})


def test_event_count_limit():
    with pytest.raises(ValidationError):
        ProducerBatch.model_validate({**BATCH, "events": []})
    with pytest.raises(ValidationError):
        ProducerBatch.model_validate({**BATCH, "events": [DRAFT] * 65})


def test_retry_receipt_may_have_gaps_but_new_batch_may_not():
    second = {
        "producerKey": f"{ID}/content/1",
        "contentDigest": "a" * 64,
        "eventId": str(uuid4()),
        "sessionSeq": "3",
        "disposition": "created",
    }
    with pytest.raises(ValidationError):
        CommittedBatch.model_validate({**RECEIPT, "entries": [RECEIPT["entries"][0], second]})
    first = {**RECEIPT["entries"][0], "disposition": "reused"}
    assert CommittedBatch.model_validate({**RECEIPT, "entries": [first, second]})


@pytest.mark.parametrize("key", ["scopeId", "sessionId", "runId", "attemptId", "itemId"])
def test_receipt_identity_mismatch(key):
    data = {**RECEIPT, key: str(uuid4())}
    with pytest.raises(AgentRuntimeError):
        validate_committed_batch(
            ProducerBatch.model_validate(BATCH), CommittedBatch.model_validate(data)
        )


def test_wrong_producer_key():
    data = deepcopy(RECEIPT)
    data["entries"][0]["producerKey"] = "other"
    with pytest.raises(AgentRuntimeError):
        validate_committed_batch(
            ProducerBatch.model_validate(BATCH), CommittedBatch.model_validate(data)
        )


def test_same_producer_key_with_changed_content_is_not_reused():
    changed = deepcopy(BATCH)
    changed["events"][0]["payload"]["text"] = "changed"
    with pytest.raises(AgentRuntimeError):
        validate_committed_batch(
            ProducerBatch.model_validate(changed), CommittedBatch.model_validate(RECEIPT)
        )


@pytest.mark.parametrize("patch", [{"lastSessionSeq": "2"}, {"lastSessionSeq": None}])
def test_flush_cannot_claim_uncommitted_output(patch):
    data = deepcopy(PROOF)
    data["channels"][0].update(patch)
    with pytest.raises(ValidationError):
        FlushProof.model_validate(data)


def test_flush_requires_both_channels_and_correct_empty_digest():
    for channels in (
        PROOF["channels"][:1],
        [PROOF["channels"][0]] * 2,
        [PROOF["channels"][0], {**PROOF["channels"][1], "digest": "a" * 64}],
    ):
        with pytest.raises(ValidationError):
            FlushProof.model_validate({**PROOF, "channels": channels})

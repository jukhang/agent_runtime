import hashlib
from copy import deepcopy
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.contracts.transitions import (
    PauseCommitRequest,
    StateWriteGuard,
    TerminalCommitRequest,
    control_outcome_adapter,
)

ID = str(uuid4())
GUARD = {
    "actor": {"scopeId": "scope", "actorId": "user", "authRevision": "1", "grantsRef": "grant"},
    "lease": {
        "scopeId": "scope",
        "sessionId": ID,
        "runId": ID,
        "workId": ID,
        "ownerId": "w",
        "runEpoch": "2",
        "workEpoch": "3",
        "slotVersion": "1",
        "runLeaseUntil": "2026-09-14T11:00:00Z",
        "workLeaseUntil": "2026-09-14T11:00:00Z",
    },
    "expectedStateVersion": "2",
    "status": "running",
    "desiredControl": None,
    "controlCommandId": None,
    "cancelVersion": "0",
}
RESULT = {
    "status": "completed",
    "runId": ID,
    "outcome": "success",
    "failure": None,
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
BASE = {
    "guard": GUARD,
    "checkpointId": ID,
    "evidence": {"kind": "no_producers", "runId": ID, "enumeratedSessionSeq": "1"},
    "executions": [],
}
EXEC = {
    "executionRef": {
        "resourceType": "execution",
        "resourceId": ID,
        "resourceVersion": "1",
        "scope": {"scopeId": "scope"},
    },
    "generation": "1",
    "state": "stopped",
    "evidenceRef": "supervisor-observation",
    "supervisionWorkId": None,
}


def test_normal_completion_roundtrip():
    request = TerminalCommitRequest.model_validate({**BASE, "result": RESULT})
    assert (
        TerminalCommitRequest.model_validate_json(request.model_dump_json(by_alias=True)) == request
    )


@pytest.mark.parametrize(
    "patch",
    [
        {"desiredControl": "pause", "controlCommandId": ID},
        {"status": "cancelling", "desiredControl": "cancel", "controlCommandId": ID},
        {"status": "paused"},
        {"status": "queued"},
    ],
)
def test_completion_cannot_override_control_or_inactive_run(patch):
    with pytest.raises(ValidationError):
        TerminalCommitRequest.model_validate(
            {**BASE, "guard": {**GUARD, **patch}, "result": RESULT}
        )


@pytest.mark.parametrize(
    "patch",
    [
        {"desiredControl": "pause"},
        {"controlCommandId": ID},
        {"status": "cancelling"},
        {"desiredControl": "cancel", "controlCommandId": ID},
        {"status": "completed"},
        {"status": "failed"},
        {"status": "cancelled"},
    ],
)
def test_invalid_guard(patch):
    with pytest.raises(ValidationError):
        StateWriteGuard.model_validate({**GUARD, **patch})


@pytest.mark.parametrize("state", ["stopped", "isolated"])
def test_pause_accepts_only_confirmed_safe_execution(state):
    data = {
        **BASE,
        "guard": {**GUARD, "desiredControl": "pause", "controlCommandId": ID},
        "executions": [{**EXEC, "state": state, "supervisionWorkId": ID}],
    }
    assert PauseCommitRequest.model_validate(data)
    data["executions"][0]["state"] = "unknown"
    with pytest.raises(ValidationError):
        PauseCommitRequest.model_validate(data)


def test_cancel_requires_stopped_not_isolated():
    data = {
        **BASE,
        "guard": {
            **GUARD,
            "status": "cancelling",
            "desiredControl": "cancel",
            "controlCommandId": ID,
        },
        "result": {**RESULT, "status": "cancelled", "outcome": None},
    }
    assert TerminalCommitRequest.model_validate({**data, "executions": [EXEC]})
    for state in ("isolated", "unknown"):
        with pytest.raises(ValidationError):
            TerminalCommitRequest.model_validate(
                {**data, "executions": [{**EXEC, "state": state, "supervisionWorkId": ID}]}
            )


def test_failure_keeps_unknown_execution_under_supervision():
    result = {
        **RESULT,
        "status": "failed",
        "outcome": None,
        "failure": {
            "code": "SIDE_EFFECT_UNKNOWN",
            "category": "tool",
            "retryable": False,
            "publicMessage": "Execution state must be reconciled",
        },
    }
    data = {
        **BASE,
        "result": result,
        "executions": [{**EXEC, "state": "unknown", "supervisionWorkId": ID}],
    }
    assert TerminalCommitRequest.model_validate(data)
    data["executions"][0]["supervisionWorkId"] = None
    with pytest.raises(ValidationError):
        TerminalCommitRequest.model_validate(data)


def proof():
    return {
        "proofId": ID,
        "scopeId": "scope",
        "sessionId": ID,
        "runId": ID,
        "attemptId": ID,
        "itemId": ID,
        "durableSeq": "0",
        "channels": [
            {
                "channel": channel,
                "lastDeltaIndex": None,
                "lastSessionSeq": None,
                "digest": hashlib.sha256(b"").hexdigest(),
            }
            for channel in ("content", "analysis")
        ],
    }


@pytest.mark.parametrize("key", ["scopeId", "sessionId", "runId"])
def test_flush_proof_must_belong_to_run(key):
    data = {
        **BASE,
        "result": RESULT,
        "evidence": {
            "kind": "verified",
            "proofs": [{**proof(), key: str(uuid4())}],
            "enumeratedSessionSeq": "1",
        },
    }
    with pytest.raises(ValidationError):
        TerminalCommitRequest.model_validate(data)


def test_duplicate_proofs_and_wrong_result_rejected():
    for patch in (
        {
            "evidence": {
                "kind": "verified",
                "proofs": [proof(), proof()],
                "enumeratedSessionSeq": "1",
            }
        },
        {"result": {**RESULT, "runId": str(uuid4())}},
        {"executions": [EXEC, EXEC]},
    ):
        with pytest.raises(ValidationError):
            TerminalCommitRequest.model_validate({**BASE, "result": RESULT, **patch})


def test_cross_scope_execution_rejected():
    execution = deepcopy(EXEC)
    execution["executionRef"]["scope"]["scopeId"] = "other"
    with pytest.raises(ValidationError):
        TerminalCommitRequest.model_validate({**BASE, "result": RESULT, "executions": [execution]})


def test_flush_evidence_union_requires_declared_producer_state():
    # A verified batch cannot be empty, and a no-producer confirmation must be this run.
    for evidence in (
        {"kind": "verified", "proofs": [], "enumeratedSessionSeq": "1"},
        {"kind": "verified", "proofs": [proof()]},  # missing enumeration watermark
        {"kind": "no_producers", "runId": str(uuid4()), "enumeratedSessionSeq": "1"},
    ):
        with pytest.raises(ValidationError):
            TerminalCommitRequest.model_validate({**BASE, "result": RESULT, "evidence": evidence})
    TerminalCommitRequest.model_validate({**BASE, "result": RESULT})


def durable_proof():
    """A single content producer flushed at session sequence 5."""
    flushed = proof()
    flushed["durableSeq"] = "5"
    flushed["channels"] = [
        {
            "channel": "content",
            "lastDeltaIndex": "3",
            "lastSessionSeq": "5",
            "digest": "a" * 64,
        },
        {
            "channel": "analysis",
            "lastDeltaIndex": None,
            "lastSessionSeq": None,
            "digest": hashlib.sha256(b"").hexdigest(),
        },
    ]
    return flushed


def test_enumerated_watermark_cannot_predate_the_evidence_it_reports():
    for watermark in ("0", "4"):
        with pytest.raises(ValidationError):
            TerminalCommitRequest.model_validate(
                {
                    **BASE,
                    "result": RESULT,
                    "evidence": {
                        "kind": "verified",
                        "proofs": [durable_proof()],
                        "enumeratedSessionSeq": watermark,
                    },
                }
            )
    TerminalCommitRequest.model_validate(
        {
            **BASE,
            "result": RESULT,
            "evidence": {
                "kind": "verified",
                "proofs": [durable_proof()],
                "enumeratedSessionSeq": "5",
            },
        }
    )
    with pytest.raises(ValidationError):
        TerminalCommitRequest.model_validate(
            {
                **BASE,
                "result": RESULT,
                "evidence": {"kind": "no_producers", "runId": ID, "enumeratedSessionSeq": "0"},
            }
        )


def control(**changes):
    payload = {
        "outcome": "applied",
        "commandId": str(uuid4()),
        "runId": ID,
        "kind": "pause",
        "status": "paused",
        "stateVersion": "3",
        "desiredControl": None,
        "workId": None,
    }
    return {**payload, **changes}


@pytest.mark.parametrize(
    ("kind", "status", "desired_control", "work"),
    [
        ("pause", "paused", None, None),
        ("resume", "queued", None, "work"),
        ("resume", "waiting_tool", None, "work"),
        ("resume", "waiting_user", None, "work"),
        ("cancel", "cancelling", "cancel", None),
        ("cancel", "cancelled", None, None),
    ],
)
def test_control_applied_accepts_consistent_outcomes(kind, status, desired_control, work):
    control_outcome_adapter.validate_python(
        control(
            kind=kind,
            status=status,
            desiredControl=desired_control,
            workId=str(uuid4()) if work else None,
        )
    )


@pytest.mark.parametrize(
    "patch",
    [
        {"kind": "pause", "status": "running"},  # pause must land on paused
        {"kind": "resume", "status": "queued", "workId": None},  # resume needs its work
        {"kind": "resume", "status": "queued", "desiredControl": "pause", "workId": str(uuid4())},
        {"kind": "cancel", "status": "paused", "desiredControl": "cancel"},
        {"kind": "pause", "status": "cancelled"},
        {"kind": "cancel", "status": "cancelled", "desiredControl": "pause"},
    ],
)
def test_control_applied_rejects_inconsistent_outcomes(patch):
    with pytest.raises(ValidationError):
        control_outcome_adapter.validate_python(control(**patch))


@pytest.mark.parametrize(
    "patch",
    [
        {"kind": "pause", "status": "paused", "workId": str(uuid4())},
        {"kind": "cancel", "status": "cancelling", "desiredControl": "cancel", "workId": ID},
        {"kind": "cancel", "status": "cancelled", "workId": str(uuid4())},
    ],
)
def test_only_resume_control_may_claim_new_work(patch):
    with pytest.raises(ValidationError):
        control_outcome_adapter.validate_python(control(**patch))


def test_control_rejected_carries_a_structured_failure():
    control_outcome_adapter.validate_python(
        {
            "outcome": "rejected",
            "commandId": str(uuid4()),
            "runId": ID,
            "failure": {
                "code": "CONTROL_PENDING",
                "category": "concurrency",
                "retryable": False,
                "publicMessage": "A newer control command already won.",
            },
        }
    )

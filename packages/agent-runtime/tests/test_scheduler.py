from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.contracts.scheduler import (
    ClaimRequest,
    InputWatermark,
    WorkerCapabilities,
    claim_adapter,
    renew_adapter,
    wake_adapter,
    yield_adapter,
)

ID = str(uuid4())
LEASE = {
    "scopeId": "scope",
    "sessionId": ID,
    "runId": ID,
    "workId": ID,
    "ownerId": "worker",
    "runEpoch": "2",
    "workEpoch": "5",
    "slotVersion": "1",
    "runLeaseUntil": "2026-09-14T11:00:00Z",
    "workLeaseUntil": "2026-09-14T11:00:00Z",
}


RUNTIME_REF = {"key": {"namespace": "builtin", "key": "runtime"}, "revision": "1"}
CAPABILITY_A = {"key": {"namespace": "builtin", "key": "a"}, "revision": "1"}


def test_claim_preserves_independent_epochs():
    result = claim_adapter.validate_python(
        {
            "disposition": "claimed",
            "lease": LEASE,
            "stateVersion": "1",
            "checkpointId": None,
            "budgetAllocationId": ID,
        }
    )
    assert result.lease.run_epoch != result.lease.work_epoch
    assert claim_adapter.validate_json(claim_adapter.dump_json(result)) == result


@pytest.mark.parametrize("reason", ["no_work", "capacity", "capability_mismatch"])
def test_idle_is_not_failed_run(reason):
    result = claim_adapter.validate_python(
        {
            "disposition": "idle",
            "reason": reason,
            "retryAfterMs": 1000,
        }
    )
    assert result.disposition == "idle"


@pytest.mark.parametrize("seconds", [0, True, 3601])
def test_invalid_lease_duration(seconds):
    with pytest.raises(ValidationError):
        ClaimRequest.model_validate(
            {
                "scopeId": "scope",
                "worker": {
                    "workerId": "w",
                    "runtimeRevision": RUNTIME_REF,
                    "capabilities": [],
                },
                "leaseSeconds": seconds,
            }
        )


def test_worker_duplicate_capabilities():
    with pytest.raises(ValidationError):
        WorkerCapabilities.model_validate(
            {
                "workerId": "w",
                "runtimeRevision": RUNTIME_REF,
                "capabilities": [CAPABILITY_A] * 2,
            }
        )


def test_renew_result_discriminator():
    assert renew_adapter.validate_python({"disposition": "renewed", "lease": LEASE}).lease
    assert (
        renew_adapter.validate_python(
            {
                "disposition": "lost",
                "failure": {
                    "code": "FENCED",
                    "category": "concurrency",
                    "retryable": False,
                    "publicMessage": "Ownership lost",
                },
            }
        ).disposition
        == "lost"
    )
    with pytest.raises(ValidationError):
        renew_adapter.validate_python({"disposition": "lost", "lease": LEASE})


def test_waiting_cannot_drop_unhandled_input():
    base = {
        "runId": ID,
        "stateVersion": "2",
        "checkpointId": ID,
        "inputs": {"pendingVersion": "2", "appliedVersion": "1"},
    }
    with pytest.raises(ValidationError):
        yield_adapter.validate_python({**base, "disposition": "waiting", "status": "waiting_tool"})
    assert yield_adapter.validate_python({**base, "disposition": "requeued", "workId": ID})
    base["inputs"]["appliedVersion"] = "2"
    assert yield_adapter.validate_python(
        {**base, "disposition": "waiting", "status": "waiting_user"}
    )
    with pytest.raises(ValidationError):
        yield_adapter.validate_python({**base, "disposition": "requeued", "workId": ID})


def test_watermark_cannot_advance_beyond_pending():
    with pytest.raises(ValidationError):
        InputWatermark(pending_version="1", applied_version="2")


@pytest.mark.parametrize("kind", ["scheduled", "deferred", "duplicate", "terminal"])
def test_wake_results_roundtrip(kind):
    raw = {
        "runId": ID,
        "inputs": {"pendingVersion": "2", "appliedVersion": "1"},
        "disposition": kind,
    }
    if kind == "scheduled":
        raw["workId"] = ID
    if kind == "terminal":
        raw["status"] = "completed"
    result = wake_adapter.validate_python(raw)
    assert wake_adapter.validate_json(wake_adapter.dump_json(result)) == result


def test_terminal_wake_does_not_schedule_work():
    with pytest.raises(ValidationError):
        wake_adapter.validate_python(
            {
                "disposition": "terminal",
                "runId": ID,
                "inputs": {"pendingVersion": "1", "appliedVersion": "1"},
                "status": "completed",
                "workId": ID,
            }
        )


@pytest.mark.parametrize("kind", ["scheduled", "deferred"])
def test_new_wakeup_requires_pending_input(kind):
    data = {
        "disposition": kind,
        "runId": ID,
        "inputs": {"pendingVersion": "1", "appliedVersion": "1"},
    }
    if kind == "scheduled":
        data["workId"] = ID
    with pytest.raises(ValidationError):
        wake_adapter.validate_python(data)

from copy import deepcopy
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.contracts.ownership import (
    ChildCommitGuard,
    ChildLeaseToken,
    ExecutionObservation,
    ExecutionReconcileRequest,
    RecoveryRequest,
    StaleRunRef,
    recovery_adapter,
)
from agent_runtime.contracts.transactions import LeaseToken

ID = str(uuid4())
OTHER = str(uuid4())
ACTOR = {"scopeId": "scope", "actorId": "user", "authRevision": "1", "grantsRef": "grant"}
MAINTENANCE = {
    "scopeId": "scope",
    "workId": ID,
    "ownerId": "worker",
    "workEpoch": "3",
    "workLeaseUntil": "2026-09-14T11:00:00Z",
}
CHILD = {
    **MAINTENANCE,
    "sessionId": ID,
    "runId": ID,
    "runEpoch": "2",
    "runLeaseUntil": "2026-09-14T11:00:00Z",
}
WORKSPACE = {
    "resourceType": "workspace",
    "resourceId": ID,
    "resourceVersion": "1",
    "scope": {"scopeId": "scope"},
}
EXECUTION = {**WORKSPACE, "resourceType": "execution"}
GUARD = {
    "actor": ACTOR,
    "lease": CHILD,
    "expectedStateVersion": "1",
    "rootRunId": OTHER,
    "expectedRootCancelVersion": "1",
    "workspaceRef": WORKSPACE,
    "workspaceOwnerRunId": ID,
    "workspaceGeneration": "1",
}
STALE = {
    "scopeId": "scope",
    "sessionId": ID,
    "runId": ID,
    "expectedStateVersion": "1",
    "expectedOwnerEpoch": "2",
    "ownerId": None,
    "leaseUntil": None,
}


def test_child_cannot_impersonate_main_slot():
    assert ChildLeaseToken.model_validate(CHILD).run_epoch == "2"
    with pytest.raises(ValidationError):
        ChildLeaseToken.model_validate({**CHILD, "slotVersion": "1"})
    with pytest.raises(ValidationError):
        LeaseToken.model_validate(CHILD)


def test_child_guard_roundtrip():
    guard = ChildCommitGuard.model_validate(GUARD)
    assert ChildCommitGuard.model_validate_json(guard.model_dump_json(by_alias=True)) == guard


@pytest.mark.parametrize(
    "patch",
    [
        {"rootRunId": ID},
        {"workspaceOwnerRunId": OTHER},
        {"workspaceRef": {**WORKSPACE, "scope": {"scopeId": "other"}}},
        {"actor": {**ACTOR, "scopeId": "other"}},
        # F-B5: the child must submit the revision it validated, not a floating reference.
        {"workspaceRef": {**WORKSPACE, "resourceVersion": None}},
        {"workspaceGeneration": "0"},
    ],
)
def test_child_workspace_and_scope_boundaries(patch):
    with pytest.raises(ValidationError):
        ChildCommitGuard.model_validate({**GUARD, **patch})


def test_root_cancel_version_required():
    data = deepcopy(GUARD)
    del data["expectedRootCancelVersion"]
    with pytest.raises(ValidationError):
        ChildCommitGuard.model_validate(data)


def test_supervisor_uses_work_lease_only():
    request = {"actor": ACTOR, "lease": MAINTENANCE, "executionRef": EXECUTION, "generation": "2"}
    assert ExecutionReconcileRequest.model_validate(request)
    with pytest.raises(ValidationError):
        ExecutionReconcileRequest.model_validate({**request, "lease": CHILD})
    with pytest.raises(ValidationError):
        ExecutionReconcileRequest.model_validate({**request, "executionRef": WORKSPACE})


def test_observation_is_not_run_finalization():
    observation = {
        "executionRef": EXECUTION,
        "generation": "1",
        "observedAt": "2026-09-14T10:00:00Z",
        "state": "completed",
        "evidenceRef": "evidence",
        "failure": None,
    }
    assert ExecutionObservation.model_validate(observation)
    for patch in ({"runStatus": "completed"}, {"state": "failed"}):
        with pytest.raises(ValidationError):
            ExecutionObservation.model_validate({**observation, **patch})


def test_recovery_checks_scope_and_observed_lease_pair():
    assert RecoveryRequest.model_validate({"actor": ACTOR, "lease": MAINTENANCE, "target": STALE})
    with pytest.raises(ValidationError):
        RecoveryRequest.model_validate(
            {"actor": ACTOR, "lease": MAINTENANCE, "target": {**STALE, "scopeId": "other"}}
        )
    with pytest.raises(ValidationError):
        StaleRunRef.model_validate({**STALE, "ownerId": "worker"})


@pytest.mark.parametrize("status,reason", [("queued", "recovery"), ("cancelling", "control")])
def test_recovery_schedule_preserves_control(status, reason):
    data = {
        "disposition": "scheduled",
        "runId": ID,
        "workId": OTHER,
        "stateVersion": "3",
        "status": status,
        "wakeReason": reason,
    }
    assert recovery_adapter.validate_python(data)
    with pytest.raises(ValidationError):
        recovery_adapter.validate_python(
            {**data, "wakeReason": "control" if reason == "recovery" else "recovery"}
        )


@pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
def test_terminal_recovery_cannot_schedule(status):
    data = {"disposition": "terminal", "runId": ID, "stateVersion": "3", "status": status}
    assert recovery_adapter.validate_python(data)
    with pytest.raises(ValidationError):
        recovery_adapter.validate_python({**data, "workId": OTHER})


def test_paused_recovery_only_defers():
    assert recovery_adapter.validate_python(
        {"disposition": "deferred", "runId": ID, "reason": "paused", "recheckAt": None}
    )
    with pytest.raises(ValidationError):
        recovery_adapter.validate_python(
            {
                "disposition": "scheduled",
                "runId": ID,
                "workId": OTHER,
                "stateVersion": "3",
                "status": "paused",
                "wakeReason": "recovery",
            }
        )

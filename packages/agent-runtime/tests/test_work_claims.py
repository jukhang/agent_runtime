from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_ownership import CHILD, EXECUTION, ID, MAINTENANCE, OTHER, WORKSPACE

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.work_claims import (
    ChildRenewRequest,
    MaintenanceRenewRequest,
    WorkClaimRequest,
    child_renew_adapter,
    maintenance_renew_adapter,
    work_claim_adapter,
)
from agent_runtime.work_claim_validation import (
    validate_child_renew,
    validate_maintenance_renew,
    validate_work_claim,
)

REQUEST = {
    "scopeId": "scope",
    "worker": {
        "workerId": "worker",
        "runtimeRevision": {"key": {"namespace": "builtin", "key": "runtime"}, "revision": "1"},
        "capabilities": [],
    },
    "leaseSeconds": 60,
    "lanes": ["child"],
}
CHILD_RESULT = {
    "disposition": "child_claimed",
    "lease": CHILD,
    "rootRunId": OTHER,
    "rootCancelVersion": "1",
    "stateVersion": "1",
    "checkpointId": None,
    "budgetAllocationId": ID,
    "workspaceRef": WORKSPACE,
    "workspaceGeneration": "1",
}


def test_child_and_maintenance_claims_have_no_main_slot():
    result = work_claim_adapter.validate_python(CHILD_RESULT)
    validate_work_claim(WorkClaimRequest.model_validate(REQUEST), result)
    with pytest.raises(ValidationError):
        work_claim_adapter.validate_python({**CHILD_RESULT, "lease": {**CHILD, "slotVersion": "1"}})
    maintenance = {
        "disposition": "maintenance_claimed",
        "lease": MAINTENANCE,
        "kind": "reconcile_execution",
        "subjectRef": EXECUTION,
        "payloadRef": "payload",
    }
    req = WorkClaimRequest.model_validate(
        {**REQUEST, "lanes": ["maintenance"], "maintenanceKinds": ["reconcile_execution"]}
    )
    validate_work_claim(req, work_claim_adapter.validate_python(maintenance))
    with pytest.raises(AgentRuntimeError):
        validate_work_claim(
            WorkClaimRequest.model_validate(REQUEST),
            work_claim_adapter.validate_python(maintenance),
        )


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        # A lease scope that contradicts the child workspace is rejected by the model itself.
        ("scopeId", "other", ValidationError),
        # A different worker passes model validation and must be rejected by the claim check.
        ("ownerId", "another-worker", AgentRuntimeError),
    ],
)
def test_claim_cannot_change_worker_or_scope(field, value, error):
    with pytest.raises(error):
        validate_work_claim(
            WorkClaimRequest.model_validate(REQUEST),
            work_claim_adapter.validate_python({**CHILD_RESULT, "lease": {**CHILD, field: value}}),
        )


@pytest.mark.parametrize(
    "field,value",
    [("runEpoch", "9"), ("workEpoch", "8"), ("runId", str(uuid4())), ("ownerId", "other")],
)
def test_child_renew_cannot_change_execution_identity(field, value):
    req = ChildRenewRequest.model_validate({"lease": CHILD, "leaseSeconds": 60})
    with pytest.raises(AgentRuntimeError):
        validate_child_renew(
            req,
            child_renew_adapter.validate_python(
                {"disposition": "renewed", "lease": {**CHILD, field: value}}
            ),
        )
    validate_child_renew(
        req,
        child_renew_adapter.validate_python(
            {
                "disposition": "renewed",
                "lease": {
                    **CHILD,
                    "runLeaseUntil": "2026-09-14T11:01:00Z",
                    "workLeaseUntil": "2026-09-14T11:01:00Z",
                },
            }
        ),
    )


def test_maintenance_renew_cannot_change_epoch_or_shorten_lease():
    req = MaintenanceRenewRequest.model_validate({"lease": MAINTENANCE, "leaseSeconds": 60})
    for patch in [{"workEpoch": "8"}, {"workLeaseUntil": "2026-09-14T10:59:00Z"}]:
        with pytest.raises(AgentRuntimeError):
            validate_maintenance_renew(
                req,
                maintenance_renew_adapter.validate_python(
                    {"disposition": "renewed", "lease": {**MAINTENANCE, **patch}}
                ),
            )


def test_control_claim_does_not_need_model_budget_and_cannot_restart_cancelled_work():
    data = {
        "disposition": "control_claimed",
        "lease": CHILD,
        "stateVersion": "1",
        "commandId": ID,
        "desiredControl": "cancel",
        "status": "cancelling",
        "cancelVersion": "1",
        "checkpointId": None,
    }
    work_claim_adapter.validate_python(data)
    with pytest.raises(ValidationError):
        work_claim_adapter.validate_python({**data, "status": "running"})
    with pytest.raises(ValidationError):
        WorkClaimRequest.model_validate({**REQUEST, "lanes": ["maintenance"]})

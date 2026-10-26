from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_credential_validation import authorized_request
from test_foundation_boundaries import ACTOR, CHECKPOINT, ID, INVOCATION, REF, VERSION

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.mailbox import ClaimedBatch, MailboxReceipt
from agent_runtime.contracts.ownership import (
    ExecutionCancelRequest,
    ExecutionCancelResult,
    RecoveryDeferred,
    RecoveryRequest,
    RecoveryScheduled,
    RecoveryTerminal,
)
from agent_runtime.contracts.policy import PolicyRequest, policy_adapter
from agent_runtime.contracts.subagents import ChildRunRef, VerifiedResult
from agent_runtime.contracts.transitions import ResumedRun
from agent_runtime.contracts.workspace import RestoredWorkspaceRef, WorkspaceRestoreRequest
from agent_runtime.policy_validation import policy_input_digest, validate_policy_decision
from agent_runtime.service_validation import (
    validate_cancel_result,
    validate_grant_binding,
    validate_recovery_result,
    validate_workspace_restore,
)
from agent_runtime.tool_validation import arguments_digest


def test_resume_must_clear_control_fields():
    payload = {
        "scopeId": "scope",
        "sessionId": ID,
        "runId": ID,
        "commandId": ID,
        "workId": ID,
        "stateVersion": "2",
        "status": "queued",
        "desiredControl": None,
        "controlCommandId": None,
        "wakeReason": "resume",
    }
    ResumedRun.model_validate(payload)
    for key, value in [
        ("desiredControl", "pause"),
        ("controlCommandId", ID),
        ("status", "running"),
    ]:
        with pytest.raises(ValidationError):
            ResumedRun.model_validate({**payload, key: value})


def test_stop_ack_is_not_stopped_process():
    observation = {
        "executionRef": {
            "resourceType": "execution",
            "resourceId": ID,
            "scope": {"scopeId": "scope"},
        },
        "generation": "1",
        "observedAt": "2026-09-14T00:00:00Z",
        "state": "running",
        "evidenceRef": "evidence",
        "failure": None,
    }
    payload = {
        "commandId": ID,
        "observation": observation,
        "disposition": "requested",
        "supervisionWorkId": ID,
    }
    ExecutionCancelResult.model_validate(payload)
    with pytest.raises(ValidationError):
        ExecutionCancelResult.model_validate({**payload, "disposition": "stopped"})
    with pytest.raises(ValidationError):
        ExecutionCancelResult.model_validate({**payload, "supervisionWorkId": None})


def test_child_evidence_cannot_cross_scope():
    child = {
        "scopeId": "scope",
        "sessionId": ID,
        "parentRunId": ID,
        "childRunId": str(uuid4()),
        "taskId": ID,
        "agent": REF,
        "budgetAllocationId": ID,
        "workspaceId": ID,
    }
    ChildRunRef.model_validate(child)
    with pytest.raises(ValidationError):
        ChildRunRef.model_validate({**child, "childRunId": ID})
    data = {
        "child": child,
        "proposalId": ID,
        "proposalRevision": "1",
        "acceptanceSpec": REF,
        "disposition": "accepted",
        "resultRefs": [],
        "publicReason": "verified",
        "evidenceRefs": [
            {
                "resourceType": "evidence",
                "resourceId": ID,
                "resourceVersion": "1",
                "scope": {"scopeId": "other"},
            }
        ],
    }
    with pytest.raises(ValidationError):
        VerifiedResult.model_validate(data)
    data["evidenceRefs"][0]["scope"]["scopeId"] = "scope"
    VerifiedResult.model_validate(data)


def test_empty_mailbox_claim_does_not_allocate_fake_input():
    ClaimedBatch(scope_id="scope", recipient_run_id=ID, previous_applied_seq="0", entries=())
    with pytest.raises(ValidationError):
        MailboxReceipt(
            scope_id="scope",
            message_id=ID,
            recipient_run_id=ID,
            recipient_seq="0",
            disposition="accepted",
        )


def test_policy_approval_cannot_substitute_another_operation():
    request = PolicyRequest.model_validate(
        {"invocation": INVOCATION, "policy": REF, "resources": []}
    )
    data = {
        "decisionId": ID,
        "policy": REF,
        "inputDigest": policy_input_digest(request),
        "evidenceRefs": [
            {"resourceType": "evidence", "resourceId": ID, "scope": {"scopeId": "scope"}}
        ],
        "evaluatedAt": "2026-09-14T00:00:00Z",
        "expiresAt": "2026-09-15T00:00:00Z",
        "decision": "ask",
        "approval": {
            "approvalId": ID,
            "idempotencyKey": "approval",
            "publicSummary": "approve",
            "expiresAt": "2026-09-15T00:00:00Z",
            "binding": {
                "scopeId": "scope",
                "actorId": ACTOR["actorId"],
                "sessionId": ID,
                "tool": REF,
                "operationId": ID,
                "argumentsDigest": arguments_digest({"x": 1, "y": 2}),
                "policy": REF,
                "authRevision": "1",
                "resources": [],
            },
        },
    }
    validate_policy_decision(
        request, policy_adapter.validate_python(data), now=datetime(2026, 9, 14, tzinfo=UTC)
    )
    changed = deepcopy(data)
    changed["approval"]["binding"]["operationId"] = str(uuid4())
    with pytest.raises(AgentRuntimeError):
        validate_policy_decision(
            request, policy_adapter.validate_python(changed), now=datetime(2026, 9, 14, tzinfo=UTC)
        )


def test_restore_receipt_cannot_reuse_old_generation_or_revision():
    request = WorkspaceRestoreRequest.model_validate(
        {
            "actor": ACTOR,
            "expected": VERSION,
            "idempotencyKey": "restore",
            "checkpoint": CHECKPOINT,
            "targetGeneration": "2",
        }
    )
    data = {
        "checkpointId": ID,
        "workspace": {**VERSION, "generation": "2", "revision": "3"},
        "verifiedManifestDigest": CHECKPOINT["manifestDigest"],
        "verificationRef": "proof",
        "status": "ready",
    }
    validate_workspace_restore(request, RestoredWorkspaceRef.model_validate(data))
    for key, value in [("generation", "1"), ("revision", "2"), ("scopeId", "other")]:
        with pytest.raises(AgentRuntimeError):
            validate_workspace_restore(
                request,
                RestoredWorkspaceRef.model_validate(
                    {
                        **data,
                        "workspace": {**data["workspace"], key: value},
                    }
                ),
            )


LEASE = {
    "scopeId": "scope",
    "workId": ID,
    "ownerId": "maintenance",
    "workEpoch": "1",
    "workLeaseUntil": "2026-09-14T11:00:00Z",
}
EXECUTION_REF = {
    "resourceType": "execution",
    "resourceId": ID,
    "resourceVersion": "1",
    "scope": {"scopeId": "scope", "sessionId": ID},
}


def test_grant_binding_matches_the_scope_and_its_lifetime():
    _, kwargs = authorized_request()
    scope, grant, now = kwargs["authorized_scope"], kwargs["current_grant"], kwargs["now"]
    validate_grant_binding(scope, grant, now=now)
    for patch in (
        {"scope_id": "other"},
        {"run_id": uuid4()},
        {"execution_id": uuid4()},
        {"execution_generation": "2"},
    ):
        with pytest.raises(AgentRuntimeError) as exc:
            validate_grant_binding(scope, grant.model_copy(update=patch), now=now)
        assert exc.value.failure.code == "FORBIDDEN"
    for patch in (
        {"issued_at": now + timedelta(seconds=1)},
        {"expires_at": now},
        {"expires_at": scope.expires_at + timedelta(seconds=1)},
    ):
        with pytest.raises(AgentRuntimeError) as exc:
            validate_grant_binding(scope, grant.model_copy(update=patch), now=now)
        assert exc.value.failure.code == "DELIVERY_EXPIRED"
    with pytest.raises(ValueError):
        validate_grant_binding(scope, grant, now=datetime(2026, 9, 14, 10))


def cancel_payloads(generation="1", command_id=None, execution_ref=None, disposition="requested"):
    command = command_id or ID
    request = ExecutionCancelRequest.model_validate(
        {
            "actor": ACTOR,
            "lease": LEASE,
            "executionRef": execution_ref or EXECUTION_REF,
            "generation": generation,
            "commandId": command,
            "rootRunId": ID,
            "expectedCancelVersion": "1",
            "deadlineAt": "2026-09-14T11:00:00Z",
        }
    )
    result = ExecutionCancelResult.model_validate(
        {
            "commandId": command,
            "observation": {
                "executionRef": execution_ref or EXECUTION_REF,
                "generation": generation,
                "observedAt": "2026-09-14T10:00:00Z",
                "state": "running",
                "evidenceRef": "evidence",
                "failure": None,
            },
            "disposition": disposition,
            "supervisionWorkId": ID,
        }
    )
    return request, result


def test_cancel_result_must_match_its_command_and_execution():
    request, result = cancel_payloads()
    validate_cancel_result(request, result)
    other_ref = {**EXECUTION_REF, "resourceId": str(uuid4())}
    for changed in (
        cancel_payloads(command_id=str(uuid4()))[1],
        cancel_payloads(execution_ref=other_ref)[1],
        cancel_payloads(generation="2", execution_ref=other_ref)[1],
    ):
        with pytest.raises(AgentRuntimeError) as exc:
            validate_cancel_result(request, changed)
        assert exc.value.failure.code == "IDENTITY_MISMATCH"


def recovery_payloads(state_version="4"):
    request = RecoveryRequest.model_validate(
        {
            "actor": ACTOR,
            "lease": LEASE,
            "target": {
                "scopeId": "scope",
                "sessionId": ID,
                "runId": ID,
                "expectedStateVersion": "3",
                "expectedOwnerEpoch": "2",
                "ownerId": None,
                "leaseUntil": None,
            },
        }
    )
    scheduled = RecoveryScheduled.model_validate(
        {
            "disposition": "scheduled",
            "runId": ID,
            "workId": ID,
            "stateVersion": state_version,
            "status": "queued",
            "wakeReason": "recovery",
        }
    )
    return request, scheduled


def test_recovery_result_must_advance_the_observed_run():
    request, scheduled = recovery_payloads()
    validate_recovery_result(request, scheduled)
    validate_recovery_result(
        request,
        RecoveryDeferred.model_validate(
            {"disposition": "deferred", "runId": ID, "reason": "paused", "recheckAt": None}
        ),
    )
    validate_recovery_result(
        request,
        RecoveryTerminal.model_validate(
            {"disposition": "terminal", "runId": ID, "status": "cancelled", "stateVersion": "3"}
        ),
    )
    with pytest.raises(AgentRuntimeError) as mismatch:
        validate_recovery_result(
            request, scheduled.model_copy(update={"run_id": uuid4()})
        )
    assert mismatch.value.failure.code == "IDENTITY_MISMATCH"
    for stale in (recovery_payloads("3")[1], recovery_payloads("2")[1]):
        with pytest.raises(AgentRuntimeError) as conflict:
            validate_recovery_result(request, stale)
        assert conflict.value.failure.code == "VERSION_CONFLICT"
    with pytest.raises(AgentRuntimeError) as terminal_conflict:
        validate_recovery_result(
            request,
            RecoveryTerminal.model_validate(
                {"disposition": "terminal", "runId": ID, "status": "failed", "stateVersion": "2"}
            ),
        )
    assert terminal_conflict.value.failure.code == "VERSION_CONFLICT"

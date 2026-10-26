from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.admission import submission_digest
from agent_runtime.contracts.credentials import GrantRef, ScopeRequest
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.loop import AppliedInputs, BudgetCounter
from agent_runtime.contracts.policy import PolicyRequest, policy_adapter
from agent_runtime.contracts.requests import SubmitRequest
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.workspace import (
    ReadyCheckpoint,
    WorkspaceFile,
    WorkspaceRestoreRequest,
)
from agent_runtime.policy_validation import policy_input_digest, validate_policy_decision

ID = str(uuid4())
ACTOR = {"scopeId": "scope", "actorId": "actor", "authRevision": "1", "grantsRef": "grant"}
REF = {"key": {"namespace": "builtin", "key": "tool"}, "revision": "1"}
FILE = {"path": "reports/result.csv", "blobRef": "blob", "digest": "a" * 64, "sizeBytes": 4}
VERSION = {
    "scopeId": "scope",
    "workspaceId": ID,
    "ownerRunId": ID,
    "generation": "1",
    "revision": "2",
}
CHECKPOINT = {
    "checkpointId": ID,
    "workspace": VERSION,
    "manifestRef": "manifest",
    "manifestDigest": "b" * 64,
    "files": [FILE],
    "totalBytes": 4,
    "status": "ready",
}
INVOCATION = {
    "actor": ACTOR,
    "sessionId": ID,
    "runId": ID,
    "modelAttemptId": None,
    "contextSnapshotId": ID,
    "contextVersion": "1",
    "operationId": ID,
    "deadlineAt": "2026-09-15T00:00:00Z",
    "call": {"toolCallId": ID, "tool": REF, "arguments": {"x": 1, "y": 2}},
}


@pytest.mark.parametrize(
    "path",
    [
        "",
        ".",
        "../x",
        "a/../b",
        "/etc/passwd",
        "a//b",
        "a/./b",
        "a/",
        "C:/foo",
        "a\\b",
        "a\x00b",
        "a\nb",
    ],
)
def test_workspace_rejects_unsafe_paths(path):
    with pytest.raises(ValidationError):
        WorkspaceFile.model_validate({**FILE, "path": path})


def test_checkpoint_manifest_and_restore_generation():
    checkpoint = ReadyCheckpoint.model_validate(CHECKPOINT)
    assert checkpoint.total_bytes == 4
    request = {
        "actor": ACTOR,
        "expected": VERSION,
        "idempotencyKey": "restore",
        "checkpoint": CHECKPOINT,
        "targetGeneration": "2",
    }
    WorkspaceRestoreRequest.model_validate(request)
    for key, value in [("targetGeneration", "1"), ("actor", {**ACTOR, "scopeId": "other"})]:
        with pytest.raises(ValidationError):
            WorkspaceRestoreRequest.model_validate({**request, key: value})
    for files, size in [([FILE, FILE], 8), ([FILE, {**FILE, "path": "reports"}], 8), ([FILE], 5)]:
        with pytest.raises(ValidationError):
            ReadyCheckpoint.model_validate({**CHECKPOINT, "files": files, "totalBytes": size})


def test_policy_hash_changes_with_authorization_and_payload_not_dictionary_order():
    data = {"invocation": INVOCATION, "policy": REF, "resources": []}
    request = PolicyRequest.model_validate(data)
    changed = deepcopy(data)
    changed["invocation"]["call"]["arguments"] = {"y": 2, "x": 1}
    assert policy_input_digest(request) == policy_input_digest(
        PolicyRequest.model_validate(changed)
    )
    changed["invocation"]["actor"]["authRevision"] = "2"
    assert policy_input_digest(request) != policy_input_digest(
        PolicyRequest.model_validate(changed)
    )


def test_policy_decision_is_bound_and_expires():
    request = PolicyRequest.model_validate(
        {"invocation": INVOCATION, "policy": REF, "resources": []}
    )
    data = {
        "decisionId": ID,
        "policy": REF,
        "inputDigest": policy_input_digest(request),
        "evidenceRefs": [
            {"resourceType": "policy_evidence", "resourceId": ID, "scope": {"scopeId": "scope"}}
        ],
        "evaluatedAt": "2026-09-14T00:00:00Z",
        "expiresAt": "2026-09-15T00:00:00Z",
        "decision": "allow",
        "grantRef": "grant",
    }
    decision = policy_adapter.validate_python(data)
    validate_policy_decision(request, decision, now=datetime(2026, 9, 14, tzinfo=UTC))
    with pytest.raises(AgentRuntimeError):
        validate_policy_decision(request, decision, now=datetime(2026, 9, 15, tzinfo=UTC))
    with pytest.raises(AgentRuntimeError):
        validate_policy_decision(
            request,
            policy_adapter.validate_python({**data, "inputDigest": "f" * 64}),
            now=datetime(2026, 9, 14, tzinfo=UTC),
        )


def test_credential_scope_has_no_arbitrary_url_or_token():
    data = {
        "actor": ACTOR,
        "sessionId": ID,
        "runId": ID,
        "executionId": ID,
        "executionGeneration": "1",
        "operationId": ID,
        "destination": REF,
        "actions": ["read"],
        "policyDecisionId": ID,
        "inputDigest": "a" * 64,
        "expiresAt": "2026-09-15T00:00:00Z",
        "maxRequests": 1,
        "maxResponseBytes": 1000,
    }
    ScopeRequest.model_validate(data)
    for key, value in [
        ("url", "http://169.254.169.254/"),
        ("token", "secret"),
        ("actions", ["read", "read"]),
    ]:
        with pytest.raises(ValidationError):
            ScopeRequest.model_validate({**data, key: value})
    with pytest.raises(ValidationError):
        GrantRef.model_validate(
            {
                "grantId": ID,
                "scopeId": "scope",
                "runId": ID,
                "executionId": ID,
                "executionGeneration": "1",
                "revision": "1",
                "issuedAt": "2026-09-15T00:00:00Z",
                "expiresAt": "2026-09-14T00:00:00Z",
            }
        )


def test_unknown_budget_remains_reserved_and_overspend_is_reportable():
    counter = BudgetCounter(limit="10", spent="4", reserved="5", unknown_reserved="3")
    assert counter.available == 1
    assert BudgetCounter(limit="10", spent="11", reserved="0", unknown_reserved="0").available == 0
    with pytest.raises(ValidationError):
        BudgetCounter(limit="10", spent="4", reserved="2", unknown_reserved="3")


def test_applied_inputs_cannot_advance_without_consuming_messages():
    with pytest.raises(ValidationError):
        AppliedInputs(
            run_id=ID, checkpoint_ref="cp", previous_seq="0", applied_seq="1", messages=()
        )


def test_admission_digest_does_not_depend_on_renewed_grant_or_request_key():
    actor = ActorContext.model_validate(ACTOR)
    data = {
        "sessionId": ID,
        "agent": REF,
        "idempotencyKey": "first",
        "inputs": [{"kind": "text", "text": "hello"}],
    }
    request = SubmitRequest.model_validate(data)
    original = submission_digest(actor, request)
    assert (
        submission_digest(
            ActorContext.model_validate({**ACTOR, "grantsRef": "renewed"}),
            SubmitRequest.model_validate({**data, "idempotencyKey": "second"}),
        )
        == original
    )
    assert (
        submission_digest(ActorContext.model_validate({**ACTOR, "scopeId": "other"}), request)
        != original
    )
    assert (
        submission_digest(
            actor,
            SubmitRequest.model_validate({**data, "inputs": [{"kind": "text", "text": "changed"}]}),
        )
        != original
    )

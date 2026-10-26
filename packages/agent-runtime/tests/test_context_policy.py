from copy import deepcopy
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.context_validation import validate_context_binding
from agent_runtime.contracts.context import ContextSourceRecord, PreparedContext
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.contracts.policy import policy_adapter

ID = str(uuid4())
REF = {"key": {"namespace": "builtin", "key": "model"}, "revision": "1"}
RESOURCE = {
    "resourceType": "artifact",
    "resourceId": ID,
    "resourceVersion": "1",
    "scope": {"scopeId": "scope"},
}
SOURCE = {
    "sourceId": "source",
    "resource": RESOURCE,
    "required": True,
    "disposition": "included",
    "reason": None,
    "summaryRef": None,
    "summaryStatus": None,
}
CONTEXT = {
    "scopeId": "scope",
    "sessionId": ID,
    "runId": ID,
    "authRevision": "1",
    "snapshotId": ID,
    "contextVersion": "1",
    "payloadRef": "payload",
    "contentHash": "a" * 64,
    "model": REF,
    "prompt": {"key": {"namespace": "builtin", "key": "prompt"}, "revision": "1"},
    "tools": [],
    "budget": {"allocationId": ID, "maxInputTokens": 1000, "reservedOutputTokens": 500},
    "estimatedInputTokens": 100,
    "sources": [SOURCE],
}
REQUEST = {
    "scopeId": "scope",
    "sessionId": ID,
    "runId": ID,
    "attemptId": ID,
    "contextSnapshotId": ID,
    "contextVersion": "1",
    "inputRef": "payload",
    "inputDigest": "a" * 64,
    "estimatedInputTokens": 100,
    "budgetAllocationId": ID,
    "deadlineAt": "2026-09-14T11:00:00Z",
    "model": {
        "ref": REF,
        "connection": REF,
        "modelName": "model",
        "maxOutputTokens": 500,
        "thinking": "default",
        "providerOptions": {},
    },
    "tools": [],
    "outputSchema": None,
}
POLICY = {
    "decisionId": ID,
    "policy": REF,
    "inputDigest": "a" * 64,
    "evidenceRefs": [RESOURCE],
    "evaluatedAt": "2026-09-14T10:00:00Z",
    "expiresAt": "2026-09-14T11:00:00Z",
}


def test_prepared_context_roundtrip_and_binding():
    context = PreparedContext.model_validate(CONTEXT)
    assert PreparedContext.model_validate_json(context.model_dump_json(by_alias=True)) == context
    validate_context_binding(context, ProviderRequest.model_validate(REQUEST))


@pytest.mark.parametrize(
    "key,value",
    [
        ("scopeId", "other"),
        ("runId", str(uuid4())),
        ("contextSnapshotId", str(uuid4())),
        ("contextVersion", "2"),
        ("inputRef", "other"),
        ("inputDigest", "b" * 64),
        ("estimatedInputTokens", 101),
        ("budgetAllocationId", str(uuid4())),
        ("tools", [REF]),
    ],
)
def test_provider_cannot_replace_snapshot_fields(key, value):
    with pytest.raises(AgentRuntimeError):
        validate_context_binding(
            PreparedContext.model_validate(CONTEXT),
            ProviderRequest.model_validate({**REQUEST, key: value}),
        )


@pytest.mark.parametrize("disposition", ["omitted", "truncated"])
def test_required_source_cannot_be_dropped(disposition):
    with pytest.raises(ValidationError):
        ContextSourceRecord.model_validate(
            {**SOURCE, "disposition": disposition, "reason": "budget"}
        )


def test_summary_must_be_completed_and_pinned():
    source = {
        **SOURCE,
        "disposition": "summarized",
        "reason": "history",
        "summaryRef": RESOURCE,
        "summaryStatus": "completed",
    }
    assert ContextSourceRecord.model_validate(source)
    for patch in (
        {"summaryStatus": "failed"},
        {"summaryStatus": None},
        {"summaryRef": None},
        {"summaryRef": {**RESOURCE, "resourceVersion": None}},
    ):
        with pytest.raises(ValidationError):
            ContextSourceRecord.model_validate({**source, **patch})


def test_optional_degradation_requires_reason():
    source = {**SOURCE, "required": False, "disposition": "omitted"}
    with pytest.raises(ValidationError):
        ContextSourceRecord.model_validate(source)
    assert ContextSourceRecord.model_validate({**source, "reason": "retrieval unavailable"})


def test_snapshot_budget_and_source_uniqueness():
    for patch in (
        {"estimatedInputTokens": 1001},
        {"sources": [SOURCE, SOURCE]},
        {"tools": [REF, REF]},
    ):
        with pytest.raises(ValidationError):
            PreparedContext.model_validate({**CONTEXT, **patch})


@pytest.mark.parametrize(
    "decision,fields",
    [
        ("allow", {"grantRef": "grant"}),
        ("review", {"reviewRequestRef": "review"}),
        (
            "deny",
            {
                "failure": {
                    "code": "FORBIDDEN",
                    "category": "authorization",
                    "retryable": False,
                    "publicMessage": "Not allowed",
                }
            },
        ),
    ],
)
def test_policy_union_roundtrip(decision, fields):
    model = policy_adapter.validate_python({**POLICY, "decision": decision, **fields})
    assert policy_adapter.validate_json(policy_adapter.dump_json(model)) == model


def test_review_is_not_a_grant():
    with pytest.raises(ValidationError):
        policy_adapter.validate_python(
            {**POLICY, "decision": "review", "reviewRequestRef": "review", "grantRef": "grant"}
        )


def test_policy_evidence_and_expiry_required():
    for patch in ({"evidenceRefs": []}, {"expiresAt": POLICY["evaluatedAt"]}):
        with pytest.raises(ValidationError):
            policy_adapter.validate_python(
                {**POLICY, "decision": "allow", "grantRef": "grant", **patch}
            )


def test_ask_binds_policy_revision_and_lifetime():
    binding = {
        "scopeId": "scope",
        "actorId": "user",
        "sessionId": ID,
        "tool": REF,
        "operationId": ID,
        "argumentsDigest": "a" * 64,
        "policy": REF,
        "authRevision": "1",
        "resources": [RESOURCE],
    }
    approval = {
        "approvalId": ID,
        "binding": binding,
        "idempotencyKey": "request",
        "publicSummary": "Approve?",
        "expiresAt": POLICY["expiresAt"],
    }
    assert policy_adapter.validate_python({**POLICY, "decision": "ask", "approval": approval})
    changed = deepcopy(approval)
    changed["binding"]["policy"]["revision"] = "2"
    with pytest.raises(ValidationError):
        policy_adapter.validate_python({**POLICY, "decision": "ask", "approval": changed})
    with pytest.raises(ValidationError):
        policy_adapter.validate_python(
            {
                **POLICY,
                "decision": "ask",
                "approval": {**approval, "expiresAt": POLICY["evaluatedAt"]},
            }
        )

import json
from copy import deepcopy
from datetime import UTC, datetime
from importlib.resources import files
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.tools import (
    ApprovalBinding,
    ApprovalDecision,
    ApprovedGrant,
    ToolInvocation,
    ToolSpec,
    tool_outcome_adapter,
)
from agent_runtime.tool_validation import (
    arguments_digest,
    validate_approved_grant,
    validate_tool_invocation,
    validate_tool_outcome,
)

DATA = json.loads(files("agent_runtime.contracts").joinpath("fixtures/tools.json").read_text())
NOW = datetime(2026, 9, 14, 10, tzinfo=UTC)


def objects():
    return ToolSpec.model_validate(DATA["spec"]), ToolInvocation.model_validate(DATA["invocation"])


@pytest.mark.parametrize("raw", DATA["outcomes"])
def test_all_outcomes_roundtrip_and_bind(raw):
    result = tool_outcome_adapter.validate_python(raw)
    assert tool_outcome_adapter.validate_json(tool_outcome_adapter.dump_json(result)) == result
    spec, invocation = objects()
    validate_tool_outcome(spec, invocation, result)


def test_all_statuses_have_shared_fixtures():
    assert set(tool_outcome_adapter.json_schema()["discriminator"]["mapping"]) == {
        outcome["status"] for outcome in DATA["outcomes"]
    }


def test_valid_invocation_and_canonical_digest():
    validate_tool_invocation(*objects(), now=NOW)
    assert arguments_digest({}) == DATA["binding"]["argumentsDigest"]
    assert arguments_digest({"b": [2], "a": 1}) == arguments_digest({"a": 1, "b": [2]})
    assert arguments_digest({"a": True}) != arguments_digest({"a": 1})


@pytest.mark.parametrize("value", [float("nan"), float("inf"), "\ud800"])
def test_invalid_argument_values(value):
    with pytest.raises(AgentRuntimeError):
        arguments_digest({"a": value})


@pytest.mark.parametrize(
    "patch",
    [
        {"operationId": None},
        {"deadlineAt": "2026-09-14T10:00:00Z"},
        {
            "call": {
                **DATA["invocation"]["call"],
                "tool": {
                    "key": {"namespace": "builtin", "key": "other"},
                    "revision": "1",
                },
            }
        },
    ],
)
def test_invalid_invocation(patch):
    spec, _ = objects()
    invocation = ToolInvocation.model_validate({**DATA["invocation"], **patch})
    with pytest.raises(AgentRuntimeError):
        validate_tool_invocation(spec, invocation, now=NOW)


@pytest.mark.parametrize("status", ["failed", "cancelled"])
@pytest.mark.parametrize("effect", ["committed", "unknown"])
@pytest.mark.parametrize(
    "unsafe",
    [
        {"retryable": True},
        {"retryable": False, "recoveryOptions": [{"action": "retry_same", "label": "Retry"}]},
        {
            "retryable": False,
            "recoveryOptions": [{"action": "retry_with_compaction", "label": "Retry"}],
        },
    ],
)
def test_no_automatic_replay_of_uncertain_effects(status, effect, unsafe):
    data = deepcopy(DATA["outcomes"][1])
    data.update(status=status, effect=effect)
    data["failure"].update(unsafe)
    with pytest.raises(ValidationError):
        tool_outcome_adapter.validate_python(data)


@pytest.mark.parametrize(
    "patch",
    [
        {"effect": "unknown"},
        {"effect": "rolled_back"},
        {"operationId": None},
    ],
)
def test_completed_cannot_hide_unknown_or_missing_operation(patch):
    with pytest.raises(ValidationError):
        tool_outcome_adapter.validate_python({**DATA["outcomes"][0], **patch})


@pytest.mark.parametrize("policy", ["summary", "reference_only"])
def test_restricted_projection_cannot_contain_data(policy):
    raw = deepcopy(DATA["outcomes"][0])
    raw["result"] = {"policy": policy, "text": "summary", "data": {"private": "value"}}
    with pytest.raises(ValidationError):
        tool_outcome_adapter.validate_python(raw)


def test_mismatched_projection_or_identity_is_rejected():
    spec, invocation = objects()
    for patch in (
        {"runId": str(uuid4())},
        {"toolCallId": str(uuid4())},
        {"operationId": str(uuid4())},
        {"result": {"policy": "full", "data": {}}},
    ):
        result = tool_outcome_adapter.validate_python({**DATA["outcomes"][0], **patch})
        with pytest.raises(AgentRuntimeError):
            validate_tool_outcome(spec, invocation, result)


def grant():
    return ApprovedGrant.model_validate(
        {
            "approvalId": str(uuid4()),
            "revision": "1",
            "binding": DATA["binding"],
            "approvedAt": "2026-09-14T09:00:00Z",
            "expiresAt": "2026-09-14T11:00:00Z",
        }
    )


def test_approval_validity_and_current_binding():
    expected = ApprovalBinding.model_validate(DATA["binding"])
    validate_approved_grant(grant(), expected, now=NOW)
    for key, value in {
        "actorId": "other",
        "scopeId": "other",
        "authRevision": "2",
        "operationId": str(uuid4()),
        "argumentsDigest": "a" * 64,
        "tool": {"key": {"namespace": "builtin", "key": "artifact.revise"}, "revision": "2"},
    }.items():
        changed = ApprovalBinding.model_validate({**DATA["binding"], key: value})
        with pytest.raises(AgentRuntimeError):
            validate_approved_grant(grant(), changed, now=NOW)
    for hour in (8, 11):
        with pytest.raises(AgentRuntimeError):
            validate_approved_grant(grant(), expected, now=NOW.replace(hour=hour))


def test_resource_version_and_scope_required():
    ref = {
        "resourceType": "artifact",
        "resourceId": "a",
        "resourceVersion": "1",
        "scope": {"scopeId": "scope"},
    }
    ApprovalBinding.model_validate({**DATA["binding"], "resources": [ref]})
    for resources in (
        [{**ref, "resourceVersion": None}],
        [ref, ref],
        [{**ref, "scope": {"scopeId": "other"}}],
    ):
        with pytest.raises(ValidationError):
            ApprovalBinding.model_validate({**DATA["binding"], "resources": resources})


def test_approval_decision_is_not_a_grant():
    for decision in ("allow", "pending", "invalid"):
        with pytest.raises(ValidationError):
            ApprovalDecision.model_validate(
                {
                    "approvalId": str(uuid4()),
                    "expectedVersion": "1",
                    "decision": decision,
                    "idempotencyKey": "request",
                }
            )


def test_waiting_approval_must_match_call_arguments():
    spec, invocation = objects()
    invocation.call.arguments["changed"] = True
    waiting = tool_outcome_adapter.validate_python(DATA["outcomes"][4])
    with pytest.raises(AgentRuntimeError):
        validate_tool_outcome(spec, invocation, waiting)


def test_spec_rejects_false_read_only_declaration():
    for patch in ({"kind": "read"}, {"idempotency": "read_only"}, {"maxConcurrency": True}):
        with pytest.raises(ValidationError):
            ToolSpec.model_validate({**DATA["spec"], **patch})


def test_failure_cannot_point_to_another_operation():
    raw = deepcopy(DATA["outcomes"][1])
    raw["failure"]["operationId"] = str(uuid4())
    with pytest.raises(ValidationError):
        tool_outcome_adapter.validate_python(raw)


def test_unexecuted_outcome_cannot_hide_missing_write_operation():
    spec, invocation = objects()
    invocation = invocation.model_copy(update={"operation_id": None})
    raw = {**DATA["outcomes"][3], "operationId": None}
    outcome = tool_outcome_adapter.validate_python(raw)
    with pytest.raises(AgentRuntimeError):
        validate_tool_outcome(spec, invocation, outcome)


def test_pending_execution_is_scoped():
    raw = deepcopy(DATA["outcomes"][5])
    raw["executionRef"]["scope"]["scopeId"] = "other"
    with pytest.raises(AgentRuntimeError):
        validate_tool_outcome(*objects(), tool_outcome_adapter.validate_python(raw))


def test_read_only_tool_cannot_report_effects():
    spec, invocation = objects()
    spec = spec.model_copy(update={"side_effect": "none", "idempotency": "read_only"})
    with pytest.raises(AgentRuntimeError):
        validate_tool_outcome(
            spec, invocation, tool_outcome_adapter.validate_python(DATA["outcomes"][0])
        )

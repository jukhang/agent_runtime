from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.model import (
    RESERVED_OPTIONS,
    ModelConfig,
    ProviderCapabilities,
    ProviderRequest,
)
from agent_runtime.provider_preflight import validate_provider_request

REF = {"key": {"namespace": "builtin", "key": "model"}, "revision": "1"}
CONFIG = {
    "ref": REF,
    "connection": {"key": {"namespace": "builtin", "key": "private"}, "revision": "1"},
    "modelName": "model",
    "maxOutputTokens": 1000,
    "thinking": "default",
    "providerOptions": {},
}
CAPS = {
    "model": REF,
    "streaming": "supported",
    "toolCalls": "supported",
    "structuredOutput": "supported",
    "thinkingControl": "supported",
    "toolsWithStructuredOutput": "supported",
    "toolsWithThinking": "supported",
    "usage": "unknown",
    "cancellation": "unknown",
    "contextWindow": 2000,
    "maxOutputTokens": 1000,
    "allowedOptionKeys": ["chat_template_kwargs"],
}
REQUEST = {
    "scopeId": "scope",
    "sessionId": str(uuid4()),
    "runId": str(uuid4()),
    "attemptId": str(uuid4()),
    "contextSnapshotId": str(uuid4()),
    "contextVersion": "1",
    "inputRef": "immutable-context",
    "inputDigest": "a" * 64,
    "estimatedInputTokens": 1000,
    "budgetAllocationId": str(uuid4()),
    "deadlineAt": "2026-09-14T11:00:00Z",
    "model": CONFIG,
    "tools": [],
    "outputSchema": None,
}
NOW = datetime(2026, 9, 14, 10, tzinfo=UTC)


def check(request=None, caps=None, **kwargs):
    validate_provider_request(
        ProviderRequest.model_validate(request or REQUEST),
        ProviderCapabilities.model_validate(caps or CAPS),
        now=kwargs.pop("now", NOW),
        **kwargs,
    )


def test_basic_request_and_schema_roundtrip():
    check()
    request = ProviderRequest.model_validate(REQUEST)
    assert ProviderRequest.model_validate_json(request.model_dump_json(by_alias=True)) == request


@pytest.mark.parametrize(
    "field",
    [
        "contextSnapshotId",
        "contextVersion",
        "attemptId",
        "inputDigest",
        "budgetAllocationId",
        "scopeId",
        "deadlineAt",
    ],
)
def test_required_binding(field):
    data = deepcopy(REQUEST)
    del data[field]
    with pytest.raises(ValidationError):
        ProviderRequest.model_validate(data)


@pytest.mark.parametrize(
    "patch",
    [
        {"streaming": "unknown"},
        {"streaming": "unsupported"},
        {"contextWindow": None},
        {"maxOutputTokens": None},
        {"model": {"key": {"namespace": "builtin", "key": "model"}, "revision": "2"}},
        {"contextWindow": 1999},
        {"maxOutputTokens": 999},
    ],
)
def test_unverified_or_exceeded_capability(patch):
    with pytest.raises(AgentRuntimeError):
        check(caps={**CAPS, **patch})


@pytest.mark.parametrize(
    "key",
    [
        "messages",
        "tools",
        "stream",
        "api_key",
        "extra_body",
        "URL",
        "metadata",
        "max_tokens",
        "timeout",
        "extra_headers",
    ],
)
def test_runtime_field_cannot_be_overridden_or_allowlisted(key):
    with pytest.raises(ValidationError):
        ModelConfig.model_validate({**CONFIG, "providerOptions": {key: "value"}})
    with pytest.raises(ValidationError):
        ProviderCapabilities.model_validate({**CAPS, "allowedOptionKeys": [key]})


def test_every_reserved_option_is_rejected_in_any_spelling():
    for key in sorted(RESERVED_OPTIONS):
        for spelling in (key, key.upper(), key.title().replace("_", ""), key.replace("_", "-")):
            with pytest.raises(ValidationError):
                ModelConfig.model_validate({**CONFIG, "providerOptions": {spelling: "value"}})
            with pytest.raises(ValidationError):
                ProviderCapabilities.model_validate({**CAPS, "allowedOptionKeys": [spelling]})


@pytest.mark.parametrize(
    "key",
    [
        "apiBase",
        "api-base",
        "API_BASE",
        "azureEndpoint",
        "deploymentName",
        "defaultHeaders",
        "httpClient",
        "organization",
        "apiVersion",
        "apiType",
        "accessToken",
        "systemPrompt",
        "caBundle",
        "requestTimeout",
        "webhookUrl",
        "callback.url",
    ],
)
def test_sdk_alias_spellings_cannot_override_runtime_fields(key):
    with pytest.raises(ValidationError):
        ModelConfig.model_validate({**CONFIG, "providerOptions": {key: "value"}})
    with pytest.raises(ValidationError):
        ProviderCapabilities.model_validate({**CAPS, "allowedOptionKeys": [key]})


def test_private_options_require_adapter_validation():
    request = deepcopy(REQUEST)
    request["model"]["providerOptions"] = {"chat_template_kwargs": {"enable_thinking": True}}
    with pytest.raises(AgentRuntimeError):
        check(request)
    seen = []

    def validate(model, options):
        assert model.ref.key.key == "model"
        assert model.thinking == "default"
        assert type(options["chat_template_kwargs"]["enable_thinking"]) is bool
        seen.append(deepcopy(options))
        options.clear()

    check(request, validate_options=validate)
    assert seen
    assert request["model"]["providerOptions"]
    with pytest.raises(AgentRuntimeError):
        check(request, caps={**CAPS, "allowedOptionKeys": []}, validate_options=validate)


def test_option_errors_do_not_leak_values():
    request = deepcopy(REQUEST)
    request["model"]["providerOptions"] = {"chat_template_kwargs": {"bad": "private-value"}}

    def reject(ref, options):
        raise ValueError("private-value")

    with pytest.raises(AgentRuntimeError) as exc:
        check(request, validate_options=reject)
    assert "private-value" not in str(exc.value)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), "x" * 65536])
def test_bounded_finite_options(value):
    with pytest.raises(ValidationError):
        ModelConfig.model_validate({**CONFIG, "providerOptions": {"option": value}})


def test_thinking_and_tool_combinations():
    request = deepcopy(REQUEST)
    request["tools"] = [{"key": {"namespace": "builtin", "key": "search"}, "revision": "1"}]
    check(request)
    for patch in ({"toolCalls": "unknown"}, {"toolsWithThinking": "unknown"}):
        with pytest.raises(AgentRuntimeError):
            check(request, {**CAPS, **patch})
    request["model"]["thinking"] = "disabled"
    check(request, {**CAPS, "toolsWithThinking": "unsupported"})
    with pytest.raises(AgentRuntimeError):
        check(request, {**CAPS, "thinkingControl": "unsupported"})
    request["outputSchema"] = {"key": {"namespace": "builtin", "key": "answer"}, "revision": "1"}
    for field in ("structuredOutput", "toolsWithStructuredOutput"):
        with pytest.raises(AgentRuntimeError):
            check(request, {**CAPS, field: "unknown"})


def test_duplicate_tools_and_mutation_revalidation():
    request = deepcopy(REQUEST)
    request["tools"] = [{"key": {"namespace": "builtin", "key": "search"}, "revision": "1"}] * 2
    with pytest.raises(ValidationError):
        ProviderRequest.model_validate(request)
    parsed = ProviderRequest.model_validate(REQUEST)
    parsed.model.provider_options["messages"] = []
    with pytest.raises(ValidationError):
        validate_provider_request(parsed, ProviderCapabilities.model_validate(CAPS), now=NOW)


def test_deadline_is_explicit_not_wall_clock():
    with pytest.raises(AgentRuntimeError) as exc:
        check(now=datetime(2026, 9, 14, 11, tzinfo=UTC))
    assert exc.value.failure.code == "PROVIDER_TIMEOUT"
    with pytest.raises(ValueError):
        check(now=datetime(2026, 9, 14, 10))

"""vLLM error adaptation is profile-scoped and verifies the complete numeric grammar."""

import httpx
import pytest
from test_provider_openai import build_provider, collect, request_data

from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.providers.context_errors import vllm_context_error
from agent_runtime.providers.openai_compatible import OpenAIAdapterProfile


def rejected(*, maximum=262144, output=2048, prompt=260097, total=262145, value=260097):
    return {
        "type": "BadRequestError",
        "code": 400,
        "message": (
            f"This model's maximum context length is {maximum} tokens. "
            f"However, you requested {output} output tokens and your prompt contains "
            f"at least {prompt} input tokens, for a total of at least {total} tokens. "
            "Please reduce the length of the input prompt "
            "or the number of requested output tokens. "
            f"(parameter=input_tokens, value={value})"
        ),
    }


@pytest.mark.parametrize(
    "error",
    [
        None,
        [],
        {**rejected(), "type": "OtherError"},
        {**rejected(), "code": "400"},
        {**rejected(), "code": 400.0},
        {**rejected(), "message": "maximum context length exceeded"},
        {**rejected(), "message": rejected()["message"] + " extra instructions"},
        {**rejected(), "message": "prefix " + rejected()["message"]},
        {**rejected(), "message": "x" * 2049},
        rejected(total=262146),
        rejected(value=260098),
        rejected(maximum=262145),
        rejected(maximum=0),
        rejected(prompt=0),
        rejected(output=0),
        rejected(prompt=10**100),
    ],
)
def test_vllm_context_error_does_not_guess_from_incomplete_or_inconsistent_messages(error):
    assert not vllm_context_error(error)


def test_observed_vllm_context_error_is_supported():
    assert vllm_context_error(rejected())


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("status", [400, 401, 413, 422, 500])
async def test_vendor_error_requires_opted_in_profile_and_allowed_http_status(enabled, status):
    profile = OpenAIAdapterProfile.qwen() if enabled else OpenAIAdapterProfile()
    request = ProviderRequest.model_validate(request_data())
    provider, original = build_provider("", request, profile=profile)
    await original.aclose()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(status, json={"error": rejected()}))
    ) as client:
        provider._client = client
        events = await collect(provider, request)
    expected = (
        "CONTEXT_LIMIT"
        if enabled and status in {400, 413, 422}
        else "DEPENDENCY_UNAVAILABLE"
        if status == 500
        else "INVALID_INPUT"
    )
    assert events[-1].failure.code == expected
    assert all(event.kind != "usage" for event in events)

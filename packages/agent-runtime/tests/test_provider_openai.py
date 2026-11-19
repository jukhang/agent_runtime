import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest

from agent_runtime.contracts.model import ProviderCapabilities, ProviderRequest
from agent_runtime.provider import ProviderInvoker
from agent_runtime.providers.openai_compatible import (
    OpenAIAdapterProfile,
    OpenAICompatibleProvider,
    ProviderConnection,
    ResolvedProviderInput,
    ResolvedTool,
)


def request_data(*, thinking="default", tools=(), options=None):
    return {
        "scopeId": "scope",
        "sessionId": str(uuid4()),
        "runId": str(uuid4()),
        "attemptId": str(uuid4()),
        "contextSnapshotId": str(uuid4()),
        "contextVersion": "1",
        "inputRef": "input-1",
        "inputDigest": "a" * 64,
        "estimatedInputTokens": 20,
        "budgetAllocationId": str(uuid4()),
        "deadlineAt": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        "model": {
            "ref": {"key": {"namespace": "test", "key": "model"}, "revision": "1"},
            "connection": {
                "key": {"namespace": "test", "key": "connection"},
                "revision": "1",
            },
            "modelName": "qwen-test",
            "maxOutputTokens": 256,
            "thinking": thinking,
            "providerOptions": options or {},
        },
        "tools": list(tools),
        "outputSchema": None,
    }


def sse(*chunks):
    return "".join(f"data: {json.dumps(chunk)}\n\n" for chunk in chunks) + "data: [DONE]\n\n"


@pytest.mark.asyncio
async def test_bad_tool_json_is_retryable_diagnosed_and_never_completed(caplog):
    request = ProviderRequest.model_validate(request_data())
    provider, client = build_provider(
        sse(
            {
                "choices": [
                    {
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "secret-call",
                                    "function": {
                                        "name": "secret-tool",
                                        "arguments": "secret-broken-json",
                                    },
                                }
                            ]
                        },
                        "finish_reason": "tool_calls",
                    }
                ]
            }
        ),
        request,
    )
    diagnostics = []

    async def record(call, detail):
        diagnostics.append(detail)
        return "private-diagnostic"

    provider._diagnostics = record
    try:
        events = await collect(provider, request)
    finally:
        await client.aclose()
    assert not any(e.kind == "tool.complete" for e in events)
    assert events[-1].kind == "error" and events[-1].failure.retryable
    assert events[-1].failure.detail_ref == "private-diagnostic"
    assert diagnostics[0]["exception"] == "JSONDecodeError"
    assert "secret" not in json.dumps(diagnostics) and "secret" not in caplog.text


@pytest.mark.asyncio
async def test_failed_candidate_drains_usage_but_never_resumes_tool_parsing():
    request = ProviderRequest.model_validate(request_data())
    response = sse(
        {
            "choices": [
                {
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "bad",
                                "function": {"name": "tool", "arguments": "{"},
                            }
                        ]
                    },
                    "finish_reason": "tool_calls",
                }
            ]
        },
        {
            "choices": [
                {
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "good",
                                "function": {"name": "tool", "arguments": "{}"},
                            }
                        ]
                    },
                    "finish_reason": "tool_calls",
                }
            ]
        },
        {"choices": [], "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}},
    )
    provider, client = build_provider(response, request)
    try:
        events = await collect(provider, request)
    finally:
        await client.aclose()
    assert not any(e.kind in {"tool.complete", "finish"} for e in events)
    assert events[-2].kind == "usage" and events[-2].usage.total_tokens == "15"
    assert events[-1].kind == "error"


@pytest.mark.asyncio
async def test_empty_continuation_tool_identity_is_not_a_changed_name():
    request = ProviderRequest.model_validate(request_data())
    provider, client = build_provider(
        sse(
            {
                "choices": [
                    {
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call-1",
                                    "function": {"name": "read", "arguments": "{"},
                                }
                            ]
                        }
                    }
                ]
            },
            {
                "choices": [
                    {
                        "delta": {
                            "tool_calls": [
                                {"index": 0, "id": "", "function": {"name": "", "arguments": "}"}}
                            ]
                        },
                        "finish_reason": "tool_calls",
                    }
                ]
            },
        ),
        request,
    )
    try:
        events = await collect(provider, request)
    finally:
        await client.aclose()
    completed = [e for e in events if e.kind == "tool.complete"]
    assert len(completed) == 1 and completed[0].name == "read" and completed[0].arguments == {}
    assert events[-1].kind == "finish"


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", [OpenAIAdapterProfile.deepseek(), OpenAIAdapterProfile.qwen()])
async def test_explicit_structured_output_capability_preserves_bound_schema(profile):
    request = ProviderRequest.model_validate(request_data())
    schema = {
        "type": "object",
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"],
        "additionalProperties": False,
    }
    resolved = ResolvedProviderInput(
        input_ref=request.input_ref,
        input_digest=request.input_digest,
        messages=({"role": "user", "content": "hello"},),
        response_format={
            "type": "json_schema",
            "json_schema": {"name": "answer", "strict": True, "schema": schema},
        },
    )
    provider, client = build_provider(sse(), request, profile=profile)
    try:
        body = provider._request_body(request, resolved)
        if profile.structured_output_style == "json_object":
            assert body["response_format"] == {"type": "json_object"}
            assert json.dumps(schema, ensure_ascii=False) in body["messages"][0]["content"]
            assert len(body["messages"]) == 2
        else:
            assert body["response_format"] == resolved.response_format
            assert len(body["messages"]) == 1
        assert resolved.messages == ({"role": "user", "content": "hello"},)
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_json_object_profile_rejects_missing_output_schema():
    request = ProviderRequest.model_validate(request_data())
    provider, client = build_provider(sse(), request, profile=OpenAIAdapterProfile.deepseek())
    try:
        with pytest.raises(ValueError, match="bound output schema"):
            provider._request_body(
                request,
                ResolvedProviderInput(
                    input_ref=request.input_ref,
                    input_digest=request.input_digest,
                    messages=({"role": "user", "content": "hello"},),
                    response_format={"type": "json_schema"},
                ),
            )
    finally:
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("thinking", ["enabled", "disabled", "default"])
@pytest.mark.parametrize("profile", [OpenAIAdapterProfile.deepseek(), OpenAIAdapterProfile.qwen()])
async def test_required_tool_respects_thinking_provider_capability(profile, thinking):
    ref = {"key": {"namespace": "test", "key": "required"}, "revision": "1"}
    request = ProviderRequest.model_validate(request_data(thinking=thinking, tools=(ref,)))
    required = request.tools[0]
    resolved = ResolvedProviderInput(
        input_ref=request.input_ref,
        input_digest=request.input_digest,
        messages=({"role": "user", "content": "Use the required tool."},),
        tools=(
            ResolvedTool(
                ref=required,
                definition={
                    "type": "function",
                    "function": {"name": "required", "parameters": {"type": "object"}},
                },
            ),
        ),
        required_tool=required,
    )
    provider, client = build_provider(sse(), request, profile=profile)
    try:
        body = provider._request_body(request, resolved)
        if thinking != "disabled" and not profile.supports_forced_tool_thinking:
            assert body["tool_choice"] == "auto"
            if thinking == "enabled":
                assert body["thinking"] == {"type": "enabled"}
        else:
            assert body["tool_choice"]["type"] == "function"
        assert resolved.required_tool == required
    finally:
        await client.aclose()


async def collect(provider, request, *, cancel=None):
    return [event async for event in provider.stream(request, cancel=cancel)]


def build_provider(response_text, request, *, profile=None, capture=None, required_tool=None):
    async def handler(http_request):
        if capture is not None:
            capture.append(json.loads(http_request.content))
            capture.append(dict(http_request.headers))
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text=response_text,
        )

    async def inputs(_request):
        tools = tuple(
            ResolvedTool(
                ref=ref,
                definition={
                    "type": "function",
                    "function": {
                        "name": ref.key.key,
                        "description": "test",
                        "parameters": {"type": "object"},
                    },
                },
            )
            for ref in request.tools
        )
        return ResolvedProviderInput(
            input_ref=request.input_ref,
            input_digest=request.input_digest,
            messages=({"role": "user", "content": "hello"},),
            tools=tools,
            required_tool=required_tool,
        )

    async def connections(_request):
        return ProviderConnection(
            base_url="http://provider.test/v1",
            api_key="secret",
            headers={"X-Tenant": "test"},
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return (
        OpenAICompatibleProvider(
            client,
            inputs=inputs,
            connections=connections,
            profile=profile,
        ),
        client,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_body,status,expected",
    (
        ({"error": {"code": "context_length_exceeded"}}, 400, "CONTEXT_LIMIT"),
        ({"error": {"type": "max_context_length_exceeded"}}, 413, "CONTEXT_LIMIT"),
        ({"error": {"message": "context_length_exceeded"}}, 400, "INVALID_INPUT"),
        ({"error": {"code": "context_length_exceeded"}}, 401, "INVALID_INPUT"),
        ({"error": {"code": "other", "message": "context too long"}}, 400, "INVALID_INPUT"),
        ({"error": {"code": ["context_length_exceeded"]}}, 422, "INVALID_INPUT"),
        (
            {"error": {"code": "context_length_exceeded", "message": "x" * 17000}},
            400,
            "INVALID_INPUT",
        ),
    ),
)
async def test_context_rejection_uses_bounded_structured_code_only(error_body, status, expected):
    request = ProviderRequest.model_validate(request_data())
    provider, original_client = build_provider("", request)
    await original_client.aclose()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(status, json=error_body))
    ) as client:
        provider._client = client
        events = await collect(provider, request)
    assert events[-1].failure.code == expected
    assert events[-1].failure.retryable == (expected == "CONTEXT_LIMIT")
    assert events[-1].failure.attempt_id == str(request.attempt_id)


@pytest.mark.asyncio
async def test_context_rejection_does_not_invent_zero_usage():
    request = ProviderRequest.model_validate(request_data())
    provider, original_client = build_provider("", request)
    await original_client.aclose()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(400, json={"error": {"code": "context_length_exceeded"}})
        )
    ) as client:
        provider._client = client
        events = await collect(provider, request)
    assert [event.kind for event in events] == ["error"]


@pytest.mark.asyncio
async def test_context_rejection_preserves_explicit_provider_usage():
    request = ProviderRequest.model_validate(request_data())
    provider, original_client = build_provider("", request)
    await original_client.aclose()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                400,
                json={
                    "error": {"code": "context_length_exceeded"},
                    "usage": {"prompt_tokens": 10, "completion_tokens": 0, "total_tokens": 10},
                },
            )
        )
    ) as client:
        provider._client = client
        events = await collect(provider, request)
    assert [event.kind for event in events] == ["usage", "error"]
    assert [event.sequence for event in events] == ["0", "1"]
    assert events[0].usage.total_tokens == "10"


@pytest.mark.asyncio
async def test_unterminated_tool_arguments_are_retry_candidate_but_never_complete():
    request = ProviderRequest.model_validate(request_data())
    provider, client = build_provider(
        sse(
            {
                "choices": [
                    {
                        "index": 0,
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call-1",
                                    "function": {"name": "tool", "arguments": '{"incomplete":'},
                                }
                            ]
                        },
                        "finish_reason": None,
                    }
                ]
            }
        ),
        request,
    )
    try:
        events = await collect(provider, request)
    finally:
        await client.aclose()
    assert not any(event.kind == "tool.complete" for event in events)
    assert events[-1].kind == "error"
    assert events[-1].failure.code == "INCOMPLETE_MODEL_OUTPUT"
    assert events[-1].failure.retryable


@pytest.mark.asyncio
@pytest.mark.parametrize("control", ["deadline", "cancel"])
async def test_invoker_interrupts_adapter_input_resolver_before_http(control):
    request = ProviderRequest.model_validate(request_data())
    request = request.model_copy(
        update={"deadline_at": datetime.now(UTC) + timedelta(milliseconds=50)}
    )
    resolver_started = asyncio.Event()
    resolver_closed = asyncio.Event()
    http_calls = []

    async def inputs(_request):
        resolver_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            resolver_closed.set()

    async def connections(_request):
        return ProviderConnection(base_url="http://provider.test/v1", api_key=None)

    async def handler(http_request):
        http_calls.append(http_request)
        return httpx.Response(200, text="data: [DONE]\n\n")

    async def capabilities(_request):
        return ProviderCapabilities(
            model=request.model.ref,
            streaming="supported",
            tool_calls="supported",
            structured_output="supported",
            thinking_control="supported",
            tools_with_structured_output="supported",
            tools_with_thinking="supported",
            usage="unknown",
            cancellation="supported",
            context_window=4096,
            max_output_tokens=1024,
            allowed_option_keys=(),
        )

    class Attempts:
        def __init__(self):
            self.terminals = []

        async def begin(self, _request, *, started_at):
            return "started"

        async def finish(self, _request, terminal):
            self.terminals.append(terminal)

    class Cancel:
        def __init__(self):
            self.event = asyncio.Event()

        def is_cancelled(self):
            return self.event.is_set()

        async def wait(self):
            await self.event.wait()

    cancel = Cancel()
    attempts = Attempts()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = OpenAICompatibleProvider(client, inputs=inputs, connections=connections)
        invoker = ProviderInvoker(adapter, capabilities=capabilities, attempts=attempts)
        task = asyncio.create_task(collect(invoker, request, cancel=cancel))
        await resolver_started.wait()
        if control == "cancel":
            cancel.event.set()
        events = await asyncio.wait_for(task, timeout=1)
    assert resolver_closed.is_set()
    assert not http_calls
    assert attempts.terminals[0].status == ("cancelled" if control == "cancel" else "failed")
    assert events[-1].kind == ("finish" if control == "cancel" else "error")


@pytest.mark.asyncio
async def test_content_analysis_usage_and_finish_are_normalized():
    request = ProviderRequest.model_validate(request_data(thinking="enabled"))
    response = sse(
        {
            "choices": [
                {
                    "delta": {"reasoning_content": "think", "content": "answer"},
                    "finish_reason": None,
                }
            ]
        },
        {
            "choices": [{"delta": {}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5},
        },
    )
    captured = []
    provider, client = build_provider(
        response, request, profile=OpenAIAdapterProfile.qwen(), capture=captured
    )
    try:
        events = await collect(provider, request)
    finally:
        await client.aclose()
    assert [event.kind for event in events] == [
        "analysis.delta",
        "content.delta",
        "usage",
        "finish",
    ]
    assert [event.sequence for event in events] == ["0", "1", "2", "3"]
    assert events[-1].reason == "stop"
    assert events[-2].usage.total_tokens == "5"
    body, headers = captured
    assert body["chat_template_kwargs"] == {"enable_thinking": True}
    assert body["stream_options"] == {"include_usage": True}
    assert headers["authorization"] == "Bearer secret"
    assert "secret" not in repr(provider)


@pytest.mark.asyncio
@pytest.mark.parametrize("thinking", ["enabled", "disabled"])
async def test_deepseek_thinking_uses_native_body_and_keeps_analysis_separate(thinking):
    request = ProviderRequest.model_validate(request_data(thinking=thinking))
    captured = []
    provider, client = build_provider(
        sse(
            {
                "choices": [
                    {
                        "delta": {"reasoning_content": "reason", "content": "answer"},
                        "finish_reason": "stop",
                    }
                ]
            }
        ),
        request,
        profile=OpenAIAdapterProfile.deepseek(),
        capture=captured,
    )
    try:
        events = await collect(provider, request)
    finally:
        await client.aclose()
    assert captured[0]["thinking"] == {"type": thinking}
    assert "chat_template_kwargs" not in captured[0]
    assert [event.text for event in events if event.kind == "analysis.delta"] == ["reason"]
    assert [event.text for event in events if event.kind == "content.delta"] == ["answer"]


@pytest.mark.parametrize("option", [{"type": "disabled"}, {"type": []}, {"other": "enabled"}])
def test_deepseek_rejects_conflicting_or_malformed_thinking(option):
    request = ProviderRequest.model_validate(request_data(thinking="enabled"))
    with pytest.raises(ValueError):
        OpenAIAdapterProfile.deepseek().validate_options(request.model, {"thinking": option})


@pytest.mark.asyncio
@pytest.mark.parametrize("analysis", [None, ""])
async def test_deepseek_enabled_tool_call_may_have_no_reasoning(analysis):
    tool = {"key": {"namespace": "test", "key": "clock"}, "revision": "1"}
    request = ProviderRequest.model_validate(request_data(thinking="enabled", tools=(tool,)))
    captured = []
    provider, client = build_provider(
        sse(
            {
                "choices": [
                    {
                        "delta": {
                            "reasoning_content": analysis,
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call-1",
                                    "function": {
                                        "name": "clock",
                                        "arguments": "{}",
                                    },
                                }
                            ],
                        },
                        "finish_reason": "tool_calls",
                    }
                ]
            }
        ),
        request,
        profile=OpenAIAdapterProfile.deepseek(),
        capture=captured,
    )
    try:
        events = await collect(provider, request)
    finally:
        await client.aclose()
    assert captured[0]["thinking"] == {"type": "enabled"}
    assert not any(event.kind in {"analysis.delta", "error"} for event in events)
    assert any(event.kind == "tool.complete" for event in events)
    assert events[-1].kind == "finish" and events[-1].reason == "tool_calls"


@pytest.mark.asyncio
async def test_usage_frame_after_finish_choice_is_read_before_terminal_event():
    request = ProviderRequest.model_validate(request_data())
    response = sse(
        {"choices": [{"delta": {"content": "answer"}, "finish_reason": "stop"}]},
        {
            "choices": [],
            "usage": {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10},
        },
    )
    provider, client = build_provider(response, request)
    try:
        events = await collect(provider, request)
    finally:
        await client.aclose()
    assert [event.kind for event in events] == ["content.delta", "usage", "finish"]
    assert events[1].usage.total_tokens == "10"


@pytest.mark.asyncio
async def test_multiple_tool_calls_preserve_index_and_fragments():
    refs = (
        {"key": {"namespace": "test", "key": "weather"}, "revision": "1"},
        {"key": {"namespace": "test", "key": "clock"}, "revision": "1"},
    )
    request = ProviderRequest.model_validate(request_data(tools=refs))
    response = sse(
        {
            "choices": [
                {
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call-a",
                                "function": {"name": "weather", "arguments": '{"city":'},
                            },
                            {
                                "index": 1,
                                "id": "call-b",
                                "function": {"name": "clock", "arguments": "{}"},
                            },
                        ]
                    },
                    "finish_reason": None,
                }
            ]
        },
        {
            "choices": [
                {
                    "delta": {
                        "tool_calls": [{"index": 0, "function": {"arguments": '"Beijing"}'}}]
                    },
                    "finish_reason": "tool_calls",
                }
            ]
        },
    )
    provider, client = build_provider(response, request)
    try:
        events = await collect(provider, request)
    finally:
        await client.aclose()
    assert [event.kind for event in events] == [
        "tool.arguments.delta",
        "tool.arguments.delta",
        "tool.arguments.delta",
        "tool.complete",
        "tool.complete",
        "finish",
    ]
    complete = [event for event in events if event.kind == "tool.complete"]
    assert complete[0].arguments == {"city": "Beijing"}
    assert complete[1].arguments == {}
    assert events[-1].reason == "tool_calls"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "code", "retryable"),
    [
        (400, "INVALID_INPUT", False),
        (429, "DEPENDENCY_UNAVAILABLE", True),
        (503, "DEPENDENCY_UNAVAILABLE", True),
    ],
)
async def test_http_errors_are_structured_without_response_body(status, code, retryable):
    request = ProviderRequest.model_validate(request_data())

    async def handler(_request):
        return httpx.Response(status, text="private upstream diagnostics")

    async def inputs(_request):
        return ResolvedProviderInput(
            input_ref=request.input_ref,
            input_digest=request.input_digest,
            messages=({"role": "user", "content": "hello"},),
        )

    async def connections(_request):
        return ProviderConnection("http://provider.test/v1", "secret")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = OpenAICompatibleProvider(client, inputs=inputs, connections=connections)
    try:
        events = await collect(provider, request)
    finally:
        await client.aclose()
    assert len(events) == 1 and events[0].kind == "error"
    assert events[0].failure.code == code
    assert events[0].failure.retryable is retryable
    assert "private upstream" not in events[0].failure.public_message


@pytest.mark.asyncio
async def test_malformed_and_missing_terminal_are_not_success():
    request = ProviderRequest.model_validate(request_data())
    for response in ("data: {bad-json}\n\n", sse({"choices": [{"delta": {"content": "x"}}]})):
        provider, client = build_provider(response, request)
        try:
            events = await collect(provider, request)
        finally:
            await client.aclose()
        assert events[-1].kind == "error"
        assert events[-1].failure.code == "INCOMPLETE_MODEL_OUTPUT"


@pytest.mark.asyncio
async def test_resolved_identity_mismatch_is_rejected_without_leaking_values():
    request = ProviderRequest.model_validate(request_data())

    async def handler(_request):
        raise AssertionError("network must not be reached")

    async def inputs(_request):
        return ResolvedProviderInput(
            input_ref="wrong",
            input_digest=request.input_digest,
            messages=({"role": "user", "content": "secret-input"},),
        )

    async def connections(_request):
        return ProviderConnection("http://provider.test/v1", "secret")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = OpenAICompatibleProvider(client, inputs=inputs, connections=connections)
    try:
        events = await collect(provider, request)
    finally:
        await client.aclose()
    assert events[0].failure.code == "INVALID_INPUT"
    assert "secret-input" not in events[0].failure.public_message


def test_connection_and_profile_reject_unsafe_configuration():
    with pytest.raises(ValueError):
        ProviderConnection("http://user:password@example.test/v1", None)
    with pytest.raises(ValueError):
        ProviderConnection("file:///tmp/provider", None)
    profile = OpenAIAdapterProfile.qwen()
    with pytest.raises(ValueError):
        profile.validate_options(
            ProviderRequest.model_validate(request_data()).model,
            {"chat_template_kwargs": {"enable_thinking": "yes"}},
        )
    generic = OpenAIAdapterProfile()
    model = ProviderRequest.model_validate(request_data(thinking="enabled")).model
    with pytest.raises(ValueError):
        generic.validate_options(model, {})
    with pytest.raises(ValueError):
        generic.validate_options(model, {"chat_template_kwargs": {}})

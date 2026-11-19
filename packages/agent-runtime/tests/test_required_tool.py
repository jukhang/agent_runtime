"""Pinned native tool selection is context-bound and fails closed."""

from dataclasses import replace

import pytest
from pydantic import ValidationError
from test_agent_loop import build
from test_context_openai_input import Access, model_request
from test_context_runtime import request, runtime
from test_decision_validation import TOOL
from test_provider_openai import build_provider, collect, request_data, sse

from agent_runtime.contracts.context import ContextRequest
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.providers.context_input import ContextOpenAIInputResolver
from agent_runtime.providers.openai_compatible import (
    OpenAIAdapterProfile,
    OpenAICompatibleProvider,
    ResolvedProviderInput,
)
from agent_runtime.tool_names import provider_tool_name

REF = CapabilityRef.model_validate(TOOL)


@pytest.mark.parametrize(
    "required,known,reason",
    [
        (True, True, "stop"),
        (True, False, "stop"),
        (True, True, "length"),
        (False, True, "stop"),
    ],
)
def test_any_required_tool_stop_never_accepts_unknown_name_or_truncation(required, known, reason):
    from agent_runtime.providers.openai_compatible import _OpenAIStreamParser

    req = ProviderRequest.model_validate(request_data(tools=[TOOL]))
    parser = _OpenAIStreamParser(req, OpenAIAdapterProfile.qwen(), require_tool_call=required)
    events = parser.feed(
        {
            "choices": [
                {
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call1",
                                "type": "function",
                                "function": {
                                    "name": provider_tool_name(REF) if known else "unknown_tool",
                                    "arguments": '{"value":1}',
                                },
                            }
                        ]
                    },
                    "finish_reason": reason,
                }
            ]
        }
    )
    completed = any(e.kind == "tool.complete" for e in events)
    assert completed == (required and known and reason == "stop")
    assert parser.pending_finish.reason == ("tool_calls" if completed else reason)


@pytest.mark.asyncio
async def test_required_selection_is_snapshot_bound_and_renderer_cannot_remove_it():
    context, _, _ = runtime()
    req = request(refs=0).model_copy(update={"tools": (REF,), "require_tool_call": True})
    prepared = await context.prepare(req)
    _, payload = await context.load(req.actor.scope_id, prepared.snapshot_id)
    assert payload["requireToolCall"] is True
    with pytest.raises(ValidationError):
        ContextRequest.model_validate({**request(refs=0).model_dump(), "require_tool_call": True})
    context._payload_renderer = lambda p: {k: v for k, v in p.items() if k != "requireToolCall"}
    with pytest.raises(AgentRuntimeError, match="native tool requirement"):
        await context.prepare(req)


@pytest.mark.parametrize("forced", [True, False])
def test_required_selection_wire_and_default_are_separate(forced):
    from agent_runtime.providers.openai_compatible import ResolvedTool

    req = ProviderRequest.model_validate(request_data(thinking="disabled", tools=[TOOL]))
    resolved = ResolvedProviderInput(
        req.input_ref,
        req.input_digest,
        ({"role": "user", "content": "save"},),
        tools=(ResolvedTool(REF, definition(REF)),),
        require_tool_call=True,
    )
    provider = OpenAICompatibleProvider(
        None,
        inputs=None,
        connections=None,
        profile=replace(OpenAIAdapterProfile.qwen(), supports_forced_tool_choice=forced),
    )
    assert provider._request_body(req, resolved)["tool_choice"] == (
        "required" if forced else "auto"
    )
    assert "tool_choice" not in provider._request_body(
        req, replace(resolved, require_tool_call=False)
    )


@pytest.mark.asyncio
async def test_loop_does_not_save_prose_when_any_visible_tool_is_required():
    loop, state, inputs, budget, backend, answers = build("answer")
    original = loop._setup.resolve

    class RequiredSetup:
        async def resolve(self, *args):
            setup = await original(*args)
            return replace(
                setup,
                context_request=setup.context_request.model_copy(
                    update={"tools": (REF,), "require_tool_call": True}
                ),
            )

    loop._setup = RequiredSetup()
    with pytest.raises(AgentRuntimeError):
        await loop.decide(state, inputs, budget)
    assert not answers.saved


@pytest.mark.parametrize("thinking", ["enabled", "disabled", "default"])
def test_explicit_auto_selection_preserves_the_required_tool_contract(thinking):
    req = ProviderRequest.model_validate(request_data(thinking=thinking, tools=[TOOL]))
    resolved = ResolvedProviderInput(
        req.input_ref,
        req.input_digest,
        ({"role": "user", "content": "Use the tool."},),
        tools=(),
    )
    from agent_runtime.providers.openai_compatible import ResolvedTool

    resolved = replace(resolved, tools=(ResolvedTool(REF, definition(REF)),), required_tool=REF)
    provider = OpenAICompatibleProvider(
        None,
        inputs=None,
        connections=None,
        profile=replace(OpenAIAdapterProfile.qwen(), supports_forced_tool_choice=False),
    )
    body = provider._request_body(req, resolved)
    assert body["tool_choice"] == "auto"
    assert resolved.required_tool == REF
    assert (
        body["chat_template_kwargs"].get("enable_thinking") == (thinking == "enabled")
        if thinking != "default"
        else "chat_template_kwargs" not in body
    )


def definition(ref):
    return {
        "type": "function",
        "function": {"name": provider_tool_name(ref), "parameters": {"type": "object"}},
    }


def test_required_tool_must_be_installed_and_native():
    data = request(refs=0).model_dump()
    with pytest.raises(ValidationError):
        ContextRequest.model_validate({**data, "required_tool": REF})
    with pytest.raises(ValueError, match="not in resolved"):
        ResolvedProviderInput(
            "input", "a" * 64, ({"role": "user", "content": "hi"},), required_tool=REF
        )


@pytest.mark.asyncio
async def test_required_tool_is_bound_to_persisted_context_and_native_wire():
    context, _, _ = runtime()
    req = request(refs=0).model_copy(
        update={
            "tools": (REF,),
            "required_tool": REF,
            "prompt_text": "Commit validated result.",
            "user_input": "Create the result.",
        }
    )
    snapshot = await context.prepare(req)
    _, payload = await context.load(req.actor.scope_id, snapshot.snapshot_id)
    assert payload["requiredTool"] == REF.model_dump(mode="json", by_alias=True)

    class Definitions:
        async def definition(self, ref):
            return definition(ref)

    resolver = ContextOpenAIInputResolver(
        context=context, actor=req.actor, access=Access(), tools=Definitions()
    )
    model = model_request(snapshot).model_copy(update={"tools": (REF,)})
    resolved = await resolver(model)
    assert resolved.required_tool == REF
    # Body construction has no I/O and cannot obtain choice from provider_options.
    provider = OpenAICompatibleProvider(None, inputs=resolver, connections=None)
    wire_request = ProviderRequest.model_validate(request_data(tools=[TOOL]))
    body = provider._request_body(wire_request, resolved)
    assert body["tool_choice"]["function"]["name"] == provider_tool_name(REF)
    default = replace(resolved, required_tool=None)
    assert "tool_choice" not in provider._request_body(wire_request, default)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["answer", "tool"])
async def test_loop_rejects_prose_when_native_tool_is_required(kind):
    loop, state, inputs, budget, backend, answers = build(kind)
    original = loop._setup.resolve

    class RequiredSetup:
        async def resolve(self, *args):
            setup = await original(*args)
            return replace(
                setup,
                context_request=setup.context_request.model_copy(
                    update={"tools": (REF,), "required_tool": REF}
                ),
            )

    loop._setup = RequiredSetup()
    if kind == "answer":
        with pytest.raises(AgentRuntimeError) as error:
            await loop.decide(state, inputs, budget)
        assert error.value.failure.code == "INCOMPLETE_MODEL_OUTPUT"
        assert not answers.saved
    else:
        result = await loop.decide(state, inputs, budget)
        assert result.action.kind == "invoke_tools"


@pytest.mark.asyncio
@pytest.mark.parametrize("bound", [True, False])
async def test_named_native_stop_is_normalized_only_with_verified_binding(bound):
    req = ProviderRequest.model_validate(request_data(tools=[TOOL]))
    stream = sse(
        {
            "choices": [
                {
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call1",
                                "type": "function",
                                "function": {
                                    "name": provider_tool_name(REF),
                                    "arguments": '{"value":1}',
                                },
                            }
                        ]
                    },
                    "finish_reason": "stop",
                }
            ]
        }
    )
    provider, client = build_provider(stream, req, required_tool=REF if bound else None)
    try:
        events = await collect(provider, req)
    finally:
        await client.aclose()
    finish = next(e for e in events if e.kind == "finish")
    assert finish.raw_reason == "stop"
    assert finish.reason == ("tool_calls" if bound else "stop")
    assert any(e.kind == "tool.complete" for e in events) == bound

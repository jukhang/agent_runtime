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
    OpenAICompatibleProvider,
    ResolvedProviderInput,
)
from agent_runtime.tool_names import provider_tool_name

REF = CapabilityRef.model_validate(TOOL)


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

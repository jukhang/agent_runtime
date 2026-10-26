"""Provider messages and definitions must derive from one authorized snapshot."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from test_context_runtime import REF, Estimator, Payloads, Snapshots, request, runtime
from test_decision_validation import TOOL

from agent_runtime.context_runtime import ContextRuntime, LoadedSource
from agent_runtime.contracts.canonical import canonical_json
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.native_controls import ASK_USER_CONTROL, ask_user_definition
from agent_runtime.providers.context_input import ContextOpenAIInputResolver
from agent_runtime.tool_names import provider_tool_name


@pytest.mark.asyncio
async def test_native_control_definition_is_pinned_not_supplied_by_host_tools():
    context, _, _ = runtime()
    req = request(refs=0).model_copy(
        update={
            "tools": (ASK_USER_CONTROL,),
            "prompt_text": "Clarify required facts.",
            "user_input": "Prepare the report after asking which quarter.",
        }
    )
    snapshot = await context.prepare(req)

    class NoBusinessDefinition:
        async def definition(self, ref):
            raise AssertionError("A host must not override the Runtime control definition")

    resolver = ContextOpenAIInputResolver(
        context=context, actor=req.actor, access=Access(), tools=NoBusinessDefinition()
    )
    current = model_request(snapshot).model_copy(update={"tools": (ASK_USER_CONTROL,)})
    resolved = await resolver(current)
    assert resolved.tools[0].ref == ASK_USER_CONTROL
    assert resolved.tools[0].definition == ask_user_definition()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["paired", "call_mismatch", "foreign_run", "unknown_effect", "data_only"]
)
async def test_tool_observation_becomes_native_paired_history(case):
    req = request(refs=0).model_copy(
        update={
            "prompt_text": "Use the evidence.",
            "user_input": "Add 9 to the counter.",
            "tools": (CapabilityRef.model_validate(TOOL),),
        }
    )
    call_id = uuid4()
    data = {
        "call": {"toolCallId": str(call_id), "tool": TOOL, "arguments": {}},
        "outcome": {
            "status": "completed",
            "toolCallId": str(call_id),
            "runId": str(req.run_id),
            "operationId": None,
            "effect": "none",
            "effectRefs": [],
            "result": {"policy": "full", "data": {"counter": 5837}},
        },
    }
    if case == "call_mismatch":
        data["outcome"]["toolCallId"] = str(uuid4())
    if case == "foreign_run":
        data["outcome"]["runId"] = str(uuid4())
    if case == "unknown_effect":
        data["outcome"].update(
            {
                "status": "failed",
                "operationId": str(uuid4()),
                "effect": "unknown",
                "failure": {
                    "code": "SIDE_EFFECT_UNKNOWN",
                    "category": "tool",
                    "retryable": False,
                    "publicMessage": "Reconciliation required.",
                },
            }
        )
        del data["outcome"]["result"]
    resource = req.resource_refs[0] if req.resource_refs else request().resource_refs[0]
    resource = resource.model_copy(
        update={
            "resource_type": "artifact" if case == "data_only" else "tool_call_result",
            "resource_id": str(call_id),
            "resource_version": "1",
        }
    )
    req = req.model_copy(update={"resource_refs": (resource,)})

    class Reader:
        async def read(self, request, ref):
            return LoadedSource(resource=ref, content=canonical_json(data), required=True)

    context = ContextRuntime(
        reader=Reader(), estimator=Estimator(), payloads=Payloads(), snapshots=Snapshots()
    )
    snapshot = await context.prepare(req)
    resolver = ContextOpenAIInputResolver(
        context=context, actor=req.actor, access=Access(), tools=Definitions()
    )
    if case in {"call_mismatch", "foreign_run", "unknown_effect"}:
        with pytest.raises(AgentRuntimeError, match="pairing"):
            await resolver(model_request(snapshot))
        return
    resolved = await resolver(model_request(snapshot))
    if case == "data_only":
        assert all(message["role"] != "tool" for message in resolved.messages)
        return
    assistant, result = resolved.messages[-2:]
    assert assistant["role"] == "assistant"
    assert assistant["tool_calls"][0]["id"] == str(call_id)
    assert assistant["tool_calls"][0]["function"]["name"] == provider_tool_name(req.tools[0])
    assert result["role"] == "tool" and result["tool_call_id"] == str(call_id)
    assert "5837" in result["content"]


class Access:
    def __init__(self):
        self.denied = False

    async def verify(self, actor, snapshot):
        if self.denied:
            raise runtime_failure(
                "FORBIDDEN", category="authorization", public_message="Access revoked."
            )


class Definitions:
    def __init__(self, *, changed: bool = False):
        self.changed = changed

    async def definition(self, ref: CapabilityRef):
        return {
            "type": "function",
            "function": {
                "name": "wrong" if self.changed else provider_tool_name(ref),
                "description": "Read source",
                "parameters": {"type": "object", "properties": {}},
            },
        }


def model_request(snapshot):
    return ProviderRequest.model_validate(
        {
            "scopeId": snapshot.scope_id,
            "sessionId": str(snapshot.session_id),
            "runId": str(snapshot.run_id),
            "attemptId": str(uuid4()),
            "contextSnapshotId": str(snapshot.snapshot_id),
            "contextVersion": snapshot.context_version,
            "inputRef": snapshot.payload_ref,
            "inputDigest": snapshot.content_hash,
            "estimatedInputTokens": snapshot.estimated_input_tokens,
            "budgetAllocationId": str(snapshot.budget.allocation_id),
            "deadlineAt": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
            "model": {
                "ref": REF,
                "connection": REF,
                "modelName": "model",
                "maxOutputTokens": 100,
                "thinking": "default",
                "providerOptions": {},
            },
            "tools": [TOOL],
            "outputSchema": None,
        }
    )


@pytest.mark.asyncio
async def test_resolved_messages_match_pinned_prompt_question_and_source():
    context, _, _ = runtime()
    req = request()
    req = req.model_copy(
        update={
            "prompt_text": "Be accurate.",
            "user_input": "Summarize the file.",
            "tools": (CapabilityRef.model_validate(TOOL),),
        }
    )
    snapshot = await context.prepare(req)
    access = Access()
    resolver = ContextOpenAIInputResolver(
        context=context, actor=req.actor, access=access, tools=Definitions()
    )
    resolved = await resolver(model_request(snapshot))
    assert resolved.input_digest == snapshot.content_hash
    assert resolved.messages[0]["role"] == "system"
    assert "untrusted data" in str(resolved.messages[0]["content"])
    assert resolved.messages[1]["content"] == "Summarize the file."
    assert resolved.messages[2]["role"] == "user"
    assert "contents" in str(resolved.messages[2]["content"])
    assert resolved.tools[0].definition["function"]["name"] == provider_tool_name(snapshot.tools[0])
    access.denied = True
    with pytest.raises(AgentRuntimeError):
        await resolver(model_request(snapshot))


@pytest.mark.asyncio
async def test_unpinned_prompt_or_tool_definition_is_rejected():
    context, _, _ = runtime()
    req = request(refs=0).model_copy(
        update={
            "prompt_text": None,
            "user_input": "Hello",
            "tools": (CapabilityRef.model_validate(TOOL),),
        }
    )
    snapshot = await context.prepare(req)
    resolver = ContextOpenAIInputResolver(
        context=context, actor=req.actor, access=Access(), tools=Definitions()
    )
    with pytest.raises(AgentRuntimeError):
        await resolver(model_request(snapshot))
    req = req.model_copy(update={"prompt_text": "Trusted instruction"})
    snapshot = await context.prepare(req)
    resolver = ContextOpenAIInputResolver(
        context=context, actor=req.actor, access=Access(), tools=Definitions(changed=True)
    )
    with pytest.raises(AgentRuntimeError):
        await resolver(model_request(snapshot))


@pytest.mark.asyncio
async def test_output_format_requires_exact_installed_schema_revision():
    context, _, _ = runtime()
    req = request(refs=0).model_copy(
        update={
            "prompt_text": "Return a JSON decision.",
            "user_input": "Evaluate the result.",
            "tools": (CapabilityRef.model_validate(TOOL),),
        }
    )
    snapshot = await context.prepare(req)
    schema_ref = CapabilityRef.model_validate(REF)
    response_format = {
        "type": "json_schema",
        "json_schema": {
            "name": "decision",
            "schema": {"type": "object"},
        },
    }
    resolver = ContextOpenAIInputResolver(
        context=context,
        actor=req.actor,
        access=Access(),
        tools=Definitions(),
        output_formats={schema_ref: response_format},
    )
    response_format["type"] = "changed"
    bound = model_request(snapshot).model_copy(update={"output_schema": schema_ref})
    resolved = await resolver(bound)
    assert resolved.response_format["type"] == "json_schema"
    with pytest.raises(AgentRuntimeError):
        await resolver(
            bound.model_copy(
                update={"output_schema": schema_ref.model_copy(update={"revision": "2"})}
            )
        )


@pytest.mark.asyncio
async def test_output_format_callback_keeps_shared_owner_and_runs_after_authorization():
    context, _, _ = runtime()
    req = request(refs=0).model_copy(
        update={"prompt_text": "Evaluate the answer.", "user_input": "Hello", "tools": ()}
    )
    snapshot = await context.prepare(req)
    schema_ref = CapabilityRef.model_validate(REF)
    access = Access()

    class Owner:
        def __init__(self):
            self.calls = []

        def __deepcopy__(self, memo):
            raise AssertionError("process-owned clients must not be copied")

        async def resolve(self, bound, prepared):
            self.calls.append((bound, prepared))
            return {"type": "json_schema", "json_schema": {"name": "decision"}}

    owner = Owner()
    resolver = ContextOpenAIInputResolver(
        context=context,
        actor=req.actor,
        access=access,
        tools=Definitions(),
        output_formats={schema_ref: owner.resolve},
    )
    bound = model_request(snapshot).model_copy(update={"output_schema": schema_ref, "tools": ()})
    resolved = await resolver(bound)
    assert resolved.response_format["type"] == "json_schema"
    assert len(owner.calls) == 1
    assert owner.calls[0][1].snapshot_id == snapshot.snapshot_id
    access.denied = True
    with pytest.raises(AgentRuntimeError):
        await resolver(bound)
    assert len(owner.calls) == 1

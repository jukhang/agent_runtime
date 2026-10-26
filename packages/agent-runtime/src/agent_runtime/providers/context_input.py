"""Resolve OpenAI-compatible messages only from an authorized Context snapshot."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from copy import deepcopy
from typing import Protocol

from pydantic import JsonValue

from agent_runtime.context_runtime import ContextAccessVerifier, ContextRuntime
from agent_runtime.context_validation import validate_context_binding
from agent_runtime.contracts.canonical import canonical_json
from agent_runtime.contracts.context import PreparedContext
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.native_controls import ASK_USER_CONTROL, ask_user_definition
from agent_runtime.providers.openai_compatible import ResolvedProviderInput, ResolvedTool
from agent_runtime.tool_names import provider_tool_name
from agent_runtime.tool_transcript import ToolTranscript

type OutputFormatFactory = Callable[
    [ProviderRequest, PreparedContext], Awaitable[Mapping[str, JsonValue]]
]


class ToolDefinitionReader(Protocol):
    async def definition(self, ref: CapabilityRef) -> Mapping[str, JsonValue]:
        """Return a trusted JSON Schema/function definition for one pinned tool."""
        ...


class ContextOpenAIInputResolver:
    def __init__(
        self,
        *,
        context: ContextRuntime,
        actor: ActorContext,
        access: ContextAccessVerifier,
        tools: ToolDefinitionReader,
        output_formats: Mapping[CapabilityRef, Mapping[str, JsonValue] | OutputFormatFactory]
        | None = None,
    ) -> None:
        self._context = context
        self._actor = actor
        self._access = access
        self._tools = tools
        self._formats = {
            ref: binding if callable(binding) else deepcopy(binding)
            for ref, binding in (output_formats or {}).items()
        }

    async def __call__(self, request: ProviderRequest) -> ResolvedProviderInput:
        snapshot, payload = await self._context.load(request.scope_id, request.context_snapshot_id)
        validate_context_binding(snapshot, request)
        await self._access.verify(self._actor, snapshot)
        prompt = payload.get("promptText")
        user_input = payload.get("userInput")
        source_policy = payload.get("sourcePolicy")
        source_label = payload.get("sourceLabel")
        if not isinstance(prompt, str) or not prompt.strip():
            raise runtime_failure(
                "INVALID_INPUT", category="context", public_message="Trusted prompt is missing."
            )
        if not isinstance(user_input, str) or not user_input.strip():
            raise runtime_failure(
                "INVALID_INPUT", category="context", public_message="User input is missing."
            )
        if not isinstance(source_policy, str) or not isinstance(source_label, str):
            raise runtime_failure(
                "INVALID_INPUT", category="context", public_message="Source framing is missing."
            )
        messages: list[Mapping[str, JsonValue]] = [
            {
                "role": "system",
                "content": prompt + "\n" + source_policy,
            },
            {"role": "user", "content": user_input},
        ]
        sources = payload.get("sources")
        if not isinstance(sources, list):
            raise runtime_failure(
                "INVALID_INPUT", category="context", public_message="Context sources are invalid."
            )
        data_sources: list[JsonValue] = []
        transcripts: list[ToolTranscript] = []
        seen_calls: set[str] = set()
        for source in sources:
            if not isinstance(source, dict) or not isinstance(source.get("resource"), dict):
                raise runtime_failure(
                    "INVALID_INPUT",
                    category="context",
                    public_message="Context source framing is invalid.",
                )
            resource = source["resource"]
            if resource.get("resourceType") != "tool_call_result":
                data_sources.append(source)
                continue
            try:
                transcript = ToolTranscript.model_validate_json(source["content"])
                call_id = str(transcript.call.tool_call_id)
                if (
                    call_id != resource.get("resourceId")
                    or resource.get("resourceVersion") != "1"
                    or transcript.outcome.run_id != request.run_id
                    or call_id in seen_calls
                ):
                    raise ValueError("tool transcript source identity differs")
            except (ValueError, TypeError, KeyError) as exc:
                raise runtime_failure(
                    "INVALID_INPUT",
                    category="context",
                    public_message="Tool history pairing is invalid.",
                ) from exc
            seen_calls.add(call_id)
            transcripts.append(transcript)
        if data_sources:
            messages.append(
                {
                    "role": "user",
                    "content": source_label + "\n" + canonical_json(data_sources),
                }
            )
        for transcript in transcripts:
            call_id = str(transcript.call.tool_call_id)
            messages.append(
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": call_id,
                            "type": "function",
                            "function": {
                                "name": provider_tool_name(transcript.call.tool),
                                "arguments": canonical_json(transcript.call.arguments),
                            },
                        }
                    ],
                }
            )
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": canonical_json(
                        transcript.outcome.model_dump(mode="json", by_alias=True)
                    ),
                }
            )
        resolved_tools: list[ResolvedTool] = []
        for ref in snapshot.tools:
            definition = (
                ask_user_definition()
                if ref == ASK_USER_CONTROL
                else await self._tools.definition(ref)
            )
            function = definition.get("function")
            if (
                definition.get("type") != "function"
                or not isinstance(function, Mapping)
                or function.get("name") != provider_tool_name(ref)
            ):
                raise runtime_failure(
                    "INVALID_INPUT",
                    category="context",
                    public_message="Tool definition does not match its pinned revision.",
                )
            resolved_tools.append(ResolvedTool(ref=ref, definition=definition))
        response_format: Mapping[str, JsonValue] | None = None
        if request.output_schema is not None:
            format_binding = self._formats.get(request.output_schema)
            if format_binding is None:
                raise runtime_failure(
                    "FORBIDDEN",
                    category="context",
                    public_message="Output schema revision is not installed.",
                )
            response_format = (
                await format_binding(request, snapshot)
                if callable(format_binding)
                else format_binding
            )
        return ResolvedProviderInput(
            input_ref=snapshot.payload_ref,
            input_digest=snapshot.content_hash,
            messages=tuple(messages),
            tools=tuple(resolved_tools),
            response_format=response_format,
        )

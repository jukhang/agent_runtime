"""OpenAI-compatible streaming adapter for online and private model endpoints.

The module is part of the optional ``provider`` extra.  It accepts a lifespan-owned
``httpx.AsyncClient`` and trusted resolvers; it never reads environment configuration,
creates credentials or retries a request.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal, cast
from urllib.parse import urlsplit

import httpx

from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.model import ModelConfig, ProviderRequest
from agent_runtime.contracts.provider import (
    AnalysisDelta,
    ContentDelta,
    ProviderError,
    ProviderEvent,
    ProviderFinish,
    ProviderUsage,
    ToolArgumentsDelta,
    ToolCallComplete,
)
from agent_runtime.contracts.results import Usage
from agent_runtime.ports import CancelSignal, Provider
from agent_runtime.providers.context_errors import vllm_context_error

if TYPE_CHECKING:
    from pydantic import JsonValue

    from agent_runtime.contracts.references import CapabilityRef

InputResolver = Callable[[ProviderRequest], Awaitable["ResolvedProviderInput"]]
ConnectionResolver = Callable[[ProviderRequest], Awaitable["ProviderConnection"]]


@dataclass(frozen=True, slots=True)
class ResolvedTool:
    ref: CapabilityRef
    definition: Mapping[str, JsonValue]


@dataclass(frozen=True, slots=True)
class ResolvedProviderInput:
    input_ref: str
    input_digest: str
    messages: tuple[Mapping[str, JsonValue], ...]
    tools: tuple[ResolvedTool, ...] = ()
    response_format: Mapping[str, JsonValue] | None = None

    def __post_init__(self) -> None:
        if not self.messages:
            raise ValueError("provider input requires at least one message")
        for message in self.messages:
            role = message.get("role")
            if role not in {"system", "developer", "user", "assistant", "tool"}:
                raise ValueError("provider message has an unsupported role")
            _finite_json(message)
        for tool in self.tools:
            _finite_json(tool.definition)
        if self.response_format is not None:
            _finite_json(self.response_format)


@dataclass(frozen=True, slots=True)
class ProviderConnection:
    base_url: str
    api_key: str | None = field(repr=False)
    headers: Mapping[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        parsed = urlsplit(self.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("provider base_url must be an absolute HTTP(S) URL")
        if parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise ValueError("provider base_url cannot contain credentials, query or fragment")
        for key, value in self.headers.items():
            if not key or "\r" in key or "\n" in key or "\r" in value or "\n" in value:
                raise ValueError("provider headers contain invalid characters")


@dataclass(frozen=True, slots=True)
class OpenAIAdapterProfile:
    name: str = "openai-compatible"
    endpoint_path: str = "/chat/completions"
    max_tokens_field: Literal["max_tokens", "max_completion_tokens"] = "max_tokens"
    analysis_fields: tuple[str, ...] = ("reasoning_content", "reasoning")
    thinking_style: Literal["none", "qwen_chat_template"] = "none"
    include_usage: bool = True
    context_error_codes: tuple[str, ...] = (
        "context_length_exceeded",
        "max_context_length_exceeded",
    )
    context_error_format: Literal["structured", "vllm"] = "structured"

    def __post_init__(self) -> None:
        if not self.endpoint_path.startswith("/"):
            raise ValueError("endpoint_path must be absolute within the provider base URL")
        if not self.analysis_fields or len(set(self.analysis_fields)) != len(self.analysis_fields):
            raise ValueError("analysis_fields must be non-empty and unique")
        if any(not isinstance(code, str) or not code for code in self.context_error_codes):
            raise ValueError("context error codes must be non-empty strings")
        if self.context_error_format not in {"structured", "vllm"}:
            raise ValueError("unknown provider context error format")

    @classmethod
    def qwen(cls) -> OpenAIAdapterProfile:
        return cls(
            name="qwen-openai-compatible",
            thinking_style="qwen_chat_template",
            context_error_format="vllm",
        )

    def validate_options(self, model: ModelConfig, options: dict[str, JsonValue]) -> None:
        _finite_json(options)
        if model.thinking != "default" and self.thinking_style == "none":
            raise ValueError("this adapter profile cannot control thinking")
        template = options.get("chat_template_kwargs")
        if template is not None:
            if self.thinking_style != "qwen_chat_template" or not isinstance(template, dict):
                raise ValueError("chat_template_kwargs is only valid for the Qwen profile")
            enabled = template.get("enable_thinking")
            if enabled is not None and type(enabled) is not bool:
                raise ValueError("enable_thinking must be boolean")


@dataclass(slots=True)
class _ToolBuffer:
    call_id: str | None = None
    name: str | None = None
    arguments: str = ""
    emitted_chars: int = 0


def _finite_json(value: object) -> None:
    try:
        json.dumps(value, allow_nan=False, ensure_ascii=False)
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError("provider payload must be finite JSON") from exc


def _failure(
    request: ProviderRequest,
    *,
    code: str,
    message: str,
    retryable: bool,
    category: Literal["provider", "validation"] = "provider",
) -> FailureEnvelope:
    return FailureEnvelope(
        code=code,
        category=category,
        retryable=retryable,
        public_message=message,
        attempt_id=str(request.attempt_id),
        deadline_at=request.deadline_at,
    )


class OpenAICompatibleProvider(Provider):
    """Normalize an OpenAI chat-completions SSE stream into ProviderEvent."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        inputs: InputResolver,
        connections: ConnectionResolver,
        profile: OpenAIAdapterProfile | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._client = client
        self._inputs = inputs
        self._connections = connections
        self.profile = profile or OpenAIAdapterProfile()
        self._clock = clock

    def stream(
        self,
        request: ProviderRequest,
        *,
        cancel: CancelSignal | None = None,
    ) -> AsyncIterator[ProviderEvent]:
        return self._stream(request, cancel=cancel)

    async def _stream(
        self,
        request: ProviderRequest,
        *,
        cancel: CancelSignal | None,
    ) -> AsyncIterator[ProviderEvent]:
        sequence = 0
        try:
            resolved, connection = await asyncio.gather(
                self._inputs(request), self._connections(request)
            )
            self._validate_resolved(request, resolved)
            body = self._request_body(request, resolved)
            headers = {"Accept": "text/event-stream", "Content-Type": "application/json"}
            headers.update(connection.headers)
            if connection.api_key:
                headers["Authorization"] = f"Bearer {connection.api_key}"
            remaining = (request.deadline_at - self._aware_now()).total_seconds()
            if remaining <= 0:
                yield ProviderError(
                    attempt_id=request.attempt_id,
                    sequence=str(sequence),
                    kind="error",
                    failure=_failure(
                        request,
                        code="PROVIDER_TIMEOUT",
                        message="The model request deadline has expired.",
                        retryable=True,
                    ),
                )
                return
            url = connection.base_url.rstrip("/") + self.profile.endpoint_path
            timeout = httpx.Timeout(remaining, connect=min(remaining, 10.0))
            async with self._client.stream(
                "POST", url, headers=headers, json=body, timeout=timeout
            ) as response:
                if response.status_code < 200 or response.status_code >= 300:
                    retryable = response.status_code == 429 or response.status_code >= 500
                    context_limit = False
                    rejected_usage: ProviderUsage | None = None
                    if response.status_code in {400, 413, 422}:
                        # Error bodies are untrusted and potentially unbounded. Match
                        # structured codes or a profile's fully validated vendor grammar.
                        raw = bytearray()
                        async for chunk in response.aiter_bytes(chunk_size=4096):
                            raw.extend(chunk[: 16385 - len(raw)])
                            if len(raw) > 16384:
                                break
                        if len(raw) <= 16384:
                            try:
                                body_error = json.loads(raw)
                                error = (
                                    body_error.get("error")
                                    if isinstance(body_error, dict)
                                    else None
                                )
                                context_limit = isinstance(error, dict) and any(
                                    error.get(key) in self.profile.context_error_codes
                                    for key in ("code", "type")
                                )
                                if (
                                    not context_limit
                                    and self.profile.context_error_format == "vllm"
                                ):
                                    context_limit = vllm_context_error(error)
                                if context_limit and body_error.get("usage") is not None:
                                    rejected_usage = _OpenAIStreamParser(
                                        request, self.profile
                                    )._usage(body_error["usage"])
                            except (ValueError, UnicodeError):
                                pass
                    if rejected_usage is not None:
                        yield rejected_usage.model_copy(update={"sequence": str(sequence)})
                        sequence += 1
                    yield ProviderError(
                        attempt_id=request.attempt_id,
                        sequence=str(sequence),
                        kind="error",
                        failure=_failure(
                            request,
                            code="CONTEXT_LIMIT"
                            if context_limit
                            else ("DEPENDENCY_UNAVAILABLE" if retryable else "INVALID_INPUT"),
                            message=(
                                "The prepared input exceeds the provider context window."
                                if context_limit
                                else "The model provider is temporarily unavailable."
                                if retryable
                                else "The model provider rejected the request."
                            ),
                            retryable=retryable or context_limit,
                            category="provider" if retryable or context_limit else "validation",
                        ),
                    )
                    return
                parser = _OpenAIStreamParser(request, self.profile)
                async for payload in _sse_payloads(response, cancel=cancel):
                    if payload == "[DONE]":
                        parser.done_seen = True
                        terminal = parser.finalize()
                        if terminal is not None:
                            yield _with_sequence(terminal, sequence)
                            sequence += 1
                        continue
                    try:
                        chunk = json.loads(payload)
                    except (json.JSONDecodeError, RecursionError):
                        yield ProviderError(
                            attempt_id=request.attempt_id,
                            sequence=str(sequence),
                            kind="error",
                            failure=_failure(
                                request,
                                code="INCOMPLETE_MODEL_OUTPUT",
                                message="The model provider returned malformed stream data.",
                                retryable=False,
                            ),
                        )
                        return
                    try:
                        events = parser.feed(chunk)
                    except (TypeError, ValueError, KeyError):
                        yield ProviderError(
                            attempt_id=request.attempt_id,
                            sequence=str(sequence),
                            kind="error",
                            failure=_failure(
                                request,
                                code="INCOMPLETE_MODEL_OUTPUT",
                                message="The model provider returned an invalid stream event.",
                                retryable=False,
                            ),
                        )
                        return
                    for event in events:
                        yield _with_sequence(event, sequence)
                        sequence += 1
                if parser.pending_finish is not None and not parser.finished:
                    terminal = parser.finalize()
                    if terminal is not None:
                        yield _with_sequence(terminal, sequence)
                        sequence += 1
                elif not parser.finished:
                    reason = "cancelled" if cancel is not None and cancel.is_cancelled() else None
                    if reason == "cancelled":
                        yield ProviderFinish(
                            attempt_id=request.attempt_id,
                            sequence=str(sequence),
                            kind="finish",
                            reason="cancelled",
                            raw_reason="cancelled",
                        )
                    else:
                        yield ProviderError(
                            attempt_id=request.attempt_id,
                            sequence=str(sequence),
                            kind="error",
                            failure=_failure(
                                request,
                                code="INCOMPLETE_MODEL_OUTPUT",
                                message="The model stream ended without a finish reason.",
                                retryable=True,
                            ),
                        )
        except httpx.TimeoutException:
            yield ProviderError(
                attempt_id=request.attempt_id,
                sequence=str(sequence),
                kind="error",
                failure=_failure(
                    request,
                    code="PROVIDER_TIMEOUT",
                    message="The model provider did not answer before the deadline.",
                    retryable=True,
                ),
            )
        except httpx.RequestError:
            yield ProviderError(
                attempt_id=request.attempt_id,
                sequence=str(sequence),
                kind="error",
                failure=_failure(
                    request,
                    code="DEPENDENCY_UNAVAILABLE",
                    message="The model provider is unavailable.",
                    retryable=True,
                ),
            )
        except ValueError:
            yield ProviderError(
                attempt_id=request.attempt_id,
                sequence=str(sequence),
                kind="error",
                failure=_failure(
                    request,
                    code="INVALID_INPUT",
                    message="The resolved model request is invalid.",
                    retryable=False,
                    category="validation",
                ),
            )

    def _validate_resolved(self, request: ProviderRequest, resolved: ResolvedProviderInput) -> None:
        if resolved.input_ref != request.input_ref or resolved.input_digest != request.input_digest:
            raise ValueError("resolved provider input does not match the immutable request")
        requested = [tool.model_dump(mode="json", by_alias=True) for tool in request.tools]
        actual = [tool.ref.model_dump(mode="json", by_alias=True) for tool in resolved.tools]
        if requested != actual:
            raise ValueError("resolved tools do not match the requested capability revisions")
        if (request.output_schema is None) != (resolved.response_format is None):
            raise ValueError("resolved response format does not match the request")

    def _request_body(
        self, request: ProviderRequest, resolved: ResolvedProviderInput
    ) -> dict[str, object]:
        options = dict(request.model.provider_options)
        self.profile.validate_options(request.model, options)
        body: dict[str, object] = {
            "model": request.model.model_name,
            "messages": [dict(message) for message in resolved.messages],
            "stream": True,
            self.profile.max_tokens_field: request.model.max_output_tokens,
        }
        if self.profile.include_usage:
            body["stream_options"] = {"include_usage": True}
        if resolved.tools:
            body["tools"] = [dict(tool.definition) for tool in resolved.tools]
        if resolved.response_format is not None:
            body["response_format"] = dict(resolved.response_format)
        body.update(options)
        if (
            self.profile.thinking_style == "qwen_chat_template"
            and request.model.thinking != "default"
        ):
            template = dict(cast("dict[str, JsonValue]", body.get("chat_template_kwargs", {})))
            expected = request.model.thinking == "enabled"
            existing = template.get("enable_thinking")
            if existing is not None and existing is not expected:
                raise ValueError("thinking option conflicts with the pinned model configuration")
            template["enable_thinking"] = expected
            body["chat_template_kwargs"] = template
        _finite_json(body)
        return body

    def _aware_now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("provider clock must return a timezone-aware datetime")
        return now


class _OpenAIStreamParser:
    def __init__(self, request: ProviderRequest, profile: OpenAIAdapterProfile) -> None:
        self.request = request
        self.profile = profile
        self.tools: dict[int, _ToolBuffer] = {}
        self.finished = False
        self.done_seen = False
        self.pending_finish: ProviderFinish | None = None

    def feed(self, value: object) -> list[ProviderEvent]:
        if self.finished:
            raise ValueError("provider emitted data after finish")
        if not isinstance(value, dict):
            raise ValueError("provider chunk must be an object")
        if "error" in value:
            raise ValueError("provider error object")
        events: list[ProviderEvent] = []
        usage = value.get("usage")
        if usage is not None:
            events.append(self._usage(usage))
        choices = value.get("choices", [])
        if not isinstance(choices, list) or len(choices) > 1:
            raise ValueError("provider must return one streamed choice")
        if not choices:
            return events
        if self.pending_finish is not None:
            raise ValueError("provider emitted another choice after finish reason")
        choice = choices[0]
        if not isinstance(choice, dict):
            raise ValueError("provider choice must be an object")
        delta = choice.get("delta") or {}
        if not isinstance(delta, dict):
            raise ValueError("provider delta must be an object")
        for field_name in self.profile.analysis_fields:
            text = delta.get(field_name)
            if text is not None:
                if not isinstance(text, str):
                    raise ValueError("provider analysis delta must be text")
                if text:
                    events.append(self._analysis(text))
        content = delta.get("content")
        if content is not None:
            if not isinstance(content, str):
                raise ValueError("provider content delta must be text")
            if content:
                events.append(self._content(content))
        tool_calls = delta.get("tool_calls")
        if tool_calls is not None:
            if not isinstance(tool_calls, list):
                raise ValueError("provider tool calls must be a list")
            for call in tool_calls:
                events.extend(self._tool_delta(call))
        finish_reason = choice.get("finish_reason")
        if finish_reason is not None:
            if not isinstance(finish_reason, str):
                raise ValueError("provider finish reason must be text")
            if finish_reason == "tool_calls":
                events.extend(self._complete_tools())
            self.pending_finish = self._finish(finish_reason)
        return events

    def finalize(self) -> ProviderFinish | None:
        if self.finished:
            return None
        if self.pending_finish is None:
            return None
        self.finished = True
        return self.pending_finish

    def _usage(self, value: object) -> ProviderUsage:
        if not isinstance(value, dict):
            raise ValueError("provider usage must be an object")
        counts = (
            value.get("prompt_tokens"),
            value.get("completion_tokens"),
            value.get("total_tokens"),
        )
        if not all(type(item) is int and item >= 0 for item in counts):
            raise ValueError("provider usage counts must be non-negative integers")
        return ProviderUsage(
            attempt_id=self.request.attempt_id,
            sequence="0",
            kind="usage",
            usage=Usage(
                status="complete",
                reason="provider_reported",
                input_tokens=str(counts[0]),
                output_tokens=str(counts[1]),
                total_tokens=str(counts[2]),
            ),
        )

    def _analysis(self, text: str) -> AnalysisDelta:
        return AnalysisDelta(
            attempt_id=self.request.attempt_id,
            sequence="0",
            kind="analysis.delta",
            text=text,
        )

    def _content(self, text: str) -> ContentDelta:
        return ContentDelta(
            attempt_id=self.request.attempt_id,
            sequence="0",
            kind="content.delta",
            text=text,
        )

    def _tool_delta(self, value: object) -> list[ProviderEvent]:
        if not isinstance(value, dict) or type(value.get("index")) is not int:
            raise ValueError("provider tool delta requires an integer index")
        index = value["index"]
        if index < 0:
            raise ValueError("provider tool index must be non-negative")
        state = self.tools.setdefault(index, _ToolBuffer())
        call_id = value.get("id")
        if call_id is not None:
            if not isinstance(call_id, str) or not call_id:
                raise ValueError("provider tool id must be non-empty text")
            if state.call_id is not None and state.call_id != call_id:
                raise ValueError("provider tool id changed within one index")
            state.call_id = call_id
        function = value.get("function") or {}
        if not isinstance(function, dict):
            raise ValueError("provider tool function must be an object")
        name = function.get("name")
        if name is not None:
            if not isinstance(name, str) or not name:
                raise ValueError("provider tool name must be non-empty text")
            if state.name is not None and state.name != name:
                raise ValueError("provider tool name changed within one index")
            state.name = name
        arguments = function.get("arguments")
        if arguments is None:
            return []
        if not isinstance(arguments, str):
            raise ValueError("provider tool arguments must be text")
        state.arguments += arguments
        if not arguments or state.call_id is None:
            return []
        pending = state.arguments[state.emitted_chars :]
        if not pending:
            return []
        state.emitted_chars = len(state.arguments)
        return [
            ToolArgumentsDelta(
                attempt_id=self.request.attempt_id,
                sequence="0",
                kind="tool.arguments.delta",
                call_id=state.call_id,
                text=pending,
            )
        ]

    def _complete_tools(self) -> list[ProviderEvent]:
        if not self.tools:
            raise ValueError("tool_calls finish requires at least one tool")
        events: list[ProviderEvent] = []
        for index in sorted(self.tools):
            state = self.tools[index]
            if state.call_id is None or state.name is None:
                raise ValueError("provider tool call is missing id or name")
            arguments = json.loads(state.arguments or "{}", object_pairs_hook=_unique_pairs)
            if not isinstance(arguments, dict):
                raise ValueError("provider tool arguments must decode to an object")
            _finite_json(arguments)
            events.append(
                ToolCallComplete(
                    attempt_id=self.request.attempt_id,
                    sequence="0",
                    kind="tool.complete",
                    call_id=state.call_id,
                    name=state.name,
                    arguments=arguments,
                )
            )
        return events

    def _finish(self, raw: str) -> ProviderFinish:
        known = {"stop", "tool_calls", "length", "content_filter", "cancelled"}
        reason = raw if raw in known else "unknown"
        return ProviderFinish(
            attempt_id=self.request.attempt_id,
            sequence="0",
            kind="finish",
            reason=reason,  # type: ignore[arg-type]
            raw_reason=raw,
        )


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _with_sequence(event: ProviderEvent, sequence: int) -> ProviderEvent:
    return event.model_copy(update={"sequence": str(sequence)})


async def _sse_payloads(
    response: httpx.Response, *, cancel: CancelSignal | None
) -> AsyncIterator[str]:
    data: list[str] = []
    lines = response.aiter_lines().__aiter__()

    async def next_sse_line() -> str:
        return await anext(lines)

    while True:
        if cancel is not None and cancel.is_cancelled():
            return
        try:
            if cancel is None:
                line = await anext(lines)
            else:
                next_line = asyncio.create_task(next_sse_line())
                cancelled = asyncio.create_task(cancel.wait())
                try:
                    done, _ = await asyncio.wait(
                        {next_line, cancelled}, return_when=asyncio.FIRST_COMPLETED
                    )
                    if cancelled in done:
                        cancelled.result()
                        return
                    line = next_line.result()
                finally:
                    for task in (next_line, cancelled):
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(next_line, cancelled, return_exceptions=True)
        except StopAsyncIteration:
            break
        if not line:
            if data:
                yield "\n".join(data)
                data.clear()
            continue
        if line.startswith(":"):
            continue
        if line.startswith("data:"):
            data.append(line[5:].lstrip())
    if data:
        yield "\n".join(data)


__all__ = [
    "OpenAIAdapterProfile",
    "OpenAICompatibleProvider",
    "ProviderConnection",
    "ResolvedProviderInput",
    "ResolvedTool",
]

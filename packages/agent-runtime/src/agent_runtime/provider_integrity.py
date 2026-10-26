"""Attempt-local, bounded validation; never executes or authorizes a tool.

Limits are per single ``ModelAttempt`` (DS-P01-02): tool-argument bytes,
public content bytes and public analysis bytes are counted independently across the
whole attempt, so one channel cannot consume another channel's allowance. A streamed
JSON value is compared with the provider's completed arguments through canonical JSON
text, which preserves JSON type distinctions (``true`` vs ``1``, ``1`` vs ``1.0``) that
Python equality collapses.

Each rejection names a reason key in ``INTEGRITY_FAILURES``; DS-P01-03 promotes that
table into the reviewed error-code catalog. Do not add a reason key without a matching
catalog entry.
"""

import json
from collections.abc import Mapping
from types import MappingProxyType
from typing import NoReturn
from uuid import UUID

from agent_runtime.contracts.canonical import canonical_json
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.provider import (
    AnalysisDelta,
    ContentDelta,
    ProviderError,
    ProviderEvent,
    ProviderFinish,
    ToolArgumentsDelta,
    ToolCallComplete,
)

# Reason key -> (failure code, public message).
INTEGRITY_FAILURES: Mapping[str, tuple[str, str]] = MappingProxyType(
    {
        "after_termination": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Provider stream continued after its terminal event.",
        ),
        "sequence_mismatch": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Provider event sequence or attempt identity is inconsistent.",
        ),
        "argument_limit": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Tool arguments exceeded the per-attempt byte limit.",
        ),
        "content_limit": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Public content exceeded the per-attempt byte limit.",
        ),
        "analysis_limit": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Public analysis exceeded the per-attempt byte limit.",
        ),
        "call_limit": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Tool call count exceeded the per-attempt limit.",
        ),
        "late_parameters": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Tool parameters arrived twice or after completion.",
        ),
        "invalid_arguments": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Streamed tool arguments are not a valid JSON object.",
        ),
        "invalid_encoding": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Provider output is not valid UTF-8 text.",
        ),
        "argument_mismatch": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Completed tool arguments disagree with the streamed arguments.",
        ),
        "tool_calls_incomplete": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Model ended with tool calls that did not complete.",
        ),
        "bad_finish": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Model output ended without a successful finish reason.",
        ),
        "no_terminal": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Model stream ended without a valid terminal event.",
        ),
        "empty_output": (
            "INCOMPLETE_MODEL_OUTPUT",
            "Model finished without a non-empty answer.",
        ),
    }
)


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _constant(value: str) -> object:
    raise ValueError("non-finite JSON number")


def _numbers_match(streamed: object, complete: object) -> bool:
    """Compare numeric source tokens, which canonical JSON deliberately normalizes.

    RFC 8785 treats ``1`` and ``1.0`` as the same JSON number, but the provider stream
    and its completion are two encodings of one attempt: an integer that arrives as a
    float (or vice versa) is a stream inconsistency we still reject.
    """
    if isinstance(streamed, bool) or isinstance(complete, bool):
        return isinstance(streamed, bool) and isinstance(complete, bool) and streamed == complete
    if isinstance(streamed, int | float):
        return (
            isinstance(complete, int | float)
            and type(streamed) is type(complete)
            and streamed == complete
        )
    if isinstance(streamed, dict) and isinstance(complete, dict):
        return streamed.keys() == complete.keys() and all(
            _numbers_match(streamed[key], complete[key]) for key in streamed
        )
    if isinstance(streamed, list) and isinstance(complete, list):
        return len(streamed) == len(complete) and all(
            _numbers_match(left, right) for left, right in zip(streamed, complete, strict=True)
        )
    return True


def _utf8_size(text: str) -> int:
    """Raise ``UnicodeError`` for text that has no UTF-8 encoding (lone surrogates)."""
    return len(text.encode("utf-8"))


class ProviderIntegrity:
    """Validates one provider attempt; it does not authorize or execute tools."""

    def __init__(
        self,
        attempt_id: UUID,
        *,
        max_argument_bytes: int,
        max_content_bytes: int,
        max_analysis_bytes: int,
        max_calls: int,
    ) -> None:
        limits = {
            "max_argument_bytes": max_argument_bytes,
            "max_content_bytes": max_content_bytes,
            "max_analysis_bytes": max_analysis_bytes,
            "max_calls": max_calls,
        }
        for name, value in limits.items():
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        self.attempt_id = attempt_id
        self.max_argument_bytes = max_argument_bytes
        self.max_content_bytes = max_content_bytes
        self.max_analysis_bytes = max_analysis_bytes
        self.max_calls = max_calls
        self._next = 0
        self._argument_bytes = 0
        self._content_bytes = 0
        self._has_content = False
        self._analysis_bytes = 0
        self._arguments: dict[str, str] = {}
        self._calls: dict[str, ToolCallComplete] = {}
        self._finished = False
        self._invalid = False

    @property
    def next_sequence(self) -> int:
        """Sequence expected for the next normalized event."""
        return self._next

    def _reject(self, reason: str, *, retryable: bool = False) -> NoReturn:
        code, message = INTEGRITY_FAILURES[reason]
        self._invalid = True
        raise AgentRuntimeError(
            FailureEnvelope(
                code=code,
                category="provider",
                retryable=retryable,
                public_message=message,
                attempt_id=str(self.attempt_id),
            )
        )

    def _byte_size(self, text: str) -> int:
        """Count UTF-8 bytes; unencodable provider text is a domain rejection."""
        try:
            return _utf8_size(text)
        except UnicodeError:
            self._reject("invalid_encoding")

    def feed(self, event: ProviderEvent) -> None:
        if self._invalid or self._finished:
            self._reject("after_termination")
        if event.attempt_id != self.attempt_id or int(event.sequence) != self._next:
            self._reject("sequence_mismatch")
        self._next += 1
        if isinstance(event, ProviderError):
            self._invalid = True
            raise AgentRuntimeError(event.failure)
        if isinstance(event, ContentDelta):
            self._has_content |= bool(event.text.strip())
            self._content_bytes += self._byte_size(event.text)
            if self._content_bytes > self.max_content_bytes:
                self._reject("content_limit")
        elif isinstance(event, AnalysisDelta):
            self._analysis_bytes += self._byte_size(event.text)
            if self._analysis_bytes > self.max_analysis_bytes:
                self._reject("analysis_limit")
        elif isinstance(event, ToolArgumentsDelta | ToolCallComplete):
            self._feed_tool_event(event)
        if isinstance(event, ProviderFinish):
            if event.reason == "tool_calls":
                if not self._calls or set(self._calls) != set(self._arguments):
                    self._reject("tool_calls_incomplete")
            elif event.reason != "stop" or self._arguments:
                self._reject("bad_finish", retryable=event.reason == "length")
            elif not self._has_content:
                self._reject("empty_output", retryable=True)
            self._finished = True

    def _feed_tool_event(self, event: ToolArgumentsDelta | ToolCallComplete) -> None:
        if event.call_id in self._calls:
            self._reject("late_parameters")
        if event.call_id not in self._arguments:
            if len(self._arguments) >= self.max_calls:
                self._reject("call_limit")
            self._arguments[event.call_id] = ""
        if isinstance(event, ToolArgumentsDelta):
            self._argument_bytes += self._byte_size(event.text)
            if self._argument_bytes > self.max_argument_bytes:
                self._reject("argument_limit")
            self._arguments[event.call_id] += event.text
            return
        try:
            complete = canonical_json(event.arguments)
        except (ValueError, RecursionError, UnicodeError):
            self._reject("invalid_arguments")
        size = self._byte_size(complete)
        raw = self._arguments[event.call_id]
        if raw:
            # Streamed bytes were already counted; completion only verifies them.
            try:
                parsed = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_constant)
                streamed = canonical_json(parsed) if isinstance(parsed, dict) else None
            except (ValueError, RecursionError, UnicodeError):
                self._reject("invalid_arguments")
            if streamed != complete or not _numbers_match(parsed, event.arguments):
                self._reject("argument_mismatch")
        else:
            # Complete-only call: its canonical bytes are the counted content.
            self._argument_bytes += size
            if self._argument_bytes > self.max_argument_bytes:
                self._reject("argument_limit")
        self._calls[event.call_id] = event.model_copy(deep=True)

    def completed_calls(self) -> tuple[ToolCallComplete, ...]:
        if self._invalid or not self._finished:
            self._reject("no_terminal")
        return tuple(call.model_copy(deep=True) for call in self._calls.values())

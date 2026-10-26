"""Provider invocation boundary and attempt lifecycle convergence.

The adapter owns transport normalization.  This invoker owns preflight, stream
integrity and exactly one ModelAttempt lifecycle.  Retry and recovery policy remain
outside this module and must allocate a new attempt identity.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol

from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.model import ProviderCapabilities, ProviderRequest
from agent_runtime.contracts.provider import (
    ProviderError,
    ProviderEvent,
    ProviderFinish,
    ProviderUsage,
    ToolCallComplete,
)
from agent_runtime.contracts.results import Usage
from agent_runtime.ports import CancelSignal, Provider
from agent_runtime.provider_integrity import ProviderIntegrity
from agent_runtime.provider_preflight import OptionsValidator, validate_provider_request

Clock = Callable[[], datetime]
CapabilityResolver = Callable[[ProviderRequest], Awaitable[ProviderCapabilities]]
type AttemptStart = Literal["started", "already_running"]


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class ProviderLimits:
    max_argument_bytes: int = 1_048_576
    max_content_bytes: int = 8_388_608
    max_analysis_bytes: int = 8_388_608
    max_calls: int = 128

    def __post_init__(self) -> None:
        for name, value in (
            ("max_argument_bytes", self.max_argument_bytes),
            ("max_content_bytes", self.max_content_bytes),
            ("max_analysis_bytes", self.max_analysis_bytes),
            ("max_calls", self.max_calls),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class AttemptTerminal:
    status: str
    finish_reason: str | None
    usage: Usage
    failure: FailureEnvelope | None
    finished_at: datetime


class ModelAttemptStore(Protocol):
    async def begin(self, request: ProviderRequest, *, started_at: datetime) -> AttemptStart: ...
    async def finish(self, request: ProviderRequest, terminal: AttemptTerminal) -> None: ...


class ProviderObserver(Protocol):
    async def event(self, request: ProviderRequest, event: ProviderEvent) -> None: ...


class _ExplicitCancellation(Exception):
    """Internal control signal; never crosses the Provider boundary."""


async def _await_uninterrupted(task: asyncio.Future[None]) -> bool:
    """Finish an authoritative task despite repeated caller cancellation."""

    cancelled = False
    while True:
        try:
            await asyncio.shield(task)
            return cancelled
        except asyncio.CancelledError:
            if task.done():
                await task
                return True
            cancelled = True


async def _close_stream(iterator: AsyncIterator[ProviderEvent]) -> None:
    close = getattr(iterator, "aclose", None)
    if close is None:
        return
    closing = asyncio.ensure_future(close())
    if await _await_uninterrupted(closing):
        raise asyncio.CancelledError


async def _cancel_aware_events(
    stream: AsyncIterator[ProviderEvent],
    cancel: CancelSignal | None,
    *,
    deadline_at: datetime,
    clock: Clock,
) -> AsyncGenerator[ProviderEvent, None]:
    """Bound each provider read by the absolute deadline, excluding consumer wait."""

    iterator = stream.__aiter__()
    try:
        while True:
            if cancel is not None and cancel.is_cancelled():
                raise _ExplicitCancellation
            remaining = (deadline_at - clock()).total_seconds()
            if remaining <= 0:
                raise TimeoutError
            try:
                async with asyncio.timeout(remaining):
                    if cancel is None:
                        event = await anext(iterator)
                    else:
                        next_event = asyncio.ensure_future(anext(iterator))
                        cancelled = asyncio.create_task(cancel.wait())
                        try:
                            done, _ = await asyncio.wait(
                                {next_event, cancelled}, return_when=asyncio.FIRST_COMPLETED
                            )
                            if cancelled in done:
                                # Authority/deadline signals may fail rather than
                                # request a successful user cancellation.
                                cancelled.result()
                                raise _ExplicitCancellation
                            event = next_event.result()
                        finally:
                            for task in (next_event, cancelled):
                                if not task.done():
                                    task.cancel()
                            await asyncio.gather(next_event, cancelled, return_exceptions=True)
            except StopAsyncIteration:
                return
            yield event
    finally:
        await _close_stream(iterator)


def unknown_usage(
    reason: Literal["provider_not_reported", "collection_failed", "estimated"] = (
        "provider_not_reported"
    ),
) -> Usage:
    return Usage(
        status="unknown",
        reason=reason,
        input_tokens=None,
        output_tokens=None,
        total_tokens=None,
    )


class ProviderInvoker(Provider):
    """Validate and persist one provider call without adding an implicit retry."""

    def __init__(
        self,
        backend: Provider,
        *,
        capabilities: CapabilityResolver,
        attempts: ModelAttemptStore,
        validate_options: OptionsValidator | None = None,
        observer: ProviderObserver | None = None,
        clock: Clock = utcnow,
        limits: ProviderLimits | None = None,
    ) -> None:
        self._backend = backend
        self._capabilities = capabilities
        self._attempts = attempts
        self._validate_options = validate_options
        self._observer = observer
        self._clock = clock
        self._limits = limits or ProviderLimits()

    async def _record_terminal(
        self,
        request: ProviderRequest,
        *,
        status: str,
        finish_reason: str | None,
        usage: Usage | None,
        failure: FailureEnvelope | None,
    ) -> None:
        await self._attempts.finish(
            request,
            AttemptTerminal(
                status=status,
                finish_reason=finish_reason,
                usage=usage or unknown_usage(),
                failure=failure,
                finished_at=self._aware_now(),
            ),
        )

    async def _record_terminal_durable(
        self,
        request: ProviderRequest,
        *,
        status: str,
        finish_reason: str | None,
        usage: Usage | None,
        failure: FailureEnvelope | None,
    ) -> bool:
        write = asyncio.create_task(
            self._record_terminal(
                request,
                status=status,
                finish_reason=finish_reason,
                usage=usage,
                failure=failure,
            )
        )
        return await _await_uninterrupted(write)

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
        now = self._aware_now()
        capabilities = await self._capabilities(request)
        validate_provider_request(
            request,
            capabilities,
            now=now,
            validate_options=self._validate_options,
        )
        started = await self._attempts.begin(request, started_at=now)
        if started != "started":
            raise AgentRuntimeError(
                FailureEnvelope(
                    code="FENCED",
                    category="concurrency",
                    retryable=False,
                    public_message="The model attempt is already running.",
                    attempt_id=str(request.attempt_id),
                )
            )
        integrity = ProviderIntegrity(
            request.attempt_id,
            max_argument_bytes=self._limits.max_argument_bytes,
            max_content_bytes=self._limits.max_content_bytes,
            max_analysis_bytes=self._limits.max_analysis_bytes,
            max_calls=self._limits.max_calls,
        )
        usage: Usage | None = None
        finish: ProviderFinish | None = None
        buffered: list[ProviderEvent] = []
        observed: AsyncGenerator[ProviderEvent, None] | None = None
        terminal_state: Literal["none", "writing", "committed"] = "none"
        output_sequence = 0

        def output(event: ProviderEvent) -> ProviderEvent:
            nonlocal output_sequence
            normalized = event.model_copy(update={"sequence": str(output_sequence)})
            output_sequence += 1
            return normalized

        async def settle(
            *,
            status: str,
            finish_reason: str | None,
            failure: FailureEnvelope | None,
        ) -> None:
            nonlocal terminal_state
            terminal_state = "writing"
            was_cancelled = await self._record_terminal_durable(
                request,
                status=status,
                finish_reason=finish_reason,
                usage=usage,
                failure=failure,
            )
            terminal_state = "committed"
            if was_cancelled:
                raise asyncio.CancelledError

        try:
            stream = self._backend.stream(request, cancel=cancel)
            observed = _cancel_aware_events(
                stream, cancel, deadline_at=request.deadline_at, clock=self._aware_now
            )
            async for event in observed:
                if self._observer is not None:
                    await self._observer.event(request, event)
                if isinstance(event, ProviderUsage):
                    usage = event.usage
                if isinstance(event, ToolCallComplete) or buffered:
                    buffered.append(event)
                try:
                    integrity.feed(event)
                except AgentRuntimeError as exc:
                    if isinstance(event, ProviderFinish):
                        finish = event
                        status = "cancelled" if event.reason == "cancelled" else "failed"
                        await settle(
                            status=status,
                            finish_reason=event.reason,
                            failure=None if status == "cancelled" else exc.failure,
                        )
                        buffered.clear()
                        if status == "cancelled":
                            yield output(event)
                        else:
                            yield output(
                                ProviderError(
                                    attempt_id=request.attempt_id,
                                    sequence="0",
                                    kind="error",
                                    failure=exc.failure,
                                )
                            )
                        return
                    await settle(
                        status="failed",
                        finish_reason=None,
                        failure=exc.failure,
                    )
                    if isinstance(event, ProviderError):
                        buffered.clear()
                        yield output(event)
                        return
                    raise
                if isinstance(event, ProviderFinish):
                    finish = event
                    await settle(
                        status="completed",
                        finish_reason=event.reason,
                        failure=None,
                    )
                    if buffered:
                        for pending in buffered:
                            yield output(pending)
                        buffered.clear()
                    else:
                        yield output(event)
                    return
                if not buffered:
                    yield output(event)

            failure = FailureEnvelope(
                code="INCOMPLETE_MODEL_OUTPUT",
                category="provider",
                retryable=True,
                public_message="The model stream ended without a terminal event.",
                attempt_id=str(request.attempt_id),
            )
            await settle(
                status="failed",
                finish_reason=finish.reason if finish else None,
                failure=failure,
            )
            buffered.clear()
            yield output(
                ProviderError(
                    attempt_id=request.attempt_id,
                    sequence=str(integrity.next_sequence),
                    kind="error",
                    failure=failure,
                )
            )
        except _ExplicitCancellation:
            if terminal_state == "none":
                await settle(
                    status="cancelled",
                    finish_reason="cancelled",
                    failure=None,
                )
            buffered.clear()
            yield output(
                ProviderFinish(
                    attempt_id=request.attempt_id,
                    sequence=str(integrity.next_sequence),
                    kind="finish",
                    reason="cancelled",
                    raw_reason="cancelled",
                )
            )
        except TimeoutError:
            failure = FailureEnvelope(
                code="PROVIDER_TIMEOUT",
                category="provider",
                retryable=True,
                public_message="The model provider did not finish before the deadline.",
                attempt_id=str(request.attempt_id),
                deadline_at=request.deadline_at,
            )
            if terminal_state == "none":
                await settle(
                    status="failed",
                    finish_reason=finish.reason if finish else None,
                    failure=failure,
                )
            buffered.clear()
            yield output(
                ProviderError(
                    attempt_id=request.attempt_id,
                    sequence=str(integrity.next_sequence),
                    kind="error",
                    failure=failure,
                )
            )
        except GeneratorExit:
            if terminal_state == "none":
                terminal_state = "writing"
                await self._record_terminal_durable(
                    request,
                    status="interrupted",
                    finish_reason=None,
                    usage=unknown_usage("collection_failed"),
                    failure=None,
                )
                terminal_state = "committed"
            raise
        except asyncio.CancelledError:
            if terminal_state == "none":
                terminal_state = "writing"
                await self._record_terminal_durable(
                    request,
                    status="cancelled",
                    finish_reason="cancelled",
                    usage=usage,
                    failure=None,
                )
                terminal_state = "committed"
            raise
        except AgentRuntimeError as exc:
            if terminal_state == "none":
                await settle(
                    status="failed",
                    finish_reason=finish.reason if finish else None,
                    failure=exc.failure,
                )
            raise
        except Exception as exc:
            failure = FailureEnvelope(
                code="INTERNAL",
                category="internal",
                retryable=False,
                public_message="The provider invocation failed internally.",
                attempt_id=str(request.attempt_id),
            )
            if terminal_state == "none":
                await settle(
                    status="failed",
                    finish_reason=finish.reason if finish else None,
                    failure=failure,
                )
            raise AgentRuntimeError(failure) from exc
        finally:
            if observed is not None:
                await observed.aclose()

    def _aware_now(self) -> datetime:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("provider clock must return a timezone-aware datetime")
        return now

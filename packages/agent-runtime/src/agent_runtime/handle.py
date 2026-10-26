"""A read/control client handle, never the owner of an executing task."""

import asyncio
import math
from collections.abc import AsyncIterator
from contextlib import aclosing
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from agent_runtime.contracts.base import Revision
from agent_runtime.contracts.events import CursorFrame, EventEnvelope, ResyncFrame
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.requests import CommandReceipt, ControlRequest, StreamRequest
from agent_runtime.contracts.results import RunResult
from agent_runtime.contracts.transactions import ActorContext

type StreamFrame = EventEnvelope | CursorFrame | ResyncFrame


class EventSubscription(Protocol):
    def __aiter__(self) -> AsyncIterator[StreamFrame]: ...
    async def __anext__(self) -> StreamFrame: ...
    async def aclose(self) -> None: ...


class RunBackend(Protocol):
    """Implementations reauthorize reads/commands and never own execution here."""

    def events(self, actor: ActorContext, request: StreamRequest) -> EventSubscription: ...

    async def result(self, actor: ActorContext, session_id: UUID, run_id: UUID) -> RunResult: ...

    async def control(
        self, actor: ActorContext, session_id: UUID, request: ControlRequest
    ) -> CommandReceipt: ...


def _failure(code: str, message: str) -> AgentRuntimeError:
    return runtime_failure(code, category="delivery", public_message=message)


@dataclass(frozen=True, slots=True)
class RunHandle:
    backend: RunBackend
    actor: ActorContext
    session_id: UUID
    run_id: UUID

    async def events(self, after_cursor: Revision = "0") -> AsyncIterator[StreamFrame]:
        request = StreamRequest(
            session_id=self.session_id, after_seq=after_cursor, last_event_id=None
        )
        async with aclosing(self.backend.events(self.actor, request)) as subscription:
            async for frame in subscription:
                if not isinstance(frame, CursorFrame | ResyncFrame):
                    if frame.session_id != self.session_id:
                        raise _failure("IDENTITY_MISMATCH", "Unexpected session event")
                yield frame

    async def result(self, wait_timeout: float | None = None) -> RunResult:
        if wait_timeout is not None and (
            isinstance(wait_timeout, bool) or not math.isfinite(wait_timeout) or wait_timeout <= 0
        ):
            raise ValueError("wait_timeout must be finite and positive, or None")
        timer = asyncio.timeout(wait_timeout)
        try:
            async with timer:
                result = await self.backend.result(self.actor, self.session_id, self.run_id)
        except TimeoutError:
            if not timer.expired():
                raise
            raise _failure(
                "WAIT_TIMEOUT", "Waiting timed out; the run was not cancelled"
            ) from None
        if result.run_id != self.run_id:
            raise _failure("IDENTITY_MISMATCH", "Unexpected run result")
        return result

    async def control(self, request: ControlRequest) -> CommandReceipt:
        if request.run_id != self.run_id:
            raise ValueError("control request targets another run")
        receipt = await self.backend.control(self.actor, self.session_id, request)
        if receipt.run_id != self.run_id:
            raise _failure("IDENTITY_MISMATCH", "Unexpected command receipt")
        return receipt

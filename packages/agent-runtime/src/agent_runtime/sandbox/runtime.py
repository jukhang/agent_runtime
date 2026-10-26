"""Bounded execution operations over host-owned authority and sandbox connections."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Literal, Protocol
from uuid import UUID

from agent_runtime.contracts.failure import runtime_failure

if TYPE_CHECKING:
    from agent_runtime.sandbox.supervision import ExecutionLimits


@dataclass(frozen=True, slots=True)
class ExecutionBinding:
    scope_id: str
    execution_id: UUID
    generation: int
    workspace_generation: int
    image_digest: str
    sandbox_id: str

    def __post_init__(self) -> None:
        if not self.scope_id.strip() or not self.sandbox_id.strip():
            raise ValueError("execution scope and sandbox identity are required")
        if type(self.generation) is not int or type(self.workspace_generation) is not int:
            raise ValueError("execution generations must be integers")
        if min(self.generation, self.workspace_generation) < 1:
            raise ValueError("execution generations must be positive")
        digest = self.image_digest.rsplit("@sha256:", 1)
        if len(digest) != 2 or not digest[0] or len(digest[1]) != 64:
            raise ValueError("execution requires a digest-pinned image")
        if any(char not in "0123456789abcdef" for char in digest[1]):
            raise ValueError("invalid image digest")


@dataclass(frozen=True, slots=True)
class ExecutionHandle:
    binding: ExecutionBinding
    command_id: str

    def __post_init__(self) -> None:
        if not self.command_id.strip():
            raise ValueError("remote command identity is required")


@dataclass(frozen=True, slots=True)
class ExecutionStart:
    binding: ExecutionBinding
    command: str
    working_directory: str
    timeout_seconds: float
    deadline_at: datetime
    limits: ExecutionLimits | None = None

    def __post_init__(self) -> None:
        if not self.command.strip():
            raise ValueError("command is required")
        path = PurePosixPath(self.working_directory)
        if not path.is_absolute() or ".." in path.parts or "\x00" in str(path):
            raise ValueError("working directory must be an absolute normalized sandbox path")
        if not 0 < self.timeout_seconds <= 86400:
            raise ValueError("execution timeout must be finite, positive and <= 86400 seconds")
        if self.deadline_at.utcoffset() is None:
            raise ValueError("execution deadline must be timezone aware")


@dataclass(frozen=True, slots=True)
class RemoteStatus:
    running: bool | None
    exit_code: int | None


@dataclass(frozen=True, slots=True)
class ExecutionSnapshot:
    handle: ExecutionHandle
    state: Literal["running", "completed", "failed", "unknown"]
    exit_code: int | None = None

    @property
    def stopped(self) -> bool:
        return self.state in {"completed", "failed"}


@dataclass(frozen=True, slots=True)
class ExecutionLogs:
    content: str
    cursor: int
    channel: Literal["combined"] = "combined"


class ExecutionStartAuthority(Protocol):
    async def reserve(self, request: ExecutionStart) -> ExecutionHandle | None:
        """SQL/auth/fence check + durable dispatch intent; existing handle means reuse.

        Must reject an earlier unresolved intent, not return None and replay it.
        """
        ...

    async def started(self, request: ExecutionStart, handle: ExecutionHandle) -> None:
        """Fenced persistent handle write. Failure must leave the dispatch intent unresolved."""
        ...

    async def unknown(self, request: ExecutionStart) -> None:
        """Keep supervision durable; never delete/refund an ambiguous dispatch."""
        ...


class SandboxBackend(Protocol):
    @property
    def sandbox_id(self) -> str: ...

    async def start(self, request: ExecutionStart, timeout_seconds: float) -> str: ...
    async def status(self, command_id: str) -> RemoteStatus: ...
    async def logs(self, command_id: str, cursor: int) -> ExecutionLogs: ...
    async def interrupt(self, command_id: str) -> None: ...
    async def destroy(self) -> bool:
        """True only with confirmed deletion, not merely an accepted kill request."""
        ...

    async def aclose(self) -> None: ...


class SandboxExecutionRuntime:
    def __init__(
        self,
        backend: SandboxBackend,
        authority: ExecutionStartAuthority,
        *,
        request_timeout_seconds: float = 15,
        poll_seconds: float = 0.2,
        max_log_bytes: int = 1024 * 1024,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not 0 < request_timeout_seconds <= 300 or not 0 < poll_seconds <= 60:
            raise ValueError("sandbox API timeout/poll interval is invalid")
        if type(max_log_bytes) is not int or max_log_bytes < 1:
            raise ValueError("sandbox log bound must be a positive integer")
        self._backend = backend
        self._authority = authority
        self._timeout = request_timeout_seconds
        self._poll = poll_seconds
        self._log_limit = max_log_bytes
        self._clock = clock

    def _verify(self, binding: ExecutionBinding) -> None:
        if binding.sandbox_id != self._backend.sandbox_id:
            raise runtime_failure(
                "FORBIDDEN", category="authorization", public_message="Sandbox binding changed."
            )

    async def start(self, request: ExecutionStart) -> ExecutionHandle:
        self._verify(request.binding)
        existing = await self._authority.reserve(request)
        if existing is not None:
            if existing.binding != request.binding:
                raise runtime_failure(
                    "IDENTITY_MISMATCH",
                    category="concurrency",
                    public_message="Persisted execution identity changed.",
                )
            return existing
        remaining = (request.deadline_at - self._clock()).total_seconds()
        if remaining <= 0:
            raise runtime_failure(
                "BUDGET_EXHAUSTED", category="budget", public_message="Execution deadline expired."
            )
        try:
            async with asyncio.timeout(self._timeout):
                command = await self._backend.start(
                    request, min(request.timeout_seconds, remaining)
                )
            handle = ExecutionHandle(request.binding, command)
            await self._authority.started(request, handle)
            return handle
        except BaseException:
            # A local cancellation also leaves an ambiguous remote dispatch. A failed
            # unknown write is safe only because reserve already persisted the intent.
            try:
                await self._authority.unknown(request)
            except Exception:
                pass
            raise

    async def status(self, handle: ExecutionHandle) -> ExecutionSnapshot:
        self._verify(handle.binding)
        try:
            async with asyncio.timeout(self._timeout):
                state = await self._backend.status(handle.command_id)
        except Exception:
            return ExecutionSnapshot(handle, "unknown")
        if state.running is True:
            return ExecutionSnapshot(handle, "running")
        if state.running is False and type(state.exit_code) is int:
            return ExecutionSnapshot(
                handle, "completed" if state.exit_code == 0 else "failed", state.exit_code
            )
        return ExecutionSnapshot(handle, "unknown")

    async def cancel(self, handle: ExecutionHandle) -> ExecutionSnapshot:
        self._verify(handle.binding)
        try:
            async with asyncio.timeout(self._timeout):
                await self._backend.interrupt(handle.command_id)
        except Exception:
            pass
        return await self.status(handle)

    async def wait(self, handle: ExecutionHandle, *, timeout_seconds: float) -> ExecutionSnapshot:
        self._verify(handle.binding)
        if not 0 < timeout_seconds <= 86400:
            raise ValueError("wait timeout must be finite, positive and bounded")
        latest = ExecutionSnapshot(handle, "unknown")
        try:
            async with asyncio.timeout(timeout_seconds):
                while True:
                    latest = await self.status(handle)
                    if latest.stopped:
                        return latest
                    await asyncio.sleep(self._poll)
        except TimeoutError:
            return latest

    async def logs(self, handle: ExecutionHandle, *, cursor: int = 0) -> ExecutionLogs:
        self._verify(handle.binding)
        if type(cursor) is not int or cursor < 0:
            raise ValueError("log cursor must be a nonnegative integer")
        async with asyncio.timeout(self._timeout):
            logs = await self._backend.logs(handle.command_id, cursor)
        if logs.cursor < cursor or len(logs.content.encode("utf-8")) > self._log_limit:
            raise runtime_failure(
                "BUDGET_EXHAUSTED",
                category="tool",
                public_message="Execution logs exceeded their bound or returned an invalid cursor.",
            )
        return logs

    async def cleanup(self) -> bool:
        async with asyncio.timeout(self._timeout):
            return await self._backend.destroy()

    async def aclose(self) -> None:
        await self._backend.aclose()

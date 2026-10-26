"""Sandbox evidence boundaries; fake authority is deliberately not SQL acceptance."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.sandbox.runtime import (
    ExecutionBinding,
    ExecutionHandle,
    ExecutionLogs,
    ExecutionStart,
    RemoteStatus,
    SandboxExecutionRuntime,
)


def request():
    return ExecutionStart(
        ExecutionBinding("scope", uuid4(), 1, 1, "python@sha256:" + "a" * 64, "sandbox"),
        "python -c 'print(391)'",
        "/tmp",
        10,
        datetime.now(UTC) + timedelta(seconds=30),
    )


class Authority:
    saved = None
    unknown_count = 0
    fail_write = False
    deny = False

    async def reserve(self, request):
        if self.deny or self.unknown_count:
            raise PermissionError("unresolved or unauthorized")
        return self.saved

    async def started(self, request, handle):
        if self.fail_write:
            raise OSError("injected handle write loss")
        self.saved = handle

    async def unknown(self, request):
        self.unknown_count += 1


class Backend:
    sandbox_id = "sandbox"
    sends = 0
    closed = False
    interrupted = False
    observation = RemoteStatus(True, None)
    fail_start = False
    fail_status = False
    log_content = "hello"
    log_cursor = 5

    async def start(self, request, timeout_seconds):
        self.sends += 1
        assert 0 < timeout_seconds <= request.timeout_seconds
        if self.fail_start:
            raise TimeoutError("injected ambiguous dispatch")
        return "remote-command"

    async def status(self, command_id):
        if self.fail_status:
            raise OSError("no remote evidence")
        return self.observation

    async def interrupt(self, command_id):
        self.interrupted = True

    async def logs(self, command_id, cursor):
        return ExecutionLogs(self.log_content, self.log_cursor)

    async def destroy(self):
        return False

    async def aclose(self):
        self.closed = True


@pytest.mark.parametrize("fault", ("denied", "start", "persist", "reuse", "mismatch"))
def test_start_requires_durable_authority_and_never_replays_unknown(fault):
    async def scenario():
        backend, authority = Backend(), Authority()
        runtime = SandboxExecutionRuntime(backend, authority)
        req = request()
        if fault == "denied":
            authority.deny = True
            with pytest.raises(PermissionError):
                await runtime.start(req)
            assert backend.sends == 0
        elif fault in {"start", "persist"}:
            backend.fail_start = fault == "start"
            authority.fail_write = fault == "persist"
            with pytest.raises((TimeoutError, OSError)):
                await runtime.start(req)
            with pytest.raises(PermissionError):
                await runtime.start(req)
            assert backend.sends == 1 and authority.unknown_count == 1
        elif fault == "mismatch":
            authority.saved = ExecutionHandle(replace(req.binding, generation=2), "other")
            with pytest.raises(AgentRuntimeError):
                await runtime.start(req)
            assert backend.sends == 0
        else:
            first = await runtime.start(req)
            assert await runtime.start(req) == first and backend.sends == 1

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "observation,expected",
    [
        (RemoteStatus(True, None), "running"),
        (RemoteStatus(False, 0), "completed"),
        (RemoteStatus(False, 2), "failed"),
        (RemoteStatus(False, None), "unknown"),
        (RemoteStatus(None, 0), "unknown"),
    ],
)
def test_remote_status_requires_terminal_exit_evidence(observation, expected):
    async def scenario():
        backend = Backend()
        backend.observation = observation
        runtime = SandboxExecutionRuntime(backend, Authority())
        handle = ExecutionHandle(request().binding, "command")
        assert (await runtime.status(handle)).state == expected

    asyncio.run(scenario())


def test_cancel_wait_cleanup_and_close_do_not_invent_termination():
    async def scenario():
        backend = Backend()
        runtime = SandboxExecutionRuntime(backend, Authority(), poll_seconds=0.001)
        handle = ExecutionHandle(request().binding, "command")
        assert (await runtime.cancel(handle)).state == "running"
        assert backend.interrupted
        assert (await runtime.wait(handle, timeout_seconds=0.01)).state == "running"
        backend.fail_status = True
        assert (await runtime.status(handle)).state == "unknown"
        assert await runtime.cleanup() is False
        await runtime.aclose()
        assert backend.closed and backend.sends == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("fault", ("oversize", "cursor", "sandbox"))
def test_logs_do_not_silently_truncate_or_cross_sandboxes(fault):
    async def scenario():
        backend = Backend()
        runtime = SandboxExecutionRuntime(backend, Authority(), max_log_bytes=5)
        binding = request().binding
        if fault == "oversize":
            backend.log_content = "a" * 6
        elif fault == "cursor":
            backend.log_cursor = 0
        else:
            binding = replace(binding, sandbox_id="other")
        with pytest.raises(AgentRuntimeError):
            await runtime.logs(ExecutionHandle(binding, "command"), cursor=1)

    asyncio.run(scenario())


@pytest.mark.parametrize("seconds", (0, -1, float("inf"), float("nan")))
def test_execution_timeout_is_bounded(seconds):
    with pytest.raises(ValueError):
        replace(request(), timeout_seconds=seconds)

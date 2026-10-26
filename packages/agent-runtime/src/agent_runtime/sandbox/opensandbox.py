"""Thin bridge to pinned OpenSandbox; SDK objects never enter Runtime state."""

import base64
import json
import shlex
from datetime import timedelta
from importlib import import_module
from importlib.metadata import version
from pathlib import Path
from typing import Any

from agent_runtime.sandbox.runtime import (
    ExecutionLogs,
    ExecutionStart,
    RemoteStatus,
    SandboxBackend,
)
from agent_runtime.sandbox.supervision import ExecutionLimits, ExecutionUsage


def guard_command(mode: str, payload: dict[str, Any] | None = None) -> str:
    source = Path(__file__).with_name("guard.py").read_text()
    args = ["python", "-I", "-c", source, mode]
    if payload is not None:
        args.append(json.dumps(payload))
    return shlex.join(args)


class OpenSandboxBackend:
    """Own a connected SDK session; construct once per trusted sandbox, close explicitly.

    Dynamic imports keep the vendor optional. Any is confined to this SDK boundary;
    downstream Runtime and authority ports use immutable typed values.
    """

    def __init__(self, sandbox: Any, *, uid: int = 1000, gid: int = 1000) -> None:
        if type(uid) is not int or type(gid) is not int or min(uid, gid) < 1:
            raise ValueError("execution requires explicit non-root uid/gid")
        self._sandbox = sandbox
        self._uid = uid
        self._gid = gid

    @property
    def sandbox_id(self) -> str:
        return str(self._sandbox.id)

    async def start(self, request: ExecutionStart, timeout_seconds: float) -> str:
        if request.binding.sandbox_id != self.sandbox_id:
            raise ValueError("sandbox start identity mismatch")
        guarded = request.limits is not None
        if guarded and (
            self._uid != 1000 or self._gid != 1000 or request.working_directory != "/workspace"
        ):
            raise ValueError("guarded execution requires uid/gid 1000 and /workspace")
        command = request.command
        if request.limits is not None:
            command = guard_command(
                "start",
                {
                    "command": request.command,
                    "limits": request.limits.payload(),
                    "timeout": timeout_seconds,
                    "execution_id": str(request.binding.execution_id),
                },
            )
        opts = import_module("opensandbox.models.execd").RunCommandOpts(
            background=True,
            timeout=timedelta(seconds=timeout_seconds + 5 if guarded else timeout_seconds),
            working_directory=request.working_directory,
            uid=0 if guarded else self._uid,
            gid=0 if guarded else self._gid,
        )
        execution = await self._sandbox.commands.run(command, opts=opts)
        if not isinstance(execution.id, str) or not execution.id.strip():
            raise ValueError("OpenSandbox returned no command identity; dispatch is unknown")
        if guarded:
            await self._guard(
                "bind",
                {
                    "execution_id": str(request.binding.execution_id),
                    "command_id": execution.id,
                },
            )
        return execution.id

    async def status(self, command_id: str) -> RemoteStatus:
        state = await self._sandbox.commands.get_command_status(command_id)
        if state.id is not None and state.id != command_id:
            raise ValueError("OpenSandbox returned another command identity")
        return RemoteStatus(state.running, state.exit_code)

    async def logs(self, command_id: str, cursor: int) -> ExecutionLogs:
        logs = await self._sandbox.commands.get_background_command_logs(command_id, cursor)
        if type(logs.cursor) is not int or logs.cursor < cursor:
            raise ValueError("OpenSandbox returned no trustworthy log cursor")
        return ExecutionLogs(logs.content, logs.cursor)

    async def interrupt(self, command_id: str) -> None:
        # The root helper has no user-supplied command. Kill detached user processes
        # before interrupting execd's command group. Works after reconnect as well.
        await self._guard("stop", {"command_id": command_id})
        state = await self.status(command_id)
        if state.running is False and type(state.exit_code) is int:
            return
        await self._sandbox.commands.interrupt(command_id)

    async def _guard(self, mode: str, payload: dict[str, Any] | None = None) -> str:
        opts = import_module("opensandbox.models.execd").RunCommandOpts(
            timeout=timedelta(seconds=5),
            uid=0,
            gid=0,
        )
        execution = await self._sandbox.commands.run(guard_command(mode, payload), opts=opts)
        if execution.error:
            raise ValueError("trusted sandbox guard failed")
        text = "".join(part.text for part in execution.logs.stdout)
        if len(text.encode()) > 2 * 1024 * 1024:
            raise ValueError("guard response exceeded its bound")
        return text

    async def inspect_resources(self, payload: dict[str, Any]) -> ExecutionUsage:
        if payload.get("sandboxId") != self.sandbox_id:
            raise ValueError("resource inspector sandbox mismatch")
        ExecutionLimits.parse(payload["limits"])
        state = json.loads(await self._guard("inspect"))
        if state.get("limits") != payload["limits"]:
            raise ValueError("resource inspector authorized limits mismatch")
        if state.get("execution_id") != payload["executionId"]:
            raise ValueError("resource inspector execution mismatch")
        if state.get("command_id") != payload["remoteExecutionId"]:
            raise ValueError("resource inspector command mismatch")
        return ExecutionUsage(
            self.sandbox_id,
            payload["remoteExecutionId"],
            state["log_bytes"],
            state["workspace_bytes"],
            state["artifact_bytes"],
            state["artifact_count"],
            state["process_tree_stopped"],
            state.get("reason"),
        )

    async def read_output(self) -> bytes:
        data = base64.b64decode((await self._guard("output")).strip(), validate=True)
        if len(data) > 1024 * 1024:
            raise ValueError("guarded output exceeds its ceiling")
        return data

    async def destroy(self) -> bool:
        try:
            await self._sandbox.kill()
            await self._sandbox.get_info()
        except Exception as exc:
            # Accept only the SDK's typed 404, never a message/transport error.
            api_error = import_module("opensandbox.exceptions").SandboxApiException
            if isinstance(exc, api_error) and getattr(exc, "status_code", None) == 404:
                return True
            raise
        return False

    async def aclose(self) -> None:
        await self._sandbox.close()


async def connect_opensandbox(sandbox_id: str, connection: Any) -> OpenSandboxBackend:
    if version("opensandbox") != "0.1.16":
        raise ValueError("unverified OpenSandbox SDK; requires 0.1.16")
    sdk = import_module("opensandbox.sandbox")
    sandbox = await sdk.Sandbox.connect(
        sandbox_id, connection_config=connection, connect_timeout=timedelta(seconds=15)
    )
    return OpenSandboxBackend(sandbox)


async def confirm_opensandbox_deleted(sandbox_id: str, connection: Any) -> bool:
    """Reconcile lost deletion responses without treating network failure as absence."""
    try:
        backend = await connect_opensandbox(sandbox_id, connection)
    except Exception as exc:
        api_error = import_module("opensandbox.exceptions").SandboxApiException
        if isinstance(exc, api_error) and getattr(exc, "status_code", None) == 404:
            return True
        raise
    await backend.aclose()
    return False


async def inspect_opensandbox_resources(
    backend: SandboxBackend, payload: dict[str, Any]
) -> ExecutionUsage:
    """Direct injection into SqlSandboxMaintenanceWorker.inspect_resources."""
    if not isinstance(backend, OpenSandboxBackend):
        raise ValueError("OpenSandbox resource inspector requires its trusted backend")
    return await backend.inspect_resources(payload)

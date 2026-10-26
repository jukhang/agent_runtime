"""Stage verified Skill bytes and launch them through durable OpenSandbox authority."""

import hashlib
import shlex
from collections.abc import Awaitable, Callable
from datetime import datetime
from importlib import import_module
from pathlib import PurePosixPath
from typing import Any, Literal, Protocol

from agent_runtime.contracts.policy import PolicyAllow
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.tools import PendingExecution, ToolInvocation
from agent_runtime.sandbox.runtime import ExecutionBinding, ExecutionStart, SandboxExecutionRuntime
from agent_runtime.sandbox.supervision import ExecutionLimits
from agent_runtime.skills.git import GitSkillInstaller, VerifiedSkillBundle

type BindingResolver = Callable[[ToolInvocation, PolicyAllow], Awaitable[ExecutionBinding]]


class SkillFileEntryFactory(Protocol):
    def __call__(self, *, path: str, data: bytes, mode: int) -> Any: ...


def _opensandbox_entry(*, path: str, data: bytes, mode: int) -> Any:
    return import_module("opensandbox.models.filesystem").WriteEntry(
        path=path, data=data, mode=mode
    )


class OpenSandboxSkillRunner:
    """The host supplies a fresh, isolated sandbox and a fenced execution authority."""

    def __init__(
        self,
        *,
        sandbox: Any,
        runtime: SandboxExecutionRuntime,
        binding: BindingResolver,
        limits: ExecutionLimits,
        timeout_seconds: float,
        interpreter: Literal["python", "sh"],
        file_entry: SkillFileEntryFactory = _opensandbox_entry,
    ) -> None:
        interpreters = {"python": ("python", "-E", "-s"), "sh": ("sh",)}
        if interpreter not in interpreters:
            raise ValueError("Skill interpreter must be selected by the trusted host")
        self._sandbox, self._runtime = sandbox, runtime
        self._binding, self._limits, self._timeout = binding, limits, timeout_seconds
        self._file_entry = file_entry
        self._interpreter = interpreters[interpreter]

    async def start(
        self,
        invocation: ToolInvocation,
        grant: PolicyAllow,
        *,
        bundle: VerifiedSkillBundle,
        executable_path: str,
    ) -> PendingExecution:
        if invocation.operation_id is None:
            raise ValueError("Skill execution requires a durable operation identity")
        bundle = GitSkillInstaller().verify_bundle(
            VerifiedSkillBundle(bundle.spec, dict(bundle.files))
        )
        script = next(
            (entry for entry in bundle.spec.files if entry.path == executable_path), None
        )
        if script is None or not script.executable:
            raise ValueError("Skill execution target is not a verified executable")
        root = PurePosixPath("/workspace/skills") / str(invocation.operation_id)
        argv = invocation.call.arguments.get("argv", [])
        if (
            not isinstance(argv, list)
            or len(argv) > 64
            or any(not isinstance(arg, str) or "\x00" in arg or len(arg) > 4096 for arg in argv)
        ):
            raise ValueError("Skill script arguments must be bounded string argv")
        command = shlex.join((
            *self._interpreter, str(root / executable_path), *(str(arg) for arg in argv)
        ))
        binding = await self._binding(invocation, grant)
        if binding.scope_id != invocation.actor.scope_id or binding.sandbox_id != self._sandbox.id:
            raise ValueError("Skill sandbox binding differs from the authorized Run")
        entries = []
        for file in bundle.spec.files:
            data = bundle.files[file.path]
            if hashlib.sha256(data).hexdigest() != file.digest:
                raise ValueError("Skill bundle changed after verification")
            entries.append(
                self._file_entry(
                    path=str(root / file.path),
                    data=data,
                    mode=755 if file.executable else 644,
                )
            )
        await self._sandbox.files.write_files(entries)
        for file in bundle.spec.files:
            data = await self._sandbox.files.read_bytes(str(root / file.path))
            if hashlib.sha256(data).hexdigest() != file.digest:
                raise ValueError("Skill sandbox file differs from the pinned bundle")
        deadline: datetime = invocation.deadline_at
        handle = await self._runtime.start(
            ExecutionStart(
                binding=binding,
                command=command,
                working_directory="/workspace",
                timeout_seconds=self._timeout,
                deadline_at=deadline,
                limits=self._limits,
            )
        )
        return PendingExecution(
            tool_call_id=invocation.call.tool_call_id,
            run_id=invocation.run_id,
            operation_id=invocation.operation_id,
            status="pending_execution",
            execution_ref=ResourceRef(
                resource_type="execution",
                resource_id=str(handle.binding.execution_id),
                resource_version=str(handle.binding.generation),
                scope=ResourceScope(scope_id=invocation.actor.scope_id),
            ),
        )


__all__ = ["BindingResolver", "OpenSandboxSkillRunner"]

"""A Skill script can only run through a supervised sandbox execution port."""

from typing import Protocol
from uuid import UUID

from agent_runtime.contracts.policy import PolicyAllow
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.tools import PendingExecution, ToolInvocation
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.skills.git import VerifiedSkillBundle


class SandboxScriptRunner(Protocol):
    async def start(
        self,
        invocation: ToolInvocation,
        grant: PolicyAllow,
        *,
        bundle: VerifiedSkillBundle,
        executable_path: str,
    ) -> PendingExecution:
        """Stage verified files and launch an argv-based, supervised sandbox process."""


class SkillBundleLoader(Protocol):
    async def load(
        self, actor: ActorContext, run_id: UUID, ref: CapabilityRef
    ) -> VerifiedSkillBundle:
        """Load pinned bytes after current Run authority and revocation checks."""


class SkillScriptTool:
    def __init__(
        self,
        *,
        bundle: VerifiedSkillBundle,
        executable_path: str,
        sandbox: SandboxScriptRunner,
        available_tools: frozenset[CapabilityRef],
    ) -> None:
        file = next((item for item in bundle.spec.files if item.path == executable_path), None)
        if file is None or not file.executable or executable_path not in bundle.files:
            raise ValueError("Skill script is not a verified executable bundle file")
        if not set(bundle.spec.required_tools) <= available_tools:
            raise ValueError("Skill dependencies are not available to this Run")
        self._bundle, self._path, self._sandbox = bundle, executable_path, sandbox

    async def execute(self, invocation: ToolInvocation, grant: PolicyAllow) -> PendingExecution:
        return await self._sandbox.start(
            invocation, grant, bundle=self._bundle, executable_path=self._path
        )


class InstalledSkillScriptTool:
    """ToolHost adapter that never executes a process-cached Skill bundle."""

    def __init__(
        self,
        *,
        skill: CapabilityRef,
        loader: SkillBundleLoader,
        executable_path: str,
        sandbox: SandboxScriptRunner,
        available_tools: frozenset[CapabilityRef],
    ) -> None:
        self._skill, self._loader = skill, loader
        self._path, self._sandbox, self._tools = executable_path, sandbox, available_tools

    async def execute(self, invocation: ToolInvocation, grant: PolicyAllow) -> PendingExecution:
        bundle = await self._loader.load(invocation.actor, invocation.run_id, self._skill)
        if bundle.spec.ref != self._skill:
            raise ValueError("Loaded Skill differs from the installed script binding")
        return await SkillScriptTool(
            bundle=bundle, executable_path=self._path, sandbox=self._sandbox,
            available_tools=self._tools,
        ).execute(invocation, grant)


__all__ = [
    "InstalledSkillScriptTool", "SandboxScriptRunner", "SkillBundleLoader", "SkillScriptTool"
]

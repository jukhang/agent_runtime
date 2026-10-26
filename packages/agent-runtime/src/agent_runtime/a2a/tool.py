"""Only remotely idempotent A2A endpoints may use ToolHost's keyed effect mode."""

from typing import Protocol

from agent_runtime.contracts.registry import RemoteAgentSpec
from agent_runtime.contracts.tools import ToolInvocation
from agent_runtime.storage.tool_host import ToolEffect


class A2AKeyedExecution(Protocol):
    async def send_once(self, invocation: ToolInvocation, *, operation_key: str) -> ToolEffect:
        """Remote side must atomically dedup before creating a task."""

    async def lookup_by_key(
        self, invocation: ToolInvocation, *, operation_key: str
    ) -> ToolEffect | None:
        """Query prior task/effect without re-sending SendMessage."""


class A2AIdempotentTool:
    def __init__(self, *, spec: RemoteAgentSpec, remote: A2AKeyedExecution) -> None:
        if spec.idempotency != "provider_key" or not spec.can_query_task:
            raise ValueError("A2A keyed execution requires remote dedup and lookup")
        self._remote = remote

    async def dispatch(self, invocation: ToolInvocation, *, key: str) -> ToolEffect:
        if invocation.operation_id is None or key != str(invocation.operation_id):
            raise ValueError("A2A key differs from the persistent operation identity")
        return await self._remote.send_once(invocation, operation_key=key)

    async def lookup(self, invocation: ToolInvocation, *, key: str) -> ToolEffect | None:
        if invocation.operation_id is None or key != str(invocation.operation_id):
            raise ValueError("A2A lookup key differs from the persistent operation identity")
        return await self._remote.lookup_by_key(invocation, operation_key=key)


__all__ = ["A2AIdempotentTool", "A2AKeyedExecution"]

"""Pinned, ordered ToolHost hooks. Hooks cannot widen grants or mutate results."""

import asyncio
from collections.abc import Awaitable, Callable, Mapping

from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.registry import HookSpec
from agent_runtime.contracts.tools import ToolInvocation, ToolOutcome, ToolSpec
from agent_runtime.storage.tool_host import ToolEffect

type HookHandler = Callable[
    [ToolInvocation, ToolSpec, ToolEffect | ToolOutcome | None], Awaitable[None]
]
type HookFailureReporter = Callable[[HookSpec, ToolInvocation, Exception], Awaitable[None]]


class PinnedToolHooks:
    def __init__(
        self,
        specs: tuple[HookSpec, ...],
        handlers: Mapping[CapabilityRef, HookHandler],
        failure_reporter: HookFailureReporter | None = None,
    ) -> None:
        if len({spec.ref for spec in specs}) != len(specs):
            raise ValueError("duplicate pinned Tool hook")
        if any(spec.stage not in {"before_execute", "output", "terminal"} for spec in specs):
            raise ValueError("ToolHost cannot execute non-tool hook stages")
        if any(spec.replay != "pure" for spec in specs):
            raise ValueError("effectful hooks require a separate durable ledger")
        if any(spec.handler not in handlers for spec in specs):
            raise ValueError("pinned Tool hook handler is unavailable")
        if (
            any(spec.failure_policy == "notify_retry" for spec in specs)
            and failure_reporter is None
        ):
            raise ValueError("terminal notification hooks require a durable failure reporter")
        self._specs = tuple(
            sorted(specs, key=lambda item: (item.stage, item.order, str(item.ref.key.key)))
        )
        self._handlers = dict(handlers)
        self._reporter = failure_reporter

    @property
    def refs(self) -> frozenset[CapabilityRef]:
        return frozenset(spec.ref for spec in self._specs)

    async def _run(
        self,
        stage: str,
        invocation: ToolInvocation,
        spec: ToolSpec,
        outcome: ToolEffect | ToolOutcome | None,
    ) -> None:
        for hook in self._specs:
            if hook.stage != stage:
                continue
            try:
                async with asyncio.timeout(hook.timeout_seconds):
                    await self._handlers[hook.handler](invocation, spec, outcome)
            except Exception as exc:
                if hook.failure_policy != "notify_retry":
                    raise
                assert self._reporter is not None
                await self._reporter(hook, invocation, exc)

    async def before(self, invocation: ToolInvocation, spec: ToolSpec) -> None:
        await self._run("before_execute", invocation, spec, None)

    async def output(
        self, invocation: ToolInvocation, spec: ToolSpec, effect: ToolEffect
    ) -> None:
        for hook in self._specs:
            if hook.stage == "output":
                async with asyncio.timeout(hook.timeout_seconds):
                    await self._handlers[hook.handler](invocation, spec, effect)

    async def after(
        self, invocation: ToolInvocation, spec: ToolSpec, outcome: ToolOutcome
    ) -> None:
        await self._run("terminal", invocation, spec, outcome)


__all__ = ["HookFailureReporter", "HookHandler", "PinnedToolHooks"]

"""Bounded trusted read tools; writes/network execution require the full ToolHost."""

import asyncio
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Protocol

from pydantic import JsonValue
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.action_dispatch import ToolResourceResolver, ToolSpecResolver
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.ownership import ChildCommitGuard
from agent_runtime.contracts.policy import PolicyAllow, PolicyRequest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.tools import FullResult, ToolCompleted, ToolFailed, ToolInvocation
from agent_runtime.policy_validation import validate_policy_decision
from agent_runtime.storage.child_tools import SqlChildToolAuthorizer
from agent_runtime.storage.uow import errors


class ChildLocalReadTool(Protocol):
    async def invoke(self, db: AsyncSession, invocation: ToolInvocation) -> JsonValue:
        """Validate inputs/outputs; bounded local/SQL reads only, no network or writes."""
        ...


class SqlChildLocalTools:
    def __init__(
        self,
        engine: AsyncEngine,
        *,
        guard: Callable[[], ChildCommitGuard],
        authorizer: SqlChildToolAuthorizer,
        specs: ToolSpecResolver,
        resources: ToolResourceResolver,
        handlers: Mapping[CapabilityRef, ChildLocalReadTool],
    ) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._guard, self._authorizer = guard, authorizer
        self._specs, self._resources, self._handlers = specs, resources, dict(handlers)

    async def execute(
        self, invocation: ToolInvocation, grant: PolicyAllow
    ) -> ToolCompleted | ToolFailed:
        spec = await self._specs.get(invocation.call)
        handler = self._handlers.get(invocation.call.tool)
        if (
            handler is None
            or spec.kind != "read"
            or spec.side_effect != "none"
            or spec.idempotency != "read_only"
            or spec.public_result_policy != "full"
            or invocation.operation_id is not None
        ):
            errors.forbidden("child local executor only supports registered read-only tools")
        refs = await self._resources.resources(invocation, spec)
        validate_policy_decision(
            PolicyRequest(invocation=invocation, policy=spec.approval_policy, resources=refs),
            grant,
            now=datetime.now(UTC),
        )
        seconds = (invocation.deadline_at - datetime.now(UTC)).total_seconds()
        if seconds <= 0:
            errors.control_pending("child tool deadline elapsed")
        authorized = False
        try:
            async with asyncio.timeout(min(seconds, spec.timeout_seconds)):
                async with self._sessions() as db, db.begin():
                    await self._authorizer.require_in_transaction(
                        db, self._guard(), invocation, refs
                    )
                    authorized = True
                    data = await handler.invoke(db, invocation)
        except TimeoutError:
            if not authorized:
                raise
            return ToolFailed(
                tool_call_id=invocation.call.tool_call_id,
                run_id=invocation.run_id,
                operation_id=None,
                status="failed",
                effect="none",
                effect_refs=(),
                failure=FailureEnvelope(
                    code="TOOL_TIMEOUT",
                    category="tool",
                    retryable=True,
                    public_message="The read tool timed out before returning a result.",
                ),
            )
        except Exception:
            if not authorized:
                raise
            return ToolFailed(
                tool_call_id=invocation.call.tool_call_id,
                run_id=invocation.run_id,
                operation_id=None,
                status="failed",
                effect="none",
                effect_refs=(),
                failure=FailureEnvelope(
                    code="INTERNAL",
                    category="tool",
                    retryable=False,
                    public_message="The read tool failed without changing resources.",
                ),
            )
        return ToolCompleted(
            tool_call_id=invocation.call.tool_call_id,
            run_id=invocation.run_id,
            operation_id=None,
            status="completed",
            effect="none",
            effect_refs=(),
            result=FullResult(policy="full", data=data),
        )


__all__ = ["ChildLocalReadTool", "SqlChildLocalTools"]

"""Reject model-proposed actions that conflict with pinned runtime facts.

Passing this gate does not authorize a tool call or commit a plan. Those decisions
belong to Policy, ToolRuntime and the state transaction respectively.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ValidationError

from agent_runtime.contracts.context import PreparedContext
from agent_runtime.contracts.decisions import InvokeTools, NextAction, decision_adapter
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.loop import BudgetView, RunState
from agent_runtime.contracts.state import RunStatus


def validate_candidate_decision(
    state: RunState,
    context: PreparedContext,
    budget: BudgetView,
    candidate: NextAction | dict[str, Any],
    *,
    now: datetime,
) -> NextAction:
    """Return a typed candidate only when its read-only runtime bounds still hold."""

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if state.status != RunStatus.RUNNING or state.desired_control is not None:
        raise runtime_failure(
            "FENCED",
            category="concurrency",
            public_message="The run is not available for another model decision.",
        )
    if (
        state.scope_id != context.scope_id
        or state.session_id != context.session_id
        or state.run_id != context.run_id
    ):
        raise runtime_failure(
            "INVALID_INPUT",
            category="context",
            public_message="The decision context belongs to another run or scope.",
        )
    if context.budget.allocation_id != budget.allocation_id:
        raise runtime_failure(
            "INVALID_INPUT",
            category="budget",
            public_message="The decision uses a different budget allocation.",
        )
    if now >= budget.deadline_at:
        raise runtime_failure(
            "BUDGET_EXHAUSTED",
            category="budget",
            public_message="The run deadline has expired.",
        )
    try:
        payload = (
            candidate.model_dump(mode="json", by_alias=True)
            if isinstance(candidate, BaseModel)
            else candidate
        )
        action = decision_adapter.validate_python(payload)
    except ValidationError as exc:
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="The proposed action does not match the decision contract.",
        ) from exc
    if isinstance(action, InvokeTools):
        if len(action.payload.validated_calls) > budget.tool_calls.available:
            raise runtime_failure(
                "BUDGET_EXHAUSTED",
                category="budget",
                public_message="The proposed tool calls exceed the available budget.",
            )
        if any(
            all(call.tool != visible for visible in context.tools)
            for call in action.payload.validated_calls
        ):
            raise runtime_failure(
                "FORBIDDEN",
                category="authorization",
                public_message="The proposed tool is not visible in this context snapshot.",
            )
    return action

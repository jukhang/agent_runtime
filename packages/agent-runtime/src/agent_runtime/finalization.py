"""Budget views are projections; SQL purpose claim remains the paid-call authority."""

from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.loop import BudgetView


def decision_budget(budget: BudgetView, *, last_decision: bool) -> BudgetView:
    """Root and child loops select the same purpose; SQL still authorizes its claim."""
    if int(budget.closing_tokens) and (
        budget.model_calls.available < 1
        or last_decision
        or budget.tokens.available <= int(budget.closing_tokens)
        or (int(budget.tool_calls.limit) > 0 and budget.tool_calls.available == 0)
    ):
        return closing_budget(budget)
    return budget


def budget_guidance(budget: BudgetView) -> str:
    remaining = budget.tool_calls.available
    if not int(budget.closing_tokens) or remaining >= 4:
        return ""
    return (
        f"\nExecution budget: {remaining} tool calls remain. Converge on the current "
        "requested scope; do not start a new plan or broaden the task. Prefer bounded "
        "batch reads and combine compatible edits. Report saved, unverified and unfinished "
        "work accurately; never claim a proposal or read is a committed change. "
        "The closing model window is reserved separately and cannot fund tools."
    )


def closing_budget(budget: BudgetView) -> BudgetView:
    tokens = int(budget.closing_tokens)
    if not tokens or int(budget.model_calls.reserved) < 1 or int(budget.tokens.reserved) < tokens:
        raise runtime_failure(
            "BUDGET_EXHAUSTED",
            category="budget",
            public_message="No unused closing purpose remains.",
        )
    if int(budget.tokens.unknown_reserved) or int(budget.model_calls.unknown_reserved):
        raise runtime_failure(
            "USAGE_UNRESOLVED",
            category="budget",
            public_message="Unknown usage blocks closing model calls.",
        )
    return budget.model_copy(
        update={
            "closing": True,
            "model_calls": budget.model_calls.model_copy(
                update={"reserved": str(int(budget.model_calls.reserved) - 1)}
            ),
            "tokens": budget.tokens.model_copy(
                update={"reserved": str(int(budget.tokens.reserved) - tokens)}
            ),
        }
    )

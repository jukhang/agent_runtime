"""Model decisions are proposals; the runtime checks pinned facts before acting."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from agent_runtime.contracts.context import PreparedContext
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.loop import BudgetView, RunState
from agent_runtime.decision_validation import validate_candidate_decision

NOW = datetime(2026, 9, 28, tzinfo=UTC)
SESSION_ID = str(uuid4())
RUN_ID = str(uuid4())
ALLOCATION_ID = str(uuid4())
TOOL = {"key": {"namespace": "builtin", "key": "read"}, "revision": "1"}


def facts(*, tool_budget: int = 2):
    state = RunState.model_validate(
        {
            "scopeId": "scope",
            "sessionId": SESSION_ID,
            "runId": RUN_ID,
            "stateVersion": "1",
            "checkpointRef": "checkpoint",
            "agent": {"key": {"namespace": "builtin", "key": "agent"}, "revision": "1"},
            "status": "running",
            "desiredControl": None,
            "cancelVersion": "0",
            "pendingInputSeq": "0",
            "appliedInputSeq": "0",
        }
    )
    context = PreparedContext.model_validate(
        {
            "scopeId": "scope",
            "sessionId": SESSION_ID,
            "runId": RUN_ID,
            "authRevision": "1",
            "snapshotId": str(uuid4()),
            "contextVersion": "1",
            "payloadRef": "prompt-payload",
            "contentHash": "a" * 64,
            "model": {"key": {"namespace": "builtin", "key": "model"}, "revision": "1"},
            "prompt": {"key": {"namespace": "builtin", "key": "prompt"}, "revision": "1"},
            "tools": [TOOL],
            "budget": {
                "allocationId": ALLOCATION_ID,
                "maxInputTokens": 1000,
                "reservedOutputTokens": 100,
            },
            "estimatedInputTokens": 10,
            "sources": [],
        }
    )
    counter = {"limit": "2", "spent": "0", "reserved": "0", "unknownReserved": "0"}
    budget = BudgetView.model_validate(
        {
            "allocationId": ALLOCATION_ID,
            "revision": "1",
            "modelCalls": counter,
            "toolCalls": {**counter, "limit": str(tool_budget)},
            "tokens": {**counter, "limit": "1000"},
            "deadlineAt": (NOW + timedelta(minutes=1)).isoformat(),
        }
    )
    return state, context, budget


def tool_action(*, tool=TOOL, count=1):
    return {
        "kind": "invoke_tools",
        "payload": {
            "validatedCalls": [
                {"toolCallId": str(uuid4()), "tool": tool, "arguments": {"query": "hello"}}
                for _ in range(count)
            ]
        },
    }


def test_respond_and_visible_tool_are_valid_candidates():
    state, context, budget = facts()
    answer = validate_candidate_decision(
        state, context, budget, {"kind": "respond", "payload": {"answerRequestRef": "answer"}},
        now=NOW,
    )
    tool = validate_candidate_decision(state, context, budget, tool_action(), now=NOW)
    assert answer.kind == "respond"
    assert tool.kind == "invoke_tools"


@pytest.mark.parametrize(
    ("tool", "code"),
    [
        ({"key": {"namespace": "builtin", "key": "write"}, "revision": "1"}, "FORBIDDEN"),
        ({"key": {"namespace": "builtin", "key": "read"}, "revision": "2"}, "FORBIDDEN"),
    ],
)
def test_tool_must_match_exact_visible_revision(tool, code):
    state, context, budget = facts()
    with pytest.raises(AgentRuntimeError) as exc:
        validate_candidate_decision(state, context, budget, tool_action(tool=tool), now=NOW)
    assert exc.value.failure.code == code


def test_tool_budget_is_enforced_before_execution():
    state, context, budget = facts(tool_budget=1)
    with pytest.raises(AgentRuntimeError) as exc:
        validate_candidate_decision(state, context, budget, tool_action(count=2), now=NOW)
    assert exc.value.failure.code == "BUDGET_EXHAUSTED"


@pytest.mark.parametrize(
    "change", ["paused", "control", "scope", "session", "run", "allocation", "expired"]
)
def test_stale_or_foreign_facts_are_rejected(change):
    state, context, budget = facts()
    if change == "paused":
        state = state.model_copy(update={"status": "paused"})
    elif change == "control":
        state = state.model_copy(update={"desired_control": "pause"})
    elif change == "scope":
        context = context.model_copy(update={"scope_id": "another-scope"})
    elif change == "session":
        context = context.model_copy(update={"session_id": uuid4()})
    elif change == "run":
        context = context.model_copy(update={"run_id": uuid4()})
    elif change == "allocation":
        budget = budget.model_copy(update={"allocation_id": uuid4()})
    else:
        budget = budget.model_copy(update={"deadline_at": NOW})
    with pytest.raises(AgentRuntimeError):
        validate_candidate_decision(
            state, context, budget, {"kind": "respond", "payload": {"answerRequestRef": "a"}},
            now=NOW,
        )


def test_malformed_model_action_is_a_structured_rejection():
    state, context, budget = facts()
    with pytest.raises(AgentRuntimeError) as exc:
        validate_candidate_decision(
            state, context, budget, {"kind": "invoke_tools", "payload": {}}, now=NOW
        )
    assert exc.value.failure.code == "INVALID_INPUT"


def test_naive_clock_is_a_programming_error():
    state, context, budget = facts()
    with pytest.raises(ValueError, match="timezone-aware"):
        validate_candidate_decision(
            state, context, budget, {"kind": "respond", "payload": {"answerRequestRef": "a"}},
            now=datetime(2026, 9, 28),
        )

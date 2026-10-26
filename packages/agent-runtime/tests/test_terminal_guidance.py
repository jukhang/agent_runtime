import pytest

from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.payloads import RunCancelled
from agent_runtime.terminal_guidance import terminal_guidance


@pytest.mark.parametrize("category", ("provider", "authorization", "context", "budget", "tool"))
def test_failed_guidance_never_authorizes_automatic_retry(category):
    failure = FailureEnvelope(
        code="TEST_FAILURE", category=category, retryable=True, public_message="本轮无法完成。"
    )
    result = terminal_guidance("failed", failure=failure, retained_count=2)
    assert failure.public_message in result.message
    assert "2" in result.message
    assert result.next_steps and all(o.label.strip() for o in result.next_steps)
    assert all(o.action != "retry_same" for o in result.next_steps)


def test_unknown_effect_guidance_requires_verification():
    result = terminal_guidance(
        "failed",
        failure=FailureEnvelope(
            code="UNKNOWN_EFFECT", category="tool", retryable=False, public_message="工具结果未知。"
        ),
        unresolved_effects=True,
    )
    assert "请勿重复" in result.message
    assert result.next_steps[0].action == "manual_attention"


def test_unknown_effect_code_is_sufficient_even_without_an_execution_row():
    result = terminal_guidance(
        "failed",
        failure=FailureEnvelope(
            code="SIDE_EFFECT_UNKNOWN",
            category="tool",
            retryable=False,
            public_message="A previous dispatch needs reconciliation.",
        ),
    )
    assert "请勿重复" in result.message
    assert result.next_steps[0].action == "manual_attention"


def test_unknown_model_usage_guidance_requires_manual_settlement_not_more_budget():
    failure = FailureEnvelope(
        code="USAGE_UNRESOLVED",
        category="budget",
        retryable=False,
        public_message="Previous model usage is unresolved.",
    )
    result = terminal_guidance("failed", failure=failure, retained_count=1)
    assert failure.public_message in result.message
    assert result.next_steps[0].action == "manual_attention"
    assert "用量" in result.next_steps[0].label and "预留" in result.next_steps[0].label
    assert "调整任务范围" not in result.next_steps[0].label


@pytest.mark.parametrize("status", ("cancelled", "partial"))
def test_nonfailure_terminal_has_explicit_guidance(status):
    result = terminal_guidance(status, retained_count=1)
    assert "1" in result.message
    assert result.next_steps[0].action == "ask_user"


def test_historical_terminal_event_remains_readable():
    event = RunCancelled(
        state_version="2", final_item_ids=(), partial_result_refs=(), outcome=None, failure=None
    )
    assert event.public_message is None
    assert event.next_steps == ()


def test_failed_guidance_requires_reason():
    with pytest.raises(ValueError, match="structured reason"):
        terminal_guidance("failed")


def test_blank_public_reason_does_not_produce_empty_terminal():
    failure = FailureEnvelope(
        code="EMPTY_REASON", category="internal", retryable=False, public_message=" \n "
    )
    assert terminal_guidance("failed", failure=failure).message.strip()

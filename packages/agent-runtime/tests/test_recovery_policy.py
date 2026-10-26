from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta

import pytest

from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.state import DesiredControl, RunStatus
from agent_runtime.recovery_policy import RecoveryContext, propose_recovery

NOW = datetime(2026, 10, 10, tzinfo=UTC)


def context(**updates):
    return replace(
        RecoveryContext(
            stage="model",
            effect="none",
            attempt_count=1,
            max_attempts=3,
            model_calls_remaining=2,
            now=NOW,
            run_deadline=NOW + timedelta(minutes=5),
        ),
        **updates,
    )


def failure(**updates):
    return FailureEnvelope(
        **{
            "code": "PROVIDER_TIMEOUT",
            "category": "provider",
            "retryable": True,
            "public_message": "The model timed out.",
            **updates,
        }
    )


@pytest.mark.parametrize(
    "code",
    ("PROVIDER_TIMEOUT", "DEPENDENCY_UNAVAILABLE", "INCOMPLETE_MODEL_OUTPUT", "CONTEXT_LIMIT"),
)
@pytest.mark.parametrize("effect", ("none", "rolled_back"))
def test_transient_model_failure_proposes_retry(code, effect):
    result = propose_recovery(failure(code=code), context(effect=effect))
    assert result.kind == "retry"
    assert result.retry_at == NOW + timedelta(seconds=1)


@pytest.mark.parametrize(
    ("updates", "reason"),
    (
        ({"stage": "tool"}, "unsupported_failure"),
        ({"stage": "worker"}, "unsupported_failure"),
        ({"effect": "committed"}, "committed_effect"),
        ({"attempt_count": 3}, "attempts_exhausted"),
        ({"model_calls_remaining": 0}, "model_budget_exhausted"),
        ({"run_deadline": NOW}, "run_deadline"),
        ({"run_deadline": NOW + timedelta(seconds=1)}, "run_deadline"),
        ({"run_status": RunStatus.PAUSED}, "inactive_run"),
        ({"run_status": RunStatus.WAITING_USER}, "inactive_run"),
        ({"run_status": RunStatus.WAITING_TOOL}, "inactive_run"),
        ({"run_status": RunStatus.CANCELLING}, "inactive_run"),
    ),
)
def test_retry_limits_are_not_advisory(updates, reason):
    result = propose_recovery(failure(), context(**updates))
    assert (result.kind, result.reason, result.retry_at) == ("stop", reason, None)


@pytest.mark.parametrize("status", (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED))
def test_terminal_runs_do_not_restart(status):
    assert propose_recovery(failure(), context(run_status=status)).reason == "terminal_run"


@pytest.mark.parametrize("control", (DesiredControl.PAUSE, DesiredControl.CANCEL))
def test_control_precedes_reconciliation_and_retry(control):
    result = propose_recovery(failure(), context(effect="unknown", desired_control=control))
    assert result.kind == "control"
    assert result.retry_at is None


def test_unknown_effect_requires_reconciliation_not_replay():
    assert propose_recovery(failure(), context(effect="unknown")).kind == "reconcile"


@pytest.mark.parametrize(
    "updates",
    (
        {"retryable": False},
        {"code": "FORBIDDEN"},
        {"category": "authorization"},
        {"code": "FUTURE_PROVIDER_FAULT"},
        {"code": "VERSION_CONFLICT", "category": "concurrency"},
        {"code": "VERSION_CONFLICT", "category": "context"},
    ),
)
def test_producer_cannot_authorize_retries(updates):
    assert propose_recovery(failure(**updates), context()).kind == "stop"


def test_backoff_is_exponential_and_capped_without_integer_overflow():
    for attempt, seconds in ((1, 1), (2, 2), (8, 30), (10**9, 30)):
        result = propose_recovery(failure(), context(attempt_count=attempt, max_attempts=10**10))
        assert result.retry_at == NOW + timedelta(seconds=seconds)


def test_failed_attempt_deadline_does_not_override_run_deadline():
    original = failure(deadline_at=NOW - timedelta(seconds=1), detail_ref="detail:timeout")
    result = propose_recovery(original, context())
    assert result.kind == "retry"
    assert result.failure is original
    assert result.failure.detail_ref == "detail:timeout"


def test_failure_and_partial_results_are_preserved():
    from agent_runtime.contracts.references import ResourceRef, ResourceScope

    partial = ResourceRef(
        resource_type="artifact",
        resource_id="partial",
        resource_version="1",
        scope=ResourceScope(scope_id="scope", owner_id="user"),
    )
    original = failure(partial_result_refs=(partial,))
    result = propose_recovery(original, context(effect="unknown"))
    assert result.failure is original
    assert result.failure.partial_result_refs == original.partial_result_refs


@pytest.mark.parametrize(
    "updates",
    (
        {"attempt_count": 0},
        {"attempt_count": True},
        {"max_attempts": 0},
        {"model_calls_remaining": -1},
        {"now": NOW.replace(tzinfo=None)},
        {"run_deadline": NOW.replace(tzinfo=None)},
        {"base_backoff": timedelta(0)},
        {"max_backoff": timedelta(milliseconds=1)},
        {"stage": "unsafe"},
        {"effect": "unsafe"},
        {"run_status": "running"},
        {"desired_control": "resume"},
    ),
)
def test_invalid_trusted_inputs_fail_closed(updates):
    with pytest.raises(ValueError):
        context(**updates)


def test_context_is_immutable():
    value = context()
    with pytest.raises(FrozenInstanceError):
        value.attempt_count = 2

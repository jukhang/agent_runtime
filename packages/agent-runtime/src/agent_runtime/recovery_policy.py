"""Bounded recovery proposals; execution still requires a fresh SQL authorization."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.state import DesiredControl, RunStatus

type FailureStage = Literal["model", "tool", "worker"]
type EffectState = Literal["none", "rolled_back", "committed", "unknown"]
type RecoveryKind = Literal["retry", "control", "reconcile", "stop"]


@dataclass(frozen=True, slots=True)
class RecoveryContext:
    stage: FailureStage
    effect: EffectState
    attempt_count: int
    max_attempts: int
    model_calls_remaining: int
    now: datetime
    run_deadline: datetime
    run_status: RunStatus = RunStatus.RUNNING
    desired_control: DesiredControl | None = None
    base_backoff: timedelta = timedelta(seconds=1)
    max_backoff: timedelta = timedelta(seconds=30)

    def __post_init__(self) -> None:
        if self.stage not in {"model", "tool", "worker"}:
            raise ValueError("invalid failure stage")
        if self.effect not in {"none", "rolled_back", "committed", "unknown"}:
            raise ValueError("invalid effect state")
        for name, minimum in (
            ("attempt_count", 1),
            ("max_attempts", 1),
            ("model_calls_remaining", 0),
        ):
            value = getattr(self, name)
            if type(value) is not int or value < minimum:
                raise ValueError(f"{name} must be an integer >= {minimum}")
        if not isinstance(self.run_status, RunStatus):
            raise ValueError("run status must be a RunStatus")
        if self.desired_control is not None and not isinstance(
            self.desired_control, DesiredControl
        ):
            raise ValueError("control must be a DesiredControl")
        if any(value.utcoffset() is None for value in (self.now, self.run_deadline)):
            raise ValueError("recovery timestamps must be timezone aware")
        if self.base_backoff <= timedelta(0) or self.max_backoff < self.base_backoff:
            raise ValueError("backoff must be positive and bounded")


@dataclass(frozen=True, slots=True)
class RecoveryChoice:
    kind: RecoveryKind
    reason: str
    failure: FailureEnvelope
    retry_at: datetime | None = None


def propose_recovery(failure: FailureEnvelope, context: RecoveryContext) -> RecoveryChoice:
    """Do not infer authorization from retryable or a provider-supplied option."""
    if context.run_status in {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}:
        return RecoveryChoice("stop", "terminal_run", failure)
    if context.desired_control is not None:
        return RecoveryChoice("control", "control_pending", failure)
    if context.effect == "unknown":
        return RecoveryChoice("reconcile", "unknown_effect", failure)
    if context.effect == "committed":
        return RecoveryChoice("stop", "committed_effect", failure)
    if context.run_status != RunStatus.RUNNING:
        return RecoveryChoice("stop", "inactive_run", failure)
    if (
        context.stage != "model"
        or failure.category != "provider"
        or failure.code
        not in {
            "PROVIDER_TIMEOUT",
            "DEPENDENCY_UNAVAILABLE",
            "INCOMPLETE_MODEL_OUTPUT",
            "CONTEXT_LIMIT",
        }
    ):
        return RecoveryChoice("stop", "unsupported_failure", failure)
    if not failure.retryable:
        return RecoveryChoice("stop", "retry_disallowed", failure)
    if context.attempt_count >= context.max_attempts:
        return RecoveryChoice("stop", "attempts_exhausted", failure)
    if context.model_calls_remaining == 0:
        return RecoveryChoice("stop", "model_budget_exhausted", failure)
    # The failed attempt's deadline is not the Run's remaining execution budget.
    seconds = min(
        context.max_backoff.total_seconds(),
        context.base_backoff.total_seconds() * 2 ** min(context.attempt_count - 1, 60),
    )
    if seconds >= (context.run_deadline - context.now).total_seconds():
        return RecoveryChoice("stop", "run_deadline", failure)
    retry_at = context.now + timedelta(seconds=seconds)
    if retry_at >= context.run_deadline:
        return RecoveryChoice("stop", "run_deadline", failure)
    return RecoveryChoice("retry", "retry_scheduled", failure, retry_at)

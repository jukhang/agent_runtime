"""One serializable failure envelope, with a thin Python exception wrapper."""

from typing import Annotated, Literal

from pydantic import AwareDatetime, Field, StringConstraints

from agent_runtime.contracts.base import Contract, Identifier
from agent_runtime.contracts.references import ResourceRef

type FailureCategory = Literal[
    "validation",
    "authorization",
    "concurrency",
    "provider",
    "context",
    "storage",
    "tool",
    "budget",
    "compatibility",
    "delivery",
    "internal",
]
type RecoveryAction = Literal[
    "retry_same",
    "retry_with_compaction",
    "ask_user",
    "refresh_artifact",
    "reroute",
    "fallback_answer",
    "manual_attention",
]


class RecoveryOption(Contract):
    action: RecoveryAction
    label: Annotated[str, StringConstraints(min_length=1)]
    required_input_schema_ref: Identifier | None = None


class FailureEnvelope(Contract):
    schema_revision: Literal[1] = 1
    # Preserve future producer codes; consumers must not guess retry policy from a code.
    code: Annotated[str, StringConstraints(pattern=r"^[A-Z][A-Z0-9_]*$", max_length=128)]
    category: FailureCategory
    retryable: Annotated[bool, Field(strict=True)]
    public_message: Annotated[str, StringConstraints(min_length=1)]
    detail_ref: Identifier | None = None
    recovery_options: tuple[RecoveryOption, ...] = ()
    attempt_id: Identifier | None = None
    operation_id: Identifier | None = None
    deadline_at: AwareDatetime | None = None
    partial_result_refs: tuple[ResourceRef, ...] = ()


class AgentRuntimeError(Exception):
    def __init__(self, failure: FailureEnvelope) -> None:
        self.failure = failure
        super().__init__(failure.public_message)


def runtime_failure(
    code: str,
    *,
    category: FailureCategory,
    public_message: str,
    retryable: bool = False,
    detail_ref: Identifier | None = None,
    recovery_options: tuple[RecoveryOption, ...] = (),
    attempt_id: Identifier | None = None,
    operation_id: Identifier | None = None,
    deadline_at: AwareDatetime | None = None,
    partial_result_refs: tuple[ResourceRef, ...] = (),
) -> AgentRuntimeError:
    """Build the single failure type boundary validators raise (DS-P01-03).

    Domain rejections use this factory so every boundary check exposes one
    ``AgentRuntimeError(FailureEnvelope)``. Pure programming errors (for example a
    naive ``now`` argument) keep raising ``ValueError``; contract model validators keep
    raising ``ValueError`` so Pydantic reports them as ``ValidationError``.
    """
    return AgentRuntimeError(
        FailureEnvelope(
            code=code,
            category=category,
            retryable=retryable,
            public_message=public_message,
            detail_ref=detail_ref,
            recovery_options=recovery_options,
            attempt_id=attempt_id,
            operation_id=operation_id,
            deadline_at=deadline_at,
            partial_result_refs=partial_result_refs,
        )
    )

"""Stable failure classification for the SQL unit of work (DS-P02-02 requirement 4).

Every refusal is one of the frozen error codes; the code catalog itself is *not* extended
by this task. The mapping is deliberately coarse and stable so callers can branch on it:

======================  =========================================================
observed database fact  classification
======================  =========================================================
slot owner / run owner / owner_epoch / work owner / lease_epoch / expired lease
                        ``FENCED`` (category ``concurrency``)
expected state / cancel / resource version mismatch, terminal target
                        ``VERSION_CONFLICT`` (category ``concurrency``)
pending control blocks a normal transition
                        ``CONTROL_PENDING`` (category ``concurrency``)
actor authentication revision, revoked or expired grant
                        ``FORBIDDEN`` (category ``authorization``)
declared capacity would be exceeded
                        ``CAPACITY_UNAVAILABLE`` (category ``budget``, retryable)
cross-database / unregistered domain resource
                        ``FORBIDDEN`` (category ``authorization``)
======================  =========================================================
"""

from typing import NoReturn

from agent_runtime.contracts.failure import FailureEnvelope, RecoveryOption, runtime_failure

FENCED = "FENCED"
VERSION_CONFLICT = "VERSION_CONFLICT"
CONTROL_PENDING = "CONTROL_PENDING"
FORBIDDEN = "FORBIDDEN"
CAPACITY_UNAVAILABLE = "CAPACITY_UNAVAILABLE"
INVALID_INPUT = "INVALID_INPUT"


def fenced(detail: str) -> NoReturn:
    """The caller no longer owns the slot, run, work item or lease."""
    raise runtime_failure(
        FENCED,
        category="concurrency",
        public_message=f"The caller no longer owns this run or work item ({detail}).",
    )


def version_conflict(detail: str) -> NoReturn:
    """An expected state, cancel or resource version no longer matches."""
    raise runtime_failure(
        VERSION_CONFLICT,
        category="concurrency",
        public_message=f"The expected version no longer matches the stored fact ({detail}).",
    )


def control_pending(detail: str) -> NoReturn:
    """A pending pause/cancel command blocks this transition."""
    raise runtime_failure(
        CONTROL_PENDING,
        category="concurrency",
        public_message=f"A pending control command blocks this transition ({detail}).",
    )


def forbidden(detail: str) -> NoReturn:
    """Authorization, grant revision or destination scope refusal."""
    raise runtime_failure(
        FORBIDDEN,
        category="authorization",
        public_message=f"The caller is not authorized for this operation ({detail}).",
    )


def invalid_input(detail: str) -> NoReturn:
    """A shape or identity that cannot be reconciled with the stored facts."""
    raise runtime_failure(
        INVALID_INPUT,
        category="validation",
        public_message=f"The request cannot be reconciled with stored facts ({detail}).",
    )


def capacity_unavailable(detail: str) -> NoReturn:
    """Declared platform capacity would be exceeded; the caller may retry."""
    raise runtime_failure(
        CAPACITY_UNAVAILABLE,
        category="budget",
        public_message=f"Platform capacity is not available for this request ({detail}).",
        retryable=True,
        recovery_options=(RecoveryOption(action="retry_same", label="Retry when available"),),
    )


def authorization_failure(detail: str) -> FailureEnvelope:
    """Non-raising variant used by ``ControlRejected``."""
    return runtime_failure(
        FORBIDDEN,
        category="authorization",
        public_message=f"The caller is not authorized for this operation ({detail}).",
    ).failure


def conflict_failure(detail: str, *, code: str = VERSION_CONFLICT) -> FailureEnvelope:
    """Non-raising variant used by control/capacity rejections."""
    return runtime_failure(
        code,
        category="concurrency",
        public_message=f"The request conflicts with the stored fact ({detail}).",
    ).failure


__all__ = [
    "CAPACITY_UNAVAILABLE",
    "CONTROL_PENDING",
    "FENCED",
    "FORBIDDEN",
    "INVALID_INPUT",
    "VERSION_CONFLICT",
    "authorization_failure",
    "capacity_unavailable",
    "conflict_failure",
    "fenced",
    "forbidden",
    "invalid_input",
    "control_pending",
    "version_conflict",
]

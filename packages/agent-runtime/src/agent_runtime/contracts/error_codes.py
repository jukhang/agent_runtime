"""Reviewed failure-code catalog (DS-P01-03).

Every ``FailureEnvelope.code`` produced by this package, or used in a shared fixture,
must be registered here. ``FailureEnvelope`` deliberately still accepts unknown
producer codes so a newer upstream cannot be rejected by an older client, but the codes
this package emits are closed and audited by tests (``tests/test_error_codes.py``).
"""

from collections.abc import Iterable, Mapping
from types import MappingProxyType

INITIAL_REVIEW = "2026-09-17"

ERROR_CODES: Mapping[str, str] = MappingProxyType(
    {
        "AUDIT_UNAVAILABLE": "Authoritative audit or journal facts cannot be read or committed.",
        "BUDGET_EXHAUSTED": "A model, tool, execution or credential budget is exhausted.",
        "CAPACITY_UNAVAILABLE": (
            "Platform or scoped resource capacity is exhausted; the caller may retry."
        ),
        "CHECKPOINT_INCOMPATIBLE": (
            "A checkpoint representation cannot be read by this runtime build."
        ),
        "CONTEXT_LIMIT": "Prepared context or provider output exceeds a declared limit.",
        "CONTROL_PENDING": "A pending pause/cancel command blocks the requested transition.",
        "DELIVERY_EXPIRED": "A grant, approval or delivery window has expired or been revoked.",
        "DEPENDENCY_UNAVAILABLE": "A required external dependency is not available.",
        "FENCED": "The caller no longer owns the run, work item, slot or execution generation.",
        "FORBIDDEN": "Authorization or destination scope does not allow the operation.",
        "IDENTITY_MISMATCH": "A response, event or receipt belongs to another identity.",
        "INCOMPLETE_MODEL_OUTPUT": (
            "Model output is truncated, malformed or missing a valid terminal event."
        ),
        "INTERNAL": "An unexpected internal failure with no safer public classification.",
        "INVALID_INPUT": "Input violates a contract, digest or shape rule.",
        "PROVIDER_TIMEOUT": "The model provider did not answer within its deadline.",
        "SIDE_EFFECT_UNKNOWN": "An external side effect may have happened and cannot be confirmed.",
        "TOOL_TIMEOUT": "A tool did not finish within its deadline.",
        "USAGE_UNRESOLVED": "Unknown model usage reservations block further paid calls.",
        "VERSION_CONFLICT": "An expected version, revision or generation no longer matches.",
        "WAIT_TIMEOUT": "Waiting for a terminal result exceeded the caller's wait window.",
    }
)


def unknown_codes(used: Iterable[str]) -> tuple[str, ...]:
    """Return the sorted codes that are used but not registered in this catalog."""
    return tuple(sorted({code for code in used if code not in ERROR_CODES}))

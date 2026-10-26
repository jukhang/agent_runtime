"""Public foundational contracts."""

from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope, RecoveryOption
from agent_runtime.contracts.references import (
    CapabilityKey,
    CapabilityRef,
    ResourceRef,
    ResourceScope,
)
from agent_runtime.contracts.state import DesiredControl, RunStatus, WakeReason

# Event contracts are imported from agent_runtime.contracts.events to keep this
# foundational module free of circular dependencies with event payloads.

__all__ = [
    "AgentRuntimeError",
    "CapabilityKey",
    "CapabilityRef",
    "DesiredControl",
    "FailureEnvelope",
    "RecoveryOption",
    "ResourceRef",
    "ResourceScope",
    "RunStatus",
    "WakeReason",
]

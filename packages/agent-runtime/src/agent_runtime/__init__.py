"""Agent Runtime public API. Importing the package does not start services."""

from agent_runtime.action_dispatch import RunActionDispatcher
from agent_runtime.agent import Agent, Budget
from agent_runtime.agent_loop import DecisionFrame, DecisionSetup, ModelAgentLoop
from agent_runtime.contracts import (
    AgentRuntimeError,
    CapabilityKey,
    CapabilityRef,
    DesiredControl,
    FailureEnvelope,
    RecoveryOption,
    ResourceRef,
    ResourceScope,
    RunStatus,
    WakeReason,
)
from agent_runtime.contracts.events import EventEnvelope, event_adapter
from agent_runtime.contracts.model import ModelConfig, ProviderCapabilities, ProviderRequest
from agent_runtime.contracts.requests import (
    AcceptedTurn,
    CommandReceipt,
    ControlRequest,
    StreamRequest,
    SubmitRequest,
)
from agent_runtime.contracts.results import RunResult, result_adapter
from agent_runtime.contracts.views import SnapshotEnvelope
from agent_runtime.handle import RunBackend, RunHandle
from agent_runtime.policy_runtime import PolicyFacts, PolicyRuntime
from agent_runtime.runtime import Runtime, SubmittedRun

__all__ = [
    "Runtime", "SubmittedRun",
    "ModelConfig", "ProviderCapabilities", "ProviderRequest",
    "DecisionFrame", "DecisionSetup", "ModelAgentLoop", "PolicyFacts", "PolicyRuntime",
    "RunActionDispatcher",
    "RunBackend", "RunHandle",
    "AcceptedTurn", "CommandReceipt", "ControlRequest", "EventEnvelope", "RunResult",
    "SnapshotEnvelope", "StreamRequest", "SubmitRequest", "event_adapter", "result_adapter",
    "Agent", "AgentRuntimeError", "Budget", "CapabilityKey", "CapabilityRef", "DesiredControl",
    "FailureEnvelope", "RecoveryOption", "ResourceRef", "ResourceScope", "RunStatus", "WakeReason",
]

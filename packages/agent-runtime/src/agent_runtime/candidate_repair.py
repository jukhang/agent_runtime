"""Candidate defects are not permission grants or failed external effects."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Protocol
from uuid import UUID

from pydantic import Field

from agent_runtime.contracts.base import Contract
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.loop import RunState

if TYPE_CHECKING:
    from agent_runtime.agent_loop import DecisionFrame


class CandidateObservation(Contract):
    observation_id: UUID
    kind: Literal["candidate_rejected"] = "candidate_rejected"
    schema_revision: Literal[1] = 1
    run_id: UUID
    attempt_id: UUID
    snapshot_id: UUID
    tool_call_id: UUID | None
    reason: str
    fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    effect: Literal["none", "rolled_back"]
    total: int = Field(strict=True, ge=1, le=4)
    fingerprint_count: int = Field(strict=True, ge=1, le=2)


class CandidateRejected(AgentRuntimeError):
    def __init__(
        self,
        *,
        attempt_id: UUID,
        snapshot_id: UUID,
        reason: str,
        detail: str,
        tool_call_id: UUID | None = None,
    ):
        self.attempt_id, self.snapshot_id = attempt_id, snapshot_id
        if reason not in {
            "unknown_tool",
            "invalid_decision",
            "invalid_arguments",
            "invalid_evidence",
            "tool_rejected",
        }:
            raise ValueError("Unsupported candidate correction reason")
        self.reason, self.detail = reason, detail.replace("\x00", "")[:1000]
        self.tool_call_id = tool_call_id
        super().__init__(
            FailureEnvelope(
                code="INVALID_INPUT",
                category="validation",
                retryable=False,
                attempt_id=str(attempt_id),
                public_message="模型提出的操作尚未通过校验，本次候选未执行。",
            )
        )


class CandidateRepairs(Protocol):
    async def validate(self, state: RunState, frame: DecisionFrame) -> None: ...
    async def latest(self, state: RunState) -> CandidateRejected | None: ...
    async def record(self, state: RunState, rejection: CandidateRejected) -> RunState: ...

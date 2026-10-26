"""Trusted call/result pairing inside an authorized context payload."""

from typing import Self

from pydantic import model_validator

from agent_runtime.contracts.base import Contract
from agent_runtime.contracts.decisions import ToolCallRequest
from agent_runtime.contracts.tools import TerminalToolOutcome, ToolOutcome


class ToolTranscript(Contract):
    call: ToolCallRequest
    outcome: ToolOutcome

    @model_validator(mode="after")
    def matching_identity(self) -> Self:
        if self.call.tool_call_id != self.outcome.tool_call_id:
            raise ValueError("tool call and result identities differ")
        if not isinstance(self.outcome, TerminalToolOutcome):
            raise ValueError("nonterminal tool results cannot enter model history")
        if self.outcome.effect == "unknown":
            raise ValueError("unknown effects require reconciliation, not model continuation")
        return self

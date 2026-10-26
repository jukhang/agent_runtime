"""Trusted, host-owned delegation policy; never sourced from a model payload."""

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.message_payloads import ResultProposed
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.subagents import CollectRequest, VerifiedResult


@dataclass(frozen=True, slots=True)
class DelegationPolicy:
    agent: CapabilityRef
    agent_revision_id: UUID
    parent_allocation_id: UUID
    model_calls: int
    tool_calls: int
    tokens: int
    tools: tuple[CapabilityRef, ...]
    acceptance_spec: CapabilityRef
    output_schema_ref: str
    max_active_children: int = 4

    def __post_init__(self) -> None:
        for value in (self.model_calls, self.tool_calls, self.tokens, self.max_active_children):
            if type(value) is not int or not 0 <= value < 2**53:
                raise ValueError("delegation limits must be nonnegative safe integers")
        if not self.model_calls or not self.tokens or not self.max_active_children:
            raise ValueError("delegation requires model, token and concurrency budgets")

    @property
    def limits(self) -> dict[str, int]:
        return {
            "model_calls": self.model_calls,
            "tool_calls": self.tool_calls,
            "tokens": self.tokens,
        }


class AcceptanceVerifier(Protocol):
    async def verify_in_transaction(
        self, session: AsyncSession, request: CollectRequest, proposal: ResultProposed
    ) -> VerifiedResult:
        """Bounded DB/local validation; no model or network calls while locks are held."""
        ...


__all__ = ["AcceptanceVerifier", "DelegationPolicy"]

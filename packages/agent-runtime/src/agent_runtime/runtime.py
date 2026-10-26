"""Small host-facing client; accepting work never starts an in-process Agent task."""

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.requests import (
    AcceptedTurn,
    InteractionReceipt,
    InteractionRequest,
    SubmitRequest,
)
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.handle import RunBackend, RunHandle
from agent_runtime.ports import Admission


@dataclass(frozen=True, slots=True)
class SubmittedRun:
    accepted: AcceptedTurn
    handle: RunHandle


class InteractionPort(Protocol):
    async def submit(
        self, actor: ActorContext, request: InteractionRequest
    ) -> InteractionReceipt: ...


class Runtime:
    """Dependency-injected SDK entry. The host owns connections and Worker processes."""

    def __init__(
        self,
        *,
        admission: Admission,
        backend: RunBackend,
        interactions: InteractionPort | None = None,
    ) -> None:
        self._admission, self._backend = admission, backend
        self._interactions = interactions

    async def interact(
        self, actor: ActorContext, request: InteractionRequest
    ) -> InteractionReceipt:
        if self._interactions is None:
            raise runtime_failure(
                "INVALID_INPUT",
                category="validation",
                public_message="Human interaction is not installed in this host.",
            )
        receipt = await self._interactions.submit(actor, request)
        if (receipt.session_id, receipt.run_id) != (request.session_id, request.run_id):
            raise ValueError("Interaction returned another target")
        return receipt

    async def submit(self, actor: ActorContext, request: SubmitRequest) -> SubmittedRun:
        accepted = await self._admission.accept(actor, request)
        if accepted.session_id != request.session_id:
            raise ValueError("Admission returned another session")
        return SubmittedRun(accepted, self.attach(actor, accepted.session_id, accepted.run_id))

    def attach(self, actor: ActorContext, session_id: UUID, run_id: UUID) -> RunHandle:
        """Attaching is local; the backend reauthorizes every read and command."""
        return RunHandle(self._backend, actor, session_id, run_id)


__all__ = ["Runtime", "SubmittedRun"]

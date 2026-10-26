"""Read-only loop inputs. Scheduling and state persistence remain outside the loop."""

from typing import Self
from uuid import UUID

from pydantic import AwareDatetime, model_validator

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.messages import AgentEnvelope
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.state import DesiredControl, RunStatus


class BudgetCounter(Contract):
    limit: Revision
    spent: Revision
    reserved: Revision
    unknown_reserved: Revision

    @model_validator(mode="after")
    def conservative_reservation(self) -> Self:
        if int(self.unknown_reserved) > int(self.reserved):
            raise ValueError("unknown usage must remain part of the reservation")
        # Actual usage may exceed a reservation; report it rather than discarding it.
        return self

    @property
    def available(self) -> int:
        return max(0, int(self.limit) - int(self.spent) - int(self.reserved))


class BudgetView(Contract):
    allocation_id: UUID
    revision: Revision
    model_calls: BudgetCounter
    tool_calls: BudgetCounter
    tokens: BudgetCounter
    deadline_at: AwareDatetime


ACTIVE_CONTROL_STATUSES = frozenset(
    {
        RunStatus.QUEUED,
        RunStatus.RUNNING,
        RunStatus.WAITING_TOOL,
        RunStatus.WAITING_USER,
    }
)


class RunState(Contract):
    scope_id: Identifier
    session_id: UUID
    run_id: UUID
    state_version: Revision
    checkpoint_ref: Identifier
    agent: CapabilityRef
    status: RunStatus
    desired_control: DesiredControl | None
    cancel_version: Revision
    pending_input_seq: Revision
    applied_input_seq: Revision

    @model_validator(mode="after")
    def valid_watermark(self) -> Self:
        if int(self.pending_input_seq) < int(self.applied_input_seq):
            raise ValueError("applied input exceeds pending watermark")
        return self

    @model_validator(mode="after")
    def consistent_control(self) -> Self:
        """Mirror the state-write guard and the ``run`` table CHECKs.

        The loop reads this snapshot as its authority, so it must not be able to express
        a control lifecycle the storage transaction would refuse (DS-P01-08).
        """
        cancelling = self.status == RunStatus.CANCELLING
        if cancelling != (self.desired_control == DesiredControl.CANCEL):
            raise ValueError("cancelling state and cancel command must match")
        allowed = ACTIVE_CONTROL_STATUSES | {RunStatus.CANCELLING}
        if self.desired_control is not None and self.status not in allowed:
            raise ValueError("a pending control requires an active or cancelling run")
        return self


class AppliedInputs(Contract):
    run_id: UUID
    checkpoint_ref: Identifier
    previous_seq: Revision
    applied_seq: Revision
    messages: tuple[AgentEnvelope, ...]

    @model_validator(mode="after")
    def contiguous_inputs(self) -> Self:
        previous = int(self.previous_seq)
        if int(self.applied_seq) != previous + len(self.messages):
            raise ValueError("applied inputs cannot skip a sequence")
        for offset, message in enumerate(self.messages, start=1):
            if (
                message.recipient_run_id != self.run_id
                or int(message.recipient_seq) != previous + offset
            ):
                raise ValueError("applied input recipient or sequence mismatch")
        if len({item.message_id for item in self.messages}) != len(self.messages):
            raise ValueError("duplicate applied message")
        return self

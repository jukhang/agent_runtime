"""Child results are proposals until the parent verifies their acceptance evidence."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from agent_runtime.contracts.base import Contract, Identifier, MachineKey, Revision
from agent_runtime.contracts.references import CapabilityRef, ResourceRef
from agent_runtime.contracts.tasks import TaskPackage
from agent_runtime.contracts.transactions import CommitGuard


class SpawnRequest(Contract):
    parent_guard: CommitGuard
    task: TaskPackage
    idempotency_key: MachineKey


class ChildRunRef(Contract):
    scope_id: Identifier
    session_id: UUID
    parent_run_id: UUID
    child_run_id: UUID
    task_id: UUID
    agent: CapabilityRef
    budget_allocation_id: UUID
    workspace_id: UUID

    @model_validator(mode="after")
    def distinct_child(self) -> Self:
        if self.parent_run_id == self.child_run_id:
            raise ValueError("child run must differ from its parent")
        return self


class CollectRequest(Contract):
    parent_guard: CommitGuard
    child: ChildRunRef
    proposal_id: UUID
    expected_proposal_revision: Revision
    acceptance_spec: CapabilityRef

    @model_validator(mode="after")
    def parent_owns_collection(self) -> Self:
        lease = self.parent_guard.lease
        if (self.child.scope_id, self.child.session_id, self.child.parent_run_id) != (
            lease.scope_id,
            lease.session_id,
            lease.run_id,
        ):
            raise ValueError("only the owning parent may collect a child result")
        return self


class VerifiedResult(Contract):
    child: ChildRunRef
    proposal_id: UUID
    proposal_revision: Revision
    acceptance_spec: CapabilityRef
    disposition: Literal["accepted", "rejected"]
    evidence_refs: Annotated[tuple[ResourceRef, ...], Field(min_length=1)]
    result_refs: tuple[ResourceRef, ...]
    public_reason: Annotated[str, Field(min_length=1)]

    @model_validator(mode="after")
    def local_evidence(self) -> Self:
        for ref in (*self.evidence_refs, *self.result_refs):
            if ref.scope.scope_id != self.child.scope_id or ref.resource_version is None:
                raise ValueError("collection evidence requires same scope and pinned version")
        return self

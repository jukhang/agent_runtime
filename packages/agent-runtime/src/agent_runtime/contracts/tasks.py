"""Delegation requests reference resources, budgets and registered predicates."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, StringConstraints

from agent_runtime.contracts.base import Contract, Identifier
from agent_runtime.contracts.references import CapabilityRef, ResourceRef

NonblankText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class ValidationEvidence(Contract):
    status: Literal["passed", "failed", "unknown"]
    evidence_refs: tuple[ResourceRef, ...]


class TaskPackage(Contract):
    task_id: UUID
    target_agent: CapabilityRef
    objective: NonblankText
    constraints: tuple[str, ...]
    acceptance_spec_ref: Identifier
    output_schema_ref: Identifier
    resource_refs: tuple[ResourceRef, ...]
    budget_allocation_id: UUID
    deadline_at: AwareDatetime


class WaitSpec(Contract):
    kind: Literal["tool", "approval", "message", "timer", "external"]
    subject_id: Identifier
    deadline_at: AwareDatetime
    resume_predicate: CapabilityRef

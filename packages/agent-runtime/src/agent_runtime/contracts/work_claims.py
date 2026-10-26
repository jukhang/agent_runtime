"""Execution lanes share a scheduler but cannot interchange ownership tokens."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, TypeAdapter, model_validator

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.ownership import ChildLeaseToken
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.scheduler import ClaimedWork, ClaimRequest, IdleWork, LostLease
from agent_runtime.contracts.transactions import LeaseToken, MaintenanceLease

type MaintenanceKind = Literal[
    "reconcile_execution",
    "migrate_checkpoint",
    "rebuild_projection",
    "recover_scope",
    "collect_garbage",
    "evaluate",
    "domain_job",
]


class WorkClaimRequest(ClaimRequest):
    lanes: Annotated[
        tuple[Literal["main", "child", "control", "maintenance"], ...], Field(min_length=1)
    ]
    maintenance_kinds: tuple[MaintenanceKind, ...] = ()

    @model_validator(mode="after")
    def distinct_lanes(self) -> Self:
        if len(self.lanes) != len(set(self.lanes)) or len(self.maintenance_kinds) != len(
            set(self.maintenance_kinds)
        ):
            raise ValueError("duplicate worker lane or maintenance kind")
        if ("maintenance" in self.lanes) != bool(self.maintenance_kinds):
            raise ValueError("maintenance claims must explicitly select kinds")
        return self


class ChildClaimedWork(Contract):
    disposition: Literal["child_claimed"]
    lease: ChildLeaseToken
    root_run_id: UUID
    root_cancel_version: Revision
    state_version: Revision
    checkpoint_id: UUID | None
    budget_allocation_id: UUID
    workspace_ref: ResourceRef
    workspace_generation: Revision

    @model_validator(mode="after")
    def isolated_child(self) -> Self:
        if self.root_run_id == self.lease.run_id:
            raise ValueError("child cannot be the root run")
        if (
            self.workspace_ref.resource_type != "workspace"
            or self.workspace_ref.scope.scope_id != self.lease.scope_id
        ):
            raise ValueError("child workspace requires matching scope")
        return self


class MaintenanceClaimedWork(Contract):
    disposition: Literal["maintenance_claimed"]
    lease: MaintenanceLease
    kind: MaintenanceKind
    subject_ref: ResourceRef
    payload_ref: Identifier

    @model_validator(mode="after")
    def local_subject(self) -> Self:
        if self.subject_ref.scope.scope_id != self.lease.scope_id:
            raise ValueError("maintenance subject scope mismatch")
        return self


class ControlClaimedWork(Contract):
    disposition: Literal["control_claimed"]
    lease: LeaseToken | ChildLeaseToken
    state_version: Revision
    command_id: UUID
    desired_control: Literal["pause", "cancel"]
    status: Literal["queued", "running", "waiting_tool", "waiting_user", "cancelling"]
    cancel_version: Revision
    checkpoint_id: UUID | None

    @model_validator(mode="after")
    def cancellation_is_not_normal_work(self) -> Self:
        if (self.status == "cancelling") != (self.desired_control == "cancel"):
            raise ValueError("cancel claim must preserve cancelling state")
        return self


type WorkClaimResult = Annotated[
    ClaimedWork | ChildClaimedWork | MaintenanceClaimedWork | ControlClaimedWork | IdleWork,
    Field(discriminator="disposition"),
]
work_claim_adapter: TypeAdapter[WorkClaimResult] = TypeAdapter(WorkClaimResult)


class ChildRenewRequest(Contract):
    lease: ChildLeaseToken
    lease_seconds: Annotated[int, Field(strict=True, ge=1, le=3600)]


class MaintenanceRenewRequest(Contract):
    lease: MaintenanceLease
    lease_seconds: Annotated[int, Field(strict=True, ge=1, le=3600)]


class ChildRenewed(Contract):
    disposition: Literal["renewed"]
    lease: ChildLeaseToken


class MaintenanceRenewed(Contract):
    disposition: Literal["renewed"]
    lease: MaintenanceLease


type ChildRenewResult = Annotated[ChildRenewed | LostLease, Field(discriminator="disposition")]
type MaintenanceRenewResult = Annotated[
    MaintenanceRenewed | LostLease, Field(discriminator="disposition")
]
child_renew_adapter: TypeAdapter[ChildRenewResult] = TypeAdapter(ChildRenewResult)
maintenance_renew_adapter: TypeAdapter[MaintenanceRenewResult] = TypeAdapter(MaintenanceRenewResult)

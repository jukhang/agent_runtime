"""Validate scheduler responses before relying on their claimed identities.

DS-P01-03: domain rejections raise ``AgentRuntimeError(FailureEnvelope)`` through
``runtime_failure``; claims still require database revalidation.
"""

from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.scheduler import ClaimedWork, IdleWork
from agent_runtime.contracts.work_claims import (
    ChildClaimedWork,
    ChildRenewed,
    ChildRenewRequest,
    ChildRenewResult,
    ControlClaimedWork,
    MaintenanceClaimedWork,
    MaintenanceRenewed,
    MaintenanceRenewRequest,
    MaintenanceRenewResult,
    WorkClaimRequest,
    WorkClaimResult,
)


def validate_work_claim(request: WorkClaimRequest, result: WorkClaimResult) -> None:
    if isinstance(result, IdleWork):
        return
    lane = {
        ClaimedWork: "main",
        ChildClaimedWork: "child",
        ControlClaimedWork: "control",
        MaintenanceClaimedWork: "maintenance",
    }.get(type(result))
    if lane is None:
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="The scheduler returned an unknown execution lane.",
        )
    if lane not in request.lanes:
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="The scheduler returned an unrequested execution lane.",
        )
    if (result.lease.scope_id, result.lease.owner_id) != (
        request.scope_id,
        request.worker.worker_id,
    ):
        raise runtime_failure(
            "FORBIDDEN",
            category="authorization",
            public_message="The scheduler returned work for a different scope or worker.",
        )
    if isinstance(result, MaintenanceClaimedWork) and result.kind not in request.maintenance_kinds:
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="The scheduler returned an unrequested maintenance kind.",
        )


def validate_child_renew(request: ChildRenewRequest, result: ChildRenewResult) -> None:
    if not isinstance(result, ChildRenewed):
        return
    clocks = {"run_lease_until", "work_lease_until"}
    if request.lease.model_dump(exclude=clocks) != result.lease.model_dump(exclude=clocks):
        raise runtime_failure(
            "FENCED",
            category="concurrency",
            public_message="A child renewal cannot transfer execution authority.",
        )
    if (
        result.lease.run_lease_until < request.lease.run_lease_until
        or result.lease.work_lease_until < request.lease.work_lease_until
    ):
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="A child renewal cannot shorten the observed leases.",
        )


def validate_maintenance_renew(
    request: MaintenanceRenewRequest, result: MaintenanceRenewResult
) -> None:
    if not isinstance(result, MaintenanceRenewed):
        return
    if request.lease.model_dump(exclude={"work_lease_until"}) != result.lease.model_dump(
        exclude={"work_lease_until"}
    ):
        raise runtime_failure(
            "FENCED",
            category="concurrency",
            public_message="A maintenance renewal cannot transfer execution authority.",
        )
    if result.lease.work_lease_until < request.lease.work_lease_until:
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="A maintenance renewal cannot shorten the observed lease.",
        )

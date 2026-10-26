"""Database-fact revalidation: the stored row is the authority, never the DTO.

Every check here reads the *locked row* and the caller's claim, and fails closed with the
frozen classification. A request DTO can only express expectations.
"""

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.storage.uow import errors

TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})
#: Statuses in which a run holds (or may hold) a worker lease.
LEASED_STATUSES = frozenset({"running", "waiting_tool", "waiting_user", "cancelling"})
ACTIVE_STATUSES = frozenset({"queued", "running", "waiting_tool", "waiting_user"})


def is_terminal(status: str) -> bool:
    return status in TERMINAL_STATUSES


def verify_slot(slot: Mapping[str, Any] | None, lease: LeaseToken) -> None:
    """The session slot must still belong to this run at the claimed version."""
    if slot is None:
        errors.fenced("session slot does not exist")
    if slot["owner_run_id"] != lease.run_id:
        errors.fenced("session slot owner is another run")
    if int(slot["slot_version"]) != int(lease.slot_version):
        errors.fenced("session slot version changed")


def verify_lease(lease: LeaseToken, *, now: datetime) -> None:
    """The caller's own lease claim must be internally ordered."""
    if lease.run_lease_until <= now:
        errors.fenced("run lease claim is already expired")
    if lease.work_lease_until <= now:
        errors.fenced("work lease claim is already expired")


def verify_run(
    run: Mapping[str, Any] | None,
    actor: ActorContext,
    lease: LeaseToken,
    *,
    now: datetime,
    enforce_lease: bool = True,
) -> Mapping[str, Any]:
    """Ownership, epoch, lease and scope facts for the run row."""
    if run is None:
        errors.fenced("run does not exist")
    if run["scope_id"] != actor.scope_id or run["scope_id"] != lease.scope_id:
        errors.forbidden("run belongs to another scope")
    if run["session_id"] != lease.session_id:
        errors.invalid_input("run and lease session disagree")
    if is_terminal(run["status"]):
        errors.version_conflict("run is already terminal")
    if run["owner_id"] != lease.owner_id:
        errors.fenced("run owner changed")
    if int(run["owner_epoch"]) != int(lease.run_epoch):
        errors.fenced("run epoch changed")
    if enforce_lease:
        if run["lease_until"] is None:
            errors.fenced("run holds no lease")
        if run["lease_until"] <= now:
            errors.fenced("run lease expired")
    return run


def verify_work(
    work: Mapping[str, Any] | None,
    lease: LeaseToken,
    *,
    now: datetime,
    enforce_lease: bool = True,
) -> Mapping[str, Any]:
    """Ownership, epoch, status and lease facts for the claimed work item."""
    if work is None:
        errors.fenced("work item does not exist")
    if lease.work_id != work["id"]:
        errors.invalid_input("lease and work item identity disagree")
    if work["run_id"] != lease.run_id:
        errors.fenced("work item belongs to another run")
    if work["status"] != "leased":
        errors.fenced("work item is not leased")
    if work["owner_id"] != lease.owner_id:
        errors.fenced("work owner changed")
    if int(work["lease_epoch"]) != int(lease.work_epoch):
        errors.fenced("work epoch changed")
    if enforce_lease and (work["lease_until"] is None or work["lease_until"] <= now):
        errors.fenced("work lease expired")
    return work


def verify_guard(
    run: Mapping[str, Any],
    *,
    expected_state_version: int,
    status: str,
    desired_control: str | None,
    control_command_id: Any,
    cancel_version: int,
) -> None:
    """Expected-value CAS over the run's state, control and cancel facts."""
    if int(run["state_version"]) != int(expected_state_version):
        errors.version_conflict("state version changed")
    if int(run["cancel_version"]) != int(cancel_version):
        errors.version_conflict("cancel version changed")
    if run["status"] != status:
        errors.version_conflict("run status changed")
    if run["desired_control"] != desired_control:
        errors.version_conflict("pending control changed")
    if run["control_command_id"] != control_command_id:
        errors.version_conflict("control command identity changed")


def require_no_pending_control(run: Mapping[str, Any], *, transition: str) -> None:
    if run["desired_control"] is not None:
        errors.control_pending(transition)


__all__ = [
    "ACTIVE_STATUSES",
    "LEASED_STATUSES",
    "TERMINAL_STATUSES",
    "is_terminal",
    "require_no_pending_control",
    "verify_guard",
    "verify_lease",
    "verify_run",
    "verify_slot",
    "verify_work",
]

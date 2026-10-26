"""Cross-request identity checks, separate from trusted backend evidence checks.

DS-P01-03: domain rejections raise ``AgentRuntimeError(FailureEnvelope)`` through
``runtime_failure``; a naive ``now`` stays a ``ValueError`` programming error.
"""

from datetime import datetime

from agent_runtime.contracts.credentials import GrantRef, ScopeRequest
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.ownership import (
    ExecutionCancelRequest,
    ExecutionCancelResult,
    RecoveryDisposition,
    RecoveryRequest,
    RecoveryScheduled,
    RecoveryTerminal,
)
from agent_runtime.contracts.workspace import RestoredWorkspaceRef, WorkspaceRestoreRequest


def validate_grant_binding(request: ScopeRequest, grant: GrantRef, *, now: datetime) -> None:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if (grant.scope_id, grant.run_id, grant.execution_id, grant.execution_generation) != (
        request.actor.scope_id,
        request.run_id,
        request.execution_id,
        request.execution_generation,
    ):
        raise runtime_failure(
            "FORBIDDEN",
            category="authorization",
            public_message="The credential grant belongs to a different execution.",
        )
    if not grant.issued_at <= now < grant.expires_at <= request.expires_at:
        raise runtime_failure(
            "DELIVERY_EXPIRED",
            category="delivery",
            public_message="The credential grant validity does not match the request.",
        )


def validate_workspace_restore(
    request: WorkspaceRestoreRequest, result: RestoredWorkspaceRef
) -> None:
    target, expected = result.workspace, request.expected
    if (target.scope_id, target.workspace_id, target.owner_run_id, target.generation) != (
        expected.scope_id,
        expected.workspace_id,
        expected.owner_run_id,
        request.target_generation,
    ):
        raise runtime_failure(
            "VERSION_CONFLICT",
            category="concurrency",
            public_message="The restored workspace identity or generation does not match.",
        )
    if (
        result.checkpoint_id != request.checkpoint.checkpoint_id
        or result.verified_manifest_digest != request.checkpoint.manifest_digest
    ):
        raise runtime_failure(
            "CHECKPOINT_INCOMPATIBLE",
            category="compatibility",
            public_message="The restored workspace references a different checkpoint.",
        )
    if int(target.revision) <= int(expected.revision):
        raise runtime_failure(
            "VERSION_CONFLICT",
            category="concurrency",
            public_message="Workspace restoration must advance the state revision.",
        )


def validate_cancel_result(request: ExecutionCancelRequest, result: ExecutionCancelResult) -> None:
    if result.command_id != request.command_id or (
        result.observation.execution_ref,
        result.observation.generation,
    ) != (request.execution_ref, request.generation):
        raise runtime_failure(
            "IDENTITY_MISMATCH",
            category="delivery",
            public_message="The cancellation result belongs to another command or execution.",
        )


def validate_recovery_result(request: RecoveryRequest, result: RecoveryDisposition) -> None:
    if result.run_id != request.target.run_id:
        raise runtime_failure(
            "IDENTITY_MISMATCH",
            category="delivery",
            public_message="The recovery result belongs to a different run.",
        )
    if isinstance(result, RecoveryScheduled) and int(result.state_version) <= int(
        request.target.expected_state_version
    ):
        raise runtime_failure(
            "VERSION_CONFLICT",
            category="concurrency",
            public_message="Recovery must advance the observed state version.",
        )
    if isinstance(result, RecoveryTerminal) and int(result.state_version) < int(
        request.target.expected_state_version
    ):
        raise runtime_failure(
            "VERSION_CONFLICT",
            category="concurrency",
            public_message="Recovery returned a stale terminal state version.",
        )

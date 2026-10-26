"""Export/check shared JSON Schemas without importing the host application."""

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any

from pydantic import TypeAdapter

from agent_runtime.agent import Agent
from agent_runtime.contracts.audit import AuditEntry, AuditQueryRequest, AuditQueryResult
from agent_runtime.contracts.base import Contract
from agent_runtime.contracts.capacity import (
    CapacityKey,
    CapacityObserveRequest,
    CapacityReleaseApplied,
    CapacityReleaseRejected,
    CapacityReleaseRequest,
    CapacityReleaseReused,
    CapacityReservation,
    CapacityReserveRequest,
    CapacityView,
    capacity_release_adapter,
)
from agent_runtime.contracts.context import ContextRequest, PreparedContext
from agent_runtime.contracts.credentials import (
    EgressDestination,
    FilteredResponse,
    GrantRef,
    ProxyRequest,
    ScopeRequest,
)
from agent_runtime.contracts.decisions import decision_adapter
from agent_runtime.contracts.events import CursorFrame, ResyncFrame, event_adapter
from agent_runtime.contracts.journal import (
    CommittedBatch,
    FlushProof,
    JournalPage,
    OutboxClaim,
    OutboxClaimRequest,
    OutboxDelivery,
    OutboxNotice,
    ProducerBatch,
    SealOutputRequest,
)
from agent_runtime.contracts.loop import AppliedInputs, BudgetView, RunState
from agent_runtime.contracts.mailbox import (
    AppliedInputRef,
    ClaimedBatch,
    MailboxClaimRequest,
    MailboxReceipt,
)
from agent_runtime.contracts.messages import message_adapter
from agent_runtime.contracts.migration import (
    MigrationDeferred,
    MigrationHeads,
    MigrationPlanRequest,
    MigrationRejected,
    MigrationStep,
    MigrationStepRef,
    MigrationSucceeded,
    migration_adapter,
)
from agent_runtime.contracts.model import ModelConfig, ProviderCapabilities, ProviderRequest
from agent_runtime.contracts.ownership import (
    ChildCommitGuard,
    ChildProposalRequest,
    ExecutionCancelRequest,
    ExecutionCancelResult,
    ExecutionObservation,
    ExecutionReconcileRequest,
    RecoveryRequest,
    StaleRunRef,
    recovery_adapter,
)
from agent_runtime.contracts.policy import PolicyRequest, policy_adapter
from agent_runtime.contracts.provider import provider_event_adapter
from agent_runtime.contracts.registry import (
    CapabilityAvailability,
    CapabilityManifest,
    HookSpec,
    KnowledgeBinding,
    PromptSpec,
    RemoteAgentSpec,
    SkillSpec,
)
from agent_runtime.contracts.requests import (
    AcceptedTurn,
    CommandReceipt,
    ControlRequest,
    InteractionReceipt,
    InteractionRequest,
    StreamRequest,
    SubmitRequest,
)
from agent_runtime.contracts.results import result_adapter
from agent_runtime.contracts.scheduler import (
    ClaimRequest,
    RenewRequest,
    WakeInputRequest,
    YieldRequest,
    claim_adapter,
    renew_adapter,
    wake_adapter,
    yield_adapter,
)
from agent_runtime.contracts.state_events import (
    StateEventBatch,
    StateEventReceipt,
    state_draft_adapter,
)
from agent_runtime.contracts.subagents import (
    ChildRunRef,
    CollectRequest,
    SpawnRequest,
    VerifiedResult,
)
from agent_runtime.contracts.tools import (
    ApprovalBinding,
    ApprovalDecision,
    ApprovalRequest,
    ApprovedGrant,
    ToolInvocation,
    ToolSpec,
    tool_outcome_adapter,
)
from agent_runtime.contracts.transactions import (
    ApplyInputRequest,
    CheckpointMigrationRequest,
    CommitGuard,
    DomainCommitRequest,
    HandoffRequest,
    LeaseToken,
    PrepareModelRequest,
)
from agent_runtime.contracts.transitions import (
    AdvanceCommitRequest,
    CancelCommitRequest,
    NoProducersConfirmed,
    PauseCommitRequest,
    ResumeCommitRequest,
    ResumedRun,
    StateWriteGuard,
    TerminalCommitRequest,
    UpdatedStateRef,
    VerifiedEvidence,
    control_outcome_adapter,
)
from agent_runtime.contracts.views import SnapshotEnvelope
from agent_runtime.contracts.work_claims import (
    ChildRenewRequest,
    MaintenanceRenewRequest,
    WorkClaimRequest,
    child_renew_adapter,
    maintenance_renew_adapter,
    work_claim_adapter,
)
from agent_runtime.contracts.workspace import (
    ReadyCheckpoint,
    RestoredWorkspaceRef,
    WorkspaceCheckpointRequest,
    WorkspaceRestoreRequest,
    WorkspaceVersion,
)

ADAPTER_SCHEMAS: dict[str, TypeAdapter[Any]] = {
    "work-claim-result": work_claim_adapter,
    "child-renew-result": child_renew_adapter,
    "maintenance-renew-result": maintenance_renew_adapter,
    "state-event-draft": state_draft_adapter,
    "events": event_adapter,
    "run-result": result_adapter,
    "agent-message": message_adapter,
    "next-action": decision_adapter,
    "provider-event": provider_event_adapter,
    "tool-outcome": tool_outcome_adapter,
    "claim-result": claim_adapter,
    "renew-result": renew_adapter,
    "yield-result": yield_adapter,
    "wake-result": wake_adapter,
    "policy-decision": policy_adapter,
    "recovery-disposition": recovery_adapter,
    "control-outcome": control_outcome_adapter,
    "migration-outcome": migration_adapter,
    "capacity-release-result": capacity_release_adapter,
}

MODEL_SCHEMA_ENTRIES: tuple[tuple[str, type[Contract]], ...] = (
    ("capacity-key", CapacityKey),
    ("capacity-reserve-request", CapacityReserveRequest),
    ("capacity-release-request", CapacityReleaseRequest),
    ("capacity-observe-request", CapacityObserveRequest),
    ("capacity-reservation", CapacityReservation),
    ("capacity-view", CapacityView),
    ("audit-query-request", AuditQueryRequest),
    ("audit-entry", AuditEntry),
    ("audit-query-result", AuditQueryResult),
    ("migration-heads", MigrationHeads),
    ("migration-step", MigrationStep),
    ("migration-step-ref", MigrationStepRef),
    ("migration-plan-request", MigrationPlanRequest),
    ("migration-succeeded", MigrationSucceeded),
    ("migration-deferred", MigrationDeferred),
    ("migration-rejected", MigrationRejected),
    ("capability-manifest", CapabilityManifest),
    ("capability-availability", CapabilityAvailability),
    ("capacity-release-applied", CapacityReleaseApplied),
    ("capacity-release-reused", CapacityReleaseReused),
    ("capacity-release-rejected", CapacityReleaseRejected),
    ("work-claim-request", WorkClaimRequest),
    ("child-renew-request", ChildRenewRequest),
    ("maintenance-renew-request", MaintenanceRenewRequest),
    ("state-event-batch", StateEventBatch),
    ("state-event-receipt", StateEventReceipt),
    ("journal-page", JournalPage),
    ("outbox-notice", OutboxNotice),
    ("outbox-claim-request", OutboxClaimRequest),
    ("outbox-claim", OutboxClaim),
    ("outbox-delivery", OutboxDelivery),
    ("agent", Agent),
    ("hook-spec", HookSpec),
    ("knowledge-binding", KnowledgeBinding),
    ("prompt-spec", PromptSpec),
    ("remote-agent-spec", RemoteAgentSpec),
    ("skill-spec", SkillSpec),
    ("mailbox-receipt", MailboxReceipt),
    ("mailbox-claim", MailboxClaimRequest),
    ("mailbox-claimed-batch", ClaimedBatch),
    ("mailbox-applied", AppliedInputRef),
    ("subagent-spawn", SpawnRequest),
    ("child-run-ref", ChildRunRef),
    ("subagent-collect", CollectRequest),
    ("subagent-verified", VerifiedResult),
    ("execution-cancel", ExecutionCancelRequest),
    ("execution-cancel-result", ExecutionCancelResult),
    ("advance-commit", AdvanceCommitRequest),
    ("resume-commit", ResumeCommitRequest),
    ("resumed-run", ResumedRun),
    ("policy-request", PolicyRequest),
    ("credential-scope-request", ScopeRequest),
    ("credential-grant-ref", GrantRef),
    ("credential-proxy-request", ProxyRequest),
    ("credential-filtered-response", FilteredResponse),
    ("credential-egress-destination", EgressDestination),
    ("workspace-version", WorkspaceVersion),
    ("workspace-checkpoint", ReadyCheckpoint),
    ("workspace-checkpoint-request", WorkspaceCheckpointRequest),
    ("workspace-restore-request", WorkspaceRestoreRequest),
    ("workspace-restored", RestoredWorkspaceRef),
    ("run-state", RunState),
    ("applied-inputs", AppliedInputs),
    ("budget-view", BudgetView),
    ("snapshot", SnapshotEnvelope),
    ("submit", SubmitRequest),
    ("interaction", InteractionRequest),
    ("interaction-receipt", InteractionReceipt),
    ("control", ControlRequest),
    ("command-receipt", CommandReceipt),
    ("accepted-turn", AcceptedTurn),
    ("stream-request", StreamRequest),
    ("cursor", CursorFrame),
    ("resync", ResyncFrame),
    ("lease-token", LeaseToken),
    ("commit-guard", CommitGuard),
    ("prepare-model", PrepareModelRequest),
    ("apply-input", ApplyInputRequest),
    ("domain-commit", DomainCommitRequest),
    ("handoff-request", HandoffRequest),
    ("checkpoint-migration", CheckpointMigrationRequest),
    ("model-config", ModelConfig),
    ("provider-capabilities", ProviderCapabilities),
    ("provider-request", ProviderRequest),
    ("tool-spec", ToolSpec),
    ("tool-invocation", ToolInvocation),
    ("approval-binding", ApprovalBinding),
    ("approval-request", ApprovalRequest),
    ("approval-decision", ApprovalDecision),
    ("approved-grant", ApprovedGrant),
    ("claim-request", ClaimRequest),
    ("renew-request", RenewRequest),
    ("yield-request", YieldRequest),
    ("wake-input-request", WakeInputRequest),
    ("producer-batch", ProducerBatch),
    ("committed-batch", CommittedBatch),
    ("flush-proof", FlushProof),
    ("seal-output-request", SealOutputRequest),
    ("state-write-guard", StateWriteGuard),
    ("pause-commit", PauseCommitRequest),
    ("cancel-commit", CancelCommitRequest),
    ("terminal-commit", TerminalCommitRequest),
    ("updated-state", UpdatedStateRef),
    ("context-request", ContextRequest),
    ("prepared-context", PreparedContext),
    ("child-commit-guard", ChildCommitGuard),
    ("child-proposal", ChildProposalRequest),
    ("execution-reconcile", ExecutionReconcileRequest),
    ("execution-observation", ExecutionObservation),
    ("stale-run", StaleRunRef),
    ("recovery-request", RecoveryRequest),
    ("verified-evidence", VerifiedEvidence),
    ("no-producers-confirmed", NoProducersConfirmed),
)

SCHEMA_REGISTRY: Mapping[str, TypeAdapter[Any] | type[Contract]] = MappingProxyType(
    {**ADAPTER_SCHEMAS, **dict(MODEL_SCHEMA_ENTRIES)}
)


def schema_documents() -> dict[str, str]:
    """Generate every exported JSON Schema from the single reviewed registry."""
    schemas: dict[str, dict[str, Any]] = {}
    for name, entry in SCHEMA_REGISTRY.items():
        schemas[name] = (
            entry.json_schema(by_alias=True)
            if isinstance(entry, TypeAdapter)
            else entry.model_json_schema(by_alias=True)
        )
    return {
        name + ".schema.json": json.dumps(schema, indent=2, sort_keys=True) + "\n"
        for name, schema in schemas.items()
    }


def covered_type_names() -> frozenset[str]:
    """Contract type names reachable from the exported schemas (DS-P01-03 coverage check).

    A type is covered when it is a schema root or appears in a schema's ``$defs``.
    Tests compare this set with every contract leaf class so an unregistered wire type
    fails CI instead of silently shipping without a schema.
    """
    names: set[str] = set()
    for body in schema_documents().values():
        document: dict[str, Any] = json.loads(body)
        names.update(document.get("$defs", {}))
        title = document.get("title")
        if isinstance(title, str) and title:
            names.add(title)
    return frozenset(names)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    documents = schema_documents()
    if args.check:
        for name, content in documents.items():
            path = args.directory / name
            if not path.exists() or path.read_text(encoding="utf-8") != content:
                parser.exit(1, f"Schema missing or stale: {path}\n")
        if {p.name for p in args.directory.glob("*.schema.json")} != set(documents):
            parser.exit(1, "Unexpected schema file\n")
    else:
        args.directory.mkdir(parents=True, exist_ok=True)
        for name, content in documents.items():
            (args.directory / name).write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()

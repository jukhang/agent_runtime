"""Explicit ``*Row`` models for the Runtime-owned schema.

Importing this package registers every table on :data:`agent_runtime.storage.base.Base`
metadata, which is what makes cross-module string foreign keys resolvable. Import
:mod:`agent_runtime.storage` (or this package) before using the metadata.
"""

from agent_runtime.storage.models.capacity import (
    ResourcePermitRow,
    ResourcePoolRow,
    SystemCapacityReservationRow,
    SystemCapacityRow,
)
from agent_runtime.storage.models.credentials import (
    CredentialAuditRow,
    CredentialRequestRow,
    CredentialServiceBindingRow,
)
from agent_runtime.storage.models.journal import (
    AgentMessageRow,
    CommandRow,
    EventRow,
    MailboxCursorRow,
    OutboxRow,
    OutputSealRow,
)
from agent_runtime.storage.models.provider import (
    BudgetAllocationRow,
    BudgetEntryRow,
    ContextBlobRow,
    ContextSnapshotRow,
    ModelAttemptRow,
)
from agent_runtime.storage.models.registry import (
    AgentRevisionRow,
    AgentRow,
    CapabilityRevisionRow,
    CapabilityRow,
)
from agent_runtime.storage.models.session import (
    ProjectionRow,
    RunRow,
    SessionRow,
    SessionSlotRow,
    TimelineItemRow,
    TurnRow,
)
from agent_runtime.storage.models.tools import (
    ApprovalRow,
    ArtifactRevisionRow,
    ArtifactRow,
    CapabilityGrantRow,
    ToolCallRow,
    WorkspaceCheckpointRow,
    WorkspaceRow,
)
from agent_runtime.storage.models.work import (
    CheckpointRow,
    ExecutionRow,
    GoalRevisionRow,
    HandoffRow,
    OperationRow,
    PlanRevisionRow,
    PlanRow,
    TaskDependencyRow,
    TaskRow,
    WorkItemRow,
)

__all__ = [
    "AgentMessageRow",
    "AgentRevisionRow",
    "AgentRow",
    "ApprovalRow",
    "ArtifactRevisionRow",
    "ArtifactRow",
    "BudgetAllocationRow",
    "BudgetEntryRow",
    "CapabilityGrantRow",
    "CapabilityRevisionRow",
    "CapabilityRow",
    "CheckpointRow",
    "CommandRow",
    "ContextBlobRow",
    "ContextSnapshotRow",
    "CredentialAuditRow",
    "CredentialServiceBindingRow",
    "CredentialRequestRow",
    "EventRow",
    "ExecutionRow",
    "GoalRevisionRow",
    "HandoffRow",
    "MailboxCursorRow",
    "ModelAttemptRow",
    "OperationRow",
    "OutboxRow",
    "OutputSealRow",
    "PlanRevisionRow",
    "PlanRow",
    "ProjectionRow",
    "ResourcePermitRow",
    "ResourcePoolRow",
    "RunRow",
    "SessionRow",
    "SessionSlotRow",
    "SystemCapacityReservationRow",
    "SystemCapacityRow",
    "TaskDependencyRow",
    "TaskRow",
    "TimelineItemRow",
    "ToolCallRow",
    "TurnRow",
    "WorkspaceCheckpointRow",
    "WorkspaceRow",
    "WorkItemRow",
]

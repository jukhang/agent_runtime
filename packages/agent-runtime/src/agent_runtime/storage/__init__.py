"""Standard PostgreSQL storage adapter: explicit models, engines and migrations.

Importing this package requires the ``storage`` extra (SQLAlchemy/Alembic/asyncpg) and
registers the Runtime-owned table metadata. The core ``agent_runtime.contracts`` and
``agent_runtime.ports`` modules never import it (Q38).
"""

from agent_runtime.storage import models
from agent_runtime.storage.action_commit import SqlWaitActionCommitter
from agent_runtime.storage.admission import AdmissionAgent, AdmissionAuthorizer, SqlAdmission
from agent_runtime.storage.base import NAMING_CONVENTION, Base
from agent_runtime.storage.child_budget import SqlChildBudgetReader
from agent_runtime.storage.child_factory import (
    ChildAgentProfile,
    ChildHostPorts,
    ChildPortsFactory,
    SqlChildHandlerFactory,
)
from agent_runtime.storage.child_local_tools import ChildLocalReadTool, SqlChildLocalTools
from agent_runtime.storage.child_output import ChildProducerBatch, ChildSealOutputRequest
from agent_runtime.storage.child_setup import SqlChildLoopSetup
from agent_runtime.storage.child_stream import SqlChildStream
from agent_runtime.storage.child_tools import SqlChildToolAuthorizer
from agent_runtime.storage.child_worker import ChildAgentBinding, SqlChildWorkerHandler
from agent_runtime.storage.context_factory import (
    SqlContextComponents,
    build_sql_context_components,
)
from agent_runtime.storage.credential_broker import (
    CredentialAuthorizer,
    CredentialPhase,
    SqlCredentialGrantStore,
)
from agent_runtime.storage.decisions import RecoveredDecision, SqlDecisionStore
from agent_runtime.storage.delegate_commit import SqlDelegateActionCommitter
from agent_runtime.storage.delegation_types import AcceptanceVerifier, DelegationPolicy
from agent_runtime.storage.engines import (
    StorageConfig,
    StorageContext,
    create_session_factory,
    create_storage_engine,
)
from agent_runtime.storage.execution_supervisor import (
    RemoteExecutionInspector,
    RemoteState,
    SqlExecutionSupervisor,
)
from agent_runtime.storage.host_tool_access import SqlHostToolAccess
from agent_runtime.storage.host_worker import (
    SqlClaimActorResolver,
    SqlClaimChildHandler,
    SqlClaimMainHandler,
    SqlWorkerRecoverySink,
    SqlWorkerService,
)
from agent_runtime.storage.journal import (
    OutboxPublisher,
    SqlJournal,
    SqlJournalReader,
    SqlOutboxStore,
)
from agent_runtime.storage.live_run import SqlLiveRunGuard
from agent_runtime.storage.main_budget import SqlMainBudgetReader
from agent_runtime.storage.main_control import SqlMainControlHandler
from agent_runtime.storage.main_factory import (
    MainAgentProfile,
    MainHostPorts,
    MainPortsFactory,
    SqlMainHandlerFactory,
)
from agent_runtime.storage.main_run_loader import MainAgentBinding, SqlMainRunLoader
from agent_runtime.storage.main_stream import SqlMainOutputFactory, SqlMainStream
from agent_runtime.storage.main_worker import MainOutputs, SqlMainWorkerHandler
from agent_runtime.storage.model_attempts import SqlModelAttemptStore
from agent_runtime.storage.model_events import SqlModelEventSink
from agent_runtime.storage.plan_commit import SqlPlanActionCommitter
from agent_runtime.storage.recovery import (
    FailureSettlement,
    RecoveryConfig,
    RecoveryReport,
    SqlRecovery,
)
from agent_runtime.storage.sandbox_results import SqlSandboxResultCollector
from agent_runtime.storage.scheduler import SchedulerConfig, SqlScheduler
from agent_runtime.storage.subagents import SqlSubagentRuntime
from agent_runtime.storage.tool_assembly import SqlToolRuntimeAssembly
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.tool_host import (
    IdempotentExternalTool,
    SqlToolHost,
    ToolEffect,
    ToolSchemaValidator,
    TransactionalTool,
    TransactionAuthorizer,
)
from agent_runtime.storage.uow import (
    DomainRegistry,
    DomainResourceSpec,
    SqlStateUnitOfWork,
    SqlSystemCapacityStore,
)
from agent_runtime.storage.wake import SqlWaitWaker, WakeReceipt, WakeRequest
from agent_runtime.storage.worker_topology import SqlWorkerTopology

__all__ = [
    "AdmissionAgent",
    "AdmissionAuthorizer",
    "SqlAdmission",
    "NAMING_CONVENTION",
    "Base",
    "ChildAgentBinding",
    "ChildAgentProfile",
    "ChildHostPorts",
    "ChildPortsFactory",
    "ChildLocalReadTool",
    "ChildProducerBatch",
    "ChildSealOutputRequest",
    "CredentialAuthorizer",
    "CredentialPhase",
    "SqlCredentialGrantStore",
    "SqlChildBudgetReader",
    "SqlChildLocalTools",
    "SqlChildHandlerFactory",
    "SqlChildLoopSetup",
    "SqlChildStream",
    "SqlChildToolAuthorizer",
    "SqlChildWorkerHandler",
    "AcceptanceVerifier",
    "DelegationPolicy",
    "DomainRegistry",
    "DomainResourceSpec",
    "FailureSettlement",
    "OutboxPublisher",
    "RecoveryConfig",
    "RecoveryReport",
    "RecoveredDecision",
    "SchedulerConfig",
    "SqlJournal",
    "SqlJournalReader",
    "SqlLiveRunGuard",
    "MainAgentBinding",
    "MainOutputs",
    "SqlMainBudgetReader",
    "SqlMainControlHandler",
    "SqlClaimActorResolver",
    "SqlClaimChildHandler",
    "SqlClaimMainHandler",
    "SqlHostToolAccess",
    "SqlWorkerRecoverySink",
    "SqlWorkerTopology",
    "SqlWorkerService",
    "SqlMainOutputFactory",
    "SqlMainHandlerFactory",
    "MainAgentProfile",
    "MainHostPorts",
    "MainPortsFactory",
    "SqlMainRunLoader",
    "SqlMainStream",
    "SqlMainWorkerHandler",
    "SqlContextComponents",
    "SqlDecisionStore",
    "SqlDelegateActionCommitter",
    "SqlModelAttemptStore",
    "SqlModelEventSink",
    "SqlOutboxStore",
    "SqlPlanActionCommitter",
    "SqlRecovery",
    "SqlScheduler",
    "SqlSubagentRuntime",
    "SqlSandboxResultCollector",
    "SqlToolCapacity",
    "SqlToolRuntimeAssembly",
    "SqlToolHost",
    "SqlExecutionSupervisor",
    "RemoteExecutionInspector",
    "RemoteState",
    "ToolEffect",
    "ToolSchemaValidator",
    "TransactionalTool",
    "TransactionAuthorizer",
    "IdempotentExternalTool",
    "SqlStateUnitOfWork",
    "SqlSystemCapacityStore",
    "SqlWaitActionCommitter",
    "SqlWaitWaker",
    "StorageConfig",
    "StorageContext",
    "WakeReceipt",
    "WakeRequest",
    "create_session_factory",
    "build_sql_context_components",
    "create_storage_engine",
    "models",
]

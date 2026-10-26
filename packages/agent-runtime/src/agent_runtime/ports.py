"""Runtime module boundaries. Implementations are injected; these ports do no I/O."""

from collections.abc import AsyncIterator
from typing import Protocol
from uuid import UUID

from agent_runtime.contracts.context import ContextRequest, PreparedContext
from agent_runtime.contracts.credentials import (
    FilteredResponse,
    GrantRef,
    ProxyRequest,
    ScopeRequest,
)
from agent_runtime.contracts.decisions import NextAction
from agent_runtime.contracts.events import ResyncFrame
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
from agent_runtime.contracts.messages import AgentEnvelope
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.contracts.ownership import (
    ExecutionCancelRequest,
    ExecutionCancelResult,
    ExecutionObservation,
    ExecutionReconcileRequest,
    RecoveryDisposition,
    RecoveryRequest,
)
from agent_runtime.contracts.policy import PolicyDecision, PolicyRequest
from agent_runtime.contracts.provider import ProviderEvent
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.requests import AcceptedTurn, StreamRequest, SubmitRequest
from agent_runtime.contracts.scheduler import (
    ClaimRequest,
    ClaimResult,
    RenewRequest,
    RenewResult,
    WakeInputRequest,
    WakeResult,
    YieldRequest,
    YieldResult,
)
from agent_runtime.contracts.state_events import StateEventBatch, StateEventReceipt
from agent_runtime.contracts.subagents import (
    ChildRunRef,
    CollectRequest,
    SpawnRequest,
    VerifiedResult,
)
from agent_runtime.contracts.tools import ToolInvocation, ToolOutcome
from agent_runtime.contracts.transactions import (
    ActorContext,
    ApplyInputRequest,
    DomainCommitRequest,
)
from agent_runtime.contracts.transitions import (
    AdvanceCommitRequest,
    CancelCommitRequest,
    ControlOutcome,
    PauseCommitRequest,
    ResumeCommitRequest,
    ResumedRun,
    TerminalCommitRequest,
    UpdatedStateRef,
)
from agent_runtime.contracts.views import SnapshotEnvelope
from agent_runtime.contracts.work_claims import (
    ChildRenewRequest,
    ChildRenewResult,
    MaintenanceRenewRequest,
    MaintenanceRenewResult,
    WorkClaimRequest,
    WorkClaimResult,
)
from agent_runtime.contracts.workspace import (
    ReadyCheckpoint,
    RestoredWorkspaceRef,
    WorkspaceCheckpointRequest,
    WorkspaceRestoreRequest,
)


class Admission(Protocol):
    async def accept(self, actor: ActorContext, request: SubmitRequest) -> AcceptedTurn: ...


class Scheduler(Protocol):
    async def claim_work(self, request: WorkClaimRequest) -> WorkClaimResult: ...
    async def renew_child(self, request: ChildRenewRequest) -> ChildRenewResult: ...
    async def renew_maintenance(
        self, request: MaintenanceRenewRequest
    ) -> MaintenanceRenewResult: ...
    async def claim(self, request: ClaimRequest) -> ClaimResult: ...
    async def renew(self, request: RenewRequest) -> RenewResult: ...
    async def yield_run(self, request: YieldRequest) -> YieldResult: ...


class AgentLoop(Protocol):
    async def advance(
        self, state: RunState, inputs: AppliedInputs, budget: BudgetView
    ) -> NextAction: ...


class Context(Protocol):
    async def prepare(self, request: ContextRequest) -> PreparedContext: ...


class CancelSignal(Protocol):
    """Cooperative cancellation observed by an in-flight provider stream."""

    def is_cancelled(self) -> bool: ...
    async def wait(self) -> None: ...


class Provider(Protocol):
    """Normalized model stream; it never retries or makes Agent decisions."""

    def stream(
        self,
        request: ProviderRequest,
        *,
        cancel: CancelSignal | None = None,
    ) -> AsyncIterator[ProviderEvent]: ...


class Journal(Protocol):
    async def append(self, batch: ProducerBatch) -> CommittedBatch: ...
    async def seal_output(self, request: SealOutputRequest) -> FlushProof: ...
    async def append_state(self, batch: StateEventBatch) -> StateEventReceipt:
        """Internal UoW-only entry; state, events and outbox must commit together."""
        ...


class JournalReader(Protocol):
    async def read_public(
        self, actor: ActorContext, request: StreamRequest
    ) -> JournalPage | ResyncFrame: ...
    async def read_authorized(
        self, actor: ActorContext, request: StreamRequest
    ) -> JournalPage | ResyncFrame: ...


class Outbox(Protocol):
    async def claim(self, request: OutboxClaimRequest) -> OutboxClaim | None: ...
    async def delivered(self, delivery: OutboxDelivery) -> int: ...
    async def retry(self, delivery: OutboxDelivery, *, delay_seconds: int) -> int: ...


class NotificationSink(Protocol):
    async def publish(self, notice: OutboxNotice) -> None: ...


class ToolRuntime(Protocol):
    async def execute(self, invocation: ToolInvocation) -> ToolOutcome: ...


class Policy(Protocol):
    async def evaluate(self, request: PolicyRequest) -> PolicyDecision: ...


class CredentialBroker(Protocol):
    async def authorize(self, request: ScopeRequest) -> GrantRef: ...
    async def proxy(self, request: ProxyRequest) -> FilteredResponse: ...


class StateStore(Protocol):
    """Storage-side state transactions; the only state write port (DS-P02-00 P2-1).

    The application service layer and the storage UnitOfWork both depend on this
    interface; the concrete implementation (``SqlStateUnitOfWork`` in DS-P02-02) owns
    the lock order, CAS and fencing checks. Storage methods are split by transaction
    intent instead of one generic union so each one can be validated and tested alone.
    """

    async def commit_advance(self, request: AdvanceCommitRequest) -> UpdatedStateRef: ...
    async def commit_pause(self, request: PauseCommitRequest) -> UpdatedStateRef: ...
    async def commit_terminal(self, request: TerminalCommitRequest) -> UpdatedStateRef: ...
    async def resume(self, request: ResumeCommitRequest) -> ResumedRun: ...
    async def apply_input(self, request: ApplyInputRequest) -> AppliedInputs:
        """Apply claimed mailbox inputs and advance the run's applied watermark."""
        ...
    async def commit_cancel(self, request: CancelCommitRequest) -> ControlOutcome:
        """Apply the persisted cancel command at a safe point.

        Cancellation is a trusted program path that never calls a model and never consumes
        model budget, but it is still a *state commit*: it carries the worker lease guard,
        the new checkpoint, flush evidence and execution resolutions (Q47). A refusal
        returns ``ControlRejected`` instead of raising. Once every execution is confirmed
        stopped, ``commit_terminal`` finishes the run.
        """
        ...
    async def commit_domain(self, request: DomainCommitRequest) -> ResourceRef:
        """Commit a same-database domain resource change inside the run's transaction.

        Cross-database domain writes cannot join this unit of work; they must go through
        an external idempotency protocol and record their own operation identity.
        """
        ...


class Mailbox(Protocol):
    async def send(self, actor: ActorContext, message: AgentEnvelope) -> MailboxReceipt: ...
    async def claim(self, request: MailboxClaimRequest) -> ClaimedBatch: ...
    async def apply(self, request: ApplyInputRequest) -> AppliedInputRef: ...


class Subagent(Protocol):
    async def spawn(self, request: SpawnRequest) -> ChildRunRef: ...
    async def collect(self, request: CollectRequest) -> VerifiedResult: ...


class Workspace(Protocol):
    async def checkpoint(self, request: WorkspaceCheckpointRequest) -> ReadyCheckpoint: ...
    async def restore(self, request: WorkspaceRestoreRequest) -> RestoredWorkspaceRef: ...


class Supervisor(Protocol):
    async def reconcile(self, request: ExecutionReconcileRequest) -> ExecutionObservation: ...
    async def cancel(self, request: ExecutionCancelRequest) -> ExecutionCancelResult: ...


class Recovery(Protocol):
    async def wake(self, request: WakeInputRequest) -> WakeResult: ...
    async def reconcile(self, request: RecoveryRequest) -> RecoveryDisposition: ...


class Projection(Protocol):
    async def snapshot(self, actor: ActorContext, session_id: UUID) -> SnapshotEnvelope: ...

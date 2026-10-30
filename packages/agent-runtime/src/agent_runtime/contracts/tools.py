"""Tool execution shapes; no DTO grants authority or executes side effects."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, JsonValue, TypeAdapter, model_validator

from agent_runtime.contracts.base import Contract, Identifier, MachineKey, Revision
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.decisions import ToolCallRequest
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.references import CapabilityRef, ResourceRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.views import Digest

type PublicPolicy = Literal["full", "summary", "reference_only"]


class McpToolBinding(Contract):
    """Immutable, host-approved remote identity and schemas; never a model-supplied URL."""

    protocol_version: Annotated[str, Field(pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")]
    tool_name: MachineKey
    destination: CapabilityRef
    input_schema: dict[str, JsonValue]
    output_schema: dict[str, JsonValue]
    lookup_name: MachineKey | None = None
    lookup_destination: CapabilityRef | None = None
    cancel_name: MachineKey | None = None
    cancel_destination: CapabilityRef | None = None

    @model_validator(mode="after")
    def coherent_lookup(self) -> Self:
        if (self.lookup_name is None) != (self.lookup_destination is None):
            raise ValueError("MCP lookup name and destination must be pinned together")
        if self.lookup_destination is not None and self.lookup_destination == self.destination:
            raise ValueError("MCP effect and lookup need separate destinations")
        if (self.cancel_name is None) != (self.cancel_destination is None):
            raise ValueError("MCP cancel name and destination must be pinned together")
        if self.cancel_destination is not None and self.cancel_destination in {
            self.destination,
            self.lookup_destination,
        }:
            raise ValueError("MCP cancellation needs a separate destination")
        names = [
            name
            for name in (self.tool_name, self.lookup_name, self.cancel_name)
            if name is not None
        ]
        if len(names) != len(set(names)):
            raise ValueError("MCP effect, lookup and cancel names must be distinct")
        return self


class ToolSpec(Contract):
    ref: CapabilityRef
    description: Annotated[str, Field(min_length=1)]
    source: CapabilityRef
    kind: Literal["read", "write", "control", "background", "interaction"]
    side_effect: Literal["none", "database", "external"]
    input_schema: CapabilityRef
    output_schema: CapabilityRef
    permissions: tuple[CapabilityRef, ...]
    approval_policy: CapabilityRef
    idempotency: Literal["read_only", "transactional", "provider_key", "reconcile", "manual"]
    timeout_seconds: Annotated[int, Field(strict=True, gt=0)]
    concurrency_group: Identifier
    max_concurrency: Annotated[int, Field(strict=True, gt=0)]
    cancellation: Literal["stop", "detach", "unsupported"]
    public_result_policy: PublicPolicy
    mcp: McpToolBinding | None = None

    @model_validator(mode="after")
    def coherent_effect(self) -> Self:
        if self.side_effect != "none" and self.idempotency == "read_only":
            raise ValueError("side effects cannot use read-only idempotency")
        if self.kind == "read" and self.side_effect != "none":
            raise ValueError("read tools cannot declare writes")
        keys = [ref.key for ref in self.permissions]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate permission binding")
        if self.mcp is not None:
            if self.input_schema.key.namespace != "mcp.input" or (
                self.input_schema.key.key != content_digest(self.mcp.input_schema)
            ):
                raise ValueError("MCP input schema reference does not match its content")
            if self.output_schema.key.namespace != "mcp.output" or (
                self.output_schema.key.key != content_digest(self.mcp.output_schema)
            ):
                raise ValueError("MCP output schema reference does not match its content")
            if self.kind == "read" and (
                self.mcp.lookup_name is not None or self.mcp.cancel_name is not None
            ):
                raise ValueError("read-only MCP tool cannot have effect controls")
            if self.side_effect == "external" and (
                self.idempotency != "provider_key" or self.mcp.lookup_name is None
            ):
                raise ValueError("external MCP effect requires a pinned lookup tool")
        return self


class ToolInvocation(Contract):
    actor: ActorContext
    session_id: UUID
    run_id: UUID
    model_attempt_id: UUID | None
    context_snapshot_id: UUID
    context_version: Revision
    call: ToolCallRequest
    operation_id: UUID | None
    deadline_at: AwareDatetime


class ApprovalBinding(Contract):
    scope_id: Identifier
    actor_id: Identifier
    session_id: UUID
    tool: CapabilityRef
    operation_id: UUID
    arguments_digest: Digest
    policy: CapabilityRef
    auth_revision: Revision
    resources: tuple[ResourceRef, ...]

    @model_validator(mode="after")
    def versioned_targets(self) -> Self:
        keys = [(ref.resource_type, ref.resource_id) for ref in self.resources]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate approval target")
        for ref in self.resources:
            if ref.scope.scope_id != self.scope_id or ref.resource_version is None:
                raise ValueError("approval targets require same scope and expected version")
        return self


class ApprovalRequest(Contract):
    approval_id: UUID
    binding: ApprovalBinding
    idempotency_key: MachineKey
    public_summary: Annotated[str, Field(min_length=1)]
    expires_at: AwareDatetime


class ApprovalDecision(Contract):
    approval_id: UUID
    expected_version: Revision
    decision: Literal["approved", "rejected", "cancelled"]
    idempotency_key: MachineKey


class ApprovedGrant(Contract):
    approval_id: UUID
    revision: Revision
    binding: ApprovalBinding
    approved_at: AwareDatetime
    expires_at: AwareDatetime

    @model_validator(mode="after")
    def valid_interval(self) -> Self:
        if self.expires_at <= self.approved_at:
            raise ValueError("approval must expire after approval time")
        return self


class FullResult(Contract):
    policy: Literal["full"]
    data: JsonValue


class SummaryResult(Contract):
    policy: Literal["summary"]
    text: Annotated[str, Field(min_length=1)]


class ReferenceResult(Contract):
    policy: Literal["reference_only"]
    refs: Annotated[tuple[ResourceRef, ...], Field(min_length=1)]


type PublicToolResult = Annotated[
    FullResult | SummaryResult | ReferenceResult, Field(discriminator="policy")
]


class ToolOutcomeBase(Contract):
    tool_call_id: UUID
    run_id: UUID
    operation_id: UUID | None


class TerminalToolOutcome(ToolOutcomeBase):
    effect: Literal["none", "committed", "rolled_back", "unknown"]
    effect_refs: tuple[ResourceRef, ...]

    @model_validator(mode="after")
    def effect_identity(self) -> Self:
        if self.effect != "none" and self.operation_id is None:
            raise ValueError("side effect outcomes require operation identity")
        if self.effect == "none" and self.effect_refs:
            raise ValueError("no-effect outcome cannot contain effect references")
        return self


class ToolCompleted(TerminalToolOutcome):
    status: Literal["completed"]
    result: PublicToolResult

    @model_validator(mode="after")
    def successful_effect(self) -> Self:
        if self.effect not in {"none", "committed"}:
            raise ValueError("completed tool cannot have rolled-back or unknown effect")
        return self


class UnsuccessfulToolOutcome(TerminalToolOutcome):
    failure: FailureEnvelope

    @model_validator(mode="after")
    def no_unsafe_replay(self) -> Self:
        if self.failure.operation_id is not None and (
            self.operation_id is None or self.failure.operation_id != str(self.operation_id)
        ):
            raise ValueError("failure operation identity does not match tool outcome")
        if self.effect in {"committed", "unknown"}:
            replay = {"retry_same", "retry_with_compaction"}
            if self.failure.retryable or any(
                option.action in replay for option in self.failure.recovery_options
            ):
                raise ValueError("committed or unknown effects cannot be retried automatically")
        return self


class ToolFailed(UnsuccessfulToolOutcome):
    status: Literal["failed"]


class ToolCancelled(UnsuccessfulToolOutcome):
    status: Literal["cancelled"]


class ToolNotExecuted(UnsuccessfulToolOutcome):
    status: Literal["not_executed"]

    @model_validator(mode="after")
    def no_execution(self) -> Self:
        if self.effect != "none":
            raise ValueError("not-executed tool cannot have effects")
        return self


class ToolWaitingApproval(ToolOutcomeBase):
    status: Literal["waiting_approval"]
    approval: ApprovalRequest

    @model_validator(mode="after")
    def same_operation(self) -> Self:
        if self.operation_id != self.approval.binding.operation_id:
            raise ValueError("approval must bind the tool operation")
        return self


class PendingExecution(ToolOutcomeBase):
    status: Literal["pending_execution"]
    execution_ref: ResourceRef


type ToolOutcome = Annotated[
    ToolCompleted
    | ToolFailed
    | ToolCancelled
    | ToolNotExecuted
    | ToolWaitingApproval
    | PendingExecution,
    Field(discriminator="status"),
]
tool_outcome_adapter: TypeAdapter[ToolOutcome] = TypeAdapter(ToolOutcome)

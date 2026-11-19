"""Next-action shape validation does not authorize tool execution."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, JsonValue, TypeAdapter, model_validator

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.references import CapabilityRef, ResourceRef
from agent_runtime.contracts.tasks import NonblankText, TaskPackage, WaitSpec
from agent_runtime.contracts.views import PlanStep, validate_steps


class RespondPayload(Contract):
    answer_request_ref: Identifier


class ToolCallRequest(Contract):
    tool_call_id: UUID
    tool: CapabilityRef
    arguments: dict[str, JsonValue]


class InvokeToolsPayload(Contract):
    validated_calls: Annotated[tuple[ToolCallRequest, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def distinct_calls(self) -> Self:
        ids = [call.tool_call_id for call in self.validated_calls]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate tool call identity")
        return self


class UpdatePlanPayload(Contract):
    plan_id: UUID
    expected_revision: Revision
    steps: tuple[PlanStep, ...]

    @model_validator(mode="after")
    def valid_steps(self) -> Self:
        validate_steps(self.steps)
        return self


class DelegatePayload(Contract):
    task_package: TaskPackage


class AskUserPayload(Contract):
    question: NonblankText
    required_facts: tuple[NonblankText, ...]


class WaitPayload(Contract):
    wait_spec: WaitSpec


class RecoveredToolFailure(Contract):
    failed_tool_call_id: UUID
    recovery_tool_call_ids: Annotated[tuple[UUID, ...], Field(min_length=1, max_length=32)]
    reason: Annotated[NonblankText, Field(max_length=600)]

    @model_validator(mode="after")
    def distinct_calls(self) -> Self:
        if len(set(self.recovery_tool_call_ids)) != len(self.recovery_tool_call_ids):
            raise ValueError("duplicate recovery tool call")
        if self.failed_tool_call_id in self.recovery_tool_call_ids:
            raise ValueError("a failed tool cannot verify its own recovery")
        return self


class FinishPayload(Contract):
    outcome: Literal["success", "partial"]
    evidence_refs: tuple[ResourceRef, ...]
    final_item_ids: tuple[UUID, ...]
    recovered_tool_failures: Annotated[tuple[RecoveredToolFailure, ...], Field(max_length=128)] = ()

    @model_validator(mode="after")
    def distinct_recoveries(self) -> Self:
        ids = [entry.failed_tool_call_id for entry in self.recovered_tool_failures]
        if len(ids) != len(set(ids)):
            raise ValueError("a tool failure may be assessed only once")
        return self


class Respond(Contract):
    kind: Literal["respond"]
    payload: RespondPayload


class InvokeTools(Contract):
    kind: Literal["invoke_tools"]
    payload: InvokeToolsPayload


class UpdatePlan(Contract):
    kind: Literal["update_plan"]
    payload: UpdatePlanPayload


class Delegate(Contract):
    kind: Literal["delegate"]
    payload: DelegatePayload


class AskUser(Contract):
    kind: Literal["ask_user"]
    payload: AskUserPayload


class Wait(Contract):
    kind: Literal["wait"]
    payload: WaitPayload


class Finish(Contract):
    kind: Literal["finish"]
    payload: FinishPayload


type NextAction = Annotated[
    Respond | InvokeTools | UpdatePlan | Delegate | AskUser | Wait | Finish,
    Field(discriminator="kind"),
]
decision_adapter: TypeAdapter[NextAction] = TypeAdapter(NextAction)

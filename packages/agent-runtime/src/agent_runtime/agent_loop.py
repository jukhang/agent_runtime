"""One bounded Agent decision from immutable context and a complete model stream.

This module does not commit state or execute tools. The claimed worker must persist
the validated action through StateStore before any effectful tool dispatch.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from agent_runtime.candidate_repair import CandidateRejected
from agent_runtime.context_runtime import (
    ContextAccessVerifier,
    ContextBoundProvider,
    ContextRuntime,
)
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.context import ContextRequest, PreparedContext
from agent_runtime.contracts.decisions import (
    AskUser,
    AskUserPayload,
    InvokeTools,
    InvokeToolsPayload,
    NextAction,
    Respond,
    RespondPayload,
    ToolCallRequest,
    decision_adapter,
)
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.loop import AppliedInputs, BudgetView, RunState
from agent_runtime.contracts.model import ModelConfig, ProviderRequest
from agent_runtime.contracts.provider import (
    AnalysisDelta,
    ContentDelta,
    ProviderError,
    ProviderEvent,
    ProviderFinish,
    ToolCallComplete,
)
from agent_runtime.contracts.references import CapabilityRef, ResourceRef
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.decision_validation import validate_candidate_decision
from agent_runtime.finalization import closing_budget
from agent_runtime.native_controls import ASK_USER_CONTROL
from agent_runtime.ports import CancelSignal, Provider
from agent_runtime.provider import _close_stream
from agent_runtime.tool_names import provider_tool_name

type Clock = Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class DecisionSetup:
    context_request: ContextRequest
    model: ModelConfig
    mode: Literal["native", "structured"] = "native"
    output_schema: CapabilityRef | None = None
    validate_action: Callable[[NextAction, PreparedContext], None] | None = None
    observation_policy: Literal["include", "isolate"] = "include"

    def __post_init__(self) -> None:
        if self.observation_policy not in {"include", "isolate"}:
            raise ValueError("Unknown trusted observation policy")


@dataclass(frozen=True, slots=True)
class DecisionFrame:
    """Pinned provenance required by the trusted action runner."""

    actor: ActorContext
    context: PreparedContext
    attempt_id: UUID
    action: NextAction


class LoopSetup(Protocol):
    async def resolve(
        self, state: RunState, inputs: AppliedInputs, budget: BudgetView
    ) -> DecisionSetup:
        """Resolve pinned Agent/profile, model, prompt, sources and budget from trusted state."""
        ...


class AnswerStore(Protocol):
    async def save(self, scope_id: str, run_id: str, ref: str, text: str) -> None:
        """Persist a completed answer with a stable content-derived reference."""
        ...


class ModelEventSink(Protocol):
    async def record(self, request: ProviderRequest, event: ProviderEvent) -> None:
        """Durably append a channel-separated event before it reaches loop state."""
        ...


class ToolNames(Protocol):
    def resolve(self, name: str, visible: tuple[CapabilityRef, ...]) -> CapabilityRef:
        """Resolve a native provider name to one exact, visible tool revision."""
        ...


class ExactToolNames:
    def resolve(self, name: str, visible: tuple[CapabilityRef, ...]) -> CapabilityRef:
        matches = [ref for ref in visible if provider_tool_name(ref) == name]
        if len(matches) != 1:
            raise runtime_failure(
                "FORBIDDEN",
                category="authorization",
                public_message="The proposed tool is not uniquely visible to this run.",
            )
        return matches[0]


class ModelAgentLoop:
    """Build a bound model request and return one validated candidate action."""

    def __init__(
        self,
        *,
        context: ContextRuntime,
        access: ContextAccessVerifier,
        provider: Provider,
        setup: LoopSetup,
        answers: AnswerStore,
        events: ModelEventSink,
        tool_names: ToolNames | None = None,
        cancel: CancelSignal | None = None,
        clock: Clock = lambda: datetime.now(UTC),
        candidate_repair: bool = False,
    ) -> None:
        self._context = context
        self._access = access
        self._provider = provider
        self._setup = setup
        self._answers = answers
        self._events = events
        self._tool_names = tool_names or ExactToolNames()
        self._cancel = cancel
        self._clock = clock
        self._candidate_repair = candidate_repair

    async def advance(
        self, state: RunState, inputs: AppliedInputs, budget: BudgetView
    ) -> NextAction:
        return (await self.decide(state, inputs, budget)).action

    async def decide(
        self,
        state: RunState,
        inputs: AppliedInputs,
        budget: BudgetView,
        *,
        observation_refs: tuple[ResourceRef, ...] = (),
        candidate_feedback: str | None = None,
    ) -> DecisionFrame:
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("loop clock must be timezone-aware")
        if self._cancel is not None and self._cancel.is_cancelled():
            raise runtime_failure(
                "CONTROL_PENDING", category="concurrency", public_message="Run was cancelled."
            )
        if state.status != RunStatus.RUNNING or state.desired_control is not None:
            raise runtime_failure(
                "FENCED", category="concurrency", public_message="The run is not active."
            )
        if inputs.run_id != state.run_id or inputs.checkpoint_ref != state.checkpoint_ref:
            raise runtime_failure(
                "VERSION_CONFLICT",
                category="concurrency",
                public_message="Inputs do not match the current checkpoint.",
            )
        if int(inputs.applied_seq) != int(state.applied_input_seq):
            raise runtime_failure(
                "VERSION_CONFLICT",
                category="concurrency",
                public_message="Input watermark does not match the run.",
            )
        if now >= budget.deadline_at:
            raise runtime_failure(
                "BUDGET_EXHAUSTED", category="budget", public_message="Run deadline expired."
            )
        for counter in (budget.model_calls, budget.tokens):
            without_unknown = (
                int(counter.limit)
                - int(counter.spent)
                - int(counter.reserved)
                + int(counter.unknown_reserved)
            )
            if counter.available == 0 and int(counter.unknown_reserved) > 0 and without_unknown > 0:
                raise runtime_failure(
                    "USAGE_UNRESOLVED",
                    category="budget",
                    public_message=(
                        "Previous model usage is unresolved. Verify provider usage and settle "
                        "the unknown reservation before continuing; no new model call was made."
                    ),
                )
        if budget.model_calls.available < 1:
            raise runtime_failure(
                "BUDGET_EXHAUSTED", category="budget", public_message="Model budget is exhausted."
            )
        try:
            setup = await self._setup.resolve(state, inputs, budget)
        except AgentRuntimeError as exc:
            if (
                exc.failure.code != "BUDGET_EXHAUSTED"
                or budget.closing
                or not int(budget.closing_tokens)
            ):
                raise
            # Setup only: no Provider request or paid attempt exists yet.
            budget = closing_budget(budget)
            setup = await self._setup.resolve(state, inputs, budget)
        if candidate_feedback is not None:
            setup = replace(
                setup,
                context_request=setup.context_request.model_copy(
                    update={"candidate_feedback": candidate_feedback}
                ),
            )
        setup, prepared = await self._prepare_setup(state, inputs, budget, setup, observation_refs)
        context_request = setup.context_request
        request = self._model_request(state, budget, setup.model, prepared, setup.output_schema)
        wrapped = ContextBoundProvider(
            self._context,
            self._provider,
            actor=context_request.actor,
            access=self._access,
        )
        return await self._decide_prepared(state, budget, setup, prepared, request, wrapped)

    async def _prepare_setup(
        self,
        state: RunState,
        inputs: AppliedInputs,
        budget: BudgetView,
        setup: DecisionSetup,
        observation_refs: tuple[ResourceRef, ...],
    ) -> tuple[DecisionSetup, PreparedContext]:
        original = setup
        try:
            return setup, await self._prepare_context(state, budget, setup, observation_refs)
        except AgentRuntimeError as exc:
            if exc.failure.code != "VERSION_CONFLICT" or exc.failure.category != "context":
                raise
            fresh = await self._setup.resolve(state, inputs, budget)
            fresh = replace(
                fresh,
                context_request=fresh.context_request.model_copy(
                    update={"candidate_feedback": original.context_request.candidate_feedback}
                ),
            )
            old_request, new_request = original.context_request, fresh.context_request
            invariant_fields = (
                "actor",
                "session_id",
                "run_id",
                "profile",
                "model",
                "prompt",
                "tools",
                "budget",
                "user_input",
            )
            if (
                any(
                    getattr(old_request, key) != getattr(new_request, key)
                    for key in invariant_fields
                )
                or old_request.resource_refs == new_request.resource_refs
                or original.model != fresh.model
                or original.mode != fresh.mode
                or original.output_schema != fresh.output_schema
                or original.observation_policy != fresh.observation_policy
            ):
                raise exc
            # A second conflict propagates. Never substitute versions from model
            # output or retry a decision/tool commit with refreshed arguments.
            return fresh, await self._prepare_context(state, budget, fresh, observation_refs)

    async def _prepare_context(
        self,
        state: RunState,
        budget: BudgetView,
        setup: DecisionSetup,
        observation_refs: tuple[ResourceRef, ...],
    ) -> PreparedContext:
        if self._cancel is not None and self._cancel.is_cancelled():
            raise runtime_failure(
                "CONTROL_PENDING", category="concurrency", public_message="Run was cancelled."
            )
        if self._clock() >= budget.deadline_at:
            raise runtime_failure(
                "BUDGET_EXHAUSTED", category="budget", public_message="Run deadline expired."
            )
        context_request = setup.context_request
        if observation_refs:
            if any(ref.scope.scope_id != state.scope_id for ref in observation_refs):
                raise runtime_failure(
                    "FORBIDDEN",
                    category="authorization",
                    public_message="Observation belongs to another scope.",
                )
        if observation_refs and setup.observation_policy == "include":
            context_request = context_request.model_copy(
                update={
                    "resource_refs": context_request.resource_refs
                    + tuple(
                        ref for ref in observation_refs if ref not in context_request.resource_refs
                    )
                }
            )
        self._validate_setup(state, budget, setup)
        prepared = await self._context.prepare(context_request)
        await self._access.verify(context_request.actor, prepared)
        return prepared

    async def _decide_prepared(
        self,
        state: RunState,
        budget: BudgetView,
        setup: DecisionSetup,
        prepared: PreparedContext,
        request: ProviderRequest,
        wrapped: ContextBoundProvider,
    ) -> DecisionFrame:
        context_request = setup.context_request
        content: list[str] = []
        tool_calls: list[ToolCallRequest] = []
        questions: list[AskUserPayload] = []
        raw_calls: list[ToolCallComplete] = []
        finish: ProviderFinish | None = None
        stream = wrapped.stream(request, cancel=self._cancel)
        try:
            async for event in stream:
                if setup.mode == "native" or not isinstance(event, (ContentDelta, AnalysisDelta)):
                    await self._events.record(request, event)
                if isinstance(event, ProviderError):
                    raise AgentRuntimeError(event.failure)
                if isinstance(event, ContentDelta):
                    content.append(event.text)
                elif isinstance(event, ToolCallComplete):
                    raw_calls.append(event)
                elif isinstance(event, ProviderFinish):
                    finish = event
        except BaseException as error:
            try:
                await _close_stream(stream)
            except BaseException as close_error:
                raise error from close_error
            raise
        else:
            await _close_stream(stream)
        if finish is not None and finish.reason in {"stop", "tool_calls"}:
            for event in raw_calls:
                try:
                    tool_ref = self._tool_names.resolve(event.name, prepared.tools)
                except AgentRuntimeError:
                    if not self._candidate_repair or not isinstance(
                        self._tool_names, ExactToolNames
                    ):
                        raise
                    raise CandidateRejected(
                        attempt_id=request.attempt_id,
                        snapshot_id=prepared.snapshot_id,
                        reason="unknown_tool",
                        detail=event.name,
                    ) from None
                if tool_ref == ASK_USER_CONTROL:
                    try:
                        questions.append(AskUserPayload.model_validate(event.arguments))
                    except ValueError as exc:
                        if self._candidate_repair:
                            raise CandidateRejected(
                                attempt_id=request.attempt_id,
                                snapshot_id=prepared.snapshot_id,
                                reason="invalid_arguments",
                                detail="Clarification arguments do not match the schema.",
                            ) from exc
                        raise runtime_failure(
                            "INVALID_INPUT",
                            category="validation",
                            public_message="The clarification control arguments are invalid.",
                        ) from exc
                    continue
                tool_calls.append(
                    ToolCallRequest(
                        tool_call_id=uuid5(NAMESPACE_URL, f"{request.attempt_id}:{event.call_id}"),
                        tool=tool_ref,
                        arguments=event.arguments,
                    )
                )
        if finish is not None and finish.reason == "cancelled":
            raise runtime_failure(
                "CONTROL_PENDING", category="concurrency", public_message="Run was cancelled."
            )
        if finish is None or finish.reason not in {"stop", "tool_calls"}:
            raise runtime_failure(
                "INCOMPLETE_MODEL_OUTPUT",
                category="provider",
                public_message="The model decision did not finish completely.",
            )
        await self._access.verify(context_request.actor, prepared)
        if setup.mode == "structured":
            if finish.reason != "stop" or tool_calls or questions:
                raise runtime_failure(
                    "INCOMPLETE_MODEL_OUTPUT",
                    category="provider",
                    public_message="Structured decision did not finish cleanly.",
                )
            try:
                proposal = json.loads("".join(content))
                action = decision_adapter.validate_python(proposal)
            except (json.JSONDecodeError, ValueError, TypeError) as exc:
                if self._candidate_repair:
                    raise CandidateRejected(
                        attempt_id=request.attempt_id,
                        snapshot_id=prepared.snapshot_id,
                        reason="invalid_decision",
                        detail="Return one valid decision object matching the advertised schema.",
                    ) from exc
                raise runtime_failure(
                    "INVALID_INPUT",
                    category="validation",
                    public_message="The proposed structured decision is invalid.",
                ) from exc
            if isinstance(action, (Respond, InvokeTools)):
                raise runtime_failure(
                    "INVALID_INPUT",
                    category="validation",
                    public_message="Answer and tool calls require the native model channel.",
                )
        elif finish.reason == "tool_calls":
            if questions:
                if len(questions) != 1 or tool_calls:
                    if self._candidate_repair:
                        raise CandidateRejected(
                            attempt_id=request.attempt_id,
                            snapshot_id=prepared.snapshot_id,
                            reason="invalid_decision",
                            detail="Submit one clarification or business tool calls, not both.",
                        )
                    raise runtime_failure(
                        "INVALID_INPUT",
                        category="validation",
                        public_message="Clarification cannot be mixed with business tool calls.",
                    )
                action = AskUser(kind="ask_user", payload=questions[0])
            elif not tool_calls:
                raise runtime_failure(
                    "INCOMPLETE_MODEL_OUTPUT",
                    category="provider",
                    public_message="The model announced tool calls without completed arguments.",
                )
            else:
                action = InvokeTools(
                    kind="invoke_tools",
                    payload=InvokeToolsPayload(validated_calls=tuple(tool_calls)),
                )
        else:
            if tool_calls or questions or not "".join(content).strip():
                raise runtime_failure(
                    "INCOMPLETE_MODEL_OUTPUT",
                    category="provider",
                    public_message="The model did not produce a complete answer.",
                )
            text = "".join(content)
            bare = text.strip()
            if (
                self._candidate_repair
                and request.tools
                and bare.startswith("<tool_call>")
                and bare.endswith("</tool_call>")
            ):
                raise CandidateRejected(
                    attempt_id=request.attempt_id,
                    snapshot_id=prepared.snapshot_id,
                    reason="invalid_decision",
                    detail="The provider returned a native tool envelope as answer text. "
                    "Use the declared native tool channel with the exact current function name, "
                    "or answer in ordinary prose if the requested work is complete. "
                    "No tool was executed from that text.",
                )
            answer_ref = f"answer-{content_digest({'runId': str(state.run_id), 'text': text})}"
            action = Respond(kind="respond", payload=RespondPayload(answer_request_ref=answer_ref))
        checked = validate_candidate_decision(state, prepared, budget, action, now=self._clock())
        required = context_request.required_tool
        if (context_request.require_tool_call and not isinstance(checked, InvokeTools)) or (
            required is not None
            and (
                not isinstance(checked, InvokeTools)
                or any(call.tool != required for call in checked.payload.validated_calls)
            )
        ):
            if self._candidate_repair:
                raise CandidateRejected(
                    attempt_id=request.attempt_id,
                    snapshot_id=prepared.snapshot_id,
                    reason="invalid_decision",
                    detail=(
                        "Use the required visible native tool selection; "
                        "prose is not a saved result."
                    ),
                )
            raise runtime_failure(
                "INCOMPLETE_MODEL_OUTPUT",
                category="provider",
                public_message="The model did not complete the required native tool call.",
            )
        if setup.validate_action is not None:
            try:
                setup.validate_action(checked, prepared)
            except AgentRuntimeError as exc:
                if self._candidate_repair and exc.failure.category == "validation":
                    raise CandidateRejected(
                        attempt_id=request.attempt_id,
                        snapshot_id=prepared.snapshot_id,
                        reason="invalid_decision",
                        detail=exc.failure.public_message,
                    ) from exc
                raise
        if isinstance(checked, Respond):
            await self._answers.save(state.scope_id, str(state.run_id), answer_ref, text)
        return DecisionFrame(
            actor=context_request.actor,
            context=prepared,
            attempt_id=request.attempt_id,
            action=checked,
        )

    @staticmethod
    def _validate_setup(state: RunState, budget: BudgetView, setup: DecisionSetup) -> None:
        request = setup.context_request
        if (
            request.actor.scope_id != state.scope_id
            or request.session_id != state.session_id
            or request.run_id != state.run_id
            or request.budget.allocation_id != budget.allocation_id
            or request.model != setup.model.ref
            or setup.model.max_output_tokens > request.budget.reserved_output_tokens
            or not (request.user_input or "").strip()
            or (setup.mode == "native" and setup.output_schema is not None)
            or (request.required_tool is not None and setup.mode != "native")
            or (request.require_tool_call and (setup.mode != "native" or not request.tools))
            or (request.required_tool is not None and request.required_tool not in request.tools)
        ):
            raise runtime_failure(
                "INVALID_INPUT",
                category="context",
                public_message="Loop setup is not pinned to the run.",
            )

    @staticmethod
    def _model_request(
        state: RunState,
        budget: BudgetView,
        model: ModelConfig,
        context: PreparedContext,
        output_schema: CapabilityRef | None = None,
    ) -> ProviderRequest:
        return ProviderRequest(
            scope_id=state.scope_id,
            session_id=state.session_id,
            run_id=state.run_id,
            attempt_id=uuid4(),
            context_snapshot_id=context.snapshot_id,
            context_version=context.context_version,
            input_ref=context.payload_ref,
            input_digest=context.content_hash,
            estimated_input_tokens=context.estimated_input_tokens,
            budget_allocation_id=budget.allocation_id,
            deadline_at=budget.deadline_at,
            model=model,
            tools=context.tools,
            output_schema=output_schema,
            purpose=context.purpose,
        )

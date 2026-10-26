"""A complete provider stream is required before the loop returns an action."""

from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
from test_context_runtime import request, runtime
from test_decision_validation import NOW, TOOL, facts

from agent_runtime.agent_loop import DecisionSetup, ModelAgentLoop
from agent_runtime.contracts.context import ContextRequest, PreparedContext
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.loop import AppliedInputs, BudgetView, RunState
from agent_runtime.contracts.model import ModelConfig, ProviderRequest
from agent_runtime.contracts.provider import (
    ContentDelta,
    ProviderEvent,
    ProviderFinish,
    ToolCallComplete,
)
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.native_controls import ASK_USER_CONTROL
from agent_runtime.ports import CancelSignal
from agent_runtime.tool_names import provider_tool_name


class Setup:
    def __init__(self, context_request: ContextRequest, *, mode: str = "native"):
        self.context_request = context_request
        self.mode = mode

    async def resolve(
        self, state: RunState, inputs: AppliedInputs, budget: BudgetView
    ) -> DecisionSetup:
        model = ModelConfig.model_validate(
            {
                "ref": self.context_request.model.model_dump(by_alias=True),
                "connection": self.context_request.model.model_dump(by_alias=True),
                "modelName": "model",
                "maxOutputTokens": 100,
                "thinking": "default",
                "providerOptions": {},
            }
        )
        return DecisionSetup(self.context_request, model, self.mode)  # type: ignore[arg-type]


class Access:
    def __init__(self, *, deny_after_first: bool = False):
        self.denied = False
        self.checked = 0
        self.deny_after_first = deny_after_first

    async def verify(self, actor: ActorContext, snapshot: PreparedContext) -> None:
        self.checked += 1
        if self.denied or (self.deny_after_first and self.checked > 2):
            raise runtime_failure(
                "FORBIDDEN", category="authorization", public_message="Access revoked."
            )


@pytest.mark.asyncio
async def test_profile_candidate_validation_runs_before_answer_persistence():
    from dataclasses import replace

    loop, state, inputs, budget, backend, answers = build("answer")
    resolve = loop._setup.resolve

    def reject(action, snapshot):
        raise runtime_failure(
            "FORBIDDEN", category="authorization", public_message="Unbound candidate"
        )

    class BoundSetup:
        async def resolve(self, *args):
            return replace(await resolve(*args), validate_action=reject)

    loop._setup = BoundSetup()
    with pytest.raises(AgentRuntimeError) as error:
        await loop.decide(state, inputs, budget)
    assert error.value.failure.code == "FORBIDDEN"
    assert not answers.saved and backend.opened == 1


class Answers:
    def __init__(self):
        self.saved: list[str] = []

    async def save(self, scope_id: str, run_id: str, ref: str, text: str) -> None:
        self.saved.append(text)


class Events:
    def __init__(self):
        self.kinds: list[str] = []

    async def record(self, request: ProviderRequest, event: ProviderEvent) -> None:
        self.kinds.append(event.kind)


class Backend:
    def __init__(self, kind: str):
        self.kind = kind
        self.opened = 0

    async def stream(
        self, request: ProviderRequest, *, cancel: CancelSignal | None = None
    ) -> AsyncIterator[ProviderEvent]:
        self.opened += 1
        if self.kind in {"answer", "revoked"}:
            yield ContentDelta(
                attempt_id=request.attempt_id, sequence="0", kind="content.delta", text="Hello"
            )
            yield ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="finish",
                reason="stop",
                raw_reason=None,
            )
        elif self.kind in {"tool", "truncated"}:
            yield ToolCallComplete(
                attempt_id=request.attempt_id,
                sequence="0",
                kind="tool.complete",
                call_id="call-1",
                name=provider_tool_name(request.tools[0]),
                arguments={"query": "hello"},
            )
            if self.kind == "tool":
                yield ProviderFinish(
                    attempt_id=request.attempt_id,
                    sequence="1",
                    kind="finish",
                    reason="tool_calls",
                    raw_reason=None,
                )
        elif self.kind in {"native_question", "mixed_question", "truncated_question"}:
            yield ToolCallComplete(
                attempt_id=request.attempt_id,
                sequence="0",
                kind="tool.complete",
                call_id="question",
                name=provider_tool_name(ASK_USER_CONTROL),
                arguments={"question": "Which quarter?", "requiredFacts": ["quarter"]},
            )
            if self.kind == "mixed_question":
                yield ToolCallComplete(
                    attempt_id=request.attempt_id,
                    sequence="1",
                    kind="tool.complete",
                    call_id="effect",
                    name=provider_tool_name(request.tools[1]),
                    arguments={"query": "x"},
                )
            if self.kind != "truncated_question":
                yield ProviderFinish(
                    attempt_id=request.attempt_id,
                    sequence="2",
                    kind="finish",
                    reason="tool_calls",
                    raw_reason=None,
                )
        elif self.kind == "length":
            yield ContentDelta(
                attempt_id=request.attempt_id, sequence="0", kind="content.delta", text="partial"
            )
            yield ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="finish",
                reason="length",
                raw_reason=None,
            )
        elif self.kind in {"ask_user", "bad_structured"}:
            text = (
                '{"kind":"ask_user","payload":{"question":"Which period?","requiredFacts":[]}}'
                if self.kind == "ask_user"
                else "{broken"
            )
            yield ContentDelta(
                attempt_id=request.attempt_id, sequence="0", kind="content.delta", text=text
            )
            yield ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="finish",
                reason="stop",
                raw_reason=None,
            )
        elif self.kind == "cancelled":
            yield ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="0",
                kind="finish",
                reason="cancelled",
                raw_reason=None,
            )


def build(
    kind: str, *, tool_budget: int = 2
) -> tuple[ModelAgentLoop, RunState, AppliedInputs, BudgetView, Backend, Answers]:
    state, _, budget = facts(tool_budget=tool_budget)
    req = request(max_input_tokens=10000, refs=0)
    req = req.model_copy(
        update={
            "session_id": state.session_id,
            "run_id": state.run_id,
            "model": ModelConfig.model_validate(
                {
                    "ref": req.model.model_dump(by_alias=True),
                    "connection": req.model.model_dump(by_alias=True),
                    "modelName": "model",
                    "maxOutputTokens": 100,
                    "thinking": "default",
                    "providerOptions": {},
                }
            ).ref,
            "budget": req.budget.model_copy(update={"allocation_id": budget.allocation_id}),
            "user_input": "What is this?",
            "prompt_text": "Answer the user's question or call a visible tool.",
            "tools": (
                (type(state.agent).model_validate(TOOL),) if kind in {"tool", "truncated"} else ()
            ),
        }
    )
    if kind in {"native_question", "mixed_question", "truncated_question"}:
        req = req.model_copy(
            update={"tools": (ASK_USER_CONTROL, type(state.agent).model_validate(TOOL))}
        )
    context, _, _ = runtime()
    backend = Backend(kind)
    answers = Answers()
    loop = ModelAgentLoop(
        context=context,
        access=Access(deny_after_first=kind == "revoked"),
        provider=backend,
        setup=Setup(req, mode="structured" if kind in {"ask_user", "bad_structured"} else "native"),
        answers=answers,
        events=Events(),
        clock=lambda: NOW,
    )
    inputs = AppliedInputs(
        run_id=state.run_id,
        checkpoint_ref=state.checkpoint_ref,
        previous_seq="0",
        applied_seq="0",
        messages=(),
    )
    return loop, state, inputs, budget, backend, answers


@pytest.mark.asyncio
async def test_native_question_is_a_runtime_action_not_a_business_tool():
    loop, state, inputs, budget, _, answers = build("native_question", tool_budget=0)
    result = await loop.advance(state, inputs, budget)
    assert result.kind == "ask_user" and result.payload.question == "Which quarter?"
    assert not answers.saved


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind,code",
    [("mixed_question", "INVALID_INPUT"), ("truncated_question", "INCOMPLETE_MODEL_OUTPUT")],
)
async def test_native_control_never_mixes_effects_or_accepts_truncated_output(kind, code):
    loop, state, inputs, budget, _, answers = build(kind)
    with pytest.raises(AgentRuntimeError) as error:
        await loop.advance(state, inputs, budget)
    assert error.value.failure.code == code
    assert not answers.saved


@pytest.mark.asyncio
async def test_native_control_requires_explicit_context_visibility():
    loop, state, inputs, budget, _, _ = build("native_question")
    loop._setup.context_request = loop._setup.context_request.model_copy(update={"tools": ()})
    with pytest.raises(AgentRuntimeError) as error:
        await loop.advance(state, inputs, budget)
    assert error.value.failure.code == "FORBIDDEN"


@pytest.mark.asyncio
async def test_complete_answer_is_saved_and_returned():
    loop, state, inputs, budget, backend, answers = build("answer")
    result = await loop.advance(state, inputs, budget)
    assert result.kind == "respond"
    assert answers.saved == ["Hello"]
    assert backend.opened == 1


@pytest.mark.asyncio
async def test_complete_tool_call_is_only_a_candidate():
    loop, state, inputs, budget, _, answers = build("tool")
    result = await loop.advance(state, inputs, budget)
    assert result.kind == "invoke_tools"
    assert result.payload.validated_calls[0].tool.key.key == "read"
    assert not answers.saved


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["truncated", "length", "empty"])
async def test_incomplete_model_never_returns_action(kind: str):
    loop, state, inputs, budget, _, answers = build(kind)
    with pytest.raises(AgentRuntimeError) as exc:
        await loop.advance(state, inputs, budget)
    assert exc.value.failure.code == "INCOMPLETE_MODEL_OUTPUT"
    assert not answers.saved


@pytest.mark.asyncio
async def test_budget_exhaustion_prevents_provider_open():
    loop, state, inputs, budget, backend, _ = build("answer")
    exhausted = budget.model_copy(update={"deadline_at": NOW - timedelta(seconds=1)})
    with pytest.raises(AgentRuntimeError):
        await loop.advance(state, inputs, exhausted)
    assert backend.opened == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("dimension", ["model_calls", "tokens"])
async def test_unknown_usage_reservation_is_not_reported_as_spent_budget(dimension):
    loop, state, inputs, budget, backend, _ = build("answer")
    counter = getattr(budget, dimension)
    blocked = counter.model_copy(
        update={
            "spent": "0",
            "reserved": counter.limit,
            "unknown_reserved": counter.limit,
        }
    )
    with pytest.raises(AgentRuntimeError) as error:
        await loop.advance(state, inputs, budget.model_copy(update={dimension: blocked}))
    assert error.value.failure.code == "USAGE_UNRESOLVED"
    assert error.value.failure.retryable is False
    assert "settle" in error.value.failure.public_message
    assert backend.opened == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("reservation", [False, True])
async def test_known_budget_exhaustion_keeps_its_original_code(reservation):
    loop, state, inputs, budget, backend, _ = build("answer")
    calls = budget.model_calls
    exhausted = calls.model_copy(
        update={
            "spent": "0" if reservation else calls.limit,
            "reserved": calls.limit if reservation else "1",
            "unknown_reserved": "0" if reservation else "1",
        }
    )
    with pytest.raises(AgentRuntimeError) as error:
        await loop.advance(state, inputs, budget.model_copy(update={"model_calls": exhausted}))
    assert error.value.failure.code == "BUDGET_EXHAUSTED"
    assert backend.opened == 0


@pytest.mark.asyncio
async def test_structured_clarification_is_not_mistaken_for_answer():
    loop, state, inputs, budget, _, answers = build("ask_user")
    action = await loop.advance(state, inputs, budget)
    assert action.kind == "ask_user"
    assert action.payload.question == "Which period?"
    assert not answers.saved


@pytest.mark.asyncio
async def test_invalid_structured_output_is_not_an_action():
    loop, state, inputs, budget, _, answers = build("bad_structured")
    with pytest.raises(AgentRuntimeError) as exc:
        await loop.advance(state, inputs, budget)
    assert exc.value.failure.code == "INVALID_INPUT"
    assert not answers.saved


@pytest.mark.asyncio
async def test_cancelled_stream_has_control_reason_not_model_truncation():
    loop, state, inputs, budget, _, answers = build("cancelled")
    with pytest.raises(AgentRuntimeError) as exc:
        await loop.advance(state, inputs, budget)
    assert exc.value.failure.code == "CONTROL_PENDING"
    assert not answers.saved


@pytest.mark.asyncio
async def test_context_revoke_during_model_call_prevents_final_action():
    loop, state, inputs, budget, backend, answers = build("revoked")
    with pytest.raises(AgentRuntimeError) as exc:
        await loop.advance(state, inputs, budget)
    assert exc.value.failure.code == "FORBIDDEN"
    assert backend.opened == 1
    assert not answers.saved


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("version", "unchanged", "actor", "twice", "denied"))
async def test_context_refresh_is_trusted_bounded_and_does_not_relax_authority(change):
    from test_context_runtime import Estimator, Payloads, Snapshots

    from agent_runtime.context_runtime import ContextRuntime, LoadedSource

    loop, state, inputs, budget, backend, answers = build("answer")
    req = loop._setup.context_request
    source = request(refs=1).resource_refs[0]
    req = req.model_copy(update={"resource_refs": (source,)})

    class RefreshSetup(Setup):
        calls = 0

        async def resolve(self, state, inputs, budget):
            self.calls += 1
            self.context_request = req
            if self.calls > 1:
                if change != "unchanged":
                    self.context_request = req.model_copy(
                        update={
                            "resource_refs": (source.model_copy(update={"resource_version": "2"}),)
                        }
                    )
                if change == "actor":
                    self.context_request = self.context_request.model_copy(
                        update={"actor": req.actor.model_copy(update={"actor_id": "other"})}
                    )
            return await super().resolve(state, inputs, budget)

    class ChangingReader:
        async def read(self, request, resource):
            if change == "denied":
                raise runtime_failure(
                    "FORBIDDEN", category="authorization", public_message="Denied"
                )
            if resource.resource_version == "1" or change == "twice":
                raise runtime_failure(
                    "VERSION_CONFLICT", category="context", public_message="Source changed"
                )
            return LoadedSource(resource=resource, content="fresh", required=True)

    setup = RefreshSetup(req)
    loop._setup = setup
    loop._context = ContextRuntime(
        reader=ChangingReader(), estimator=Estimator(), payloads=Payloads(), snapshots=Snapshots()
    )
    if change == "version":
        result = await loop.decide(state, inputs, budget)
        assert result.context.sources[0].resource.resource_version == "2"
        assert answers.saved == ["Hello"]
        assert backend.opened == 1
    else:
        with pytest.raises(AgentRuntimeError) as error:
            await loop.decide(state, inputs, budget)
        assert error.value.failure.code == (
            "FORBIDDEN" if change == "denied" else "VERSION_CONFLICT"
        )
        assert backend.opened == 0
        assert not answers.saved
    assert setup.calls == (1 if change == "denied" else 2)

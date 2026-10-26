"""A single bounded decision, not a durable Run or permission to invoke a tool."""

import argparse
import asyncio
import os
from collections.abc import AsyncIterator, Mapping
from datetime import UTC, datetime, timedelta

import httpx
from pydantic import JsonValue

from agent_runtime import DecisionSetup, ModelAgentLoop
from agent_runtime.contracts.context import ContextRequest
from agent_runtime.contracts.loop import AppliedInputs, BudgetCounter, BudgetView, RunState
from agent_runtime.contracts.model import ModelConfig, ProviderRequest
from agent_runtime.contracts.provider import (
    ContentDelta,
    ProviderEvent,
    ProviderFinish,
    ToolCallComplete,
)
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.state import RunStatus
from agent_runtime.ports import CancelSignal, Provider
from agent_runtime.providers.context_input import ContextOpenAIInputResolver
from agent_runtime.providers.openai_compatible import (
    OpenAIAdapterProfile,
    OpenAICompatibleProvider,
    ProviderConnection,
)
from agent_runtime.tool_names import provider_tool_name
from runtime_examples.agent_config import model_config, ref
from runtime_examples.context import actor, build_context, request
from runtime_examples.tools import MultiplyInput, tool_spec


class Setup:
    def __init__(self, context_request: ContextRequest, model: ModelConfig) -> None:
        self.request, self.model = context_request, model

    async def resolve(
        self, state: RunState, inputs: AppliedInputs, budget: BudgetView
    ) -> DecisionSetup:
        return DecisionSetup(self.request, self.model)


class Answers:
    def __init__(self) -> None:
        self.texts: dict[tuple[str, str, str], str] = {}

    async def save(self, scope_id: str, run_id: str, ref: str, text: str) -> None:
        key = scope_id, run_id, ref
        if key in self.texts and self.texts[key] != text:
            raise ValueError("Answer identity reused with different bytes")
        self.texts[key] = text


class Events:
    def __init__(self, *, echo: bool = False) -> None:
        self.events: list[ProviderEvent] = []
        self.echo = echo

    async def record(self, request: ProviderRequest, event: ProviderEvent) -> None:
        self.events.append(event)
        if self.echo:
            print(event.model_dump_json(by_alias=True), flush=True)


class ScriptedProvider:
    """Offline protocol fixture, intentionally not called a real model test."""

    def __init__(self, *, tool_decision: bool = False) -> None:
        self.tool_decision = tool_decision

    async def stream(
        self, request: ProviderRequest, *, cancel: CancelSignal | None = None
    ) -> AsyncIterator[ProviderEvent]:
        if self.tool_decision:
            yield ToolCallComplete(
                attempt_id=request.attempt_id,
                sequence="0",
                kind="tool.complete",
                call_id="multiply-001",
                name=provider_tool_name(tool_spec().ref),
                arguments={"left": 23.0, "right": 17.0},
            )
            yield ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="finish",
                reason="tool_calls",
                raw_reason=None,
            )
            return
        yield ContentDelta(
            attempt_id=request.attempt_id, sequence="0", kind="content.delta", text="Hello "
        )
        yield ContentDelta(
            attempt_id=request.attempt_id, sequence="1", kind="content.delta", text="from the SDK."
        )
        yield ProviderFinish(
            attempt_id=request.attempt_id,
            sequence="2",
            kind="finish",
            reason="stop",
            raw_reason=None,
        )


class ToolDefinitions:
    async def definition(self, ref: CapabilityRef) -> Mapping[str, JsonValue]:
        spec = tool_spec()
        if ref != spec.ref:
            raise ValueError("Unconfigured tool revision")
        return {
            "type": "function",
            "function": {
                "name": provider_tool_name(ref),
                "description": spec.description,
                "parameters": MultiplyInput.model_json_schema(),
            },
        }


async def demo(
    *,
    live: bool = False,
    thinking: bool = False,
    echo: bool = False,
    tool_decision: bool = False,
) -> dict[str, object]:
    req = request(
        "Use multiply to calculate 23 times 17." if tool_decision else "Say hello briefly."
    )
    if tool_decision:
        req = req.model_copy(update={"tools": (tool_spec().ref,)})
    context, access = build_context()
    answers, events = Answers(), Events(echo=echo)
    counter = BudgetCounter(limit="16", spent="0", reserved="0", unknown_reserved="0")
    budget = BudgetView(
        allocation_id=req.budget.allocation_id,
        revision="0",
        model_calls=counter,
        tool_calls=counter,
        tokens=counter.model_copy(update={"limit": "30000"}),
        deadline_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    state = RunState(
        scope_id=req.actor.scope_id,
        session_id=req.session_id,
        run_id=req.run_id,
        state_version="0",
        checkpoint_ref="local-checkpoint",
        agent=ref("analyst"),
        status=RunStatus.RUNNING,
        desired_control=None,
        cancel_version="0",
        pending_input_seq="0",
        applied_input_seq="0",
    )
    inputs = AppliedInputs(
        run_id=state.run_id,
        checkpoint_ref=state.checkpoint_ref,
        previous_seq="0",
        applied_seq="0",
        messages=(),
    )
    model = model_config(os.environ.get("CENTN_MODEL", "configured-model"), thinking=thinking)
    async with httpx.AsyncClient(timeout=240, trust_env=False) as client:
        provider: Provider = ScriptedProvider(tool_decision=tool_decision)
        if live:
            connection = ProviderConnection(
                base_url=os.environ["CENTN_PROVIDER_URL"], api_key=os.environ["CENTN_PROVIDER_KEY"]
            )

            async def resolve_connection(request: ProviderRequest) -> ProviderConnection:
                if request.model != model:
                    raise ValueError("Unexpected model binding")
                return connection

            provider = OpenAICompatibleProvider(
                client,
                inputs=ContextOpenAIInputResolver(
                    context=context, actor=actor(), access=access, tools=ToolDefinitions()
                ),
                connections=resolve_connection,
                profile=OpenAIAdapterProfile.qwen(),
            )
        loop = ModelAgentLoop(
            context=context,
            access=access,
            provider=provider,
            setup=Setup(req, model),
            answers=answers,
            events=events,
        )
        frame = await loop.decide(state, inputs, budget)
    return {
        "mode": "real-provider" if live else "offline-scripted-provider",
        "action": frame.action.model_dump(mode="json", by_alias=True),
        "snapshotId": str(frame.context.snapshot_id),
        "answerTexts": list(answers.texts.values()),
        "eventKinds": [event.kind for event in events.events],
    }


if __name__ == "__main__":
    import json

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--thinking", action="store_true")
    parser.add_argument("--tool-decision", action="store_true")
    args = parser.parse_args()
    print(
        json.dumps(
            asyncio.run(
                demo(
                    live=args.live,
                    thinking=args.thinking,
                    echo=True,
                    tool_decision=args.tool_decision,
                )
            ),
            indent=2,
        )
    )

"""Tool declarations and local business logic, not a ToolHost authorization bypass."""

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.registry import CapabilityManifest, ToolCapability
from agent_runtime.contracts.tools import ToolInvocation, ToolSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.capability_registry import SqlCapabilityRegistry
from agent_runtime.storage.tool_host import ToolEffect
from agent_runtime.tool_schemas import PinnedToolSchemas
from runtime_examples.agent_config import build_agent, ref


class MultiplyInput(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    left: float = Field(ge=-1e12, le=1e12, allow_inf_nan=False)
    right: float = Field(ge=-1e12, le=1e12, allow_inf_nan=False)


class MultiplyOutput(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    value: float = Field(allow_inf_nan=False)


def calculate(arguments: MultiplyInput) -> MultiplyOutput:
    return MultiplyOutput(value=arguments.left * arguments.right)


def tool_spec() -> ToolSpec:
    return ToolSpec(
        ref=ref("multiply"),
        description="Multiply two bounded numbers.",
        source=ref("host"),
        kind="read",
        side_effect="none",
        input_schema=ref("multiply-input"),
        output_schema=ref("multiply-output"),
        permissions=(ref("read"),),
        approval_policy=ref("read-policy"),
        idempotency="read_only",
        timeout_seconds=30,
        concurrency_group="tutorial-read",
        max_concurrency=4,
        cancellation="stop",
        public_result_policy="full",
    )


def schemas() -> PinnedToolSchemas:
    return PinnedToolSchemas(
        {
            ref("multiply-input"): TypeAdapter(MultiplyInput),
            ref("multiply-output"): TypeAdapter(MultiplyOutput),
        }
    )


async def install(actor: ActorContext, registry: SqlCapabilityRegistry) -> None:
    spec = tool_spec()
    await registry.install(
        actor,
        CapabilityManifest(
            ref=spec.ref,
            status="draft",
            content_digest=content_digest(spec.model_dump(mode="json", by_alias=True)),
            spec=ToolCapability(kind="tool", spec=spec),
        ),
    )
    # Publishing does not replace Run ACL checks or handler registration.
    await registry.publish(actor, spec.ref)


class MultiplyHandler:
    """Install as a readonly handler in SqlToolHost, never call from a model directly."""

    async def invoke(self, invocation: ToolInvocation) -> ToolEffect:
        if invocation.call.tool != tool_spec().ref:
            raise ValueError("Unexpected tool revision")
        result = calculate(MultiplyInput.model_validate(invocation.call.arguments))
        return ToolEffect(result.model_dump(mode="json"), "Multiplication completed.")


if __name__ == "__main__":
    agent, spec = build_agent(), tool_spec()
    assert spec.ref in agent.tools
    schemas().input(spec, {"left": 23.0, "right": 17.0})
    print(calculate(MultiplyInput(left=23.0, right=17.0)).model_dump_json())
    print(spec.model_dump_json(by_alias=True, indent=2))

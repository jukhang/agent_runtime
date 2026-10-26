"""MCP declaration, reviewed discovery install and per-invocation Broker adapter."""

from uuid import UUID

from pydantic import JsonValue

from agent_runtime import Agent
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.tools import McpToolBinding, ToolSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.credentials.broker import CredentialBroker
from agent_runtime.credentials.ports import BrokerBlobs
from agent_runtime.mcp.client import McpClient, McpTransport
from agent_runtime.mcp.host import GrantIssuer, McpBrokerReadTool
from agent_runtime.mcp.install import McpCapabilityInstaller, McpCapabilityRegistry, McpToolSchemas
from runtime_examples.agent_config import build_agent, ref

INPUT: dict[str, JsonValue] = {
    "type": "object",
    "properties": {"factory": {"type": "string"}},
    "required": ["factory"],
    "additionalProperties": False,
}
OUTPUT: dict[str, JsonValue] = {"type": "object"}


def tool_spec() -> ToolSpec:
    return ToolSpec(
        ref=ref("read-metrics"),
        description="Read the selected factory's production metrics.",
        source=ref("factory-mcp"),
        kind="read",
        side_effect="none",
        input_schema=ref(content_digest(INPUT), "mcp.input"),
        output_schema=ref(content_digest(OUTPUT), "mcp.output"),
        permissions=(ref("factory-read"),),
        approval_policy=ref("read-policy"),
        idempotency="read_only",
        timeout_seconds=30,
        concurrency_group="factory-mcp",
        max_concurrency=4,
        cancellation="stop",
        public_result_policy="summary",
        mcp=McpToolBinding(
            protocol_version="2026-07-28",
            tool_name="read_metrics",
            destination=ref("metrics-endpoint"),
            input_schema=INPUT,
            output_schema=OUTPUT,
        ),
    )


def configured_agent() -> Agent:
    agent = build_agent()
    return agent.model_copy(update={"tools": (*agent.tools, tool_spec().ref)})


async def install_reviewed_tool(
    actor: ActorContext, registry: McpCapabilityRegistry, discovery: McpTransport
) -> UUID:
    # The host supplies a Broker-backed discovery grant, not a URL/token from the model.
    spec = tool_spec()
    return await McpCapabilityInstaller(McpClient(discovery), registry, source=spec.source).install(
        actor, spec
    )


def make_handler(
    broker: CredentialBroker, blobs: BrokerBlobs, issue_grant: GrantIssuer
) -> McpBrokerReadTool:
    spec = tool_spec()
    return McpBrokerReadTool(
        spec=spec, source=spec.source, broker=broker, blobs=blobs, issue_grant=issue_grant
    )


if __name__ == "__main__":
    spec = tool_spec()
    McpToolSchemas().input(spec, {"factory": "workshop-A"})
    print("Declaration validated only; no server connection or tool execution occurred.")
    print(configured_agent().model_dump_json(by_alias=True, indent=2))
    print(spec.model_dump_json(by_alias=True, indent=2))

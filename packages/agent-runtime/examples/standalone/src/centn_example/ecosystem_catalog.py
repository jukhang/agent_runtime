"""Reviewed ecosystem tools, schemas and policy. No model-provided endpoint or code."""

from datetime import UTC, datetime
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.policy import PolicyAllow
from agent_runtime.contracts.references import CapabilityKey, CapabilityRef
from agent_runtime.contracts.tools import ToolSpec
from agent_runtime.mcp.install import McpToolSchemas
from agent_runtime.policy_validation import policy_input_digest
from agent_runtime.tool_names import provider_tool_name
from centn_example.tools import ref

DATA_INPUT = {"type": "object", "properties": {}, "additionalProperties": False}
DATA_OUTPUT = {
    "type": "object",
    "required": ["resultType", "content", "structuredContent"],
    "properties": {
        "resultType": {"const": "complete"},
        "content": {"type": "array"},
        "structuredContent": {"type": "object"},
    },
}


class ChartArguments(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    argv: list[str] = Field(min_length=1, max_length=1)


class ChartOutput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    png_base64: str = Field(max_length=1400000)
    report: str = Field(max_length=65536)
    data: dict
    uid: int


def ecosystem_specs():
    shared = dict(
        source=ref("ecosystem"),
        permissions=(ref("read"),),
        approval_policy=ref("ecosystem-policy"),
        max_concurrency=2,
        cancellation="stop",
    )
    mcp = ToolSpec(
        ref=ref("read-metrics"),
        kind="read",
        side_effect="none",
        idempotency="read_only",
        input_schema=CapabilityRef(
            key=CapabilityKey(namespace="mcp.input", key=content_digest(DATA_INPUT)), revision="1"
        ),
        output_schema=CapabilityRef(
            key=CapabilityKey(namespace="mcp.output", key=content_digest(DATA_OUTPUT)), revision="1"
        ),
        description="Read current workshop metrics and dataset identity from the MCP service.",
        timeout_seconds=30,
        concurrency_group="ecosystem-mcp",
        public_result_policy="full",
        mcp={
            "protocolVersion": "2026-07-28",
            "destination": ref("metrics-endpoint"),
            "toolName": "read_metrics",
            "inputSchema": DATA_INPUT,
            "outputSchema": DATA_OUTPUT,
        },
        **shared,
    )
    chart = ToolSpec(
        ref=ref("chart-report"),
        kind="write",
        side_effect="external",
        idempotency="reconcile",
        input_schema=ref("chart-input"),
        output_schema=ref("chart-output"),
        description="Run the installed Git chart Skill in an isolated sandbox. argv must contain "
        "one JSON string with the exact structuredContent object returned by read-metrics. "
        "Creates a PNG bar chart and a text report; never accepts code or shell commands.",
        timeout_seconds=90,
        concurrency_group="ecosystem-chart",
        public_result_policy="summary",
        **shared,
    )
    return mcp, chart


class EcosystemCatalog:
    def __init__(self, base):
        self.base = base
        self.engine, self.schema = base.engine, base.schema
        self.spec = base.spec
        self.ecosystem_specs = ecosystem_specs()
        self.specs = (*base.specs, *self.ecosystem_specs)
        self.schemas = self
        self.arguments = TypeAdapter(ChartArguments)
        self.output_schema = TypeAdapter(ChartOutput)

    async def definition(self, capability):
        if capability not in {s.ref for s in self.ecosystem_specs}:
            return await self.base.definition(capability)
        spec = next(s for s in self.ecosystem_specs if s.ref == capability)
        return {
            "type": "function",
            "function": {
                "name": provider_tool_name(capability),
                "description": spec.description,
                "parameters": DATA_INPUT if spec.mcp else ChartArguments.model_json_schema(),
            },
        }

    async def get(self, call):
        return next((s for s in self.specs if s.ref == call.tool), None) or await self.base.get(
            call
        )

    async def resources(self, invocation, spec):
        return await self.base.resources(invocation, self.base.spec)

    async def invoke(self, invocation):
        return await self.base.invoke(invocation)

    async def evaluate(self, request):
        if request.invocation.call.tool not in {s.ref for s in self.ecosystem_specs}:
            return await self.base.evaluate(request)
        if request.policy != ref("ecosystem-policy"):
            raise ValueError("Unconfigured ecosystem policy")
        return PolicyAllow(
            decision="allow",
            decision_id=uuid4(),
            policy=request.policy,
            input_digest=policy_input_digest(request),
            evidence_refs=request.resources,
            evaluated_at=datetime.now(UTC),
            expires_at=request.invocation.deadline_at,
            grant_ref="host-isolated-report-policy",
        )

    def input(self, spec, arguments):
        if spec.mcp:
            return McpToolSchemas().input(spec, arguments)
        if spec.ref == ref("chart-report"):
            return self.arguments.validate_python(arguments)
        return self.base.schemas.input(spec, arguments)

    def output(self, spec, data):
        if spec.mcp:
            return McpToolSchemas().output(spec, data)
        if spec.ref == ref("chart-report"):
            return self.output_schema.validate_python(data)
        return self.base.schemas.output(spec, data)

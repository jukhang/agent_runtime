"""Admin-reviewed MCP catalog installation into the immutable capability registry."""

from typing import Protocol
from uuid import UUID

from jsonschema import (  # type: ignore[import-untyped]
    Draft202012Validator,
    SchemaError,
    ValidationError,
)
from pydantic import JsonValue

from agent_runtime.contracts.canonical import canonical_bytes, content_digest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.registry import CapabilityManifest, ToolCapability
from agent_runtime.contracts.tools import ToolSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.mcp.client import McpClient


class McpCapabilityRegistry(Protocol):
    async def install(self, actor: ActorContext, manifest: CapabilityManifest) -> UUID: ...


class McpCapabilityInstaller:
    """Discovery is evidence, never execution authority or permission inference."""

    def __init__(
        self, client: McpClient, registry: McpCapabilityRegistry, *, source: CapabilityRef
    ) -> None:
        self._client, self._registry, self._source = client, registry, source

    async def install(self, actor: ActorContext, spec: ToolSpec) -> UUID:
        binding = spec.mcp
        if binding is None:
            raise ValueError("MCP installation requires a pinned binding")
        if spec.source != self._source:
            raise ValueError("MCP tool source differs from its fixed server")
        _validate_schema(binding.input_schema)
        _validate_schema(binding.output_schema)
        catalog = await self._client.test_connection()
        found = next((item for item in catalog if item.name == binding.tool_name), None)
        if found is None or content_digest(found.input_schema) != content_digest(
            binding.input_schema
        ):
            raise ValueError("MCP catalog differs from the reviewed tool schema")
        if binding.lookup_name is not None and not any(
            item.name == binding.lookup_name for item in catalog
        ):
            raise ValueError("MCP keyed-effect lookup tool is unavailable")
        if binding.cancel_name is not None and not any(
            item.name == binding.cancel_name for item in catalog
        ):
            raise ValueError("MCP cancellation tool is unavailable")
        manifest = CapabilityManifest(
            ref=spec.ref,
            status="draft",
            content_digest=content_digest(spec.model_dump(mode="json", by_alias=True)),
            spec=ToolCapability(kind="tool", spec=spec),
        )
        return await self._registry.install(actor, manifest)


class McpToolSchemas:
    """Validate MCP input and private output from the immutable ToolSpec itself."""

    def input(self, spec: ToolSpec, arguments: dict[str, JsonValue]) -> None:
        if spec.mcp is None:
            raise ValueError("tool has no MCP schema binding")
        _validate_value(spec.mcp.input_schema, arguments)

    def output(self, spec: ToolSpec, data: JsonValue) -> None:
        if spec.mcp is None:
            raise ValueError("tool has no MCP schema binding")
        _validate_value(spec.mcp.output_schema, data)


def _validate_schema(schema: dict[str, JsonValue]) -> None:
    if len(canonical_bytes(schema)) > 65_536:
        raise ValueError("MCP schema exceeds the installation limit")
    pending: list[tuple[JsonValue, int]] = [(schema, 0)]
    visited = 0
    while pending:
        value, depth = pending.pop()
        visited += 1
        if visited > 2048 or depth > 32:
            raise ValueError("MCP schema is too complex")
        if isinstance(value, dict):
            for key in ("$ref", "$dynamicRef", "$recursiveRef"):
                if key in value:
                    raise ValueError("MCP schema references are not supported")
            pending.extend((child, depth + 1) for child in value.values())
        elif isinstance(value, list):
            pending.extend((child, depth + 1) for child in value)
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise ValueError("MCP schema is invalid") from exc


def _validate_value(schema: dict[str, JsonValue], value: JsonValue) -> None:
    _validate_schema(schema)
    try:
        Draft202012Validator(schema).validate(value)
    except ValidationError as exc:
        raise ValueError("MCP tool data does not match its pinned schema") from exc


__all__ = ["McpCapabilityInstaller", "McpToolSchemas"]

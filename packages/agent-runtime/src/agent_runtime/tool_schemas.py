"""Host-owned, pinned Pydantic schemas for tool input and private output."""

from collections.abc import Mapping

from pydantic import JsonValue, TypeAdapter, ValidationError

from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.tools import ToolSpec


class PinnedToolSchemas:
    def __init__(self, schemas: Mapping[CapabilityRef, TypeAdapter[object]]) -> None:
        self._schemas = dict(schemas)

    def _check(self, ref: CapabilityRef, value: object) -> None:
        adapter = self._schemas.get(ref)
        if adapter is None:
            raise runtime_failure(
                "INVALID_INPUT",
                category="validation",
                public_message="The pinned tool schema is unavailable.",
            )
        try:
            adapter.validate_python(value)
        except ValidationError as exc:
            raise runtime_failure(
                "INVALID_INPUT",
                category="validation",
                public_message="Tool data does not match its pinned schema.",
            ) from exc

    def input(self, spec: ToolSpec, arguments: dict[str, JsonValue]) -> None:
        self._check(spec.input_schema, arguments)

    def output(self, spec: ToolSpec, data: JsonValue) -> None:
        self._check(spec.output_schema, data)


__all__ = ["PinnedToolSchemas"]

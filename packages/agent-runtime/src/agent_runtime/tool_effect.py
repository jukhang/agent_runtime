"""Storage-independent private Tool result value shared by execution adapters."""

from dataclasses import dataclass

from pydantic import JsonValue

from agent_runtime.contracts.references import ResourceRef


@dataclass(frozen=True, slots=True)
class ToolEffect:
    data: JsonValue
    summary: str
    refs: tuple[ResourceRef, ...] = ()

"""Declarative Agent configuration; execution belongs to AgentRuntime."""

from typing import Annotated, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, StringConstraints, field_validator, model_validator

from agent_runtime.contracts.base import Contract, Identifier
from agent_runtime.contracts.references import CapabilityRef


class Budget(Contract):
    max_model_calls: Annotated[int, Field(strict=True, ge=1)] = 32
    max_tool_calls: Annotated[int, Field(strict=True, ge=0)] = 24
    max_runtime_seconds: Annotated[int, Field(strict=True, ge=1)] = 1800


class Agent(Contract):
    id: Identifier
    name: Annotated[str, StringConstraints(min_length=1, max_length=255)]
    description: str = ""
    model: CapabilityRef | None = None
    instructions: tuple[str, ...] = ()
    prompt: CapabilityRef | None = None
    knowledge: tuple[CapabilityRef, ...] = ()
    tools: tuple[CapabilityRef, ...] = ()
    skills: tuple[CapabilityRef, ...] = ()
    subagents: tuple[CapabilityRef, ...] = ()
    hooks: tuple[CapabilityRef, ...] = ()
    policy: tuple[CapabilityRef, ...] = ()
    context: CapabilityRef | None = None
    memory: CapabilityRef | None = None
    input_schema: CapabilityRef | None = None
    output_schema: CapabilityRef | None = None
    budget: Budget = Field(default_factory=Budget)
    add_datetime_to_context: Annotated[bool, Field(strict=True)] = False
    timezone: str = "UTC"

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError("timezone must be an available IANA timezone") from exc
        return value

    @model_validator(mode="after")
    def unique_bindings(self) -> Self:
        for field in ("knowledge", "tools", "skills", "subagents", "hooks", "policy"):
            bindings: tuple[CapabilityRef, ...] = getattr(self, field)
            keys = [binding.key for binding in bindings]
            if len(keys) != len(set(keys)):
                raise ValueError(f"duplicate capability key in {field}")
        return self

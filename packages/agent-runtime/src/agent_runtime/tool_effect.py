"""Storage-independent private Tool result value shared by execution adapters."""

from dataclasses import dataclass

from pydantic import JsonValue

from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.references import ResourceRef


class DatabaseToolRejected(Exception):
    """Known domain rejection; only the SQL executor can prove its rollback."""

    def __init__(self, failure: FailureEnvelope) -> None:
        super().__init__(failure.public_message)
        self.failure = failure


@dataclass(frozen=True, slots=True)
class ToolEffect:
    data: JsonValue
    summary: str
    refs: tuple[ResourceRef, ...] = ()

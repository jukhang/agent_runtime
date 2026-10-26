"""Discriminated terminal results, distinct from a wait timeout or active view."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, TypeAdapter, model_validator

from agent_runtime.contracts.base import Contract, Revision
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.views import TimelineItem


class Usage(Contract):
    """Reported token usage and *how* it is known (DS-P02-00 frozen matrix).

    ``reason`` records provenance, not accuracy: a provider report, an absent provider
    report, a failed collection, or a runtime estimate. Unknown usage must never be
    settled as zero, so the status/reason pair is constrained:

    - ``complete`` requires a provider report plus all counts;
    - ``partial`` requires a provider report or an estimate;
    - ``unknown`` forbids a provider report.
    """

    status: Literal["complete", "partial", "unknown"]
    reason: Literal["provider_reported", "provider_not_reported", "collection_failed", "estimated"]
    input_tokens: Revision | None
    output_tokens: Revision | None
    total_tokens: Revision | None

    @model_validator(mode="after")
    def consistent_usage(self) -> Self:
        counts = (self.input_tokens, self.output_tokens, self.total_tokens)
        allowed = {
            "complete": {"provider_reported"},
            "partial": {"provider_reported", "estimated"},
            "unknown": {"provider_not_reported", "collection_failed", "estimated"},
        }
        if self.reason not in allowed[self.status]:
            raise ValueError("usage reason does not match the usage status")
        if self.status == "complete" and any(value is None for value in counts):
            raise ValueError("complete usage requires all counts")
        if self.status == "partial" and all(value is None for value in counts):
            raise ValueError("partial usage requires at least one reported count")
        if (
            self.status == "unknown"
            and self.reason != "estimated"
            and any(value is not None for value in counts)
        ):
            raise ValueError("unknown usage carries no provider-reported counts")
        if all(value is not None for value in counts):
            if int(self.input_tokens or "0") + int(self.output_tokens or "0") != int(
                self.total_tokens or "0"
            ):
                raise ValueError("usage counts disagree")
        return self


class TerminalResult(Contract):
    run_id: UUID
    final_items: tuple[TimelineItem, ...]
    artifact_refs: tuple[ResourceRef, ...]
    partial_results: tuple[ResourceRef, ...]
    usage: Usage

    @model_validator(mode="after")
    def finished_items(self) -> Self:
        ids = [item.id for item in self.final_items]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate final item")
        if any(item.status in {"pending", "streaming"} for item in self.final_items):
            raise ValueError("terminal result cannot contain unfinished items")
        return self


class CompletedResult(TerminalResult):
    status: Literal["completed"]
    outcome: Literal["success", "partial"]
    failure: None


class FailedResult(TerminalResult):
    status: Literal["failed"]
    outcome: None
    failure: FailureEnvelope


class CancelledResult(TerminalResult):
    status: Literal["cancelled"]
    outcome: None
    failure: None


type RunResult = Annotated[
    CompletedResult | FailedResult | CancelledResult, Field(discriminator="status")
]
result_adapter: TypeAdapter[RunResult] = TypeAdapter(RunResult)

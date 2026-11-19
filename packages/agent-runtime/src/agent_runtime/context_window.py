"""Explicit host policies for retrievable tool-result working sets, not log deletion."""

from collections.abc import Callable, Mapping

from pydantic import JsonValue

from agent_runtime.context_runtime import LoadedSource
from agent_runtime.contracts.canonical import canonical_json, content_digest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.tools import FullResult, ToolCompleted
from agent_runtime.tool_transcript import NativeToolTranscript


class ToolResultWindow:
    """Offer older successful results as references; always retain the latest per tool.

    A registered projector must provide a bounded index and a callable re-read route.
    This is a trusted host policy, never permission to summarize arbitrary tool results.
    Original transcripts, effects, failures and call identities remain in storage.
    """

    def __init__(
        self,
        policies: Mapping[CapabilityRef, Callable[[JsonValue], JsonValue]],
        *,
        retain_latest: bool = True,
        retire_successful: bool = False,
    ) -> None:
        self._policies = dict(policies)
        self._retain_latest = retain_latest
        self._retire_successful = retire_successful

    def __call__(self, sources: tuple[LoadedSource, ...]) -> tuple[LoadedSource, ...]:
        eligible: dict[int, NativeToolTranscript] = {}
        latest: dict[CapabilityRef, int] = {}
        for index, source in enumerate(sources):
            if source.resource.resource_type != "tool_call_result":
                continue
            transcript = NativeToolTranscript.model_validate_json(source.content)
            if (
                transcript.call.tool not in self._policies
                or not isinstance(transcript.outcome, ToolCompleted)
                or not isinstance(transcript.outcome.result, FullResult)
            ):
                continue
            if str(transcript.call.tool_call_id) != source.resource.resource_id:
                raise ValueError("Offload transcript identity differs from its resource")
            eligible[index] = transcript
            latest[transcript.call.tool] = index
        result = list(sources)
        for index, transcript in eligible.items():
            if self._retain_latest and latest[transcript.call.tool] == index:
                continue
            assert isinstance(transcript.outcome, ToolCompleted)
            assert isinstance(transcript.outcome.result, FullResult)
            view: dict[str, JsonValue] = {
                "offloaded": True,
                "originalResource": sources[index].resource.model_dump(mode="json", by_alias=True),
                "originalContentDigest": content_digest({"content": sources[index].content}),
                "reread": {
                    "tool": transcript.call.tool.model_dump(mode="json", by_alias=True),
                    "arguments": transcript.call.arguments,
                },
                "index": self._policies[transcript.call.tool](transcript.outcome.result.data),
            }
            alternative = transcript.model_copy(
                update={
                    "outcome": transcript.outcome.model_copy(
                        update={
                            "result": FullResult(policy="full", data=view),
                        }
                    ),
                }
            )
            result[index] = sources[index].model_copy(
                update={
                    "retired": self._retire_successful,
                    "offloaded_content": canonical_json(
                        alternative.model_dump(mode="json", by_alias=True)
                    ),
                }
            )
        return tuple(result)

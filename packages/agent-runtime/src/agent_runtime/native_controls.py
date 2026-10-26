"""Pinned Runtime control definitions, never external ToolHost effects."""

from pydantic import JsonValue

from agent_runtime.contracts.decisions import AskUserPayload
from agent_runtime.contracts.references import CapabilityKey, CapabilityRef
from agent_runtime.tool_names import provider_tool_name

ASK_USER_CONTROL = CapabilityRef(
    key=CapabilityKey(namespace="runtime.control", key="ask-user"), revision="1"
)


def ask_user_definition() -> dict[str, JsonValue]:
    return {
        "type": "function",
        "function": {
            "name": provider_tool_name(ASK_USER_CONTROL),
            "description": (
                "Ask the user for essential missing task facts and durably wait for their reply. "
                "Call this alone before completing the answer when those facts are necessary. "
                "Do not use it to ask the user to assess your own answer."
            ),
            "parameters": AskUserPayload.model_json_schema(by_alias=True),
        },
    }

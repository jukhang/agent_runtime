"""A2A 1.0 JSON-RPC task client; remote effects are never blindly resent."""

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from pydantic import JsonValue

from agent_runtime.a2a.envelope import A2ADelegationEnvelope
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.registry import RemoteAgentSpec

TERMINAL = frozenset(
    {"TASK_STATE_COMPLETED", "TASK_STATE_FAILED", "TASK_STATE_CANCELED", "TASK_STATE_REJECTED"}
)
STATES = TERMINAL | {
    "TASK_STATE_SUBMITTED",
    "TASK_STATE_WORKING",
    "TASK_STATE_INPUT_REQUIRED",
    "TASK_STATE_AUTH_REQUIRED",
}


class A2ATransport(Protocol):
    async def request(self, method: str, params: Mapping[str, JsonValue]) -> JsonValue:
        """Authenticate via a pinned destination, not a URL from the Agent Card."""


@dataclass(frozen=True, slots=True)
class RemoteTask:
    task_id: str
    context_id: str
    state: str
    artifacts: tuple[JsonValue, ...]


@dataclass(frozen=True, slots=True)
class RemoteMessage:
    message_id: str
    context_id: str
    parts: tuple[JsonValue, ...]


def _reply(value: JsonValue) -> RemoteTask | RemoteMessage:
    if isinstance(value, dict) and "message" in value:
        message = value["message"]
        if not isinstance(message, dict):
            raise ValueError("A2A direct message is invalid")
        message_id, context_id, parts = (
            message.get("messageId"),
            message.get("contextId"),
            message.get("parts"),
        )
        if (
            not isinstance(message_id, str)
            or not message_id
            or not isinstance(context_id, str)
            or not context_id
            or not isinstance(parts, list)
        ):
            raise ValueError("A2A direct message identity or parts are invalid")
        return RemoteMessage(message_id, context_id, tuple(parts))
    return _task(value)


def _task(value: JsonValue, *, expected_id: str | None = None) -> RemoteTask:
    if not isinstance(value, dict):
        raise ValueError("A2A task response is invalid")
    raw = value.get("task") if "task" in value else value
    if not isinstance(raw, dict):
        raise ValueError("A2A response has no task status")
    status = raw.get("status")
    if not isinstance(status, dict):
        raise ValueError("A2A response has no task status")
    task_id, context_id = raw.get("id"), raw.get("contextId")
    state, artifacts = status.get("state"), raw.get("artifacts", [])
    if (
        not isinstance(task_id, str)
        or not task_id
        or (expected_id is not None and task_id != expected_id)
        or not isinstance(context_id, str)
        or not context_id
        or state not in STATES
        or not isinstance(artifacts, list)
    ):
        raise ValueError("A2A task identity or state is invalid")
    return RemoteTask(task_id, context_id, state, tuple(artifacts))


class A2AClient:
    def __init__(
        self,
        *,
        spec: RemoteAgentSpec,
        card: dict[str, JsonValue],
        transport: A2ATransport,
    ) -> None:
        if content_digest(card) != spec.card_digest:
            raise ValueError("A2A Agent Card differs from its pinned digest")
        interfaces = card.get("supportedInterfaces")
        if not isinstance(interfaces, list) or not any(
            isinstance(item, dict)
            and item.get("protocolBinding") == "JSONRPC"
            and item.get("protocolVersion") == "1.0"
            for item in interfaces
        ):
            raise ValueError("A2A Agent Card does not support pinned JSON-RPC 1.0")
        self._spec, self._transport = spec, transport

    async def send(
        self,
        *,
        message_id: str,
        context_id: str,
        parts: tuple[dict[str, JsonValue], ...],
        delegation: A2ADelegationEnvelope | None = None,
    ) -> RemoteTask | RemoteMessage:
        if not message_id or not context_id or not parts:
            raise ValueError("A2A message identity, context and parts are required")
        if delegation is not None and delegation.message_id != message_id:
            raise ValueError("A2A delegation identity differs from the message")
        message: dict[str, JsonValue] = {
            "messageId": message_id,
            "contextId": context_id,
            "role": "ROLE_USER",
            "parts": list(parts),
        }
        if delegation is not None:
            message["metadata"] = {
                "centn.delegation": delegation.model_dump(mode="json", by_alias=True)
            }
        response = await self._transport.request(
            "SendMessage",
            {"message": message},
        )
        return _reply(response)

    async def get(self, task_id: str) -> RemoteTask:
        if not self._spec.can_query_task:
            raise ValueError("A2A task lookup was not declared by this agent")
        return _task(await self._transport.request("GetTask", {"id": task_id}), expected_id=task_id)

    async def wait(
        self, task: RemoteTask, *, deadline_at: datetime, poll_seconds: float = 1.0
    ) -> RemoteTask:
        if deadline_at.utcoffset() is None or not 0 < poll_seconds <= 60:
            raise ValueError("A2A wait needs a bounded poll interval and aware deadline")
        current = task
        stop_states = TERMINAL | {"TASK_STATE_INPUT_REQUIRED", "TASK_STATE_AUTH_REQUIRED"}
        while current.state not in stop_states:
            remaining = (deadline_at - datetime.now(UTC)).total_seconds()
            if remaining <= 0:
                return current
            await asyncio.sleep(min(poll_seconds, remaining))
            current = await self.get(task.task_id)
            if current.context_id != task.context_id:
                raise ValueError("A2A task context changed while polling")
        return current

    async def send_input(
        self,
        task: RemoteTask,
        *,
        message_id: str,
        parts: tuple[dict[str, JsonValue], ...],
        delegation: A2ADelegationEnvelope,
    ) -> RemoteTask | RemoteMessage:
        if task.state != "TASK_STATE_INPUT_REQUIRED" or not message_id or not parts:
            raise ValueError("A2A task is not waiting for input")
        if delegation.message_id != message_id:
            raise ValueError("A2A input delegation identity differs from the message")
        reply = _reply(
            await self._transport.request(
                "SendMessage",
                {
                    "message": {
                        "messageId": message_id,
                        "contextId": task.context_id,
                        "taskId": task.task_id,
                        "role": "ROLE_USER",
                        "parts": list(parts),
                        "metadata": {
                            "centn.delegation": delegation.model_dump(mode="json", by_alias=True)
                        },
                    }
                },
            )
        )
        if isinstance(reply, RemoteTask) and reply.task_id != task.task_id:
            raise ValueError("A2A response belongs to another task")
        return reply

    async def cancel(self, task: RemoteTask) -> RemoteTask:
        if not self._spec.can_cancel_task:
            raise ValueError("A2A agent did not declare cancellation")
        if task.state in TERMINAL:
            return task
        response = await self._transport.request("CancelTask", {"id": task.task_id})
        # The response is a new observation, not proof that a remote process stopped.
        return _task(response, expected_id=task.task_id)

    def collect(self, task: RemoteTask) -> tuple[JsonValue, ...]:
        if task.state != "TASK_STATE_COMPLETED":
            raise ValueError("A2A task is not successfully complete")
        return task.artifacts


__all__ = ["A2AClient", "A2ATransport", "RemoteMessage", "RemoteTask", "STATES", "TERMINAL"]

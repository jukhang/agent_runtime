"""A2A 1.0 JSON-RPC boundary for a host-owned persistent task runtime."""

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from pydantic import JsonValue

from agent_runtime.a2a.envelope import A2ADelegationEnvelope


@dataclass(frozen=True, slots=True)
class A2ACaller:
    scope_id: str
    actor_id: str
    auth_revision: str


class A2AHost(Protocol):
    async def send(
        self,
        caller: A2ACaller,
        delegation: A2ADelegationEnvelope,
        message: Mapping[str, JsonValue],
    ) -> dict[str, JsonValue]:
        """Atomically dedup messageId, enforce scope/budget, and create or update a task."""

    async def get(self, caller: A2ACaller, task_id: str) -> dict[str, JsonValue]:
        """Read a task only after current ACL and scope checks."""

    async def cancel(self, caller: A2ACaller, task_id: str) -> dict[str, JsonValue]:
        """Persist cancellation intent; report current remote state, never assume stopped."""


class A2AServer:
    def __init__(self, *, host: A2AHost, card: dict[str, JsonValue]) -> None:
        interfaces = card.get("supportedInterfaces")
        if not isinstance(interfaces, list) or not any(
            isinstance(item, dict)
            and item.get("protocolBinding") == "JSONRPC"
            and item.get("protocolVersion") == "1.0"
            for item in interfaces
        ):
            raise ValueError("A2A server must publish a JSON-RPC 1.0 interface")
        self._host, self._card = host, deepcopy(card)

    def card(self) -> dict[str, JsonValue]:
        return deepcopy(self._card)

    async def dispatch(
        self, caller: A2ACaller, request: Mapping[str, JsonValue]
    ) -> dict[str, JsonValue]:
        if request.get("jsonrpc") != "2.0" or not isinstance(request.get("id"), (str, int)):
            raise ValueError("A2A request is not JSON-RPC 2.0")
        method, params = request.get("method"), request.get("params")
        if not isinstance(params, dict):
            raise ValueError("A2A request parameters are invalid")
        if method == "SendMessage":
            message = params.get("message")
            if (
                not isinstance(message, dict)
                or not isinstance(message.get("messageId"), str)
                or not message["messageId"]
                or message.get("role") != "ROLE_USER"
                or not isinstance(message.get("parts"), list)
                or not message["parts"]
            ):
                raise ValueError("A2A user message is invalid")
            metadata = message.get("metadata")
            if not isinstance(metadata, dict):
                raise ValueError("A2A delegation metadata is required")
            delegation = A2ADelegationEnvelope.model_validate(metadata.get("centn.delegation"))
            if (
                delegation.scope_id != caller.scope_id
                or delegation.message_id != message["messageId"]
                or delegation.deadline_at <= datetime.now(UTC)
            ):
                raise ValueError("A2A delegation identity or deadline is invalid")
            result: JsonValue = {"task": await self._host.send(caller, delegation, message)}
        elif method in {"GetTask", "CancelTask"}:
            task_id = params.get("id")
            if not isinstance(task_id, str) or not task_id:
                raise ValueError("A2A task identity is invalid")
            task = (
                await self._host.get(caller, task_id)
                if method == "GetTask"
                else await self._host.cancel(caller, task_id)
            )
            result = {"task": task}
        else:
            raise ValueError("A2A method is not supported")
        return {"jsonrpc": "2.0", "id": request["id"], "result": result}


__all__ = ["A2ACaller", "A2AHost", "A2AServer"]

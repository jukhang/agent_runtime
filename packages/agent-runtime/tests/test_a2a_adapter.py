from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from agent_runtime.a2a.client import A2AClient, RemoteMessage, RemoteTask
from agent_runtime.a2a.envelope import A2ADelegationEnvelope
from agent_runtime.a2a.server import A2ACaller, A2AServer
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.registry import RemoteAgentSpec

REF = {"key": {"namespace": "builtin", "key": "remote"}, "revision": "1"}
CARD = {"supportedInterfaces": [{"protocolBinding": "JSONRPC", "protocolVersion": "1.0"}]}


def spec():
    return RemoteAgentSpec.model_validate(
        {
            "ref": REF,
            "endpoint": REF,
            "protocol": REF,
            "cardDigest": content_digest(CARD),
            "credentialPolicy": REF,
            "canQueryTask": True,
            "canCancelTask": True,
            "idempotency": "query",
        }
    )


class Transport:
    def __init__(self):
        self.calls = []

    async def request(self, method, params):
        self.calls.append((method, params))
        state = "TASK_STATE_WORKING" if method != "GetTask" else "TASK_STATE_COMPLETED"
        return {
            "task": {
                "id": "task-1",
                "contextId": "context-1",
                "status": {"state": state},
                "artifacts": [{"artifactId": "result"}] if state == "TASK_STATE_COMPLETED" else [],
            }
        }


@pytest.mark.asyncio
async def test_a2a_client_pins_card_and_collects_only_terminal_task():
    transport = Transport()
    client = A2AClient(spec=spec(), card=CARD, transport=transport)
    task = await client.send(
        message_id="message-1", context_id="context-1", parts=({"text": "work"},)
    )
    with pytest.raises(ValueError, match="not successfully complete"):
        client.collect(task)
    assert (await client.cancel(task)).state == "TASK_STATE_WORKING"
    finished = await client.get(task.task_id)
    assert client.collect(finished) == ({"artifactId": "result"},)
    assert (
        await client.wait(
            task, deadline_at=datetime.now(UTC) + timedelta(seconds=2), poll_seconds=0.01
        )
    ).state == "TASK_STATE_COMPLETED"
    with pytest.raises(ValueError, match="Card differs"):
        A2AClient(spec=spec(), card={"supportedInterfaces": []}, transport=transport)


@pytest.mark.asyncio
async def test_a2a_immediate_message_is_not_mistaken_for_a_task():
    class Direct:
        async def request(self, method, params):
            return {
                "message": {
                    "messageId": "reply-1",
                    "contextId": "context-1",
                    "parts": [{"text": "done"}],
                }
            }

    reply = await A2AClient(spec=spec(), card=CARD, transport=Direct()).send(
        message_id="message-1", context_id="context-1", parts=({"text": "work"},)
    )
    assert isinstance(reply, RemoteMessage) and reply.parts == ({"text": "done"},)


class Host:
    async def send(self, caller, delegation, message):
        assert caller.scope_id == "scope" and message["messageId"] == "message-1"
        assert delegation.sequence == 1
        return {"id": "task-1", "contextId": "context-1", "status": {"state": "TASK_STATE_WORKING"}}

    async def get(self, caller, task_id):
        return {"id": task_id, "contextId": "context-1", "status": {"state": "TASK_STATE_WORKING"}}

    async def cancel(self, caller, task_id):
        return {"id": task_id, "contextId": "context-1", "status": {"state": "TASK_STATE_WORKING"}}


@pytest.mark.asyncio
async def test_a2a_server_requires_authenticated_caller_and_message_identity():
    server = A2AServer(host=Host(), card=CARD)
    caller = A2ACaller("scope", "actor", "1")
    delegation = A2ADelegationEnvelope(
        scope_id="scope",
        parent_run_id=uuid4(),
        child_run_id=uuid4(),
        message_id="message-1",
        sequence=1,
        budget_model_calls=2,
        budget_tool_calls=3,
        budget_tokens=1000,
        deadline_at=datetime.now(UTC) + timedelta(minutes=1),
        resources=(),
    )
    response = await server.dispatch(
        caller,
        {
            "jsonrpc": "2.0",
            "id": "rpc-1",
            "method": "SendMessage",
            "params": {
                "message": {
                    "messageId": "message-1",
                    "role": "ROLE_USER",
                    "parts": [{}],
                    "metadata": {
                        "centn.delegation": delegation.model_dump(mode="json", by_alias=True)
                    },
                }
            },
        },
    )
    assert response["result"]["task"]["id"] == "task-1"
    with pytest.raises(ValueError, match="not supported"):
        await server.dispatch(
            caller, {"jsonrpc": "2.0", "id": "2", "method": "Other", "params": {}}
        )


@pytest.mark.asyncio
async def test_a2a_waiting_input_preserves_delegation_at_server_boundary():
    seen = []

    class InputHost:
        async def send(self, caller, delegation, message):
            seen.append((delegation.sequence, message["taskId"]))
            return {
                "id": "task-1", "contextId": "context-1",
                "status": {"state": "TASK_STATE_WORKING"},
            }

        async def get(self, caller, task_id):
            raise AssertionError("not needed")

        async def cancel(self, caller, task_id):
            raise AssertionError("not needed")

    server = A2AServer(host=InputHost(), card=CARD)
    caller = A2ACaller("scope", "actor", "1")

    class ServerTransport:
        async def request(self, method, params):
            response = await server.dispatch(
                caller, {"jsonrpc": "2.0", "id": "rpc-1", "method": method, "params": params}
            )
            return response["result"]

    client = A2AClient(spec=spec(), card=CARD, transport=ServerTransport())
    delegation = A2ADelegationEnvelope(
        scope_id="scope", parent_run_id=uuid4(), child_run_id=uuid4(),
        message_id="input-2", sequence=2, budget_model_calls=2,
        budget_tool_calls=3, budget_tokens=1000,
        deadline_at=datetime.now(UTC) + timedelta(minutes=1), resources=(),
    )
    waiting = RemoteTask("task-1", "context-1", "TASK_STATE_INPUT_REQUIRED", ())
    result = await client.send_input(
        waiting, message_id="input-2", parts=({"text": "continue"},),
        delegation=delegation,
    )
    assert result.state == "TASK_STATE_WORKING"
    assert seen == [(2, "task-1")]

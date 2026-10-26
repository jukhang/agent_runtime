import asyncio
from contextlib import aclosing
from uuid import uuid4

import pytest
from test_events import EVENTS
from test_transitions import RESULT

from agent_runtime.contracts.events import CursorFrame, event_adapter
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.requests import CommandReceipt, ControlRequest
from agent_runtime.contracts.results import result_adapter
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.handle import RunHandle


class Backend:
    def __init__(self):
        self.closed = False
        self.commands = []
        self.read_cancelled = False
        self.actor = None

    async def events(self, actor, request):
        self.actor = actor
        try:
            yield CursorFrame(kind="cursor.advanced", cursor="1", auth_revision="1")
            await asyncio.Event().wait()
        finally:
            self.closed = True

    async def result(self, actor, session_id, run_id):
        self.actor = actor
        try:
            await asyncio.Event().wait()
        finally:
            self.read_cancelled = True

    async def control(self, actor, session_id, request):
        self.actor = actor
        self.commands.append(request)
        return CommandReceipt(
            command_id=uuid4(),
            run_id=request.run_id,
            status="accepted",
            state_version="1",
            public_reason="Queued",
        )


def handle(backend):
    return RunHandle(
        backend=backend,
        actor=ActorContext(scope_id="s", actor_id="u", auth_revision="1", grants_ref="grant"),
        session_id=uuid4(),
        run_id=uuid4(),
    )


def test_subscription_close_does_not_cancel_run():
    async def scenario():
        backend = Backend()
        client = handle(backend)
        async with aclosing(client.events()) as stream:
            assert (await anext(stream)).cursor == "1"
        assert backend.closed
        assert not backend.commands
        assert backend.actor == client.actor

    asyncio.run(scenario())


def test_wait_timeout_only_cancels_read():
    async def scenario():
        backend = Backend()
        with pytest.raises(AgentRuntimeError) as exc:
            await handle(backend).result(0.01)
        assert exc.value.failure.code == "WAIT_TIMEOUT"
        assert backend.read_cancelled
        assert not backend.commands

    asyncio.run(scenario())


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), True])
def test_invalid_wait_timeout(timeout):
    with pytest.raises(ValueError):
        asyncio.run(handle(Backend()).result(timeout))


def test_control_is_explicit_and_scoped():
    async def scenario():
        backend = Backend()
        client = handle(backend)
        request = ControlRequest(
            run_id=client.run_id, kind="pause", idempotency_key="key", expected_version="0"
        )
        assert (await client.control(request)).status == "accepted"
        with pytest.raises(ValueError):
            await client.control(request.model_copy(update={"run_id": uuid4()}))
        assert backend.commands == [request]

    asyncio.run(scenario())


def test_backend_timeout_is_not_client_wait_timeout():
    class Broken(Backend):
        async def result(self, actor, session_id, run_id):
            raise TimeoutError("storage timeout")

    with pytest.raises(TimeoutError, match="storage timeout"):
        asyncio.run(handle(Broken()).result(1))


def test_caller_cancellation_is_not_run_cancellation():
    async def scenario():
        backend = Backend()
        task = asyncio.create_task(handle(backend).result())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert backend.read_cancelled
        assert not backend.commands

    asyncio.run(scenario())


def test_stream_event_from_another_session_is_rejected():
    class ForeignSession(Backend):
        async def events(self, actor, request):
            event = event_adapter.validate_python(EVENTS["run.status"]).model_copy(
                update={"session_id": uuid4()}
            )
            yield event

    async def scenario():
        with pytest.raises(AgentRuntimeError) as exc:
            async for _ in handle(ForeignSession()).events():
                pass
        assert exc.value.failure.code == "IDENTITY_MISMATCH"

    asyncio.run(scenario())


def test_result_for_another_run_is_rejected():
    class ForeignResult(Backend):
        async def result(self, actor, session_id, run_id):
            result = result_adapter.validate_python(RESULT)
            return result.model_copy(update={"run_id": uuid4()})

    with pytest.raises(AgentRuntimeError) as exc:
        asyncio.run(handle(ForeignResult()).result())
    assert exc.value.failure.code == "IDENTITY_MISMATCH"


def test_command_receipt_for_another_run_is_rejected():
    class ForeignReceipt(Backend):
        async def control(self, actor, session_id, request):
            receipt = await super().control(actor, session_id, request)
            return receipt.model_copy(update={"run_id": uuid4()})

    async def scenario():
        client = handle(ForeignReceipt())
        request = ControlRequest(
            run_id=client.run_id, kind="pause", idempotency_key="key", expected_version="0"
        )
        with pytest.raises(AgentRuntimeError) as exc:
            await client.control(request)
        assert exc.value.failure.code == "IDENTITY_MISMATCH"

    asyncio.run(scenario())

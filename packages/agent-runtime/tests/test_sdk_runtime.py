"""SDK admission cannot accidentally become an in-process execution owner."""

from uuid import uuid4

import pytest

from agent_runtime import Runtime
from agent_runtime.contracts.references import CapabilityKey, CapabilityRef
from agent_runtime.contracts.requests import AcceptedTurn, SubmitRequest, TextInput
from agent_runtime.contracts.transactions import ActorContext


@pytest.mark.asyncio
async def test_submit_returns_admission_and_handle_without_running_backend():
    actor = ActorContext(scope_id="test", actor_id="operator", auth_revision="1", grants_ref="g")
    request = SubmitRequest(
        session_id=uuid4(),
        agent=CapabilityRef(key=CapabilityKey(namespace="test", key="agent"), revision="1"),
        idempotency_key="request",
        inputs=(TextInput(kind="text", text="hello"),),
    )
    receipt = AcceptedTurn(
        session_id=request.session_id,
        turn_id=uuid4(),
        run_id=uuid4(),
        assistant_item_id=uuid4(),
        accepted_seq="1",
    )

    class Admission:
        async def accept(self, principal, submitted):
            assert principal == actor and submitted == request
            return receipt

    backend = object()
    submitted = await Runtime(admission=Admission(), backend=backend).submit(actor, request)
    assert submitted.accepted == receipt
    assert submitted.handle.run_id == receipt.run_id
    assert submitted.handle.backend is backend


@pytest.mark.asyncio
async def test_submit_rejects_mismatched_admission_identity():
    class Admission:
        async def accept(self, actor, request):
            return AcceptedTurn(
                session_id=uuid4(),
                turn_id=uuid4(),
                run_id=uuid4(),
                assistant_item_id=uuid4(),
                accepted_seq="1",
            )

    actor = ActorContext(scope_id="test", actor_id="operator", auth_revision="1", grants_ref="g")
    request = SubmitRequest(
        session_id=uuid4(),
        agent=CapabilityRef(key=CapabilityKey(namespace="test", key="agent"), revision="1"),
        idempotency_key="request",
        inputs=(TextInput(kind="text", text="hello"),),
    )
    with pytest.raises(ValueError, match="another session"):
        await Runtime(admission=Admission(), backend=object()).submit(actor, request)

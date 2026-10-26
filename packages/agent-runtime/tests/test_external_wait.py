"""External wake predicates are opt-in, transactional, and fail closed."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.tasks import WaitSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.wake import SqlWaitWaker, WakeRequest


@pytest.mark.asyncio
@pytest.mark.parametrize("condition", ["missing", "false", "true", "error", "expired"])
async def test_external_evidence_is_explicit_and_fail_closed(condition):
    now = datetime.now(UTC)
    predicate = CapabilityRef.model_validate(
        {"key": {"namespace": "host", "key": "batch-ready"}, "revision": "1"}
    )
    actor = ActorContext(scope_id="scope", actor_id="owner", auth_revision="1", grants_ref="g")
    run_id = uuid4()
    request = WakeRequest(actor, uuid4(), run_id, 1, "external", str(run_id))
    spec = WaitSpec(
        kind="external",
        subject_id=str(run_id),
        resume_predicate=predicate,
        deadline_at=now + timedelta(seconds=-1 if condition == "expired" else 30),
    )
    verify = AsyncMock(return_value=condition == "true")
    if condition == "error":
        verify.side_effect = OSError("Dependency unavailable")
    waker = SqlWaitWaker(
        AsyncMock(),
        "example",
        actor=actor,
        journal=AsyncMock(),
        payloads=AsyncMock(),
        external_predicates={} if condition == "missing" else {predicate: verify},
    )
    db = AsyncMock()
    if condition == "true":
        await waker._verify_evidence(db, request, spec, now)
        verify.assert_awaited_once_with(db, request, spec)
    elif condition == "error":
        with pytest.raises(OSError):
            await waker._verify_evidence(db, request, spec, now)
    else:
        with pytest.raises(AgentRuntimeError) as exc:
            await waker._verify_evidence(db, request, spec, now)
        assert exc.value.failure.code == (
            "FORBIDDEN" if condition == "missing" else "VERSION_CONFLICT"
        )
        if condition in {"missing", "expired"}:
            verify.assert_not_awaited()

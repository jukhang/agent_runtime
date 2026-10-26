"""Offline lock-order and recovery-envelope checks; not SQL concurrency evidence."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from agent_runtime.storage.child_recovery import lock_recovery_runs
from agent_runtime.storage.recovery import SqlRecovery
from agent_runtime.storage.uow import locks


@pytest.mark.asyncio
@pytest.mark.parametrize("child_first", [True, False])
async def test_recovery_locks_slot_then_parent_child_in_uuid_order(monkeypatch, child_first):
    lower, upper = UUID(int=1), UUID(int=2)
    child, parent = (lower, upper) if child_first else (upper, lower)
    identity = SimpleNamespace(session_id=UUID(int=3), parent_run_id=parent)
    db = AsyncMock()
    db.execute.return_value = Mock(one_or_none=Mock(return_value=identity))
    calls = []

    async def slot(*args):
        calls.append(("slot", args[-1]))
        return {"owner_run_id": parent}

    async def run(*args):
        calls.append(("run", args[-1]))
        return {"id": args[-1]}

    monkeypatch.setattr(locks, "lock_slot", slot)
    monkeypatch.setattr(locks, "lock_run", run)
    _, child_row, parent_row = await lock_recovery_runs(
        db, "test_schema", locks.build_lock_statements("test_schema"), "scope", child
    )
    assert calls == [("slot", identity.session_id), ("run", lower), ("run", upper)]
    assert child_row["id"] == child and parent_row["id"] == parent


@pytest.mark.asyncio
async def test_unresolved_effect_envelope_is_never_scheduler_claimable():
    recovery = SqlRecovery(AsyncMock(), "test_schema")
    db = AsyncMock()
    now = datetime.now(UTC)
    await recovery._defer_child_effects(
        db, {"id": UUID(int=1), "scope_id": "scope"}, {"id": UUID(int=2)}, now
    )
    work_sql, params = db.execute.await_args_list[0].args
    assert "available_at=:revisit,deadline_at=:revisit" in str(work_sql)
    assert params["revisit"] > now
    assert "lease_epoch=lease_epoch+1" in str(work_sql)
    assert "owner_epoch=owner_epoch+1" in str(db.execute.await_args_list[1].args[0])

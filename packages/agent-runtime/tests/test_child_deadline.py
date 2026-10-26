"""A delegated deadline is a failed budget boundary, not user cancellation."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from test_provider_invoker import Attempts, capabilities, make_request
from test_worker_topology import request as worker_request

from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.ownership import ChildCommitGuard, ChildLeaseToken
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.work_claims import ChildClaimedWork
from agent_runtime.provider import ProviderInvoker
from agent_runtime.storage import child_authority, child_cancel
from agent_runtime.storage.child_cancel import SqlChildCancelSignal
from agent_runtime.storage.worker_topology import SqlWorkerTopology


def claim():
    now = datetime.now(UTC)
    session, child, root, work, workspace = (uuid4() for _ in range(5))
    lease = ChildLeaseToken(
        scope_id="test",
        session_id=session,
        run_id=child,
        work_id=work,
        owner_id="worker",
        run_epoch="1",
        work_epoch="1",
        run_lease_until=now + timedelta(minutes=5),
        work_lease_until=now + timedelta(minutes=5),
    )
    actor = ActorContext(scope_id="test", actor_id="actor", auth_revision="1", grants_ref="test")
    guard = ChildCommitGuard(
        actor=actor,
        lease=lease,
        expected_state_version="0",
        root_run_id=root,
        expected_root_cancel_version="0",
        workspace_owner_run_id=child,
        workspace_generation="1",
        workspace_ref=ResourceRef(
            resource_type="workspace",
            resource_id=str(workspace),
            resource_version="1",
            scope=ResourceScope(scope_id="test"),
        ),
    )
    return guard, now


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["child", "root", "cancel", "owner", "allow_control"])
async def test_deadline_is_typed_after_control_and_identity_checks(monkeypatch, boundary):
    guard, now = claim()
    root = dict(
        id=guard.root_run_id,
        parent_run_id=None,
        root_run_id=guard.root_run_id,
        session_id=guard.lease.session_id,
        cancel_version=0,
        status="running",
        desired_control=None,
        deadline_at=now + timedelta(minutes=5),
    )
    child = dict(
        root,
        id=guard.lease.run_id,
        parent_run_id=root["id"],
        owner_id="worker",
        owner_epoch=1,
        lease_until=guard.lease.run_lease_until,
        state_version=0,
        pending_input_version=1,
        applied_input_version=1,
    )
    (root if boundary == "root" else child)["deadline_at"] = now
    if boundary == "cancel":
        root["desired_control"] = "cancel"
    if boundary == "owner":
        child["owner_id"] = "stale"

    async def slot(*args):
        return None

    async def run(*args):
        return root if args[-1] == root["id"] else child

    async def grant(*args, **kwargs):
        raise runtime_failure(
            "FORBIDDEN", category="authorization", public_message="Expired grant."
        )

    monkeypatch.setattr(child_authority.locks, "lock_slot", slot)
    monkeypatch.setattr(child_authority.locks, "lock_run", run)
    monkeypatch.setattr(child_authority, "revalidate_authority", grant)
    with pytest.raises(AgentRuntimeError) as error:
        await child_authority.require_child(
            None, "test", guard, now=now, allow_control=boundary == "allow_control"
        )
    expected = {"cancel": "CONTROL_PENDING", "owner": "FENCED", "allow_control": "FORBIDDEN"}
    assert error.value.failure.code == expected.get(boundary, "BUDGET_EXHAUSTED")
    if boundary in {"child", "root"}:
        assert error.value.failure.deadline_at == now
        assert not error.value.failure.retryable


def signal_with_poll(monkeypatch, error):
    guard, _ = claim()
    work = SimpleNamespace(
        root_run_id=guard.root_run_id,
        state_version="0",
        root_cancel_version="0",
        workspace_ref=guard.workspace_ref,
        workspace_generation="1",
    )
    signal = SqlChildCancelSignal(
        None, "test", guard.actor, work, SimpleNamespace(current=lambda: guard.lease)
    )

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def begin(self):
            return self

    async def poll(*args, **kwargs):
        raise error

    signal._sessions = Session
    monkeypatch.setattr(child_cancel, "require_child", poll)
    return signal


@pytest.mark.asyncio
@pytest.mark.parametrize("waiting", [False, True])
async def test_budget_signal_raises_for_current_and_future_waiters(monkeypatch, waiting):
    error = runtime_failure("BUDGET_EXHAUSTED", category="budget", public_message="Expired.")
    signal = signal_with_poll(monkeypatch, error)
    waiter = asyncio.create_task(signal.wait()) if waiting else None
    await signal.watch()
    with pytest.raises(AgentRuntimeError) as caught:
        signal.is_cancelled()
    assert caught.value is error
    with pytest.raises(AgentRuntimeError) as caught:
        if waiter is not None:
            await waiter
        else:
            await signal.wait()
    assert caught.value is error


@pytest.mark.asyncio
@pytest.mark.parametrize("budget", [False, True])
async def test_child_signal_interrupts_provider_without_turning_budget_into_cancel(
    monkeypatch, budget
):
    error = runtime_failure(
        "BUDGET_EXHAUSTED" if budget else "CONTROL_PENDING",
        category="budget" if budget else "concurrency",
        public_message="Stopped.",
    )
    signal = signal_with_poll(monkeypatch, error)
    entered, closed = asyncio.Event(), asyncio.Event()

    class Provider:
        async def stream(self, request, *, cancel=None):
            try:
                entered.set()
                await asyncio.Event().wait()
                yield
            finally:
                closed.set()

    request = make_request().model_copy(
        update={"deadline_at": datetime.now(UTC) + timedelta(minutes=1)}
    )
    attempts = Attempts()
    invoker = ProviderInvoker(Provider(), capabilities=capabilities, attempts=attempts)

    async def collect():
        return [event async for event in invoker.stream(request, cancel=signal)]

    consuming = asyncio.create_task(collect())
    await entered.wait()
    if budget:
        await signal.watch()
        with pytest.raises(AgentRuntimeError) as caught:
            await consuming
        assert caught.value is error
    else:
        with pytest.raises(AgentRuntimeError):
            await signal.watch()
        result = await consuming
        assert result[-1].reason == "cancelled"
    assert closed.is_set()
    assert len(attempts.terminals) == 1
    terminal = attempts.terminals[0]
    assert terminal.status == ("failed" if budget else "cancelled")
    assert terminal.usage.status == "unknown"
    assert terminal.failure == (error.failure if budget else None)


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["BUDGET_EXHAUSTED", "CONTROL_PENDING", "PROVIDER_TIMEOUT", None])
@pytest.mark.parametrize("expired", [False, True])
async def test_topology_only_routes_budget_or_expired_child_to_shared_recovery(
    monkeypatch, code, expired
):
    guard, _ = claim()
    work = ChildClaimedWork(
        disposition="child_claimed",
        lease=guard.lease,
        root_run_id=guard.root_run_id,
        root_cancel_version="0",
        state_version="0",
        checkpoint_id=None,
        budget_allocation_id=uuid4(),
        workspace_ref=guard.workspace_ref,
        workspace_generation="1",
    )
    refreshed = guard.lease.model_copy(update={"work_epoch": "2"})
    handle = SimpleNamespace(current=lambda: refreshed)
    recovery_calls, user_calls = [], []

    class Recovery:
        def __init__(self, *args):
            pass

        async def handle_worker_failure(self, claimed, error):
            recovery_calls.append((self, claimed, error))

    async def user_errors(*args):
        user_calls.append(args)

    queries = []

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def execute(self, sql, params):
            queries.append((str(sql), params))
            assert "deadline_at<=clock_timestamp()" in str(sql)
            assert "parent_run_id=:parent AND root_run_id=:parent" in str(sql)
            assert params == {
                "scope": work.lease.scope_id,
                "run": work.lease.run_id,
                "parent": work.root_run_id,
            }
            return SimpleNamespace(scalar_one=lambda: expired)

    monkeypatch.setattr("agent_runtime.storage.worker_topology.SqlRecovery", Recovery)
    monkeypatch.setattr(
        "agent_runtime.storage.worker_topology.async_sessionmaker", lambda *args, **kwargs: Session
    )
    topology = SqlWorkerTopology(None, "test", scope_id="test")
    request = worker_request(["child"]).model_copy(update={"scope_id": "test"})
    service = topology.child(request, handlers=object(), errors=user_errors)
    error = (
        runtime_failure(
            code,
            category="budget" if code == "BUDGET_EXHAUSTED" else "concurrency",
            public_message="Stopped.",
        )
        if code
        else RuntimeError("ordinary failure")
    )
    await service._runner._child_error_sink(work, handle, error)
    assert len(queries) == (0 if code == "BUDGET_EXHAUSTED" else 1)
    if code == "BUDGET_EXHAUSTED" or expired:
        assert not user_calls and len(recovery_calls) == 1
        recovery, claimed, observed = recovery_calls[0]
        assert recovery is service._recovery and observed is error
        assert claimed.lease == refreshed and work.lease == guard.lease
    else:
        assert not recovery_calls and user_calls == [(work, handle, error)]

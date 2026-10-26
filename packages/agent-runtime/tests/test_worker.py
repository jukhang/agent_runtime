import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.scheduler import ClaimedWork, IdleWork, LostLease, RenewedLease
from agent_runtime.contracts.work_claims import ChildClaimedWork, ChildRenewed, WorkClaimRequest
from agent_runtime.worker import ChildLeaseHandle, LeaseHandle, WorkerConfig, WorkerRunner


@pytest.mark.parametrize("handler_failed", [False, True])
def test_completed_handler_result_wins_simultaneous_renewal_loss(monkeypatch, handler_failed):
    async def scenario():
        claim = _claim()

        class Scheduler(FakeScheduler):
            async def renew(self, request):
                return LostLease(
                    disposition="lost",
                    failure=FailureEnvelope(
                        code="FENCED",
                        category="concurrency",
                        retryable=False,
                        public_message="Lease released.",
                    ),
                )

        async def handler(work):
            if handler_failed:
                raise ValueError("handler failure")

        async def both_finished(tasks, *, return_when):
            await asyncio.gather(*tasks, return_exceptions=True)
            return set(tasks), set()

        errors = []

        async def sink(work, error):
            errors.append(error)

        worker = WorkerRunner(
            Scheduler(claim),
            _request(),
            handler,
            error_sink=sink,
            config=WorkerConfig(renewal_fraction=0.05),
        )
        monkeypatch.setattr(asyncio, "wait", both_finished)
        await worker._execute(claim)
        assert len(errors) == int(handler_failed)
        if handler_failed:
            assert isinstance(errors[0], ValueError)

    asyncio.run(scenario())


def _request() -> WorkClaimRequest:
    return WorkClaimRequest.model_validate(
        {
            "scopeId": "scope",
            "worker": {
                "workerId": "worker",
                "runtimeRevision": {
                    "key": {"namespace": "builtin", "key": "runtime"},
                    "revision": "1",
                },
                "capabilities": [],
            },
            "leaseSeconds": 1,
            "lanes": ["main"],
        }
    )


def _claim() -> ClaimedWork:
    now = datetime.now(UTC)
    identity = uuid4()
    return ClaimedWork.model_validate(
        {
            "disposition": "claimed",
            "lease": {
                "scopeId": "scope",
                "sessionId": identity,
                "runId": identity,
                "workId": identity,
                "ownerId": "worker",
                "runEpoch": "1",
                "workEpoch": "1",
                "slotVersion": "1",
                "runLeaseUntil": now + timedelta(seconds=2),
                "workLeaseUntil": now + timedelta(seconds=2),
            },
            "stateVersion": "1",
            "checkpointId": None,
            "budgetAllocationId": identity,
        }
    )


class FakeScheduler:
    def __init__(self, claim: ClaimedWork) -> None:
        self.claim = claim
        self.claim_count = 0
        self.renew_count = 0

    async def claim_work(self, request: Any) -> Any:
        self.claim_count += 1
        if self.claim_count == 1:
            return self.claim
        return IdleWork(disposition="idle", reason="no_work", retry_after_ms=10)

    async def renew(self, request: Any) -> Any:
        self.renew_count += 1
        deadline = datetime.now(UTC) + timedelta(seconds=2)
        return RenewedLease(
            disposition="renewed",
            lease=request.lease.model_copy(
                update={"run_lease_until": deadline, "work_lease_until": deadline}
            ),
        )

    async def renew_child(self, request: Any) -> Any:
        raise AssertionError("child renewal was not expected")

    async def renew_maintenance(self, request: Any) -> Any:
        raise AssertionError("maintenance renewal was not expected")

    async def claim(self, request: Any) -> Any:
        return await self.claim_work(request)

    async def yield_run(self, request: Any) -> Any:
        raise AssertionError("yield was not expected")


def _child_claim() -> ChildClaimedWork:
    main = _claim()
    return ChildClaimedWork.model_validate(
        {
            "disposition": "child_claimed",
            "lease": main.lease.model_dump(exclude={"slot_version"}),
            "root_run_id": uuid4(),
            "root_cancel_version": "0",
            "state_version": "0",
            "checkpoint_id": None,
            "budget_allocation_id": uuid4(),
            "workspace_ref": {
                "resource_type": "workspace",
                "resource_id": str(uuid4()),
                "resource_version": "1",
                "scope": {"scope_id": "scope"},
            },
            "workspace_generation": "1",
        }
    )


def test_child_handle_rejects_identity_changes() -> None:
    child = _child_claim()
    handle = ChildLeaseHandle(child.lease)
    with pytest.raises(ValueError):
        handle.update(child.lease.model_copy(update={"run_epoch": "2"}))
    assert handle.current() == child.lease


@pytest.mark.parametrize("lost", [False, True])
def test_child_handler_receives_renewed_lease_and_stops_on_loss(lost: bool) -> None:
    async def scenario() -> None:
        child = _child_claim()
        renewed = asyncio.Event()
        stopped = asyncio.Event()

        class Scheduler(FakeScheduler):
            async def renew_child(self, request: Any) -> Any:
                self.renew_count += 1
                if lost:
                    return LostLease(
                        disposition="lost",
                        failure=FailureEnvelope(
                            code="FENCED",
                            category="concurrency",
                            retryable=False,
                            public_message="Child lease lost.",
                        ),
                    )
                renewed.set()
                return ChildRenewed(
                    disposition="renewed",
                    lease=request.lease.model_copy(
                        update={
                            "run_lease_until": datetime.now(UTC) + timedelta(seconds=10),
                            "work_lease_until": datetime.now(UTC) + timedelta(seconds=10),
                        }
                    ),
                )

        scheduler = Scheduler(child)  # type: ignore[arg-type]

        async def fallback(_: Any) -> None:
            raise AssertionError("child work must use the child handler")

        async def child_handler(work: ChildClaimedWork, handle: ChildLeaseHandle) -> None:
            try:
                if lost:
                    await asyncio.Event().wait()
                else:
                    await renewed.wait()
                    await asyncio.sleep(0)
                    assert handle.current().run_lease_until > work.lease.run_lease_until
                    worker.request_drain()
            finally:
                stopped.set()

        errors: list[BaseException] = []

        async def sink(_: Any, error: BaseException) -> None:
            errors.append(error)
            worker.request_drain()

        worker = WorkerRunner(
            scheduler,
            _request(),
            fallback,
            child_handler=child_handler,
            error_sink=sink,
            config=WorkerConfig(renewal_fraction=0.05),
        )
        await asyncio.wait_for(worker.serve(), timeout=2)
        assert stopped.is_set()
        assert bool(errors) == lost

    asyncio.run(scenario())


def test_worker_drain_stops_claiming_and_waits_for_inflight() -> None:
    async def scenario() -> None:
        scheduler = FakeScheduler(_claim())
        started = asyncio.Event()
        release = asyncio.Event()

        async def handler(_: Any) -> None:
            started.set()
            await release.wait()

        worker = WorkerRunner(
            scheduler,
            _request(),
            handler,
            config=WorkerConfig(
                concurrency=1,
                idle_sleep_seconds=0.01,
                renewal_fraction=0.1,
                drain_grace_seconds=1,
            ),
        )
        serving = asyncio.create_task(worker.serve())
        await asyncio.wait_for(started.wait(), timeout=1)
        worker.request_drain()
        await asyncio.sleep(0)
        assert scheduler.claim_count == 1
        assert worker.inflight == 1
        release.set()
        await asyncio.wait_for(serving, timeout=1)
        assert worker.inflight == 0

    asyncio.run(scenario())


def test_worker_renews_claim_while_handler_runs() -> None:
    async def scenario() -> None:
        scheduler = FakeScheduler(_claim())
        renewed = asyncio.Event()

        original = scheduler.renew

        async def renew(request: Any) -> Any:
            result = await original(request)
            renewed.set()
            return result

        scheduler.renew = renew  # type: ignore[method-assign]

        async def handler(_: Any) -> None:
            await asyncio.wait_for(renewed.wait(), timeout=1)

        worker = WorkerRunner(
            scheduler,
            _request(),
            handler,
            config=WorkerConfig(
                concurrency=1,
                idle_sleep_seconds=0.01,
                renewal_fraction=0.05,
                drain_grace_seconds=1,
            ),
        )
        serving = asyncio.create_task(worker.serve())
        await asyncio.wait_for(renewed.wait(), timeout=1)
        worker.request_drain()
        await asyncio.wait_for(serving, timeout=1)
        assert scheduler.renew_count >= 1

    asyncio.run(scenario())


def test_main_handler_receives_renewed_lease() -> None:
    async def scenario() -> None:
        claim = _claim()
        scheduler = FakeScheduler(claim)
        observed = asyncio.Event()

        async def legacy_handler(_: Any) -> None:
            raise AssertionError("main handler should own this claim")

        async def main_handler(_: Any, handle: LeaseHandle) -> None:
            original = handle.current().work_lease_until
            for _ in range(100):
                if handle.current().work_lease_until != original:
                    observed.set()
                    return
                await asyncio.sleep(0.01)
            raise AssertionError("renewed lease was not delivered")

        worker = WorkerRunner(
            scheduler,
            _request(),
            legacy_handler,
            main_handler=main_handler,
            config=WorkerConfig(
                concurrency=1,
                idle_sleep_seconds=0.01,
                renewal_fraction=0.05,
                drain_grace_seconds=1,
            ),
        )
        serving = asyncio.create_task(worker.serve())
        await asyncio.wait_for(observed.wait(), timeout=1)
        worker.request_drain()
        await asyncio.wait_for(serving, timeout=1)
        assert scheduler.renew_count >= 1

    asyncio.run(scenario())


def test_lease_handle_rejects_identity_change() -> None:
    claim = _claim()
    handle = LeaseHandle(claim.lease)
    with pytest.raises(ValueError, match="identity"):
        handle.update(claim.lease.model_copy(update={"work_id": uuid4()}))


def test_worker_without_durable_error_sink_fails_closed() -> None:
    async def scenario() -> None:
        scheduler = FakeScheduler(_claim())

        async def handler(_: Any) -> None:
            raise RuntimeError("handler failed")

        worker = WorkerRunner(
            scheduler,
            _request(),
            handler,
            config=WorkerConfig(
                concurrency=1,
                idle_sleep_seconds=0.01,
                renewal_fraction=0.1,
                drain_grace_seconds=1,
            ),
        )
        try:
            await asyncio.wait_for(worker.serve(), timeout=1)
        except RuntimeError as exc:
            assert str(exc) == "handler failed"
        else:
            raise AssertionError("worker swallowed an unpersisted handler failure")
        await asyncio.wait_for(worker.wait_stopped(), timeout=1)
        assert worker.inflight == 0

    asyncio.run(scenario())


def test_lost_lease_cancels_handler_and_reaches_error_sink() -> None:
    async def scenario() -> None:
        scheduler = FakeScheduler(_claim())
        handler_cancelled = asyncio.Event()
        persisted = asyncio.Event()

        async def lost(_: Any) -> Any:
            return LostLease(
                disposition="lost",
                failure=FailureEnvelope(
                    code="FENCED",
                    category="concurrency",
                    retryable=False,
                    public_message="Lease lost",
                ),
            )

        scheduler.renew = lost  # type: ignore[method-assign]

        async def handler(_: Any) -> None:
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                handler_cancelled.set()
                raise

        async def error_sink(_: Any, exc: BaseException) -> None:
            assert exc.failure.code == "FENCED"
            persisted.set()

        worker = WorkerRunner(
            scheduler,
            _request(),
            handler,
            config=WorkerConfig(
                concurrency=1,
                idle_sleep_seconds=0.01,
                renewal_fraction=0.05,
                drain_grace_seconds=1,
            ),
            error_sink=error_sink,
        )
        serving = asyncio.create_task(worker.serve())
        await asyncio.wait_for(persisted.wait(), timeout=1)
        worker.request_drain()
        await asyncio.wait_for(serving, timeout=1)
        assert handler_cancelled.is_set()

    asyncio.run(scenario())

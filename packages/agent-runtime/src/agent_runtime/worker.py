"""Supervised worker lifecycle over the persistent Scheduler port."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Protocol

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.ownership import ChildLeaseToken
from agent_runtime.contracts.scheduler import IdleWork, LostLease, RenewRequest
from agent_runtime.contracts.transactions import LeaseToken, MaintenanceLease
from agent_runtime.contracts.work_claims import (
    ChildClaimedWork,
    ChildRenewRequest,
    ControlClaimedWork,
    MaintenanceClaimedWork,
    MaintenanceRenewRequest,
    WorkClaimRequest,
    WorkClaimResult,
)
from agent_runtime.ports import Scheduler

ErrorSink = Callable[[WorkClaimResult, BaseException], Awaitable[None]]


class WorkHandler(Protocol):
    async def __call__(self, work: WorkClaimResult) -> None:
        """Execute one claim; durable completion is committed through its owning UoW."""
        ...


class LeaseHandle:
    """Current main-run lease shared by renewal and a claimed handler."""

    def __init__(self, lease: LeaseToken) -> None:
        self._lease = lease

    def current(self) -> LeaseToken:
        return self._lease

    def update(self, lease: LeaseToken) -> None:
        old = self._lease
        if (
            lease.scope_id,
            lease.session_id,
            lease.run_id,
            lease.work_id,
            lease.owner_id,
            lease.run_epoch,
            lease.work_epoch,
            lease.slot_version,
        ) != (
            old.scope_id,
            old.session_id,
            old.run_id,
            old.work_id,
            old.owner_id,
            old.run_epoch,
            old.work_epoch,
            old.slot_version,
        ):
            raise ValueError("renewal changed lease identity")
        self._lease = lease


class MainWorkHandler(Protocol):
    async def __call__(self, work: WorkClaimResult, lease: LeaseHandle) -> None:
        """Run main work with a lease handle refreshed by the worker."""
        ...


class ChildLeaseHandle:
    """Renewed child authority; it never acquires a main session slot."""

    def __init__(self, lease: ChildLeaseToken) -> None:
        self._lease = lease

    def current(self) -> ChildLeaseToken:
        return self._lease

    def update(self, lease: ChildLeaseToken) -> None:
        old = self._lease
        if lease.model_dump(exclude={"run_lease_until", "work_lease_until"}) != old.model_dump(
            exclude={"run_lease_until", "work_lease_until"}
        ):
            raise ValueError("renewal changed child lease identity")
        self._lease = lease


class ChildWorkHandler(Protocol):
    async def __call__(self, work: ChildClaimedWork, lease: ChildLeaseHandle) -> None: ...


class ChildErrorSink(Protocol):
    async def __call__(
        self, work: ChildClaimedWork, lease: ChildLeaseHandle, error: BaseException
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    concurrency: int = 4
    idle_sleep_seconds: float = 0.25
    renewal_fraction: float = 1 / 3
    drain_grace_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.concurrency < 1:
            raise ValueError("worker concurrency must be positive")
        if self.idle_sleep_seconds <= 0:
            raise ValueError("idle sleep must be positive")
        if not 0 < self.renewal_fraction < 1:
            raise ValueError("renewal_fraction must be between zero and one")
        if self.drain_grace_seconds < 0:
            raise ValueError("drain grace must be non-negative")


class WorkerRunner:
    """A host-started worker; all spawned tasks are tracked and drained explicitly."""

    def __init__(
        self,
        scheduler: Scheduler,
        request: WorkClaimRequest,
        handler: WorkHandler,
        *,
        config: WorkerConfig | None = None,
        error_sink: ErrorSink | None = None,
        main_handler: MainWorkHandler | None = None,
        child_handler: ChildWorkHandler | None = None,
        child_error_sink: ChildErrorSink | None = None,
    ) -> None:
        self._scheduler = scheduler
        self._request = request
        self._handler = handler
        self._config = config or WorkerConfig()
        self._error_sink = error_sink
        self._main_handler = main_handler
        self._child_handler = child_handler
        self._child_error_sink = child_error_sink
        self._draining = asyncio.Event()
        self._stopped = asyncio.Event()
        self._tasks: set[asyncio.Task[None]] = set()

    @property
    def inflight(self) -> int:
        return len(self._tasks)

    def request_drain(self) -> None:
        """Stop claiming new work while allowing tracked handlers to finish."""
        self._draining.set()

    async def serve(self) -> None:
        """Claim until drain is requested, then await the bounded shutdown path."""
        try:
            while not self._draining.is_set():
                self._discard_finished()
                if len(self._tasks) >= self._config.concurrency:
                    await self._wait_for_capacity()
                    continue
                result = await self._scheduler.claim_work(self._request)
                if isinstance(result, IdleWork):
                    await self._interruptible_sleep(
                        max(self._config.idle_sleep_seconds, result.retry_after_ms / 1000)
                    )
                    continue
                task = asyncio.create_task(self._execute(result))
                self._tasks.add(task)
        finally:
            try:
                await self._drain_inflight()
            finally:
                self._stopped.set()

    async def wait_stopped(self) -> None:
        await self._stopped.wait()

    async def _execute(self, work: WorkClaimResult) -> None:
        lease_handle = (
            LeaseHandle(work.lease)
            if not isinstance(work, IdleWork) and isinstance(work.lease, LeaseToken)
            else None
        )
        child_handle = ChildLeaseHandle(work.lease) if isinstance(work, ChildClaimedWork) else None
        renewal = asyncio.create_task(self._renew_loop(work, lease_handle, child_handle))
        if isinstance(work, ChildClaimedWork) and self._child_handler is not None:
            assert child_handle is not None
            handler = asyncio.create_task(self._child_handler(work, child_handle))
        elif lease_handle is not None and self._main_handler is not None:
            handler = asyncio.create_task(self._main_handler(work, lease_handle))
        else:
            handler = asyncio.create_task(self._handler(work))
        try:
            done, _ = await asyncio.wait({handler, renewal}, return_when=asyncio.FIRST_COMPLETED)
            if handler in done:
                # A committed transition may release its lease just as renewal fails.
                # A finished handler cannot produce further effects; preserve its result.
                await handler
            elif renewal in done:
                # Renewal only returns by losing authority (or by failing). Continuing the
                # handler could emit an external side effect after fencing.
                renewal.result()
                raise RuntimeError("lease renewal stopped without a failure")
        except asyncio.CancelledError:
            handler.cancel()
            await asyncio.gather(handler, return_exceptions=True)
            raise
        except BaseException as exc:
            if not handler.done():
                handler.cancel()
                await asyncio.gather(handler, return_exceptions=True)
            if isinstance(work, ChildClaimedWork) and self._child_error_sink is not None:
                assert child_handle is not None
                await self._child_error_sink(work, child_handle, exc)
            elif self._error_sink is None:
                raise
            else:
                await self._error_sink(work, exc)
        finally:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)

    async def _renew_loop(
        self,
        work: WorkClaimResult,
        lease_handle: LeaseHandle | None = None,
        child_handle: ChildLeaseHandle | None = None,
    ) -> None:
        if isinstance(work, IdleWork):
            return
        delay = max(0.05, self._request.lease_seconds * self._config.renewal_fraction)
        if isinstance(work, MaintenanceClaimedWork):
            await self._renew_maintenance(work.lease, delay)
        elif isinstance(work, (ChildClaimedWork, ControlClaimedWork)) and isinstance(
            work.lease, ChildLeaseToken
        ):
            await self._renew_child(work.lease, delay, child_handle)
        else:
            assert isinstance(work.lease, LeaseToken)
            await self._renew_main(work.lease, delay, lease_handle)

    async def _renew_main(
        self, current: LeaseToken, delay: float, lease_handle: LeaseHandle | None
    ) -> None:
        while True:
            await asyncio.sleep(delay)
            result = await self._scheduler.renew(
                RenewRequest(
                    lease=current,
                    lease_seconds=self._request.lease_seconds,
                )
            )
            if isinstance(result, LostLease):
                raise AgentRuntimeError(result.failure)
            current = result.lease
            if lease_handle is not None:
                lease_handle.update(current)

    async def _renew_child(
        self, current: ChildLeaseToken, delay: float, handle: ChildLeaseHandle | None = None
    ) -> None:
        while True:
            await asyncio.sleep(delay)
            result = await self._scheduler.renew_child(
                ChildRenewRequest(
                    lease=current,
                    lease_seconds=self._request.lease_seconds,
                )
            )
            if isinstance(result, LostLease):
                raise AgentRuntimeError(result.failure)
            current = result.lease
            if handle is not None:
                handle.update(current)

    async def _renew_maintenance(self, current: MaintenanceLease, delay: float) -> None:
        while True:
            await asyncio.sleep(delay)
            result = await self._scheduler.renew_maintenance(
                MaintenanceRenewRequest(
                    lease=current,
                    lease_seconds=self._request.lease_seconds,
                )
            )
            if isinstance(result, LostLease):
                raise AgentRuntimeError(result.failure)
            current = result.lease

    async def _interruptible_sleep(self, seconds: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(self._draining.wait(), timeout=seconds)

    def _discard_finished(self) -> None:
        pending: set[asyncio.Task[None]] = set()
        failure: BaseException | None = None
        for task in self._tasks:
            if task.done():
                # Observe and propagate an unhandled handler failure. A worker without a
                # durable error sink must stop instead of silently waiting for lease expiry.
                try:
                    task.result()
                except BaseException as exc:
                    if failure is None:
                        failure = exc
            else:
                pending.add(task)
        self._tasks = pending
        if failure is not None:
            raise failure

    async def _wait_for_capacity(self) -> None:
        if not self._tasks:
            return
        draining = asyncio.create_task(self._draining.wait())
        try:
            await asyncio.wait(
                {*self._tasks, draining},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not self._draining.is_set():
                self._discard_finished()
        finally:
            draining.cancel()
            await asyncio.gather(draining, return_exceptions=True)

    async def _drain_inflight(self) -> None:
        if not self._tasks:
            return
        done, pending = await asyncio.wait(
            self._tasks,
            timeout=self._config.drain_grace_seconds,
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._tasks.clear()
        for task in done:
            task.result()


__all__ = [
    "ChildErrorSink",
    "ChildLeaseHandle",
    "ChildWorkHandler",
    "LeaseHandle",
    "MainWorkHandler",
    "WorkHandler",
    "WorkerConfig",
    "WorkerRunner",
]

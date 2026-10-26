import asyncio

import pytest

from agent_runtime.storage.host_worker import SqlWorkerService


@pytest.mark.parametrize("failure", (None, "worker", "recovery", "approval"))
def test_host_supervises_recovery_and_drains_worker(failure):
    async def scenario():
        stopped = asyncio.Event()
        recovery_called = asyncio.Event()
        approval_called = asyncio.Event()

        class Runner:
            def request_drain(self):
                stopped.set()

            async def serve(self):
                if failure == "worker":
                    raise ValueError("worker stopped")
                await stopped.wait()

        class Recovery:
            async def reconcile_scope(self, scope):
                assert scope == "tenant-a"
                recovery_called.set()
                if failure == "recovery":
                    raise ValueError("recovery stopped")

        class Approvals:
            async def wake_ready(self, scope, *, limit=100):
                assert scope == "tenant-a" and limit == 100
                approval_called.set()
                if failure == "approval":
                    raise ValueError("approval stopped")
                return 0

        service = SqlWorkerService(
            Runner(), Recovery(), scope_id="tenant-a", approval_wakes=Approvals(),
            recovery_interval_seconds=0.01,
        )
        task = asyncio.create_task(service.serve())
        if failure is None:
            await asyncio.wait_for(recovery_called.wait(), 1)
            await asyncio.wait_for(approval_called.wait(), 1)
            service.request_drain()
            await asyncio.wait_for(task, 1)
        else:
            with pytest.raises(ValueError, match=failure):
                await asyncio.wait_for(task, 1)
        assert stopped.is_set()
        assert not [
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task() and not task.done()
        ]

    asyncio.run(scenario())


def test_host_drain_waits_for_active_recovery_transaction():
    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()
        stop = asyncio.Event()
        cancelled = False

        class Runner:
            def request_drain(self):
                stop.set()

            async def serve(self):
                await stop.wait()

        class Recovery:
            async def reconcile_scope(self, scope):
                nonlocal cancelled
                started.set()
                try:
                    await release.wait()
                except asyncio.CancelledError:
                    cancelled = True
                    raise

        service = SqlWorkerService(
            Runner(), Recovery(), scope_id="tenant-a", recovery_interval_seconds=0.01
        )
        task = asyncio.create_task(service.serve())
        await asyncio.wait_for(started.wait(), 1)
        service.request_drain()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        await asyncio.wait_for(task, 1)
        assert not cancelled

    asyncio.run(scenario())

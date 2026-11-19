import asyncio
import traceback

import pytest

from agent_runtime.storage.host_worker import SqlWorkerService


def test_recovery_cleanup_failure_never_replaces_initial_worker_failure():
    async def scenario():
        stopped = asyncio.Event()

        class Runner:
            def request_drain(self):
                stopped.set()

            async def serve(self):
                await asyncio.sleep(0)
                raise ValueError("original worker failure")

        class Recovery:
            async def reconcile_scope(self, scope):
                await stopped.wait()
                raise RuntimeError("later cleanup failure")

        service = SqlWorkerService(Runner(), Recovery(), scope_id="tenant-a")
        with pytest.raises(ValueError, match="original worker failure"):
            await service.serve()
        assert stopped.is_set()

    asyncio.run(scenario())


@pytest.mark.parametrize("failed_role", ["worker", "recovery"])
def test_role_failure_retains_origin_traceback_after_sibling_cleanup(failed_role):
    async def scenario():
        stopped = asyncio.Event()

        async def database_origin():
            raise TimeoutError("database deadline")

        class Runner:
            def request_drain(self):
                stopped.set()

            async def serve(self):
                if failed_role == "worker":
                    await database_origin()
                await stopped.wait()

        class Recovery:
            async def reconcile_scope(self, scope):
                if failed_role == "recovery":
                    await database_origin()

        service = SqlWorkerService(Runner(), Recovery(), scope_id="tenant-a")
        with pytest.raises(TimeoutError) as raised:
            await service.serve()
        assert "database_origin" in {
            frame.name for frame in traceback.extract_tb(raised.value.__traceback__)
        }
        assert stopped.is_set()

    asyncio.run(scenario())


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

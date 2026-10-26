"""Independent SQL Runtime worker entry point; never executes Celery tasks."""

import argparse
import asyncio
import importlib
import signal
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Protocol, cast


class WorkerProcess(Protocol):
    def request_drain(self) -> None: ...

    async def serve(self) -> None: ...


type WorkerBootstrap = Callable[[], AbstractAsyncContextManager[WorkerProcess]]


def load_bootstrap(path: str) -> WorkerBootstrap:
    """Trusted operator configuration only; not a client or WorkItem parameter."""
    module, separator, name = path.partition(":")
    if not separator or not module or not name.isidentifier():
        raise ValueError("worker factory must be module:function")
    factory = getattr(importlib.import_module(module), name)
    if not callable(factory):
        raise ValueError("worker factory must be callable")
    return cast("WorkerBootstrap", factory)


async def serve(bootstrap: WorkerBootstrap) -> None:
    loop = asyncio.get_running_loop()
    async with bootstrap() as worker:
        installed: list[signal.Signals] = []
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, worker.request_drain)
                installed.append(sig)
            await worker.serve()
        finally:
            for sig in installed:
                loop.remove_signal_handler(sig)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the independent SQL Agent worker")
    parser.add_argument(
        "--factory", required=True, help="Trusted host async context manager (module:function)"
    )
    args = parser.parse_args()
    asyncio.run(serve(load_bootstrap(args.factory)))


if __name__ == "__main__":
    main()

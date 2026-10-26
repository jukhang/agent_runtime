"""Separate migrate, API and Worker commands; no embedded background execution."""

import argparse
import asyncio
import signal
from uuid import UUID

from agent_runtime.storage.migrate import execute
from centn_example.config import Settings
from centn_example.host import Host
from centn_example.resource_migrate import upgrade as upgrade_resources


async def worker(settings, *, maintenance=False, child=False):
    host = Host(settings)
    try:
        await host.initialize()
        service = await host.maintenance() if maintenance else await host.worker(child=child)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, service.request_drain)
        await service.serve()
    finally:
        await host.aclose()


async def materialize(settings, session):
    host = Host(settings)
    try:
        snapshot = await host.backend.materialize(host.actor(), session)
        print({"sessionId": str(snapshot.session_id), "snapshotSeq": snapshot.snapshot_seq})
    finally:
        await host.aclose()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command", choices=("migrate", "api", "worker", "child", "maintenance", "snapshot")
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8790, type=int)
    parser.add_argument("--session", type=UUID)
    args = parser.parse_args()
    settings = Settings.from_env()
    if args.command == "migrate":
        result = asyncio.run(execute("upgrade", settings.dsn, settings.schema, "base", 60))
        if result.status not in {"applied", "up-to-date"}:
            raise SystemExit("Runtime migration did not complete")
        asyncio.run(upgrade_resources(settings.dsn, settings.schema))
    elif args.command in {"worker", "child", "maintenance"}:
        asyncio.run(
            worker(
                settings, maintenance=args.command == "maintenance", child=args.command == "child"
            )
        )
    elif args.command == "snapshot":
        if args.session is None:
            parser.error("snapshot requires --session")
        asyncio.run(materialize(settings, args.session))
    else:
        import uvicorn

        from centn_example.api import create_app

        uvicorn.run(create_app(settings), host=args.host, port=args.port)


if __name__ == "__main__":
    main()

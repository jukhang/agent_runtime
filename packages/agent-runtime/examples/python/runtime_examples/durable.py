"""Pure Python SQL host client and independent Workers; no API or browser required."""

import argparse
import asyncio
import json
import signal
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import aclosing
from typing import cast
from uuid import UUID

from agent_runtime import Runtime
from agent_runtime.contracts.events import CursorFrame, ResyncFrame
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.requests import ControlRequest, SubmitRequest, TextInput
from agent_runtime.handle import RunHandle, StreamFrame


async def stream(handle: RunHandle, after: str) -> None:
    # RunHandle.events is an async generator; its public return annotation is AsyncIterator.
    subscription = cast(AsyncGenerator[StreamFrame, None], handle.events(after))
    async with aclosing(subscription) as events:
        async for event in events:
            if isinstance(event, ResyncFrame):
                raise RuntimeError("Snapshot required; restore the authorized host projection")
            if isinstance(event, CursorFrame) or (
                event.run_id == handle.run_id or event.parent_run_id == handle.run_id
            ):
                print(event.model_dump_json(by_alias=True), flush=True)


async def watch(handle: RunHandle, *, after: str, timeout: float) -> None:
    listener = asyncio.create_task(stream(handle, after))
    result_task = asyncio.create_task(handle.result(wait_timeout=timeout))
    try:
        # Surface stream validation/ACL failures instead of hiding them behind result polling.
        done, _ = await asyncio.wait((listener, result_task), return_when=asyncio.FIRST_COMPLETED)
        if listener in done:
            await listener
        result = await result_task
        print(json.dumps({"result": result.model_dump(mode="json", by_alias=True)}), flush=True)
    finally:
        for task in (listener, result_task):
            if not task.done():
                task.cancel()
        # Drain both tasks before the caller closes host DB/HTTP clients. Do not let
        # a failed listener interrupt cleanup or replace the original exception.
        await asyncio.gather(listener, result_task, return_exceptions=True)


async def migrate(
    dsn: str, schema: str, upgrade_host: Callable[[str, str], Awaitable[None]]
) -> None:
    from agent_runtime.storage.migrate import execute

    result = await execute("upgrade", dsn, schema, "base", 60)
    if result.status not in {"applied", "up-to-date"}:
        raise RuntimeError("Migration not applied; inspect its lease and retry later")
    await upgrade_host(dsn, schema)


async def run(args: argparse.Namespace) -> None:
    # Optional host extra; importing this module alone never imports FastAPI or opens SQL.
    from centn_example.config import Settings
    from centn_example.host import Host
    from centn_example.resource_migrate import upgrade as upgrade_resources

    settings = Settings.from_env()
    if args.command == "migrate":
        await migrate(settings.dsn, settings.schema, upgrade_resources)
        return
    host = Host(settings)
    try:
        await host.initialize()
        principal = host.actor()
        runtime = host.runtime
        installed = host.installed
        if not isinstance(runtime, Runtime) or installed is None:
            raise RuntimeError("Host initialization did not install Runtime and Agent")
        if args.command in {"worker", "child", "maintenance"}:
            service = (
                await host.maintenance()
                if args.command == "maintenance"
                else await host.worker(child=args.command == "child")
            )
            for sig in (signal.SIGTERM, signal.SIGINT):
                asyncio.get_running_loop().add_signal_handler(sig, service.request_drain)
            await service.serve()
        elif args.command == "create-session":
            session = await host.sessions.create(principal, title="Python SDK demo")
            print(json.dumps({"sessionId": str(session.id)}), flush=True)
        elif args.command == "submit":
            submitted = await runtime.submit(
                principal,
                SubmitRequest(
                    session_id=args.session,
                    agent=installed.ref,
                    idempotency_key=args.key,
                    inputs=(TextInput(kind="text", text=args.text),),
                ),
            )
            print(submitted.accepted.model_dump_json(by_alias=True), flush=True)
            await watch(submitted.handle, after=args.after, timeout=args.timeout)
        else:
            handle = runtime.attach(principal, args.session, args.run)
            if args.command == "control":
                receipt = await handle.control(
                    ControlRequest(
                        run_id=args.run,
                        kind=args.kind,
                        expected_version=args.expected_version,
                        idempotency_key=args.key,
                    )
                )
                print(receipt.model_dump_json(by_alias=True), flush=True)
            else:
                await watch(handle, after=args.after, timeout=args.timeout)
    finally:
        await host.aclose()


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("migrate", "create-session", "worker", "child", "maintenance"):
        commands.add_parser(name)
    for name in ("submit", "attach", "control"):
        command = commands.add_parser(name)
        command.add_argument("--session", type=UUID, required=True)
        if name != "submit":
            command.add_argument("--run", type=UUID, required=True)
        if name != "attach":
            command.add_argument("--key", required=True, help="Keep this identity on network retry")
        if name == "submit":
            command.add_argument("--text", required=True)
        if name == "control":
            command.add_argument("--kind", choices=("pause", "resume", "cancel"), required=True)
            command.add_argument("--expected-version", required=True)
        else:
            command.add_argument("--after", default="0")
            command.add_argument("--timeout", type=float, default=600)
    return result


def main() -> None:
    try:
        asyncio.run(run(parser().parse_args()))
    except AgentRuntimeError as exc:
        # In particular WAIT_TIMEOUT does not cancel the persisted Run.
        print(exc.failure.model_dump_json(by_alias=True), flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()

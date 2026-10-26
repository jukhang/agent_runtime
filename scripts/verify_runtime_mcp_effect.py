"""Exercise the local keyed MCP effect server; requires a separately started fixture."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import timedelta
from uuid import uuid4

import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


async def invoke(url: str, token: str, name: str, arguments: dict) -> dict:
    async with (
        httpx.AsyncClient(
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
            trust_env=False,
        ) as client,
        streamable_http_client(url, http_client=client) as streams,
    ):
        reader, writer, _ = streams
        async with ClientSession(
            reader, writer, read_timeout_seconds=timedelta(seconds=15)
        ) as session:
            await session.initialize()
            response = await session.call_tool(name, arguments)
            if response.isError:
                return {"_error": True}
            if not isinstance(response.structuredContent, dict):
                raise RuntimeError("MCP server did not return structured content")
            return response.structuredContent


async def verify(url: str, token: str) -> dict:
    key = str(uuid4())
    counter = f"probe-{key}"
    first = await invoke(
        url,
        token,
        "increment_counter",
        {"operation_key": key, "counter": counter, "amount": 3},
    )
    reused = await invoke(
        url,
        token,
        "increment_counter",
        {"operation_key": key, "counter": counter, "amount": 3},
    )
    looked_up = await invoke(url, token, "lookup_effect", {"operation_key": key})
    mismatch = await invoke(
        url,
        token,
        "increment_counter",
        {"operation_key": key, "counter": counter, "amount": 9},
    )
    mismatch_rejected = mismatch == {"_error": True}
    unauthorized = False
    try:
        await invoke(url, "wrong-token", "lookup_effect", {"operation_key": key})
    except Exception:
        unauthorized = True
    assert first["value"] == reused["value"] == looked_up["value"] == 3
    assert first["reused"] is False and reused["reused"] is True
    assert looked_up["found"] is True and mismatch_rejected and unauthorized
    return {
        "status": "passed",
        "uniqueWrite": True,
        "lookup": True,
        "inputMismatchRejected": mismatch_rejected,
        "unauthorizedRejected": unauthorized,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18151/mcp")
    parser.add_argument("--token", default="local-runtime-test-token")
    args = parser.parse_args()
    print(json.dumps(asyncio.run(verify(args.url, args.token)), ensure_ascii=False))


if __name__ == "__main__":
    main()

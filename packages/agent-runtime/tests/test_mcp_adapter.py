import pytest

from agent_runtime.mcp.client import McpClient
from agent_runtime.mcp.write import McpKeyedEffectClient


class Transport:
    def __init__(self, *, catalog=None, result=None, discovery=None):
        self.catalog = catalog or {"resultType": "complete", "tools": []}
        self.result = result or {
            "resultType": "complete", "content": [{"type": "text", "text": "ok"}],
        }
        self.discovery = discovery or {
            "resultType": "complete", "supportedVersions": ["2026-07-28"],
            "capabilities": {"tools": {}},
        }
        self.calls = []

    async def request(self, method, params):
        self.calls.append((method, params))
        return {
            "server/discover": self.discovery,
            "tools/list": self.catalog,
        }.get(method, self.result)


@pytest.mark.asyncio
async def test_mcp_read_tool_discovers_and_calls_pinned_name():
    transport = Transport(catalog={
        "resultType": "complete",
        "tools": [{"name": "search", "inputSchema": {"type": "object"}}],
    })
    client = McpClient(transport, calls={"search": transport}, profile="legacy-discovery")
    assert (await client.test_connection())[0].name == "search"
    assert transport.calls[:2] == [("server/discover", {}), ("tools/list", {})]
    effect = await client.call_read("search", {"query": "x"})
    assert effect.summary == "ok"
    assert transport.calls[-1] == ("tools/call", {"name": "search", "arguments": {"query": "x"}})


@pytest.mark.asyncio
async def test_mcp_duplicate_names_and_tool_error_fail_closed():
    transport = Transport(
        catalog={
            "resultType": "complete",
            "tools": [
                {"name": "same", "inputSchema": {"type": "object"}},
                {"name": "same", "inputSchema": {"type": "object"}},
            ]
        },
        result={
            "resultType": "complete", "isError": True,
            "content": [{"type": "text", "text": "secret"}],
        },
    )
    client = McpClient(transport, calls={"same": transport}, profile="legacy-discovery")
    with pytest.raises(ValueError, match="conflicts"):
        await client.list_tools()
    with pytest.raises(ValueError, match="failed"):
        await client.call_read("same", {})


@pytest.mark.asyncio
async def test_mcp_keyed_effect_has_separate_pinned_lookup_and_identity():
    write = Transport(result={
        "resultType": "complete", "structuredContent": {"operation_key": "op-1", "value": 2},
    })
    lookup = Transport(
        result={
            "resultType": "complete",
            "structuredContent": {"operation_key": "op-1", "found": True, "value": 2},
        }
    )
    client = McpKeyedEffectClient(
        effect_name="increment_counter", lookup_name="lookup_effect",
        effect_transport=write, lookup_transport=lookup,
    )
    assert (await client.call_once(
        name="increment_counter", arguments={"counter": "x"}, operation_key="op-1"
    )).data["value"] == 2
    assert (await client.lookup(name="increment_counter", operation_key="op-1")).data["value"] == 2
    assert write.calls == [
        ("tools/call", {"name": "increment_counter", "arguments": {
            "counter": "x", "operation_key": "op-1",
        }})
    ]
    assert lookup.calls == [
        ("tools/call", {"name": "lookup_effect", "arguments": {"operation_key": "op-1"}})
    ]
    with pytest.raises(ValueError, match="reserved"):
        await client.call_once(
            name="increment_counter", arguments={"operation_key": "model-key"},
            operation_key="op-1",
        )
    lookup.result = {
        "resultType": "complete", "structuredContent": {"operation_key": "op-1", "found": False},
    }
    assert await client.lookup(name="increment_counter", operation_key="op-1") is None


@pytest.mark.asyncio
async def test_mcp_modern_protocol_and_incomplete_results_fail_closed():
    transport = Transport(
        catalog={"resultType": "complete", "tools": []},
        discovery={
            "resultType": "complete", "supportedVersions": ["2025-11-25"],
            "capabilities": {"tools": {}},
        },
    )
    client = McpClient(transport, calls={"search": transport}, profile="legacy-discovery")
    with pytest.raises(ValueError, match="pinned tools protocol"):
        await client.test_connection()
    assert transport.calls == [("server/discover", {})]

    transport.discovery = {
        "resultType": "complete", "supportedVersions": ["2026-07-28"],
        "capabilities": {"tools": {}},
    }
    transport.result = {"resultType": "input_required", "content": []}
    with pytest.raises(ValueError, match="invalid result"):
        await client.call_read("search", {})

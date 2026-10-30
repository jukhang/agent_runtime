import json

import httpx
import pytest

from agent_runtime.mcp import connect_mcp


def server(version, seen, *, sse=False, tool_error=False):
    def respond(request):
        if request.method != "POST":
            seen.append((request.method, None, dict(request.headers)))
            return httpx.Response(200 if request.method == "DELETE" else 405)
        message = json.loads(request.content)
        seen.append(("POST", message, dict(request.headers)))
        method = message["method"]
        if method == "notifications/initialized":
            return httpx.Response(202)
        if method == "initialize":
            result = {
                "protocolVersion": version,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "fixture", "version": "1"},
            }
        elif method == "tools/list":
            result = {
                "tools": [
                    {"name": "search", "description": "Search", "inputSchema": {"type": "object"}}
                ]
            }
        else:
            result = {"content": [{"type": "text", "text": "found"}], "isError": tool_error}
        body = {"jsonrpc": "2.0", "id": message["id"], "result": result}
        headers = {"Mcp-Session-Id": "fixture-session"}
        if sse:
            headers["Content-Type"] = "text/event-stream"
            return httpx.Response(
                200, headers=headers, text="event: message\ndata: " + json.dumps(body) + "\n\n"
            )
        return httpx.Response(200, headers=headers, json=body)

    return httpx.MockTransport(respond)


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["2025-03-26", "2025-06-18", "2025-11-25"])
@pytest.mark.parametrize("sse", [False, True])
async def test_standard_versions_and_json_or_sse_lifecycle(version, sse):
    seen = []
    async with httpx.AsyncClient(transport=server(version, seen, sse=sse)) as http:
        async with connect_mcp(
            "https://mcp.test/mcp", http_client=http, allowed_tools=frozenset({"search"})
        ) as client:
            assert client.protocol_version == version
            assert (await client.test_connection())[0].name == "search"
            assert (await client.call_read("search", {"q": "test"})).summary == "found"
            with pytest.raises(ValueError, match="pinned"):
                await client.call_read("not-authorized", {})
        with pytest.raises(ValueError, match="closed"):
            await client.list_tools()
        assert not http.is_closed
    methods = [body["method"] for _, body, _ in seen if body is not None]
    assert methods[0] == "initialize"
    assert "server/discover" not in methods
    assert methods.count("initialize") == 1
    assert methods.index("notifications/initialized") < methods.index("tools/list")
    assert any(method == "DELETE" for method, _, _ in seen)


@pytest.mark.asyncio
@pytest.mark.parametrize("version,pin", [("2099-01-01", None), ("2025-03-26", "2025-11-25")])
async def test_unknown_versions_and_pin_mismatch_do_not_fallback(version, pin):
    seen = []
    async with httpx.AsyncClient(transport=server(version, seen)) as http:
        with pytest.raises((ValueError, ExceptionGroup)):
            async with connect_mcp("https://mcp.test/mcp", http_client=http, expected_version=pin):
                pytest.fail("An incompatible session must not open")
    assert all(body is None or body["method"] != "tools/call" for _, body, _ in seen)


@pytest.mark.asyncio
async def test_discovery_does_not_grant_execution_and_errors_are_not_success():
    seen = []
    async with httpx.AsyncClient(transport=server("2025-03-26", seen, tool_error=True)) as http:
        async with connect_mcp("https://mcp.test/mcp", http_client=http) as client:
            await client.list_tools()
            with pytest.raises(ValueError, match="pinned"):
                await client.call_read("search", {})
        async with connect_mcp(
            "https://mcp.test/mcp", http_client=http, allowed_tools=frozenset({"search"})
        ) as client:
            with pytest.raises(ValueError, match="failed"):
                await client.call_read("search", {})


@pytest.mark.asyncio
async def test_redirects_and_embedded_credentials_are_rejected_before_network():
    async with httpx.AsyncClient(follow_redirects=True) as http:
        with pytest.raises(ValueError, match="redirects"):
            async with connect_mcp("https://mcp.test/mcp", http_client=http):
                pytest.fail("Redirects must not be enabled")
    async with httpx.AsyncClient() as http:
        with pytest.raises(ValueError, match="endpoint"):
            async with connect_mcp("https://token:secret@mcp.test/mcp", http_client=http):
                pytest.fail("Embedded credentials must not be accepted")

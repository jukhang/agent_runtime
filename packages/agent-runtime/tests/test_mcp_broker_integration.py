"""Modern stateless MCP over real TLS and the Credential Broker boundary."""

import json
import ssl
import subprocess
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import pytest

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.credentials import EgressDestination, GrantRef, ScopeRequest
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.tools import McpToolBinding, ToolSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.credentials import CredentialBroker
from agent_runtime.credentials.https import OutboundProtocolProfile, PinnedHttpsTransport
from agent_runtime.credentials.jsonrpc import BrokerJsonRpcTransport
from agent_runtime.credentials.ports import FilteredBody, GrantState
from agent_runtime.mcp.client import McpClient
from agent_runtime.mcp.install import McpCapabilityInstaller, McpToolSchemas


def ref(namespace, key):
    return CapabilityRef.model_validate(
        {"key": {"namespace": namespace, "key": key}, "revision": "1"}
    )


@pytest.fixture(scope="module")
def mcp_server(tmp_path_factory):
    root = tmp_path_factory.mktemp("modern-mcp")
    key, cert = root / "key.pem", root / "cert.pem"
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(key), "-out", str(cert), "-days", "1",
            "-subj", "/CN=mcp.test", "-addext", "subjectAltName=DNS:mcp.test",
        ],
        check=True,
        capture_output=True,
    )
    state = {"version": "2026-07-28", "schema": {"type": "object", "properties": {
        "query": {"type": "string"}}, "required": ["query"], "additionalProperties": False},
        "seen": [], "token": "fixture-secret", "effects": {}, "effect_count": 0,
        "drop_next_effect": False, "cert_file": str(cert), "cancel_count": 0,
        "delay_next_lookup": False}
    effect_lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            request = json.loads(body)
            method, params = request["method"], request["params"]
            valid = (
                self.path == "/mcp"
                and self.headers.get("Authorization") == "Bearer fixture-secret"
                and self.headers.get("MCP-Protocol-Version") == "2026-07-28"
                and self.headers.get("Mcp-Method") == method
                and params.get("_meta", {}).get(
                    "io.modelcontextprotocol/protocolVersion") == "2026-07-28"
            )
            state["seen"].append((method, valid, self.headers.get("Mcp-Name")))
            if not valid:
                self.send_error(403)
                return
            if method == "server/discover":
                result = {"resultType": "complete", "supportedVersions": [state["version"]],
                          "capabilities": {"tools": {}},
                          "_meta": {"io.modelcontextprotocol/serverInfo": {
                              "name": "fixture", "version": "1.0"}}}
            elif method == "tools/list":
                result = {"resultType": "complete", "tools": [{"name": "search",
                    "description": "Search the fixture", "inputSchema": state["schema"]},
                    {"name": "increment_counter", "inputSchema": {"type": "object"}},
                    {"name": "lookup_effect", "inputSchema": {"type": "object"}},
                    {"name": "cancel_effect", "inputSchema": {"type": "object"}}]}
            elif method == "tools/call" and params.get("name") == "search" and (
                self.headers.get("Mcp-Name") == "search"
            ):
                result = {"resultType": "complete", "content": [{"type": "text",
                    "text": "found " + str(params["arguments"]["query"])}], "isError": False}
            elif method == "tools/call" and params.get("name") == "increment_counter" and (
                self.headers.get("Mcp-Name") == "increment_counter"
            ):
                operation_key = params["arguments"]["operation_key"]
                with effect_lock:
                    effects = state["effects"]
                    if operation_key not in effects:
                        effects[operation_key] = {"operation_key": operation_key, "found": True,
                                                  "ok": True, "value": 3}
                        state["effect_count"] += 1
                    stored = effects[operation_key]
                    drop = state["drop_next_effect"]
                    state["drop_next_effect"] = False
                if drop:
                    self.close_connection = True
                    return
                result = {"resultType": "complete", "structuredContent": stored,
                          "content": [{"type": "text", "text": "updated"}]}
            elif method == "tools/call" and params.get("name") == "lookup_effect" and (
                self.headers.get("Mcp-Name") == "lookup_effect"
            ):
                operation_key = params["arguments"]["operation_key"]
                with effect_lock:
                    delay = state["delay_next_lookup"]
                    state["delay_next_lookup"] = False
                if delay:
                    import time
                    time.sleep(30)
                stored = state["effects"].get(operation_key)
                result = {"resultType": "complete", "structuredContent": stored or {
                    "operation_key": operation_key, "found": False},
                    "content": [{"type": "text", "text": "lookup"}]}
            elif method == "tools/call" and params.get("name") == "cancel_effect" and (
                self.headers.get("Mcp-Name") == "cancel_effect"
            ):
                operation_key = params["arguments"]["operation_key"]
                with effect_lock:
                    state["cancel_count"] += 1
                    stored = state["effects"].get(operation_key)
                    if stored is not None and stored.get("status") == "running":
                        stored["status"] = "stopped"
                result = {"resultType": "complete", "structuredContent": {
                    "operation_key": operation_key, "accepted": True},
                    "content": [{"type": "text", "text": "cancel accepted"}]}
            else:
                self.send_error(400)
                return
            payload = json.dumps({"jsonrpc": "2.0", "id": request["id"], "result": result}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(cert, key)
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port, ssl.create_default_context(cafile=str(cert)), state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)


@pytest.mark.asyncio
async def test_modern_mcp_uses_broker_and_pinned_tls(mcp_server):
    port, tls, server_state = mcp_server
    actor = ActorContext(scope_id="scope", actor_id="admin", auth_revision="1", grants_ref="a")
    discovery_ref, call_ref = ref("test", "discover"), ref("test", "search")
    targets = {
        discovery_ref: EgressDestination(destination=discovery_ref, hostname="mcp.test",
            port=port, action="read", method="POST", path="/mcp",
            private_addresses=("127.0.0.1",)),
        call_ref: EgressDestination(destination=call_ref, hostname="mcp.test", port=port,
            action="read", method="POST", path="/mcp", private_addresses=("127.0.0.1",)),
    }
    now = datetime.now(UTC)
    scopes = {}
    grants = {}
    for destination in targets:
        scope = ScopeRequest(actor=actor, session_id=uuid4(), run_id=uuid4(),
            execution_id=uuid4(), execution_generation="1", operation_id=uuid4(),
            destination=destination, actions=("read",), policy_decision_id=uuid4(),
            input_digest="a" * 64, expires_at=now + timedelta(minutes=2),
            max_requests=8, max_response_bytes=4096)
        scopes[destination] = scope
        grants[destination] = GrantRef(grant_id=uuid4(), scope_id=actor.scope_id,
            run_id=scope.run_id, execution_id=scope.execution_id, execution_generation="1",
            revision="1", issued_at=now, expires_at=scope.expires_at)

    class Store:
        def __init__(self):
            self.used = {destination: 0 for destination in targets}
            self.revoked = set()

        async def issue(self, caller, request):
            assert caller == actor and request == scopes[request.destination]
            return grants[request.destination]

        async def inspect(self, caller, request):
            destination = next(ref for ref, grant in grants.items() if grant == request.grant)
            return GrantState(scopes[destination], request.grant, "1",
                              now if destination in self.revoked else None,
                              self.used[destination])

        async def reserve(self, caller, request):
            destination = next(ref for ref, grant in grants.items() if grant == request.grant)
            if destination in self.revoked or self.used[destination] >= 8:
                raise ValueError("grant denied")
            self.used[destination] += 1

        async def check_reserved(self, caller, request):
            if request.grant in (grants[ref] for ref in self.revoked):
                raise ValueError("grant revoked")

        async def finish(self, caller, request, response):
            return response

        async def unknown(self, caller, request):
            pass

        async def revoke(self, caller, grant):
            self.revoked.add(next(ref for ref, value in grants.items() if value == grant))

    class Blobs:
        def __init__(self):
            self.items = {}

        async def read(self, caller, key, limit):
            value = self.items[key]
            assert len(value) <= limit
            return value

        async def put_private(self, caller, request_id, data):
            key = str(uuid4())
            self.items[key] = data
            return key

    class Registry:
        def resolve(self, destination):
            return targets[destination]

    class Vault:
        async def bearer(self, scope_id, destination):
            assert scope_id == actor.scope_id and destination in targets
            return server_state["token"]

    class Resolver:
        async def resolve(self, hostname, requested_port):
            assert hostname == "mcp.test" and requested_port == port
            return ("127.0.0.1",)

    class Filter:
        async def apply(self, target, reply):
            return FilteredBody(reply.body, target.destination, "allowed")

    blobs, store = Blobs(), Store()
    transport = PinnedHttpsTransport(tls=tls, protocol_profiles={
        discovery_ref: OutboundProtocolProfile(kind="mcp_discovery"),
        call_ref: OutboundProtocolProfile(kind="mcp_tool", tool_name="search"),
    })
    broker = CredentialBroker(store=store, vault=Vault(), blobs=blobs,
        registry=Registry(), resolver=Resolver(), transport=transport,
        response_filter=Filter())
    discovery = BrokerJsonRpcTransport(broker=broker, blobs=blobs, actor=actor,
        grant=grants[discovery_ref], action="read",
        mcp_client_info={"name": "centn-runtime", "version": "1.0"})
    call = BrokerJsonRpcTransport(broker=broker, blobs=blobs, actor=actor,
        grant=grants[call_ref], action="read",
        mcp_client_info={"name": "centn-runtime", "version": "1.0"})
    client = McpClient(discovery, calls={"search": call}, profile="legacy-discovery")
    try:
        catalog = await client.test_connection()
        assert catalog[0].name == "search"
        effect = await client.call_read("search", {"query": "fixture"})
        assert effect.summary == "found fixture"
        assert server_state["seen"] == [
            ("server/discover", True, None), ("tools/list", True, None),
            ("tools/call", True, "search")]
        assert all(server_state["token"].encode() not in item for item in blobs.items.values())
        server_state["version"] = "2025-11-25"
        with pytest.raises(ValueError, match="pinned tools protocol"):
            await client.verify_server()
        before = len(server_state["seen"])
        await broker.revoke(actor, grants[call_ref])
        with pytest.raises(AgentRuntimeError):
            await client.call_read("search", {"query": "denied"})
        assert len(server_state["seen"]) == before
    finally:
        await transport.aclose()


@pytest.mark.asyncio
async def test_mcp_install_requires_reviewed_schema_and_never_publishes():
    source = ref("server", "reports")
    input_schema = {"type": "object", "properties": {"query": {"type": "string"}},
                    "required": ["query"], "additionalProperties": False}
    output_schema = {"type": "object", "required": ["resultType", "content"]}
    binding = McpToolBinding(protocol_version="2026-07-28", tool_name="search",
                             destination=ref("test", "search"),
                             input_schema=input_schema, output_schema=output_schema)
    spec = ToolSpec(ref=ref("mcp.reports", "search"), description="Search reports",
        source=source, kind="read", side_effect="none",
        input_schema=ref("mcp.input", content_digest(input_schema)),
        output_schema=ref("mcp.output", content_digest(output_schema)),
        permissions=(ref("policy", "search"),), approval_policy=ref("policy", "never"),
        idempotency="read_only", timeout_seconds=10, concurrency_group="mcp-reports",
        max_concurrency=2, cancellation="stop", public_result_policy="full", mcp=binding)

    class Client:
        protocol_version = "2026-07-28"

        async def test_connection(self):
            from agent_runtime.mcp.client import McpToolInfo
            return (McpToolInfo("search", "remote", input_schema),)

    class Registry:
        manifest = None

        async def install(self, actor, manifest):
            self.manifest = manifest
            return uuid4()

    actor = ActorContext(scope_id="scope", actor_id="admin", auth_revision="1", grants_ref="a")
    registry = Registry()
    await McpCapabilityInstaller(Client(), registry, source=source).install(actor, spec)
    assert registry.manifest.status == "draft"
    schemas = McpToolSchemas()
    schemas.input(spec, {"query": "ok"})
    with pytest.raises(ValueError, match="pinned schema"):
        schemas.input(spec, {"query": 2})

    class Drifted(Client):
        async def test_connection(self):
            from agent_runtime.mcp.client import McpToolInfo
            return (McpToolInfo("search", "remote", {"type": "object"}),)

    with pytest.raises(ValueError, match="catalog differs"):
        await McpCapabilityInstaller(Drifted(), registry, source=source).install(actor, spec)
    remote_ref = {"type": "object", "properties": {"leak": {
        "$ref": "https://untrusted.example/schema.json"}}}
    unsafe = spec.model_copy(update={"mcp": binding.model_copy(
        update={"input_schema": remote_ref})})
    with pytest.raises(ValueError, match="references are not supported"):
        await McpCapabilityInstaller(Client(), registry, source=source).install(actor, unsafe)
    recursive_ref = {"type": "object", "properties": {"again": {"$ref": "#"}}}
    recursive = spec.model_copy(update={"mcp": binding.model_copy(
        update={"input_schema": recursive_ref})})
    with pytest.raises(ValueError, match="references are not supported"):
        await McpCapabilityInstaller(Client(), registry, source=source).install(actor, recursive)

"""Real local TLS, not a mock HTTP transport. No external service credentials."""

import ssl
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpcore
import pytest

from agent_runtime.contracts.credentials import EgressDestination
from agent_runtime.credentials.https import OutboundProtocolProfile, PinnedHttpsTransport


@pytest.fixture(scope="module")
def tls_server(tmp_path_factory):
    root = tmp_path_factory.mktemp("broker-tls")
    key, cert = root / "key.pem", root / "cert.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=egress.test",
            "-addext",
            "subjectAltName=DNS:egress.test",
        ],
        check=True,
        capture_output=True,
    )
    observations = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            observed = (self.path, self.headers.get("Authorization") == "Bearer fixture")
            observations.append(
                (*observed, dict(self.headers)) if self.path in {"/mcp", "/a2a"} else observed
            )
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if self.path == "/slow":
                time.sleep(0.2)
            body = b"report" if self.path != "/large" else b"x" * 200
            self.send_response(302 if self.path == "/redirect" else 200)
            if self.path == "/redirect":
                self.send_header("Location", "https://other.invalid/steal")
            if self.path == "/encoded":
                self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (OSError, ssl.SSLError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(cert, key)
    server.socket = tls.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client_tls = ssl.create_default_context(cafile=str(cert))
    try:
        yield server.server_port, client_tls, observations
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5)
        assert not thread.is_alive()


def destination(port, path="/report", hostname="egress.test"):
    return EgressDestination(
        destination={"key": {"namespace": "test", "key": "api"}, "revision": "1"},
        hostname=hostname,
        port=port,
        action="read",
        method="POST",
        path=path,
        private_addresses=("127.0.0.1",),
    )


@pytest.mark.asyncio
async def test_numeric_peer_uses_original_tls_identity(tls_server):
    port, tls, observed = tls_server
    transport = PinnedHttpsTransport(tls=tls)
    try:
        for _ in range(2):
            reply = await transport.send(
                destination(port),
                ("127.0.0.1",),
                bearer="fixture",
                payload=b"{}",
                max_bytes=128,
                timeout_seconds=2,
            )
            assert reply.status_code == 200 and reply.body == b"report"
        assert observed[-2:] == [("/report", True), ("/report", True)]
    finally:
        await transport.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["/redirect", "/large", "/encoded"])
async def test_unsafe_http_responses_rejected(tls_server, path):
    port, tls, observed = tls_server
    before = len(observed)
    transport = PinnedHttpsTransport(tls=tls)
    try:
        with pytest.raises(ValueError):
            await transport.send(
                destination(port, path),
                ("127.0.0.1",),
                bearer="fixture",
                payload=b"{}",
                max_bytes=128,
                timeout_seconds=2,
            )
        assert len(observed) == before + 1  # No redirected request or transparent retry.
    finally:
        await transport.aclose()


@pytest.mark.asyncio
async def test_slow_server_cannot_extend_total_deadline(tls_server):
    port, tls, _ = tls_server
    transport = PinnedHttpsTransport(tls=tls)
    try:
        with pytest.raises((TimeoutError, httpcore.TimeoutException)):
            await transport.send(
                destination(port, "/slow"),
                ("127.0.0.1",),
                bearer="fixture",
                payload=b"{}",
                max_bytes=128,
                timeout_seconds=0.04,
            )
    finally:
        await transport.aclose()


@pytest.mark.asyncio
async def test_tls_mismatch_never_reaches_http_handler(tls_server):
    port, tls, observed = tls_server
    before = len(observed)
    transport = PinnedHttpsTransport(tls=tls)
    try:
        with pytest.raises(httpcore.ConnectError):
            await transport.send(
                destination(port, hostname="wrong.test"),
                ("127.0.0.1",),
                bearer="fixture",
                payload=b"{}",
                max_bytes=128,
                timeout_seconds=2,
            )
        assert len(observed) == before
    finally:
        await transport.aclose()


def test_unverified_tls_configuration_is_refused():
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    tls.check_hostname = False
    tls.verify_mode = ssl.CERT_NONE
    with pytest.raises(ValueError):
        PinnedHttpsTransport(tls=tls)


@pytest.mark.asyncio
async def test_mcp_profile_adds_pinned_method_headers(tls_server):
    port, tls, observed = tls_server
    target = destination(port, "/mcp")
    transport = PinnedHttpsTransport(
        tls=tls,
        protocol_profiles={
            target.destination: OutboundProtocolProfile(kind="mcp_tool", tool_name="search")
        },
    )
    try:
        await transport.send(
            target,
            ("127.0.0.1",),
            bearer="fixture",
            payload=b'{"jsonrpc":"2.0","id":"1","method":"tools/call",'
            b'"params":{"name":"search","arguments":{},"_meta":{'
            b'"io.modelcontextprotocol/protocolVersion":"2026-07-28",'
            b'"io.modelcontextprotocol/clientInfo":{"name":"centn-runtime"},'
            b'"io.modelcontextprotocol/clientCapabilities":{}}}}',
            max_bytes=128,
            timeout_seconds=2,
        )
        headers = observed[-1][2]
        assert headers["MCP-Protocol-Version"] == "2026-07-28"
        assert headers["Mcp-Method"] == "tools/call"
        assert headers["Mcp-Name"] == "search"
        with pytest.raises(ValueError, match="not pinned"):
            await transport.send(
                target,
                ("127.0.0.1",),
                bearer="fixture",
                payload=b'{"method":"tools/call","params":{"name":"other","_meta":{'
                b'"io.modelcontextprotocol/protocolVersion":"2026-07-28",'
                b'"io.modelcontextprotocol/clientInfo":{},'
                b'"io.modelcontextprotocol/clientCapabilities":{}}}}',
                max_bytes=128,
                timeout_seconds=2,
            )
        with pytest.raises(ValueError, match="not allowed"):
            await transport.send(
                target,
                ("127.0.0.1",),
                bearer="fixture",
                payload=b'{"method":"initialize"}',
                max_bytes=128,
                timeout_seconds=2,
            )
    finally:
        await transport.aclose()


@pytest.mark.asyncio
async def test_mcp_discovery_profile_allows_only_modern_discovery_and_listing(tls_server):
    port, tls, observed = tls_server
    target = destination(port, "/mcp")
    transport = PinnedHttpsTransport(
        tls=tls,
        protocol_profiles={target.destination: OutboundProtocolProfile(kind="mcp_discovery")},
    )
    try:
        for method in ("server/discover", "tools/list"):
            await transport.send(
                target,
                ("127.0.0.1",),
                bearer="fixture",
                payload=(
                    '{"jsonrpc":"2.0","id":"1","method":"' + method + '",'
                    '"params":{"_meta":{'
                    '"io.modelcontextprotocol/protocolVersion":"2026-07-28",'
                    '"io.modelcontextprotocol/clientInfo":{"name":"centn-runtime"},'
                    '"io.modelcontextprotocol/clientCapabilities":{}}}}'
                ).encode(),
                max_bytes=128,
                timeout_seconds=2,
            )
            headers = observed[-1][2]
            assert headers["MCP-Protocol-Version"] == "2026-07-28"
            assert headers["Mcp-Method"] == method
            assert "Mcp-Name" not in headers
        with pytest.raises(ValueError, match="not allowed"):
            await transport.send(
                target,
                ("127.0.0.1",),
                bearer="fixture",
                payload=b'{"method":"tools/call"}',
                max_bytes=128,
                timeout_seconds=2,
            )
    finally:
        await transport.aclose()


@pytest.mark.asyncio
async def test_a2a_profile_restricts_remote_methods(tls_server):
    port, tls, observed = tls_server
    target = destination(port, "/a2a")
    transport = PinnedHttpsTransport(
        tls=tls,
        protocol_profiles={
            target.destination: OutboundProtocolProfile(
                kind="a2a", allowed_methods=("SendMessage", "GetTask")
            )
        },
    )
    try:
        await transport.send(
            target,
            ("127.0.0.1",),
            bearer="fixture",
            payload=b'{"jsonrpc":"2.0","id":"1","method":"GetTask","params":{"id":"t"}}',
            max_bytes=128,
            timeout_seconds=2,
        )
        assert observed[-1][2]["A2A-Version"] == "1.0"
        with pytest.raises(ValueError, match="not allowed"):
            await transport.send(
                target,
                ("127.0.0.1",),
                bearer="fixture",
                payload=b'{"method":"CancelTask"}',
                max_bytes=128,
                timeout_seconds=2,
            )
    finally:
        await transport.aclose()

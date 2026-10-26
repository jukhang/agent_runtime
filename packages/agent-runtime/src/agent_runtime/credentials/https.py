"""Fixed-destination HTTPS, pinned DNS, verified SNI, no proxy or transparent retry."""

import asyncio
import json
import socket
import ssl
from collections.abc import Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Literal

import httpcore

from agent_runtime.contracts.credentials import EgressDestination
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.credential_validation import approved_addresses
from agent_runtime.credentials.ports import ProxyReply


class SystemResolver:
    async def resolve(self, hostname: str, port: int) -> tuple[str, ...]:
        rows = await asyncio.get_running_loop().getaddrinfo(
            hostname, port, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP
        )
        return tuple(dict.fromkeys(str(row[4][0]) for row in rows))


@dataclass(frozen=True, slots=True)
class _Binding:
    hostname: str
    port: int
    address: str


@dataclass(frozen=True, slots=True)
class OutboundProtocolProfile:
    kind: Literal["mcp_discovery", "mcp_tool", "a2a"]
    tool_name: str | None = None
    allowed_methods: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.kind == "mcp_tool":
            if not self.tool_name or not self.tool_name.isascii() or len(self.tool_name) > 128:
                raise ValueError("MCP tool profile requires a pinned ASCII name")
            if any(not (char.isalnum() or char in "_.-/") for char in self.tool_name):
                raise ValueError("MCP tool profile name is invalid")
        elif self.tool_name is not None:
            raise ValueError("non-tool profiles cannot pin a tool name")
        if self.kind == "a2a":
            allowed = {"SendMessage", "GetTask", "CancelTask"}
            if not self.allowed_methods or not set(self.allowed_methods) <= allowed:
                raise ValueError("A2A profile requires an explicit method subset")
        elif self.allowed_methods:
            raise ValueError("MCP profile method follows its pinned kind")


_binding: ContextVar[_Binding] = ContextVar("credential_egress_binding")


class _PinnedBackend(httpcore.AsyncNetworkBackend):
    def __init__(self) -> None:
        self._backend = httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Any = None,
    ) -> httpcore.AsyncNetworkStream:
        current = _binding.get()
        if host != current.hostname or port != current.port:
            raise ValueError("connection outside pinned destination")
        return await self._backend.connect_tcp(
            current.address,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )

    async def connect_unix_socket(
        self, path: str, timeout: float | None = None, socket_options: Any = None
    ) -> httpcore.AsyncNetworkStream:
        raise ValueError("Unix socket egress is forbidden")

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class PinnedHttpsTransport:
    """Own once in lifespan, close with aclose. No secret is retained in instance state.

    No keepalive: every request gets its own validated DNS binding and TLS connection.
    httpcore supplies the HTTP parser; no homemade HTTP framing or implicit redirects.
    """

    def __init__(
        self,
        *,
        tls: ssl.SSLContext | None = None,
        max_connections: int = 32,
        protocol_profiles: Mapping[CapabilityRef, OutboundProtocolProfile] | None = None,
    ) -> None:
        context = tls or ssl.create_default_context()
        if context.verify_mode != ssl.CERT_REQUIRED or not context.check_hostname:
            raise ValueError("verified TLS hostname and certificate are required")
        if max_connections < 1:
            raise ValueError("max_connections must be positive")
        self._profiles = dict(protocol_profiles or {})
        self._pool = httpcore.AsyncConnectionPool(
            ssl_context=context,
            max_connections=max_connections,
            max_keepalive_connections=0,
            retries=0,
            network_backend=_PinnedBackend(),
        )

    async def aclose(self) -> None:
        await self._pool.aclose()

    async def send(
        self,
        target: EgressDestination,
        addresses: tuple[str, ...],
        *,
        bearer: str,
        payload: bytes,
        max_bytes: int,
        timeout_seconds: float,
    ) -> ProxyReply:
        approved = approved_addresses(target, addresses)
        if max_bytes < 1 or timeout_seconds <= 0:
            raise ValueError("invalid transport bounds")
        if not bearer or any(ord(c) <= 32 or ord(c) >= 127 for c in bearer):
            raise ValueError("invalid credential header")
        authority = target.hostname if target.port == 443 else f"{target.hostname}:{target.port}"
        headers = [
            (b"Host", authority.encode("ascii")),
            (b"Authorization", b"Bearer " + bearer.encode("ascii")),
            (b"Accept-Encoding", b"identity"),
            (b"Content-Type", b"application/json"),
            (b"Content-Length", str(len(payload)).encode("ascii")),
        ]
        profile = self._profiles.get(target.destination)
        if profile is not None and profile.kind in {"mcp_discovery", "mcp_tool"}:
            try:
                message = json.loads(payload)
                method = message["method"]
                params = message.get("params", {})
                name = params.get("name") if isinstance(params, dict) else None
            except (ValueError, TypeError, KeyError) as exc:
                raise ValueError("invalid MCP request payload") from exc
            allowed_methods = (
                {"server/discover", "tools/list"}
                if profile.kind == "mcp_discovery"
                else {"tools/call"}
            )
            if method not in allowed_methods:
                raise ValueError("MCP method is not allowed at this destination")
            meta = params.get("_meta") if isinstance(params, dict) else None
            if (
                not isinstance(meta, dict)
                or meta.get("io.modelcontextprotocol/protocolVersion") != "2026-07-28"
                or not isinstance(meta.get("io.modelcontextprotocol/clientInfo"), dict)
                or not isinstance(meta.get("io.modelcontextprotocol/clientCapabilities"), dict)
            ):
                raise ValueError("MCP request lacks pinned per-request metadata")
            headers.extend(
                [
                    (b"MCP-Protocol-Version", b"2026-07-28"),
                    (b"Mcp-Method", method.encode("ascii")),
                    (b"Accept", b"application/json, text/event-stream"),
                ]
            )
            if method == "tools/call":
                if (
                    not isinstance(name, str)
                    or name != profile.tool_name
                ):
                    raise ValueError("MCP tool is not pinned to this grant")
                headers.append((b"Mcp-Name", name.encode("ascii")))
        elif profile is not None and profile.kind == "a2a":
            try:
                message = json.loads(payload)
                method = message["method"]
            except (ValueError, TypeError, KeyError) as exc:
                raise ValueError("invalid A2A request payload") from exc
            if method not in profile.allowed_methods:
                raise ValueError("A2A method is not allowed at this destination")
            headers.append((b"A2A-Version", b"1.0"))
        token = _binding.set(_Binding(target.hostname, target.port, approved[0]))
        try:
            async with asyncio.timeout(timeout_seconds):
                async with self._pool.stream(
                    target.method,
                    f"https://{authority}{target.path}",
                    headers=headers,
                    content=payload,
                    extensions={
                        "timeout": dict.fromkeys(
                            ("connect", "read", "write", "pool"), timeout_seconds
                        )
                    },
                ) as response:
                    if 300 <= response.status <= 399:
                        raise ValueError("redirect is forbidden")
                    for key, value in response.headers:
                        if key.lower() == b"content-encoding" and value.lower() != b"identity":
                            raise ValueError("compressed response is forbidden")
                    result = bytearray()
                    async for part in response.aiter_stream():
                        if len(result) + len(part) > max_bytes:
                            raise ValueError("response byte limit exceeded")
                        result.extend(part)
                    return ProxyReply(response.status, bytes(result))
        finally:
            _binding.reset(token)

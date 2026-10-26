"""Bounded single-request TLS RPC. Only transport-authenticated services may proxy."""

import asyncio
import hashlib
import json
import ssl
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from agent_runtime.contracts.credentials import FilteredResponse, GrantRef, ProxyRequest
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.credentials.broker import CredentialBroker


class ServiceIdentityResolver(Protocol):
    async def resolve_service(self, peer_sha256: str, grant: GrantRef) -> ActorContext: ...


def server_tls(ca: Path, cert: Path, key: Path) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cafile=str(ca))
    context.load_cert_chain(cert, key)
    return context


def client_tls(ca: Path, cert: Path, key: Path) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=str(ca))
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(cert, key)
    return context


@dataclass(frozen=True, slots=True)
class IngressLimits:
    frame_bytes: int = 32768
    timeout_seconds: float = 35
    max_connections: int = 32

    def __post_init__(self) -> None:
        if not 1024 <= self.frame_bytes <= 262144:
            raise ValueError("invalid ingress frame limit")
        if not 0 < self.timeout_seconds <= 300 or not 1 <= self.max_connections <= 1024:
            raise ValueError("invalid ingress limits")


async def read_frame(reader: asyncio.StreamReader, maximum: int) -> bytes:
    size = struct.unpack("!I", await reader.readexactly(4))[0]
    if not 0 < size <= maximum:
        raise ValueError("frame exceeds bound")
    return await reader.readexactly(size)


async def write_frame(writer: asyncio.StreamWriter, payload: bytes, maximum: int) -> None:
    if not 0 < len(payload) <= maximum:
        raise ValueError("frame exceeds bound")
    writer.write(struct.pack("!I", len(payload)) + payload)
    await writer.drain()


async def close_stream(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        async with asyncio.timeout(1):
            await writer.wait_closed()
    except (OSError, TimeoutError):
        pass


class CredentialIngress:
    """Own server lifespan. No issue/bind/admin methods, auth headers or actor JSON."""

    def __init__(
        self,
        broker: CredentialBroker,
        identities: ServiceIdentityResolver,
        tls: ssl.SSLContext,
        *,
        limits: IngressLimits | None = None,
    ) -> None:
        if tls.verify_mode != ssl.CERT_REQUIRED or tls.minimum_version < ssl.TLSVersion.TLSv1_3:
            raise ValueError("ingress requires mutual TLS 1.3")
        self._broker, self._identities, self._tls = broker, identities, tls
        self._limits = limits or IngressLimits()
        self._server: asyncio.Server | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._closing = False

    async def start(self, host: str, port: int) -> int:
        if self._server is not None:
            raise ValueError("ingress already started")
        self._closing = False
        self._server = await asyncio.start_server(
            self._accept,
            host,
            port,
            ssl=self._tls,
            ssl_handshake_timeout=5,
            ssl_shutdown_timeout=1,
            limit=self._limits.frame_bytes + 4,
            backlog=128,
        )
        return int(self._server.sockets[0].getsockname()[1])

    def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self._closing or len(self._tasks) >= self._limits.max_connections:
            writer.close()
            return
        task = asyncio.create_task(self._handle(reader, writer))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        proxy_started = False
        identity_started = False
        try:
            async with asyncio.timeout(self._limits.timeout_seconds):
                tls = writer.get_extra_info("ssl_object")
                cert = tls.getpeercert(binary_form=True) if tls is not None else None
                if not cert:
                    raise ValueError("missing authenticated peer")
                request = ProxyRequest.model_validate_json(
                    await read_frame(reader, self._limits.frame_bytes)
                )
                identity_started = True
                actor = await self._identities.resolve_service(
                    hashlib.sha256(cert).hexdigest(), request.grant
                )
                proxy_started = True
                response = await self._broker.proxy(actor, request)
                await write_frame(
                    writer,
                    response.model_dump_json(by_alias=True).encode(),
                    self._limits.frame_bytes,
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Never serialize provider/SQL/validation exceptions or secret-bearing cause chains.
            try:
                error = "request_denied"
                if proxy_started:
                    error = "outcome_unknown"
                elif identity_started and not (
                    isinstance(exc, AgentRuntimeError)
                    and exc.failure.code in {"FORBIDDEN", "CONTROL_PENDING"}
                ):
                    error = "service_unavailable"
                async with asyncio.timeout(1):
                    await write_frame(
                        writer,
                        json.dumps({"error": error}).encode(),
                        self._limits.frame_bytes,
                    )
            except Exception:
                pass
        finally:
            await close_stream(writer)

    async def aclose(self) -> None:
        self._closing = True
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.difference_update(tasks)


class CredentialServiceClient:
    """Host Relay only; never install its TLS keys in the sandbox or model context."""

    def __init__(
        self,
        host: str,
        port: int,
        server_name: str,
        tls: ssl.SSLContext,
        *,
        limits: IngressLimits | None = None,
    ) -> None:
        if not tls.check_hostname or tls.verify_mode != ssl.CERT_REQUIRED:
            raise ValueError("client must verify Broker hostname and CA")
        self._host, self._port, self._name, self._tls = host, port, server_name, tls
        self._limits = limits or IngressLimits()

    async def proxy(self, request: ProxyRequest) -> FilteredResponse:
        request = ProxyRequest.model_validate(request.model_dump())
        try:
            return await self._proxy(request)
        except AgentRuntimeError:
            raise
        except Exception:
            raise runtime_failure(
                "SIDE_EFFECT_UNKNOWN",
                category="tool",
                public_message=(
                    "Broker response unavailable; outcome may be unknown. "
                    "Do not replay automatically."
                ),
            ) from None

    async def _proxy(self, request: ProxyRequest) -> FilteredResponse:
        async with asyncio.timeout(self._limits.timeout_seconds):
            reader, writer = await asyncio.open_connection(
                self._host,
                self._port,
                ssl=self._tls,
                server_hostname=self._name,
                ssl_handshake_timeout=5,
                ssl_shutdown_timeout=1,
                limit=self._limits.frame_bytes + 4,
            )
            try:
                await write_frame(
                    writer,
                    request.model_dump_json(by_alias=True).encode(),
                    self._limits.frame_bytes,
                )
                payload = await read_frame(reader, self._limits.frame_bytes)
                error = json.loads(payload).get("error")
                if error is not None:
                    code = {
                        "request_denied": "FORBIDDEN",
                        "service_unavailable": "DEPENDENCY_UNAVAILABLE",
                    }.get(error, "SIDE_EFFECT_UNKNOWN")
                    raise runtime_failure(
                        code,
                        category=(
                            "authorization"
                            if error == "request_denied"
                            else "delivery"
                            if error == "service_unavailable"
                            else "tool"
                        ),
                        public_message="Broker request not completed. Do not replay automatically.",
                    )
                response = FilteredResponse.model_validate_json(payload)
                if (
                    response.request_id != request.request_id
                    or response.grant_id != request.grant.grant_id
                ):
                    raise ValueError("Broker response identity mismatch")
                return response
            finally:
                await close_stream(writer)

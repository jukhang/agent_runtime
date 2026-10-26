"""Real mTLS ingress: no mocked transport authentication."""

import asyncio
import json
import ssl
import struct
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from credential_test_support import certificates

from agent_runtime.contracts.credentials import FilteredResponse, GrantRef, ProxyRequest
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.credentials.ingress import (
    CredentialIngress,
    CredentialServiceClient,
    IngressLimits,
    client_tls,
    close_stream,
    read_frame,
    server_tls,
    write_frame,
)


@pytest.fixture(scope="module")
def certs(tmp_path_factory):
    directory = tmp_path_factory.mktemp("credential-mtls")
    return directory, certificates(directory)


def proxy_request():
    now = datetime.now(UTC)
    return ProxyRequest(
        grant=GrantRef(
            grant_id=uuid4(),
            scope_id="s",
            run_id=uuid4(),
            execution_id=uuid4(),
            execution_generation="1",
            revision="1",
            issued_at=now,
            expires_at=now + timedelta(minutes=5),
        ),
        request_id=uuid4(),
        action="read",
        request_payload_ref="private:test",
        request_digest="a" * 64,
    )


def filtered(request):
    return FilteredResponse(
        request_id=request.request_id,
        grant_id=request.grant.grant_id,
        status_code=200,
        body_ref="private:filtered",
        body_digest="b" * 64,
        size_bytes=2,
        filter_policy={"key": {"namespace": "test", "key": "filter"}, "revision": "1"},
        audit_ref=f"credential-request:{request.request_id}",
        disposition="allowed",
    )


@pytest_asyncio.fixture
async def ingress(certs):
    root, hashes = certs
    request = proxy_request()
    calls = []

    class Identity:
        async def resolve_service(self, fingerprint, grant):
            if fingerprint != hashes["relay"] or grant != request.grant:
                raise runtime_failure(
                    "FORBIDDEN", category="authorization", public_message="SECRET: denied peer"
                )
            return ActorContext(
                scope_id="s", actor_id="owner", auth_revision="1", grants_ref="test:grant"
            )

    class Broker:
        async def proxy(self, caller, actual):
            assert caller.actor_id == "owner"
            calls.append(actual)
            return filtered(actual)

    server = CredentialIngress(
        Broker(),
        Identity(),
        server_tls(root / "ca.pem", root / "broker.pem", root / "broker.key"),
        limits=IngressLimits(timeout_seconds=0.3),
    )
    port = await server.start("127.0.0.1", 0)
    try:
        yield server, port, request, calls
    finally:
        await server.aclose()
        assert not server._tasks


def context(certs, name="relay"):
    root, _ = certs
    return client_tls(root / "ca.pem", root / f"{name}.pem", root / f"{name}.key")


@pytest.mark.asyncio
async def test_transport_identity_is_derived_from_certificate(ingress, certs):
    _, port, request, calls = ingress
    client = CredentialServiceClient("127.0.0.1", port, "broker.test", context(certs))
    assert await client.proxy(request) == filtered(request)
    assert calls == [request]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "other_certificate",
        "other_grant",
        "self_reported_actor",
        "oversized",
        "partial",
        "missing_certificate",
        "wrong_hostname",
    ],
)
async def test_ingress_fails_closed(ingress, certs, case):
    _, port, request, calls = ingress
    tls = context(certs, "other" if case == "other_certificate" else "relay")
    if case == "missing_certificate":
        tls = ssl.create_default_context(cafile=str(certs[0] / "ca.pem"))
    reader = writer = None
    try:
        reader, writer = await asyncio.open_connection(
            "127.0.0.1",
            port,
            ssl=tls,
            server_hostname="wrong.test" if case == "wrong_hostname" else "broker.test",
        )
        if case == "other_grant":
            request = request.model_copy(
                update={"grant": request.grant.model_copy(update={"grant_id": uuid4()})}
            )
        if case == "oversized":
            writer.write(struct.pack("!I", 999999))
            await writer.drain()
        elif case == "partial":
            writer.write(struct.pack("!I", 100))
            await writer.drain()
        else:
            value = request.model_dump(mode="json", by_alias=True)
            if case == "self_reported_actor":
                value["actor"] = {"actorId": "owner"}
            await write_frame(writer, json.dumps(value).encode(), 32768)
        async with asyncio.timeout(2):
            assert json.loads(await read_frame(reader, 32768)) == {"error": "request_denied"}
    except (OSError, asyncio.IncompleteReadError):
        assert case in {"missing_certificate", "wrong_hostname"}
    finally:
        if writer is not None:
            await close_stream(writer)
    assert calls == []


def test_plain_or_unverified_configuration_refused(certs):
    with pytest.raises(ValueError):
        CredentialIngress(None, None, ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER))
    tls = context(certs)
    tls.check_hostname = False
    with pytest.raises(ValueError):
        CredentialServiceClient("127.0.0.1", 443, "broker.test", tls)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["identity", "proxy", "timeout"])
async def test_failures_are_structured_sanitized_and_never_retried(ingress, certs, phase):
    server, port, request, calls = ingress

    class BrokenIdentity:
        async def resolve_service(self, fingerprint, grant):
            raise ConnectionError("SECRET identity database")

    class BrokenBroker:
        async def proxy(self, caller, actual):
            calls.append(actual)
            if phase == "timeout":
                await asyncio.sleep(2)
            raise runtime_failure(
                "DEPENDENCY_UNAVAILABLE", category="delivery", public_message="SECRET from plugin"
            )

    if phase == "identity":
        server._identities = BrokenIdentity()
    else:
        server._broker = BrokenBroker()
    client = CredentialServiceClient("127.0.0.1", port, "broker.test", context(certs))
    with pytest.raises(AgentRuntimeError) as error:
        await client.proxy(request)
    expected = "DEPENDENCY_UNAVAILABLE" if phase == "identity" else "SIDE_EFFECT_UNKNOWN"
    assert error.value.failure.code == expected
    assert error.value.failure.retryable is False
    assert "SECRET" not in str(error.value)
    assert len(calls) == int(phase != "identity")


@pytest.mark.asyncio
async def test_authenticated_concurrency_limit_does_not_dispatch_excess_requests(ingress, certs):
    server, port, request, calls = ingress
    server._limits = IngressLimits(timeout_seconds=2, max_connections=1)
    started, release = asyncio.Event(), asyncio.Event()

    class WaitingBroker:
        async def proxy(self, caller, actual):
            calls.append(actual)
            started.set()
            await release.wait()
            return filtered(actual)

    server._broker = WaitingBroker()
    client = CredentialServiceClient("127.0.0.1", port, "broker.test", context(certs))
    first = asyncio.create_task(client.proxy(request))
    try:
        async with asyncio.timeout(2):
            await started.wait()
        with pytest.raises(AgentRuntimeError) as error:
            await client.proxy(request)
        assert error.value.failure.code == "SIDE_EFFECT_UNKNOWN"
        assert calls == [request]
        release.set()
        assert await first == filtered(request)
    finally:
        release.set()
        if not first.done():
            first.cancel()
        await asyncio.gather(first, return_exceptions=True)

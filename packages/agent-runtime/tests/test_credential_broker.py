"""B01 orchestration tests. Memory authority is NOT the SQL/multi-process gate."""

import asyncio
import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from agent_runtime.contracts.credentials import (
    EgressDestination,
    GrantRef,
    ProxyRequest,
    ScopeRequest,
)
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.credentials import BrokerLimits, CredentialBroker
from agent_runtime.credentials.ports import FilteredBody, GrantState, ProxyReply


@pytest.fixture
def world():
    now = datetime.now(UTC)
    ref = {"key": {"namespace": "fixture", "key": "api"}, "revision": "1"}
    target = EgressDestination(
        destination=ref, hostname="example.com", action="read", method="POST", path="/report"
    )
    actor = ActorContext(scope_id="one", actor_id="user", auth_revision="1", grants_ref="authority")
    scope = ScopeRequest(
        actor=actor,
        session_id=uuid4(),
        run_id=uuid4(),
        execution_id=uuid4(),
        execution_generation="1",
        operation_id=uuid4(),
        destination=target.destination,
        actions=("read",),
        policy_decision_id=uuid4(),
        input_digest="a" * 64,
        expires_at=now + timedelta(minutes=2),
        max_requests=1,
        max_response_bytes=128,
    )
    grant = GrantRef(
        grant_id=uuid4(),
        scope_id=actor.scope_id,
        run_id=scope.run_id,
        execution_id=scope.execution_id,
        execution_generation="1",
        revision="1",
        issued_at=now,
        expires_at=scope.expires_at,
    )
    payload = b'{"query":"aggregate"}'
    request = ProxyRequest(
        grant=grant,
        request_id=uuid4(),
        action="read",
        request_payload_ref="request-private",
        request_digest=hashlib.sha256(payload).hexdigest(),
    )

    class Store:
        state = GrantState(scope, grant, "1", None, 0)
        reservations = {}
        completed = {}
        unknowns = []

        async def issue(self, caller, request):
            assert caller == actor and request == scope
            return grant

        async def inspect(self, caller, request):
            return self.state

        async def reserve(self, caller, request):
            key = request.request_id
            if key in self.completed:
                assert self.reservations[key] == request
                return self.completed[key]
            if key in self.reservations or self.state.used_requests >= scope.max_requests:
                raise ValueError("request already reserved or quota exhausted")
            self.reservations[key] = request
            self.state = replace(self.state, used_requests=self.state.used_requests + 1)

        async def check_reserved(self, caller, request):
            if self.state.revoked_at or self.state.generation != "1":
                raise ValueError("revoked")

        async def finish(self, caller, request, response):
            await self.check_reserved(caller, request)
            self.completed[request.request_id] = response
            return response

        async def unknown(self, caller, request):
            self.unknowns.append(request.request_id)

        async def revoke(self, caller, reference):
            assert caller == actor and reference == grant
            self.state = replace(self.state, revoked_at=now)

    class Registry:
        def resolve(self, ref):
            assert ref == target.destination
            return target

    class Vault:
        async def bearer(self, scope_id, ref):
            assert scope_id == actor.scope_id and ref == target.destination
            return "fixture-secret-never-persist"

    class Blobs:
        writes = []

        async def read(self, caller, ref, limit):
            assert caller == actor and ref == "request-private"
            return payload

        async def put_private(self, caller, request_id, data):
            assert caller == actor and request_id == request.request_id
            self.writes.append(data)
            return "filtered-private"

    class Resolver:
        addresses = ("93.184.216.34",)

        async def resolve(self, host, port):
            assert host == target.hostname and port == 443
            return self.addresses

    class Transport:
        calls = 0
        reply = ProxyReply(200, b'{"total":42}')

        async def send(self, resolved, addresses, **kwargs):
            assert resolved == target and addresses == ("93.184.216.34",)
            assert kwargs["bearer"] == "fixture-secret-never-persist"
            self.calls += 1
            return self.reply

    class Filter:
        async def apply(self, target, reply):
            return FilteredBody(reply.body, target.destination, "allowed")

    store, vault, blobs = Store(), Vault(), Blobs()
    resolver, transport = Resolver(), Transport()
    broker = CredentialBroker(
        store=store,
        vault=vault,
        blobs=blobs,
        registry=Registry(),
        resolver=resolver,
        transport=transport,
        response_filter=Filter(),
    )
    return locals()


@pytest.mark.asyncio
async def test_authorize_and_private_result_replay(world):
    w = world
    assert await w["broker"].authorize(w["actor"], w["scope"]) == w["grant"]
    result = await w["broker"].proxy(w["actor"], w["request"])
    assert result.size_bytes == 12 and result.body_ref == "filtered-private"
    assert "fixture-secret" not in result.model_dump_json()
    assert await w["broker"].proxy(w["actor"], w["request"]) == result
    assert w["transport"].calls == 1 and len(w["blobs"].writes) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("offset_seconds,allowed", [(3, True), (6, False)])
async def test_grant_not_before_allows_only_bounded_clock_skew(world, offset_seconds, allowed):
    w = world
    grant = w["grant"].model_copy(update={
        "issued_at": datetime.now(UTC) + timedelta(seconds=offset_seconds)
    })
    w["store"].state = replace(w["store"].state, grant=grant)
    request = w["request"].model_copy(update={"grant": grant})
    if allowed:
        assert (await w["broker"].proxy(w["actor"], request)).status_code == 200
        assert w["transport"].calls == 1
    else:
        with pytest.raises(AgentRuntimeError) as failure:
            await w["broker"].proxy(w["actor"], request)
        assert failure.value.failure.code == "DELIVERY_EXPIRED"
        assert w["transport"].calls == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["actor", "scope", "generation", "revoked", "digest", "dns"])
async def test_preflight_rejection_never_sends(world, change):
    w = world
    caller, request = w["actor"], w["request"]
    if change == "actor":
        caller = caller.model_copy(update={"actor_id": "another"})
    elif change == "scope":
        caller = caller.model_copy(update={"scope_id": "another"})
    elif change == "generation":
        w["store"].state = replace(w["store"].state, generation="2")
    elif change == "revoked":
        await w["broker"].revoke(caller, w["grant"])
    elif change == "digest":
        request = request.model_copy(update={"request_digest": "b" * 64})
    else:
        w["resolver"].addresses = ("93.184.216.34", "169.254.169.254")
    with pytest.raises(AgentRuntimeError):
        await w["broker"].proxy(caller, request)
    assert w["transport"].calls == 0 and not w["store"].reservations


@pytest.mark.asyncio
async def test_allowance_and_request_identity(world):
    w = world
    await w["broker"].proxy(w["actor"], w["request"])
    with pytest.raises(AgentRuntimeError):
        await w["broker"].proxy(w["actor"], w["request"].model_copy(update={"request_id": uuid4()}))
    assert w["transport"].calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["vault", "transport", "filter", "blobs"])
async def test_failure_does_not_leak_secret_or_replay(world, failure):
    w = world

    async def fail(*args, **kwargs):
        raise RuntimeError("fixture-secret-never-persist")

    component, method = {
        "vault": (w["vault"], "bearer"),
        "transport": (w["transport"], "send"),
        "filter": (w["broker"]._filter, "apply"),
        "blobs": (w["blobs"], "put_private"),
    }[failure]
    setattr(component, method, fail)
    with pytest.raises(AgentRuntimeError) as error:
        await w["broker"].proxy(w["actor"], w["request"])
    assert "fixture-secret" not in str(error.value)
    assert w["store"].state.used_requests == 1 and not w["store"].completed
    with pytest.raises(AgentRuntimeError):
        await w["broker"].proxy(w["actor"], w["request"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reply",
    [
        ProxyReply(302, b""),
        ProxyReply(200, b"x" * 129),
        ProxyReply(200, b"fixture-secret-never-persist"),
    ],
)
async def test_redirect_oversize_and_echo_are_not_persisted(world, reply):
    w = world
    w["transport"].reply = reply
    with pytest.raises(AgentRuntimeError):
        await w["broker"].proxy(w["actor"], w["request"])
    assert not w["blobs"].writes and not w["store"].completed


@pytest.mark.asyncio
@pytest.mark.parametrize("moment", ["before_send", "before_finish"])
async def test_revoke_during_request_blocks_send_or_delivery(world, moment):
    w = world
    if moment == "before_send":
        original = w["vault"].bearer

        async def revoke(*args):
            secret = await original(*args)
            await w["broker"].revoke(w["actor"], w["grant"])
            return secret

        w["vault"].bearer = revoke
    else:
        original = w["transport"].send

        async def revoke(*args, **kwargs):
            result = await original(*args, **kwargs)
            await w["broker"].revoke(w["actor"], w["grant"])
            return result

        w["transport"].send = revoke
    with pytest.raises(AgentRuntimeError):
        await w["broker"].proxy(w["actor"], w["request"])
    assert w["transport"].calls == (0 if moment == "before_send" else 1)
    assert not w["blobs"].writes and not w["store"].completed


@pytest.mark.asyncio
async def test_cancellation_keeps_consumed_reservation(world):
    w = world
    started = asyncio.Event()

    async def blocked(*args, **kwargs):
        started.set()
        await asyncio.Event().wait()

    w["transport"].send = blocked
    task = asyncio.create_task(w["broker"].proxy(w["actor"], w["request"]))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert w["store"].state.used_requests == 1 and w["store"].unknowns
    assert not w["store"].completed


@pytest.mark.asyncio
async def test_duplicate_failure_does_not_settle_original_inflight_request(world):
    w = world
    started, proceed = asyncio.Event(), asyncio.Event()
    original_send = w["transport"].send

    async def blocked(*args, **kwargs):
        started.set()
        await proceed.wait()
        return await original_send(*args, **kwargs)

    w["transport"].send = blocked
    task = asyncio.create_task(w["broker"].proxy(w["actor"], w["request"]))
    await started.wait()
    try:
        with pytest.raises(AgentRuntimeError):
            await w["broker"].proxy(w["actor"], w["request"])
        assert not w["store"].unknowns
    finally:
        proceed.set()
    result = await task
    assert result.status_code == 200 and w["transport"].calls == 1


@pytest.mark.asyncio
async def test_total_deadline_includes_vault(world):
    w = world
    w["broker"]._limits = BrokerLimits(timeout_seconds=0.02)

    async def slow(*args):
        await asyncio.sleep(1)

    w["vault"].bearer = slow
    with pytest.raises(AgentRuntimeError):
        await w["broker"].proxy(w["actor"], w["request"])
    assert w["transport"].calls == 0 and w["store"].unknowns


@pytest.mark.asyncio
async def test_vault_typed_exception_is_also_sanitized(world):
    w = world

    async def unsafe(*args):
        raise runtime_failure(
            "FORBIDDEN", category="authorization", public_message="fixture-secret-never-persist"
        )

    w["vault"].bearer = unsafe
    with pytest.raises(AgentRuntimeError) as error:
        await w["broker"].proxy(w["actor"], w["request"])
    assert "fixture-secret" not in str(error.value)
    assert w["transport"].calls == 0 and not w["blobs"].writes


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["inspect", "read", "resolve", "registry"])
async def test_pre_reservation_dependency_typed_errors_cannot_leak(world, phase):
    w = world

    def unsafe_sync(*args):
        raise runtime_failure(
            "FORBIDDEN",
            category="authorization",
            public_message="fixture-secret-never-persist",
            detail_ref="fixture-secret-never-persist",
        )

    async def unsafe(*args):
        unsafe_sync(*args)

    if phase == "inspect":
        w["store"].inspect = unsafe
    elif phase == "read":
        w["blobs"].read = unsafe
    elif phase == "resolve":
        w["broker"]._resolver.resolve = unsafe
    else:
        w["broker"]._registry.resolve = unsafe_sync
    with pytest.raises(AgentRuntimeError) as error:
        await w["broker"].proxy(w["actor"], w["request"])
    assert "fixture-secret" not in error.value.failure.model_dump_json()
    assert error.value.failure.code == "FORBIDDEN"
    assert error.value.failure.retryable is False
    assert w["transport"].calls == 0
    assert w["store"].state.used_requests == 0
    assert not w["store"].unknowns


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["issue", "revoke"])
@pytest.mark.parametrize("typed", [True, False])
async def test_control_plane_dependency_errors_are_also_sanitized(world, phase, typed):
    w = world

    async def unsafe(*args):
        if typed:
            raise runtime_failure(
                "FORBIDDEN", category="authorization", public_message="fixture-secret-never-persist"
            )
        raise RuntimeError("fixture-secret-never-persist")

    setattr(w["store"], phase, unsafe)
    with pytest.raises(AgentRuntimeError) as error:
        if phase == "issue":
            await w["broker"].authorize(w["actor"], w["scope"])
        else:
            await w["broker"].revoke(w["actor"], w["grant"])
    assert "fixture-secret" not in error.value.failure.model_dump_json()
    assert error.value.failure.code == ("FORBIDDEN" if typed else "DEPENDENCY_UNAVAILABLE")
    assert error.value.failure.retryable is False


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["lifetime", "quota", "bytes", "actor"])
async def test_issue_rejects_overbroad_requests(world, change):
    w = world
    field, value = {
        "lifetime": ("expires_at", datetime.now(UTC) + timedelta(hours=2)),
        "quota": ("max_requests", 129),
        "bytes": ("max_response_bytes", 10_000_000),
        "actor": ("actor", w["actor"].model_copy(update={"actor_id": "fake"})),
    }[change]
    with pytest.raises(AgentRuntimeError):
        await w["broker"].authorize(w["actor"], w["scope"].model_copy(update={field: value}))

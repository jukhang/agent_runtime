"""B02 real PostgreSQL ledger matrix; no SQLite or mocked SQL evidence."""

import asyncio
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

pytest.importorskip("sqlalchemy")
import sqlalchemy as sa  # noqa: E402
from test_mcp_broker_integration import mcp_server as mcp_server  # noqa: E402
from test_storage_uow import (  # noqa: E402, F401
    _row,
    _seed,
    _seed_execution,
    _seed_work,
    actor,
    engine_for,
    migrate,
    run,
)

from agent_runtime.contracts.canonical import content_digest  # noqa: E402
from agent_runtime.contracts.credentials import (  # noqa: E402
    FilteredResponse,
    MaintenanceCredentialLease,
    ProxyRequest,
    ScopeRequest,
)
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure  # noqa: E402
from agent_runtime.storage.credential_broker import SqlCredentialGrantStore  # noqa: E402

pytestmark = pytest.mark.skipif(not os.getenv("AGENT_RUNTIME_TEST_DSN"), reason="real PG required")


def test_mcp_maintenance_credential_fences_work_takeover(world):
    async def exercise():
        engine = engine_for(world.schema)
        try:
            work_id = await _seed_work(world, status="leased", lease_epoch=1)
            execution_id = await _seed_execution(
                world, status="unknown", supervisor_work_id=work_id
            )
            operation_id = (
                await _row(world, "SELECT operation_id FROM execution WHERE id=:i", i=execution_id)
            )["operation_id"]
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f"UPDATE \"{world.schema}\".operation SET status='unknown',result_ref=NULL "
                        "WHERE id=:i"
                    ),
                    {"i": operation_id},
                )
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".work_item SET '
                        "lease_until=clock_timestamp()+interval '5 minutes' WHERE id=:i"
                    ),
                    {"i": work_id},
                )
            item = await _row(
                world,
                "SELECT owner_id,lease_epoch,lease_until FROM work_item WHERE id=:i",
                i=work_id,
            )
            principal = actor(world=world)
            destination = {"key": {"namespace": "test", "key": "mcp-lookup"}, "revision": "1"}
            scope = ScopeRequest(
                actor=principal,
                session_id=world.session_id,
                run_id=world.run_id,
                execution_id=execution_id,
                execution_generation="1",
                operation_id=operation_id,
                maintenance_lease=MaintenanceCredentialLease(
                    work_id=work_id,
                    owner_id=item["owner_id"],
                    epoch=str(item["lease_epoch"]),
                    lease_until=item["lease_until"],
                ),
                destination=destination,
                actions=("read",),
                policy_decision_id=uuid4(),
                input_digest="a" * 64,
                expires_at=min(item["lease_until"], datetime.now(UTC) + timedelta(seconds=20)),
                max_requests=1,
                max_response_bytes=1024,
            )

            async def authorize(db, caller, current, request, phase):
                assert caller == principal and current.destination == scope.destination
                assert current.maintenance_lease == scope.maintenance_lease

            store = SqlCredentialGrantStore(engine, world.schema, authorize=authorize)
            grant = await store.issue(principal, scope)
            request = ProxyRequest(
                grant=grant,
                request_id=uuid4(),
                action="read",
                request_payload_ref="private:lookup",
                request_digest=hashlib.sha256(b"{}").hexdigest(),
            )
            assert (await store.inspect(principal, request)).grant == grant
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".work_item SET lease_epoch=lease_epoch+1 '
                        "WHERE id=:i"
                    ),
                    {"i": work_id},
                )
            with pytest.raises(AgentRuntimeError) as stale:
                await store.inspect(principal, request)
            assert stale.value.failure.code == "FENCED"
        finally:
            await engine.dispose()

    run(exercise)


def test_mcp_maintenance_inspector_uses_sql_broker_and_remote_evidence(world, mcp_server):
    from agent_runtime.contracts.credentials import EgressDestination
    from agent_runtime.contracts.references import CapabilityRef
    from agent_runtime.contracts.tools import McpToolBinding, ToolSpec
    from agent_runtime.contracts.transactions import MaintenanceLease
    from agent_runtime.contracts.work_claims import MaintenanceClaimedWork
    from agent_runtime.credentials import CredentialBroker
    from agent_runtime.credentials.https import OutboundProtocolProfile, PinnedHttpsTransport
    from agent_runtime.credentials.ports import FilteredBody
    from agent_runtime.storage.mcp_maintenance import SqlMcpMaintenanceInspectorFactory

    async def exercise():
        port, tls, server_state = mcp_server
        engine = engine_for(world.schema)
        work_id = await _seed_work(world, status="leased", lease_epoch=1)
        execution_id = await _seed_execution(
            world, status="unknown", supervisor_work_id=work_id
        )
        operation_id = (
            await _row(world, "SELECT operation_id FROM execution WHERE id=:i", i=execution_id)
        )["operation_id"]
        key = f"remote-{execution_id}"
        server_state["effects"][key] = {
            "operation_key": key, "found": True, "status": "running"
        }
        async with engine.begin() as db:
            await db.execute(
                sa.text(f'UPDATE "{world.schema}".operation SET status=\'unknown\','
                        'result_ref=NULL WHERE id=:i'),
                {"i": operation_id},
            )
            await db.execute(
                sa.text(f'UPDATE "{world.schema}".execution SET deadline_at='
                        "clock_timestamp()-interval '1 second' WHERE id=:i"),
                {"i": execution_id},
            )
            await db.execute(
                sa.text(f'UPDATE "{world.schema}".work_item SET lease_until='
                        "clock_timestamp()+interval '5 minutes' WHERE id=:i"),
                {"i": work_id},
            )
        row = await _row(
            world, "SELECT owner_id,lease_epoch,lease_until FROM work_item WHERE id=:i",
            i=work_id,
        )
        principal = actor(world=world)
        lookup = CapabilityRef.model_validate(
            {"key": {"namespace": "test.mcp", "key": "maintenance-lookup"}, "revision": "1"}
        )
        effect = CapabilityRef.model_validate(
            {"key": {"namespace": "test.mcp", "key": "maintenance-effect"}, "revision": "1"}
        )
        cancel = CapabilityRef.model_validate(
            {"key": {"namespace": "test.mcp", "key": "maintenance-cancel"}, "revision": "1"}
        )
        targets = {
            ref: EgressDestination(
                destination=ref, hostname="mcp.test", port=port,
                action="read" if ref == lookup else "write", method="POST", path="/mcp",
                private_addresses=("127.0.0.1",),
            )
            for ref in (lookup, cancel)
        }

        async def authorize(db, caller, scope, request, phase):
            assert caller == principal and scope.maintenance_lease is not None
            assert scope.destination in targets
            assert scope.actions == (targets[scope.destination].action,)

        class Vault:
            async def bearer(self, scope_id, destination):
                assert scope_id == world.scope and destination in targets
                return server_state["token"]

        class Registry:
            def resolve(self, destination):
                return targets[destination]

        class Resolver:
            async def resolve(self, hostname, requested_port):
                assert hostname == "mcp.test" and requested_port == port
                return ("127.0.0.1",)

        class Filter:
            async def apply(self, target, reply):
                return FilteredBody(reply.body, target.destination, "allowed")

        class Blobs:
            def __init__(self):
                self.items = {}

            async def read(self, caller, ref, limit):
                value = self.items[ref]
                assert len(value) <= limit
                return value

            async def put_private(self, caller, request_id, data):
                ref = str(uuid4())
                self.items[ref] = data
                return ref

        binding = McpToolBinding(
            protocol_version="2026-07-28", tool_name="increment_counter",
            destination=effect, lookup_name="lookup_effect", lookup_destination=lookup,
            cancel_name="cancel_effect", cancel_destination=cancel,
            input_schema={"type": "object"}, output_schema={"type": "object"},
        )
        def ref(name):
            return CapabilityRef.model_validate(
                {"key": {"namespace": "test", "key": name}, "revision": "1"}
            )
        spec = ToolSpec(
            ref=ref("effect"), description="effect", source=ref("server"),
            kind="write", side_effect="external", input_schema=CapabilityRef.model_validate({
                "key": {"namespace": "mcp.input", "key": content_digest(binding.input_schema)},
                "revision": "1",
            }), output_schema=CapabilityRef.model_validate({
                "key": {"namespace": "mcp.output", "key": content_digest(binding.output_schema)},
                "revision": "1",
            }), permissions=(ref("permission"),),
            approval_policy=ref("approval"), idempotency="provider_key",
            timeout_seconds=30, concurrency_group="mcp", max_concurrency=1,
            cancellation="stop", public_result_policy="full", mcp=binding,
        )
        work = MaintenanceClaimedWork.model_validate({
            "disposition": "maintenance_claimed", "kind": "reconcile_execution",
            "lease": MaintenanceLease(
                scope_id=world.scope, work_id=work_id, owner_id=row["owner_id"],
                work_epoch=str(row["lease_epoch"]), work_lease_until=row["lease_until"],
            ),
            "subject_ref": {
                "resourceType": "execution", "resourceId": str(execution_id),
                "resourceVersion": "1", "scope": {"scopeId": world.scope},
            },
            "payload_ref": "maintenance",
        })
        blobs = Blobs()
        transport = PinnedHttpsTransport(
            tls=tls, protocol_profiles={
                lookup: OutboundProtocolProfile(kind="mcp_tool", tool_name="lookup_effect"),
                cancel: OutboundProtocolProfile(kind="mcp_tool", tool_name="cancel_effect"),
            },
        )
        broker = CredentialBroker(
            store=SqlCredentialGrantStore(engine, world.schema, authorize=authorize),
            vault=Vault(), blobs=blobs, registry=Registry(), resolver=Resolver(),
            transport=transport, response_filter=Filter(),
        )
        inspector = SqlMcpMaintenanceInspectorFactory(
            engine, world.schema, actor=principal, spec=spec, broker=broker, blobs=blobs
        )(work)
        try:
            assert await inspector.inspect(key) == "running"
            await inspector.cancel(key)
            assert await inspector.inspect(key) == "stopped"
            assert server_state["cancel_count"] >= 1
            assert all(server_state["token"].encode() not in value
                       for value in blobs.items.values())
            async with engine.begin() as db:
                await db.execute(
                    sa.text(f'UPDATE "{world.schema}".work_item SET lease_epoch=lease_epoch+1 '
                            'WHERE id=:i'),
                    {"i": work_id},
                )
            with pytest.raises(AgentRuntimeError) as stale:
                await inspector.inspect(key)
            assert stale.value.failure.code == "FENCED"
        finally:
            await transport.aclose()
            await engine.dispose()

    run(exercise)


@pytest.fixture(scope="module")
def credential_schema():
    name = f"p04_credentials_{uuid4().hex[:12]}"
    assert migrate("upgrade", name).status == "applied"
    try:
        yield name
    finally:

        async def drop():
            engine = engine_for(name)
            try:
                async with engine.begin() as db:
                    await db.execute(sa.text(f'DROP SCHEMA "{name}" CASCADE'))
                    assert (
                        await db.execute(
                            sa.text("SELECT count(*) FROM pg_namespace WHERE nspname=:s"),
                            {"s": name},
                        )
                    ).scalar_one() == 0
            finally:
                await engine.dispose()

        run(drop)


@pytest.fixture
def world(credential_schema):
    from agent_runtime.storage_spec import TABLES

    try:
        yield run(lambda: _seed(credential_schema))
    finally:

        async def clear():
            engine = engine_for(credential_schema)
            try:
                async with engine.begin() as db:
                    tables = ",".join(f'"{credential_schema}"."{table.name}"' for table in TABLES)
                    await db.execute(sa.text(f"TRUNCATE {tables}"))
            finally:
                await engine.dispose()

        run(clear)


async def setup(world, engine, *, max_requests=2):
    execution_id = await _seed_execution(world)
    operation = (
        await _row(world, "SELECT operation_id FROM execution WHERE id=:i", i=execution_id)
    )["operation_id"]
    async with engine.begin() as db:
        from agent_runtime.storage.uow.schema import scoped

        await db.execute(
            scoped(
                "UPDATE {t}operation SET status='unknown',result_ref=NULL WHERE id=:i", world.schema
            ),
            {"i": operation},
        )
    principal = actor(world=world)
    policy_id = uuid4()
    scope = ScopeRequest(
        actor=principal,
        session_id=world.session_id,
        run_id=world.run_id,
        execution_id=execution_id,
        execution_generation="1",
        operation_id=operation,
        destination={"key": {"namespace": "test", "key": "api"}, "revision": "1"},
        actions=("read",),
        policy_decision_id=policy_id,
        input_digest="a" * 64,
        expires_at=datetime.now(UTC) + timedelta(minutes=2),
        max_requests=max_requests,
        max_response_bytes=1024,
    )
    access = {"allowed": True}

    async def authorize(db, caller, current, request, phase):
        assert caller.scope_id == world.scope and caller.actor_id == world.actor_id
        assert current.policy_decision_id == policy_id and current.destination == scope.destination
        if not access["allowed"]:
            raise runtime_failure(
                "FORBIDDEN", category="authorization", public_message="host denied"
            )

    store = SqlCredentialGrantStore(engine, world.schema, authorize=authorize)
    grant = await store.issue(principal, scope)
    request = ProxyRequest(
        grant=grant,
        request_id=uuid4(),
        action="read",
        request_payload_ref="private:request",
        request_digest=hashlib.sha256(b"{}").hexdigest(),
    )
    return store, scope, principal, request, access


def response(request):
    return FilteredResponse(
        request_id=request.request_id,
        grant_id=request.grant.grant_id,
        status_code=200,
        body_ref="private:filtered",
        body_digest=hashlib.sha256(b"ok").hexdigest(),
        size_bytes=2,
        filter_policy={"key": {"namespace": "test", "key": "filter"}, "revision": "1"},
        audit_ref=f"credential-request:{request.request_id}",
        disposition="allowed",
    )


def test_issue_reserve_finish_replay_and_audit(world):
    async def check():
        engine = engine_for(world.schema, search_path=False)
        try:
            store, scope, principal, request, _ = await setup(world, engine)
            assert await store.issue(principal, scope) == request.grant
            assert await store.reserve(principal, request) is None
            await store.check_reserved(principal, request)
            sealed = await store.finish(principal, request, response(request))
            assert await store.finish(principal, request, sealed) == sealed
            assert await store.reserve(principal, request) == sealed
            state = await store.inspect(principal, request)
            assert state.used_requests == 1
            audit = await _row(
                world,
                "SELECT array_agg(action ORDER BY created_at,id) AS actions FROM credential_audit",
            )
            assert set(audit["actions"]) == {"issued", "reserved", "completed"}
            assert len(audit["actions"]) == 3
            await store.unknown(principal, request)
            assert (await _row(world, "SELECT status FROM credential_request"))[
                "status"
            ] == "completed"
        finally:
            await engine.dispose()

    run(check)


def test_concurrent_reservations_cannot_exceed_allowance(world):
    async def check():
        engine = engine_for(world.schema, pool_size=8, search_path=False)
        try:
            store, _, principal, original, _ = await setup(world, engine, max_requests=3)
            requests = [original.model_copy(update={"request_id": uuid4()}) for _ in range(12)]
            result = await asyncio.gather(
                *(store.reserve(principal, req) for req in requests), return_exceptions=True
            )
            assert sum(item is None for item in result) == 3
            assert all(item is None or isinstance(item, AgentRuntimeError) for item in result)
            assert (await store.inspect(principal, original)).used_requests == 3
            assert (
                await _row(
                    world, "SELECT count(*) AS n FROM credential_audit WHERE action='reserved'"
                )
            )["n"] == 3
        finally:
            await engine.dispose()

    run(check)


@pytest.mark.parametrize(
    "change",
    [
        "actor",
        "scope",
        "generation",
        "auth",
        "control",
        "execution",
        "acl",
        "digest",
        "action",
        "slot",
        "operation",
        "expiry",
    ],
)
def test_current_authority_and_identity_are_enforced(world, change):
    async def check():
        engine = engine_for(world.schema)
        try:
            store, scope, principal, request, access = await setup(world, engine)
            await store.reserve(principal, request)
            if change == "actor":
                principal = principal.model_copy(update={"actor_id": "other"})
            elif change == "scope":
                principal = principal.model_copy(update={"scope_id": "other"})
            elif change == "generation":
                request = request.model_copy(
                    update={"grant": request.grant.model_copy(update={"execution_generation": "2"})}
                )
            elif change == "acl":
                access["allowed"] = False
            elif change == "digest":
                request = request.model_copy(update={"request_digest": "b" * 64})
            elif change == "action":
                request = request.model_copy(update={"action": "write"})
            else:
                async with engine.begin() as db:
                    sql = {
                        "auth": "UPDATE run SET authority_revision=2 WHERE id=:i",
                        "control": (
                            "UPDATE run SET status='paused',owner_id=NULL,lease_until=NULL "
                            "WHERE id=:i"
                        ),
                        "execution": "UPDATE execution SET status='completed' WHERE run_id=:i",
                        "slot": "UPDATE session_slot SET owner_run_id=NULL WHERE owner_run_id=:i",
                        "operation": (
                            "UPDATE operation SET status='failed_safe' "
                            "WHERE id IN (SELECT operation_id FROM execution WHERE run_id=:i)"
                        ),
                        "expiry": (
                            "UPDATE capability_grant "
                            "SET issued_at=clock_timestamp()-interval '2s', "
                            "expires_at=clock_timestamp()-interval '1s' "
                            "WHERE run_id=:i"
                        ),
                    }[change]
                    await db.execute(sa.text(sql), {"i": world.run_id})
            with pytest.raises(AgentRuntimeError):
                await store.check_reserved(principal, request)
        finally:
            await engine.dispose()

    run(check)


def test_unknown_survives_revocation_and_cannot_replay(world):
    async def check():
        engine = engine_for(world.schema)
        try:
            store, scope, principal, request, _ = await setup(world, engine)
            await store.reserve(principal, request)
            await store.revoke(principal, request.grant)
            await store.revoke(principal, request.grant)
            await store.unknown(principal, request)
            await store.unknown(principal, request)
            assert (await _row(world, "SELECT status FROM credential_request"))[
                "status"
            ] == "unknown"
            for operation in (
                store.inspect(principal, request),
                store.reserve(principal, request),
                store.issue(principal, scope),
            ):
                with pytest.raises(AgentRuntimeError):
                    await operation
            assert (
                await _row(
                    world,
                    "SELECT count(*) AS n FROM credential_audit "
                    "WHERE action IN ('unknown','revoked')",
                )
            )["n"] == 2
        finally:
            await engine.dispose()

    run(check)


def test_unknown_and_reserved_are_not_replayed_by_new_store(world):
    async def check():
        engine = engine_for(world.schema)
        try:
            store, scope, principal, request, _ = await setup(world, engine)
            await store.reserve(principal, request)
            fresh = SqlCredentialGrantStore(engine, world.schema, authorize=store._authorize)
            with pytest.raises(AgentRuntimeError):
                await fresh.reserve(principal, request)
            await store.unknown(principal, request)
            with pytest.raises(AgentRuntimeError):
                await fresh.reserve(principal, request)
            with pytest.raises(AgentRuntimeError):
                await fresh.finish(principal, request, response(request))
            assert (await fresh.inspect(principal, request)).used_requests == 1
        finally:
            await engine.dispose()

    run(check)


@pytest.mark.parametrize("phase", ["reserve", "finish", "revoke"])
def test_audit_failure_rolls_back_mutation(world, phase):
    async def check():
        engine = engine_for(world.schema)
        try:
            store, _, principal, request, _ = await setup(world, engine)
            if phase != "reserve":
                await store.reserve(principal, request)
            original = store._audit

            async def fault(*args, **kwargs):
                await original(*args, **kwargs)
                raise RuntimeError("injected audit failure")

            store._audit = fault
            with pytest.raises(RuntimeError):
                if phase == "reserve":
                    await store.reserve(principal, request)
                elif phase == "finish":
                    await store.finish(principal, request, response(request))
                else:
                    await store.revoke(principal, request.grant)
            assert (await store.inspect(principal, request)).used_requests == int(
                phase != "reserve"
            )
            assert (await _row(world, "SELECT count(*) AS n FROM credential_audit"))["n"] == (
                1 if phase == "reserve" else 2
            )
            if phase != "reserve":
                assert (await _row(world, "SELECT status FROM credential_request"))["status"] == (
                    "reserved"
                )
        finally:
            await engine.dispose()

    run(check)


def test_response_binding_and_replay_after_revoke(world):
    async def check():
        engine = engine_for(world.schema)
        try:
            store, _, principal, request, _ = await setup(world, engine)
            await store.reserve(principal, request)
            with pytest.raises(AgentRuntimeError):
                await store.finish(
                    principal, request, response(request).model_copy(update={"request_id": uuid4()})
                )
            await store.finish(principal, request, response(request))
            await store.revoke(principal, request.grant)
            with pytest.raises(AgentRuntimeError):
                await store.reserve(principal, request)
            raw = await _row(world, "SELECT json_agg(evidence) AS evidence FROM credential_audit")
            assert "Bearer" not in json.dumps(raw, default=str)
        finally:
            await engine.dispose()

    run(check)


def test_service_binding_is_persistent_immutable_and_revocable(world):
    async def check():
        engine = engine_for(world.schema)
        try:
            store, _, principal, request, _ = await setup(world, engine)
            peer = "a" * 64
            with pytest.raises(AgentRuntimeError):
                await store.resolve_service(peer, request.grant)
            await store.bind_service(principal, request.grant, peer)
            await store.bind_service(principal, request.grant, peer)
            fresh = SqlCredentialGrantStore(engine, world.schema, authorize=store._authorize)
            assert await fresh.resolve_service(peer, request.grant) == principal
            for wrong in ("b" * 64, "", "A" * 64):
                with pytest.raises(AgentRuntimeError):
                    await fresh.resolve_service(wrong, request.grant)
            with pytest.raises(AgentRuntimeError):
                await store.bind_service(principal, request.grant, "b" * 64)
            await store.revoke(principal, request.grant)
            with pytest.raises(AgentRuntimeError):
                await fresh.resolve_service(peer, request.grant)
            assert (await _row(world, "SELECT count(*) AS n FROM credential_service_binding"))[
                "n"
            ] == 1
        finally:
            await engine.dispose()

    run(check)

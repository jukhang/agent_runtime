"""SQL credential authority and private audit; no network calls or raw secrets."""

import hmac
import json
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from typing import Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.credentials import (
    FilteredResponse,
    GrantRef,
    ProxyRequest,
    ScopeRequest,
)
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.credentials import BrokerLimits
from agent_runtime.credentials.ports import GrantState
from agent_runtime.storage.uow import errors, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped

type CredentialPhase = Literal["issue", "inspect", "reserve", "send", "finish", "revoke"]
type CredentialAuthorizer = Callable[
    [AsyncSession, ActorContext, ScopeRequest, ProxyRequest | None, CredentialPhase],
    Awaitable[None],
]


class SqlCredentialGrantStore:
    """Host callback is mandatory and transactional: Policy, ACL, destination/input binding.

    It must not acquire locks preceding SessionSlot/Run or do network I/O. The caller
    comes from authenticated transport. This store is NOT an authentication service.
    Counts are durable request rows, serialized by the grant lock; there is no counter
    cache and no automatic refund. Audit failures roll back the corresponding mutation.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        authorize: CredentialAuthorizer,
        limits: BrokerLimits | None = None,
    ) -> None:
        self._schema = require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._locks = locks.build_lock_statements(schema)
        self._authorize = authorize
        self._limits = limits or BrokerLimits()

    async def _one(self, db: AsyncSession, sql: str, **params: Any) -> dict[str, Any] | None:
        row = (await db.execute(scoped(sql, self._schema), params)).mappings().one_or_none()
        return None if row is None else dict(row)

    async def _now(self, db: AsyncSession) -> datetime:
        value = (
            await db.execute(scoped("SELECT clock_timestamp() AS now", self._schema))
        ).scalar_one()
        assert isinstance(value, datetime)
        return value

    async def _run(self, db: AsyncSession, caller: ActorContext, run_id: UUID) -> dict[str, Any]:
        hint = await self._one(
            db,
            "SELECT session_id,root_run_id,parent_run_id FROM {t}run WHERE scope_id=:s AND id=:r",
            s=caller.scope_id,
            r=run_id,
        )
        if hint is None:
            errors.forbidden("credential run is not available")
        slot = await locks.lock_slot(db, self._locks, caller.scope_id, hint["session_id"])
        if slot is None:
            errors.forbidden("credential session slot is unavailable")
        rows = {}
        for identity in sorted({run_id, hint["root_run_id"]}, key=str):
            row = await locks.lock_run(db, self._locks, caller.scope_id, identity)
            if row is None or row["session_id"] != hint["session_id"]:
                errors.forbidden("credential run ownership changed")
            rows[identity] = row
        run = rows[run_id]
        if (
            run["root_run_id"] != hint["root_run_id"]
            or run["parent_run_id"] != hint["parent_run_id"]
        ):
            errors.forbidden("credential run ancestry changed")
        root = rows[hint["root_run_id"]]
        if root["parent_run_id"] is not None or root["root_run_id"] != root["id"]:
            errors.forbidden("credential root ownership is invalid")
        # Current Runtime supports root/direct-child delegation only; refuse unknown ancestry.
        if run_id != root["id"] and run["parent_run_id"] != root["id"]:
            errors.forbidden("unsupported credential delegation ancestry")
        run["_root"] = root
        run["_slot_owner"] = slot["owner_run_id"]
        return run

    async def _live(
        self,
        db: AsyncSession,
        caller: ActorContext,
        scope: ScopeRequest,
        run: Mapping[str, Any],
        request: ProxyRequest | None,
        phase: CredentialPhase,
    ) -> datetime:
        now = await self._now(db)
        if scope.maintenance_lease is not None:
            return await self._maintenance_live(db, caller, scope, run, request, phase, now)
        if run["id"] == run["root_run_id"]:
            if run["_slot_owner"] != run["root_run_id"]:
                errors.forbidden("credential root no longer owns the session slot")
        elif run["_slot_owner"] is not None or run["_root"]["status"] not in {
            "waiting_tool",
            "waiting_user",
        }:
            errors.forbidden("credential child requires a yielded root session slot")
        if scope.actor != caller or scope.session_id != run["session_id"]:
            errors.forbidden("credential actor/session binding changed")
        session = await self._one(
            db,
            "SELECT deleted_at FROM {t}session WHERE scope_id=:s AND id=:i",
            s=caller.scope_id,
            i=scope.session_id,
        )
        if session is None or session["deleted_at"] is not None:
            errors.forbidden("credential session is unavailable")
        for owner in (run["_root"], run):
            allowed_statuses = (
                {"running", "waiting_tool", "waiting_user"}
                if run["id"] != run["root_run_id"] and owner["id"] == run["root_run_id"]
                else {"running", "waiting_tool"}
            )
            if (
                owner["status"] not in allowed_statuses
                or owner["desired_control"] is not None
                or owner["deadline_at"] <= now
            ):
                errors.control_pending("credential owner cannot execute or deliver")
            await revalidate_authority(db, self._locks, caller, run=owner, now=now)
        maximum_deadline = run["deadline_at"]
        if scope.tool_call_id is not None:
            call = await self._one(
                db,
                "SELECT c.run_id,c.status,c.input_hash,a.status AS attempt_status "
                "FROM {t}tool_call c JOIN {t}model_attempt a "
                "ON a.scope_id=c.scope_id AND a.id=c.attempt_id "
                "WHERE c.scope_id=:s AND c.id=:i FOR UPDATE OF c",
                s=caller.scope_id,
                i=scope.tool_call_id,
            )
            if (
                call is None
                or call["run_id"] != scope.run_id
                or call["status"] not in {"pending", "running"}
                or call["attempt_status"] != "completed"
                or call["input_hash"] != scope.input_digest
            ):
                errors.forbidden("read credential is not bound to a live model tool call")
        else:
            execution = await self._one(
                db,
                "SELECT * FROM {t}execution WHERE scope_id=:s AND id=:i FOR UPDATE",
                s=caller.scope_id,
                i=scope.execution_id,
            )
            if (
                execution is None
                or execution["run_id"] != scope.run_id
                or execution["operation_id"] != scope.operation_id
                or execution["generation"] != int(scope.execution_generation or "-1")
                or execution["status"] != "running"
                or execution["failure"] is not None
                or execution["deadline_at"] <= now
            ):
                errors.forbidden("credential execution is not live at this generation")
            operation = await self._one(
                db,
                "SELECT task_id,status FROM {t}operation WHERE scope_id=:s AND id=:i",
                s=caller.scope_id,
                i=scope.operation_id,
            )
            if (
                operation is None
                or operation["task_id"] != run["task_id"]
                or operation["status"] not in {"dispatching", "unknown"}
            ):
                errors.forbidden("credential operation is not awaiting external execution")
            maximum_deadline = min(maximum_deadline, execution["deadline_at"])
        if (
            not now < scope.expires_at <= maximum_deadline
            or (scope.expires_at - now).total_seconds() > self._limits.max_grant_seconds
            or scope.max_requests > self._limits.max_requests
            or scope.max_response_bytes > self._limits.max_response_bytes
        ):
            errors.forbidden("credential scope exceeds current limits or deadline")
        await self._authorize(db, caller, scope, request, phase)
        return now

    async def _maintenance_live(
        self,
        db: AsyncSession,
        caller: ActorContext,
        scope: ScopeRequest,
        run: Mapping[str, Any],
        request: ProxyRequest | None,
        phase: CredentialPhase,
        now: datetime,
    ) -> datetime:
        lease = scope.maintenance_lease
        assert lease is not None
        if scope.actor != caller or scope.session_id != run["session_id"]:
            errors.forbidden("maintenance credential actor or session changed")
        execution = await self._one(
            db,
            "SELECT run_id,operation_id,supervisor_work_id,generation,status,deadline_at "
            "FROM {t}execution WHERE scope_id=:s AND id=:i FOR UPDATE",
            s=caller.scope_id,
            i=scope.execution_id,
        )
        item = await self._one(
            db,
            "SELECT run_id,kind,status,owner_id,lease_epoch,lease_until "
            "FROM {t}work_item WHERE scope_id=:s AND id=:i FOR UPDATE",
            s=caller.scope_id,
            i=lease.work_id,
        )
        operation = await self._one(
            db,
            "SELECT task_id,status FROM {t}operation WHERE scope_id=:s AND id=:i",
            s=caller.scope_id,
            i=scope.operation_id,
        )
        if (
            item is None
            or execution is None
            or operation is None
            or item["run_id"] != scope.run_id
            or item["kind"] != "reconcile_execution"
            or item["status"] != "leased"
            or item["owner_id"] != lease.owner_id
            or int(item["lease_epoch"]) != int(lease.epoch)
            or item["lease_until"] != lease.lease_until
            or item["lease_until"] <= now
            or execution["supervisor_work_id"] != lease.work_id
            or execution["run_id"] != scope.run_id
            or execution["operation_id"] != scope.operation_id
            or str(execution["generation"]) != scope.execution_generation
            or execution["status"] not in {"running", "unknown"}
            or operation["task_id"] != run["task_id"]
            or operation["status"] not in {"dispatching", "unknown"}
        ):
            errors.fenced("maintenance credential lease or effect changed")
        if scope.actions == ("write",) and not (
            run["desired_control"] == "cancel" or execution["deadline_at"] <= now
        ):
            errors.forbidden("maintenance cancellation is not requested")
        if (
            not now < scope.expires_at <= lease.lease_until
            or (scope.expires_at - now).total_seconds() > self._limits.max_grant_seconds
            or scope.max_requests > self._limits.max_requests
            or scope.max_response_bytes > self._limits.max_response_bytes
        ):
            errors.forbidden("maintenance credential exceeds its work lease")
        await self._authorize(db, caller, scope, request, phase)
        return now

    async def _grant(
        self, db: AsyncSession, caller: ActorContext, ref: GrantRef
    ) -> tuple[dict[str, Any], ScopeRequest]:
        row = await self._one(
            db,
            "SELECT * FROM {t}capability_grant WHERE scope_id=:s AND id=:i FOR UPDATE",
            s=caller.scope_id,
            i=ref.grant_id,
        )
        if row is None or ref.scope_id != caller.scope_id or row["actor_id"] != caller.actor_id:
            errors.forbidden("credential grant is unavailable")
        manifest = row["allowed_scope"]
        if not isinstance(manifest, dict) or manifest.get("kind") != "credential_proxy":
            errors.forbidden("not a credential grant")
        scope = ScopeRequest.model_validate(manifest["request"])
        if self._ref(row) != ref:
            errors.forbidden("credential grant reference does not match its stored identity")
        return row, scope

    @staticmethod
    def _ref(row: Mapping[str, Any]) -> GrantRef:
        return GrantRef(
            grant_id=row["id"],
            scope_id=row["scope_id"],
            run_id=row["run_id"],
            execution_id=row["execution_id"],
            execution_generation=(
                str(row["execution_generation"])
                if row["execution_generation"] is not None
                else None
            ),
            revision=str(row["revision"]),
            issued_at=row["issued_at"],
            expires_at=row["expires_at"],
        )

    async def _audit(
        self,
        db: AsyncSession,
        caller: ActorContext,
        grant_id: UUID,
        action: str,
        request_id: UUID | None = None,
        evidence: dict[str, object] | None = None,
    ) -> None:
        identity = uuid5(
            NAMESPACE_URL, f"credential-audit:{caller.scope_id}:{grant_id}:{request_id}:{action}"
        )
        await db.execute(
            scoped(
                "INSERT INTO {t}credential_audit "
                "(id,scope_id,grant_id,request_id,actor_id,action,evidence) "
                "VALUES (:i,:s,:g,:r,:a,:k,CAST(:e AS jsonb))",
                self._schema,
            ),
            {
                "i": identity,
                "s": caller.scope_id,
                "g": grant_id,
                "r": request_id,
                "a": caller.actor_id,
                "k": action,
                "e": json.dumps(evidence or {}),
            },
        )

    async def issue(self, caller: ActorContext, request: ScopeRequest) -> GrantRef:
        request = ScopeRequest.model_validate(request.model_dump())
        digest = content_digest(request.model_dump(mode="json", by_alias=True))
        identity = uuid5(NAMESPACE_URL, f"credential-grant:{caller.scope_id}:{digest}")
        async with self._sessions.begin() as db:
            run = await self._run(db, caller, request.run_id)
            now = await self._live(db, caller, request, run, None, "issue")
            row = await self._one(
                db,
                "SELECT * FROM {t}capability_grant WHERE scope_id=:s AND id=:i FOR UPDATE",
                s=caller.scope_id,
                i=identity,
            )
            if row is not None:
                if row["revoked_at"] is not None:
                    errors.forbidden("credential grant was already revoked")
                return self._ref(row)
            manifest = {
                "kind": "credential_proxy",
                "request": request.model_dump(mode="json", by_alias=True),
            }
            await db.execute(
                scoped(
                    "INSERT INTO {t}capability_grant "
                    "(id,scope_id,run_id,execution_id,execution_generation,"
                    "actor_id,revision,token_hash,allowed_scope,issued_at,expires_at,revoked_at) "
                    "VALUES (:i,:s,:r,:e,:g,:a,1,NULL,CAST(:m AS jsonb),:n,:x,NULL)",
                    self._schema,
                ),
                {
                    "i": identity,
                    "s": caller.scope_id,
                    "r": request.run_id,
                    "e": request.execution_id,
                    "g": (
                        int(request.execution_generation)
                        if request.execution_generation is not None
                        else None
                    ),
                    "a": caller.actor_id,
                    "m": json.dumps(manifest),
                    "n": now,
                    "x": request.expires_at,
                },
            )
            await self._audit(db, caller, identity, "issued", evidence={"scopeDigest": digest})
            return GrantRef(
                grant_id=identity,
                scope_id=caller.scope_id,
                run_id=request.run_id,
                execution_id=request.execution_id,
                execution_generation=request.execution_generation,
                revision="1",
                issued_at=now,
                expires_at=request.expires_at,
            )

    async def _current(
        self,
        db: AsyncSession,
        caller: ActorContext,
        request: ProxyRequest,
        phase: CredentialPhase,
    ) -> tuple[dict[str, Any], ScopeRequest, datetime]:
        run = await self._run(db, caller, request.grant.run_id)
        # Decode before checking authority, but acquire the grant lock only AFTER Execution.
        initial = await self._one(
            db,
            "SELECT allowed_scope FROM {t}capability_grant WHERE scope_id=:s AND id=:i",
            s=caller.scope_id,
            i=request.grant.grant_id,
        )
        if (
            initial is None
            or not isinstance(initial["allowed_scope"], dict)
            or initial["allowed_scope"].get("kind") != "credential_proxy"
        ):
            errors.forbidden("credential grant is unavailable")
        scope = ScopeRequest.model_validate(initial["allowed_scope"]["request"])
        if scope.run_id != request.grant.run_id or request.action not in scope.actions:
            errors.forbidden("credential action or run binding changed")
        now = await self._live(db, caller, scope, run, request, phase)
        row, stored = await self._grant(db, caller, request.grant)
        if stored != scope or row["revoked_at"] is not None or row["expires_at"] <= now:
            errors.forbidden("credential grant changed or was revoked")
        return row, scope, now

    async def _request(
        self, db: AsyncSession, caller: ActorContext, request: ProxyRequest
    ) -> dict[str, Any] | None:
        row = await self._one(
            db,
            "SELECT * FROM {t}credential_request WHERE scope_id=:s AND id=:i FOR UPDATE",
            s=caller.scope_id,
            i=request.request_id,
        )
        if row is not None and (
            row["grant_id"] != request.grant.grant_id
            or row["request_hash"] != content_digest(request.model_dump(mode="json", by_alias=True))
        ):
            errors.forbidden("credential request identity was reused with different input")
        return row

    async def inspect(self, caller: ActorContext, request: ProxyRequest) -> GrantState:
        async with self._sessions.begin() as db:
            row, scope, _ = await self._current(db, caller, request, "inspect")
            count = await self._count(db, caller.scope_id, row["id"])
            return GrantState(
                scope, self._ref(row), scope.execution_generation, row["revoked_at"], count
            )

    async def _count(self, db: AsyncSession, scope_id: str, grant_id: UUID) -> int:
        row = await self._one(
            db,
            "SELECT count(*) AS n FROM {t}credential_request WHERE scope_id=:s AND grant_id=:g",
            s=scope_id,
            g=grant_id,
        )
        assert row is not None
        return int(row["n"])

    async def reserve(self, caller: ActorContext, request: ProxyRequest) -> FilteredResponse | None:
        async with self._sessions.begin() as db:
            _, scope, _ = await self._current(db, caller, request, "reserve")
            row = await self._request(db, caller, request)
            if row is not None:
                if row["status"] == "completed":
                    return FilteredResponse.model_validate(row["response"])
                raise runtime_failure(
                    "SIDE_EFFECT_UNKNOWN",
                    category="tool",
                    public_message="Credential request is reserved or unknown; do not replay.",
                )
            if await self._count(db, caller.scope_id, request.grant.grant_id) >= scope.max_requests:
                raise runtime_failure(
                    "BUDGET_EXHAUSTED",
                    category="budget",
                    public_message="Credential allowance exhausted.",
                )
            digest = content_digest(request.model_dump(mode="json", by_alias=True))
            await db.execute(
                scoped(
                    "INSERT INTO {t}credential_request (id,scope_id,grant_id,request_hash,status) "
                    "VALUES (:i,:s,:g,:h,'reserved')",
                    self._schema,
                ),
                {
                    "i": request.request_id,
                    "s": caller.scope_id,
                    "g": request.grant.grant_id,
                    "h": digest,
                },
            )
            await self._audit(
                db,
                caller,
                request.grant.grant_id,
                "reserved",
                request.request_id,
                {"requestDigest": digest},
            )
            return None

    async def check_reserved(self, caller: ActorContext, request: ProxyRequest) -> None:
        async with self._sessions.begin() as db:
            await self._current(db, caller, request, "send")
            row = await self._request(db, caller, request)
            if row is None or row["status"] != "reserved":
                errors.forbidden("credential reservation is not dispatchable")

    async def finish(
        self, caller: ActorContext, request: ProxyRequest, response: FilteredResponse
    ) -> FilteredResponse:
        response = FilteredResponse.model_validate(response.model_dump())
        async with self._sessions.begin() as db:
            _, scope, now = await self._current(db, caller, request, "finish")
            row = await self._request(db, caller, request)
            if row is None or row["status"] not in {"reserved", "completed"}:
                errors.forbidden("credential reservation cannot finish")
            if (
                response.grant_id != request.grant.grant_id
                or response.request_id != request.request_id
                or response.size_bytes > scope.max_response_bytes
                or response.audit_ref != f"credential-request:{request.request_id}"
                or (response.disposition == "blocked" and response.size_bytes != 0)
            ):
                errors.forbidden("credential response binding or limit is invalid")
            value = response.model_dump(mode="json", by_alias=True)
            if row["status"] == "completed":
                if row["response"] != value:
                    errors.version_conflict("credential result was already sealed differently")
                return response
            await db.execute(
                scoped(
                    "UPDATE {t}credential_request SET status='completed',"
                    "response=CAST(:v AS jsonb),"
                    "finished_at=:n,updated_at=:n WHERE scope_id=:s AND id=:i",
                    self._schema,
                ),
                {"v": json.dumps(value), "n": now, "s": caller.scope_id, "i": request.request_id},
            )
            await self._audit(
                db,
                caller,
                request.grant.grant_id,
                "completed",
                request.request_id,
                {"responseDigest": content_digest(value)},
            )
            return response

    async def unknown(self, caller: ActorContext, request: ProxyRequest) -> None:
        async with self._sessions.begin() as db:
            await self._run(db, caller, request.grant.run_id)
            _, scope = await self._grant(db, caller, request.grant)
            if scope.actor != caller:
                errors.forbidden("credential settlement actor does not match")
            row = await self._request(db, caller, request)
            if row is None or row["status"] != "reserved":
                return
            await db.execute(
                scoped(
                    "UPDATE {t}credential_request SET status='unknown',"
                    "updated_at=clock_timestamp() "
                    "WHERE scope_id=:s AND id=:i",
                    self._schema,
                ),
                {"s": caller.scope_id, "i": request.request_id},
            )
            await self._audit(db, caller, request.grant.grant_id, "unknown", request.request_id)

    async def revoke(self, caller: ActorContext, grant: GrantRef) -> None:
        async with self._sessions.begin() as db:
            await self._run(db, caller, grant.run_id)
            row, scope = await self._grant(db, caller, grant)
            await self._authorize(db, caller, scope, None, "revoke")
            if row["revoked_at"] is not None:
                return
            await db.execute(
                scoped(
                    "UPDATE {t}capability_grant SET revoked_at=clock_timestamp(),"
                    "updated_at=clock_timestamp() WHERE scope_id=:s AND id=:i",
                    self._schema,
                ),
                {"s": caller.scope_id, "i": grant.grant_id},
            )
            await self._audit(db, caller, grant.grant_id, "revoked")

    async def bind_service(self, caller: ActorContext, grant: GrantRef, peer_sha256: str) -> None:
        """Trusted control plane only; no network endpoint exposes this operation.

        Binding is immutable and covered by grant revocation. Certificate rotation
        requires a newly authorized grant rather than overwriting service identity.
        """
        if len(peer_sha256) != 64 or any(c not in "0123456789abcdef" for c in peer_sha256):
            errors.forbidden("invalid service certificate fingerprint")
        async with self._sessions.begin() as db:
            run = await self._run(db, caller, grant.run_id)
            initial = await self._one(
                db,
                "SELECT allowed_scope FROM {t}capability_grant WHERE scope_id=:s AND id=:i",
                s=caller.scope_id,
                i=grant.grant_id,
            )
            if (
                initial is None
                or not isinstance(initial["allowed_scope"], dict)
                or initial["allowed_scope"].get("kind") != "credential_proxy"
            ):
                errors.forbidden("credential grant is unavailable")
            scope = ScopeRequest.model_validate(initial["allowed_scope"]["request"])
            now = await self._live(db, caller, scope, run, None, "issue")
            row, _ = await self._grant(db, caller, grant)
            if row["revoked_at"] is not None or row["expires_at"] <= now:
                errors.forbidden("credential grant is not live")
            binding = await self._one(
                db,
                "SELECT peer_sha256 FROM {t}credential_service_binding "
                "WHERE scope_id=:s AND grant_id=:g",
                s=caller.scope_id,
                g=grant.grant_id,
            )
            if binding is not None:
                if not hmac.compare_digest(binding["peer_sha256"], peer_sha256):
                    errors.forbidden("service binding cannot be replaced")
                return
            await db.execute(
                scoped(
                    "INSERT INTO {t}credential_service_binding (id,scope_id,grant_id,peer_sha256) "
                    "VALUES (:i,:s,:g,:p)",
                    self._schema,
                ),
                {
                    "i": uuid5(
                        NAMESPACE_URL, f"credential-service:{caller.scope_id}:{grant.grant_id}"
                    ),
                    "s": caller.scope_id,
                    "g": grant.grant_id,
                    "p": peer_sha256,
                },
            )

    async def resolve_service(self, peer_sha256: str, grant: GrantRef) -> ActorContext:
        """Fingerprint is provided by verified TLS, NOT request JSON or headers."""
        async with self._sessions.begin() as db:
            binding = await self._one(
                db,
                "SELECT b.peer_sha256,g.allowed_scope FROM {t}credential_service_binding b "
                "JOIN {t}capability_grant g ON g.scope_id=b.scope_id AND g.id=b.grant_id "
                "WHERE b.scope_id=:s AND b.grant_id=:g",
                s=grant.scope_id,
                g=grant.grant_id,
            )
            if binding is None or not hmac.compare_digest(binding["peer_sha256"], peer_sha256):
                errors.forbidden("service identity is not bound to this grant")
            scope = ScopeRequest.model_validate(binding["allowed_scope"]["request"])
            run = await self._run(db, scope.actor, scope.run_id)
            now = await self._live(db, scope.actor, scope, run, None, "inspect")
            row, stored = await self._grant(db, scope.actor, grant)
            if stored != scope or row["revoked_at"] is not None or row["expires_at"] <= now:
                errors.forbidden("service grant is no longer live")
            return scope.actor

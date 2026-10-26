"""Bounded credential mediation. Persistence and host authority are explicit ports."""

import asyncio
import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from agent_runtime.contracts.credentials import (
    EgressRegistry,
    FilteredResponse,
    GrantRef,
    ProxyRequest,
    ScopeRequest,
)
from agent_runtime.contracts.error_codes import ERROR_CODES
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.credential_validation import (
    resolve_destination,
    validate_filtered_response,
    validate_proxy_request,
)
from agent_runtime.credentials.ports import (
    AddressResolver,
    BrokerBlobs,
    CredentialVault,
    EgressTransport,
    GrantStore,
    ResponseFilter,
)


def utcnow() -> datetime:
    return datetime.now(UTC)


def _safe_dependency_failure(exc: Exception) -> AgentRuntimeError:
    if isinstance(exc, AgentRuntimeError):
        return runtime_failure(
            exc.failure.code if exc.failure.code in ERROR_CODES else "DEPENDENCY_UNAVAILABLE",
            category=exc.failure.category,
            public_message="Credential proxy request rejected or unavailable.",
        )
    return runtime_failure(
        "DEPENDENCY_UNAVAILABLE",
        category="delivery",
        public_message="Credential authority unavailable; verify its stored state before retrying.",
    )


@dataclass(frozen=True, slots=True)
class BrokerLimits:
    timeout_seconds: float = 30
    max_request_bytes: int = 1_048_576
    max_response_bytes: int = 4_194_304
    max_requests: int = 128
    max_grant_seconds: int = 900

    def __post_init__(self) -> None:
        if not 0 < self.timeout_seconds <= 300:
            raise ValueError("invalid broker timeout")
        if (
            min(
                self.max_request_bytes,
                self.max_response_bytes,
                self.max_requests,
                self.max_grant_seconds,
            )
            <= 0
        ):
            raise ValueError("broker limits must be positive")


class CredentialBroker:
    def __init__(
        self,
        *,
        store: GrantStore,
        vault: CredentialVault,
        blobs: BrokerBlobs,
        registry: EgressRegistry,
        resolver: AddressResolver,
        transport: EgressTransport,
        response_filter: ResponseFilter,
        limits: BrokerLimits | None = None,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self._store, self._vault, self._blobs = store, vault, blobs
        self._registry, self._resolver, self._transport = registry, resolver, transport
        self._filter, self._limits, self._clock = response_filter, limits or BrokerLimits(), clock

    async def authorize(self, caller: ActorContext, request: ScopeRequest) -> GrantRef:
        """caller must be supplied by authenticated host transport, never request JSON."""
        request = ScopeRequest.model_validate(request.model_dump())
        try:
            target = resolve_destination(self._registry, request.destination)
        except Exception as exc:
            raise _safe_dependency_failure(exc) from None
        now = self._clock()
        if caller != request.actor or request.actions != (target.action,):
            raise runtime_failure(
                "FORBIDDEN", category="authorization", public_message="Credential scope denied."
            )
        if (
            not 0 < (request.expires_at - now).total_seconds() <= self._limits.max_grant_seconds
            or request.max_requests > self._limits.max_requests
            or request.max_response_bytes > self._limits.max_response_bytes
        ):
            raise runtime_failure(
                "INVALID_INPUT", category="validation", public_message="Credential limits invalid."
            )
        try:
            async with asyncio.timeout(self._limits.timeout_seconds):
                return await self._store.issue(caller, request)
        except Exception as exc:
            raise _safe_dependency_failure(exc) from None

    async def revoke(self, caller: ActorContext, grant: GrantRef) -> None:
        try:
            async with asyncio.timeout(self._limits.timeout_seconds):
                await self._store.revoke(caller, grant)
        except Exception as exc:
            raise _safe_dependency_failure(exc) from None

    async def proxy(self, caller: ActorContext, request: ProxyRequest) -> FilteredResponse:
        request = ProxyRequest.model_validate(request.model_dump())
        # Mark uncertainty before entering reserve: cancellation may race its SQL commit.
        attempted_reservation = False
        owns_reservation = False
        try:
            async with asyncio.timeout(self._limits.timeout_seconds):
                state = await self._store.inspect(caller, request)
                if (
                    state.scope.max_response_bytes > self._limits.max_response_bytes
                    or state.scope.max_requests > self._limits.max_requests
                ):
                    raise ValueError("stored grant exceeds broker limits")
                target = resolve_destination(self._registry, state.scope.destination)
                payload = await self._blobs.read(
                    caller, request.request_payload_ref, self._limits.max_request_bytes
                )
                if len(payload) > self._limits.max_request_bytes:
                    raise runtime_failure(
                        "BUDGET_EXHAUSTED",
                        category="budget",
                        public_message="Credential request exceeds its byte limit.",
                    )
                addresses = await self._resolver.resolve(target.hostname, target.port)
                # Store enforces allowance on reservation, including replay. Pure validation
                # checks identity and binding before any response can be reused.
                approved = validate_proxy_request(
                    request,
                    authorized_scope=state.scope,
                    current_grant=state.grant,
                    caller=caller,
                    current_generation=state.generation,
                    registry=self._registry,
                    resolved_addresses=addresses,
                    payload=payload,
                    now=self._clock(),
                    revoked_at=state.revoked_at,
                    used_requests=0,
                )
                attempted_reservation = True
                cached = await self._store.reserve(caller, request)
                if cached is not None:
                    validate_filtered_response(
                        cached, request=request, authorized_scope=state.scope, now=self._clock()
                    )
                    return cached
                owns_reservation = True
                secret = await self._vault.bearer(caller.scope_id, target.destination)
                if (
                    not secret
                    or len(secret) > 8192
                    or any(ord(c) <= 32 or ord(c) >= 127 for c in secret)
                ):
                    raise ValueError("invalid Vault credential")
                await self._store.check_reserved(caller, request)
                expiry = min(state.scope.expires_at, state.grant.expires_at)
                remaining = min(
                    self._limits.timeout_seconds,
                    (expiry - self._clock()).total_seconds(),
                )
                if remaining <= 0:
                    raise TimeoutError("grant expired")
                async with asyncio.timeout(remaining):
                    reply = await self._transport.send(
                        target,
                        approved,
                        bearer=secret,
                        payload=payload,
                        max_bytes=state.scope.max_response_bytes,
                        timeout_seconds=remaining,
                    )
                if (
                    not 100 <= reply.status_code <= 599
                    or 300 <= reply.status_code <= 399
                    or len(reply.body) > state.scope.max_response_bytes
                    or secret.encode("ascii") in reply.body
                ):
                    raise ValueError("unsafe upstream response")
                filtered = await self._filter.apply(target, reply)
                if (
                    len(filtered.body) > state.scope.max_response_bytes
                    or secret.encode("ascii") in filtered.body
                    or (filtered.disposition == "blocked" and filtered.body)
                ):
                    raise ValueError("unsafe filtered response")
                del secret
                await self._store.check_reserved(caller, request)
                body_ref = await self._blobs.put_private(caller, request.request_id, filtered.body)
                response = FilteredResponse(
                    request_id=request.request_id,
                    grant_id=request.grant.grant_id,
                    status_code=reply.status_code,
                    body_ref=body_ref,
                    body_digest=hashlib.sha256(filtered.body).hexdigest(),
                    size_bytes=len(filtered.body),
                    filter_policy=filtered.policy,
                    audit_ref=f"credential-request:{request.request_id}",
                    disposition=filtered.disposition,
                )
                validate_filtered_response(
                    response, request=request, authorized_scope=state.scope, now=self._clock()
                )
                return await self._store.finish(caller, request, response)
        except BaseException as exc:
            # Unknown settlement is best effort. A durable reservation itself is already
            # non-replayable if SQL is unavailable; never release allowance on this path.
            if owns_reservation:
                try:
                    async with asyncio.timeout(2):
                        await self._store.unknown(caller, request)
                except BaseException:
                    pass
            if isinstance(exc, asyncio.CancelledError):
                raise
            if not isinstance(exc, Exception):
                raise
            if isinstance(exc, AgentRuntimeError) and not attempted_reservation:
                raise _safe_dependency_failure(exc) from None
            # Exception text and cause can contain Authorization/header/body material.
            raise runtime_failure(
                "SIDE_EFFECT_UNKNOWN" if attempted_reservation else "DEPENDENCY_UNAVAILABLE",
                category="tool" if attempted_reservation else "delivery",
                public_message="Credential proxy unavailable or response blocked; do not replay.",
            ) from None

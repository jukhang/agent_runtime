"""Pure checks on trusted broker state; no authorization issuance or network side effects.

DS-P01-04: the caller never supplies the egress target. ``validate_proxy_request``
resolves the destination from the trusted ``EgressRegistry`` by reference, validates
the DNS answer set through ``approved_addresses``, and re-checks scope, actor, grant,
generation, allowance, revocation and payload digest. Response size, identity and
deadline are checked by ``validate_filtered_response``.
"""

import hashlib
import ipaddress
from datetime import datetime, timedelta

from agent_runtime.contracts.credentials import (
    EgressDestination,
    EgressRegistry,
    FilteredResponse,
    GrantRef,
    ProxyRequest,
    ScopeRequest,
)
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.transactions import ActorContext

MAX_GRANT_CLOCK_SKEW = timedelta(seconds=5)


def resolve_destination(registry: EgressRegistry, ref: CapabilityRef) -> EgressDestination:
    """Resolve a fixed-revision destination from the trusted registry (fail closed)."""
    try:
        resolved = registry.resolve(ref)
    except LookupError as exc:
        raise runtime_failure(
            "FORBIDDEN",
            category="authorization",
            public_message="The egress destination is not registered.",
        ) from exc
    target = EgressDestination.model_validate(resolved.model_dump())
    if target.destination != ref:
        raise runtime_failure(
            "FORBIDDEN",
            category="authorization",
            public_message="The registry resolved a different destination revision.",
        )
    return target


# IPv6 forms that embed or translate an IPv4 destination. ``is_global`` is true for these
# networks, so treating them as ordinary peers would let a DNS answer smuggle a loopback,
# link-local or cloud-metadata IPv4 address past the public-address check (DS-P01-04).
# The contract approves literal unicast addresses only; translated or tunnelled forms are
# refused rather than unwrapped, because the adapter cannot know the host's translation
# policy and must connect to the validated address itself.
TRANSLATED_IPV6_NETWORKS = tuple(
    ipaddress.ip_network(cidr)
    for cidr in ("64:ff9b::/96", "64:ff9b:1::/48", "2002::/16", "2001::/32")
)


def approved_addresses(target: EgressDestination, addresses: tuple[str, ...]) -> tuple[str, ...]:
    target = EgressDestination.model_validate(target.model_dump())
    if not addresses or len(addresses) > 16:
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="DNS answer set is empty or exceeds the allowed size.",
        )
    result: list[str] = []
    for raw in addresses:
        if "%" in raw:
            raise runtime_failure(
                "INVALID_INPUT",
                category="validation",
                public_message="Scoped network addresses are not permitted.",
            )
        try:
            address = ipaddress.ip_address(raw)
        except ValueError as exc:
            raise runtime_failure(
                "INVALID_INPUT",
                category="validation",
                public_message="The DNS answer set contains a malformed address.",
            ) from exc
        if isinstance(address, ipaddress.IPv6Address):
            if address.ipv4_mapped is not None:
                address = address.ipv4_mapped
            elif any(address in network for network in TRANSLATED_IPV6_NETWORKS):
                raise runtime_failure(
                    "FORBIDDEN",
                    category="authorization",
                    public_message=(
                        "Translated or tunnelled IPv6 destinations are not approved directly."
                    ),
                )
        canonical = str(address)
        if address.is_link_local or address.is_multicast or address.is_unspecified:
            raise runtime_failure(
                "FORBIDDEN",
                category="authorization",
                public_message="The destination resolves to a forbidden network address.",
            )
        if not address.is_global and canonical not in target.private_addresses:
            raise runtime_failure(
                "FORBIDDEN",
                category="authorization",
                public_message=(
                    "The destination resolves to a private address that is not registered."
                ),
            )
        if canonical not in result:
            result.append(canonical)
    return tuple(result)


def validate_proxy_request(
    request: ProxyRequest,
    *,
    authorized_scope: ScopeRequest,
    current_grant: GrantRef,
    caller: ActorContext,
    current_generation: str | None,
    registry: EgressRegistry,
    resolved_addresses: tuple[str, ...],
    payload: bytes,
    now: datetime,
    revoked_at: datetime | None,
    used_requests: int,
) -> tuple[str, ...]:
    """Validate one proxy request; return the approved numeric addresses to connect to.

    The caller must connect to one of the returned addresses with TLS hostname
    verification against the resolved destination. Redirects are not part of this
    contract: the adapter must not forward credentials to a new origin.
    """
    request = ProxyRequest.model_validate(request.model_dump())
    scope = ScopeRequest.model_validate(authorized_scope.model_dump())
    grant = GrantRef.model_validate(current_grant.model_dump())
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("aware broker clock required")
    target = resolve_destination(registry, scope.destination)
    approved = approved_addresses(target, resolved_addresses)
    if caller != scope.actor or request.grant != grant:
        raise runtime_failure(
            "FORBIDDEN",
            category="authorization",
            public_message="The caller or current grant does not match this request.",
        )
    if (
        grant.scope_id != scope.actor.scope_id
        or grant.run_id != scope.run_id
        or grant.execution_id != scope.execution_id
        or grant.execution_generation != scope.execution_generation
        or grant.execution_generation != current_generation
    ):
        raise runtime_failure(
            "FENCED",
            category="concurrency",
            public_message="The credential grant belongs to another execution generation.",
        )
    if revoked_at is not None or not (
        grant.issued_at <= now + MAX_GRANT_CLOCK_SKEW
        and now < min(grant.expires_at, scope.expires_at)
    ):
        raise runtime_failure(
            "DELIVERY_EXPIRED",
            category="delivery",
            public_message="The credential grant has expired or been revoked.",
        )
    if used_requests < 0 or used_requests >= scope.max_requests:
        raise runtime_failure(
            "BUDGET_EXHAUSTED",
            category="budget",
            public_message="The credential request allowance is exhausted.",
        )
    if (
        target.destination != scope.destination
        or request.action != target.action
        or (request.action not in scope.actions)
    ):
        raise runtime_failure(
            "FORBIDDEN",
            category="authorization",
            public_message="The destination or action is outside the authorized scope.",
        )
    if hashlib.sha256(payload).hexdigest() != request.request_digest:
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="The request payload digest does not match the request.",
        )
    return approved


def validate_filtered_response(
    response: FilteredResponse,
    *,
    request: ProxyRequest,
    authorized_scope: ScopeRequest,
    now: datetime,
) -> None:
    """Check response identity, byte allowance and delivery deadline (DS-P01-04)."""
    response = FilteredResponse.model_validate(response.model_dump())
    request = ProxyRequest.model_validate(request.model_dump())
    scope = ScopeRequest.model_validate(authorized_scope.model_dump())
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("aware broker clock required")
    if response.request_id != request.request_id or response.grant_id != request.grant.grant_id:
        raise runtime_failure(
            "IDENTITY_MISMATCH",
            category="delivery",
            public_message="The filtered response belongs to another request or grant.",
        )
    if response.size_bytes > scope.max_response_bytes:
        raise runtime_failure(
            "BUDGET_EXHAUSTED",
            category="budget",
            public_message="The response exceeds the authorized byte allowance.",
        )
    if now >= scope.expires_at:
        raise runtime_failure(
            "DELIVERY_EXPIRED",
            category="delivery",
            public_message="The authorized response window has expired.",
        )

"""Trusted Broker ports. Implementations must not log request or secret contents."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

from agent_runtime.contracts.credentials import (
    EgressDestination,
    FilteredResponse,
    GrantRef,
    ProxyRequest,
    ScopeRequest,
)
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.transactions import ActorContext


@dataclass(frozen=True, slots=True)
class GrantState:
    scope: ScopeRequest
    grant: GrantRef
    generation: str | None
    revoked_at: datetime | None
    used_requests: int


class GrantStore(Protocol):
    """Durable authority, not an in-memory token cache.

    Each method must revalidate live host/Policy/Run/Execution authority. reserve
    atomically consumes allowance and binds requestId + full request digest;
    duplicates return only a previously sealed response, otherwise fail closed.
    check_reserved does not consume again; finish rechecks before publishing.
    A reservation never refunds itself on timeout or process death.
    """

    async def issue(self, caller: ActorContext, request: ScopeRequest) -> GrantRef: ...

    async def inspect(self, caller: ActorContext, request: ProxyRequest) -> GrantState: ...

    async def reserve(
        self, caller: ActorContext, request: ProxyRequest
    ) -> FilteredResponse | None: ...

    async def check_reserved(self, caller: ActorContext, request: ProxyRequest) -> None: ...

    async def finish(
        self, caller: ActorContext, request: ProxyRequest, response: FilteredResponse
    ) -> FilteredResponse: ...

    async def unknown(self, caller: ActorContext, request: ProxyRequest) -> None:
        """Retain consumed allowance and unknown effect without replacing a sealed result."""
        ...

    async def revoke(self, caller: ActorContext, grant: GrantRef) -> None: ...


class CredentialVault(Protocol):
    async def bearer(self, scope_id: str, destination: CapabilityRef) -> str:
        """Resolve a trusted fixed destination mapping, never a caller-selected secret path."""
        ...


class BrokerBlobs(Protocol):
    async def read(self, caller: ActorContext, ref: str, max_bytes: int) -> bytes:
        """Read only this caller's authorized immutable request Blob, bounded at source."""
        ...

    async def put_private(self, caller: ActorContext, request_id: UUID, data: bytes) -> str:
        """Persist scoped filtered bytes, never automatically expose them in events."""
        ...


class AddressResolver(Protocol):
    async def resolve(self, hostname: str, port: int) -> tuple[str, ...]: ...


@dataclass(frozen=True, slots=True)
class ProxyReply:
    status_code: int
    body: bytes = field(repr=False)


class EgressTransport(Protocol):
    async def send(
        self,
        target: EgressDestination,
        addresses: tuple[str, ...],
        *,
        bearer: str,
        payload: bytes,
        max_bytes: int,
        timeout_seconds: float,
    ) -> ProxyReply: ...


@dataclass(frozen=True, slots=True)
class FilteredBody:
    body: bytes = field(repr=False)
    policy: CapabilityRef
    disposition: Literal["allowed", "redacted", "blocked"]


class ResponseFilter(Protocol):
    async def apply(self, target: EgressDestination, reply: ProxyReply) -> FilteredBody:
        """Mandatory trusted inspection/DLP; a blocked result must contain no private bytes."""
        ...

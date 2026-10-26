"""Broker contracts never carry provider credentials or caller-selected URLs."""

import ipaddress
from typing import Annotated, Literal, Protocol, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.views import Digest


class MaintenanceCredentialLease(Contract):
    work_id: UUID
    owner_id: Identifier
    epoch: Revision
    lease_until: AwareDatetime


class ScopeRequest(Contract):
    actor: ActorContext
    session_id: UUID
    run_id: UUID
    execution_id: UUID | None = None
    execution_generation: Revision | None = None
    operation_id: UUID | None = None
    tool_call_id: UUID | None = None
    maintenance_lease: MaintenanceCredentialLease | None = None
    destination: CapabilityRef
    actions: Annotated[tuple[Identifier, ...], Field(min_length=1)]
    policy_decision_id: UUID
    input_digest: Digest
    expires_at: AwareDatetime
    max_requests: Annotated[int, Field(strict=True, ge=1)]
    max_response_bytes: Annotated[int, Field(strict=True, ge=1)]

    @model_validator(mode="after")
    def unique_actions(self) -> Self:
        if len(self.actions) != len(set(self.actions)):
            raise ValueError("duplicate credential action")
        if self.tool_call_id is not None:
            if (
                self.actions != ("read",)
                or self.maintenance_lease is not None
                or any(
                    value is not None
                    for value in (self.execution_id, self.execution_generation, self.operation_id)
                )
            ):
                raise ValueError("read credential must bind only a persisted tool call")
        elif any(
            value is None
            for value in (self.execution_id, self.execution_generation, self.operation_id)
        ):
            raise ValueError("effect credential requires an execution and operation")
        elif self.maintenance_lease is not None and self.actions not in {("read",), ("write",)}:
            raise ValueError("maintenance credential must bind one operation action")
        return self


class GrantRef(Contract):
    grant_id: UUID
    scope_id: Identifier
    run_id: UUID
    execution_id: UUID | None = None
    execution_generation: Revision | None = None
    revision: Revision
    issued_at: AwareDatetime
    expires_at: AwareDatetime

    @model_validator(mode="after")
    def positive_lifetime(self) -> Self:
        if self.expires_at <= self.issued_at:
            raise ValueError("grant lifetime must be positive")
        if (self.execution_id is None) != (self.execution_generation is None):
            raise ValueError("grant execution identity must be complete or absent")
        return self


class ProxyRequest(Contract):
    grant: GrantRef
    request_id: UUID
    action: Identifier
    request_payload_ref: Identifier
    request_digest: Digest


class FilteredResponse(Contract):
    request_id: UUID
    grant_id: UUID
    status_code: Annotated[int, Field(strict=True, ge=100, le=599)]
    body_ref: Identifier
    body_digest: Digest
    size_bytes: Annotated[int, Field(strict=True, ge=0)]
    filter_policy: CapabilityRef
    audit_ref: Identifier
    disposition: Literal["allowed", "redacted", "blocked"]


class EgressDestination(Contract):
    """Trusted registry configuration, never an endpoint supplied by the model."""

    destination: CapabilityRef
    hostname: str
    port: Annotated[int, Field(strict=True, ge=1, le=65535)] = 443
    action: Identifier
    method: Literal["GET", "POST"]
    path: str
    private_addresses: tuple[str, ...] = ()

    @model_validator(mode="after")
    def unambiguous_target(self) -> Self:
        host = self.hostname
        if (
            not host
            or len(host) > 253
            or host != host.lower()
            or any(not (c.isascii() and (c.isalnum() or c in "-.")) for c in host)
            or any(
                not label or len(label) > 63 or label.startswith("-") or label.endswith("-")
                for label in host.split(".")
            )
        ):
            raise ValueError("hostname must be a canonical ASCII DNS name")
        try:
            ipaddress.ip_address(host)
        except ValueError:
            pass
        else:
            raise ValueError("register a DNS identity, not an IP URL")
        if (
            not self.path.startswith("/")
            or self.path.startswith("//")
            or any(c in self.path for c in ("%", "\\", "?", "#", "\r", "\n"))
            or any(part in {".", ".."} for part in self.path.split("/"))
        ):
            raise ValueError("path must be a fixed unambiguous request path")
        if any(ord(c) <= 32 or ord(c) >= 127 for c in self.path):
            raise ValueError("path must be printable ASCII without whitespace")
        if len(set(self.private_addresses)) != len(self.private_addresses):
            raise ValueError("duplicate private address")
        for address in self.private_addresses:
            parsed = ipaddress.ip_address(address)
            if (
                str(parsed) != address
                or parsed.is_global
                or parsed.is_link_local
                or (parsed.is_multicast or parsed.is_unspecified)
            ):
                raise ValueError("invalid explicit private address")
        return self


class EgressRegistry(Protocol):
    """Trusted registry that resolves a fixed-revision destination reference.

    DS-P01-04: callers pass a reference, never host/port/path. The registry is a
    trusted component; an unknown reference or a revision mismatch fails closed.
    """

    def resolve(self, destination: CapabilityRef) -> EgressDestination: ...


# A serialized GrantRef is not a bearer token. Transport authentication, live
# revocation, destination resolution and response inspection belong to the broker.

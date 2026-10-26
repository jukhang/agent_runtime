from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.contracts.credentials import (
    EgressDestination,
    FilteredResponse,
    GrantRef,
    ProxyRequest,
    ScopeRequest,
)
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.credential_validation import (
    approved_addresses,
    resolve_destination,
    validate_filtered_response,
    validate_proxy_request,
)


def target(**changes):
    return EgressDestination.model_validate(
        {
            "destination": {"key": {"namespace": "builtin", "key": "reports"}, "revision": "1"},
            "hostname": "reports.example.com",
            "action": "read",
            "method": "GET",
            "path": "/report",
            **changes,
        }
    )


class Registry:
    """Trusted-registry test double; unknown references raise LookupError."""

    def __init__(self, *destinations: EgressDestination) -> None:
        self._destinations = {
            (item.destination.key, item.destination.revision): item for item in destinations
        }

    def resolve(self, destination: CapabilityRef) -> EgressDestination:
        try:
            return self._destinations[(destination.key, destination.revision)]
        except KeyError as exc:
            raise LookupError("unknown destination") from exc


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.1.2.3",
        "169.254.169.254",
        "::1",
        "::ffff:127.0.0.1",
        "fe80::1",
        "0.0.0.0",
        "224.0.0.1",
        "100.64.0.1",
        "fe80::1%en0",
        # Translated or tunnelled IPv6 forms embed an IPv4 destination and are not approved.
        "64:ff9b::a9fe:a9fe",
        "64:ff9b::7f00:1",
        "64:ff9b:1::a9fe:a9fe",
        "2002:a9fe:a9fe::",
        "2001::a9fe:a9fe",
    ],
)
def test_nonpublic_dns_answers_rejected_by_default(address):
    with pytest.raises(AgentRuntimeError):
        approved_addresses(target(), (address,))
    with pytest.raises(AgentRuntimeError):
        approved_addresses(target(), ("1.1.1.1", address))


def test_translated_ipv6_and_malformed_answers_keep_their_failure_code():
    for address in ("64:ff9b::7f00:1", "2002:7f00:1::", "2001::7f00:1"):
        with pytest.raises(AgentRuntimeError) as rejected:
            approved_addresses(target(), (address,))
        assert rejected.value.failure.code == "FORBIDDEN"
    with pytest.raises(AgentRuntimeError) as malformed:
        approved_addresses(target(), ("not-an-address",))
    assert malformed.value.failure.code == "INVALID_INPUT"
    # A destination cannot register a translated form as an explicit private address.
    with pytest.raises(ValidationError):
        target(private_addresses=["64:ff9b::a9fe:a9fe"])


def test_only_registered_private_address_is_allowed_and_empty_dns_rejected():
    endpoint = target(private_addresses=["10.1.2.3"])
    assert approved_addresses(endpoint, ("10.1.2.3",)) == ("10.1.2.3",)
    assert approved_addresses(target(), ("1.1.1.1", "1.1.1.1")) == ("1.1.1.1",)
    for addresses in ((), ("10.1.2.4",), ("1.1.1.1",) * 17):
        with pytest.raises(AgentRuntimeError):
            approved_addresses(endpoint, addresses)


@pytest.mark.parametrize(
    "changes",
    [
        {"hostname": "reports.example.com@localhost"},
        {"hostname": "reports.example.com:443"},
        {"hostname": "reports.example.com."},
        {"hostname": "127.0.0.1"},
        {"path": "//localhost"},
        {"path": "/a/../admin"},
        {"path": "/%2e%2e/admin"},
        {"path": "/report?url=http://localhost"},
        {"path": "/report\r\nx: a"},
        {"private_addresses": ["169.254.169.254"]},
        {"private_addresses": ["0.0.0.0"]},
    ],
)
def test_ambiguous_targets_and_metadata_exceptions_rejected(changes):
    with pytest.raises(ValidationError):
        target(**changes)


def authorized_request():
    now = datetime.now(UTC)
    actor = ActorContext(scope_id="scope", actor_id="actor", auth_revision="1", grants_ref="grants")
    endpoint = target()
    scope = ScopeRequest(
        actor=actor,
        session_id=uuid4(),
        run_id=uuid4(),
        execution_id=uuid4(),
        execution_generation="1",
        operation_id=uuid4(),
        destination=endpoint.destination,
        actions=("read",),
        policy_decision_id=uuid4(),
        input_digest="0" * 64,
        expires_at=now + timedelta(minutes=1),
        max_requests=2,
        max_response_bytes=1024,
    )
    grant = GrantRef(
        grant_id=uuid4(),
        scope_id="scope",
        run_id=scope.run_id,
        execution_id=scope.execution_id,
        execution_generation="1",
        revision="1",
        issued_at=now - timedelta(seconds=1),
        expires_at=scope.expires_at,
    )
    request = ProxyRequest(
        grant=grant,
        request_id=uuid4(),
        action="read",
        request_payload_ref="body",
        request_digest=sha256(b"{}").hexdigest(),
    )
    kwargs = dict(
        authorized_scope=scope,
        current_grant=grant,
        caller=actor,
        current_generation="1",
        registry=Registry(endpoint),
        resolved_addresses=("1.1.1.1",),
        payload=b"{}",
        now=now,
        revoked_at=None,
        used_requests=0,
    )
    return request, kwargs


def test_valid_request_and_stale_authority_rejected():
    request, kwargs = authorized_request()
    assert validate_proxy_request(request, **kwargs) == ("1.1.1.1",)
    for changes in (
        {"current_generation": "2"},
        {"revoked_at": kwargs["now"]},
        {"used_requests": 2},
        {"used_requests": -1},
        {"now": kwargs["current_grant"].expires_at},
        {"payload": b"changed"},
        # A registry that only knows another revision must not satisfy the requested ref.
        {
            "registry": Registry(
                target(
                    destination={
                        "key": {"namespace": "builtin", "key": "reports"},
                        "revision": "2",
                    }
                )
            )
        },
        {"caller": kwargs["caller"].model_copy(update={"scope_id": "other"})},
        {"caller": kwargs["caller"].model_copy(update={"auth_revision": "2"})},
        {"current_grant": kwargs["current_grant"].model_copy(update={"revision": "2"})},
    ):
        with pytest.raises(AgentRuntimeError):
            validate_proxy_request(request, **{**kwargs, **changes})


def test_registry_rejects_unknown_and_mismatched_destinations():
    with pytest.raises(AgentRuntimeError):
        resolve_destination(Registry(), target().destination)
    other = Registry(
        target(
            destination={
                "key": {"namespace": "builtin", "key": "other"},
                "revision": "1",
            }
        )
    )
    with pytest.raises(AgentRuntimeError):
        resolve_destination(other, target().destination)


def test_dns_answers_are_validated_inside_the_proxy_request():
    request, kwargs = authorized_request()
    for addresses in (("169.254.169.254",), ("10.0.0.5",), ("fe80::1",)):
        with pytest.raises(AgentRuntimeError):
            validate_proxy_request(request, **{**kwargs, "resolved_addresses": addresses})
    with pytest.raises(AgentRuntimeError):
        validate_proxy_request(request, **{**kwargs, "resolved_addresses": ()})


def test_registered_private_destination_is_allowed_when_declared():
    now = datetime.now(UTC)
    endpoint = target(private_addresses=["10.1.2.3"])
    request, kwargs = authorized_request()
    scope = kwargs["authorized_scope"].model_copy(update={"destination": endpoint.destination})
    grant = kwargs["current_grant"].model_copy(update={"expires_at": scope.expires_at})
    kwargs = {
        **kwargs,
        "authorized_scope": scope,
        "current_grant": grant,
        "registry": Registry(endpoint),
        "resolved_addresses": ("10.1.2.3",),
        "now": now,
    }
    request = request.model_copy(update={"grant": grant})
    assert validate_proxy_request(request, **kwargs) == ("10.1.2.3",)


def filtered_response(request, **changes):
    return FilteredResponse.model_validate(
        {
            "requestId": request.request_id,
            "grantId": request.grant.grant_id,
            "statusCode": 200,
            "bodyRef": "body",
            "bodyDigest": "0" * 64,
            "sizeBytes": 16,
            "filterPolicy": {"key": {"namespace": "builtin", "key": "redact"}, "revision": "1"},
            "auditRef": "audit",
            "disposition": "allowed",
            **changes,
        }
    )


def test_filtered_response_checks_identity_size_and_deadline():
    request, kwargs = authorized_request()
    scope = kwargs["authorized_scope"]
    now = kwargs["now"]
    validate_filtered_response(
        filtered_response(request), request=request, authorized_scope=scope, now=now
    )
    for response in (
        filtered_response(request, requestId=uuid4()),
        filtered_response(request, grantId=uuid4()),
    ):
        with pytest.raises(AgentRuntimeError):
            validate_filtered_response(response, request=request, authorized_scope=scope, now=now)
    with pytest.raises(AgentRuntimeError):
        validate_filtered_response(
            filtered_response(request, sizeBytes=scope.max_response_bytes + 1),
            request=request,
            authorized_scope=scope,
            now=now,
        )
    with pytest.raises(AgentRuntimeError):
        validate_filtered_response(
            filtered_response(request),
            request=request,
            authorized_scope=scope,
            now=scope.expires_at,
        )


def test_stale_authority_failures_keep_their_specific_codes():
    request, kwargs = authorized_request()
    cases = {
        "FENCED": {"current_generation": "2"},
        "DELIVERY_EXPIRED": {"revoked_at": kwargs["now"]},
        "BUDGET_EXHAUSTED": {"used_requests": 2},
        "INVALID_INPUT": {"payload": b"changed"},
        "FORBIDDEN": {"caller": kwargs["caller"].model_copy(update={"scope_id": "other"})},
    }
    for code, changes in cases.items():
        with pytest.raises(AgentRuntimeError) as exc:
            validate_proxy_request(request, **{**kwargs, **changes})
        assert exc.value.failure.code == code


def test_filtered_response_failures_keep_their_specific_codes():
    request, kwargs = authorized_request()
    scope, now = kwargs["authorized_scope"], kwargs["now"]
    cases = {
        "IDENTITY_MISMATCH": filtered_response(request, requestId=uuid4()),
        "BUDGET_EXHAUSTED": filtered_response(request, sizeBytes=scope.max_response_bytes + 1),
    }
    for code, response in cases.items():
        with pytest.raises(AgentRuntimeError) as exc:
            validate_filtered_response(response, request=request, authorized_scope=scope, now=now)
        assert exc.value.failure.code == code
    with pytest.raises(AgentRuntimeError) as expired:
        validate_filtered_response(
            filtered_response(request),
            request=request,
            authorized_scope=scope,
            now=scope.expires_at,
        )
    assert expired.value.failure.code == "DELIVERY_EXPIRED"

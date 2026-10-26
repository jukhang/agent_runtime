"""DS-P02-00 frozen data, port, error and migration semantics (post-review P1/P2)."""

import json
from importlib.resources import files
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.contracts.audit import AuditEntry, AuditQueryRequest, AuditQueryResult
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.capacity import (
    CapacityKey,
    CapacityObserveRequest,
    CapacityReleaseRejected,
    CapacityReleaseRequest,
    CapacityReservation,
    CapacityReserveRequest,
    CapacityView,
    capacity_release_adapter,
    capacity_unavailable,
)
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.migration import (
    MigrationDeferred,
    MigrationHeads,
    MigrationPlanRequest,
    MigrationRejected,
    MigrationStep,
    MigrationStepRef,
    MigrationSucceeded,
    migration_adapter,
)
from agent_runtime.contracts.references import (
    CAPABILITY_KINDS,
    CapabilityKey,
    CapabilityRef,
)
from agent_runtime.contracts.registry import (
    CapabilityAvailability,
    CapabilityManifest,
    capability_spec_adapter,
)
from agent_runtime.contracts.results import Usage
from agent_runtime.contracts.tools import ToolSpec
from agent_runtime.storage_spec import RESOURCE_KINDS, TABLES, USAGE_REASONS

NOW = "2026-09-21T12:00:00Z"
LATER = "2026-09-21T13:00:00Z"
ACTOR = {"scopeId": "scope", "actorId": "actor", "authRevision": "1", "grantsRef": "grants"}
PLAN_ID = "00000000-0000-4000-8000-0000000000d1"
P02_FIXTURE = json.loads(
    files("agent_runtime.contracts").joinpath("fixtures/p02.json").read_text()
)


def table(name: str):
    return next(item for item in TABLES if item.name == name)


def check_for(item, columns: tuple[str, ...]) -> str:
    matches = [check.expression for check in item.checks if check.columns == columns]
    assert matches, f"{item.name}: no check on {columns}"
    return " AND ".join(matches)


def capability_ref(key: str = "tool", revision: str = "1") -> CapabilityRef:
    return CapabilityRef(key=CapabilityKey(namespace="builtin", key=key), revision=revision)


# --- Usage reason matrix ---------------------------------------------------------------


def usage(status: str, reason: str):
    counts = {
        "complete": ("10", "5", "15"),
        "partial": ("10", None, None),
        "unknown": (None, None, None),
    }[status]
    return Usage.model_validate(
        {
            "status": status,
            "reason": reason,
            "inputTokens": counts[0],
            "outputTokens": counts[1],
            "totalTokens": counts[2],
        }
    )


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        ("complete", "provider_reported"),
        ("partial", "provider_reported"),
        ("partial", "estimated"),
        ("unknown", "provider_not_reported"),
        ("unknown", "collection_failed"),
        ("unknown", "estimated"),
    ],
)
def test_legal_usage_reason_combinations(status, reason):
    assert usage(status, reason).reason == reason


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        ("complete", "estimated"),
        ("complete", "provider_not_reported"),
        ("partial", "collection_failed"),
        ("unknown", "provider_reported"),
    ],
)
def test_illegal_usage_reason_combinations_are_rejected(status, reason):
    with pytest.raises(ValidationError):
        usage(status, reason)


def test_unknown_usage_cannot_carry_provider_counts():
    with pytest.raises(ValidationError):
        Usage.model_validate(
            {
                "status": "unknown",
                "reason": "provider_not_reported",
                "inputTokens": "10",
                "outputTokens": "5",
                "totalTokens": "15",
            }
        )


# --- Capability identity (P1-2) --------------------------------------------------------


def test_capability_key_is_namespaced_and_machine_keyed():
    key = CapabilityKey(namespace="vendor-a", key="search")
    assert (key.namespace, key.key) == ("vendor-a", "search")
    assert capability_ref("search").key != CapabilityKey(namespace="vendor-b", key="search")
    for bad in ("café", "with space", "with\nnewline"):
        with pytest.raises(ValidationError):
            CapabilityKey(namespace="builtin", key=bad)


def test_capability_kinds_are_semantic_not_transport():
    assert set(CAPABILITY_KINDS) == {
        "model",
        "tool",
        "skill",
        "prompt",
        "hook",
        "knowledge",
        "agent",
    }
    assert "mcp" not in CAPABILITY_KINDS and "a2a" not in CAPABILITY_KINDS
    assert "code" not in CAPABILITY_KINDS


def tool_spec_payload() -> dict:
    spec = ToolSpec(
        ref=capability_ref("search"),
        description="Search the web",
        source=capability_ref("builtin"),
        kind="read",
        side_effect="none",
        input_schema=capability_ref("schema-in"),
        output_schema=capability_ref("schema-out"),
        permissions=(),
        approval_policy=capability_ref("auto"),
        idempotency="read_only",
        timeout_seconds=30,
        concurrency_group="search",
        max_concurrency=4,
        cancellation="stop",
        public_result_policy="summary",
    )
    return spec.model_dump(mode="json", by_alias=True)


def manifest_payload(**changes):
    spec = tool_spec_payload()
    payload = {
        "ref": spec["ref"],
        "status": "active",
        "contentDigest": content_digest(spec),
        "spec": {"kind": "tool", "spec": spec},
        **changes,
    }
    return payload


def test_capability_manifest_binds_kind_spec_and_digest():
    manifest = CapabilityManifest.model_validate(manifest_payload())
    assert manifest.spec.kind == "tool"
    assert manifest.ref.key.key == "search"
    for patch in ({"contentDigest": "a" * 64}, {"status": "unknown"}):
        with pytest.raises(ValidationError):
            CapabilityManifest.model_validate(manifest_payload(**patch))
    with pytest.raises(ValidationError):
        capability_spec_adapter.validate_python({"kind": "code", "spec": {}})


def test_capability_availability_keeps_one_active_revision():
    key = CapabilityKey(namespace="builtin", key="search")
    CapabilityAvailability(key=key, active_revision="2", revoked_revisions=("1",))
    with pytest.raises(ValidationError):
        CapabilityAvailability(key=key, active_revision="2", revoked_revisions=("2",))


# --- Capacity (P1-1) -------------------------------------------------------------------


def capacity_key(**changes):
    return CapacityKey.model_validate(
        {
            "deploymentId": "deployment-1",
            "resourceKind": "model_slot",
            "resourceVersion": "1",
            "resourceKey": "supplier-a",
            **changes,
        }
    )


def reserve_request(**changes):
    return CapacityReserveRequest.model_validate(
        {
            "key": capacity_key().model_dump(by_alias=True),
            "amount": 2,
            "holderRef": "run:1",
            "requestId": "req-1",
            "leaseUntil": LATER,
            "deadlineAt": LATER,
            **changes,
        }
    )


def test_capacity_key_separates_resource_identity():
    assert capacity_key().resource_key == "supplier-a"
    assert "run_slot" in RESOURCE_KINDS
    assert capacity_key(resourceKey="supplier-b") != capacity_key()


def test_capacity_key_rejects_an_unknown_resource_kind():
    with pytest.raises(ValidationError):
        capacity_key(resourceKind="not_a_slot")


@pytest.mark.parametrize("amount", [0, -1, 1025, True, 1.5])
def test_capacity_reserve_amount_is_bounded(amount):
    with pytest.raises(ValidationError):
        reserve_request(amount=amount)


def test_reservation_lease_cannot_outlive_its_deadline():
    with pytest.raises(ValidationError):
        reserve_request(leaseUntil="2026-09-21T14:00:00Z", deadlineAt=LATER)
    assert reserve_request().holder_ref == "run:1"


def test_release_result_is_a_discriminated_union():
    applied = capacity_release_adapter.validate_python(
        {
            "outcome": "applied",
            "reservationId": str(uuid4()),
            "status": "released",
            "version": "6",
            "releasedAt": NOW,
        }
    )
    reused = capacity_release_adapter.validate_python(
        {
            "outcome": "reused",
            "reservationId": str(uuid4()),
            "status": "expired",
            "version": "6",
            "releasedAt": NOW,
        }
    )
    rejected = CapacityReleaseRejected.model_validate(
        {
            "outcome": "rejected",
            "reservationId": str(uuid4()),
            "failure": {
                "code": "VERSION_CONFLICT",
                "category": "concurrency",
                "retryable": False,
                "publicMessage": "The reservation was already released.",
            },
        }
    )
    assert (applied.outcome, reused.outcome, rejected.outcome) == ("applied", "reused", "rejected")


def test_reclamation_requires_a_confirmed_holder():
    base = {
        "key": capacity_key().model_dump(by_alias=True),
        "reservationId": str(uuid4()),
        "requestId": "release-1",
        "reason": "reclaimed",
    }
    for holder_state in ("stopped", "terminal"):
        assert (
            CapacityReleaseRequest.model_validate({**base, "holderState": holder_state}).reason
            == "reclaimed"
        )
    with pytest.raises(ValidationError):
        CapacityReleaseRequest.model_validate({**base, "holderState": "unknown"})


def test_returned_reservation_must_still_be_held():
    reservation = CapacityReservation.model_validate(
        {
            "key": capacity_key().model_dump(by_alias=True),
            "reservationId": str(uuid4()),
            "amount": 1,
            "status": "reserved",
            "version": "1",
            "leaseUntil": LATER,
            "holderRef": "run:1",
        }
    )
    assert reservation.amount == 1
    with pytest.raises(ValidationError):
        CapacityReservation.model_validate(
            {**reservation.model_dump(by_alias=True), "status": "released"}
        )


def test_capacity_view_rejects_reserved_above_capacity():
    CapacityView(key=capacity_key(), capacity="8", reserved="8", version="3")
    with pytest.raises(ValidationError):
        CapacityView(key=capacity_key(), capacity="8", reserved="9", version="3")


def test_capacity_failure_is_public_retryable_and_leak_free():
    key = capacity_key()
    with pytest.raises(AgentRuntimeError) as exc:
        capacity_unavailable(key, requested=4, retry_after_seconds=15)
    failure = exc.value.failure
    assert failure.code == "CAPACITY_UNAVAILABLE"
    assert failure.category == "budget"
    assert failure.retryable is True
    assert [option.action for option in failure.recovery_options] == ["retry_same"]
    assert key.resource_kind in failure.public_message
    assert key.resource_key not in failure.public_message
    assert "supplier-a" not in failure.public_message
    assert "8" not in failure.public_message


def test_capacity_failure_without_backoff_still_offers_retry():
    with pytest.raises(AgentRuntimeError) as exc:
        capacity_unavailable(capacity_key(), requested=1)
    assert exc.value.failure.recovery_options[0].label == "Retry when capacity is available"


# --- Audit -----------------------------------------------------------------------------


def audit_request(**changes):
    return AuditQueryRequest.model_validate(
        {
            "actor": ACTOR,
            "purpose": "security",
            "sessionId": str(uuid4()),
            "kinds": ["model.prepared"],
            "deadlineAt": NOW,
            **changes,
        }
    )


def test_audit_requires_explicit_authorization_context():
    payload = audit_request().model_dump(by_alias=True)
    payload.pop("actor")
    with pytest.raises(ValidationError):
        AuditQueryRequest.model_validate(payload)


def test_audit_requires_a_target_and_unique_kinds():
    with pytest.raises(ValidationError):
        audit_request(sessionId=None)
    with pytest.raises(ValidationError):
        audit_request(kinds=["run.status", "run.status"])
    assert audit_request(runId=str(uuid4())).after_seq == "0"


def test_audit_result_is_restricted_ordered_and_cursor_advances():
    entry = {
        "sessionSeq": "3",
        "kind": "model.prepared",
        "visibility": "restricted",
        "recordedAt": NOW,
        "payloadRef": "payload-3",
        "contentDigest": "a" * 64,
    }
    result = AuditQueryResult.model_validate(
        {"scopeId": "scope", "sessionId": str(uuid4()), "entries": [entry], "nextSeq": "4"}
    )
    assert result.restricted is True
    with pytest.raises(ValidationError):
        AuditQueryResult.model_validate(
            {
                "scopeId": "scope",
                "sessionId": str(uuid4()),
                "entries": [entry],
                "nextSeq": "3",
                "restricted": False,
            }
        )
    with pytest.raises(ValidationError):
        AuditQueryResult.model_validate(
            {
                "scopeId": "scope",
                "sessionId": str(uuid4()),
                "entries": [{**entry, "sessionSeq": "5"}, entry],
                "nextSeq": "6",
            }
        )
    assert AuditEntry.model_validate(entry).visibility == "restricted"


# --- Migration orchestration (P1-3) ----------------------------------------------------


def step(chain: str, key: str, *, frm: str | None = None, to: str | None = None):
    return {
        "chain": chain,
        "kind": "upgrade",
        "fromRevision": frm or f"{chain}-r1",
        "toRevision": to or f"{chain}-r2",
        "idempotencyKey": key,
        "deadlineAt": NOW,
    }


def plan(*steps, heads=None):
    return {
        "planId": PLAN_ID,
        "planRevision": "1",
        "deploymentId": "deployment-1",
        "actor": ACTOR,
        "heads": heads or {"hostRevision": "host-r1", "runtimeRevision": "runtime-r1"},
        "steps": list(steps),
        "idempotencyKey": "plan-1",
        "deadlineAt": NOW,
    }


def test_migration_step_must_move_the_chain_and_cannot_downgrade():
    with pytest.raises(ValidationError):
        MigrationStep.model_validate(step("runtime", "k1", to="runtime-r1"))
    with pytest.raises(ValidationError):
        MigrationStep.model_validate({**step("runtime", "k1"), "kind": "downgrade"})
    # ``kind`` is part of the step identity (review C1): omitting it is not an implicit
    # upgrade, and the published JSON Schema must list it as required.
    with pytest.raises(ValidationError):
        MigrationStep.model_validate(
            {key: value for key, value in step("runtime", "k1").items() if key != "kind"}
        )
    schema = json.loads(
        (files("agent_runtime.contracts.schemas") / "migration-step.schema.json").read_text()
    )
    assert "kind" in schema["required"]
    assert {field.alias for field in MigrationStep.model_fields.values()} == set(schema["required"])


def test_migration_runtime_steps_follow_host_steps_and_stay_continuous():
    MigrationPlanRequest.model_validate(
        plan(
            step("host", "k1", frm="host-r1", to="host-r2"),
            step("runtime", "k2", frm="runtime-r1", to="runtime-r2"),
        )
    )
    with pytest.raises(ValidationError):
        MigrationPlanRequest.model_validate(plan(step("runtime", "k2"), step("host", "k1")))
    with pytest.raises(ValidationError):
        MigrationPlanRequest.model_validate(plan(step("host", "k1", frm="host-r9")))
    with pytest.raises(ValidationError):
        MigrationPlanRequest.model_validate(
            plan(step("host", "k1", frm="host-r1", to="host-r2"), step("host", "k2", frm="host-r5"))
        )
    with pytest.raises(ValidationError):
        MigrationPlanRequest.model_validate(plan(step("host", "k1"), step("runtime", "k1")))
    assert MigrationHeads(hostRevision="host-r1", runtimeRevision="runtime-r1").host_revision


def test_migration_results_echo_plan_identity_and_structured_steps():
    completed = MigrationStepRef(
        chain="host", from_revision="host-r1", to_revision="host-r2", idempotency_key="k1"
    )
    base = {
        "disposition": "deferred",
        "planId": PLAN_ID,
        "planDigest": "d" * 64,
        "deploymentId": "deployment-1",
        "reason": "runtime_failed",
        "completed": [completed.model_dump(by_alias=True)],
    }
    MigrationDeferred.model_validate(base)
    for patch in (
        {"completed": []},
        {
            "reason": "host_failed",
            "completed": [completed.model_dump(by_alias=True)],
        },
        {
            "completed": [
                completed.model_dump(by_alias=True),
                MigrationStepRef(
                    chain="host",
                    from_revision="host-r9",
                    to_revision="host-r10",
                    idempotency_key="k2",
                ).model_dump(by_alias=True),
            ]
        },
    ):
        with pytest.raises(ValidationError):
            MigrationDeferred.model_validate({**base, **patch})
    succeeded = MigrationSucceeded.model_validate(
        {
            "disposition": "succeeded",
            "planId": PLAN_ID,
            "planDigest": "d" * 64,
            "deploymentId": "deployment-1",
            "hostRevision": "host-r2",
            "runtimeRevision": None,
            "applied": [completed.model_dump(by_alias=True)],
        }
    )
    assert succeeded.plan_id == UUID(PLAN_ID)
    rejected = MigrationRejected.model_validate(
        {
            "disposition": "rejected",
            "planId": PLAN_ID,
            "deploymentId": "deployment-1",
            "failure": {
                "code": "VERSION_CONFLICT",
                "category": "concurrency",
                "retryable": False,
                "publicMessage": "The plan does not continue the observed head.",
            },
        }
    )
    assert rejected.disposition == "rejected"
    with pytest.raises(ValidationError):
        migration_adapter.validate_python(
            {"disposition": "rejected", "planId": PLAN_ID, "deploymentId": "deployment-1"}
        )


# --- Frozen storage details -----------------------------------------------------------


def test_system_capacity_and_reservations_are_scope_less_and_cas_ready():
    capacity = table("system_capacity")
    columns = {column.name: column for column in capacity.columns}
    assert "scope_id" not in columns
    assert capacity.primary_key == (
        "deployment_id",
        "resource_kind",
        "resource_version",
        "resource_key",
    )
    assert "reserved <= capacity" in check_for(capacity, ("reserved", "capacity"))
    kinds = check_for(capacity, ("resource_kind",))
    assert all(kind in kinds for kind in RESOURCE_KINDS)

    reservation = table("system_capacity_reservation")
    reserved_columns = {column.name: column for column in reservation.columns}
    assert "scope_id" not in reserved_columns
    for name in ("holder_ref", "request_id", "amount", "status", "lease_until", "released_at"):
        assert name in reserved_columns
    assert "amount > 0" in check_for(reservation, ("amount",))
    assert "(status = 'reserved') = (released_at IS NULL)" in check_for(
        reservation, ("status", "released_at")
    )
    unique = {index.columns for index in reservation.indexes if index.unique}
    assert (
        "deployment_id",
        "resource_kind",
        "resource_version",
        "resource_key",
        "request_id",
    ) in unique
    assert ("status", "lease_until") in {index.columns for index in reservation.indexes}


def test_capability_identity_uses_namespace_and_machine_key():
    item = table("capability")
    unique = {index.columns for index in item.indexes if index.unique}
    assert ("scope_id", "namespace", "key") in unique
    kinds = check_for(item, ("kind",))
    for kind in CAPABILITY_KINDS:
        assert f"'{kind}'" in kinds
    assert "'mcp'" not in kinds and "'a2a'" not in kinds


def test_capability_revision_has_one_active_pointer_and_digest():
    item = table("capability_revision")
    unique = {index.columns for index in item.indexes if index.unique}
    assert ("scope_id", "capability_id", "revision") in unique
    active = [index for index in item.indexes if index.unique and index.predicate]
    assert [(index.columns, index.predicate) for index in active] == [
        (("scope_id", "capability_id"), "status = 'active'")
    ]
    assert "length(content_digest) = 64" in check_for(item, ("status", "content_digest"))
    assert item.foreign_keys[0].target == "capability"


def test_model_attempt_usage_reason_matrix_is_frozen():
    item = table("model_attempt")
    columns = {column.name: column for column in item.columns}
    assert columns["usage_reason"].nullable is False
    reasons = check_for(item, ("usage_reason",))
    assert all(reason in reasons for reason in USAGE_REASONS)
    matrix = check_for(item, ("usage_status", "usage_reason"))
    for fragment in ("'pending'", "'complete'", "'partial'", "'unknown'"):
        assert fragment in matrix


def test_budget_entry_reason_is_required_for_settlement_rows():
    item = table("budget_entry")
    columns = {column.name: column for column in item.columns}
    assert columns["usage_reason"].nullable is True
    expression = check_for(item, ("kind", "usage_status", "usage_reason"))
    assert "((kind = 'settle') = (usage_reason IS NOT NULL))" in expression


def test_domain_job_handler_identity_is_checked_and_indexed():
    item = table("work_item")
    columns = {column.name: column for column in item.columns}
    for name in ("handler_namespace", "handler_key", "handler_revision"):
        assert columns[name].nullable is True
    partial = [index for index in item.indexes if index.predicate == "kind = 'domain_job'"]
    assert partial and partial[0].columns == (
        "scope_id",
        "handler_namespace",
        "handler_key",
        "handler_revision",
        "status",
    )
    expression = check_for(item, ("kind", "handler_namespace", "handler_key", "handler_revision"))
    assert "(kind = 'domain_job') = (handler_namespace IS NOT NULL " in expression


def test_scope_less_tables_cannot_declare_scope_foreign_keys():
    from agent_runtime.storage_spec import Column, validate_storage
    from agent_runtime.storage_spec import table as build_table

    invalid = build_table(
        "invalid_scope_less",
        Column("deployment_id", "TEXT", comment="Deployment"),
        scoped=False,
        primary=("deployment_id",),
        refs=(("run_id", "run"),),
    )
    # A scope-less table may only reference another aggregate through an explicit
    # composite key; a scope-style ref needs a ``scope_id`` column it does not have.
    with pytest.raises(ValueError):
        validate_storage((*TABLES, invalid))
    assert CapacityObserveRequest(key=capacity_key()).key.resource_key == "supplier-a"


# --- Shared fixtures -------------------------------------------------------------------


def test_p02_fixtures_cover_the_frozen_wire_shapes():
    capacity_release_adapter.validate_python(P02_FIXTURE["capacityReleases"][0])
    CapacityView.model_validate(P02_FIXTURE["capacityViews"][0])
    CapacityReservation.model_validate(P02_FIXTURE["capacityReservations"][0])
    AuditQueryRequest.model_validate(P02_FIXTURE["auditQueryRequests"][0])
    AuditQueryResult.model_validate(P02_FIXTURE["auditQueryResults"][0])
    failure = P02_FIXTURE["capacityFailures"][0]
    assert failure["code"] == "CAPACITY_UNAVAILABLE"
    assert failure["recoveryOptions"][0]["action"] == "retry_same"

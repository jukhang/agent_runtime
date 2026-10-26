"""Every exported wire union has a positive fixture per kind (DS-P01-03 / DS-P01-07).

The shared fixtures are the contract examples hosts and the TypeScript client consume.
A union whose kinds are not all present in a fixture, whose fixture value is invalid, or
that is silently unclassified, fails here.
"""

import json
from importlib.resources import files
from typing import Any

import pytest

from agent_runtime.contracts.capacity import capacity_release_adapter
from agent_runtime.contracts.decisions import decision_adapter
from agent_runtime.contracts.events import event_adapter
from agent_runtime.contracts.messages import message_adapter
from agent_runtime.contracts.migration import migration_adapter
from agent_runtime.contracts.ownership import recovery_adapter
from agent_runtime.contracts.policy import policy_adapter
from agent_runtime.contracts.provider import provider_event_adapter
from agent_runtime.contracts.results import result_adapter
from agent_runtime.contracts.scheduler import (
    claim_adapter,
    renew_adapter,
    wake_adapter,
    yield_adapter,
)
from agent_runtime.contracts.schema import SCHEMA_REGISTRY
from agent_runtime.contracts.tools import tool_outcome_adapter
from agent_runtime.contracts.transitions import control_outcome_adapter
from agent_runtime.contracts.work_claims import (
    child_renew_adapter,
    maintenance_renew_adapter,
    work_claim_adapter,
)

FIXTURES = files("agent_runtime.contracts").joinpath("fixtures")


def _fixture(name: str) -> dict[str, Any]:
    return json.loads(FIXTURES.joinpath(name).read_text(encoding="utf-8"))


# Union name -> (adapter, discriminator property, fixture loader).
COVERED: dict[str, tuple[Any, str, Any]] = {
    "events": (event_adapter, "kind", lambda: _fixture("events.json")["events"]),
    "agent-message": (message_adapter, "kind", lambda: _fixture("internal.json")["messages"]),
    "next-action": (decision_adapter, "kind", lambda: _fixture("internal.json")["actions"]),
    "tool-outcome": (
        tool_outcome_adapter,
        "status",
        lambda: _fixture("tools.json")["outcomes"],
    ),
    "provider-event": (
        provider_event_adapter,
        "kind",
        lambda: _fixture("provider-events.json")["events"],
    ),
    "run-result": (result_adapter, "status", lambda: _fixture("results.json")["results"]),
    "claim-result": (
        claim_adapter,
        "disposition",
        lambda: _fixture("scheduler.json")["claimResults"],
    ),
    "renew-result": (
        renew_adapter,
        "disposition",
        lambda: _fixture("scheduler.json")["renewResults"],
    ),
    "yield-result": (
        yield_adapter,
        "disposition",
        lambda: _fixture("scheduler.json")["yieldResults"],
    ),
    "wake-result": (wake_adapter, "disposition", lambda: _fixture("scheduler.json")["wakeResults"]),
    "work-claim-result": (
        work_claim_adapter,
        "disposition",
        lambda: _fixture("scheduler.json")["workClaimResults"],
    ),
    "child-renew-result": (
        child_renew_adapter,
        "disposition",
        lambda: _fixture("scheduler.json")["childRenewResults"],
    ),
    "maintenance-renew-result": (
        maintenance_renew_adapter,
        "disposition",
        lambda: _fixture("scheduler.json")["maintenanceRenewResults"],
    ),
    "policy-decision": (
        policy_adapter,
        "decision",
        lambda: _fixture("policy-recovery.json")["policyDecisions"],
    ),
    "recovery-disposition": (
        recovery_adapter,
        "disposition",
        lambda: _fixture("policy-recovery.json")["recoveryDispositions"],
    ),
    "control-outcome": (
        control_outcome_adapter,
        "outcome",
        lambda: _fixture("control.json")["outcomes"],
    ),
    "migration-outcome": (
        migration_adapter,
        "disposition",
        lambda: _fixture("migration.json")["outcomes"],
    ),
    "capacity-release-result": (
        capacity_release_adapter,
        "outcome",
        lambda: _fixture("p02.json")["capacityReleases"],
    ),
}

# Exported unions whose positive examples live in test constructors instead of a shared
# fixture. Keep this empty unless a gap is deliberate and explained.
BACKLOG: dict[str, str] = {}

# state-event-draft kinds are derived from the event fixture in test_state_event_drafts.py.
DERIVED = {"state-event-draft"}


def _adapter_names() -> set[str]:
    return {
        name for name, entry in SCHEMA_REGISTRY.items() if entry.__class__.__name__ == "TypeAdapter"
    }


def _discriminators(adapter: Any) -> set[str]:
    return set(adapter.json_schema()["discriminator"]["mapping"])


def test_every_exported_union_is_classified():
    adapter_names = _adapter_names()
    classified = set(COVERED) | set(BACKLOG) | DERIVED
    unclassified = sorted(adapter_names - classified)
    assert unclassified == [], f"unclassified: {unclassified}"
    assert classified - adapter_names == set(), f"unknown: {sorted(classified - adapter_names)}"
    assert all(reason.strip() for reason in BACKLOG.values())


@pytest.mark.parametrize("union", sorted(COVERED))
def test_covered_union_has_a_valid_positive_fixture_for_every_kind(union: str):
    adapter, key, load = COVERED[union]
    items = load()
    assert items, union
    mapping = _discriminators(adapter)
    found: set[str] = set()
    for item in items:
        adapter.validate_python(item)
        value = item.get(key)
        assert value in mapping, f"{union}: fixture value {value!r} is not a known {key}"
        found.add(value)
    assert found == mapping, f"{union}: missing {sorted(mapping - found)}"

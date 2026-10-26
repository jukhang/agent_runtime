"""Keep declared interface coverage honest and catch dangling package symbols."""

import importlib
import json
from importlib.resources import files

from pydantic import TypeAdapter

from agent_runtime.contracts.events import event_adapter

CATALOG = json.loads(files("agent_runtime.contracts").joinpath("catalog.json").read_text())


def test_catalog_covers_design_interfaces_and_transactions():
    names = [row["name"] for row in CATALOG["interfaces"]]
    assert len(names) == len(set(names))
    assert {name.split(".")[0] for name in names} == {
        "Admission", "Scheduler", "AgentLoop", "Context", "Provider", "Journal", "ToolRuntime",
        "Policy", "CredentialBroker", "StateStore", "Mailbox", "Subagent", "Workspace",
        "Supervisor", "Recovery", "Projection",
        # DS-P02-00 frozen ports: platform capacity, restricted audit, migration entry.
        "SystemCapacity", "Audit", "Migration", "Capability",
    }
    ids = [row["id"] for row in CATALOG["transactions"]]
    assert len(ids) == len(set(ids)) == 10
    assert set(ids) == {f"T{n}" for n in range(1, 11)}


def test_catalog_has_no_unresolved_implemented_symbols():
    for row in CATALOG["interfaces"] + CATALOG["transactions"]:
        refs = row.get("contracts", row.get("requests", []))
        for ref in refs:
            assert ref["module"].startswith("agent_runtime.")
            symbol = getattr(importlib.import_module(ref["module"]), ref["symbol"])
            assert TypeAdapter(symbol).json_schema()


def test_catalog_transactions_and_event_kinds_exist():
    ids = {row["id"] for row in CATALOG["transactions"]}
    kinds = set(event_adapter.json_schema()["discriminator"]["mapping"])
    for row in CATALOG["interfaces"]:
        assert set(row["transactions"]) <= ids
    for row in CATALOG["transactions"]:
        assert set(row["events"]) <= kinds
        assert row["records"]
        assert len(row["records"]) == len(set(row["records"]))


def test_catalog_does_not_claim_missing_runtime_implementation():
    for row in CATALOG["interfaces"]:
        assert row["status"] == ("partial" if row["contracts"] else "missing")
        assert row["gaps"] and all(row["gaps"])
    for row in CATALOG["transactions"]:
        assert row["implementation"] == "not_implemented"
        assert row["gaps"] and all(row["gaps"])


def test_catalog_interface_transactions_reference_real_transactions():
    ids = {row["id"] for row in CATALOG["transactions"]}
    assert all(set(row["transactions"]) <= ids for row in CATALOG["interfaces"])

"""Every contract leaf is reachable from the single schema registry (DS-P01-03)."""

import importlib
import json
import pkgutil
from pathlib import Path

import agent_runtime.contracts as contracts_package
from agent_runtime.contracts.base import Contract
from agent_runtime.contracts.schema import (
    ADAPTER_SCHEMAS,
    MODEL_SCHEMA_ENTRIES,
    SCHEMA_REGISTRY,
    covered_type_names,
    schema_documents,
)

PACKAGE = Path(__file__).resolve().parents[1] / "src" / "agent_runtime"
SCHEMA_DIRECTORY = PACKAGE / "contracts" / "schemas"


def _contract_leaves() -> set[str]:
    """Concrete contract classes: no in-package subclasses, not a parametrized generic."""
    leaves: set[str] = set()
    for module_info in pkgutil.iter_modules(contracts_package.__path__):
        module = importlib.import_module(f"agent_runtime.contracts.{module_info.name}")
        for name, obj in vars(module).items():
            if "[" in name or not isinstance(obj, type):
                continue
            if not issubclass(obj, Contract) or obj is Contract:
                continue
            subclasses = [
                sub for sub in obj.__subclasses__() if sub.__module__.startswith("agent_runtime")
            ]
            if not subclasses:
                leaves.add(obj.__name__)
    return leaves


def test_registry_names_are_unique_and_generate_every_schema():
    documents = schema_documents()
    # A shared name would be dropped by the dict merge, so the two sources must not collide
    # and no contract may be registered twice under one name.
    model_names = [name for name, _ in MODEL_SCHEMA_ENTRIES]
    assert set(ADAPTER_SCHEMAS) & set(model_names) == set()
    assert len(model_names) == len(set(model_names))
    assert len({entry for _, entry in MODEL_SCHEMA_ENTRIES}) == len(MODEL_SCHEMA_ENTRIES)
    assert set(SCHEMA_REGISTRY) == set(ADAPTER_SCHEMAS) | set(model_names)
    assert set(documents) == {f"{name}.schema.json" for name in SCHEMA_REGISTRY}
    for body in documents.values():
        json.loads(body)


def test_every_contract_leaf_is_reachable_from_a_schema():
    uncovered = sorted(_contract_leaves() - covered_type_names())
    assert uncovered == [], f"contracts without a reachable schema: {uncovered}"


def test_checked_in_schemas_match_the_registry():
    on_disk = {path.name for path in SCHEMA_DIRECTORY.glob("*.schema.json")}
    documents = schema_documents()
    assert on_disk == set(documents)
    for name, body in documents.items():
        assert (SCHEMA_DIRECTORY / name).read_text(encoding="utf-8") == body

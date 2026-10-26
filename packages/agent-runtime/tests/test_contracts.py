import ast
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from agent_runtime import (
    Agent,
    AgentRuntimeError,
    Budget,
    CapabilityKey,
    CapabilityRef,
    DesiredControl,
    FailureEnvelope,
    ResourceRef,
    ResourceScope,
    RunStatus,
    WakeReason,
)


def test_agent_json_roundtrip_and_isolation() -> None:
    source = [{"key": {"namespace": "builtin", "key": "search"}, "revision": "1"}]
    agent = Agent.model_validate({
        "id": "research", "name": "Research", "tools": source,
        "addDatetimeToContext": True, "timezone": "Asia/Shanghai",
    })
    source[0]["key"] = "changed"
    assert agent.tools[0].key.key == "search"
    wire = agent.model_dump(mode="json", by_alias=True)
    assert wire["addDatetimeToContext"] is True
    assert isinstance(wire["tools"], list)
    assert Agent.model_validate_json(json.dumps(wire)) == agent
    with pytest.raises(ValidationError):
        agent.tools[0].key = "mutated"
    with pytest.raises(ValidationError):
        agent.budget.max_tool_calls = 999


@pytest.mark.parametrize("extra", ["user_id", "session_id", "db", "client", "unknown"])
def test_agent_rejects_host_state(extra: str) -> None:
    with pytest.raises(ValidationError):
        Agent.model_validate({"id": "a", "name": "A", extra: "value"})


@pytest.mark.parametrize("revision", ["-1", "01", "1.0", "", "9223372036854775808", 1])
def test_revision_rejects_noncanonical_or_overflow(revision: object) -> None:
    with pytest.raises(ValidationError):
        CapabilityRef.model_validate(
            {"key": {"namespace": "builtin", "key": "search"}, "revision": revision}
        )


def test_revision_boundary_and_zero() -> None:
    for value in ("0", "9223372036854775807"):
        assert (
            CapabilityRef(
                key=CapabilityKey(namespace="builtin", key="search"), revision=value
            ).revision
            == value
        )


@pytest.mark.parametrize("field", ["tools", "knowledge", "skills", "subagents", "hooks", "policy"])
def test_binding_conflicts_are_rejected(field: str) -> None:
    with pytest.raises(ValidationError, match="duplicate capability"):
        Agent.model_validate({"id": "a", "name": "A", field: [
            {"key": {"namespace": "builtin", "key": "same"}, "revision": "1"},
            {"key": {"namespace": "builtin", "key": "same"}, "revision": "2"},
        ]})


@pytest.mark.parametrize("timezone", ["invalid/timezone", "../UTC", "/etc/passwd"])
def test_invalid_timezone(timezone: str) -> None:
    with pytest.raises(ValidationError):
        Agent(id="a", name="A", timezone=timezone)


@pytest.mark.parametrize("value", [-1, True, "24", 1.5])
def test_budget_is_not_coerced(value: object) -> None:
    with pytest.raises(ValidationError):
        Budget.model_validate({"max_tool_calls": value})


def test_resource_scope_required_and_external_version_preserved() -> None:
    with pytest.raises(ValidationError):
        ResourceRef.model_validate({"resourceType": "crm.customer", "resourceId": "123"})
    ref = ResourceRef(resource_type="crm.customer", resource_id="123",
                      resource_version="sha256:abc", scope=ResourceScope(scope_id="company"))
    assert ref.model_dump(mode="json", by_alias=True)["resourceVersion"] == "sha256:abc"


def test_failure_roundtrip_unknown_code_and_safe_exception() -> None:
    failure = FailureEnvelope(
        code="FUTURE_PROVIDER_ERROR", category="provider", retryable=False,
        public_message="Provider unavailable", detail_ref="private-audit-id",
    )
    assert FailureEnvelope.model_validate_json(failure.model_dump_json()) == failure
    assert str(AgentRuntimeError(failure)) == "Provider unavailable"
    assert "private-audit-id" not in str(AgentRuntimeError(failure))


@pytest.mark.parametrize("extra", [
    {"schemaRevision": 2}, {"retryable": "false"},
    {"deadlineAt": "2026-09-14T10:00:00"}, {"secret": "hidden"},
])
def test_failure_rejects_invalid_wire(extra: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        FailureEnvelope.model_validate({
            "code": "INTERNAL", "category": "internal", "retryable": False,
            "publicMessage": "Unavailable", **extra,
        })


def test_control_and_wait_states_are_explicit() -> None:
    assert WakeReason("control") is WakeReason.CONTROL
    assert RunStatus("waiting_user") is RunStatus.WAITING_USER
    with pytest.raises(ValueError):
        DesiredControl("resume")


def test_json_schema_is_closed_and_alias_aware() -> None:
    schema = Agent.model_json_schema(by_alias=True)
    assert schema["additionalProperties"] is False
    assert "addDatetimeToContext" in schema["properties"]


# DS-P01-05 / A1 dependency boundary. The package is a pure library today; when P02 adds
# the standard storage adapter, only designated adapter modules may import an ORM or a
# network/SDK client. Host-application module roots stay forbidden everywhere.
ALWAYS_FORBIDDEN = {"app", "agno", "fastapi", "celery", "temporal"}
ORM_MODULES = {"sqlalchemy", "alembic", "asyncpg", "psycopg", "psycopg2", "aiomysql", "asyncmy"}
NETWORK_MODULES = {
    "httpx",
    "requests",
    "aiohttp",
    "openai",
    "anthropic",
    "google",
    "boto3",
    "redis",
}
ORM_ALLOWED_PREFIXES = ("agent_runtime/storage",)
NETWORK_ALLOWED_PREFIXES = (
    "agent_runtime/storage",
    "agent_runtime/providers",
    "agent_runtime/integrations",
    "agent_runtime/execution",
)


def import_roots(source: str) -> set[str]:
    roots: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".")[0])
    return roots


def boundary_violations(relative_path: str, source: str) -> set[str]:
    roots = import_roots(source)
    violations = roots & ALWAYS_FORBIDDEN
    if roots & ORM_MODULES and not relative_path.startswith(ORM_ALLOWED_PREFIXES):
        violations |= roots & ORM_MODULES
    if roots & NETWORK_MODULES and not relative_path.startswith(NETWORK_ALLOWED_PREFIXES):
        violations |= roots & NETWORK_MODULES
    return violations


def test_core_import_boundary() -> None:
    root = Path(__file__).resolve().parents[1] / "src"
    for path in (root / "agent_runtime").rglob("*.py"):
        relative = path.relative_to(root).as_posix()
        violations = boundary_violations(relative, path.read_text())
        assert violations == set(), (relative, sorted(violations))


@pytest.mark.parametrize(
    ("relative_path", "source", "expected"),
    [
        ("agent_runtime/contracts/events.py", "import sqlalchemy", {"sqlalchemy"}),
        ("agent_runtime/contracts/events.py", "import httpx", {"httpx"}),
        ("agent_runtime/contracts/events.py", "from app.services import x", {"app"}),
        ("agent_runtime/core/loop.py", "import agno", {"agno"}),
        ("agent_runtime/contracts/events.py", "import fastapi", {"fastapi"}),
        ("agent_runtime/storage/models.py", "import sqlalchemy", set()),
        ("agent_runtime/providers/qwen.py", "import httpx", set()),
        ("agent_runtime/storage/dsn.py", "from app import config", {"app"}),
    ],
)
def test_boundary_policy_accepts_adapters_and_rejects_host_imports(
    relative_path: str, source: str, expected: set[str]
) -> None:
    assert boundary_violations(relative_path, source) == expected

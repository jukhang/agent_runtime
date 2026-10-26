"""Offline configuration/schema gates, never counted as live acceptance."""

import pytest
from centn_example.ecosystem_catalog import ChartArguments, EcosystemCatalog, ecosystem_specs
from centn_example.ecosystem_config import EcosystemSettings
from centn_example.resources import ResourceCreate
from centn_example.tools import Tools, ref
from pydantic import ValidationError

from agent_runtime.contracts.failure import AgentRuntimeError


def test_unconfigured_ecosystem_has_no_implicit_root_dotenv(monkeypatch):
    monkeypatch.delenv("CENTN_ECOSYSTEM_CONFIG", raising=False)
    assert EcosystemSettings.from_env() is None


def test_pinned_tools_separate_read_and_supervised_effect():
    mcp, chart = ecosystem_specs()
    assert mcp.mcp.protocol_version == "2026-07-28"
    assert mcp.side_effect == "none" and mcp.idempotency == "read_only"
    assert chart.side_effect == "external" and chart.idempotency == "reconcile"
    assert chart.timeout_seconds == 90 and chart.public_result_policy == "summary"


@pytest.mark.parametrize(
    "arguments",
    [
        {"argv": []},
        {"argv": ["a", "b"]},
        {"argv": [1]},
        {"argv": ["{}"], "command": "sh"},
        {"argv": ["{}"], "url": "https://other.test"},
    ],
)
def test_chart_arguments_cannot_add_code_endpoint_or_paths(arguments):
    with pytest.raises(ValidationError):
        ChartArguments.model_validate(arguments)


def test_schema_dispatch_does_not_replace_existing_report_contract():
    catalog = EcosystemCatalog(Tools(None, "ecosystem_test"))
    report = next(s for s in catalog.specs if s.ref == ref("save-report"))
    catalog.input(report, {"title": "report", "text": "actual report"})
    with pytest.raises(AgentRuntimeError):
        catalog.input(report, {"argv": ["{}"]})
    catalog.input(catalog.ecosystem_specs[0], {})
    with pytest.raises(ValueError):
        catalog.input(catalog.ecosystem_specs[0], {"url": "https://other.test"})


def test_binary_output_is_not_a_public_upload_kind():
    with pytest.raises(ValidationError):
        ResourceCreate.model_validate({"kind": "chart", "text": "data"})


@pytest.mark.parametrize(
    "field,value",
    [
        ("context_input_tokens", 0),
        ("context_input_tokens", 200001),
        ("compression_max_input_tokens", 0),
        ("compression_max_input_tokens", 240001),
        ("compression_timeout_seconds", 0),
        ("compression_timeout_seconds", 241),
    ],
)
def test_host_context_quantity_bounds(field, value):
    from centn_example.config import Settings

    arguments = dict(
        dsn="postgresql+asyncpg://unused",
        token="unused",
        scope="test",
        actor="owner",
        schema="isolated",
        provider_url="http://unused.invalid",
        provider_key="unused",
        model="qwen",
        thinking=False,
    )
    with pytest.raises(ValueError):
        Settings(**arguments, **{field: value})

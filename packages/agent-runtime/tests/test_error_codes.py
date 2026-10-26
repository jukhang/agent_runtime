"""Every failure code this package emits or ships in a fixture is registered (DS-P01-03)."""

import json
import re
from pathlib import Path
from typing import Any

from agent_runtime.contracts.error_codes import ERROR_CODES, unknown_codes
from agent_runtime.provider_integrity import INTEGRITY_FAILURES

PACKAGE = Path(__file__).resolve().parents[1] / "src" / "agent_runtime"
FIXTURES = PACKAGE / "contracts" / "fixtures"

CODE_PATTERNS = (
    re.compile(r'code="([A-Z][A-Z0-9_]*)"'),
    re.compile(r'runtime_failure\(\s*"([A-Z][A-Z0-9_]*)"'),
    re.compile(r'_failure\(\s*"([A-Z][A-Z0-9_]*)"'),
)


def _source_codes() -> set[str]:
    found: set[str] = set()
    for path in PACKAGE.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for pattern in CODE_PATTERNS:
            found.update(pattern.findall(text))
    return found


def _fixture_codes() -> set[str]:
    found: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            code = value.get("code")
            if isinstance(code, str):
                found.add(code)
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    for path in sorted(FIXTURES.glob("*.json")):
        walk(json.loads(path.read_text(encoding="utf-8")))
    return found


def test_catalog_is_wellformed_and_nonempty():
    assert ERROR_CODES
    for code, meaning in ERROR_CODES.items():
        assert re.fullmatch(r"[A-Z][A-Z0-9_]*", code), code
        assert meaning.strip()
    assert len(set(ERROR_CODES)) == len(ERROR_CODES)


def test_integrity_reason_codes_are_registered():
    assert unknown_codes({code for code, _ in INTEGRITY_FAILURES.values()}) == ()


def test_every_emitted_and_fixture_code_is_registered():
    used = _source_codes() | _fixture_codes()
    assert unknown_codes(used) == ()


def test_catalog_covers_the_designed_initial_set():
    designed = {
        "INVALID_INPUT",
        "FORBIDDEN",
        "FENCED",
        "VERSION_CONFLICT",
        "CONTROL_PENDING",
        "PROVIDER_TIMEOUT",
        "INCOMPLETE_MODEL_OUTPUT",
        "CONTEXT_LIMIT",
        "AUDIT_UNAVAILABLE",
        "TOOL_TIMEOUT",
        "SIDE_EFFECT_UNKNOWN",
        "BUDGET_EXHAUSTED",
        "CHECKPOINT_INCOMPATIBLE",
        "DELIVERY_EXPIRED",
        "DEPENDENCY_UNAVAILABLE",
        "INTERNAL",
    }
    assert designed <= set(ERROR_CODES)


def test_unknown_codes_helper_reports_sorted_unique_values():
    assert unknown_codes(["ZZZ", "AAA", "ZZZ", "INTERNAL"]) == ("AAA", "ZZZ")

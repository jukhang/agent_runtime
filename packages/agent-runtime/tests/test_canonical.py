"""One canonical JSON/digest rule across every content hash (DS-P01-03)."""

import json
import math
import random
import shutil
import struct
import subprocess
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest
from test_events import EVENTS
from test_foundation_boundaries import INVOCATION, REF

from agent_runtime.contracts.canonical import (
    MAX_SAFE_INTEGER,
    CanonicalJSONError,
    canonical_bytes,
    canonical_json,
    canonical_number,
    content_digest,
)
from agent_runtime.contracts.events import event_adapter
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.journal import ContentDraft, output_digest
from agent_runtime.contracts.policy import PolicyRequest
from agent_runtime.contracts.state_events import state_draft_adapter
from agent_runtime.policy_validation import policy_input_digest
from agent_runtime.state_event_validation import state_event_digest
from agent_runtime.tool_validation import arguments_digest

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
VECTOR_FIXTURE = PACKAGE_ROOT / "src/agent_runtime/contracts/fixtures/canonical-vectors.json"
NODE_SCRIPT = PACKAGE_ROOT / "scripts/verify_canonical_vectors.mjs"
VECTORS = json.loads(VECTOR_FIXTURE.read_text())


def run_node(*arguments: str, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
    """Run the independent Node implementation; absence of Node fails the gate (P1-1)."""
    node = shutil.which("node")
    if node is None:
        raise AssertionError(
            "Node is required for the cross-language canonical verification; "
            "install Node or mark the task BLOCKED"
        )
    return subprocess.run(
        [node, str(NODE_SCRIPT), *arguments],
        input=stdin,
        capture_output=True,
        text=True,
        check=True,
        timeout=120,
    )


def _state_draft(kind: str = "model.prepared"):
    event = event_adapter.validate_python(EVENTS[kind])
    data = event.model_dump(
        exclude={
            "schema_revision",
            "event_id",
            "session_id",
            "session_seq",
            "recorded_at",
            "visibility",
            "trace_id",
        }
    )
    return state_draft_adapter.validate_python({**data, "producer_key": f"state/{event.event_id}"})


def test_canonical_json_is_key_order_insensitive():
    assert canonical_json({"b": 1, "a": 2}) == canonical_json({"a": 2, "b": 1})
    assert canonical_json({"a": {"y": 1, "x": 2}}) == canonical_json({"a": {"x": 2, "y": 1}})


def test_canonical_json_preserves_json_type_distinctions():
    assert canonical_json(True) != canonical_json(1)
    assert canonical_json([True]) != canonical_json([1])
    # RFC 8785 canonicalizes the JSON number, not the source token: 1 and 1.0 are the
    # same number. ProviderIntegrity keeps the wire-level token distinction separately.
    assert canonical_json(1) == canonical_json(1.0) == "1"


@pytest.mark.parametrize("value", [float("nan"), float("inf"), {"x": float("-inf")}])
def test_canonical_json_rejects_non_finite_numbers(value):
    with pytest.raises(ValueError):
        canonical_json(value)


def test_content_digest_is_sha256_of_canonical_bytes():
    value = {"b": [1, None, "µ"], "a": True}
    # Golden pin, not a self-comparison: the digest must stay stable across refactors.
    assert canonical_bytes(value) == b'{"a":true,"b":[1,null,"\xc2\xb5"]}'
    assert content_digest(value) == (
        "aa744c6020ab0be0a8711cf183315e283e0876fd89957c946b370d32c3cc631f"
    )


@pytest.mark.parametrize("vector", VECTORS["vectors"], ids=lambda vector: vector["name"])
def test_canonical_golden_vectors(vector):
    value = vector["value"]
    assert canonical_json(value) == vector["canonical"]
    assert canonical_bytes(value) == vector["canonical"].encode("utf-8")
    assert content_digest(value) == vector["digest"]


@pytest.mark.parametrize("case", VECTORS["rejections"], ids=lambda case: case["name"])
def test_canonical_rejections_are_pinned(case):
    if "value" not in case:
        # Non-string keys are not expressible in JSON; covered by the unit test below.
        assert case["reason"] == FIXTURE_REASONS[case["name"]]
        return
    with pytest.raises(CanonicalJSONError):
        canonical_json(case["value"])


FIXTURE_REASONS = {
    "overflowing-integer": "canonical JSON numbers must fit an IEEE-754 double",
    "lone-surrogate": "canonical JSON text must be valid UTF-8",
    "non-string-key": "canonical JSON object keys must be strings",
}


def test_fixture_covers_the_required_boundary_categories():
    names = {vector["name"] for vector in VECTORS["vectors"]}
    assert {
        "zero-forms",
        "exponent-lower-threshold",
        "exponent-upper-threshold",
        "decimal-fractions",
        "string-with-unicode",
        "escaping",
        "nested-objects",
        "max-safe-integer",
        "min-safe-integer",
        "utf16-key-order",
        "double-extremes",
        "large-representable-integers",
        "large-representable-integer-in-object",
    } <= names
    rejection_names = {case["name"] for case in VECTORS["rejections"]}
    assert {
        "rounding-integer",
        "rounding-large-integer",
        "unrepresentable-integer",
        "overflowing-integer",
        "lone-surrogate",
        "non-string-key",
    } <= rejection_names


def test_node_reproduces_every_golden_vector():
    """P1-1: an independent implementation must derive the same texts and digests."""
    result = run_node(str(VECTOR_FIXTURE))
    summary = json.loads(result.stdout)
    assert summary["status"] == "passed"
    assert summary["vectors"] == len(VECTORS["vectors"])
    assert summary["digests"] == len(VECTORS["vectors"])


def _oracle_numbers() -> list[float]:
    rng = random.Random(20260921)
    numbers = [
        0.0,
        -0.0,
        5e-324,
        -5e-324,
        1e-7,
        1e-6,
        1e-5,
        1e-4,
        0.1,
        0.2,
        0.3,
        1 / 3,
        1.5,
        123.456,
        1e15,
        1e16,
        1e20,
        1e21,
        1e22,
        2.0**53,
        2.0**53 - 1,
        1.7976931348623157e308,
        -1.7976931348623157e308,
    ]
    while len(numbers) < 2000:
        candidate = struct.unpack("<d", rng.randbytes(8))[0]
        if math.isfinite(candidate):
            numbers.append(candidate)
    return numbers


def test_python_number_formatter_matches_ecmascript():
    """P1-1: randomized doubles must match ECMAScript Number::toString exactly."""
    numbers = _oracle_numbers()
    result = run_node("--numbers", stdin=json.dumps(numbers))
    expected = json.loads(result.stdout)
    mismatch = [
        (number, text, canonical_number(number))
        for number, text in zip(numbers, expected, strict=True)
        if canonical_number(number) != text
    ]
    assert mismatch == []


def test_python_integer_formatter_matches_ecmascript_safe_range():
    integers = [-MAX_SAFE_INTEGER, -1, 0, 1, 42, MAX_SAFE_INTEGER - 1, MAX_SAFE_INTEGER]
    result = run_node("--numbers", stdin=json.dumps([float(value) for value in integers]))
    expected = json.loads(result.stdout)
    assert [canonical_number(value) for value in integers] == expected


@pytest.mark.parametrize(
    "value",
    [2**53, 2**53 + 2, 2**54, 2**60, -(2**60), 2**400, 10**21, 10**22],
)
def test_exactly_representable_large_integers_match_ecmascript(value):
    """RFC 8785 encodes doubles, so exact large integers use the shortest text."""
    result = run_node("--numbers", stdin=json.dumps([float(value)]))
    (expected,) = json.loads(result.stdout)
    assert canonical_number(value) == expected
    assert canonical_json(value) == expected


@pytest.mark.parametrize("value", [2**53 + 1, 2**63 - 1, -(2**63) + 1, 10**30, 2**400 + 1])
def test_rounding_integers_are_rejected_instead_of_silently_rounded(value):
    with pytest.raises(CanonicalJSONError, match="exactly representable"):
        canonical_number(value)


@pytest.mark.parametrize("value", [10**400, -(10**400)])
def test_integers_beyond_the_double_range_are_rejected(value):
    with pytest.raises(CanonicalJSONError, match="fit an IEEE-754 double"):
        canonical_number(value)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_numbers_name_the_unified_rule(value):
    with pytest.raises(CanonicalJSONError, match="must be finite"):
        canonical_number(value)


def _oracle_strings() -> list[str]:
    rng = random.Random(20260922)
    sample = [
        "",
        "plain",
        'quote"back\\slash/solidus',
        "tab\tnl\ncr\rbackspace\bformfeed\f",
        "".join(chr(code) for code in range(0x20)),
        "control-del-\x7f",
        "c1-\x80\x85\x9f",
        "中文 🙂 café µ",
        "line\u2028sep\u2029para",
        "bom\ufeffrlo\u202ezwj\u200d",
        "combining-e\u0301-a\u0308",
        "astral-\U0001f600-\U0002f800",
        "\U0001f469\u200d\U0001f4bb",
    ]
    alphabet = (
        [chr(code) for code in range(0x20, 0x7F)]
        + ["\u00e9", "\u4e2d", "\U0001f600", "\u2028", "\u200d", "\ufeff", "\u0085"]
    )
    while len(sample) < 500:
        sample.append("".join(rng.choice(alphabet) for _ in range(rng.randint(0, 24))))
    return sample


def test_python_string_escaping_matches_ecmascript():
    """P1-1: string escaping must be ECMAScript JSON.stringify, character for character."""
    strings = _oracle_strings()
    result = run_node("--strings", stdin=json.dumps(strings))
    expected = json.loads(result.stdout)
    assert len(expected) == len(strings)
    mismatch = [
        (value, text, canonical_json(value))
        for value, text in zip(strings, expected, strict=True)
        if canonical_json(value) != text
    ]
    assert mismatch == []


def test_non_string_keys_and_rounding_integers_are_rejected():
    with pytest.raises(CanonicalJSONError):
        canonical_json({1: "a"})
    with pytest.raises(CanonicalJSONError):
        canonical_json({"a": [{"b": 2**53 + 1}]})
    with pytest.raises(CanonicalJSONError):
        canonical_json([-(2**53) - 1])
    assert canonical_json([2**53 - 1, -(2**53 - 1)]) == "[9007199254740991,-9007199254740991]"
    # Exactly representable integers of any size are legal IEEE-754 values.
    assert canonical_json([2**53, 2**60]) == "[9007199254740992,1152921504606847000]"


def test_unencodable_text_is_rejected_by_the_encoder():
    with pytest.raises(CanonicalJSONError):
        canonical_json({"text": "bad\ud800"})
    with pytest.raises(CanonicalJSONError):
        canonical_bytes({"text": "bad\ud800"})


def _surrogate_draft():
    draft = _state_draft("tool.progress")
    return draft.model_copy(
        update={"payload": draft.payload.model_copy(update={"summary": "bad\ud800"})}
    )


def test_digest_boundaries_convert_canonical_failure_into_a_failure_envelope():
    policy = deepcopy({"invocation": INVOCATION, "policy": REF, "resources": []})
    policy["invocation"]["call"]["arguments"] = {"x": float("nan")}
    draft = ContentDraft(
        kind="assistant.delta",
        occurred_at=datetime(2026, 9, 17, tzinfo=UTC),
        payload=EVENTS["assistant.delta"]["payload"],
    )
    surrogate_output = draft.model_copy(
        update={"payload": draft.payload.model_copy(update={"text": "bad\ud800"})}
    )
    for call in (
        lambda: policy_input_digest(PolicyRequest.model_validate(policy)),
        lambda: output_digest(surrogate_output),
        lambda: state_event_digest(_surrogate_draft()),
        lambda: arguments_digest({"x": "bad\ud800"}),
    ):
        with pytest.raises(AgentRuntimeError) as exc:
            call()
        assert exc.value.failure.code == "INVALID_INPUT"
        assert exc.value.failure.category == "validation"


def test_arguments_digest_matches_the_shared_rule():
    for arguments in ({"x": 1}, {"b": 2, "a": 1}, {"x": True}, {"nested": {"list": [1, 2]}}):
        assert arguments_digest(arguments) == content_digest(arguments)


def test_state_event_digest_uses_alias_expanded_canonical_json():
    draft = _state_draft()
    assert state_event_digest(draft) == content_digest(draft.model_dump(mode="json", by_alias=True))


def test_output_digest_uses_alias_expanded_canonical_json():
    from agent_runtime.contracts.payloads import ContentDelta

    draft = ContentDraft(
        kind="assistant.delta",
        occurred_at=datetime(2026, 9, 17, tzinfo=UTC),
        payload=ContentDelta(
            channel="content",
            delta_index="0",
            item_revision="1",
            text="hello",
        ),
    )
    assert output_digest(draft) == content_digest(draft.model_dump(mode="json", by_alias=True))


def test_policy_input_digest_uses_alias_expanded_canonical_json():
    request = PolicyRequest.model_validate(
        {"invocation": INVOCATION, "policy": REF, "resources": []}
    )
    assert policy_input_digest(request) == content_digest(
        request.model_dump(mode="json", by_alias=True)
    )


def test_policy_input_digest_is_stable_for_argument_key_order():
    ordered = deepcopy({"invocation": INVOCATION, "policy": REF, "resources": []})
    ordered["invocation"]["call"]["arguments"] = {"x": 1, "y": 2}
    reordered = deepcopy(ordered)
    reordered["invocation"]["call"]["arguments"] = {"y": 2, "x": 1}
    assert policy_input_digest(PolicyRequest.model_validate(ordered)) == policy_input_digest(
        PolicyRequest.model_validate(reordered)
    )

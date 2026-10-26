"""P1-2: identifiers and machine keys must be safe for logs, JSON and indexes."""

from uuid import uuid4

import pytest
from pydantic import TypeAdapter, ValidationError

from agent_runtime.contracts.base import Identifier, MachineKey
from agent_runtime.contracts.journal import CommittedEntry, output_producer_key
from agent_runtime.contracts.requests import SubmitRequest

IDENTIFIER = TypeAdapter(Identifier)
MACHINE_KEY = TypeAdapter(MachineKey)


@pytest.mark.parametrize(
    "value",
    [
        "scope",
        "trace-1",
        "报告-2026",
        "café",  # precomposed
        "cafe\u0301",  # decomposed: combining mark is printable, not a control
        "emoji-🙂-ok",
        "key.with:separators/and_more",
    ],
)
def test_printable_identifiers_are_accepted(value):
    assert IDENTIFIER.validate_python(value) == value
    assert IDENTIFIER.validate_json('"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"')


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "a b",
        "a\tb",
        "a\nb",
        "a\rb",
        "a\u0000b",  # NUL
        "a\u001fb",  # C0
        "a\u007fb",  # DEL
        "a\u0085b",  # C1 next line
        "a\u00a0b",  # no-break space
        "a\u200bb",  # zero width space
        "a\u200db",  # zero width joiner
        "a\u200eb",  # left-to-right mark
        "a\u202eb",  # right-to-left override (bidi)
        "a\u2060b",  # word joiner
        "a\u2028b",  # line separator
        "a\ufeffb",  # byte order mark
        "a\ud800b",  # lone surrogate
        "x" * 256,
    ],
)
def test_identifiers_reject_control_format_and_invisible_characters(value):
    with pytest.raises(ValidationError):
        IDENTIFIER.validate_python(value)


@pytest.mark.parametrize(
    "value",
    [
        "scope-1",
        "state/00000000-0000-4000-8000-000000000001/content/0",
        "urn:uuid:1234",
        "user@host+tag=",
        "k_1.2:3/4",
        "!" * 255,
        "A",
    ],
)
def test_machine_keys_accept_printable_ascii(value):
    assert MACHINE_KEY.validate_python(value) == value
    # A machine key is always also a valid free-form identifier.
    assert IDENTIFIER.validate_python(value) == value


@pytest.mark.parametrize(
    "value",
    [
        "",
        "café",
        "报告",
        "a b",
        "a\tb",
        "a\nb",
        "a\u200bb",
        "a\u0000b",
        "🙂",
        "x" * 256,
    ],
)
def test_machine_keys_reject_non_ascii_and_control_characters(value):
    with pytest.raises(ValidationError):
        MACHINE_KEY.validate_python(value)


def test_schema_patterns_expose_the_rule_to_other_languages():
    identifier = IDENTIFIER.json_schema()
    machine_key = MACHINE_KEY.json_schema()
    assert identifier["pattern"] == r"^[^\s\p{Cc}\p{Cf}]+$"
    assert (identifier["minLength"], identifier["maxLength"]) == (1, 255)
    assert machine_key["pattern"] == r"^[\x21-\x7e]+$"


def test_machine_keys_are_required_where_keys_are_persisted():
    with pytest.raises(ValidationError):
        SubmitRequest.model_validate(
            {
                "sessionId": str(uuid4()),
                "agent": {"key": {"namespace": "builtin", "key": "agent"}, "revision": "1"},
                "idempotencyKey": "ключ\nidem",
                "inputs": [{"kind": "text", "text": "hi"}],
            }
        )
    entry = {
        "producerKey": "中文",
        "contentDigest": "0" * 64,
        "eventId": str(uuid4()),
        "sessionSeq": "1",
        "disposition": "created",
    }
    with pytest.raises(ValidationError):
        CommittedEntry.model_validate(entry)
    assert CommittedEntry.model_validate({**entry, "producerKey": "a/content/0"}).producer_key


def test_generated_producer_keys_are_machine_keys():
    attempt_id = uuid4()
    key = output_producer_key(attempt_id, "content", "12")
    assert key == f"{attempt_id}/content/12"
    assert MACHINE_KEY.validate_python(key) == key

"""The private summary provider consumes the exact persisted messages and profile."""

import json

import pytest

pytest.importorskip("sqlalchemy")

from test_provider_invoker import make_request

from agent_runtime.contracts.canonical import canonical_bytes, content_digest
from agent_runtime.providers.openai_compatible import OpenAIAdapterProfile
from agent_runtime.storage.compression import render_summary_payload


@pytest.mark.parametrize("profile", [OpenAIAdapterProfile(), OpenAIAdapterProfile.qwen()])
def test_summary_messages_and_adapter_survive_sql_json_roundtrip(profile):
    model = make_request().model
    user = '{"task":"Active project","sourceLines":[{"id":0,"text":"A=1"}]}'
    payload = render_summary_payload(
        {"promptText": "Extract literal references.", "userInput": user, "sources": []},
        model,
        profile,
    )
    restored = json.loads(canonical_bytes(payload))
    expected = render_summary_payload(restored, model, profile)
    assert restored["renderedRequestDigest"] == expected["renderedRequestDigest"]
    assert content_digest(restored["renderedRequest"]) == restored["renderedRequestDigest"]
    assert restored["renderedRequest"]["messages"] == [
        {"role": "system", "content": "Extract literal references."},
        {"role": "user", "content": user},
    ]
    assert restored["renderedRequest"]["adapterProfile"]["name"] == profile.name

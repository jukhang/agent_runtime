"""Stable provider-safe names for pinned tool revisions."""

from hashlib import sha256

from agent_runtime.contracts.canonical import canonical_bytes
from agent_runtime.contracts.references import CapabilityRef


def provider_tool_name(ref: CapabilityRef) -> str:
    digest = sha256(canonical_bytes(ref.model_dump(mode="json", by_alias=True))).hexdigest()
    return f"tool_{digest[:32]}"

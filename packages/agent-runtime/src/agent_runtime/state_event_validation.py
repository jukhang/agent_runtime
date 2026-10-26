"""Materialize validated drafts only with identities allocated by the journal transaction.

DS-P01-03: digests use ``contracts.canonical.content_digest`` with alias-expanded JSON,
and receipt mismatches raise ``AgentRuntimeError(FailureEnvelope)``.
"""

from datetime import datetime
from uuid import UUID

from agent_runtime.contracts.canonical import CanonicalJSONError, content_digest
from agent_runtime.contracts.events import EventEnvelope, event_adapter
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.state_events import (
    ServiceEventAuthority,
    StateEventBatch,
    StateEventDraft,
    StateEventReceipt,
)

RESTRICTED = {"model.prepared", "mailbox.applied", "handoff.committed", "checkpoint.migrated"}


def state_event_digest(draft: StateEventDraft) -> str:
    try:
        return content_digest(draft.model_dump(mode="json", by_alias=True))
    except CanonicalJSONError as exc:
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="The state event has no canonical JSON encoding.",
        ) from exc


def validate_state_receipt(batch: StateEventBatch, receipt: StateEventReceipt) -> None:
    authority = batch.authority
    scope = (
        authority.actor.scope_id
        if isinstance(authority, ServiceEventAuthority)
        else authority.guard.actor.scope_id
    )
    if (receipt.scope_id, receipt.session_id) != (scope, batch.session_id) or len(
        batch.events
    ) != len(receipt.entries):
        raise runtime_failure(
            "AUDIT_UNAVAILABLE",
            category="storage",
            public_message="The state-event receipt identity or entry count does not match.",
        )
    for draft, entry in zip(batch.events, receipt.entries, strict=True):
        if (draft.producer_key, state_event_digest(draft)) != (
            entry.producer_key,
            entry.content_digest,
        ):
            raise runtime_failure(
                "AUDIT_UNAVAILABLE",
                category="storage",
                public_message="The state-event receipt does not match the submitted event.",
            )


def materialize_state_event(
    draft: StateEventDraft,
    *,
    event_id: UUID,
    session_id: UUID,
    session_seq: str,
    recorded_at: datetime,
    trace_id: str,
) -> EventEnvelope:
    return event_adapter.validate_python(
        {
            **draft.model_dump(mode="python", exclude={"producer_key"}),
            "schema_revision": 1,
            "event_id": event_id,
            "session_id": session_id,
            "session_seq": session_seq,
            "recorded_at": recorded_at,
            "trace_id": trace_id,
            "visibility": "restricted" if draft.kind in RESTRICTED else "public",
        }
    )

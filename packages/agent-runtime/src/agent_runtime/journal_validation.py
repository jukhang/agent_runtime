"""Validate a storage receipt against its request, without asserting database durability."""

from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.journal import (
    CommittedBatch,
    ProducerBatch,
    output_digest,
    output_producer_key,
)


def validate_committed_batch(request: ProducerBatch, receipt: CommittedBatch) -> None:
    request = ProducerBatch.model_validate(request.model_dump())
    receipt = CommittedBatch.model_validate(receipt.model_dump())
    identity = (
        request.lease.scope_id,
        request.lease.session_id,
        request.lease.run_id,
        request.attempt_id,
        request.item_id,
    )
    received = (
        receipt.scope_id,
        receipt.session_id,
        receipt.run_id,
        receipt.attempt_id,
        receipt.item_id,
    )
    keys = [
        output_producer_key(request.attempt_id, e.payload.channel, e.payload.delta_index)
        for e in request.events
    ]
    digests = [output_digest(event) for event in request.events]
    if (
        identity != received
        or keys != [entry.producer_key for entry in receipt.entries]
        or digests != [entry.content_digest for entry in receipt.entries]
    ):
        raise AgentRuntimeError(
            FailureEnvelope(
                code="AUDIT_UNAVAILABLE",
                category="storage",
                retryable=False,
                public_message="Journal receipt does not match the submitted output batch.",
            )
        )

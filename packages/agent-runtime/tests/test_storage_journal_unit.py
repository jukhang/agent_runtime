import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from agent_runtime.contracts.journal import (
    OutboxClaim,
    OutboxClaimRequest,
    OutboxDelivery,
    OutboxNotice,
)
from agent_runtime.storage.journal import OutboxPublisher


class FakeStore:
    def __init__(self, claim: OutboxClaim | None) -> None:
        self.claim_value = claim
        self.deliveries: list[OutboxDelivery] = []
        self.retries: list[tuple[OutboxDelivery, int]] = []

    async def claim(self, request: OutboxClaimRequest) -> OutboxClaim | None:
        return self.claim_value

    async def delivered(self, delivery: OutboxDelivery) -> int:
        self.deliveries.append(delivery)
        return len(delivery.event_ids)

    async def retry(self, delivery: OutboxDelivery, *, delay_seconds: int) -> int:
        self.retries.append((delivery, delay_seconds))
        return len(delivery.event_ids)


class FakeSink:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.notices: list[OutboxNotice] = []

    async def publish(self, notice: OutboxNotice) -> None:
        self.notices.append(notice)
        if self.fail:
            raise RuntimeError("transport failed")


def claim() -> OutboxClaim:
    now = datetime.now(UTC)
    return OutboxClaim(
        claim_token=uuid4(),
        lease_until=now + timedelta(seconds=30),
        notices=(
            OutboxNotice(
                scope_id="scope",
                session_id=uuid4(),
                event_id=uuid4(),
                session_seq="1",
            ),
        ),
    )


def test_publisher_acks_only_after_notification() -> None:
    store = FakeStore(claim())
    sink = FakeSink()
    publisher = OutboxPublisher(store, sink)
    delivered = asyncio.run(
        publisher.publish_once(
            OutboxClaimRequest(limit=1, lease_seconds=30)
        )
    )
    assert delivered == 1
    assert len(sink.notices) == 1
    assert len(store.deliveries) == 1
    assert not store.retries


def test_publisher_requeues_claim_when_transport_fails() -> None:
    store = FakeStore(claim())
    publisher = OutboxPublisher(store, FakeSink(fail=True))
    with pytest.raises(RuntimeError, match="transport failed"):
        asyncio.run(
            publisher.publish_once(
                OutboxClaimRequest(limit=1, lease_seconds=30)
            )
        )
    assert not store.deliveries
    assert len(store.retries) == 1 and store.retries[0][1] == 1


def test_publisher_noops_when_outbox_is_empty() -> None:
    store = FakeStore(None)
    sink = FakeSink()
    publisher = OutboxPublisher(store, sink)
    assert (
        asyncio.run(
            publisher.publish_once(
                OutboxClaimRequest(limit=1, lease_seconds=30)
            )
        )
        == 0
    )
    assert not sink.notices

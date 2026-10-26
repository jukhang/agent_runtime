"""PostgreSQL Journal, replay and transactional Outbox adapter.

The Journal is the event source of truth. A transport notification contains only event
identity and a cursor; consumers always replay the authoritative rows from PostgreSQL.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.contracts.events import EventEnvelope, ResyncFrame, event_adapter
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.journal import (
    CommittedBatch,
    CommittedEntry,
    FlushedChannel,
    FlushProof,
    JournalPage,
    OutboxClaim,
    OutboxClaimRequest,
    OutboxDelivery,
    OutboxNotice,
    ProducerBatch,
    SealOutputRequest,
    output_digest,
    output_producer_key,
)
from agent_runtime.contracts.ownership import ChildLeaseToken
from agent_runtime.contracts.requests import StreamRequest
from agent_runtime.contracts.state_events import (
    ChildEventAuthority,
    MainEventAuthority,
    ServiceEventAuthority,
    StateEventBatch,
    StateEventReceipt,
)
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.state_event_validation import materialize_state_event, state_event_digest
from agent_runtime.storage.child_authority import require_child
from agent_runtime.storage.child_output import ChildProducerBatch, ChildSealOutputRequest
from agent_runtime.storage.uow import errors, facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped

Clock = Callable[[], datetime]
ReadAuthorizer = Callable[[ActorContext, Mapping[str, Any], bool], Awaitable[bool]]


def utcnow() -> datetime:
    return datetime.now(UTC)


class NotificationSink(Protocol):
    async def publish(self, notice: OutboxNotice) -> None: ...


class OutboxStore(Protocol):
    async def claim(self, request: OutboxClaimRequest) -> OutboxClaim | None: ...
    async def delivered(self, delivery: OutboxDelivery) -> int: ...
    async def retry(self, delivery: OutboxDelivery, *, delay_seconds: int) -> int: ...


@dataclass(frozen=True, slots=True)
class _JournalSql:
    session: sa.TextClause
    session_read: sa.TextClause
    session_update: sa.TextClause
    event_by_key: sa.TextClause
    output_events: sa.TextClause
    seal_by_producer: sa.TextClause
    seal_by_proof: sa.TextClause
    seal_insert: sa.TextClause
    event_insert: sa.TextClause
    outbox_insert: sa.TextClause
    attempt: sa.TextClause
    item: sa.TextClause
    replay: sa.TextClause
    first_seq: sa.TextClause
    claim_outbox: sa.TextClause
    deliver_outbox: sa.TextClause
    retry_outbox: sa.TextClause
    workspace: sa.TextClause


def _statements(schema: str) -> _JournalSql:
    return _JournalSql(
        session=scoped(
            "SELECT id, scope_id, owner_actor_id, event_seq, auth_revision, deleted_at "
            "FROM {t}session WHERE scope_id = :scope_id AND id = :session_id FOR UPDATE",
            schema,
        ),
        session_read=scoped(
            "SELECT id, scope_id, owner_actor_id, event_seq, auth_revision, deleted_at "
            "FROM {t}session WHERE scope_id = :scope_id AND id = :session_id",
            schema,
        ),
        session_update=scoped(
            "UPDATE {t}session SET event_seq = :event_seq, updated_at = :now "
            "WHERE scope_id = :scope_id AND id = :session_id",
            schema,
        ),
        event_by_key=scoped(
            "SELECT * FROM {t}event WHERE scope_id = :scope_id "
            "AND producer_key = :producer_key FOR UPDATE",
            schema,
        ),
        output_events=scoped(
            "SELECT producer_key, kind, payload, session_seq FROM {t}event "
            "WHERE scope_id = :scope_id AND session_id = :session_id "
            "AND model_attempt_id = :attempt_id AND item_id = :item_id "
            "AND kind IN ('assistant.delta', 'analysis.delta') ORDER BY session_seq",
            schema,
        ),
        seal_by_producer=scoped(
            "SELECT * FROM {t}output_seal WHERE scope_id = :scope_id AND run_id = :run_id "
            "AND model_attempt_id = :attempt_id AND item_id = :item_id FOR UPDATE",
            schema,
        ),
        seal_by_proof=scoped(
            "SELECT * FROM {t}output_seal WHERE scope_id = :scope_id "
            "AND proof_id = :proof_id FOR UPDATE",
            schema,
        ),
        seal_insert=scoped(
            "INSERT INTO {t}output_seal (id, scope_id, session_id, run_id, model_attempt_id, "
            "item_id, proof_id, content_digest, analysis_digest, content_last_delta_index, "
            "content_last_session_seq, analysis_last_delta_index, analysis_last_session_seq, "
            "durable_seq, sealed_revision, sealed_at) VALUES (:id, :scope_id, :session_id, "
            ":run_id, :model_attempt_id, :item_id, :proof_id, :content_digest, :analysis_digest, "
            ":content_last_delta_index, :content_last_session_seq, :analysis_last_delta_index, "
            ":analysis_last_session_seq, :durable_seq, :sealed_revision, :sealed_at)",
            schema,
        ),
        event_insert=scoped(
            "INSERT INTO {t}event "
            "(id, scope_id, created_at, updated_at, session_id, turn_id, run_id, "
            "parent_run_id, step_id, item_id, model_attempt_id, session_seq, "
            "schema_revision, producer_key, producer_digest, kind, visibility, trace_id, "
            "occurred_at, recorded_at, payload) VALUES "
            "(:id, :scope_id, :now, :now, :session_id, :turn_id, :run_id, :parent_run_id, "
            ":step_id, :item_id, :model_attempt_id, :session_seq, :schema_revision, "
            ":producer_key, :producer_digest, :kind, :visibility, :trace_id, :occurred_at, "
            ":recorded_at, CAST(:payload AS jsonb))",
            schema,
        ),
        outbox_insert=scoped(
            "INSERT INTO {t}outbox "
            "(id, scope_id, created_at, updated_at, event_id, claim_token, status, "
            "available_at, lease_until, delivered_at, attempt_count) VALUES "
            "(:id, :scope_id, :now, :now, :event_id, NULL, 'pending', :now, NULL, NULL, 0)",
            schema,
        ),
        attempt=scoped(
            "SELECT run_id FROM {t}model_attempt WHERE scope_id = :scope_id AND id = :id",
            schema,
        ),
        item=scoped(
            "SELECT run_id, session_id FROM {t}timeline_item "
            "WHERE scope_id = :scope_id AND id = :id",
            schema,
        ),
        replay=scoped(
            "SELECT * FROM {t}event WHERE scope_id = :scope_id AND session_id = :session_id "
            "AND session_seq > :after_seq ORDER BY session_seq LIMIT :limit",
            schema,
        ),
        first_seq=scoped(
            "SELECT MIN(session_seq) FROM {t}event "
            "WHERE scope_id = :scope_id AND session_id = :session_id",
            schema,
        ),
        claim_outbox=scoped(
            "WITH candidates AS ("
            "SELECT id FROM {t}outbox WHERE scope_id = :scope_id AND "
            "((status = 'pending' AND available_at <= :now) OR "
            "(status = 'claimed' AND lease_until <= :now)) "
            "ORDER BY available_at, id FOR UPDATE SKIP LOCKED LIMIT :limit), "
            "claimed AS (UPDATE {t}outbox o SET status = 'claimed', claim_token = :token, "
            "lease_until = :lease_until, attempt_count = o.attempt_count + 1, updated_at = :now "
            "FROM candidates c WHERE o.id = c.id RETURNING o.event_id) "
            "SELECT e.id AS event_id, e.session_id, e.session_seq FROM claimed c "
            "JOIN {t}event e ON e.scope_id = :scope_id AND e.id = c.event_id "
            "ORDER BY e.session_id, e.session_seq",
            schema,
        ),
        deliver_outbox=scoped(
            "UPDATE {t}outbox SET status = 'delivered', claim_token = NULL, lease_until = NULL, "
            "delivered_at = :now, updated_at = :now WHERE scope_id = :scope_id "
            "AND claim_token = :token AND status = 'claimed' AND event_id = ANY(:event_ids)",
            schema,
        ),
        retry_outbox=scoped(
            "UPDATE {t}outbox SET status = 'pending', claim_token = NULL, lease_until = NULL, "
            "available_at = :available_at, updated_at = :now WHERE scope_id = :scope_id "
            "AND claim_token = :token AND status = 'claimed' AND event_id = ANY(:event_ids)",
            schema,
        ),
        workspace=scoped(
            "SELECT owner_run_id, generation, revision FROM {t}workspace "
            "WHERE scope_id = :scope_id AND id = :workspace_id FOR UPDATE",
            schema,
        ),
    )


def _event_payload(event: EventEnvelope) -> dict[str, Any]:
    return {
        "id": event.event_id,
        "session_id": event.session_id,
        "turn_id": event.turn_id,
        "run_id": event.run_id,
        "parent_run_id": event.parent_run_id,
        "step_id": event.step_id,
        "item_id": event.item_id,
        "model_attempt_id": event.model_attempt_id,
        "schema_revision": event.schema_revision,
        "kind": event.kind,
        "visibility": event.visibility,
        "trace_id": event.trace_id,
        "occurred_at": event.occurred_at,
        "recorded_at": event.recorded_at,
        "payload": json.dumps(
            event.payload.model_dump(mode="json", by_alias=True),
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    }


def _same_event(row: Mapping[str, Any], event: EventEnvelope, digest: str) -> bool:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return bool(
        row["session_id"] == event.session_id
        and row["turn_id"] == event.turn_id
        and row["run_id"] == event.run_id
        and row["parent_run_id"] == event.parent_run_id
        and row["step_id"] == event.step_id
        and row["item_id"] == event.item_id
        and row["model_attempt_id"] == event.model_attempt_id
        and row["kind"] == event.kind
        and row["visibility"] == event.visibility
        and row["trace_id"] == event.trace_id
        and row["producer_digest"] == digest
        and payload == event.payload.model_dump(mode="json", by_alias=True)
    )


def _row_event(row: Mapping[str, Any]) -> EventEnvelope:
    payload = row["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return event_adapter.validate_python(
        {
            "schema_revision": row["schema_revision"],
            "event_id": row["id"],
            "session_id": row["session_id"],
            "session_seq": str(row["session_seq"]),
            "turn_id": row["turn_id"],
            "run_id": row["run_id"],
            "parent_run_id": row["parent_run_id"],
            "step_id": row["step_id"],
            "item_id": row["item_id"],
            "model_attempt_id": row["model_attempt_id"],
            "kind": row["kind"],
            "occurred_at": row["occurred_at"],
            "recorded_at": row["recorded_at"],
            "visibility": row["visibility"],
            "trace_id": row["trace_id"],
            "payload": payload,
        }
    )


class SqlJournal:
    """Append output or state events with Event + Outbox atomicity."""

    @property
    def schema(self) -> str:
        return self._schema

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        actor: ActorContext,
        *,
        clock: Clock = utcnow,
    ) -> None:
        self._engine = engine
        self._schema = require_schema(schema)
        self._actor = actor
        self._clock = clock
        self._session_factory = async_sessionmaker(engine, expire_on_commit=False)
        self._sql = _statements(self._schema)
        self._locks = locks.build_lock_statements(self._schema)

    async def append(self, batch: ProducerBatch | ChildProducerBatch) -> CommittedBatch:
        """Append one output batch after revalidating its live worker lease."""
        lease = batch.lease
        if lease.scope_id != self._actor.scope_id:
            errors.forbidden("output lease belongs to another scope")
        async with self._session_factory() as session:
            async with session.begin():
                now = self._clock()
                run = await self._lock_output_run(session, batch, now=now)
                if batch.turn_id != run["turn_id"]:
                    errors.forbidden("output turn differs from its run")
                await self._verify_output_owner(
                    session,
                    lease=batch.lease,
                    attempt_id=batch.attempt_id,
                    item_id=batch.item_id,
                )
                session_row = await self._lock_session(session, lease.session_id)
                await self._verify_output_continuity(session, batch)
                envelopes: list[tuple[str, str, EventEnvelope]] = []
                for draft in batch.events:
                    key = output_producer_key(
                        batch.attempt_id, draft.payload.channel, draft.payload.delta_index
                    )
                    digest = output_digest(draft)
                    envelope = event_adapter.validate_python(
                        {
                            "schema_revision": 1,
                            "event_id": uuid4(),
                            "session_id": lease.session_id,
                            "session_seq": "1",
                            "turn_id": batch.turn_id,
                            "run_id": lease.run_id,
                            "parent_run_id": run["parent_run_id"],
                            "step_id": None,
                            "item_id": batch.item_id,
                            "model_attempt_id": batch.attempt_id,
                            "kind": draft.kind,
                            "occurred_at": draft.occurred_at,
                            "recorded_at": now,
                            "visibility": (
                                "restricted" if draft.kind == "analysis.delta" else "public"
                            ),
                            "trace_id": batch.trace_id,
                            "payload": draft.payload.model_dump(mode="python"),
                        }
                    )
                    envelopes.append((key, digest, envelope))
                entries = await self._append_locked(
                    session, envelopes, now=now, session=session_row
                )
                return CommittedBatch(
                    scope_id=self._actor.scope_id,
                    session_id=lease.session_id,
                    run_id=lease.run_id,
                    attempt_id=batch.attempt_id,
                    item_id=batch.item_id,
                    entries=entries,
                )

    async def seal_output(self, request: SealOutputRequest | ChildSealOutputRequest) -> FlushProof:
        """Derive an immutable producer seal from authoritative Journal rows."""
        lease = request.lease
        if int(request.sealed_revision) < 1:
            errors.invalid_input("sealed revision must be positive")
        if lease.scope_id != self._actor.scope_id:
            errors.forbidden("output seal lease belongs to another scope")
        async with self._session_factory() as session:
            async with session.begin():
                now = self._clock()
                await self._lock_output_run(session, request, now=now)
                await self._verify_output_owner(
                    session,
                    lease=lease,
                    attempt_id=request.attempt_id,
                    item_id=request.item_id,
                )
                session_row = await self._lock_session(session, lease.session_id)
                query = {
                    "scope_id": lease.scope_id,
                    "run_id": lease.run_id,
                    "attempt_id": request.attempt_id,
                    "item_id": request.item_id,
                    "proof_id": request.proof_id,
                }
                producer_row = (
                    (await session.execute(self._sql.seal_by_producer, query))
                    .mappings()
                    .one_or_none()
                )
                proof_row = (
                    (await session.execute(self._sql.seal_by_proof, query)).mappings().one_or_none()
                )
                if producer_row is not None or proof_row is not None:
                    if (
                        producer_row is not None
                        and proof_row is not None
                        and producer_row["id"] == proof_row["id"]
                        and int(producer_row["sealed_revision"]) == int(request.sealed_revision)
                    ):
                        return self._stored_flush_proof(dict(producer_row))
                    errors.invalid_input(
                        "output seal identity was already used with another producer or payload"
                    )
                rows = await self._output_rows(
                    session,
                    lease.session_id,
                    request.attempt_id,
                    request.item_id,
                )
                if not rows:
                    errors.invalid_input("cannot seal an output producer without events")
                proof = self.derive_flush_proof(
                    rows,
                    durable_seq=int(session_row["event_seq"]),
                    proof_id=request.proof_id,
                    scope_id=lease.scope_id,
                    session_id=lease.session_id,
                    run_id=lease.run_id,
                    attempt_id=request.attempt_id,
                    item_id=request.item_id,
                )
                payload = self._seal_payload(proof, int(request.sealed_revision))
                await session.execute(
                    self._sql.seal_insert,
                    {"id": uuid4(), **payload, "sealed_at": now},
                )
                return proof

    async def append_state(self, batch: StateEventBatch) -> StateEventReceipt:
        """Standalone state append for service transactions.

        State-changing UoWs must call :meth:`append_state_in_transaction` with their own
        session so state, Event and Outbox have one commit boundary.
        """
        async with self._session_factory() as session:
            async with session.begin():
                return await self.append_state_in_transaction(session, batch)

    async def append_state_in_transaction(
        self, session: AsyncSession, batch: StateEventBatch
    ) -> StateEventReceipt:
        if not session.in_transaction():
            errors.invalid_input("state events require an active caller-owned transaction")
        actor = (
            batch.authority.actor
            if isinstance(batch.authority, ServiceEventAuthority)
            else batch.authority.guard.actor
        )
        if actor != self._actor:
            errors.forbidden("state event actor is not the authenticated actor")
        now = self._clock()
        await self._verify_state_authority(session, batch, now=now)
        session_row = await self._lock_session(
            session,
            batch.session_id,
            allow_deleted=isinstance(batch.authority, ServiceEventAuthority)
            and batch.authority.purpose in {"recovery", "approval"},
        )
        envelopes: list[tuple[str, str, EventEnvelope]] = []
        next_placeholder = int(session_row["event_seq"]) + 1
        for offset, draft in enumerate(batch.events):
            envelope = materialize_state_event(
                draft,
                event_id=uuid4(),
                session_id=batch.session_id,
                session_seq=str(next_placeholder + offset),
                recorded_at=now,
                trace_id=batch.trace_id,
            )
            envelopes.append((draft.producer_key, state_event_digest(draft), envelope))
        entries = await self._append_locked(session, envelopes, now=now, session=session_row)
        return StateEventReceipt(
            scope_id=self._actor.scope_id,
            session_id=batch.session_id,
            entries=entries,
        )

    async def _lock_output_run(
        self,
        session: AsyncSession,
        request: ProducerBatch | ChildProducerBatch | SealOutputRequest | ChildSealOutputRequest,
        *,
        now: datetime,
    ) -> dict[str, Any]:
        if isinstance(request, (ChildProducerBatch, ChildSealOutputRequest)):
            if request.guard.actor != self._actor:
                errors.forbidden("child output actor changed")
            return await require_child(session, self._schema, request.guard, now=now)
        lease = request.lease
        facts.verify_lease(lease, now=now)
        slot = await locks.lock_slot(session, self._locks, lease.scope_id, lease.session_id)
        facts.verify_slot(slot, lease)
        run = dict(
            facts.verify_run(
                await locks.lock_run(session, self._locks, lease.scope_id, lease.run_id),
                self._actor,
                lease,
                now=now,
            )
        )
        await revalidate_authority(session, self._locks, self._actor, run=run, now=now)
        facts.verify_work(
            await locks.lock_work_item(session, self._locks, lease.scope_id, lease.work_id),
            lease,
            now=now,
        )
        return run

    async def _verify_output_owner(
        self,
        session: AsyncSession,
        *,
        lease: LeaseToken | ChildLeaseToken,
        attempt_id: UUID,
        item_id: UUID,
    ) -> None:
        params = {"scope_id": self._actor.scope_id}
        attempt = (
            (await session.execute(self._sql.attempt, {**params, "id": attempt_id}))
            .mappings()
            .one_or_none()
        )
        item = (
            (await session.execute(self._sql.item, {**params, "id": item_id}))
            .mappings()
            .one_or_none()
        )
        if attempt is None or attempt["run_id"] != lease.run_id:
            errors.invalid_input("model attempt does not belong to the output run")
        if item is None or item["run_id"] != lease.run_id or item["session_id"] != lease.session_id:
            errors.invalid_input("timeline item does not belong to the output run/session")

    async def _output_rows(
        self, session: AsyncSession, session_id: UUID, attempt_id: UUID, item_id: UUID
    ) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in (
                await session.execute(
                    self._sql.output_events,
                    {
                        "scope_id": self._actor.scope_id,
                        "session_id": session_id,
                        "attempt_id": attempt_id,
                        "item_id": item_id,
                    },
                )
            ).mappings()
        ]

    async def _verify_output_continuity(
        self, session: AsyncSession, batch: ProducerBatch | ChildProducerBatch
    ) -> None:
        """Reject cross-transaction gaps while allowing exact producer replays.

        The caller holds the Session row lock, which serializes every writer that can append
        to this session. Persisted channel watermarks therefore remain stable until commit.
        """
        rows = await self._output_rows(
            session, batch.lease.session_id, batch.attempt_id, batch.item_id
        )
        persisted_keys: set[str] = set()
        last_index = {"content": -1, "analysis": -1}
        item_revision: str | None = None
        for row in rows:
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            channel = str(payload["channel"])
            index = int(payload["deltaIndex"])
            if channel not in last_index or index != last_index[channel] + 1:
                errors.version_conflict("persisted output channel contains a delta gap")
            persisted_revision = str(payload["itemRevision"])
            if item_revision is not None and persisted_revision != item_revision:
                errors.version_conflict("persisted output changes item revision")
            item_revision = persisted_revision
            last_index[channel] = index
            persisted_keys.add(str(row["producer_key"]))

        for draft in batch.events:
            payload = draft.payload
            if item_revision is not None and payload.item_revision != item_revision:
                errors.invalid_input("output batch changes the persisted item revision")
            item_revision = payload.item_revision
            key = output_producer_key(batch.attempt_id, payload.channel, payload.delta_index)
            if key in persisted_keys:
                continue
            index = int(payload.delta_index)
            if index != last_index[payload.channel] + 1:
                errors.invalid_input("output delta does not continue the persisted channel")
            last_index[payload.channel] = index

        seal = (
            (
                await session.execute(
                    self._sql.seal_by_producer,
                    {
                        "scope_id": batch.lease.scope_id,
                        "run_id": batch.lease.run_id,
                        "attempt_id": batch.attempt_id,
                        "item_id": batch.item_id,
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        if seal is not None:
            new_keys = {
                output_producer_key(
                    batch.attempt_id, draft.payload.channel, draft.payload.delta_index
                )
                for draft in batch.events
            } - persisted_keys
            if new_keys:
                errors.version_conflict("sealed output producer cannot accept new deltas")

    @staticmethod
    def derive_flush_proof(
        rows: list[dict[str, Any]],
        *,
        durable_seq: int,
        proof_id: UUID,
        scope_id: str,
        session_id: UUID,
        run_id: UUID,
        attempt_id: UUID,
        item_id: UUID,
    ) -> FlushProof:
        texts = {"content": bytearray(), "analysis": bytearray()}
        heads: dict[str, tuple[int, int] | None] = {"content": None, "analysis": None}
        item_revision: str | None = None
        for row in rows:
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            channel = str(payload["channel"])
            index = int(payload["deltaIndex"])
            previous = heads[channel]
            if index != (0 if previous is None else previous[0] + 1):
                errors.version_conflict("cannot seal an output channel with a delta gap")
            revision = str(payload["itemRevision"])
            if item_revision is not None and revision != item_revision:
                errors.version_conflict("cannot seal output with mixed item revisions")
            item_revision = revision
            texts[channel].extend(str(payload["text"]).encode("utf-8"))
            heads[channel] = (index, int(row["session_seq"]))

        flushed: list[FlushedChannel] = []
        channel_names: tuple[Literal["content"], Literal["analysis"]] = (
            "content",
            "analysis",
        )
        for channel in channel_names:
            head = heads[channel]
            flushed.append(
                FlushedChannel(
                    channel=channel,
                    last_delta_index=None if head is None else str(head[0]),
                    last_session_seq=None if head is None else str(head[1]),
                    digest=hashlib.sha256(texts[channel]).hexdigest(),
                )
            )
        channels = tuple(flushed)
        return FlushProof(
            proof_id=proof_id,
            scope_id=scope_id,
            session_id=session_id,
            run_id=run_id,
            attempt_id=attempt_id,
            item_id=item_id,
            durable_seq=str(durable_seq),
            channels=channels,
        )

    @staticmethod
    def _seal_payload(proof: FlushProof, sealed_revision: int) -> dict[str, Any]:
        channels = {channel.channel: channel for channel in proof.channels}
        content = channels["content"]
        analysis = channels["analysis"]
        return {
            "scope_id": proof.scope_id,
            "session_id": proof.session_id,
            "run_id": proof.run_id,
            "model_attempt_id": proof.attempt_id,
            "item_id": proof.item_id,
            "proof_id": proof.proof_id,
            "content_digest": content.digest,
            "analysis_digest": analysis.digest,
            "content_last_delta_index": (
                None if content.last_delta_index is None else int(content.last_delta_index)
            ),
            "content_last_session_seq": (
                None if content.last_session_seq is None else int(content.last_session_seq)
            ),
            "analysis_last_delta_index": (
                None if analysis.last_delta_index is None else int(analysis.last_delta_index)
            ),
            "analysis_last_session_seq": (
                None if analysis.last_session_seq is None else int(analysis.last_session_seq)
            ),
            "durable_seq": int(proof.durable_seq),
            "sealed_revision": sealed_revision,
        }

    @staticmethod
    def _stored_flush_proof(row: Mapping[str, Any]) -> FlushProof:
        channel_names: tuple[Literal["content"], Literal["analysis"]] = (
            "content",
            "analysis",
        )
        channels = tuple(
            FlushedChannel(
                channel=channel,
                last_delta_index=(
                    None
                    if row[f"{channel}_last_delta_index"] is None
                    else str(row[f"{channel}_last_delta_index"])
                ),
                last_session_seq=(
                    None
                    if row[f"{channel}_last_session_seq"] is None
                    else str(row[f"{channel}_last_session_seq"])
                ),
                digest=str(row[f"{channel}_digest"]),
            )
            for channel in channel_names
        )
        return FlushProof(
            proof_id=row["proof_id"],
            scope_id=row["scope_id"],
            session_id=row["session_id"],
            run_id=row["run_id"],
            attempt_id=row["model_attempt_id"],
            item_id=row["item_id"],
            durable_seq=str(row["durable_seq"]),
            channels=channels,
        )

    async def _verify_state_authority(
        self, session: AsyncSession, batch: StateEventBatch, *, now: datetime
    ) -> None:
        authority = batch.authority
        if isinstance(authority, MainEventAuthority):
            main_guard = authority.guard
            lease = main_guard.lease
            facts.verify_lease(lease, now=now)
            slot = await locks.lock_slot(session, self._locks, lease.scope_id, lease.session_id)
            facts.verify_slot(slot, lease)
            run = dict(
                facts.verify_run(
                    await locks.lock_run(session, self._locks, lease.scope_id, lease.run_id),
                    main_guard.actor,
                    lease,
                    now=now,
                )
            )
            if int(run["state_version"]) != int(main_guard.expected_state_version):
                errors.version_conflict("state event run version changed")
            await revalidate_authority(session, self._locks, main_guard.actor, run=run, now=now)
            facts.verify_work(
                await locks.lock_work_item(session, self._locks, lease.scope_id, lease.work_id),
                lease,
                now=now,
            )
            return
        if isinstance(authority, ServiceEventAuthority):
            session_row = await self._lock_session(
                session,
                batch.session_id,
                allow_deleted=authority.purpose in {"recovery", "approval"},
            )
            if int(session_row["auth_revision"]) != int(authority.actor.auth_revision):
                errors.forbidden("session authorization revision changed")
            if authority.purpose == "approval":
                source = authority.source
                try:
                    approval_id = UUID(source.resource_id)
                except ValueError:
                    errors.invalid_input("approval event requires a UUID approval identity")
                row = (
                    (
                        await session.execute(
                            scoped(
                                "SELECT a.run_id,a.status,a.revision,a.expires_at,"
                                "r.session_id,r.turn_id,r.parent_run_id "
                                "FROM {t}approval a "
                                "JOIN {t}run r ON r.scope_id=a.scope_id AND r.id=a.run_id "
                                "WHERE a.scope_id=:scope AND a.id=:id",
                                self._schema,
                            ),
                            {"scope": authority.actor.scope_id, "id": approval_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if (
                    row is None
                    or row["session_id"] != batch.session_id
                    or any(
                        event.run_id != row["run_id"]
                        or event.turn_id != row["turn_id"]
                        or event.parent_run_id != row["parent_run_id"]
                        or event.payload.model_dump().get("status") != row["status"]
                        or event.payload.model_dump().get("expires_at") != row["expires_at"]
                        or source.resource_version != str(row["revision"])
                        for event in batch.events
                    )
                ):
                    errors.version_conflict("approval event differs from its committed decision")
            return
        if isinstance(authority, ChildEventAuthority):
            if authority.guard.actor != self._actor:
                errors.forbidden("child state actor changed")
            await require_child(session, self._schema, authority.guard, now=now, allow_control=True)
            return
        errors.invalid_input("unknown state event authority")

    async def _lock_session(
        self, session: AsyncSession, session_id: UUID, *, allow_deleted: bool = False
    ) -> dict[str, Any]:
        row = (
            (
                await session.execute(
                    self._sql.session,
                    {"scope_id": self._actor.scope_id, "session_id": session_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            errors.invalid_input("journal session does not exist")
        result = dict(row)
        if result["deleted_at"] is not None and not allow_deleted:
            errors.version_conflict("deleted session cannot accept events")
        return result

    async def _append_locked(
        self,
        db: AsyncSession,
        envelopes: list[tuple[str, str, EventEnvelope]],
        *,
        now: datetime,
        session: Mapping[str, Any] | None = None,
    ) -> tuple[CommittedEntry, ...]:
        session_id = envelopes[0][2].session_id
        session_row = (
            dict(session) if session is not None else await self._lock_session(db, session_id)
        )
        existing: dict[str, Mapping[str, Any]] = {}
        new: list[tuple[str, str, EventEnvelope]] = []
        for key, digest, event in envelopes:
            row = (
                (
                    await db.execute(
                        self._sql.event_by_key,
                        {"scope_id": self._actor.scope_id, "producer_key": key},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                new.append((key, digest, event))
                continue
            row_data = dict(row)
            if not _same_event(row_data, event, digest):
                errors.invalid_input("producer key was already used with another event payload")
            existing[key] = row_data

        next_seq = int(session_row["event_seq"])
        created: dict[str, tuple[UUID, int]] = {}
        for key, digest, event in new:
            next_seq += 1
            payload = _event_payload(event)
            event_id = event.event_id
            await db.execute(
                self._sql.event_insert,
                {
                    **payload,
                    "scope_id": self._actor.scope_id,
                    "session_seq": next_seq,
                    "producer_key": key,
                    "producer_digest": digest,
                    "now": now,
                },
            )
            await db.execute(
                self._sql.outbox_insert,
                {
                    "id": uuid4(),
                    "scope_id": self._actor.scope_id,
                    "event_id": event_id,
                    "now": now,
                },
            )
            created[key] = (event_id, next_seq)
        if new:
            await db.execute(
                self._sql.session_update,
                {
                    "scope_id": self._actor.scope_id,
                    "session_id": session_id,
                    "event_seq": next_seq,
                    "now": now,
                },
            )

        entries: list[CommittedEntry] = []
        for key, digest, _event in envelopes:
            if key in existing:
                existing_row = existing[key]
                entries.append(
                    CommittedEntry(
                        producer_key=key,
                        content_digest=digest,
                        event_id=existing_row["id"],
                        session_seq=str(existing_row["session_seq"]),
                        disposition="reused",
                    )
                )
            else:
                event_id, seq = created[key]
                entries.append(
                    CommittedEntry(
                        producer_key=key,
                        content_digest=digest,
                        event_id=event_id,
                        session_seq=str(seq),
                        disposition="created",
                    )
                )
        seqs = [int(entry.session_seq) for entry in entries]
        if seqs != sorted(seqs):
            errors.invalid_input("replayed producer batch no longer has monotonic journal order")
        return tuple(entries)


async def owner_read_authorizer(
    actor: ActorContext, session: Mapping[str, Any], restricted: bool
) -> bool:
    """Conservative default; hosts may inject ACL-aware authorization."""
    return (
        session["owner_actor_id"] == actor.actor_id
        and int(session["auth_revision"]) == int(actor.auth_revision)
        and session["deleted_at"] is None
    )


class SqlJournalReader:
    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        authorizer: ReadAuthorizer = owner_read_authorizer,
    ) -> None:
        self._schema = require_schema(schema)
        self._session_factory = async_sessionmaker(engine, expire_on_commit=False)
        self._sql = _statements(self._schema)
        self._authorizer = authorizer

    async def read_public(
        self, actor: ActorContext, request: StreamRequest, *, limit: int = 256
    ) -> JournalPage | ResyncFrame:
        return await self._read(actor, request, include_restricted=False, limit=limit)

    async def read_authorized(
        self, actor: ActorContext, request: StreamRequest, *, limit: int = 256
    ) -> JournalPage | ResyncFrame:
        return await self._read(actor, request, include_restricted=True, limit=limit)

    async def _read(
        self,
        actor: ActorContext,
        request: StreamRequest,
        *,
        include_restricted: bool,
        limit: int,
    ) -> JournalPage | ResyncFrame:
        if not 1 <= limit <= 1024:
            errors.invalid_input("journal replay limit must be between 1 and 1024")
        async with self._session_factory() as db:
            # Watermark, retention boundary and page must share one MVCC snapshot.
            # Under READ COMMITTED a concurrent append between these SELECTs can
            # manufacture a gap/count mismatch and spuriously force client resync.
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            session_row = (
                (
                    await db.execute(
                        self._sql.session_read,
                        {"scope_id": actor.scope_id, "session_id": request.session_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if session_row is None:
                errors.invalid_input("journal session does not exist")
            session = dict(session_row)
            if not await self._authorizer(actor, session, include_restricted):
                return self._resync(request.session_id, "authorization_changed")
            after = int(request.after_seq)
            watermark = int(session["event_seq"])
            if after > watermark:
                return self._resync(request.session_id, "projection_changed")
            first = (
                await db.execute(
                    self._sql.first_seq,
                    {"scope_id": actor.scope_id, "session_id": request.session_id},
                )
            ).scalar_one()
            if first is not None and after + 1 < int(first):
                return self._resync(request.session_id, "cursor_expired")
            rows = [
                dict(row)
                for row in (
                    await db.execute(
                        self._sql.replay,
                        {
                            "scope_id": actor.scope_id,
                            "session_id": request.session_id,
                            "after_seq": after,
                            "limit": limit,
                        },
                    )
                ).mappings()
            ]
            expected_count = min(limit, max(0, watermark - after))
            if len(rows) != expected_count:
                return self._resync(request.session_id, "projection_changed")
            if rows:
                expected = list(range(after + 1, after + 1 + len(rows)))
                actual = [int(row["session_seq"]) for row in rows]
                if actual != expected:
                    return self._resync(request.session_id, "projection_changed")
                cursor = actual[-1]
            else:
                cursor = after
            events = tuple(
                _row_event(row)
                for row in rows
                if include_restricted or row["visibility"] == "public"
            )
            return JournalPage(
                session_id=request.session_id,
                after_seq=str(after),
                cursor=str(cursor),
                auth_revision=str(session["auth_revision"]),
                events=events,
            )

    @staticmethod
    def _resync(
        session_id: UUID,
        reason: Literal["cursor_expired", "authorization_changed", "projection_changed"],
    ) -> ResyncFrame:
        return ResyncFrame(
            kind="resync_required",
            reason=reason,
            snapshot_url=f"/api/runtime/sessions/{session_id}/snapshot",
        )


class SqlOutboxStore:
    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        scope_id: str,
        *,
        clock: Clock = utcnow,
    ) -> None:
        self._scope_id = scope_id
        self._clock = clock
        self._session_factory = async_sessionmaker(engine, expire_on_commit=False)
        self._sql = _statements(require_schema(schema))

    async def claim(self, request: OutboxClaimRequest) -> OutboxClaim | None:
        now = self._clock()
        token = uuid4()
        lease_until = now + timedelta(seconds=request.lease_seconds)
        async with self._session_factory() as db:
            async with db.begin():
                rows = list(
                    (
                        await db.execute(
                            self._sql.claim_outbox,
                            {
                                "scope_id": self._scope_id,
                                "now": now,
                                "token": token,
                                "lease_until": lease_until,
                                "limit": request.limit,
                            },
                        )
                    ).mappings()
                )
        if not rows:
            return None
        return OutboxClaim(
            claim_token=token,
            lease_until=lease_until,
            notices=tuple(
                OutboxNotice(
                    scope_id=self._scope_id,
                    session_id=row["session_id"],
                    event_id=row["event_id"],
                    session_seq=str(row["session_seq"]),
                )
                for row in rows
            ),
        )

    async def delivered(self, delivery: OutboxDelivery) -> int:
        return await self._finish(delivery, delay_seconds=None)

    async def retry(self, delivery: OutboxDelivery, *, delay_seconds: int) -> int:
        if not 0 <= delay_seconds <= 86_400:
            errors.invalid_input("outbox retry delay is outside the supported range")
        return await self._finish(delivery, delay_seconds=delay_seconds)

    async def _finish(self, delivery: OutboxDelivery, *, delay_seconds: int | None) -> int:
        now = self._clock()
        params = {
            "scope_id": self._scope_id,
            "token": delivery.claim_token,
            "event_ids": list(delivery.event_ids),
            "now": now,
        }
        statement = self._sql.deliver_outbox
        if delay_seconds is not None:
            statement = self._sql.retry_outbox
            params["available_at"] = now + timedelta(seconds=delay_seconds)
        async with self._session_factory() as db:
            async with db.begin():
                result = await db.execute(statement, params)
                return int(getattr(result, "rowcount", 0) or 0)


class OutboxPublisher:
    """One bounded at-least-once publication cycle; scheduling belongs to DS-P02-04."""

    def __init__(self, store: OutboxStore, sink: NotificationSink) -> None:
        self._store = store
        self._sink = sink

    async def publish_once(self, request: OutboxClaimRequest) -> int:
        claim = await self._store.claim(request)
        if claim is None:
            return 0
        delivery = OutboxDelivery(
            claim_token=claim.claim_token,
            event_ids=tuple(notice.event_id for notice in claim.notices),
        )
        try:
            for notice in claim.notices:
                await self._sink.publish(notice)
        except Exception:
            await self._store.retry(delivery, delay_seconds=1)
            raise
        delivered = await self._store.delivered(delivery)
        if delivered != len(delivery.event_ids):
            raise runtime_failure(
                "FENCED",
                category="concurrency",
                public_message="The outbox claim expired before delivery was acknowledged.",
            )
        return delivered


__all__ = [
    "NotificationSink",
    "OutboxStore",
    "OutboxPublisher",
    "SqlJournal",
    "SqlJournalReader",
    "SqlOutboxStore",
    "owner_read_authorizer",
]

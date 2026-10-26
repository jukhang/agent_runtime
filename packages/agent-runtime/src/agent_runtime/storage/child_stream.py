"""Per-attempt child output, durable answers and immutable publication."""

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.decisions import Respond
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.contracts.ownership import ChildCommitGuard
from agent_runtime.contracts.payloads import ChannelDigest, ChannelIndex, ItemCompleted, ItemStarted
from agent_runtime.contracts.provider import AnalysisDelta, ContentDelta, ProviderEvent
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.state_events import (
    ChildEventAuthority,
    ItemCompletedDraft,
    ItemStartedDraft,
    StateEventBatch,
)
from agent_runtime.storage.child_authority import require_child
from agent_runtime.storage.child_output import ChildSealOutputRequest
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.model_events import SqlModelEventSink
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlChildStream:
    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        journal: SqlJournal,
        guard: Callable[[], ChildCommitGuard],
    ) -> None:
        self._schema = require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._journal, self._guard = journal, guard
        self._payloads = BlobContextPayloadStore(SqlContextBlobBackend(engine, schema))
        self._sinks: dict[UUID, SqlModelEventSink] = {}

    async def save(self, scope_id: str, run_id: str, ref: str, text: str) -> None:
        guard = self._guard()
        payload: dict[str, object] = {"runId": run_id, "text": text}
        digest = content_digest(payload)
        if (
            scope_id != guard.actor.scope_id
            or run_id != str(guard.lease.run_id)
            or ref != f"answer-{digest}"
        ):
            errors.forbidden("child answer identity changed")
        async with self._sessions() as db, db.begin():
            await require_child(db, self._schema, guard, now=datetime.now(UTC))
        await self._payloads.put(scope_id, payload, digest)

    async def answer_text(self, ref: str) -> str:
        guard = self._guard()
        if not ref.startswith("answer-"):
            errors.invalid_input("child answer reference is not content-addressed")
        digest = ref.removeprefix("answer-")
        blob_ref = f"context-{hashlib.sha256(guard.actor.scope_id.encode()).hexdigest()}-{digest}"
        payload = await self._payloads.get(guard.actor.scope_id, blob_ref)
        if (
            not isinstance(payload, dict)
            or payload.get("runId") != str(guard.lease.run_id)
            or not isinstance(payload.get("text"), str)
            or content_digest(payload) != digest
        ):
            errors.version_conflict("child answer blob differs from its reference")
        return str(payload["text"])

    async def record(self, request: ProviderRequest, event: ProviderEvent) -> None:
        if not isinstance(event, (ContentDelta, AnalysisDelta)):
            return
        guard = self._guard()
        if (request.scope_id, request.session_id, request.run_id) != (
            guard.lease.scope_id,
            guard.lease.session_id,
            guard.lease.run_id,
        ) or event.attempt_id != request.attempt_id:
            errors.forbidden("child stream request changed owner")
        if request.attempt_id not in self._sinks:
            item_id = uuid5(NAMESPACE_URL, f"child-output:{request.attempt_id}")
            async with self._sessions() as db, db.begin():
                row = await require_child(db, self._schema, guard, now=datetime.now(UTC))
                await db.execute(
                    scoped(
                        "INSERT INTO {t}timeline_item (id,scope_id,session_id,run_id,item_type,"
                        "content_ref,status,final_revision,source_seq) "
                        "VALUES (:id,:scope,:session,:run,'message','pending-child-output',"
                        "'streaming',1,0)",
                        self._schema,
                    ),
                    {
                        "id": item_id,
                        "scope": request.scope_id,
                        "session": request.session_id,
                        "run": request.run_id,
                    },
                )
                receipt = await self._journal.append_state_in_transaction(
                    db,
                    StateEventBatch(
                        authority=ChildEventAuthority(kind="child", guard=guard),
                        session_id=request.session_id,
                        trace_id=str(request.attempt_id),
                        events=(
                            ItemStartedDraft(
                                kind="item.started",
                                producer_key=f"child-item:{item_id}",
                                occurred_at=datetime.now(UTC),
                                turn_id=row["turn_id"],
                                run_id=request.run_id,
                                parent_run_id=row["parent_run_id"],
                                step_id=None,
                                item_id=item_id,
                                model_attempt_id=request.attempt_id,
                                payload=ItemStarted(
                                    item_type="message",
                                    role="assistant",
                                    channels=("content", "analysis"),
                                    item_revision="1",
                                ),
                            ),
                        ),
                    ),
                )
                await db.execute(
                    scoped(
                        "UPDATE {t}timeline_item SET source_seq=:seq WHERE id=:id", self._schema
                    ),
                    {"seq": int(receipt.entries[0].session_seq), "id": item_id},
                )
            self._sinks[request.attempt_id] = SqlModelEventSink(
                self._journal,
                lease=lambda: self._guard().lease,
                child_guard=self._guard,
                turn_id=row["turn_id"],
                attempt_id=request.attempt_id,
                item_id=item_id,
                item_revision="1",
                trace_id=str(request.attempt_id),
            )
        await self._sinks[request.attempt_id].record(request, event)

    async def publish(self, frame: DecisionFrame) -> ResourceRef | None:
        guard = self._guard()
        item_id = uuid5(NAMESPACE_URL, f"child-output:{frame.attempt_id}")
        async with self._sessions() as db, db.begin():
            await require_child(db, self._schema, guard, now=datetime.now(UTC))
            item = (
                (
                    await db.execute(
                        scoped(
                            "SELECT i.status,i.content_ref FROM {t}timeline_item i "
                            "JOIN {t}model_attempt m ON m.scope_id=i.scope_id "
                            "AND m.run_id=i.run_id WHERE i.scope_id=:scope "
                            "AND i.run_id=:run AND i.id=:item AND m.id=:attempt "
                            "AND m.status='completed' AND m.context_snapshot_id=:snapshot",
                            self._schema,
                        ),
                        {
                            "scope": guard.actor.scope_id,
                            "run": guard.lease.run_id,
                            "item": item_id,
                            "attempt": frame.attempt_id,
                            "snapshot": frame.context.snapshot_id,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if item is None:
                if isinstance(frame.action, Respond):
                    errors.version_conflict("child answer has no completed output producer")
                return None
            text = (
                await db.execute(
                    scoped(
                        "SELECT COALESCE(string_agg(payload->>'text','' ORDER BY session_seq),'') "
                        "FROM {t}event WHERE scope_id=:scope AND run_id=:run AND item_id=:item "
                        "AND model_attempt_id=:attempt AND kind='assistant.delta'",
                        self._schema,
                    ),
                    {
                        "scope": guard.actor.scope_id,
                        "run": guard.lease.run_id,
                        "item": item_id,
                        "attempt": frame.attempt_id,
                    },
                )
            ).scalar_one()
        digest = content_digest({"runId": str(guard.lease.run_id), "text": text})
        if (
            isinstance(frame.action, Respond)
            and frame.action.payload.answer_request_ref != f"answer-{digest}"
        ):
            errors.version_conflict("child answer differs from its streamed content")
        ref = await self._payloads.put(
            guard.actor.scope_id, {"runId": str(guard.lease.run_id), "text": text}, digest
        )
        proof = await self._journal.seal_output(
            ChildSealOutputRequest(
                guard=self._guard(),
                attempt_id=frame.attempt_id,
                item_id=item_id,
                proof_id=uuid5(NAMESPACE_URL, f"child-seal:{frame.attempt_id}"),
                sealed_revision="1",
            )
        )
        content_proof = next(
            (channel for channel in proof.channels if channel.channel == "content"), None
        )
        if (
            content_proof is not None
            and hashlib.sha256(text.encode()).hexdigest() != content_proof.digest
        ):
            errors.version_conflict("child answer seal differs from content")
        if isinstance(frame.action, Respond) and content_proof is None:
            errors.version_conflict("child answer has no sealed content channel")
        async with self._sessions() as db, db.begin():
            guard = self._guard()
            row = await require_child(db, self._schema, guard, now=datetime.now(UTC))
            completed = (
                (
                    await db.execute(
                        scoped(
                            "SELECT status,content_ref FROM {t}timeline_item "
                            "WHERE scope_id=:scope AND id=:id",
                            self._schema,
                        ),
                        {"scope": guard.actor.scope_id, "id": item_id},
                    )
                )
                .mappings()
                .one()
            )
            if completed["status"] == "completed":
                if completed["content_ref"] != ref:
                    errors.version_conflict("published child content changed")
                return ResourceRef(
                    resource_type="timeline_item",
                    resource_id=str(item_id),
                    resource_version="1",
                    scope=ResourceScope(scope_id=guard.actor.scope_id),
                )
            await self._journal.append_state_in_transaction(
                db,
                StateEventBatch(
                    authority=ChildEventAuthority(kind="child", guard=guard),
                    session_id=guard.lease.session_id,
                    trace_id=str(frame.attempt_id),
                    events=(
                        ItemCompletedDraft(
                            kind="item.completed",
                            producer_key=f"child-item-completed:{item_id}",
                            occurred_at=datetime.now(UTC),
                            turn_id=row["turn_id"],
                            run_id=row["id"],
                            parent_run_id=row["parent_run_id"],
                            step_id=None,
                            item_id=item_id,
                            model_attempt_id=frame.attempt_id,
                            payload=ItemCompleted(
                                item_revision="1",
                                status="completed",
                                channel_digests=tuple(
                                    ChannelDigest(channel=c.channel, digest=c.digest)
                                    for c in proof.channels
                                ),
                                last_delta_indexes=tuple(
                                    ChannelIndex(
                                        channel=c.channel, last_delta_index=c.last_delta_index
                                    )
                                    for c in proof.channels
                                ),
                            ),
                        ),
                    ),
                ),
            )
            await db.execute(
                scoped(
                    "UPDATE {t}timeline_item SET content_ref=:ref,status='completed',"
                    "updated_at=CURRENT_TIMESTAMP WHERE scope_id=:scope AND id=:id",
                    self._schema,
                ),
                {"ref": ref, "scope": guard.actor.scope_id, "id": item_id},
            )
        return ResourceRef(
            resource_type="timeline_item",
            resource_id=str(item_id),
            resource_version="1",
            scope=ResourceScope(scope_id=guard.actor.scope_id),
        )


__all__ = ["SqlChildStream"]

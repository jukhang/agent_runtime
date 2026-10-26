"""Journal-backed root output, answer blobs, and idempotent publication."""

from __future__ import annotations

import hashlib
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.decisions import Respond
from agent_runtime.contracts.journal import SealOutputRequest
from agent_runtime.contracts.loop import RunState
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.contracts.payloads import ChannelDigest, ChannelIndex, ItemCompleted, ItemStarted
from agent_runtime.contracts.provider import AnalysisDelta, ContentDelta, ProviderEvent
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.state_events import (
    ItemCompletedDraft,
    ItemStartedDraft,
    MainEventAuthority,
    StateEventBatch,
)
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.transitions import StateWriteGuard
from agent_runtime.storage.answer_commit import AnswerPublication
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.main_run_loader import LoadedMainRun
from agent_runtime.storage.model_events import SqlModelEventSink
from agent_runtime.storage.uow import errors, facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.worker import LeaseHandle

if TYPE_CHECKING:
    from agent_runtime.storage.main_worker import MainOutputs


class SqlMainStream:
    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        loaded: LoadedMainRun,
        lease: LeaseHandle,
    ) -> None:
        self._schema = require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._locks = locks.build_lock_statements(self._schema)
        self._actor, self._loaded, self._lease = actor, loaded, lease
        self._journal = SqlJournal(engine, schema, actor)
        self._payloads = BlobContextPayloadStore(SqlContextBlobBackend(engine, schema))
        self._sinks: dict[UUID, SqlModelEventSink] = {}
        self._pending: dict[UUID, list[ProviderEvent]] = {}
        self._pending_bytes: dict[UUID, int] = {}
        self._requests: dict[UUID, ProviderRequest] = {}
        self._last_flush: dict[UUID, float] = {}
        self._published_channels: dict[UUID, set[str]] = {}

    async def _require_main(self, db: AsyncSession) -> dict[str, object]:
        lease = self._lease.current()
        now = datetime.now(UTC)
        facts.verify_lease(lease, now=now)
        slot = await locks.lock_slot(db, self._locks, lease.scope_id, lease.session_id)
        facts.verify_slot(slot, lease)
        run = dict(
            facts.verify_run(
                await locks.lock_run(db, self._locks, lease.scope_id, lease.run_id),
                self._actor,
                lease,
                now=now,
            )
        )
        await revalidate_authority(db, self._locks, self._actor, run=run, now=now)
        work = facts.verify_work(
            await locks.lock_work_item(db, self._locks, lease.scope_id, lease.work_id),
            lease,
            now=now,
        )
        if (
            work["kind"] != "advance_run"
            or run["parent_run_id"] is not None
            or run["status"] != "running"
            or run["desired_control"] is not None
        ):
            errors.control_pending("root output no longer has active authority")
        return run

    def _authority(self, run: dict[str, object]) -> MainEventAuthority:
        return MainEventAuthority(
            kind="main",
            guard=StateWriteGuard(
                actor=self._actor,
                lease=self._lease.current(),
                expected_state_version=str(run["state_version"]),
                status=RunStatus.RUNNING,
                desired_control=None,
                control_command_id=None,
                cancel_version=str(run["cancel_version"]),
            ),
        )

    async def save(self, scope_id: str, run_id: str, ref: str, text: str) -> None:
        payload: dict[str, object] = {"runId": run_id, "text": text}
        digest = content_digest(payload)
        lease = self._lease.current()
        if (
            scope_id != self._actor.scope_id
            or run_id != str(lease.run_id)
            or ref != f"answer-{digest}"
        ):
            errors.forbidden("root answer identity changed")
        async with self._sessions() as db, db.begin():
            await self._require_main(db)
        await self._payloads.put(scope_id, payload, digest)

    async def record(self, request: ProviderRequest, event: ProviderEvent) -> None:
        lease = self._lease.current()
        if (request.scope_id, request.session_id, request.run_id) != (
            lease.scope_id,
            lease.session_id,
            lease.run_id,
        ) or event.attempt_id != request.attempt_id:
            errors.forbidden("root stream request changed owner")
        if not isinstance(event, (ContentDelta, AnalysisDelta)):
            await self._flush(request.attempt_id)
            return
        if request.attempt_id not in self._sinks:
            async with self._sessions() as db, db.begin():
                run = await self._require_main(db)
                item_id = await self._item_id(db, request.attempt_id)
                await db.execute(
                    scoped(
                        "INSERT INTO {t}timeline_item (id,scope_id,session_id,run_id,"
                        "item_type,content_ref,status,final_revision,source_seq) "
                        "VALUES (:id,:scope,:session,:run,'message','pending-main-output',"
                        "'streaming',1,0)",
                        self._schema,
                    ),
                    {
                        "id": item_id,
                        "scope": lease.scope_id,
                        "session": lease.session_id,
                        "run": lease.run_id,
                    },
                )
                receipt = await self._journal.append_state_in_transaction(
                    db,
                    StateEventBatch(
                        authority=self._authority(run),
                        session_id=lease.session_id,
                        trace_id=str(request.attempt_id),
                        events=(
                            ItemStartedDraft(
                                kind="item.started",
                                producer_key=f"main-item:{item_id}",
                                occurred_at=datetime.now(UTC),
                                turn_id=self._loaded.turn_id,
                                run_id=lease.run_id,
                                parent_run_id=None,
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
                        "UPDATE {t}timeline_item SET source_seq=:seq WHERE id=:id",
                        self._schema,
                    ),
                    {"seq": int(receipt.entries[0].session_seq), "id": item_id},
                )
            self._sinks[request.attempt_id] = SqlModelEventSink(
                self._journal,
                lease=self._lease.current,
                turn_id=self._loaded.turn_id,
                attempt_id=request.attempt_id,
                item_id=item_id,
                item_revision="1",
                trace_id=str(request.attempt_id),
            )
            self._requests[request.attempt_id] = request
        identity = request.attempt_id
        size = len(event.model_dump_json(by_alias=True).encode())
        if self._pending_bytes.get(identity, 0) + size > 16384:
            await self._flush(identity)
        pending = self._pending.setdefault(identity, [])
        pending.append(event)
        self._pending_bytes[identity] = self._pending_bytes.get(identity, 0) + size
        channel = "content" if isinstance(event, ContentDelta) else "analysis"
        if (
            channel not in self._published_channels.get(identity, set())
            or len(pending) >= 32
            or self._pending_bytes[identity] >= 16384
            or time.monotonic() - self._last_flush.get(identity, 0) >= 0.05
        ):
            await self._flush(identity)

    async def _flush(self, identity: UUID) -> None:
        pending = self._pending.get(identity)
        if not pending:
            return
        await self._sinks[identity].record_many(self._requests[identity], tuple(pending))
        channels = self._published_channels.setdefault(identity, set())
        for event in pending:
            channels.add("content" if isinstance(event, ContentDelta) else "analysis")
        pending.clear()
        self._pending_bytes[identity] = 0
        self._last_flush[identity] = time.monotonic()

    async def publish(self, state: RunState, frame: DecisionFrame) -> AnswerPublication:
        if not isinstance(frame.action, Respond):
            errors.invalid_input("root answer publication requires Respond")
        if (state.scope_id, state.session_id, state.run_id) != (
            self._actor.scope_id,
            self._lease.current().session_id,
            self._lease.current().run_id,
        ):
            errors.forbidden("root answer state changed owner")
        ids = await self._finish(frame, answer_required=True)
        if not ids:
            errors.version_conflict("root answer has no completed output")
        return AnswerPublication(ids)

    async def finalize_tool(self, frame: DecisionFrame) -> None:
        await self._finish(frame, answer_required=False)

    async def _item_id(self, db: AsyncSession, attempt_id: UUID) -> UUID:
        """The first public output uses Admission's reserved identity; retries do not."""
        lease = self._lease.current()
        rows = (
            (
                await db.execute(
                    scoped(
                        "SELECT kind,item_id,model_attempt_id,payload FROM {t}event "
                        "WHERE scope_id=:scope AND run_id=:run "
                        "AND kind IN ('turn.accepted','item.started') ORDER BY session_seq",
                        self._schema,
                    ),
                    {"scope": lease.scope_id, "run": lease.run_id},
                )
            )
            .mappings()
            .all()
        )
        for row in rows:
            if row["kind"] == "item.started" and row["model_attempt_id"] == attempt_id:
                return UUID(str(row["item_id"]))
        if not any(row["kind"] == "item.started" for row in rows):
            for row in rows:
                if row["kind"] == "turn.accepted":
                    return UUID(str(row["payload"]["assistantItemId"]))
        return uuid5(NAMESPACE_URL, f"main-output:{attempt_id}")

    async def _finish(self, frame: DecisionFrame, *, answer_required: bool) -> tuple[UUID, ...]:
        await self._flush(frame.attempt_id)
        lease = self._lease.current()
        async with self._sessions() as db, db.begin():
            await self._require_main(db)
            item_id = await self._item_id(db, frame.attempt_id)
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
                            "scope": lease.scope_id,
                            "run": lease.run_id,
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
                if answer_required:
                    errors.version_conflict("root answer has no completed output producer")
                return ()
            text = (
                await db.execute(
                    scoped(
                        "SELECT COALESCE(string_agg(payload->>'text','' ORDER BY session_seq),'') "
                        "FROM {t}event WHERE scope_id=:scope AND run_id=:run AND item_id=:item "
                        "AND model_attempt_id=:attempt AND kind='assistant.delta'",
                        self._schema,
                    ),
                    {
                        "scope": lease.scope_id,
                        "run": lease.run_id,
                        "item": item_id,
                        "attempt": frame.attempt_id,
                    },
                )
            ).scalar_one()
        digest = content_digest({"runId": str(lease.run_id), "text": text})
        if answer_required:
            if not isinstance(frame.action, Respond):
                errors.invalid_input("root answer publication requires Respond")
            if frame.action.payload.answer_request_ref != f"answer-{digest}":
                errors.version_conflict("root answer differs from streamed content")
        ref = await self._payloads.put(
            lease.scope_id, {"runId": str(lease.run_id), "text": text}, digest
        )
        proof = await self._journal.seal_output(
            SealOutputRequest(
                lease=self._lease.current(),
                attempt_id=frame.attempt_id,
                item_id=item_id,
                proof_id=uuid5(NAMESPACE_URL, f"main-seal:{frame.attempt_id}"),
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
            errors.version_conflict("root answer seal differs from content")
        if answer_required and content_proof is None:
            errors.version_conflict("root answer has no sealed content channel")
        async with self._sessions() as db, db.begin():
            run = await self._require_main(db)
            completed = (
                (
                    await db.execute(
                        scoped(
                            "SELECT status,content_ref FROM {t}timeline_item "
                            "WHERE scope_id=:scope AND id=:id",
                            self._schema,
                        ),
                        {"scope": lease.scope_id, "id": item_id},
                    )
                )
                .mappings()
                .one()
            )
            if completed["status"] == "completed":
                if completed["content_ref"] != ref:
                    errors.version_conflict("published root content changed")
                return (item_id,)
            await self._journal.append_state_in_transaction(
                db,
                StateEventBatch(
                    authority=self._authority(run),
                    session_id=lease.session_id,
                    trace_id=str(frame.attempt_id),
                    events=(
                        ItemCompletedDraft(
                            kind="item.completed",
                            producer_key=f"main-item-completed:{item_id}",
                            occurred_at=datetime.now(UTC),
                            turn_id=self._loaded.turn_id,
                            run_id=lease.run_id,
                            parent_run_id=None,
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
                {"ref": ref, "scope": lease.scope_id, "id": item_id},
            )
        return (item_id,)


class SqlMainOutputFactory:
    def __init__(self, engine: AsyncEngine, schema: str, actor: ActorContext) -> None:
        self._engine, self._schema, self._actor = engine, require_schema(schema), actor

    async def open(self, loaded: LoadedMainRun, lease: LeaseHandle) -> MainOutputs:
        from agent_runtime.storage.main_worker import MainOutputs

        stream = SqlMainStream(
            self._engine, self._schema, actor=self._actor, loaded=loaded, lease=lease
        )
        return MainOutputs(answers=stream, events=stream, publisher=stream, tool_output=stream)


__all__ = ["SqlMainOutputFactory", "SqlMainStream"]

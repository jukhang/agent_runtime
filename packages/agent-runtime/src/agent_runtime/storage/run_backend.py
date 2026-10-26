"""SQL read/control adapter for RunHandle. Execution remains in independent Workers."""

import asyncio
import json
import math
from collections.abc import AsyncIterator
from typing import Any, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.context_payloads import context_blob_key
from agent_runtime.contracts.canonical import canonical_json, content_digest
from agent_runtime.contracts.events import CursorFrame, ResyncFrame
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.journal import JournalPage
from agent_runtime.contracts.payloads import RunCancelled, RunCompleted, RunFailed
from agent_runtime.contracts.requests import CommandReceipt, ControlRequest, StreamRequest
from agent_runtime.contracts.results import RunResult, Usage, result_adapter
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.transitions import ResumeCommitRequest
from agent_runtime.contracts.views import Control, RunView, SnapshotEnvelope
from agent_runtime.handle import StreamFrame
from agent_runtime.projection import ProjectionState, SessionProjection, display_event
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.journal import (
    ReadAuthorizer,
    SqlJournalReader,
    _row_event,
    owner_read_authorizer,
)
from agent_runtime.storage.uow import SqlStateUnitOfWork, errors
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlRunBackend:
    """Host-owned engine, explicit read policy, bounded replay and cancellable waiting.

    Snapshots restore an authorized immutable projection plus retained Journal tail.
    Hosts must materialize before pruning and coordinate all execution/audit retention;
    missing prefixes fail closed rather than silently producing empty history.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        authorizer: ReadAuthorizer = owner_read_authorizer,
        poll_seconds: float = 0.25,
        max_snapshot_events: int = 100_000,
        max_snapshot_bytes: int = 8 * 1024 * 1024,
        include_analysis: bool = False,
    ) -> None:
        if (
            not math.isfinite(poll_seconds)
            or poll_seconds <= 0
            or max_snapshot_events < 1
            or max_snapshot_bytes < 1
        ):
            raise ValueError("Read bounds must be positive and finite")
        self._engine, self._schema = engine, require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._authorize, self._poll, self._maximum = authorizer, poll_seconds, max_snapshot_events
        self._maximum_bytes = max_snapshot_bytes
        self._analysis = include_analysis
        self._reader = SqlJournalReader(engine, schema, authorizer=authorizer)

    async def events(
        self, actor: ActorContext, request: StreamRequest
    ) -> AsyncIterator[StreamFrame]:
        current = request
        auth: str | None = None
        while True:
            page = await self.read_page(actor, current)
            if isinstance(page, ResyncFrame):
                yield page
                return
            if auth is not None and page.auth_revision != auth:
                yield ResyncFrame(
                    kind="resync_required",
                    reason="authorization_changed",
                    snapshot_url=f"/api/runtime/sessions/{request.session_id}/snapshot",
                )
                return
            auth = page.auth_revision
            for event in page.events:
                yield event
            if page.cursor != current.after_seq:
                yield CursorFrame(kind="cursor.advanced", cursor=page.cursor, auth_revision=auth)
            else:
                await asyncio.sleep(self._poll)
            current = current.model_copy(update={"after_seq": page.cursor, "last_event_id": None})

    async def read_page(
        self, actor: ActorContext, request: StreamRequest, *, limit: int = 256
    ) -> JournalPage | ResyncFrame:
        read = self._reader.read_authorized if self._analysis else self._reader.read_public
        page = await read(actor, request, limit=limit)
        if isinstance(page, ResyncFrame):
            return page
        events = tuple(
            projected
            for event in page.events
            if (projected := display_event(event, include_analysis=self._analysis)) is not None
        )
        return page.model_copy(update={"events": events})

    async def _load(
        self, actor: ActorContext, session_id: UUID, *, result_run: UUID | None = None
    ) -> tuple[SnapshotEnvelope, SessionProjection, list[dict[str, Any]]]:
        params = {"scope": actor.scope_id, "session": session_id}
        async with self._sessions() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            session = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}session WHERE scope_id=:scope AND id=:session",
                            self._schema,
                        ),
                        params,
                    )
                )
                .mappings()
                .one_or_none()
            )
            if session is None or not await self._authorize(actor, dict(session), self._analysis):
                errors.forbidden("Session is unavailable or authorization changed")
            watermark = int(session["event_seq"])
            projection = await self._restore_projection(db, actor, session_id, watermark)
            after = projection.cursor
            params["after"] = after
            if watermark - after > self._maximum:
                errors.version_conflict("Session requires the host's archived snapshot reader")
            bounds = (
                (
                    await db.execute(
                        scoped(
                            "SELECT count(*) AS n,min(session_seq) AS first,"
                            "max(session_seq) AS last,"
                            "COALESCE(sum(CASE WHEN visibility='public' OR "
                            "(:analysis AND kind='analysis.delta') THEN "
                            "octet_length(payload::text)+512 ELSE 0 END),0) AS bytes "
                            "FROM {t}event WHERE scope_id=:scope AND session_id=:session "
                            "AND session_seq>:after",
                            self._schema,
                        ),
                        {**params, "analysis": self._analysis},
                    )
                )
                .mappings()
                .one()
            )
            if int(bounds["n"]) != watermark - after or (
                watermark > after and (bounds["first"] != after + 1 or bounds["last"] != watermark)
            ):
                errors.version_conflict("Retained history requires a materialized snapshot rebuild")
            if int(bounds["bytes"]) > self._maximum_bytes:
                errors.version_conflict(
                    "Snapshot exceeds the configured byte budget; archive it first"
                )
            rows = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}event WHERE scope_id=:scope AND session_id=:session "
                            "AND session_seq>:after "
                            "AND (visibility='public' OR (:analysis AND kind='analysis.delta')) "
                            "ORDER BY session_seq LIMIT :limit",
                            self._schema,
                        ),
                        {**params, "limit": self._maximum, "analysis": self._analysis},
                    )
                )
                .mappings()
                .all()
            )
            try:
                for row in rows:
                    event = display_event(_row_event(dict(row)), include_analysis=self._analysis)
                    if event is not None:
                        projection.apply(event)
            except ValueError as exc:
                raise runtime_failure(
                    "AUDIT_UNAVAILABLE",
                    category="storage",
                    public_message=(
                        "Session history failed integrity verification; rebuild its snapshot."
                    ),
                ) from exc
            runs = [
                dict(row)
                for row in (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}run WHERE scope_id=:scope AND session_id=:session "
                            "ORDER BY created_at,id",
                            self._schema,
                        ),
                        params,
                    )
                ).mappings()
            ]
            if result_run is not None:
                attempts = (
                    (
                        await db.execute(
                            scoped(
                                "SELECT m.usage,m.usage_status FROM {t}model_attempt m "
                                "JOIN {t}run r ON r.scope_id=m.scope_id AND r.id=m.run_id "
                                "WHERE m.scope_id=:scope AND (r.id=:run OR r.root_run_id=:run)",
                                self._schema,
                            ),
                            {"scope": actor.scope_id, "run": result_run},
                        )
                    )
                    .mappings()
                    .all()
                )
                for run in runs:
                    if run["id"] == result_run:
                        run["result_usage"] = self._usage([dict(row) for row in attempts])
            snapshot = SnapshotEnvelope(
                schema_revision=1,
                projection_version=1,
                session_id=session_id,
                snapshot_seq=str(watermark),
                auth_revision=str(session["auth_revision"]),
                retained_from_seq=str(
                    (
                        await db.execute(
                            scoped(
                                "SELECT min(session_seq) FROM {t}event "
                                "WHERE scope_id=:scope AND session_id=:session",
                                self._schema,
                            ),
                            params,
                        )
                    ).scalar_one()
                    or watermark + 1
                ),
                items=tuple(projection.items.values()),
                runs=tuple(self._view(row, projection.terminals.get(row["id"])) for row in runs),
                plans=tuple(projection.plans.values()),
                resource_refs=tuple(
                    dict.fromkeys(
                        [ref for refs in projection.artifacts.values() for ref in refs]
                        + [ref for tool in projection.tools.values() for ref in tool.result_refs]
                        + [ref for item in projection.items.values() for ref in item.resource_refs]
                        + [
                            ref
                            for terminal in projection.terminals.values()
                            for ref in terminal.partial_result_refs
                        ]
                    )
                ),
                tools=tuple(projection.tools.values()),
                artifacts=tuple(projection.artifact_views.values()),
                input_requests=tuple(projection.input_requests.values()),
                approvals=tuple(projection.approvals.values()),
            )
            return snapshot, projection, runs

    async def _restore_projection(
        self, db: AsyncSession, actor: ActorContext, session_id: UUID, watermark: int
    ) -> SessionProjection:
        row = (
            (
                await db.execute(
                    scoped(
                        "SELECT applied_seq,state_ref,content_hash FROM {t}projection "
                        "WHERE scope_id=:scope AND session_id=:session",
                        self._schema,
                    ),
                    {"scope": actor.scope_id, "session": session_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            return SessionProjection(session_id, include_analysis=self._analysis)
        raw = await SqlContextBlobBackend(self._engine, self._schema).get_in_transaction(
            db, actor.scope_id, str(row["state_ref"])
        )
        if len(raw) > self._maximum_bytes:
            errors.version_conflict("Materialized snapshot exceeds the configured byte budget")
        try:
            cached = ProjectionState.model_validate_json(raw)
            snapshot = cached.snapshot
            if (
                content_digest(json.loads(raw)) != row["content_hash"]
                or snapshot.session_id != session_id
                or int(snapshot.snapshot_seq) != row["applied_seq"]
                or int(snapshot.snapshot_seq) > watermark
            ):
                raise ValueError("Materialized snapshot identity changed")
            if snapshot.auth_revision != actor.auth_revision:
                # Rebuild under current authorization if full history remains;
                # otherwise the normal retained-history guard fails closed.
                return SessionProjection(session_id, include_analysis=self._analysis)
            return SessionProjection.restore(cached, include_analysis=self._analysis)
        except ValueError as exc:
            raise runtime_failure(
                "AUDIT_UNAVAILABLE",
                category="storage",
                public_message="Materialized snapshot failed integrity checks.",
            ) from exc

    async def materialize(self, actor: ActorContext, session_id: UUID) -> SnapshotEnvelope:
        """Checkpoint both display channels; host maintenance must allow restricted reads.

        A monotonic prefix is valid even if new events arrive while it is being built.
        No retention is enabled or performed here; external effects must settle before GC.
        """
        reader = SqlRunBackend(
            self._engine,
            self._schema,
            authorizer=self._authorize,
            include_analysis=True,
            max_snapshot_events=self._maximum,
            max_snapshot_bytes=self._maximum_bytes,
            poll_seconds=self._poll,
        )
        snapshot, projection, _ = await reader._load(actor, session_id)
        state = ProjectionState(
            snapshot=snapshot,
            item_runs=projection.item_runs,
            terminal_events=tuple(projection.terminal_events.values()),
        )
        body = state.model_dump(mode="json", by_alias=True)
        digest = content_digest(body)
        raw = canonical_json(body).encode()
        if len(raw) > self._maximum_bytes:
            errors.version_conflict("Materialized snapshot exceeds the configured byte budget")
        ref = context_blob_key(actor.scope_id, digest)
        async with self._sessions() as db, db.begin():
            session = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}session WHERE scope_id=:scope "
                            "AND id=:session FOR UPDATE",
                            self._schema,
                        ),
                        {"scope": actor.scope_id, "session": session_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if session is None or not await self._authorize(actor, dict(session), True):
                errors.forbidden("Snapshot materialization authorization changed")
            await SqlContextBlobBackend(self._engine, self._schema).put_in_transaction(
                db, actor.scope_id, ref, raw
            )
            await db.execute(
                scoped(
                    "INSERT INTO {t}projection (scope_id,session_id,projection_revision,"
                    "applied_seq,"
                    "state_ref,content_hash) VALUES (:scope,:session,1,:seq,:ref,:hash) "
                    "ON CONFLICT (scope_id,session_id) DO UPDATE SET "
                    "projection_revision={t}projection.projection_revision+1,"
                    "applied_seq=EXCLUDED.applied_seq,state_ref=EXCLUDED.state_ref,"
                    "content_hash=EXCLUDED.content_hash,updated_at=CURRENT_TIMESTAMP "
                    "WHERE {t}projection.applied_seq<=EXCLUDED.applied_seq",
                    self._schema,
                ),
                {
                    "scope": actor.scope_id,
                    "session": session_id,
                    "seq": int(snapshot.snapshot_seq),
                    "ref": ref,
                    "hash": digest,
                },
            )
        return snapshot

    @staticmethod
    def _view(
        row: dict[str, Any], terminal: RunCompleted | RunFailed | RunCancelled | None = None
    ) -> RunView:
        status = RunStatus(row["status"])
        controls: tuple[Control, ...] = (
            ("pause", "cancel")
            if status in {RunStatus.QUEUED, RunStatus.RUNNING}
            else ("resume", "cancel")
            if status == RunStatus.PAUSED
            else ("cancel",)
            if status in {RunStatus.WAITING_TOOL, RunStatus.WAITING_USER}
            else ()
        )
        return RunView(
            id=row["id"],
            parent_run_id=row.get("parent_run_id"),
            status=status,
            state_version=str(row["state_version"]),
            phase=row["phase"],
            phase_started_at=row["phase_started_at"],
            allowed_controls=controls,
            failure=terminal.failure if terminal else None,
            public_message=terminal.public_message if terminal else None,
            next_steps=terminal.next_steps if terminal else (),
            partial_result_refs=terminal.partial_result_refs if terminal else (),
        )

    async def snapshot(self, actor: ActorContext, session_id: UUID) -> SnapshotEnvelope:
        return (await self._load(actor, session_id))[0]

    async def _authorized_run(
        self, actor: ActorContext, session_id: UUID, run_id: UUID, *, restricted: bool
    ) -> dict[str, Any]:
        async with self._sessions() as db:
            session = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}session WHERE scope_id=:scope AND id=:session",
                            self._schema,
                        ),
                        {"scope": actor.scope_id, "session": session_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if session is None or not await self._authorize(actor, dict(session), restricted):
                errors.forbidden("Session is unavailable or authorization changed")
            row = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}run WHERE scope_id=:scope AND id=:run "
                            "AND session_id=:session",
                            self._schema,
                        ),
                        {"scope": actor.scope_id, "session": session_id, "run": run_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                errors.forbidden("Run does not belong to this session")
            return dict(row)

    async def result(self, actor: ActorContext, session_id: UUID, run_id: UUID) -> RunResult:
        while True:
            # Waiting clients do not repeatedly replay a large Session transcript.
            current = await self._authorized_run(
                actor, session_id, run_id, restricted=self._analysis
            )
            if current["status"] not in {"completed", "failed", "cancelled"}:
                await asyncio.sleep(self._poll)
                continue
            _, projection, runs = await self._load(actor, session_id, result_run=run_id)
            row = next((run for run in runs if run["id"] == run_id), None)
            if row is None:
                errors.forbidden("Run does not belong to this session")
            if row["status"] in {"completed", "failed", "cancelled"}:
                return result_adapter.validate_python(
                    {
                        "run_id": run_id,
                        "status": row["status"],
                        "outcome": row["outcome"],
                        "failure": row["failure"],
                        "final_items": tuple(
                            projection.items[key] for key in projection.final_items.get(run_id, ())
                        ),
                        "artifact_refs": tuple(projection.artifacts.get(run_id, ())),
                        "partial_results": projection.partial_results.get(run_id, ()),
                        "usage": row["result_usage"],
                    }
                )
            await asyncio.sleep(self._poll)

    @staticmethod
    def _usage(attempts: list[dict[str, Any]]) -> Usage:
        reported = [Usage.model_validate(row["usage"]) for row in attempts if row["usage"]]
        if (
            len(reported) != len(attempts)
            or not reported
            or any(usage.status != "complete" for usage in reported)
        ):
            return Usage(
                status="unknown",
                reason="collection_failed",
                input_tokens=None,
                output_tokens=None,
                total_tokens=None,
            )
        return Usage(
            status="complete",
            reason="provider_reported",
            input_tokens=str(sum(int(usage.input_tokens or "0") for usage in reported)),
            output_tokens=str(sum(int(usage.output_tokens or "0") for usage in reported)),
            total_tokens=str(sum(int(usage.total_tokens or "0") for usage in reported)),
        )

    async def control(
        self, actor: ActorContext, session_id: UUID, request: ControlRequest
    ) -> CommandReceipt:
        # Controls must remain available when projection/history is damaged or large.
        row = await self._authorized_run(actor, session_id, request.run_id, restricted=False)
        state = SqlStateUnitOfWork(self._engine, self._schema, actor)
        if request.kind != "resume":
            return await state.record_control(request)
        identity = uuid5(
            NAMESPACE_URL,
            f"resume:{actor.scope_id}:{actor.actor_id}:{request.run_id}:{request.idempotency_key}",
        )
        resumed = await state.resume(
            ResumeCommitRequest(
                actor=actor,
                session_id=session_id,
                run_id=request.run_id,
                command_id=identity,
                expected_state_version=request.expected_version,
                expected_cancel_version=str(row["cancel_version"]),
                checkpoint_id=cast(UUID, row["checkpoint_id"]),
                idempotency_key=request.idempotency_key,
            )
        )
        return CommandReceipt(
            command_id=identity,
            run_id=request.run_id,
            status="applied",
            state_version=resumed.state_version,
            public_reason="Run resumed; queued for an independent Worker.",
        )


__all__ = ["SqlRunBackend"]

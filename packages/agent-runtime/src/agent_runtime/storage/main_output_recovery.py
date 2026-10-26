"""Seal abandoned root output under the new fenced execution owner."""

from datetime import UTC, datetime
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.journal import SealOutputRequest
from agent_runtime.contracts.payloads import ChannelDigest, ChannelIndex, ItemCompleted
from agent_runtime.contracts.scheduler import ClaimedWork
from agent_runtime.contracts.state import DesiredControl, RunStatus
from agent_runtime.contracts.state_events import (
    ItemCompletedDraft,
    MainEventAuthority,
    StateEventBatch,
)
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.transitions import StateWriteGuard
from agent_runtime.contracts.work_claims import ControlClaimedWork
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.uow import errors, facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.worker import LeaseHandle


class SqlMainOutputRecovery:
    def __init__(self, engine: AsyncEngine, schema: str, actor: ActorContext) -> None:
        self._schema = require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._locks = locks.build_lock_statements(schema)
        self._actor = actor
        self._journal = SqlJournal(engine, schema, actor)
        self._payloads = BlobContextPayloadStore(SqlContextBlobBackend(engine, schema))

    async def _verify(
        self, db: AsyncSession, work: ClaimedWork | ControlClaimedWork, handle: LeaseHandle
    ) -> tuple[UUID, str]:
        lease, now = handle.current(), datetime.now(UTC)
        facts.verify_lease(lease, now=now)
        facts.verify_slot(
            await locks.lock_slot(db, self._locks, lease.scope_id, lease.session_id), lease
        )
        run = facts.verify_run(
            await locks.lock_run(db, self._locks, lease.scope_id, lease.run_id),
            self._actor,
            lease,
            now=now,
        )
        await revalidate_authority(db, self._locks, self._actor, run=run, now=now)
        claimed = facts.verify_work(
            await locks.lock_work_item(db, self._locks, lease.scope_id, lease.work_id),
            lease,
            now=now,
        )
        facts.verify_guard(
            run,
            expected_state_version=int(work.state_version),
            status=work.status if isinstance(work, ControlClaimedWork) else "running",
            desired_control=work.desired_control if isinstance(work, ControlClaimedWork) else None,
            control_command_id=work.command_id if isinstance(work, ControlClaimedWork) else None,
            cancel_version=int(work.cancel_version)
            if isinstance(work, ControlClaimedWork)
            else int(run["cancel_version"]),
        )
        if run["parent_run_id"] is not None or claimed["kind"] not in {
            "advance_run",
            "reconcile_execution",
        }:
            errors.forbidden("output recovery requires a root execution claim")
        active = await db.execute(
            scoped(
                "SELECT id FROM {t}model_attempt WHERE scope_id=:scope AND run_id=:run "
                "AND status IN ('pending','running') LIMIT 1",
                self._schema,
            ),
            {"scope": lease.scope_id, "run": lease.run_id},
        )
        if active.scalar_one_or_none() is not None:
            raise runtime_failure(
                "CONTROL_PENDING",
                category="concurrency",
                retryable=True,
                public_message="Active model must stop before output recovery.",
            )
        return UUID(str(run["turn_id"])), str(run["cancel_version"])

    async def recover(
        self,
        work: ClaimedWork | ControlClaimedWork,
        handle: LeaseHandle,
        *,
        protected_attempt_id: UUID | None = None,
    ) -> None:
        lease = handle.current()
        params = {"scope": lease.scope_id, "run": lease.run_id}
        async with self._sessions() as db, db.begin():
            await self._verify(db, work, handle)
            rows = (
                (
                    await db.execute(
                        scoped(
                            "SELECT DISTINCT e.model_attempt_id,e.item_id FROM {t}event e "
                            "JOIN {t}timeline_item i ON i.scope_id=e.scope_id AND i.id=e.item_id "
                            "WHERE e.scope_id=:scope AND e.run_id=:run "
                            "AND e.kind IN ('assistant.delta','analysis.delta','item.started') "
                            "AND i.status='streaming'",
                            self._schema,
                        ),
                        params,
                    )
                )
                .mappings()
                .all()
            )
        for row in rows:
            if row["model_attempt_id"] == protected_attempt_id:
                continue
            await self._recover_item(work, handle, row["model_attempt_id"], row["item_id"])

    async def _recover_item(
        self,
        work: ClaimedWork | ControlClaimedWork,
        handle: LeaseHandle,
        attempt_id: UUID,
        item_id: UUID,
    ) -> None:
        lease = handle.current()
        params = {
            "scope": lease.scope_id,
            "run": lease.run_id,
            "item": item_id,
            "attempt": attempt_id,
        }
        async with self._sessions() as db, db.begin():
            await self._verify(db, work, handle)
            text = (
                await db.execute(
                    scoped(
                        "SELECT COALESCE(string_agg(payload->>'text','' ORDER BY session_seq),'') "
                        "FROM {t}event WHERE scope_id=:scope AND run_id=:run AND item_id=:item "
                        "AND model_attempt_id=:attempt AND kind='assistant.delta'",
                        self._schema,
                    ),
                    params,
                )
            ).scalar_one()
            delta_count = (
                await db.execute(
                    scoped(
                        "SELECT count(*) FROM {t}event WHERE scope_id=:scope AND run_id=:run "
                        "AND item_id=:item AND model_attempt_id=:attempt "
                        "AND kind IN ('assistant.delta','analysis.delta')",
                        self._schema,
                    ),
                    params,
                )
            ).scalar_one()
        payload: dict[str, object] = {"runId": str(lease.run_id), "text": text}
        ref = await self._payloads.put(lease.scope_id, payload, content_digest(payload))
        proof = (
            await self._journal.seal_output(
                SealOutputRequest(
                    lease=handle.current(),
                    attempt_id=attempt_id,
                    item_id=item_id,
                    proof_id=uuid5(NAMESPACE_URL, f"main-seal:{attempt_id}"),
                    sealed_revision="1",
                )
            )
            if delta_count
            else None
        )
        async with self._sessions() as db, db.begin():
            turn_id, cancel_version = await self._verify(db, work, handle)
            item = (
                (
                    await db.execute(
                        scoped(
                            "SELECT status FROM {t}timeline_item "
                            "WHERE scope_id=:scope AND id=:item",
                            self._schema,
                        ),
                        params,
                    )
                )
                .mappings()
                .one()
            )
            if item["status"] != "streaming":
                return
            await self._journal.append_state_in_transaction(
                db,
                StateEventBatch(
                    authority=MainEventAuthority(
                        kind="main",
                        guard=StateWriteGuard(
                            actor=self._actor,
                            lease=handle.current(),
                            expected_state_version=work.state_version,
                            status=RunStatus(work.status)
                            if isinstance(work, ControlClaimedWork)
                            else RunStatus.RUNNING,
                            desired_control=DesiredControl(work.desired_control)
                            if isinstance(work, ControlClaimedWork)
                            else None,
                            control_command_id=work.command_id
                            if isinstance(work, ControlClaimedWork)
                            else None,
                            cancel_version=cancel_version,
                        ),
                    ),
                    session_id=lease.session_id,
                    trace_id=str(attempt_id),
                    events=(
                        ItemCompletedDraft(
                            kind="item.completed",
                            producer_key=f"main-item-interrupted:{item_id}",
                            occurred_at=datetime.now(UTC),
                            turn_id=turn_id,
                            run_id=lease.run_id,
                            parent_run_id=None,
                            step_id=None,
                            item_id=item_id,
                            model_attempt_id=attempt_id,
                            payload=ItemCompleted(
                                item_revision="1",
                                status="cancelled",
                                channel_digests=tuple(
                                    ChannelDigest(channel=c.channel, digest=c.digest)
                                    for c in (() if proof is None else proof.channels)
                                ),
                                last_delta_indexes=tuple(
                                    ChannelIndex(
                                        channel=c.channel,
                                        last_delta_index=c.last_delta_index,
                                    )
                                    for c in (() if proof is None else proof.channels)
                                ),
                            ),
                        ),
                    ),
                ),
            )
            await db.execute(
                scoped(
                    "UPDATE {t}timeline_item SET status='cancelled',content_ref=:ref,"
                    "updated_at=CURRENT_TIMESTAMP WHERE scope_id=:scope AND id=:item",
                    self._schema,
                ),
                {**params, "ref": ref},
            )

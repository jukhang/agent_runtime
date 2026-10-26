"""Apply root controls only with durable output and stopped-execution evidence."""

from datetime import UTC, datetime
from typing import Literal, NoReturn

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.journal import FlushedChannel, FlushProof
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.results import CancelledResult, Usage
from agent_runtime.contracts.state import DesiredControl, RunStatus
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.transitions import (
    ExecutionResolution,
    NoProducersConfirmed,
    PauseCommitRequest,
    StateWriteGuard,
    TerminalCommitRequest,
    VerifiedEvidence,
)
from agent_runtime.contracts.work_claims import ControlClaimedWork
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.main_output_recovery import SqlMainOutputRecovery
from agent_runtime.storage.uow import SqlStateUnitOfWork, evidence, facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.worker import LeaseHandle


def _pending(detail: str) -> NoReturn:
    raise runtime_failure(
        "CONTROL_PENDING",
        category="concurrency",
        retryable=True,
        public_message=f"Control is waiting for a verified safe point ({detail}).",
    )


class SqlMainControlHandler:
    """No model invocation, no speculative sealing or fabricated process termination.

    External supervision and incomplete-stream recovery must finish before this handler
    can apply the command. The state UoW re-enumerates every fact at commit time.
    """

    def __init__(self, engine: AsyncEngine, schema: str, actor: ActorContext) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._schema = require_schema(schema)
        self._locks = locks.build_lock_statements(schema)
        self._evidence = evidence.build_evidence_statements(schema)
        self._actor = actor
        self._state = SqlStateUnitOfWork(engine, schema, actor)
        self._journal = SqlJournal(engine, schema, actor)
        self._outputs = SqlMainOutputRecovery(engine, schema, actor)

    async def __call__(self, work: ControlClaimedWork, handle: LeaseHandle) -> None:
        await self._outputs.recover(work, handle)
        lease = handle.current()
        now = datetime.now(UTC)
        async with self._sessions() as db, db.begin():
            facts.verify_lease(lease, now=now)
            facts.verify_slot(
                await locks.lock_slot(db, self._locks, lease.scope_id, lease.session_id), lease
            )
            row = facts.verify_run(
                await locks.lock_run(db, self._locks, lease.scope_id, lease.run_id),
                self._actor,
                lease,
                now=now,
            )
            await revalidate_authority(db, self._locks, self._actor, run=row, now=now)
            facts.verify_work(
                await locks.lock_work_item(db, self._locks, lease.scope_id, lease.work_id),
                lease,
                now=now,
            )
            facts.verify_guard(
                row,
                expected_state_version=int(work.state_version),
                status=work.status,
                desired_control=work.desired_control,
                control_command_id=work.command_id,
                cancel_version=int(work.cancel_version),
            )
            if row["checkpoint_id"] is None:
                _pending("control requires a recoverable checkpoint")
            parameters = {"scope": lease.scope_id, "run": lease.run_id}
            children = (
                await db.execute(
                    scoped(
                        "SELECT count(*) FROM {t}run WHERE scope_id=:scope AND root_run_id=:run "
                        "AND id<>:run AND status NOT IN ('completed','failed','cancelled')",
                        self._schema,
                    ),
                    parameters,
                )
            ).scalar_one()
            if work.desired_control == "cancel" and children:
                _pending("delegated tasks must stop before the root becomes terminal")
            if work.desired_control == "pause":
                producers = (
                    await db.execute(
                        scoped(
                            "SELECT EXISTS(SELECT 1 FROM {t}run c WHERE c.scope_id=:scope "
                            "AND c.root_run_id=:run AND c.id<>:run AND "
                            "(c.owner_id IS NOT NULL OR c.status='running')) "
                            "OR EXISTS(SELECT 1 FROM {t}execution e JOIN {t}run c "
                            "ON c.scope_id=e.scope_id AND c.id=e.run_id WHERE c.scope_id=:scope "
                            "AND c.root_run_id=:run AND c.id<>:run "
                            "AND e.status NOT IN ('completed','failed','cancelled'))",
                            self._schema,
                        ),
                        parameters,
                    )
                ).scalar_one()
                if producers:
                    _pending("delegated work must reach its bounded safe point before pause")
            active_models = (
                await db.execute(
                    scoped(
                        "SELECT count(*) FROM {t}model_attempt WHERE scope_id=:scope "
                        "AND run_id=:run AND status IN ('pending','running')",
                        self._schema,
                    ),
                    parameters,
                )
            ).scalar_one()
            if active_models:
                _pending("model attempt must stop before applying control")
            unsealed = (
                await db.execute(
                    scoped(
                        "SELECT count(*) FROM {t}event e WHERE e.scope_id=:scope "
                        "AND e.run_id=:run AND e.kind IN ('assistant.delta','analysis.delta') "
                        "AND NOT EXISTS (SELECT 1 FROM {t}output_seal s "
                        "WHERE s.scope_id=e.scope_id AND s.run_id=e.run_id "
                        "AND s.model_attempt_id=e.model_attempt_id AND s.item_id=e.item_id)",
                        self._schema,
                    ),
                    parameters,
                )
            ).scalar_one()
            if unsealed:
                _pending("output recovery must seal every interrupted producer")
            executions = await evidence.execution_facts(db, self._evidence, row)
            if any(evidence.state_bucket(str(item["status"])) != "stopped" for item in executions):
                _pending("external execution termination is not confirmed")
            resolutions = tuple(
                ExecutionResolution(
                    execution_ref=ResourceRef(
                        resource_type="execution",
                        resource_id=str(item["id"]),
                        resource_version=str(item["generation"]),
                        scope=ResourceScope(scope_id=lease.scope_id),
                    ),
                    generation=str(item["generation"]),
                    state="stopped",
                    evidence_ref=f"execution:{item['id']}:{item['generation']}",
                    supervision_work_id=None,
                )
                for item in executions
            )
            seq = (
                await db.execute(
                    scoped(
                        "SELECT event_seq FROM {t}session WHERE scope_id=:scope AND id=:session",
                        self._schema,
                    ),
                    {"scope": lease.scope_id, "session": lease.session_id},
                )
            ).scalar_one()
            seals = await evidence.producer_facts(db, self._evidence, row)
            channels: tuple[Literal["content", "analysis"], ...] = ("content", "analysis")
            proofs = tuple(
                FlushProof(
                    proof_id=item["proof_id"],
                    scope_id=lease.scope_id,
                    session_id=lease.session_id,
                    run_id=lease.run_id,
                    attempt_id=item["model_attempt_id"],
                    item_id=item["item_id"],
                    durable_seq=str(item["durable_seq"]),
                    channels=tuple(
                        FlushedChannel(
                            channel=channel,
                            digest=item[f"{channel}_digest"],
                            last_delta_index=(
                                None
                                if item[f"{channel}_last_delta_index"] is None
                                else str(item[f"{channel}_last_delta_index"])
                            ),
                            last_session_seq=(
                                None
                                if item[f"{channel}_last_session_seq"] is None
                                else str(item[f"{channel}_last_session_seq"])
                            ),
                        )
                        for channel in channels
                    ),
                )
                for item in seals
            )
            flush = (
                VerifiedEvidence(
                    kind="verified",
                    proofs=proofs,
                    enumerated_session_seq=str(max(int(seq), 1)),
                )
                if proofs
                else NoProducersConfirmed(
                    kind="no_producers",
                    run_id=lease.run_id,
                    enumerated_session_seq=str(max(int(seq), 1)),
                )
            )
            await evidence.verify_flush_evidence(db, self._evidence, row, flush)
            partial_items = (
                (
                    await db.execute(
                        scoped(
                            "SELECT i.id,i.final_revision FROM {t}timeline_item i "
                            "JOIN {t}output_seal s ON s.scope_id=i.scope_id AND s.item_id=i.id "
                            "WHERE i.scope_id=:scope AND i.run_id=:run "
                            "AND i.status IN ('completed','cancelled') "
                            "AND s.content_last_delta_index IS NOT NULL ORDER BY i.source_seq,i.id",
                            self._schema,
                        ),
                        parameters,
                    )
                )
                .mappings()
                .all()
            )
            partial_refs = tuple(
                ResourceRef(
                    resource_type="timeline_item",
                    resource_id=str(item["id"]),
                    resource_version=str(item["final_revision"]),
                    scope=ResourceScope(scope_id=lease.scope_id, session_id=str(lease.session_id)),
                )
                for item in partial_items
            )
            artifacts = (
                (
                    await db.execute(
                        scoped(
                            "SELECT payload->'resourceRef' AS ref FROM {t}event "
                            "WHERE scope_id=:scope AND run_id=:run AND kind='artifact.created' "
                            "ORDER BY session_seq",
                            self._schema,
                        ),
                        parameters,
                    )
                )
                .mappings()
                .all()
            )
            artifact_refs = tuple(ResourceRef.model_validate(item["ref"]) for item in artifacts)
            partial_refs = tuple(dict.fromkeys((*partial_refs, *artifact_refs)))
            checkpoint_id = row["checkpoint_id"]
        guard = StateWriteGuard(
            actor=self._actor,
            lease=handle.current(),
            expected_state_version=work.state_version,
            status=RunStatus(work.status),
            desired_control=DesiredControl(work.desired_control),
            control_command_id=work.command_id,
            cancel_version=work.cancel_version,
        )
        if work.desired_control == "pause":
            await self._state.commit_pause(
                PauseCommitRequest(
                    guard=guard,
                    checkpoint_id=checkpoint_id,
                    evidence=flush,
                    executions=resolutions,
                ),
                journal=self._journal,
            )
        else:
            await self._state.commit_terminal(
                TerminalCommitRequest(
                    guard=guard,
                    checkpoint_id=checkpoint_id,
                    evidence=flush,
                    executions=resolutions,
                    result=CancelledResult(
                        run_id=lease.run_id,
                        status="cancelled",
                        outcome=None,
                        failure=None,
                        final_items=(),
                        artifact_refs=artifact_refs,
                        partial_results=partial_refs,
                        usage=Usage(
                            status="unknown",
                            reason="collection_failed",
                            input_tokens=None,
                            output_tokens=None,
                            total_tokens=None,
                        ),
                    ),
                ),
                journal=self._journal,
            )

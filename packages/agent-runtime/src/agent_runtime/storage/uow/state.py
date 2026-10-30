"""``SqlStateUnitOfWork``: the only state write path (DS-P02-02 requirements 1-7).

Every public method opens exactly one database transaction with the fixed lock order from
:mod:`agent_runtime.storage.uow.locks`, re-reads the stored slot/run/grant/work/lease/
version facts, re-enumerates flush and execution evidence from the database (review §10.2
P1-1) and only then writes. The request DTOs supply expectations, never authority.

Transaction intent is split the same way the frozen ``StateStore`` protocol is:

* ``commit_advance`` / ``commit_pause`` / ``commit_cancel`` / ``commit_terminal``: guarded
  safe-point commits that carry a checkpoint and full flush/execution evidence (Q47); a
  pause keeps execution-supervision work alive, and a cancel keeps its convergence work
  claimable;
* ``resume``: authenticated control transaction that queues the run and creates exactly one
  work item owned by that command identity;
* ``apply_input``: atomic consumption of a contiguous, still-claimed mailbox batch;
* ``commit_domain``: same-database resource change under the run's transaction, with an
  atomically claimed operation identity.

Storage-side helpers (not part of the frozen port): ``record_control`` persists an accepted
control command **and** its worker-observable pending state (plus the cancel convergence
work), and ``record_output_seal`` is the journal writer's locked entry point for sealed
producer facts.

All statements are rendered with an explicit schema prefix (review §10.2 P1-3), so the
standard ``StorageConfig`` works without any ``search_path`` assumption.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.decisions import UpdatePlanPayload
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.journal import FlushProof
from agent_runtime.contracts.loop import AppliedInputs
from agent_runtime.contracts.messages import AgentEnvelope, message_adapter
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.requests import CommandReceipt, ControlRequest
from agent_runtime.contracts.results import RunResult
from agent_runtime.contracts.state import DesiredControl, RunStatus
from agent_runtime.contracts.transactions import (
    ActorContext,
    ApplyInputRequest,
    CommitGuard,
    DomainCommitRequest,
)
from agent_runtime.contracts.transitions import (
    AdvanceCommitRequest,
    CancelCommitRequest,
    ControlApplied,
    ControlOutcome,
    ControlRejected,
    PauseCommitRequest,
    ResumeCommitRequest,
    ResumedRun,
    StateWriteGuard,
    TerminalCommitRequest,
    UpdatedStateRef,
)
from agent_runtime.contracts.views import Control
from agent_runtime.storage.uow import errors, evidence, facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.domain import DomainRegistry
from agent_runtime.storage.uow.schema import require_schema, scoped

Clock = Callable[[], datetime]

if TYPE_CHECKING:
    from agent_runtime.storage.context_access import TransactionalResourceAccess
    from agent_runtime.storage.journal import SqlJournal

DEFAULT_RESUME_PRIORITY = 100
RESUME_PAYLOAD_REF = "resume"


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class _Guarded:
    run: dict[str, Any]
    work: dict[str, Any]
    now: datetime


def _json(value: object) -> str:
    return json.dumps(value, default=str)


def _loads(value: object) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    return dict(json.loads(str(value)))


def _request_digest(*parts: object) -> str:
    """Canonical digest of a request identity/payload (RFC 8785 JCS + sha256)."""
    return content_digest({"parts": [str(part) for part in parts]})


@dataclass(frozen=True, slots=True)
class _Statements:
    checkpoint: sa.TextClause
    session_seq: sa.TextClause
    slot_release: sa.TextClause
    work_finish_id: sa.TextClause
    run_advance: sa.TextClause
    run_state: sa.TextClause
    run_resume: sa.TextClause
    run_applied: sa.TextClause
    run_cancel: sa.TextClause
    run_pending_control: sa.TextClause
    command_insert: sa.TextClause
    command_applied: sa.TextClause
    command_rejected: sa.TextClause
    work_insert: sa.TextClause
    message_applied: sa.TextClause
    cursor_applied: sa.TextClause
    operation_claim: sa.TextClause
    operation_lock: sa.TextClause
    operation_commit: sa.TextClause


def build_statements(schema: str) -> _Statements:
    """Render every UoW statement for one explicit schema."""
    require_schema(schema)
    return _Statements(
        checkpoint=scoped(
            "SELECT id, run_id, scope_id, state_version, representation_revision, state_ref "
            "FROM {t}checkpoint WHERE scope_id = :scope_id AND id = :checkpoint_id",
            schema,
        ),
        session_seq=scoped(
            "SELECT COALESCE(MAX(session_seq), 0) FROM {t}event WHERE session_id = :sid",
            schema,
        ),
        slot_release=scoped(
            "UPDATE {t}session_slot SET owner_run_id = NULL, "
            "slot_version = slot_version + 1, updated_at = :now "
            "WHERE scope_id = :scope_id AND session_id = :session_id "
            "AND owner_run_id = :run_id",
            schema,
        ),
        work_finish_id=scoped(
            "UPDATE {t}work_item SET status = :status, owner_id = NULL, lease_until = NULL, "
            "lease_epoch = lease_epoch + 1, updated_at = :now "
            "WHERE scope_id = :scope_id AND id = :work_id",
            schema,
        ),
        run_advance=scoped(
            "UPDATE {t}run SET state_version = :state_version, "
            "checkpoint_id = :checkpoint_id, updated_at = :now "
            "WHERE scope_id = :scope_id AND id = :run_id",
            schema,
        ),
        run_state=scoped(
            "UPDATE {t}run SET status = :status, phase = :phase, "
            "state_version = :state_version, checkpoint_id = :checkpoint_id, "
            "outcome = :outcome, failure = :failure, desired_control = NULL, "
            "control_command_id = NULL, owner_id = NULL, lease_until = NULL, "
            "owner_epoch = owner_epoch + 1, updated_at = :now "
            "WHERE scope_id = :scope_id AND id = :run_id",
            schema,
        ),
        run_resume=scoped(
            "UPDATE {t}run SET status = 'queued', phase = 'queued', "
            "state_version = :state_version, checkpoint_id = :checkpoint_id, "
            "desired_control = NULL, control_command_id = NULL, owner_id = NULL, "
            "lease_until = NULL, owner_epoch = owner_epoch + 1, updated_at = :now "
            "WHERE scope_id = :scope_id AND id = :run_id",
            schema,
        ),
        run_applied=scoped(
            "UPDATE {t}run SET applied_input_version = :applied_seq, "
            "state_version = state_version + 1, updated_at = :now "
            "WHERE scope_id = :scope_id AND id = :run_id",
            schema,
        ),
        run_cancel=scoped(
            "UPDATE {t}run SET phase = 'cancelling', state_version = :state_version, "
            "owner_id = NULL, lease_until = NULL, owner_epoch = owner_epoch + 1, "
            "updated_at = :now WHERE scope_id = :scope_id AND id = :run_id",
            schema,
        ),
        run_pending_control=scoped(
            "UPDATE {t}run SET desired_control = :desired_control, "
            "control_command_id = :command_id, status = :status, "
            "phase = CASE WHEN :desired_control = 'cancel' THEN 'cancelling' ELSE phase END, "
            "phase_started_at = CASE WHEN :desired_control = 'cancel' THEN :now "
            "ELSE phase_started_at END, updated_at = :now "
            "WHERE scope_id = :scope_id AND id = :run_id",
            schema,
        ),
        command_insert=scoped(
            "INSERT INTO {t}command (id, scope_id, run_id, actor_id, request_key, "
            "request_hash, kind, status, expected_state_version) VALUES "
            "(:id, :scope_id, :run_id, :actor_id, :request_key, :request_hash, :kind, "
            "'accepted', :expected_state_version)",
            schema,
        ),
        command_applied=scoped(
            "UPDATE {t}command SET status = 'applied', result_ref = :result_ref, "
            "updated_at = :now WHERE scope_id = :scope_id AND id = :command_id",
            schema,
        ),
        command_rejected=scoped(
            "UPDATE {t}command SET status = 'rejected', failure = :failure, updated_at = :now "
            "WHERE scope_id = :scope_id AND id = :command_id",
            schema,
        ),
        work_insert=scoped(
            "INSERT INTO {t}work_item (id, scope_id, run_id, subject_ref, kind, dedup_key, "
            "status, payload_ref, wake_reason, priority, lease_epoch, attempt_count, "
            "available_at, deadline_at) VALUES "
            "(:id, :scope_id, :run_id, CAST(:subject_ref AS jsonb), 'advance_run', "
            ":dedup_key, 'pending', :payload_ref, 'resume', :priority, 0, 0, :now, :deadline)",
            schema,
        ),
        message_applied=scoped(
            "UPDATE {t}agent_message SET status = 'applied', claim_token = NULL, "
            "lease_until = NULL, applied_checkpoint_id = :checkpoint_id, updated_at = :now "
            "WHERE scope_id = :scope_id AND recipient_run_id = :run_id AND id = ANY(:message_ids)",
            schema,
        ),
        cursor_applied=scoped(
            "UPDATE {t}mailbox_cursor SET applied_seq = :applied_seq, updated_at = :now "
            "WHERE scope_id = :scope_id AND recipient_run_id = :run_id",
            schema,
        ),
        operation_claim=scoped(
            "INSERT INTO {t}operation (id, scope_id, task_id, idempotency_key, input_hash, "
            "status) VALUES (:id, :scope_id, :task_id, :key, :input_hash, 'prepared') "
            "ON CONFLICT (scope_id, task_id, idempotency_key) DO NOTHING",
            schema,
        ),
        operation_lock=scoped(
            "SELECT id, status, input_hash, result_ref FROM {t}operation "
            "WHERE scope_id = :scope_id AND task_id = :task_id AND idempotency_key = :key "
            "FOR UPDATE",
            schema,
        ),
        operation_commit=scoped(
            "UPDATE {t}operation SET status = 'committed', result_ref = :result_ref, "
            "updated_at = :now WHERE scope_id = :scope_id AND id = :operation_id",
            schema,
        ),
    )


class SqlStateUnitOfWork:
    """Standard PostgreSQL implementation of the frozen ``StateStore`` port."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        actor: ActorContext,
        *,
        clock: Clock = utcnow,
        registry: DomainRegistry | None = None,
    ) -> None:
        self._engine = engine
        self._schema = require_schema(schema)
        self._actor = actor
        self._clock = clock
        self._registry = registry or DomainRegistry.default()
        self._locks = locks.build_lock_statements(self._schema)
        self._sql = build_statements(self._schema)
        self._evidence = evidence.build_evidence_statements(self._schema)
        self._session_factory = async_sessionmaker(engine, expire_on_commit=False)

    @property
    def schema(self) -> str:
        return self._schema

    # ------------------------------------------------------------------ helpers

    def _assert_actor(self, actor: ActorContext) -> None:
        if actor != self._actor:
            errors.forbidden("request actor is not the authenticated actor")

    def _session(self) -> AsyncSession:
        return self._session_factory()

    async def _lock_guard(
        self, session: AsyncSession, guard: CommitGuard | StateWriteGuard
    ) -> _Guarded:
        self._assert_actor(guard.actor)
        lease = guard.lease
        now = self._clock()
        facts.verify_lease(lease, now=now)
        slot = await locks.lock_slot(session, self._locks, lease.scope_id, lease.session_id)
        facts.verify_slot(slot, lease)
        locked_run = await locks.lock_run(session, self._locks, lease.scope_id, lease.run_id)
        run = dict(facts.verify_run(locked_run, guard.actor, lease, now=now))
        # Per-run authority fence (Q46): the run row is already locked, and grant writers
        # must bump ``run.authority_revision`` while holding that same row.
        await revalidate_authority(session, self._locks, guard.actor, run=run, now=now)
        locked_work = await locks.lock_work_item(
            session, self._locks, lease.scope_id, lease.work_id
        )
        work = dict(facts.verify_work(locked_work, lease, now=now))
        return _Guarded(run=run, work=work, now=now)

    async def _verify_checkpoint(
        self,
        session: AsyncSession,
        run: Mapping[str, Any],
        checkpoint_id: UUID,
        *,
        maximum_state_version: int | None = None,
    ) -> dict[str, Any]:
        """Checkpoints are immutable inputs; the caller registers them in this database."""
        result = await session.execute(
            self._sql.checkpoint,
            {"scope_id": run["scope_id"], "checkpoint_id": checkpoint_id},
        )
        locked = result.mappings().one_or_none()
        if locked is None:
            errors.invalid_input("checkpoint does not exist in this scope")
        checkpoint = dict(locked)
        if checkpoint["run_id"] != run["id"]:
            errors.invalid_input("checkpoint belongs to another run")
        if maximum_state_version is not None:
            if int(checkpoint["state_version"]) != maximum_state_version:
                errors.version_conflict("checkpoint was cut from another state version")
        return checkpoint

    async def _session_seq(self, session: AsyncSession, session_id: UUID) -> int:
        """Current journal watermark.

        DS-P02-02 does not publish events (that is DS-P02-03); it reports the committed
        session sequence so a caller can never mistake a state commit for published events.
        """
        result = await session.execute(self._sql.session_seq, {"sid": session_id})
        return int(result.scalar_one())

    async def _release_slot(
        self, session: AsyncSession, run: Mapping[str, Any], now: datetime
    ) -> None:
        """Release the session slot **only while this run still owns it**.

        A cancel safe point releases the slot so the next turn can start; if another run has
        claimed the session in the meantime, the later terminal commit of the first run must
        not clear the new owner's slot (it would fence a live unrelated run).
        """
        await session.execute(
            self._sql.slot_release,
            {
                "now": now,
                "scope_id": run["scope_id"],
                "session_id": run["session_id"],
                "run_id": run["id"],
            },
        )

    async def _finish_work(
        self,
        session: AsyncSession,
        run: Mapping[str, Any],
        work_id: Any,
        *,
        status: str,
        now: datetime,
    ) -> None:
        """Finish exactly the claimed work item of this safe-point transaction."""
        await session.execute(
            self._locks.work_finish_id,
            {
                "status": status,
                "now": now,
                "scope_id": run["scope_id"],
                "work_id": work_id,
            },
        )

    async def _finish_advance_works(
        self,
        session: AsyncSession,
        run: Mapping[str, Any],
        *,
        status: str,
        now: datetime,
    ) -> int:
        """Cancel the run's live *advance* work without touching supervision work.

        ``reconcile_execution``/``recover_scope`` work must survive a pause, a cancel and a
        failed terminal so that external side effects are still converged (review §11.2
        P1-3/P1-5): the database terminal state is not proof that a process stopped.
        """
        live = await locks.lock_live_works(session, self._locks, run["scope_id"], run["id"])
        advance = [item for item in live if str(item["kind"]) == "advance_run"]
        if not advance:
            return 0
        await session.execute(
            self._locks.work_finish_kind,
            {
                "status": status,
                "now": now,
                "scope_id": run["scope_id"],
                "run_id": run["id"],
                "kind": "advance_run",
            },
        )
        return len(advance)

    async def _answered_control(
        self, session: AsyncSession, guard: StateWriteGuard
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]] | None:
        """Lock Slot -> Run -> Command and return an already-answered control command.

        Applying pause/cancel clears the worker owner and lease, so the original guard is
        intentionally stale afterwards: identity must be checked *before* the lease guard so
        a replay returns its recorded fact instead of ``FENCED`` (requirement 5).
        """
        if guard.control_command_id is None:
            return None
        lease = guard.lease
        await locks.lock_slot(session, self._locks, lease.scope_id, lease.session_id)
        run = await locks.lock_run(session, self._locks, lease.scope_id, lease.run_id)
        if run is None:
            return None
        command = await locks.lock_command(
            session, self._locks, lease.scope_id, guard.control_command_id
        )
        if command is None or command["run_id"] != run["id"]:
            return None
        if command["status"] == "accepted":
            return None
        return dict(run), dict(command)

    def _updated_ref(
        self,
        run: Mapping[str, Any],
        *,
        state_version: int,
        status: str,
        checkpoint_id: UUID,
        committed_seq: int,
    ) -> UpdatedStateRef:
        return UpdatedStateRef(
            scope_id=run["scope_id"],
            session_id=run["session_id"],
            run_id=run["id"],
            state_version=str(state_version),
            checkpoint_id=checkpoint_id,
            status=RunStatus(status),
            committed_seq=str(committed_seq),
        )

    async def _commit_state(
        self,
        session: AsyncSession,
        guarded: _Guarded,
        request: Any,
        *,
        status: str,
        phase: str,
        release: bool,
        outcome: str | None = None,
        failure: FailureEnvelope | None = None,
        control_kind: Control | None = None,
        journal: SqlJournal | None = None,
    ) -> UpdatedStateRef:
        run = guarded.run
        now = guarded.now
        expected = int(run["state_version"])
        state_version = expected + 1
        if run["desired_control"] is not None:
            from agent_runtime.storage.uow.control_checkpoints import transition_checkpoint

            checkpoint_id = await transition_checkpoint(
                session,
                self.schema,
                run,
                request.checkpoint_id,
                kind=str(run["desired_control"]),
                status=status,
                target_version=state_version,
            )
            request = request.model_copy(update={"checkpoint_id": checkpoint_id})
        await self._verify_checkpoint(
            session, run, request.checkpoint_id, maximum_state_version=state_version
        )
        if journal is not None:
            from agent_runtime.storage.uow.control_events import append_control_events

            if journal.schema != self.schema:
                errors.invalid_input("control Journal belongs to another schema")
            await append_control_events(session, journal, guarded, request, status=status)
        await session.execute(
            self._sql.run_state,
            {
                "status": status,
                "phase": phase,
                "state_version": state_version,
                "checkpoint_id": request.checkpoint_id,
                "outcome": outcome,
                "failure": None if failure is None else _json(failure.model_dump(mode="json")),
                "now": now,
                "scope_id": run["scope_id"],
                "run_id": run["id"],
            },
        )
        # Command, work items and slot converge in this same transaction (P1-2).
        if request.guard.control_command_id is not None:
            if control_kind is None:
                result_ref: dict[str, Any] = {
                    "runId": str(run["id"]),
                    "stateVersion": str(state_version),
                    "status": status,
                    "checkpointId": str(request.checkpoint_id),
                }
            else:
                # A control command records its full ``ControlApplied`` fact, so replaying
                # it (identity first, without a live lease) returns the same outcome.
                result_ref = ControlApplied(
                    outcome="applied",
                    command_id=request.guard.control_command_id,
                    run_id=run["id"],
                    kind=control_kind,
                    status=RunStatus(status),
                    state_version=str(state_version),
                    desired_control=None,
                    work_id=None,
                ).model_dump(mode="json")
            await session.execute(
                self._sql.command_applied,
                {
                    "result_ref": _json(result_ref),
                    "now": now,
                    "scope_id": run["scope_id"],
                    "command_id": request.guard.control_command_id,
                },
            )
        await self._finish_work(session, run, guarded.work["id"], status="completed", now=now)
        await self._finish_advance_works(session, run, status="cancelled", now=now)
        if status == "cancelled" and run["parent_run_id"] is None:
            await session.execute(
                scoped(
                    "UPDATE {t}turn SET outcome_run_id=:run,outcome=NULL,failure=NULL,"
                    "updated_at=:now WHERE scope_id=:scope AND primary_run_id=:run "
                    "AND outcome_run_id IS NULL",
                    self.schema,
                ),
                {"scope": run["scope_id"], "run": run["id"], "now": now},
            )
        if release:
            await self._release_slot(session, run, now)
        committed_seq = await self._session_seq(session, run["session_id"])
        return self._updated_ref(
            run,
            state_version=state_version,
            status=status,
            checkpoint_id=request.checkpoint_id,
            committed_seq=committed_seq,
        )

    async def _require_evidence(
        self,
        session: AsyncSession,
        run: Mapping[str, Any],
        request: (
            AdvanceCommitRequest | PauseCommitRequest | CancelCommitRequest | TerminalCommitRequest
        ),
        *,
        terminal: str | None = None,
    ) -> None:
        await evidence.verify_flush_evidence(session, self._evidence, run, request.evidence)
        await evidence.verify_executions(session, self._evidence, run, request.executions)
        if terminal is None:
            return
        for fact in await evidence.execution_facts(session, self._evidence, run):
            bucket = evidence.state_bucket(str(fact["status"]))
            if terminal == "cancelled" and bucket != "stopped":
                errors.version_conflict("cancellation requires confirmed stopped executions")
            if terminal == "completed" and bucket == "unknown":
                errors.version_conflict("completion requires known execution state")

    # ------------------------------------------------------- StateStore methods

    async def commit_advance(self, request: AdvanceCommitRequest) -> UpdatedStateRef:
        guard = request.guard
        async with self._session() as session:
            async with session.begin():
                guarded = await self._lock_guard(session, guard)
                run = guarded.run
                facts.verify_guard(
                    run,
                    expected_state_version=int(guard.expected_state_version),
                    status=str(guard.status),
                    desired_control=(
                        None if guard.desired_control is None else str(guard.desired_control)
                    ),
                    control_command_id=guard.control_command_id,
                    cancel_version=int(guard.cancel_version),
                )
                facts.require_no_pending_control(run, transition="advance")
                if run["status"] not in facts.ACTIVE_STATUSES:
                    errors.version_conflict("run is not advancing")
                expected = int(run["state_version"])
                await self._verify_checkpoint(
                    session, run, request.checkpoint_id, maximum_state_version=expected + 1
                )
                await self._require_evidence(session, run, request)
                await session.execute(
                    self._sql.run_advance,
                    {
                        "state_version": expected + 1,
                        "checkpoint_id": request.checkpoint_id,
                        "now": guarded.now,
                        "scope_id": run["scope_id"],
                        "run_id": run["id"],
                    },
                )
                await self._finish_work(
                    session, run, guarded.work["id"], status="completed", now=guarded.now
                )
                committed_seq = await self._session_seq(session, run["session_id"])
                return self._updated_ref(
                    run,
                    state_version=expected + 1,
                    status=run["status"],
                    checkpoint_id=request.checkpoint_id,
                    committed_seq=committed_seq,
                )

    async def commit_pause(
        self, request: PauseCommitRequest, *, journal: SqlJournal | None = None
    ) -> UpdatedStateRef:
        guard = request.guard
        async with self._session() as session:
            async with session.begin():
                answered = await self._answered_control(session, guard)
                if answered is not None:
                    recorded_run, command = answered
                    if command["status"] == "applied":
                        return self._updated_ref(
                            recorded_run,
                            state_version=int(recorded_run["state_version"]),
                            status=str(recorded_run["status"]),
                            checkpoint_id=recorded_run["checkpoint_id"],
                            committed_seq=await self._session_seq(
                                session, recorded_run["session_id"]
                            ),
                        )
                    raise AgentRuntimeError(
                        FailureEnvelope.model_validate(command["failure"] or {})
                    )
                guarded = await self._lock_guard(session, guard)
                run = guarded.run
                facts.verify_guard(
                    run,
                    expected_state_version=int(guard.expected_state_version),
                    status=str(guard.status),
                    desired_control=str(DesiredControl.PAUSE),
                    control_command_id=guard.control_command_id,
                    cancel_version=int(guard.cancel_version),
                )
                if guard.control_command_id is None:
                    errors.invalid_input("pause requires its persisted command identity")
                await self._require_evidence(session, run, request)
                return await self._commit_state(
                    session,
                    guarded,
                    request,
                    status=str(RunStatus.PAUSED),
                    phase="paused",
                    release=True,
                    control_kind="pause",
                    journal=journal,
                )

    async def commit_plan(
        self,
        *,
        guard: CommitGuard,
        checkpoint_id: UUID,
        plan: UpdatePlanPayload,
        revision_id: UUID,
        journal: SqlJournal,
        resources: TransactionalResourceAccess,
    ) -> UpdatedStateRef:
        from agent_runtime.storage.uow.plans import commit_plan

        return await commit_plan(
            self,
            guard=guard,
            checkpoint_id=checkpoint_id,
            plan=plan,
            revision_id=revision_id,
            journal=journal,
            resources=resources,
        )

    async def commit_answer(
        self,
        *,
        guard: CommitGuard,
        checkpoint_id: UUID,
        item_ids: tuple[UUID, ...],
        outcome: Literal["success", "partial"] | None,
        evidence_refs: tuple[ResourceRef, ...],
        journal: SqlJournal,
        resources: TransactionalResourceAccess,
    ) -> UpdatedStateRef:
        from agent_runtime.storage.uow.answers import commit_answer

        return await commit_answer(
            self,
            guard=guard,
            checkpoint_id=checkpoint_id,
            item_ids=item_ids,
            outcome=outcome,
            evidence_refs=evidence_refs,
            journal=journal,
            resources=resources,
        )

    async def commit_terminal(
        self, request: TerminalCommitRequest, *, journal: SqlJournal | None = None
    ) -> UpdatedStateRef:
        guard = request.guard
        result: RunResult = request.result
        async with self._session() as session:
            async with session.begin():
                guarded = await self._lock_guard(session, guard)
                run = guarded.run
                facts.verify_guard(
                    run,
                    expected_state_version=int(guard.expected_state_version),
                    status=str(guard.status),
                    desired_control=(
                        None if guard.desired_control is None else str(guard.desired_control)
                    ),
                    control_command_id=guard.control_command_id,
                    cancel_version=int(guard.cancel_version),
                )
                outcome = result.outcome if result.status == "completed" else None
                failure = result.failure if result.status == "failed" else None
                await self._require_evidence(session, run, request, terminal=result.status)
                return await self._commit_state(
                    session,
                    guarded,
                    request,
                    status=result.status,
                    phase="terminal",
                    release=True,
                    outcome=outcome,
                    failure=failure,
                    control_kind="cancel" if result.status == "cancelled" else None,
                    journal=journal,
                )

    async def resume(self, request: ResumeCommitRequest) -> ResumedRun:
        self._assert_actor(request.actor)
        async with self._session() as session:
            async with session.begin():
                now = self._clock()
                slot = await locks.lock_slot(
                    session, self._locks, request.actor.scope_id, request.session_id
                )
                if slot is None:
                    errors.fenced("session slot does not exist")
                run = await locks.lock_run(
                    session, self._locks, request.actor.scope_id, request.run_id
                )
                if run is None:
                    errors.fenced("run does not exist")
                if run["scope_id"] != request.actor.scope_id:
                    errors.forbidden("run belongs to another scope")
                if run["session_id"] != request.session_id:
                    errors.invalid_input("run and request session disagree")
                if facts.is_terminal(run["status"]):
                    errors.version_conflict("run is already terminal")
                await revalidate_authority(session, self._locks, request.actor, run=run, now=now)
                # Identity first: replaying an applied command returns its recorded fact.
                recorded = await locks.lock_command(
                    session, self._locks, request.actor.scope_id, request.command_id
                )
                if recorded is not None:
                    if recorded["kind"] != "resume":
                        errors.invalid_input("command identity belongs to another control kind")
                    if recorded["request_key"] != request.idempotency_key:
                        errors.invalid_input("command identity was used by another request key")
                    if recorded["status"] == "applied":
                        return self._recorded_resume(run, recorded)
                    if recorded["status"] == "rejected":
                        stored = recorded["failure"] or {}
                        raise AgentRuntimeError(FailureEnvelope.model_validate(stored))

                if int(run["state_version"]) != int(request.expected_state_version):
                    errors.version_conflict("state version changed")
                if int(run["cancel_version"]) != int(request.expected_cancel_version):
                    errors.version_conflict("cancel version changed")
                if run["status"] != str(RunStatus.PAUSED):
                    errors.version_conflict("resume requires a paused run")
                await self._verify_checkpoint(session, run, request.checkpoint_id)

                await session.execute(
                    self._sql.command_insert,
                    {
                        "id": request.command_id,
                        "scope_id": request.actor.scope_id,
                        "run_id": request.run_id,
                        "actor_id": request.actor.actor_id,
                        "request_key": request.idempotency_key,
                        "request_hash": _request_digest(
                            "resume", str(request.run_id), request.idempotency_key
                        ),
                        "kind": "resume",
                        "expected_state_version": int(request.expected_state_version),
                    },
                )
                expected = int(run["state_version"])
                state_version = expected + 1
                from agent_runtime.storage.uow.control_checkpoints import transition_checkpoint

                checkpoint_id = await transition_checkpoint(
                    session,
                    self.schema,
                    run,
                    request.checkpoint_id,
                    kind="resume",
                    status="queued",
                    target_version=state_version,
                )
                work_id = await self._resume_work(session, run, request.command_id, now)
                await session.execute(
                    self._sql.run_resume,
                    {
                        "state_version": state_version,
                        "checkpoint_id": checkpoint_id,
                        "now": now,
                        "scope_id": run["scope_id"],
                        "run_id": run["id"],
                    },
                )
                await session.execute(
                    self._sql.command_applied,
                    {
                        "result_ref": _json(
                            {"workId": str(work_id), "stateVersion": str(state_version)}
                        ),
                        "now": now,
                        "scope_id": run["scope_id"],
                        "command_id": request.command_id,
                    },
                )
                return ResumedRun(
                    scope_id=run["scope_id"],
                    session_id=run["session_id"],
                    run_id=run["id"],
                    command_id=request.command_id,
                    work_id=work_id,
                    state_version=str(state_version),
                    status="queued",
                    desired_control=None,
                    control_command_id=None,
                    wake_reason="resume",
                )

    def _recorded_resume(self, run: Mapping[str, Any], command: Mapping[str, Any]) -> ResumedRun:
        recorded = _loads(command["result_ref"])
        return ResumedRun(
            scope_id=run["scope_id"],
            session_id=run["session_id"],
            run_id=run["id"],
            command_id=command["id"],
            work_id=UUID(recorded["workId"]),
            state_version=str(recorded["stateVersion"]),
            status="queued",
            desired_control=None,
            control_command_id=None,
            wake_reason="resume",
        )

    async def _resume_work(
        self,
        session: AsyncSession,
        run: Mapping[str, Any],
        command_id: UUID,
        now: datetime,
    ) -> UUID:
        """Reuse only this command's live work; refuse another command's live work.

        The frozen spec forbids reusing another command's work item (review §10.2 P1-6):
        the database allows a single live ``advance_run`` per run, so a different command's
        live work is a stable refusal instead of a silent identity swap.
        """
        dedup_key = f"resume:{command_id}"
        live = await locks.lock_live_works(session, self._locks, run["scope_id"], run["id"])
        for item in live:
            if item["dedup_key"] == dedup_key:
                return UUID(str(item["id"]))
        if any(item["kind"] == "advance_run" for item in live):
            errors.version_conflict("another resume command owns the live advance work")
        work_id = uuid4()
        await session.execute(
            self._sql.work_insert,
            {
                "id": work_id,
                "scope_id": run["scope_id"],
                "run_id": run["id"],
                "subject_ref": _json({"kind": "resume", "commandId": str(command_id)}),
                "dedup_key": dedup_key,
                "payload_ref": RESUME_PAYLOAD_REF,
                "priority": DEFAULT_RESUME_PRIORITY,
                "now": now,
                "deadline": run["deadline_at"],
            },
        )
        return work_id

    async def apply_input(self, request: ApplyInputRequest) -> AppliedInputs:
        guard = request.guard
        async with self._session() as session:
            async with session.begin():
                guarded = await self._lock_guard(session, guard)
                run = guarded.run
                if int(run["state_version"]) != int(guard.expected_state_version):
                    errors.version_conflict("state version changed")
                now = guarded.now
                cursor = await locks.lock_mailbox_cursor(
                    session, self._locks, run["scope_id"], run["id"]
                )
                if cursor is None:
                    errors.invalid_input("mailbox cursor does not exist")
                if int(cursor["applied_seq"]) != int(request.expected_applied_seq):
                    errors.version_conflict("applied mailbox watermark changed")
                message_ids = tuple(entry.message_id for entry in request.inputs)
                rows = await locks.lock_messages(
                    session, self._locks, run["scope_id"], run["id"], message_ids
                )
                if len(rows) != len(message_ids):
                    errors.invalid_input("claimed batch is missing a message")
                by_id = {row["id"]: row for row in rows}
                expected_seq = int(request.expected_applied_seq)
                envelopes: list[AgentEnvelope] = []
                for entry in request.inputs:
                    row = by_id[entry.message_id]
                    if int(row["recipient_seq"]) != expected_seq + len(envelopes) + 1:
                        errors.invalid_input("claimed batch is not contiguous")
                    if row["status"] != "claimed" or row["claim_token"] != entry.claim_token:
                        errors.fenced("claimed input no longer carries this claim token")
                    if row["lease_until"] is None or row["lease_until"] <= now:
                        errors.fenced("claimed input lease expired")
                    envelopes.append(_envelope(row))
                applied_seq = expected_seq + len(envelopes)
                if applied_seq > int(cursor["allocated_seq"]):
                    errors.version_conflict("applied watermark would exceed allocated sequence")
                checkpoint = await self._verify_checkpoint(session, run, run["checkpoint_id"])
                await session.execute(
                    self._sql.message_applied,
                    {
                        "checkpoint_id": checkpoint["id"],
                        "now": now,
                        "scope_id": run["scope_id"],
                        "run_id": run["id"],
                        "message_ids": list(message_ids),
                    },
                )
                await session.execute(
                    self._sql.cursor_applied,
                    {
                        "applied_seq": applied_seq,
                        "now": now,
                        "scope_id": run["scope_id"],
                        "run_id": run["id"],
                    },
                )
                await session.execute(
                    self._sql.run_applied,
                    {
                        "applied_seq": applied_seq,
                        "now": now,
                        "scope_id": run["scope_id"],
                        "run_id": run["id"],
                    },
                )
                return AppliedInputs(
                    run_id=run["id"],
                    checkpoint_ref=str(checkpoint["state_ref"]),
                    previous_seq=str(expected_seq),
                    applied_seq=str(applied_seq),
                    messages=tuple(envelopes),
                )

    async def commit_cancel(self, request: CancelCommitRequest) -> ControlOutcome:
        """Apply the persisted cancel command at a safe point (review §11.2 P1-3, Q47).

        Cancellation is a state commit like pause: it carries the worker lease guard, the
        new checkpoint and full flush/execution evidence, and it keeps the run's
        execution-supervision work claimable so a cancelling run never dangles without an
        owner and without work. ``commit_terminal`` finishes the run once every execution
        is confirmed stopped.
        """
        guard = request.guard
        async with self._session() as session:
            async with session.begin():
                answered = await self._answered_control(session, guard)
                if answered is not None:
                    recorded_run, answered_command = answered
                    if answered_command["status"] == "applied":
                        return self._recorded_control(answered_command)
                    failure = FailureEnvelope.model_validate(answered_command["failure"] or {})
                    return ControlRejected(
                        outcome="rejected",
                        command_id=answered_command["id"],
                        run_id=recorded_run["id"],
                        failure=failure,
                    )
                guarded = await self._lock_guard(session, guard)
                run = guarded.run
                facts.verify_guard(
                    run,
                    expected_state_version=int(guard.expected_state_version),
                    status=str(guard.status),
                    desired_control=str(DesiredControl.CANCEL),
                    control_command_id=guard.control_command_id,
                    cancel_version=int(guard.cancel_version),
                )
                if guard.control_command_id is None:
                    errors.invalid_input("cancel requires its persisted command identity")
                await self._verify_checkpoint(
                    session,
                    run,
                    request.checkpoint_id,
                    maximum_state_version=int(run["state_version"]) + 1,
                )
                await self._require_evidence(session, run, request)
                command = await locks.lock_command(
                    session, self._locks, run["scope_id"], guard.control_command_id
                )
                if command is None:
                    errors.invalid_input("cancel command does not exist")
                if command["status"] == "applied":
                    return self._recorded_control(command)
                if command["status"] == "rejected":
                    failure = FailureEnvelope.model_validate(command["failure"] or {})
                    return ControlRejected(
                        outcome="rejected",
                        command_id=command["id"],
                        run_id=run["id"],
                        failure=failure,
                    )
                expected = int(run["state_version"])
                await session.execute(
                    self._sql.run_cancel,
                    {
                        "state_version": expected + 1,
                        "now": guarded.now,
                        "scope_id": run["scope_id"],
                        "run_id": run["id"],
                    },
                )
                applied = ControlApplied(
                    outcome="applied",
                    command_id=command["id"],
                    run_id=run["id"],
                    kind="cancel",
                    status=RunStatus.CANCELLING,
                    state_version=str(expected + 1),
                    desired_control=DesiredControl.CANCEL,
                    work_id=None,
                )
                await session.execute(
                    self._sql.command_applied,
                    {
                        "result_ref": _json(applied.model_dump(mode="json")),
                        "now": guarded.now,
                        "scope_id": run["scope_id"],
                        "command_id": command["id"],
                    },
                )
                # Only this safe-point advance work ends; supervision stays live, and the
                # cancel convergence work created by intake remains claimable.
                await self._finish_work(
                    session, run, guarded.work["id"], status="cancelled", now=guarded.now
                )
                await self._finish_advance_works(session, run, status="cancelled", now=guarded.now)
                # The session slot is **kept** until the terminal commit: the run is still
                # stopping external side effects, a later claim needs the slot to converge,
                # and the frontend must not be told the run stopped early. ``commit_terminal``
                # releases it once every execution is confirmed stopped.
                await self._ensure_cancel_work(session, run, command, now=guarded.now)
                return applied

    def _recorded_control(self, command: Mapping[str, Any]) -> ControlOutcome:
        recorded = _loads(command["result_ref"])
        return ControlApplied.model_validate(recorded)

    async def _reject_control(
        self,
        session: AsyncSession,
        run: dict[str, Any],
        command: dict[str, Any],
        failure: FailureEnvelope,
        now: datetime,
    ) -> ControlRejected:
        """A refusal is recorded and returned, never raised (frozen control contract)."""
        await session.execute(
            self._sql.command_rejected,
            {
                "failure": _json(failure.model_dump(mode="json")),
                "now": now,
                "scope_id": run["scope_id"],
                "command_id": command["id"],
            },
        )
        return ControlRejected(
            outcome="rejected",
            command_id=command["id"],
            run_id=run["id"],
            failure=failure,
        )

    async def _ensure_cancel_work(
        self,
        session: AsyncSession,
        run: Mapping[str, Any],
        command: Mapping[str, Any],
        *,
        now: datetime,
    ) -> UUID:
        """Guarantee exactly one claimable convergence work item for a cancelling run."""
        dedup_key = f"cancel:{command['id']}"
        live = await locks.lock_live_works(session, self._locks, run["scope_id"], run["id"])
        for item in live:
            if item["dedup_key"] == dedup_key:
                return UUID(str(item["id"]))
        work_id = uuid4()
        await session.execute(
            self._locks.work_insert,
            {
                "id": work_id,
                "scope_id": run["scope_id"],
                "run_id": run["id"],
                "subject_ref": _json({"kind": "cancel", "commandId": str(command["id"])}),
                "kind": "reconcile_execution",
                "dedup_key": dedup_key,
                "payload_ref": "cancel-convergence",
                # The frozen ``work_item`` CHECK pairs a wake reason with ``advance_run``
                # only: supervision work carries no wake reason and is found by the
                # scheduler's pending scan, not by a wake-up edge.
                "wake_reason": None,
                "priority": DEFAULT_RESUME_PRIORITY,
                "now": now,
                "deadline": run["deadline_at"],
            },
        )
        return work_id

    async def record_control(self, request: ControlRequest) -> CommandReceipt:
        """Persist an accepted control command together with its pending state.

        Intake must be observable by a worker (review §11.2 P1-3): it writes both the
        ``command`` row and the run's ``desired_control``/``control_command_id`` (and moves
        a cancel to ``cancelling`` with a claimable convergence work item). The safe-point
        commits (``commit_pause``/``commit_cancel``) then converge with full evidence.
        """
        actor = self._actor
        async with self._session() as session:
            async with session.begin():
                now = self._clock()
                # The request only carries the run id, so resolve its session first and lock
                # that slot: Slot -> Run is the fixed order, and the run row is verified to
                # still name the same session right after (a concurrent session move wins).
                session_id = await locks.run_session(
                    session, self._locks, actor.scope_id, request.run_id
                )
                if session_id is not None:
                    await locks.lock_slot(session, self._locks, actor.scope_id, session_id)
                run = await locks.lock_run(session, self._locks, actor.scope_id, request.run_id)
                if run is None:
                    errors.invalid_input("run does not exist")
                if session_id is None or run["session_id"] != session_id:
                    errors.fenced("run session changed while intake resolved its slot")
                if run["scope_id"] != actor.scope_id:
                    errors.forbidden("run belongs to another scope")
                await revalidate_authority(session, self._locks, actor, run=run, now=now)
                if request.kind == "resume":
                    # ``desired_control`` only ever holds null/pause/cancel (frozen catalog):
                    # resume has no intake state and is applied by the ``resume`` safe-point
                    # transaction, which owns its own command identity. Refuse here instead of
                    # leaking the database CHECK as an IntegrityError.
                    errors.invalid_input("resume has no intake state; apply it at its safe point")
                request_hash = _request_digest(
                    request.kind,
                    str(request.run_id),
                    request.idempotency_key,
                    str(request.expected_version),
                )
                command = await locks.lock_command_by_key(
                    session,
                    self._locks,
                    actor.scope_id,
                    request.run_id,
                    actor.actor_id,
                    request.idempotency_key,
                )
                if command is not None:
                    if command["kind"] != request.kind or command["request_hash"] != request_hash:
                        errors.invalid_input(
                            "control request identity was already used with another payload"
                        )
                    return CommandReceipt(
                        command_id=command["id"],
                        run_id=request.run_id,
                        status=command["status"],
                        state_version=str(run["state_version"]),
                        public_reason="command already recorded",
                    )
                if facts.is_terminal(run["status"]):
                    errors.version_conflict("run is already terminal")
                if int(run["state_version"]) != int(request.expected_version):
                    errors.version_conflict("state version changed before control intake")
                if request.kind == "pause" and str(run["status"]) not in facts.ACTIVE_STATUSES:
                    # Only an active run may carry a pending pause (frozen ``run`` CHECK 14):
                    # a paused/cancelling run must refuse instead of leaking the constraint.
                    errors.version_conflict("pause requires an active run")
                if run["desired_control"] is not None:
                    errors.control_pending("another control command is already pending")
                command_id = uuid4()
                await session.execute(
                    self._sql.command_insert,
                    {
                        "id": command_id,
                        "scope_id": actor.scope_id,
                        "run_id": request.run_id,
                        "actor_id": actor.actor_id,
                        "request_key": request.idempotency_key,
                        "request_hash": request_hash,
                        "kind": request.kind,
                        "expected_state_version": int(request.expected_version),
                    },
                )
                status = str(run["status"])
                if request.kind == "cancel":
                    status = str(RunStatus.CANCELLING)
                await session.execute(
                    self._sql.run_pending_control,
                    {
                        "desired_control": request.kind,
                        "command_id": command_id,
                        "status": status,
                        "now": now,
                        "scope_id": run["scope_id"],
                        "run_id": run["id"],
                    },
                )
                if request.kind == "cancel":
                    await self._ensure_cancel_work(
                        session,
                        run,
                        {"id": command_id},
                        now=now,
                    )
                elif request.kind == "pause":
                    # A waiting Run may have no live advance work. The command
                    # and its safe-point work must be durable in this transaction.
                    live = await locks.lock_live_works(
                        session, self._locks, run["scope_id"], run["id"]
                    )
                    if not any(item["kind"] == "advance_run" for item in live):
                        await session.execute(
                            self._locks.work_insert,
                            {
                                "id": uuid4(),
                                "scope_id": run["scope_id"],
                                "run_id": run["id"],
                                "subject_ref": _json(
                                    {"kind": "pause", "commandId": str(command_id)}
                                ),
                                "kind": "advance_run",
                                "dedup_key": f"pause:{command_id}",
                                "payload_ref": f"checkpoint:{run['checkpoint_id']}",
                                "wake_reason": "control",
                                "priority": DEFAULT_RESUME_PRIORITY,
                                "now": now,
                                "deadline": run["deadline_at"],
                            },
                        )
                return CommandReceipt(
                    command_id=command_id,
                    run_id=request.run_id,
                    status="accepted",
                    state_version=str(run["state_version"]),
                    public_reason="command accepted for its safe point",
                )

    async def record_output_seal(
        self,
        *,
        proof: FlushProof,
        sealed_revision: int,
    ) -> None:
        """Validate and persist one immutable, idempotent producer seal.

        Taking the run row in the fixed lock order is what closes the
        check-then-append window: the state commits enumerate seals while holding the same
        run row, so a seal cannot appear between enumeration and commit. Producer ownership
        is re-read from ``model_attempt`` and ``timeline_item``; caller-provided UUIDs are
        expectations, never authority. A connection-ambiguous retry with the exact same
        proof is a no-op, while any identity or payload mismatch is rejected.
        """
        if proof.scope_id != self._actor.scope_id:
            errors.forbidden("sealed producer belongs to another scope")
        if int(proof.durable_seq) < 1 or sealed_revision < 1:
            errors.invalid_input("sealed producer revisions must be positive")
        channels = {channel.channel: channel for channel in proof.channels}
        content = channels["content"]
        analysis = channels["analysis"]
        payload = {
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
        async with self._session() as session:
            async with session.begin():
                now = self._clock()
                slot = await locks.lock_slot(
                    session, self._locks, self._actor.scope_id, proof.session_id
                )
                if slot is None:
                    errors.invalid_input("session slot does not exist")
                run = await locks.lock_run(session, self._locks, self._actor.scope_id, proof.run_id)
                if run is None:
                    errors.invalid_input("run does not exist")
                if run["session_id"] != proof.session_id:
                    errors.invalid_input("run and sealed producer session disagree")
                if facts.is_terminal(str(run["status"])):
                    errors.version_conflict("terminal run cannot accept another output seal")
                await revalidate_authority(session, self._locks, self._actor, run=run, now=now)
                attempt = (
                    (
                        await session.execute(
                            self._locks.model_attempt_owner,
                            {
                                "scope_id": run["scope_id"],
                                "model_attempt_id": proof.attempt_id,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if attempt is None or attempt["run_id"] != run["id"]:
                    errors.invalid_input("model attempt does not belong to the sealed run")
                item = (
                    (
                        await session.execute(
                            self._locks.timeline_item_owner,
                            {"scope_id": run["scope_id"], "item_id": proof.item_id},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if (
                    item is None
                    or item["run_id"] != run["id"]
                    or item["session_id"] != run["session_id"]
                ):
                    errors.invalid_input("timeline item does not belong to the sealed run")

                query = {
                    "scope_id": run["scope_id"],
                    "run_id": run["id"],
                    "model_attempt_id": proof.attempt_id,
                    "item_id": proof.item_id,
                    "proof_id": proof.proof_id,
                }
                by_producer = (
                    (await session.execute(self._locks.seal_by_producer, query))
                    .mappings()
                    .one_or_none()
                )
                by_proof = (
                    (await session.execute(self._locks.seal_by_proof, query))
                    .mappings()
                    .one_or_none()
                )
                existing = by_producer if by_producer is not None else by_proof
                if existing is not None:
                    comparable = {key: existing[key] for key in payload}
                    if comparable == payload and by_producer is not None and by_proof is not None:
                        return
                    errors.invalid_input(
                        "output seal identity was already used with another producer or payload"
                    )
                await session.execute(
                    self._locks.seal_insert,
                    {
                        "id": uuid4(),
                        **payload,
                        "sealed_at": now,
                    },
                )

    async def commit_domain(self, request: DomainCommitRequest) -> ResourceRef:
        guard = request.guard
        async with self._session() as session:
            async with session.begin():
                guarded = await self._lock_guard(session, guard)
                run = guarded.run
                if int(run["state_version"]) != int(guard.expected_state_version):
                    errors.version_conflict("state version changed")
                resource = request.resource
                spec = self._registry.get(resource.resource_type)
                if spec is None:
                    errors.forbidden(
                        "cross-database or unregistered resource requires the external "
                        "idempotency protocol"
                    )
                input_hash = _request_digest(
                    "domain",
                    resource.resource_type,
                    resource.resource_id,
                    str(resource.resource_version),
                    request.action,
                    request.change_ref,
                )
                # Atomic claim: this insert serializes concurrent requests that share the
                # operation identity, so only one of them can reach the domain write
                # (review §10.2 P1-5).
                await session.execute(
                    self._sql.operation_claim,
                    {
                        "id": request.operation_id,
                        "scope_id": run["scope_id"],
                        "task_id": run["task_id"],
                        "key": str(request.operation_id),
                        "input_hash": input_hash,
                    },
                )
                claimed = (
                    (
                        await session.execute(
                            self._sql.operation_lock,
                            {
                                "scope_id": run["scope_id"],
                                "task_id": run["task_id"],
                                "key": str(request.operation_id),
                            },
                        )
                    )
                    .mappings()
                    .one()
                )
                operation = dict(claimed)
                if operation["input_hash"] != input_hash:
                    errors.invalid_input("operation identity was already used with another payload")
                if operation["status"] == "committed":
                    return ResourceRef.model_validate(_loads(operation["result_ref"]))

                row = await locks.lock_domain_resource(
                    session, self._schema, spec.table, run["scope_id"], resource.resource_id
                )
                if row is None:
                    errors.invalid_input("domain resource does not exist in this scope")
                version_column = spec.version_column
                if str(row[version_column]) != str(resource.resource_version):
                    errors.version_conflict("domain resource version changed")
                updated = ResourceRef(
                    resource_type=resource.resource_type,
                    resource_id=resource.resource_id,
                    resource_version=str(int(row[version_column]) + 1),
                    scope=ResourceScope(
                        scope_id=run["scope_id"], session_id=str(run["session_id"])
                    ),
                )
                await session.execute(
                    scoped(
                        f"UPDATE {{t}}{spec.table} SET {version_column} = {version_column} + 1, "
                        "updated_at = :now WHERE scope_id = :scope_id AND id = :resource_id",
                        self._schema,
                    ),
                    {
                        "now": guarded.now,
                        "scope_id": run["scope_id"],
                        "resource_id": resource.resource_id,
                    },
                )
                await session.execute(
                    self._sql.operation_commit,
                    {
                        "result_ref": _json(updated.model_dump(mode="json")),
                        "now": guarded.now,
                        "scope_id": run["scope_id"],
                        "operation_id": operation["id"],
                    },
                )
                return updated


def _envelope(row: Mapping[str, Any]) -> AgentEnvelope:
    """Rebuild a stored envelope; ``payload`` may carry ``resourceRefs`` (see README)."""
    body = row["payload"]
    payload: object = body
    resource_refs: Sequence[object] = ()
    if isinstance(body, dict) and set(body.keys()) == {"payload", "resourceRefs"}:
        payload = body["payload"]
        resource_refs = body["resourceRefs"] or ()
    return message_adapter.validate_python(
        {
            "kind": row["kind"],
            "protocolRevision": row["protocol_revision"],
            "messageId": row["id"],
            "correlationId": row["correlation_id"],
            "causationId": row["causation_id"],
            "senderRunId": row["sender_run_id"],
            "recipientRunId": row["recipient_run_id"],
            "taskId": row["task_id"],
            "recipientSeq": str(row["recipient_seq"]),
            "createdAt": row["created_at"],
            "deadlineAt": row["deadline_at"],
            "resourceRefs": list(resource_refs),
            "traceId": row["trace_id"],
            "payload": payload,
        }
    )


__all__ = [
    "DEFAULT_RESUME_PRIORITY",
    "RESUME_PAYLOAD_REF",
    "Clock",
    "SqlStateUnitOfWork",
    "utcnow",
]

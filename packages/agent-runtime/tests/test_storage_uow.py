"""Real PostgreSQL matrix for the SQL unit of work (DS-P02-02, second repair).

Opt-in: set ``AGENT_RUNTIME_TEST_DSN``. Every test uses a random ``p02_uow_*`` schema; the
module teardown fails when any schema is left behind. SQLite and mocks are never used as
concurrency, sealing or rollback evidence.

The matrix follows review §11.2/§11.3 and the "DS-P02-02 第二次返修任务" card:

* durable producer seals (never ``event`` rows) drive flush evidence (P1-1);
* one fixed lock order ``SessionSlot -> Run -> Authority -> Work/Command/...`` (P1-2);
* worker-visible control intake plus safe-point ``commit_pause``/``commit_cancel`` (P1-3);
* execution ownership by ``run_id`` and supervision identity from the database (P1-4);
* supervision work survives terminal failures (P1-5);
* the canonical ``release_request_hash`` covers the whole release payload (Q44).
"""

import asyncio
import hashlib
import json
import os
import threading
import time
import uuid
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from typing import Any

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy", reason="requires agent-runtime[storage]")

import sqlalchemy as sa  # noqa: E402
from pydantic import ValidationError  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker  # noqa: E402
from test_context_runtime import request as context_request  # noqa: E402
from test_decision_validation import facts as decision_facts  # noqa: E402
from test_decision_validation import tool_action  # noqa: E402
from test_provider_invoker import make_request  # noqa: E402

from agent_runtime.agent_loop import DecisionFrame  # noqa: E402
from agent_runtime.context_payloads import BlobContextPayloadStore  # noqa: E402
from agent_runtime.contracts.capacity import (  # noqa: E402
    CapacityKey,
    CapacityObserveRequest,
    CapacityReleaseRequest,
    CapacityReserveRequest,
)
from agent_runtime.contracts.decisions import decision_adapter  # noqa: E402
from agent_runtime.contracts.failure import AgentRuntimeError  # noqa: E402
from agent_runtime.contracts.journal import (  # noqa: E402
    FlushProof,
    JournalPage,
    OutboxClaimRequest,
    OutboxDelivery,
    ProducerBatch,
    SealOutputRequest,
)
from agent_runtime.contracts.payloads import ItemCompleted  # noqa: E402
from agent_runtime.contracts.provider import (  # noqa: E402
    AnalysisDelta as ProviderAnalysisDelta,
)
from agent_runtime.contracts.provider import (  # noqa: E402
    ContentDelta as ProviderContentDelta,
)
from agent_runtime.contracts.references import CapabilityRef, ResourceRef  # noqa: E402
from agent_runtime.contracts.requests import ControlRequest, StreamRequest  # noqa: E402
from agent_runtime.contracts.results import result_adapter  # noqa: E402
from agent_runtime.contracts.state import DesiredControl, RunStatus  # noqa: E402
from agent_runtime.contracts.state_events import (  # noqa: E402
    ItemCompletedDraft,
    MainEventAuthority,
    StateEventBatch,
)
from agent_runtime.contracts.tools import ToolInvocation, ToolSpec  # noqa: E402
from agent_runtime.contracts.transactions import (  # noqa: E402
    ActorContext,
    ApplyInputRequest,
    CommitGuard,
    DomainCommitRequest,
    LeaseToken,
)
from agent_runtime.contracts.transitions import (  # noqa: E402
    AdvanceCommitRequest,
    CancelCommitRequest,
    ControlApplied,
    NoProducersConfirmed,
    PauseCommitRequest,
    ResumeCommitRequest,
    ResumedRun,
    StateWriteGuard,
    TerminalCommitRequest,
)
from agent_runtime.storage.action_commit import SqlWaitActionCommitter  # noqa: E402
from agent_runtime.storage.answer_commit import (  # noqa: E402
    AnswerPublication,
    SqlAnswerActionCommitter,
)
from agent_runtime.storage.context_blobs import SqlContextBlobBackend  # noqa: E402
from agent_runtime.storage.context_sources import SqlRuntimeSourceReader  # noqa: E402
from agent_runtime.storage.decisions import SqlDecisionStore  # noqa: E402
from agent_runtime.storage.engines import (  # noqa: E402
    StorageConfig,
    create_storage_engine,
)
from agent_runtime.storage.journal import (  # noqa: E402
    SqlJournal,
    SqlJournalReader,
    SqlOutboxStore,
)
from agent_runtime.storage.live_run import SqlLiveRunGuard  # noqa: E402
from agent_runtime.storage.main_budget import SqlMainBudgetReader  # noqa: E402
from agent_runtime.storage.main_run_loader import (  # noqa: E402
    LoadedMainRun,
    MainAgentBinding,
    SqlMainRunLoader,
)
from agent_runtime.storage.main_stream import SqlMainStream  # noqa: E402
from agent_runtime.storage.migrate import MigrationResult, execute  # noqa: E402
from agent_runtime.storage.model_events import SqlModelEventSink  # noqa: E402
from agent_runtime.storage.scheduler import SqlScheduler  # noqa: E402
from agent_runtime.storage.tool_capacity import SqlToolCapacity  # noqa: E402
from agent_runtime.storage.uow import SqlStateUnitOfWork, SqlSystemCapacityStore  # noqa: E402
from agent_runtime.storage.uow.capacity import release_request_digest  # noqa: E402
from agent_runtime.storage.wake import SqlWaitWaker, WakeRequest  # noqa: E402

DSN = os.environ.get("AGENT_RUNTIME_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="AGENT_RUNTIME_TEST_DSN is not configured")
_DATABASE_VERIFIED = False

SCOPE = "scope-a"
ACTOR_ID = "actor-1"
OWNER = "worker-1"
DEFAULT_LEASE_SECONDS = 3600
_LOCK = threading.Lock()


_LOOP: Any = None


def run[T](coro: Callable[[], Awaitable[T]]) -> T:
    """Drive one coroutine on a persistent loop so pooled connections stay usable."""
    global _LOOP
    if _LOOP is None or _LOOP.is_closed():
        _LOOP = asyncio.new_event_loop()
    return _LOOP.run_until_complete(coro())


@dataclass
class World:
    schema: str
    suffix: str
    scope: str
    actor_id: str
    run_id: uuid.UUID
    session_id: uuid.UUID
    turn_id: uuid.UUID
    task_id: uuid.UUID
    work_id: uuid.UUID
    checkpoint_id: uuid.UUID
    agent_revision_id: uuid.UUID
    sender_run_id: uuid.UUID
    workspace_id: uuid.UUID
    authority_revision: int = 1
    state_version: int = 1
    owner_epoch: int = 1
    work_epoch: int = 1
    slot_version: int = 1
    cancel_version: int = 0
    auth_revision: int = 1
    status: str = "running"
    phase: str = "model"
    owner: str = OWNER
    values: dict[str, Any] = field(default_factory=dict)

    @property
    def lease(self) -> bool:
        return self.status in {"running", "waiting_tool", "waiting_user", "cancelling"}


def actor(auth_revision: int = 1, *, world: World | None = None) -> ActorContext:
    return ActorContext(
        scope_id=world.scope if world is not None else SCOPE,
        actor_id=world.actor_id if world is not None else ACTOR_ID,
        auth_revision=str(auth_revision),
        grants_ref="grants:1",
    )


def lease(world: World, **overrides: Any) -> LeaseToken:
    now = overrides.pop("now", datetime.now(UTC))
    base: dict[str, Any] = {
        "scope_id": world.scope,
        "session_id": world.session_id,
        "run_id": world.run_id,
        "work_id": world.work_id,
        "owner_id": world.owner,
        "run_epoch": str(world.owner_epoch),
        "work_epoch": str(world.work_epoch),
        "slot_version": str(world.slot_version),
        "run_lease_until": now + timedelta(seconds=DEFAULT_LEASE_SECONDS),
        "work_lease_until": now + timedelta(seconds=DEFAULT_LEASE_SECONDS),
    }
    base.update(overrides)
    return LeaseToken.model_validate(base)


def guard(world: World, **overrides: Any) -> CommitGuard:
    a = overrides.pop("actor", actor(world.auth_revision, world=world))
    payload: dict[str, Any] = {
        "actor": a,
        "lease": overrides.pop("lease", lease(world)),
        "expected_state_version": str(world.state_version),
    }
    payload.update(overrides)
    return CommitGuard.model_validate(payload)


def state_guard(world: World, **overrides: Any) -> StateWriteGuard:
    payload: dict[str, Any] = {
        "actor": actor(world.auth_revision, world=world),
        "lease": lease(world),
        "expected_state_version": str(world.state_version),
        "status": world.status,
        "desired_control": None,
        "control_command_id": None,
        "cancel_version": str(world.cancel_version),
    }
    payload.update(overrides)
    return StateWriteGuard.model_validate(payload)


def no_producers(world: World, *, enumerated: int = 1) -> dict[str, Any]:
    return {
        "kind": "no_producers",
        "runId": str(world.run_id),
        "enumeratedSessionSeq": str(enumerated),
    }


def producers_confirmed(*, run_id: uuid.UUID, enumerated: int) -> NoProducersConfirmed:
    return NoProducersConfirmed(
        kind="no_producers", run_id=run_id, enumerated_session_seq=str(enumerated)
    )


def executions_payload(
    *items: tuple[uuid.UUID, str, str | None],
) -> tuple[dict[str, Any], ...]:
    """Build execution resolutions from ``(execution_id, state, supervision_work_id)``."""
    return tuple(
        {
            "executionRef": {
                "resourceType": "execution",
                "resourceId": str(execution_id),
                "scope": {"scopeId": SCOPE},
            },
            "generation": "1",
            "state": state,
            "evidenceRef": "evidence:1",
            "supervisionWorkId": None if supervision is None else str(supervision),
        }
        for execution_id, state, supervision in items
    )


def advance_request(world: World, **overrides: Any) -> AdvanceCommitRequest:
    payload: dict[str, Any] = {
        "guard": state_guard(world),
        "checkpoint_id": world.checkpoint_id,
        "evidence": no_producers(world),
        "executions": (),
        "next_action_ref": "next:1",
    }
    payload.update(overrides)
    return AdvanceCommitRequest.model_validate(payload)


def engine_for(schema: str, pool_size: int = 4, *, search_path: bool = True) -> AsyncEngine:
    """Real adapter engine.

    ``search_path=True`` is only used by this module's own helper SQL; every UoW and
    capacity call runs on the **default** ``StorageConfig`` (no search path), which is the
    reviewer's P1-3 counterexample.
    """
    assert DSN is not None
    return create_storage_engine(
        StorageConfig(
            dsn=DSN,
            schema=schema,
            pool_size=pool_size,
            max_overflow=0,
            set_search_path=search_path,
        )
    )


async def _scalar(engine: AsyncEngine, sql: str, **params: Any) -> Any:
    async with engine.connect() as connection:
        return (await connection.execute(sa.text(sql), params)).scalar()


def scalar(schema: str, sql: str, **params: Any) -> Any:
    async def main() -> Any:
        engine = engine_for(schema)
        try:
            return await _scalar(engine, sql, **params)
        finally:
            await engine.dispose()

    return run(main)


def migrate(command: str, schema: str) -> MigrationResult:
    global _DATABASE_VERIFIED
    assert DSN is not None
    if not _DATABASE_VERIFIED:
        expected = os.environ.get("AGENT_RUNTIME_EXPECTED_DATABASE", "aletai_wangxh")
        actual = str(scalar("public", "SELECT current_database()"))
        if actual != expected:
            raise RuntimeError(f"database guard rejected {actual!r}; expected {expected!r}")
        _DATABASE_VERIFIED = True
    return run(lambda: execute(command, DSN, schema, "base", 60.0))


# --------------------------------------------------------------------------- seeding


async def _seed(
    schema: str,
    suffix: str = "a",
    *,
    scope: str = SCOPE,
    actor_id: str = ACTOR_ID,
    status: str = "running",
    phase: str = "model",
) -> World:
    """Seed one complete run world (run, slot, leased advance work, grant, checkpoint)."""
    engine = engine_for(schema)
    world = World(
        schema=schema,
        suffix=suffix,
        scope=scope,
        actor_id=actor_id,
        run_id=uuid.uuid4(),
        session_id=uuid.uuid4(),
        turn_id=uuid.uuid4(),
        task_id=uuid.uuid4(),
        work_id=uuid.uuid4(),
        checkpoint_id=uuid.uuid4(),
        agent_revision_id=uuid.uuid4(),
        sender_run_id=uuid.uuid4(),
        workspace_id=uuid.uuid4(),
        status=status,
        phase=phase,
    )
    now = datetime.now(UTC)
    agent_id = uuid.uuid4()
    leased = world.lease
    async with engine.begin() as connection:
        await connection.execute(
            sa.text(
                "INSERT INTO session (id, scope_id, owner_actor_id, title_source, event_seq, "
                "auth_revision) VALUES (:id, :scope, :actor, 'none', 0, 1)"
            ),
            {"id": world.session_id, "scope": scope, "actor": actor_id},
        )
        await connection.execute(
            sa.text(
                "INSERT INTO turn (id, scope_id, session_id, request_key, request_hash, "
                "input_ref) VALUES (:id, :scope, :session, :key, :hash, 'input:1')"
            ),
            {
                "id": world.turn_id,
                "scope": scope,
                "session": world.session_id,
                "key": f"req-{suffix}",
                "hash": "a" * 64,
            },
        )
        await connection.execute(
            sa.text("INSERT INTO agent (id, scope_id, key) VALUES (:id, :scope, :key)"),
            {"id": agent_id, "scope": scope, "key": f"agent-{suffix}"},
        )
        await connection.execute(
            sa.text(
                "INSERT INTO agent_revision (id, scope_id, agent_id, revision, manifest, "
                "content_hash) VALUES (:id, :scope, :agent, 1, '{}', :hash)"
            ),
            {
                "id": world.agent_revision_id,
                "scope": scope,
                "agent": agent_id,
                "hash": "b" * 64,
            },
        )
        await connection.execute(
            sa.text(
                "INSERT INTO run (id, scope_id, session_id, turn_id, root_run_id, "
                "agent_revision_id, task_id, checkpoint_id, status, phase, owner_id, "
                "state_version, owner_epoch, cancel_version, wake_generation, "
                "pending_input_version, applied_input_version, authority_revision, "
                "lease_until, phase_started_at, deadline_at) VALUES "
                "(:id, :scope, :session, :turn, :id, :agent_revision, :task, :checkpoint, "
                ":status, :phase, :owner, :state_version, :owner_epoch, :cancel_version, 0, 0, "
                "0, :authority_revision, :lease_until, :now, :deadline)"
            ),
            {
                "id": world.run_id,
                "scope": scope,
                "session": world.session_id,
                "turn": world.turn_id,
                "agent_revision": world.agent_revision_id,
                "task": world.task_id,
                "checkpoint": world.checkpoint_id,
                "status": status,
                "phase": phase,
                "owner": world.owner if leased else None,
                "state_version": world.state_version,
                "owner_epoch": world.owner_epoch,
                "cancel_version": world.cancel_version,
                "authority_revision": world.authority_revision,
                "lease_until": now + timedelta(seconds=DEFAULT_LEASE_SECONDS) if leased else None,
                "now": now,
                "deadline": now + timedelta(days=1),
            },
        )
        await connection.execute(
            sa.text(
                "INSERT INTO task (id, scope_id, root_run_id, acceptance_spec_ref, deadline_at) "
                "VALUES (:id, :scope, :run, 'acceptance:1', :deadline)"
            ),
            {
                "id": world.task_id,
                "scope": scope,
                "run": world.run_id,
                "deadline": now + timedelta(days=1),
            },
        )
        await connection.execute(
            sa.text(
                "INSERT INTO session_slot (scope_id, session_id, owner_run_id, slot_version) "
                "VALUES (:scope, :session, :run, :slot_version)"
            ),
            {
                "scope": scope,
                "session": world.session_id,
                "run": world.run_id,
                "slot_version": world.slot_version,
            },
        )
        await connection.execute(
            sa.text(
                "INSERT INTO work_item (id, scope_id, run_id, subject_ref, kind, dedup_key, "
                "status, payload_ref, wake_reason, owner_id, priority, lease_epoch, "
                "attempt_count, available_at, deadline_at, lease_until) VALUES "
                "(:id, :scope, :run, '{}', 'advance_run', :dedup, :status, 'advance', "
                "'start', :owner, 100, :work_epoch, 0, :now, :deadline, :lease_until)"
            ),
            {
                "id": world.work_id,
                "scope": scope,
                "run": world.run_id,
                "dedup": f"advance:{world.run_id}",
                "status": "leased" if leased else "pending",
                "owner": world.owner if leased else None,
                "work_epoch": world.work_epoch,
                "now": now,
                "deadline": now + timedelta(days=1),
                "lease_until": now + timedelta(seconds=DEFAULT_LEASE_SECONDS) if leased else None,
            },
        )
        await connection.execute(
            sa.text(
                "INSERT INTO capability_grant (id, scope_id, run_id, actor_id, revision, "
                "allowed_scope, issued_at, expires_at) VALUES "
                "(:id, :scope, :run, :actor, :revision, '[]', :now, :expires)"
            ),
            {
                "id": uuid.uuid4(),
                "scope": scope,
                "run": world.run_id,
                "actor": actor_id,
                "revision": world.authority_revision,
                "now": now,
                "expires": now + timedelta(hours=6),
            },
        )
        await connection.execute(
            sa.text(
                "INSERT INTO checkpoint (id, scope_id, run_id, state_version, "
                "representation_revision, event_seq, schema_revision, runtime_build, "
                "state_ref, content_hash) VALUES (:id, :scope, :run, :state_version, 1, 0, 1, "
                "'test', :state_ref, :hash)"
            ),
            {
                "id": world.checkpoint_id,
                "scope": scope,
                "run": world.run_id,
                "state_version": world.state_version + 1,
                "state_ref": f"state:{world.state_version + 1}",
                "hash": "c" * 64,
            },
        )
        # A sibling run under the *same task* acts as a mailbox sender and as the
        # execution-ownership counterexample (review §11.2 P1-4).
        await connection.execute(
            sa.text(
                "INSERT INTO run (id, scope_id, session_id, turn_id, root_run_id, "
                "agent_revision_id, task_id, status, phase, owner_id, state_version, "
                "owner_epoch, cancel_version, wake_generation, pending_input_version, "
                "applied_input_version, authority_revision, lease_until, phase_started_at, "
                "deadline_at) VALUES "
                "(:id, :scope, :session, :turn, :id, :agent_revision, :task, 'running', "
                "'model', :owner, 1, 1, 0, 0, 0, 0, 1, :lease_until, :now, :deadline)"
            ),
            {
                "id": world.sender_run_id,
                "scope": scope,
                "session": world.session_id,
                "turn": world.turn_id,
                "agent_revision": world.agent_revision_id,
                "task": world.task_id,
                "owner": "worker-2",
                "lease_until": now + timedelta(seconds=DEFAULT_LEASE_SECONDS),
                "now": now,
                "deadline": now + timedelta(days=1),
            },
        )
        await connection.execute(
            sa.text(
                "INSERT INTO workspace (id, scope_id, task_id, owner_run_id, generation, "
                "revision) VALUES (:id, :scope, :task, :run, 1, 1)"
            ),
            {
                "id": world.workspace_id,
                "scope": scope,
                "task": world.task_id,
                "run": world.run_id,
            },
        )
    await engine.dispose()
    return world


async def _seed_item(world: World, *, run_id: uuid.UUID | None = None) -> uuid.UUID:
    """One timeline item so a sealed producer can reference a real output item."""
    engine = engine_for(world.schema)
    item_id = uuid.uuid4()
    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    "INSERT INTO timeline_item (id, scope_id, session_id, run_id, item_type, "
                    "content_ref, status, final_revision, source_seq) VALUES "
                    "(:id, :scope, :session, :run, 'message', 'content:1', 'completed', 1, 1)"
                ),
                {
                    "id": item_id,
                    "scope": world.scope,
                    "session": world.session_id,
                    "run": run_id or world.run_id,
                },
            )
    finally:
        await engine.dispose()
    return item_id


async def _seed_attempt(world: World, *, run_id: uuid.UUID | None = None) -> uuid.UUID:
    """Context snapshot plus model attempt so seals/events have a real producer."""
    engine = engine_for(world.schema)
    attempt_id = uuid.uuid4()
    snapshot_id = uuid.uuid4()
    now = datetime.now(UTC)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    "INSERT INTO context_snapshot (id, scope_id, session_id, run_id, "
                    "auth_revision, context_version, estimated_input_tokens, schema_revision, "
                    "task_type, payload_ref, content_hash, source_refs, degradation, model_ref, "
                    "prompt_ref, tool_refs, budget) VALUES (:id, :scope, :session, :run, 1, 1, "
                    "1, 1, 'chat', 'payload:1', :hash, '[]', '{}', '{}', '{}', '[]', '{}')"
                ),
                {
                    "id": snapshot_id,
                    "scope": world.scope,
                    "session": world.session_id,
                    "run": run_id or world.run_id,
                    "hash": "1" * 64,
                },
            )
            await connection.execute(
                sa.text(
                    "INSERT INTO model_attempt (id, scope_id, run_id, context_snapshot_id, "
                    "request_key, status, input_ref, usage_status, usage_reason, finished_at) "
                    "VALUES (:id, :scope, :run, :snapshot, :key, 'completed', 'input:1', "
                    "'complete', 'provider_reported', :now)"
                ),
                {
                    "id": attempt_id,
                    "scope": world.scope,
                    "run": run_id or world.run_id,
                    "snapshot": snapshot_id,
                    "key": f"attempt-{attempt_id}",
                    "now": now,
                },
            )
    finally:
        await engine.dispose()
    return attempt_id


async def _seed_event(
    world: World,
    *,
    sequence: int,
    item_id: uuid.UUID,
    attempt_id: uuid.UUID,
    run_id: uuid.UUID | None = None,
    digest: str | None = None,
) -> None:
    """One unsealed journal delta: it blocks ``NoProducersConfirmed`` (P1-1)."""
    engine = engine_for(world.schema)
    now = datetime.now(UTC)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    "INSERT INTO event (id, scope_id, session_id, turn_id, run_id, item_id, "
                    "model_attempt_id, session_seq, schema_revision, producer_key, "
                    "producer_digest, kind, visibility, trace_id, occurred_at, recorded_at, "
                    "payload) VALUES (:id, :scope, :session, :turn, :run, :item, :attempt, "
                    ":seq, 1, 'content', :digest, 'assistant.delta', 'public', 'trace-1', "
                    ":now, :now, '{}')"
                ),
                {
                    "id": uuid.uuid4(),
                    "scope": world.scope,
                    "session": world.session_id,
                    "turn": world.turn_id,
                    "run": run_id or world.run_id,
                    "item": item_id,
                    "attempt": attempt_id,
                    "seq": sequence,
                    "digest": digest or ("e" * 64),
                    "now": now,
                },
            )
    finally:
        await engine.dispose()


async def _seed_execution(
    world: World,
    *,
    run_id: uuid.UUID | None = None,
    status: str = "running",
    generation: int = 1,
    supervisor_work_id: uuid.UUID | None = None,
) -> uuid.UUID:
    """One execution row owned by ``run_id`` (never by the shared task alone)."""
    engine = engine_for(world.schema)
    execution_id = uuid.uuid4()
    operation_id = uuid.uuid4()
    now = datetime.now(UTC)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    "INSERT INTO operation (id, scope_id, task_id, idempotency_key, input_hash, "
                    "status, result_ref) VALUES (:id, :scope, :task, :key, :hash, 'committed', "
                    "'{}')"
                ),
                {
                    "id": operation_id,
                    "scope": world.scope,
                    "task": world.task_id,
                    "key": f"exec-{execution_id}",
                    "hash": "9" * 64,
                },
            )
            await connection.execute(
                sa.text(
                    "INSERT INTO execution (id, scope_id, operation_id, run_id, "
                    "supervisor_work_id, remote_request_key, status, log_cursor, "
                    "cleanup_status, generation, reconciliation_round, deadline_at) VALUES "
                    "(:id, :scope, :operation, :run, :supervisor, :key, :status, 'cursor:0', "
                    "'completed', :generation, 0, :deadline)"
                ),
                {
                    "id": execution_id,
                    "scope": world.scope,
                    "operation": operation_id,
                    "run": run_id or world.run_id,
                    "supervisor": supervisor_work_id,
                    "key": f"remote-{execution_id}",
                    "status": status,
                    "generation": generation,
                    "deadline": now + timedelta(days=1),
                },
            )
    finally:
        await engine.dispose()
    return execution_id


async def _seed_work(
    world: World,
    *,
    run_id: uuid.UUID | None = None,
    kind: str = "reconcile_execution",
    status: str = "pending",
    dedup_key: str | None = None,
    lease_epoch: int = 0,
) -> uuid.UUID:
    """A supervision work item (pending or leased) owned by ``run_id``."""
    engine = engine_for(world.schema)
    work_id = uuid.uuid4()
    now = datetime.now(UTC)
    leased = status == "leased"
    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    "INSERT INTO work_item (id, scope_id, run_id, subject_ref, kind, dedup_key, "
                    "status, payload_ref, wake_reason, owner_id, priority, lease_epoch, "
                    "attempt_count, available_at, deadline_at, lease_until) VALUES "
                    "(:id, :scope, :run, '{}', :kind, :dedup, :status, 'supervision', "
                    ":wake, :owner, 100, :lease_epoch, 0, :now, :deadline, :lease_until)"
                ),
                {
                    "id": work_id,
                    "scope": world.scope,
                    "run": run_id or world.run_id,
                    "kind": kind,
                    "dedup": dedup_key or f"supervision:{work_id}",
                    "status": status,
                    # The frozen CHECK pairs a wake reason with ``advance_run`` only.
                    "wake": "start" if kind == "advance_run" else None,
                    "owner": world.owner if leased else None,
                    "lease_epoch": lease_epoch,
                    "now": now,
                    "deadline": now + timedelta(days=1),
                    "lease_until": (
                        now + timedelta(seconds=DEFAULT_LEASE_SECONDS) if leased else None
                    ),
                },
            )
    finally:
        await engine.dispose()
    return work_id


async def _seed_second_workspace(world: World) -> uuid.UUID:
    engine = engine_for(world.schema)
    workspace_id = uuid.uuid4()
    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    "INSERT INTO workspace (id, scope_id, task_id, owner_run_id, generation, "
                    "revision) VALUES (:id, :scope, :task, :run, 2, 1)"
                ),
                {
                    "id": workspace_id,
                    "scope": world.scope,
                    "task": world.task_id,
                    "run": world.run_id,
                },
            )
    finally:
        await engine.dispose()
    return workspace_id


def _message_body(sequence: int) -> dict[str, Any]:
    return {"text": f"input {sequence}", "expectedTaskRevision": "1"}


async def _seed_messages(
    world: World,
    sequences: tuple[int, ...],
    *,
    allocated: int,
    claim_token: uuid.UUID,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
) -> list[uuid.UUID]:
    engine = engine_for(world.schema)
    now = datetime.now(UTC)
    message_ids: list[uuid.UUID] = []
    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    "INSERT INTO mailbox_cursor (scope_id, recipient_run_id, allocated_seq, "
                    "applied_seq) VALUES (:scope, :run, :allocated, 0)"
                ),
                {"scope": world.scope, "run": world.run_id, "allocated": allocated},
            )
            await connection.execute(
                sa.text("UPDATE run SET pending_input_version = :allocated WHERE id = :run"),
                {"allocated": allocated, "run": world.run_id},
            )
            for sequence in sequences:
                message_id = uuid.uuid4()
                message_ids.append(message_id)
                await connection.execute(
                    sa.text(
                        "INSERT INTO agent_message (id, scope_id, sender_run_id, "
                        "recipient_run_id, task_id, correlation_id, causation_id, claim_token, "
                        "recipient_seq, protocol_revision, payload, kind, payload_hash, status, "
                        "trace_id, deadline_at, lease_until) VALUES "
                        "(:id, :scope, :sender, :run, :task, :correlation, NULL, :token, :seq, "
                        "1, CAST(:payload AS jsonb), 'input.append', :hash, 'claimed', "
                        "'trace-1', :deadline, :lease_until)"
                    ),
                    {
                        "id": message_id,
                        "scope": world.scope,
                        "sender": world.sender_run_id,
                        "run": world.run_id,
                        "task": world.task_id,
                        "correlation": uuid.uuid4(),
                        "token": claim_token,
                        "seq": sequence,
                        "payload": json.dumps(_message_body(sequence)),
                        "hash": "e" * 64,
                        "deadline": now + timedelta(days=1),
                        "lease_until": now + timedelta(seconds=lease_seconds),
                    },
                )
    finally:
        await engine.dispose()
    return message_ids


# --------------------------------------------------------------------------- fixtures


@pytest.fixture
def schema() -> Any:
    name = f"p02_uow_{uuid.uuid4().hex[:12]}"
    assert migrate("upgrade", name).status == "applied"
    yield name

    async def drop() -> None:
        engine = engine_for(name)
        try:
            async with engine.begin() as connection:
                await connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))
        finally:
            await engine.dispose()

    run(drop)


@pytest.fixture
def world(schema: str) -> World:
    _SCHEMA[0] = schema
    return run(lambda: _seed(schema))


@pytest.fixture(scope="module", autouse=True)
def no_leftover_schemas() -> Any:
    yield
    leftovers = scalar(
        "public",
        "SELECT coalesce(string_agg(nspname, ','), '') FROM pg_namespace "
        "WHERE nspname LIKE 'p02_uow_%'",
    )
    if _LOOP is not None:
        _LOOP.close()
    assert not leftovers, f"test schemas were left behind: {leftovers}"


def uow(world: World, auth_revision: int | None = None) -> SqlStateUnitOfWork:
    """A UoW on the default ``StorageConfig`` (no search path)."""
    return SqlStateUnitOfWork(
        engine_for(world.schema, search_path=False),
        world.schema,
        actor(auth_revision or world.auth_revision, world=world),
    )


def classify(exc: AgentRuntimeError) -> str:
    return exc.failure.code


def count(world: World, table: str, where: str = "TRUE", **params: Any) -> int:
    return int(
        scalar(world.schema, f"SELECT count(*) FROM {table} WHERE {where}", **params)  # noqa: S608
    )


def live_works(world: World, *, run_id: uuid.UUID | None = None) -> int:
    return count(
        world,
        "work_item",
        "run_id = :run AND status IN ('pending', 'leased')",
        run=run_id or world.run_id,
    )


# --------------------------------------------------------------------------- seals


def record_seal(
    world: World,
    *,
    item_id: uuid.UUID,
    attempt_id: uuid.UUID,
    content_digest: str,
    analysis_digest: str,
    content_head: tuple[int, int] | None,
    analysis_head: tuple[int, int] | None,
    durable_seq: int,
    sealed_revision: int = 1,
    proof_id: uuid.UUID | None = None,
    session_id: uuid.UUID | None = None,
    run_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    """Persist one sealed producer through the public writer entry point."""
    proof_id = proof_id or uuid.uuid4()
    proof = FlushProof.model_validate(
        {
            "proofId": str(proof_id),
            "scopeId": world.scope,
            "sessionId": str(session_id or world.session_id),
            "runId": str(run_id or world.run_id),
            "attemptId": str(attempt_id),
            "itemId": str(item_id),
            "durableSeq": str(durable_seq),
            "channels": [
                {
                    "channel": "content",
                    "lastDeltaIndex": None if content_head is None else str(content_head[0]),
                    "lastSessionSeq": None if content_head is None else str(content_head[1]),
                    "digest": content_digest,
                },
                {
                    "channel": "analysis",
                    "lastDeltaIndex": None if analysis_head is None else str(analysis_head[0]),
                    "lastSessionSeq": None if analysis_head is None else str(analysis_head[1]),
                    "digest": analysis_digest,
                },
            ],
        }
    )
    run(
        lambda: uow(world).record_output_seal(
            proof=proof,
            sealed_revision=sealed_revision,
        )
    )
    return {
        "proof_id": proof_id,
        "item_id": item_id,
        "attempt_id": attempt_id,
        "content_digest": content_digest,
        "analysis_digest": analysis_digest,
        "content_head": content_head,
        "analysis_head": analysis_head,
        "durable_seq": durable_seq,
        "sealed_revision": sealed_revision,
    }


def verified_evidence(
    world: World,
    seals: list[dict[str, Any]],
    *,
    enumerated: int | None = None,
    digests: dict[int, tuple[str, str]] | None = None,
    heads: dict[int, tuple[tuple[int, int] | None, tuple[int, int] | None]] | None = None,
    watermarks: dict[int, int] | None = None,
    scope_id: str | None = None,
    session_id: uuid.UUID | None = None,
    run_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    """Build a ``VerifiedEvidence`` payload from persisted seals (with optional tampering)."""
    proofs: list[dict[str, Any]] = []
    for index, seal in enumerate(seals):
        content_digest, analysis_digest = seal["content_digest"], seal["analysis_digest"]
        content_head, analysis_head = seal["content_head"], seal["analysis_head"]
        if digests is not None and index in digests:
            content_digest, analysis_digest = digests[index]
        if heads is not None and index in heads:
            content_head, analysis_head = heads[index]
        durable = seal["durable_seq"] if watermarks is None else watermarks[index]
        proofs.append(
            {
                "proofId": str(seal["proof_id"]),
                "scopeId": scope_id or world.scope,
                "sessionId": str(session_id or world.session_id),
                "runId": str(run_id or world.run_id),
                "attemptId": str(seal["attempt_id"]),
                "itemId": str(seal["item_id"]),
                "durableSeq": str(durable),
                "channels": [
                    {
                        "channel": "content",
                        "lastDeltaIndex": (None if content_head is None else str(content_head[0])),
                        "lastSessionSeq": (None if content_head is None else str(content_head[1])),
                        "digest": content_digest,
                    },
                    {
                        "channel": "analysis",
                        "lastDeltaIndex": (
                            None if analysis_head is None else str(analysis_head[0])
                        ),
                        "lastSessionSeq": (
                            None if analysis_head is None else str(analysis_head[1])
                        ),
                        "digest": analysis_digest,
                    },
                ],
            }
        )
    highest = max((int(proof["durableSeq"]) for proof in proofs), default=0)
    return {
        "kind": "verified",
        "enumeratedSessionSeq": str(enumerated if enumerated is not None else max(highest, 1)),
        "proofs": proofs,
    }


def terminal_request(
    world: World,
    *,
    evidence_payload: dict[str, Any] | None = None,
    executions: tuple[dict[str, Any], ...] = (),
    result_status: str = "completed",
    status: str | None = None,
    desired_control: str | None = None,
    control_command_id: uuid.UUID | None = None,
) -> TerminalCommitRequest:
    result: dict[str, Any] = {
        "status": result_status,
        "run_id": world.run_id,
        "final_items": (),
        "artifact_refs": (),
        "partial_results": (),
        "outcome": "success" if result_status == "completed" else None,
        "failure": None,
        "usage": {
            "status": "unknown",
            "reason": "provider_not_reported",
            "input_tokens": None,
            "output_tokens": None,
            "total_tokens": None,
        },
    }
    if result_status == "failed":
        result["failure"] = {
            "code": "INTERNAL",
            "category": "internal",
            "retryable": False,
            "publicMessage": "injected failure",
        }
    return TerminalCommitRequest.model_validate(
        {
            "guard": state_guard(
                world,
                status=status or world.status,
                desired_control=desired_control,
                control_command_id=control_command_id,
            ),
            "checkpoint_id": world.checkpoint_id,
            "evidence": evidence_payload or no_producers(world),
            "executions": executions,
            "result": result,
        }
    )


def pause_request(world: World, *, command_id: uuid.UUID) -> PauseCommitRequest:
    return PauseCommitRequest.model_validate(
        {
            "guard": state_guard(world, desired_control="pause", control_command_id=command_id),
            "checkpoint_id": world.checkpoint_id,
            "evidence": no_producers(world),
            "executions": (),
        }
    )


def cancel_request(
    world: World,
    *,
    command_id: uuid.UUID,
    evidence_payload: dict[str, Any] | None = None,
    executions: tuple[dict[str, Any], ...] = (),
    checkpoint_id: uuid.UUID | None = None,
) -> CancelCommitRequest:
    return CancelCommitRequest.model_validate(
        {
            "guard": state_guard(
                world,
                status="cancelling",
                desired_control="cancel",
                control_command_id=command_id,
            ),
            "checkpoint_id": checkpoint_id or world.checkpoint_id,
            "evidence": evidence_payload or no_producers(world),
            "executions": executions,
        }
    )


def intake(world: World, kind: str, key: str, *, expected_version: str | None = None) -> Any:
    return run(
        lambda: uow(world).record_control(
            ControlRequest(
                run_id=world.run_id,
                kind=kind,
                idempotency_key=key,
                expected_version=expected_version or str(world.state_version),
            )
        )
    )


def advance_unit(world: World) -> SqlStateUnitOfWork:
    return uow(world)


# --------------------------------------------------------------------------- commits


def test_commit_advance_moves_the_state_and_fences_the_old_lease(world):
    unit = uow(world)
    result = run(lambda: unit.commit_advance(advance_request(world)))
    assert result.state_version == str(world.state_version + 1)
    assert result.checkpoint_id == world.checkpoint_id
    assert (
        scalar(world.schema, "SELECT state_version FROM run WHERE id = :id", id=world.run_id)
        == world.state_version + 1
    )
    assert (
        scalar(world.schema, "SELECT status FROM work_item WHERE id = :id", id=world.work_id)
        == "completed"
    )
    # The claimed work item is finished, so the old lease is fenced before any CAS.
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: unit.commit_advance(advance_request(world)))
    assert classify(error.value) == "FENCED"


def test_two_connections_committing_the_same_version_have_exactly_one_winner(world):
    assert DSN is not None
    outcomes: list[str] = []

    def attempt() -> None:
        engine = engine_for(world.schema, pool_size=1, search_path=False)
        unit = SqlStateUnitOfWork(engine, world.schema, actor(world.auth_revision, world=world))

        async def main() -> str:
            try:
                await unit.commit_advance(advance_request(world))
                return "committed"
            except AgentRuntimeError as exc:
                return f"refused:{exc.failure.code}"
            finally:
                await engine.dispose()

        result = asyncio.run(main())
        with _LOCK:
            outcomes.append(result)

    threads = [threading.Thread(target=attempt) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(120)
    assert outcomes.count("committed") == 1, outcomes
    loser = next(item for item in outcomes if item != "committed")
    assert loser in {"refused:FENCED", "refused:VERSION_CONFLICT"}, outcomes
    assert (
        scalar(world.schema, "SELECT state_version FROM run WHERE id = :id", id=world.run_id)
        == world.state_version + 1
    )
    assert count(world, "operation", "task_id = :task", task=world.task_id) == 0


def test_stale_facts_are_each_refused_with_a_stable_classification(world):
    unit = uow(world)
    cases = [
        ("slot", lease(world, slot_version="99"), "FENCED"),
        ("owner", lease(world, owner_id="other"), "FENCED"),
        ("run epoch", lease(world, run_epoch="99"), "FENCED"),
        ("work epoch", lease(world, work_epoch="99"), "FENCED"),
        (
            "expired run lease",
            lease(world, run_lease_until=datetime.now(UTC) - timedelta(seconds=1)),
            "FENCED",
        ),
        (
            "expired work lease",
            lease(world, work_lease_until=datetime.now(UTC) - timedelta(seconds=1)),
            "FENCED",
        ),
        ("state version", lease(world), "VERSION_CONFLICT"),
    ]
    for name, token, expected in cases:
        expected_version = "99" if name == "state version" else str(world.state_version)
        request = advance_request(
            world,
            guard=state_guard(world, lease=token, expected_state_version=expected_version),
        )
        with pytest.raises(AgentRuntimeError) as error:
            run(lambda request=request: unit.commit_advance(request))
        assert classify(error.value) == expected, name
    assert (
        scalar(world.schema, "SELECT state_version FROM run WHERE id = :id", id=world.run_id) == 1
    )


def test_stale_authority_and_revoked_grants_are_refused(world):
    unit = uow(world)
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: unit.commit_advance(
                advance_request(world, guard=state_guard(world, actor=actor(9, world=world)))
            )
        )
    assert classify(error.value) == "FORBIDDEN"

    async def bump_fence() -> None:
        engine = engine_for(world.schema)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        "UPDATE run SET authority_revision = authority_revision + 1 "
                        "WHERE scope_id = :scope AND id = :run"
                    ),
                    {"scope": world.scope, "run": world.run_id},
                )
        finally:
            await engine.dispose()

    run(bump_fence)
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: unit.commit_advance(advance_request(world)))
    assert classify(error.value) == "FORBIDDEN"

    async def revoke() -> None:
        engine = engine_for(world.schema)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text("UPDATE capability_grant SET revoked_at = :now WHERE run_id = :run"),
                    {"now": datetime.now(UTC), "run": world.run_id},
                )
                await connection.execute(
                    sa.text("UPDATE run SET authority_revision = 1 WHERE id = :run"),
                    {"run": world.run_id},
                )
        finally:
            await engine.dispose()

    run(revoke)
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: unit.commit_advance(advance_request(world)))
    assert classify(error.value) == "FORBIDDEN"


def test_terminal_commit_requires_matching_guard_and_clears_control(world):
    unit = uow(world)
    parsed = result_adapter.validate_python(
        {
            "status": "cancelled",
            "run_id": world.run_id,
            "final_items": (),
            "artifact_refs": (),
            "partial_results": (),
            "outcome": None,
            "failure": None,
            "usage": {
                "status": "unknown",
                "reason": "provider_not_reported",
                "input_tokens": None,
                "output_tokens": None,
                "total_tokens": None,
            },
        }
    )
    assert parsed.status == "cancelled"
    # A cancelled terminal needs the cancelling state; the frozen request accepts it, but the
    # transaction refuses because the stored run is not cancelling.
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: unit.commit_terminal(
                terminal_request(
                    world,
                    executions=(),
                    result_status="cancelled",
                    status="cancelling",
                    desired_control="cancel",
                    control_command_id=uuid.uuid4(),
                )
            )
        )
    assert classify(error.value) == "VERSION_CONFLICT"
    # The request contract also refuses a completion guard that is not the running state.
    with pytest.raises(ValidationError):
        terminal_request(world, status="waiting_user", result_status="completed")
    # A guard that claims running while the database says otherwise is refused in-transaction
    # (the DTO cannot know the stored state).
    run(lambda: _set_run_status(world, "waiting_user"))
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: unit.commit_terminal(
                terminal_request(world, result_status="completed", status="running")
            )
        )
    assert classify(error.value) == "VERSION_CONFLICT"
    run(lambda: _set_run_status(world, "running"))
    assert (
        scalar(world.schema, "SELECT status FROM run WHERE id = :id", id=world.run_id) == "running"
    )


def test_pause_and_resume_lifecycle_with_deduplicated_resume_work(world):
    unit = uow(world)
    receipt = intake(world, "pause", "pause-lifecycle")
    paused = run(
        lambda: uow(world).commit_pause(pause_request(world, command_id=receipt.command_id))
    )
    assert paused.status == RunStatus.PAUSED
    assert (
        scalar(world.schema, "SELECT status FROM run WHERE id = :id", id=world.run_id) == "paused"
    )
    assert scalar(world.schema, "SELECT owner_id FROM run WHERE id = :id", id=world.run_id) is None
    assert (
        scalar(
            world.schema,
            "SELECT owner_run_id FROM session_slot WHERE session_id = :id",
            id=world.session_id,
        )
        is None
    )

    resume = ResumeCommitRequest.model_validate(
        {
            "actor": actor(world.auth_revision, world=world),
            "session_id": world.session_id,
            "run_id": world.run_id,
            "command_id": uuid.uuid4(),
            "expected_state_version": "2",
            "expected_cancel_version": "0",
            "checkpoint_id": world.checkpoint_id,
            "idempotency_key": "resume-lifecycle",
        }
    )
    first = run(lambda: uow(world).resume(resume))
    replay = run(lambda: uow(world).resume(resume))
    assert first.work_id == replay.work_id
    assert live_works(world) == 1
    assert first.status == "queued"
    # A second resume while the run is queued (not paused) is refused.
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: uow(world).resume(
                ResumeCommitRequest.model_validate(
                    {
                        **resume.model_dump(),
                        "command_id": str(uuid.uuid4()),
                        "idempotency_key": "resume-lifecycle-2",
                        "expected_state_version": "3",
                    }
                )
            )
        )
    assert classify(error.value) == "VERSION_CONFLICT"
    assert unit.schema == world.schema


def test_control_intake_is_worker_visible_and_separate_from_application(world):
    unit = uow(world)
    request = ControlRequest(
        run_id=world.run_id, kind="pause", idempotency_key="intake-once", expected_version="1"
    )
    first = run(lambda: unit.record_control(request))
    second = run(lambda: unit.record_control(request))
    assert first.command_id == second.command_id
    assert second.public_reason == "command already recorded"
    # Worker-visible pending control, but the run is *not* paused until the safe point.
    row = run(
        lambda: _row(
            world,
            "SELECT status, desired_control, control_command_id FROM run WHERE id = :id",
            id=world.run_id,
        )
    )
    assert row["status"] == "running"
    assert row["desired_control"] == "pause"
    assert row["control_command_id"] == first.command_id
    assert count(world, "command", "run_id = :run", run=world.run_id) == 1


def test_control_intake_refuses_resume_and_illegal_status(world):
    """Intake must not write a pending control the state machine cannot reach."""
    unit = uow(world)
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: unit.record_control(
                ControlRequest(
                    run_id=world.run_id,
                    kind="resume",
                    idempotency_key="intake-resume",
                    expected_version="1",
                )
            )
        )
    assert classify(error.value) == "INVALID_INPUT"
    receipt = intake(world, "pause", "intake-pause-once")
    run(lambda: unit.commit_pause(pause_request(world, command_id=receipt.command_id)))
    # A second pause on an already paused run is a structured refusal, not a CHECK error.
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: unit.record_control(
                ControlRequest(
                    run_id=world.run_id,
                    kind="pause",
                    idempotency_key="intake-pause-twice",
                    expected_version="2",
                )
            )
        )
    assert classify(error.value) in {"VERSION_CONFLICT", "CONTROL_PENDING", "INVALID_INPUT"}
    assert (
        scalar(world.schema, "SELECT status FROM run WHERE id = :id", id=world.run_id) == "paused"
    )


async def _row(world: World, sql: str, **params: Any) -> dict[str, Any]:
    engine = engine_for(world.schema)
    try:
        async with engine.connect() as connection:
            row = (await connection.execute(sa.text(sql), params)).mappings().one_or_none()
            return {} if row is None else dict(row)
    finally:
        await engine.dispose()


# --------------------------------------------------------------------------- mailbox


def test_apply_input_consumes_a_contiguous_batch_atomically(world):
    token = uuid.uuid4()
    ids = run(lambda: _seed_messages(world, (1, 2), allocated=3, claim_token=token))
    request = ApplyInputRequest.model_validate(
        {
            "guard": guard(world),
            "inputs": [
                {"message_id": str(ids[0]), "claim_token": str(token), "recipient_seq": "1"},
                {"message_id": str(ids[1]), "claim_token": str(token), "recipient_seq": "2"},
            ],
            "expected_applied_seq": "0",
            "checkpoint_ref": f"state:{world.state_version + 1}",
        }
    )
    applied = run(lambda: uow(world).apply_input(request))
    assert applied.applied_seq == "2"
    assert len(applied.messages) == 2
    assert (
        scalar(
            world.schema,
            "SELECT applied_seq FROM mailbox_cursor WHERE recipient_run_id = :id",
            id=world.run_id,
        )
        == 2
    )
    assert count(world, "agent_message", "status = 'applied'") == 2
    assert (
        scalar(
            world.schema, "SELECT applied_input_version FROM run WHERE id = :id", id=world.run_id
        )
        == 2
    )


def test_apply_input_refuses_gaps_wrong_tokens_and_expired_claims(world):
    token = uuid.uuid4()
    ids = run(lambda: _seed_messages(world, (1, 3), allocated=3, claim_token=token))
    gap = ApplyInputRequest.model_validate(
        {
            "guard": guard(world),
            "inputs": [
                {"message_id": str(ids[0]), "claim_token": str(token), "recipient_seq": "1"},
                {"message_id": str(ids[1]), "claim_token": str(token), "recipient_seq": "2"},
            ],
            "expected_applied_seq": "0",
            "checkpoint_ref": f"state:{world.state_version + 1}",
        }
    )
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: uow(world).apply_input(gap))
    assert classify(error.value) == "INVALID_INPUT"
    assert (
        scalar(
            world.schema,
            "SELECT applied_seq FROM mailbox_cursor WHERE recipient_run_id = :id",
            id=world.run_id,
        )
        == 0
    )
    assert count(world, "agent_message", "status = 'claimed'") == 2

    wrong = ApplyInputRequest.model_validate(
        {
            "guard": guard(world),
            "inputs": [
                {"message_id": str(ids[0]), "claim_token": str(uuid.uuid4()), "recipient_seq": "1"}
            ],
            "expected_applied_seq": "0",
            "checkpoint_ref": f"state:{world.state_version + 1}",
        }
    )
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: uow(world).apply_input(wrong))
    assert classify(error.value) == "FENCED"


# --------------------------------------------------------------------------- domain


def test_commit_domain_cas_and_operation_idempotency(world):
    operation_id = uuid.uuid4()
    request = DomainCommitRequest.model_validate(
        {
            "guard": guard(world),
            "operation_id": str(operation_id),
            "resource": {
                "resourceType": "workspace",
                "resourceId": str(world.workspace_id),
                "resourceVersion": "1",
                "scope": {"scopeId": world.scope, "sessionId": str(world.session_id)},
            },
            "action": "checkpoint",
            "changeRef": "change:1",
        }
    )
    updated = run(lambda: uow(world).commit_domain(request))
    assert updated.resource_version == "2"
    assert (
        scalar(world.schema, "SELECT revision FROM workspace WHERE id = :id", id=world.workspace_id)
        == 2
    )
    replay = run(lambda: uow(world).commit_domain(request))
    assert replay.resource_version == "2"
    assert (
        scalar(world.schema, "SELECT revision FROM workspace WHERE id = :id", id=world.workspace_id)
        == 2
    )
    assert (
        scalar(world.schema, "SELECT status FROM operation WHERE task_id = :id", id=world.task_id)
        == "committed"
    )

    stale = DomainCommitRequest.model_validate(
        {
            **request.model_dump(),
            "operation_id": str(uuid.uuid4()),
            "resource": {
                "resourceType": "workspace",
                "resourceId": str(world.workspace_id),
                "resourceVersion": "1",
                "scope": {"scopeId": world.scope},
            },
        }
    )
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: uow(world).commit_domain(stale))
    assert classify(error.value) == "VERSION_CONFLICT"

    unregistered = DomainCommitRequest.model_validate(
        {
            **request.model_dump(),
            "operation_id": str(uuid.uuid4()),
            "resource": {
                "resourceType": "artifact",
                "resourceId": str(world.workspace_id),
                "resourceVersion": "1",
                "scope": {"scopeId": world.scope},
            },
        }
    )
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: uow(world).commit_domain(unregistered))
    assert classify(error.value) == "FORBIDDEN"


def test_same_operation_identity_cannot_touch_two_resources(world):
    assert DSN is not None
    second_workspace = run(lambda: _seed_second_workspace(world))
    operation_id = uuid.uuid4()

    def attempt(workspace_id: uuid.UUID) -> str:
        engine = engine_for(world.schema, pool_size=1, search_path=False)
        unit = SqlStateUnitOfWork(engine, world.schema, actor(world.auth_revision, world=world))
        request = DomainCommitRequest.model_validate(
            {
                "guard": guard(world),
                "operation_id": str(operation_id),
                "resource": {
                    "resourceType": "workspace",
                    "resourceId": str(workspace_id),
                    "resourceVersion": "1",
                    "scope": {"scopeId": world.scope},
                },
                "action": "repair",
                "changeRef": "change:repair",
            }
        )

        async def main() -> str:
            try:
                await unit.commit_domain(request)
                return "committed"
            except AgentRuntimeError as exc:
                return f"refused:{exc.failure.code}"
            finally:
                await engine.dispose()

        return asyncio.run(main())

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, [world.workspace_id, second_workspace]))
    assert results.count("committed") == 1, results
    versions = [
        scalar(world.schema, "SELECT revision FROM workspace WHERE id = :id", id=resource)
        for resource in (world.workspace_id, second_workspace)
    ]
    assert sorted(versions) == [1, 2], versions
    assert count(world, "operation", "task_id = :task", task=world.task_id) == 1


# --------------------------------------------------------------------------- capacity


def capacity_key(deployment: str, key: str = "group-a") -> CapacityKey:
    return CapacityKey(
        deployment_id=deployment,
        resource_kind="run_slot",
        resource_version="1",
        resource_key=key,
    )


def reserve_request(key: CapacityKey, request_id: str, amount: int = 1) -> CapacityReserveRequest:
    now = datetime.now(UTC)
    return CapacityReserveRequest(
        key=key,
        amount=amount,
        holder_ref=f"run:{request_id}",
        request_id=request_id,
        lease_until=now + timedelta(minutes=10),
        deadline_at=now + timedelta(minutes=30),
    )


def test_capacity_reserve_never_oversells_and_replays_idempotently(world):
    assert DSN is not None
    store = SqlSystemCapacityStore(engine_for(world.schema, pool_size=6), world.schema)
    key = capacity_key(world.schema)
    run(lambda: store.seed(key, capacity=2))
    requests = {index: reserve_request(key, f"req-{index}") for index in range(8)}
    winners: list[str] = []

    def attempt(index: int) -> None:
        engine = engine_for(world.schema, pool_size=1)
        local = SqlSystemCapacityStore(engine, world.schema)

        async def main() -> str:
            try:
                reservation = await local.reserve(requests[index])
                return f"reserved:{index}:{reservation.reservation_id}"
            except AgentRuntimeError as exc:
                return f"refused:{exc.failure.code}"
            finally:
                await engine.dispose()

        result = asyncio.run(main())
        with _LOCK:
            winners.append(result)

    threads = [threading.Thread(target=attempt, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(180)
    reserved = [item for item in winners if item.startswith("reserved")]
    refused = [item for item in winners if item.startswith("refused")]
    assert len(reserved) == 2, winners
    assert all(item == "refused:CAPACITY_UNAVAILABLE" for item in refused), winners
    assert run(lambda: store.snapshot(key))["aggregate"]["reserved"] == 2
    winner_index, winner_reservation = reserved[0].split(":")[1:]
    replay = run(lambda: store.reserve(requests[int(winner_index)]))
    assert str(replay.reservation_id) == winner_reservation
    assert run(lambda: store.snapshot(key))["aggregate"]["reserved"] == 2


def test_tool_group_slots_are_shared_between_worker_instances(world):
    fixture = json.loads(
        files("agent_runtime.contracts").joinpath("fixtures/tools.json").read_text()
    )
    spec = ToolSpec.model_validate(fixture["spec"])
    base = ToolInvocation.model_validate(fixture["invocation"])
    deadline = datetime.now(UTC) + timedelta(minutes=2)
    actor = base.actor.model_copy(update={"scope_id": world.scope})
    calls = [
        base.model_copy(
            update={
                "actor": actor,
                "call": base.call.model_copy(update={"tool_call_id": uuid.uuid4()}),
                "deadline_at": deadline,
            }
        )
        for _ in range(2)
    ]
    first_engine = engine_for(world.schema, pool_size=2)
    second_engine = engine_for(world.schema, pool_size=2)
    first_store = SqlSystemCapacityStore(first_engine, world.schema)
    first = SqlToolCapacity(first_store, deployment_id="test-tool-workers")
    second = SqlToolCapacity(
        SqlSystemCapacityStore(second_engine, world.schema),
        deployment_id="test-tool-workers",
    )

    async def verify() -> None:
        try:
            key = first.key(world.scope, spec)
            assert key == second.key(world.scope, spec)
            await first_store.seed(key, capacity=1)
            results = await asyncio.gather(
                first.reserve(calls[0], spec),
                second.reserve(calls[1], spec),
                return_exceptions=True,
            )
            won = [
                (index, result)
                for index, result in enumerate(results)
                if not isinstance(result, BaseException)
            ]
            rejected = [result for result in results if isinstance(result, AgentRuntimeError)]
            assert len(won) == len(rejected) == 1
            assert rejected[0].failure.code == "CAPACITY_UNAVAILABLE"
            winner, reservation = won[0]
            await (first if winner == 0 else second).release(calls[winner], reservation)
            loser = 1 - winner
            later = await (first if loser == 0 else second).reserve(calls[loser], spec)
            await (first if loser == 0 else second).release(calls[loser], later)
            assert (await first_store.observe(CapacityObserveRequest(key=key))).reserved == "0"
        finally:
            await first_engine.dispose()
            await second_engine.dispose()

    run(verify)


def test_capacity_release_joins_result_transaction(world):
    engine = engine_for(world.schema, pool_size=2)
    store = SqlSystemCapacityStore(engine, world.schema)
    key = capacity_key(world.schema, "tool-result-transaction")

    async def verify() -> None:
        try:
            await store.seed(key, capacity=1)
            reservation = await store.reserve(reserve_request(key, "tool-terminal"))
            release = CapacityReleaseRequest(
                key=key,
                reservation_id=reservation.reservation_id,
                request_id="tool-terminal-release",
                reason="settled",
                holder_state="terminal",
            )
            with pytest.raises(RuntimeError, match="seal failed"):
                async with engine.begin() as db:
                    assert (await store.release_in_transaction(db, release)).outcome == "applied"
                    raise RuntimeError("seal failed")
            assert (await store.observe(CapacityObserveRequest(key=key))).reserved == "1"
            async with engine.begin() as db:
                assert (await store.release_in_transaction(db, release)).outcome == "applied"
            assert (await store.observe(CapacityObserveRequest(key=key))).reserved == "0"
        finally:
            await engine.dispose()

    run(verify)


def test_capacity_release_applied_reused_rejected_and_holder_confirmation(world):
    assert DSN is not None
    store = SqlSystemCapacityStore(engine_for(world.schema, pool_size=4), world.schema)
    key = capacity_key(world.schema, "group-b")
    run(lambda: store.seed(key, capacity=3))
    reservation = run(lambda: store.reserve(reserve_request(key, "rel-1")))
    release = CapacityReleaseRequest(
        key=key,
        reservation_id=reservation.reservation_id,
        request_id="release-1",
        reason="settled",
        holder_state="stopped",
    )
    assert run(lambda: store.release(release)).outcome == "applied"
    assert run(lambda: store.snapshot(key))["aggregate"]["reserved"] == 0
    assert run(lambda: store.release(release)).outcome == "reused"
    different_request = release.model_copy(update={"request_id": "release-2"})
    assert run(lambda: store.release(different_request)).outcome == "rejected"

    second = run(lambda: store.reserve(reserve_request(key, "rel-2")))
    unknown_holder = CapacityReleaseRequest(
        key=key,
        reservation_id=second.reservation_id,
        request_id="release-3",
        reason="expired",
        holder_state="unknown",
    )
    assert run(lambda: store.release(unknown_holder)).outcome == "rejected"
    reclaimed = unknown_holder.model_copy(
        update={"request_id": "release-4", "reason": "reclaimed", "holder_state": "terminal"}
    )
    assert run(lambda: store.release(reclaimed)).outcome == "applied"
    assert run(lambda: store.observe(CapacityObserveRequest(key=key))).reserved == "0"


def test_capacity_release_identity_hash_covers_holder_state(world):
    """Q44: the persisted canonical hash distinguishes every release payload field."""
    assert DSN is not None
    store = SqlSystemCapacityStore(engine_for(world.schema, pool_size=2), world.schema)
    key = capacity_key(world.schema, "group-hash")
    run(lambda: store.seed(key, capacity=2))
    reservation = run(lambda: store.reserve(reserve_request(key, "hash-req")))
    release = CapacityReleaseRequest(
        key=key,
        reservation_id=reservation.reservation_id,
        request_id="hash-release",
        reason="expired",
        holder_state="stopped",
    )
    assert run(lambda: store.release(release)).outcome == "applied"
    stored = scalar(
        world.schema,
        "SELECT release_request_hash FROM system_capacity_reservation WHERE id = :id",
        id=reservation.reservation_id,
    )
    assert isinstance(stored, str) and len(stored) == 64
    assert stored == release_request_digest(release)
    assert run(lambda: store.release(release)).outcome == "reused"
    # Same request id, different holder_state: rejected, and the aggregate stays at zero.
    tampered = release.model_copy(update={"holder_state": "unknown"})
    rejected = run(lambda: store.release(tampered))
    assert rejected.outcome == "rejected"
    assert run(lambda: store.snapshot(key))["aggregate"]["reserved"] == 0
    # A different reason is a different identity as well.
    other_reason = release.model_copy(update={"reason": "reclaimed"})
    assert run(lambda: store.release(other_reason)).outcome == "rejected"


# --------------------------------------------------------------------------- faults


def test_fault_injection_rolls_back_every_table(world, monkeypatch):
    unit = uow(world)
    before = {
        "run": scalar(
            world.schema, "SELECT state_version FROM run WHERE id = :id", id=world.run_id
        ),
        "work": scalar(
            world.schema, "SELECT status FROM work_item WHERE id = :id", id=world.work_id
        ),
        "command": count(world, "command", "run_id = :run", run=world.run_id),
        "checkpoint": count(world, "checkpoint", "run_id = :run", run=world.run_id),
        "operation": count(world, "operation", "task_id = :task", task=world.task_id),
        "seal": count(world, "output_seal", "run_id = :run", run=world.run_id),
    }

    async def boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("injected failure")

    windows = [
        ("beforeCommit", "_session_seq"),
        ("afterFirstUpdate", "_finish_work"),
        ("beforeStateWrite", "_verify_checkpoint"),
    ]
    for name, method in windows:
        monkeypatch.setattr(SqlStateUnitOfWork, method, boom)
        with pytest.raises(RuntimeError):
            run(lambda: unit.commit_advance(advance_request(world)))
        monkeypatch.undo()
        after = {
            "run": scalar(
                world.schema, "SELECT state_version FROM run WHERE id = :id", id=world.run_id
            ),
            "work": scalar(
                world.schema, "SELECT status FROM work_item WHERE id = :id", id=world.work_id
            ),
            "command": count(world, "command", "run_id = :run", run=world.run_id),
            "checkpoint": count(world, "checkpoint", "run_id = :run", run=world.run_id),
            "operation": count(world, "operation", "task_id = :task", task=world.task_id),
            "seal": count(world, "output_seal", "run_id = :run", run=world.run_id),
        }
        assert after == before, name


# --------------------------------------------------------------------------- P1-3 scope


def test_uow_uses_the_explicit_schema_not_the_search_path(world):
    assert DSN is not None
    default_config = StorageConfig(dsn=DSN, schema=world.schema, pool_size=2, max_overflow=0)
    assert default_config.set_search_path is False
    unit = SqlStateUnitOfWork(
        create_storage_engine(default_config), world.schema, actor(world=world)
    )
    assert run(lambda: unit.commit_advance(advance_request(world))).state_version == "2"
    wrong = SqlStateUnitOfWork(
        create_storage_engine(StorageConfig(dsn=DSN, schema="public", pool_size=1, max_overflow=0)),
        "public",
        actor(world=world),
    )
    with pytest.raises(sa.exc.ProgrammingError):
        run(lambda: wrong.commit_advance(advance_request(world)))


# =====================================================================================
# Second repair: P1-1 durable producer seals
# --------------------------------------------------------------------------- P1-1 #1


def test_sealed_producer_accepts_multi_delta_cumulative_heads(world):
    """A real producer emits many deltas per channel; only its cumulative seal counts."""
    unit = uow(world)
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    content_digest, analysis_digest = "a" * 64, "b" * 64
    # Many deltas happened; the seal stores the *cumulative* digests and channel heads.
    seal = record_seal(
        world,
        item_id=item_id,
        attempt_id=attempt_id,
        content_digest=content_digest,
        analysis_digest=analysis_digest,
        content_head=(7, 9),
        analysis_head=(4, 10),
        durable_seq=10,
        sealed_revision=1,
    )
    result = run(
        lambda: unit.commit_advance(
            advance_request(world, evidence=verified_evidence(world, [seal], enumerated=10))
        )
    )
    assert result.state_version == "2"
    assert count(world, "output_seal", "run_id = :run", run=world.run_id) == 1
    # The single-event digest column is untouched by the seal semantics.
    assert (
        scalar(
            world.schema,
            "SELECT content_last_delta_index FROM output_seal WHERE run_id = :run",
            run=world.run_id,
        )
        == 7
    )


def test_seal_proof_digest_channel_head_and_watermark_mismatches(world):
    unit = uow(world)
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    seal = record_seal(
        world,
        item_id=item_id,
        attempt_id=attempt_id,
        content_digest="a" * 64,
        analysis_digest="b" * 64,
        content_head=(3, 5),
        analysis_head=(2, 6),
        durable_seq=6,
    )
    cases = {
        "digest": verified_evidence(world, [seal], digests={0: ("c" * 64, "b" * 64)}),
        "channel head": verified_evidence(world, [seal], heads={0: ((4, 5), (2, 6))}),
        "channel watermark": verified_evidence(world, [seal], heads={0: ((3, 4), (2, 6))}),
    }
    for name, payload in cases.items():
        with pytest.raises(AgentRuntimeError) as error:
            run(
                lambda payload=payload: unit.commit_advance(
                    advance_request(world, evidence=payload)
                )
            )
        assert classify(error.value) == "VERSION_CONFLICT", name
    # A proof whose own channel is ahead of its watermark cannot even be expressed: the
    # frozen request refuses it, so the stored-watermark comparison stays defence in depth.
    with pytest.raises(ValidationError):
        advance_request(world, evidence=verified_evidence(world, [seal], watermarks={0: 5}))
    assert (
        scalar(world.schema, "SELECT state_version FROM run WHERE id = :id", id=world.run_id) == 1
    )


# --------------------------------------------------------------------------- P1-1 #2


def test_evidence_refuses_missing_unsealed_and_contradicted_producers(world):
    unit = uow(world)
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    seal = record_seal(
        world,
        item_id=item_id,
        attempt_id=attempt_id,
        content_digest="a" * 64,
        analysis_digest="b" * 64,
        content_head=(1, 2),
        analysis_head=(1, 2),
        durable_seq=2,
    )
    # (a) omitting the sealed producer.
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: unit.commit_advance(advance_request(world, evidence=no_producers(world))))
    assert classify(error.value) == "INVALID_INPUT"
    # (b) presenting an unsealed producer.
    unsealed = verified_evidence(
        world,
        [
            seal,
            {
                **seal,
                "proof_id": uuid.uuid4(),
                "item_id": uuid.uuid4(),
            },
        ],
    )
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: unit.commit_advance(advance_request(world, evidence=unsealed)))
    assert classify(error.value) == "INVALID_INPUT"
    # (c) no_producers is also contradicted by a raw journal delta.
    other_item = run(lambda: _seed_item(world))
    run(
        lambda: _seed_event(
            world, sequence=1, item_id=other_item, attempt_id=attempt_id, digest="d" * 64
        )
    )
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: unit.commit_advance(advance_request(world, evidence=no_producers(world))))
    assert classify(error.value) == "INVALID_INPUT"
    # (d) the truthful set still commits.
    sealed_only = verified_evidence(world, [seal], enumerated=2)
    assert (
        run(lambda: unit.commit_advance(advance_request(world, evidence=sealed_only))).state_version
        == "2"
    )


def test_record_output_seal_waits_for_the_run_row_fence(world):
    """P1-1 check-then-append: the seal writer needs the same run-row fence."""
    assert DSN is not None
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    seal_kwargs = {
        "item_id": item_id,
        "attempt_id": attempt_id,
        "content_digest": "a" * 64,
        "analysis_digest": "b" * 64,
        "content_head": (1, 1),
        "analysis_head": (1, 1),
        "durable_seq": 1,
    }
    result: dict[str, Any] = {}

    def writer() -> None:
        engine = engine_for(world.schema, pool_size=1, search_path=False)
        unit = SqlStateUnitOfWork(engine, world.schema, actor(world=world))

        async def main() -> None:
            try:
                await unit.record_output_seal(
                    proof=FlushProof.model_validate(
                        {
                            "proofId": str(uuid.uuid4()),
                            "scopeId": world.scope,
                            "sessionId": str(world.session_id),
                            "runId": str(world.run_id),
                            "attemptId": str(seal_kwargs["attempt_id"]),
                            "itemId": str(seal_kwargs["item_id"]),
                            "durableSeq": "1",
                            "channels": [
                                {
                                    "channel": "content",
                                    "lastDeltaIndex": "1",
                                    "lastSessionSeq": "1",
                                    "digest": seal_kwargs["content_digest"],
                                },
                                {
                                    "channel": "analysis",
                                    "lastDeltaIndex": "1",
                                    "lastSessionSeq": "1",
                                    "digest": seal_kwargs["analysis_digest"],
                                },
                            ],
                        }
                    ),
                    sealed_revision=1,
                )
                result["outcome"] = "sealed"
            except Exception as exc:  # pragma: no cover - reported through the result
                result["outcome"] = f"{type(exc).__name__}: {exc}"
            finally:
                await engine.dispose()

        asyncio.run(main())

    async def blocker() -> None:
        engine = engine_for(world.schema)
        try:
            async with engine.connect() as connection:
                await connection.execute(sa.text("BEGIN"))
                await connection.execute(
                    sa.text("SELECT id FROM run WHERE scope_id = :scope AND id = :run FOR UPDATE"),
                    {"scope": world.scope, "run": world.run_id},
                )
                worker = threading.Thread(target=writer)
                worker.start()
                await asyncio.get_running_loop().run_in_executor(None, time.sleep, 1.5)
                # The writer holds the slot lock but cannot insert: the run row is held.
                blocked = await _scalar(
                    engine, "SELECT count(*) FROM output_seal WHERE run_id = :run", run=world.run_id
                )
                assert blocked == 0, "the seal was inserted before the run fence was released"
                await connection.execute(sa.text("COMMIT"))
                await asyncio.get_running_loop().run_in_executor(None, worker.join, 60)
        finally:
            await engine.dispose()

    run(blocker)
    assert result.get("outcome") == "sealed", result
    assert count(world, "output_seal", "run_id = :run", run=world.run_id) == 1


# --------------------------------------------------------------------------- P1-2 locks


def test_control_intake_takes_the_session_slot_fence(world):
    """Intake addresses the run, so it must still take the slot first (fixed order)."""
    assert DSN is not None
    outcome: dict[str, Any] = {}

    def controller() -> None:
        # A worker thread owns its own loop and engine; the module loop is already running
        # inside ``run(blocker)``.
        engine = engine_for(world.schema, pool_size=1, search_path=False)
        unit = SqlStateUnitOfWork(engine, world.schema, actor(world.auth_revision, world=world))

        async def main() -> None:
            try:
                try:
                    await unit.record_control(
                        ControlRequest(
                            run_id=world.run_id,
                            kind="pause",
                            idempotency_key="slot-fence",
                            expected_version=str(world.state_version),
                        )
                    )
                    outcome["state"] = "accepted"
                except Exception as exc:  # pragma: no cover - reported through the result
                    outcome["state"] = f"{type(exc).__name__}: {exc}"
            finally:
                await engine.dispose()

        asyncio.run(main())

    async def blocker() -> None:
        engine = engine_for(world.schema)
        try:
            async with engine.connect() as connection:
                await connection.execute(sa.text("BEGIN"))
                await connection.execute(
                    sa.text(
                        "SELECT session_id FROM session_slot WHERE scope_id = :scope "
                        "AND session_id = :session FOR UPDATE"
                    ),
                    {"scope": world.scope, "session": world.session_id},
                )
                worker = threading.Thread(target=controller)
                worker.start()
                await asyncio.get_running_loop().run_in_executor(None, time.sleep, 1.5)
                # Async scalar: the module sync helper would re-enter the running loop.
                blocked = await _scalar(
                    engine, "SELECT count(*) FROM command WHERE run_id = :run", run=world.run_id
                )
                assert blocked == 0, "intake wrote the command without the slot fence"
                await connection.execute(sa.text("COMMIT"))
                await asyncio.get_running_loop().run_in_executor(None, worker.join, 60)
        finally:
            await engine.dispose()

    run(blocker)
    assert outcome.get("state") == "accepted", outcome
    assert count(world, "command", "run_id = :run", run=world.run_id) == 1


def test_concurrent_pause_intake_and_advance_never_deadlock(world):
    """One run, two paths, the fixed lock order: no deadlock, one consistent state."""
    assert DSN is not None
    outcomes: list[str] = []

    def do_intake() -> None:
        try:
            intake(world, "pause", "race-intake")
            result = "intake:accepted"
        except AgentRuntimeError as exc:
            result = f"intake:refused:{exc.failure.code}"
        with _LOCK:
            outcomes.append(result)

    def do_advance() -> None:
        engine = engine_for(world.schema, pool_size=1, search_path=False)
        unit = SqlStateUnitOfWork(engine, world.schema, actor(world.auth_revision, world=world))

        async def main() -> str:
            try:
                await unit.commit_advance(advance_request(world))
                return "advance:committed"
            except AgentRuntimeError as exc:
                return f"advance:refused:{exc.failure.code}"
            except sa.exc.DBAPIError as exc:  # pragma: no cover - deadlock counterexample
                return f"advance:dbapi:{getattr(exc.orig, 'sqlstate', None)}"
            finally:
                await engine.dispose()

        result = asyncio.run(main())
        with _LOCK:
            outcomes.append(result)

    worker_intake = threading.Thread(target=do_intake)
    worker_advance = threading.Thread(target=do_advance)
    worker_intake.start()
    worker_advance.start()
    worker_intake.join(120)
    worker_advance.join(120)
    assert not any("40P01" in item for item in outcomes), outcomes
    assert outcomes.count("advance:committed") in {0, 1}, outcomes
    row = run(
        lambda: _row(
            world, "SELECT status, desired_control FROM run WHERE id = :id", id=world.run_id
        )
    )
    pending = row["desired_control"] is not None
    assert (row["status"] in {"running", "paused", "cancelling"}) or pending
    if pending:
        assert count(world, "command", "run_id = :run", run=world.run_id) == 1
        assert row["status"] in {"running", "paused", "cancelling"}


def test_locked_run_does_not_block_an_unrelated_run(schema, world):
    """No global table lock: holding run A's row must not stop run B's commit."""
    assert DSN is not None
    other = run(lambda: _seed(schema, "b", scope="scope-b", actor_id="actor-2"))
    outcome: dict[str, Any] = {}

    def committer() -> None:
        engine = engine_for(other.schema, pool_size=1, search_path=False)
        unit = SqlStateUnitOfWork(engine, other.schema, actor(other.auth_revision, world=other))

        async def main() -> None:
            try:
                await unit.commit_advance(advance_request(other))
                outcome["state"] = "committed"
            except Exception as exc:  # pragma: no cover - reported through the result
                outcome["state"] = f"{type(exc).__name__}: {exc}"
            finally:
                await engine.dispose()

        asyncio.run(main())

    async def blocker() -> None:
        engine = engine_for(world.schema)
        try:
            async with engine.connect() as connection:
                await connection.execute(sa.text("BEGIN"))
                await connection.execute(
                    sa.text("SELECT id FROM run WHERE scope_id = :scope AND id = :run FOR UPDATE"),
                    {"scope": world.scope, "run": world.run_id},
                )
                worker = threading.Thread(target=committer)
                worker.start()
                await asyncio.get_running_loop().run_in_executor(None, worker.join, 60)
                assert result_ready(outcome), "the unrelated run was blocked by a local row lock"
                await connection.execute(sa.text("COMMIT"))
        finally:
            await engine.dispose()

    run(blocker)
    assert outcome.get("state") == "committed", outcome
    assert (
        scalar(other.schema, "SELECT state_version FROM run WHERE id = :id", id=other.run_id) == 2
    )


def result_ready(outcome: dict[str, Any]) -> bool:
    assert "state" in outcome
    return True


def test_revoking_one_run_grant_does_not_block_another_run(schema, world):
    """Q46: the authority fence is per run; a revoke must not serialize other runs."""
    assert DSN is not None
    other = run(lambda: _seed(schema, "b", scope="scope-b", actor_id="actor-2"))
    outcome: dict[str, Any] = {}

    def committer() -> None:
        engine = engine_for(other.schema, pool_size=1, search_path=False)
        unit = SqlStateUnitOfWork(engine, other.schema, actor(other.auth_revision, world=other))

        async def main() -> None:
            try:
                await unit.commit_advance(advance_request(other))
                outcome["state"] = "committed"
            except Exception as exc:  # pragma: no cover - reported through the result
                outcome["state"] = f"{type(exc).__name__}: {exc}"
            finally:
                await engine.dispose()

        asyncio.run(main())

    async def blocker() -> None:
        engine = engine_for(world.schema)
        try:
            async with engine.connect() as connection:
                await connection.execute(sa.text("BEGIN"))
                # The port protocol: hold the run row, then revoke and bump the fence.
                await connection.execute(
                    sa.text("SELECT id FROM run WHERE scope_id = :scope AND id = :run FOR UPDATE"),
                    {"scope": world.scope, "run": world.run_id},
                )
                await connection.execute(
                    sa.text("UPDATE capability_grant SET revoked_at = :now WHERE run_id = :run"),
                    {"now": datetime.now(UTC), "run": world.run_id},
                )
                await connection.execute(
                    sa.text(
                        "UPDATE run SET authority_revision = authority_revision + 1 WHERE id = :run"
                    ),
                    {"run": world.run_id},
                )
                worker = threading.Thread(target=committer)
                worker.start()
                await asyncio.get_running_loop().run_in_executor(None, worker.join, 60)
                assert result_ready(outcome), "the revoke serialized an unrelated run"
                await connection.execute(sa.text("COMMIT"))
        finally:
            await engine.dispose()

    run(blocker)
    assert outcome.get("state") == "committed", outcome
    # The revoked run itself is fenced out.
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: uow(world).commit_advance(advance_request(world)))
    assert classify(error.value) == "FORBIDDEN"


# --------------------------------------------------------------------------- P1-3 control


def test_cancel_intake_is_worker_visible_and_creates_one_convergence_work(world):
    unit = uow(world)
    receipt = intake(world, "cancel", "cancel-converge")
    row = run(
        lambda: _row(
            world,
            "SELECT status, desired_control, control_command_id, owner_id FROM run WHERE id = :id",
            id=world.run_id,
        )
    )
    assert row["status"] == "cancelling"
    assert row["desired_control"] == "cancel"
    assert row["control_command_id"] == receipt.command_id
    convergence = run(
        lambda: _row(
            world,
            "SELECT id, dedup_key, kind, status FROM work_item WHERE run_id = :run "
            "AND status IN ('pending', 'leased') AND kind <> 'advance_run'",
            run=world.run_id,
        )
    )
    assert convergence["dedup_key"] == f"cancel:{receipt.command_id}"
    assert convergence["kind"] == "reconcile_execution"
    assert convergence["status"] == "pending"
    # Replay of the same request is idempotent: same command, still one convergence work.
    replay = run(
        lambda: unit.record_control(
            ControlRequest(
                run_id=world.run_id,
                kind="cancel",
                idempotency_key="cancel-converge",
                expected_version="1",
            )
        )
    )
    assert replay.command_id == receipt.command_id
    assert (
        count(
            world,
            "work_item",
            "run_id = :run AND kind = 'reconcile_execution' AND status IN ('pending', 'leased')",
            run=world.run_id,
        )
        == 1
    )


def test_commit_pause_requires_the_persisted_pause_and_converges(world):
    unit = uow(world)
    receipt = intake(world, "pause", "pause-safe-point")
    # (a) a guard that claims a pause the database does not hold is refused.
    wrong_kind = run(
        lambda: _row(world, "SELECT desired_control FROM run WHERE id = :id", id=world.run_id)
    )
    assert wrong_kind["desired_control"] == "pause"
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: unit.commit_pause(pause_request(world, command_id=uuid.uuid4())))
    assert classify(error.value) == "VERSION_CONFLICT"
    # (b) with the persisted command the safe point converges command, work and slot.
    result = run(
        lambda: uow(world).commit_pause(pause_request(world, command_id=receipt.command_id))
    )
    assert result.status == RunStatus.PAUSED
    # Replaying the applied command returns its recorded fact without a live lease: applying
    # pause cleared the run owner, so identity is checked before the lease guard.
    replay = run(
        lambda: uow(world).commit_pause(pause_request(world, command_id=receipt.command_id))
    )
    assert replay.status == RunStatus.PAUSED
    assert replay.state_version == result.state_version
    assert (
        scalar(world.schema, "SELECT status FROM command WHERE id = :id", id=receipt.command_id)
        == "applied"
    )
    assert live_works(world) == 0
    assert (
        scalar(
            world.schema,
            "SELECT owner_run_id FROM session_slot WHERE session_id = :id",
            id=world.session_id,
        )
        is None
    )
    assert (
        scalar(world.schema, "SELECT desired_control FROM run WHERE id = :id", id=world.run_id)
        is None
    )


def test_commit_cancel_requires_the_full_safe_point_bundle(world):
    receipt = intake(world, "cancel", "cancel-safe-point")
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    seal = record_seal(
        world,
        item_id=item_id,
        attempt_id=attempt_id,
        content_digest="a" * 64,
        analysis_digest="b" * 64,
        content_head=(1, 3),
        analysis_head=(1, 3),
        durable_seq=3,
    )
    # (a) no-producer evidence is contradicted by the seal.
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: uow(world).commit_cancel(cancel_request(world, command_id=receipt.command_id)))
    assert classify(error.value) == "INVALID_INPUT"
    # (b) the checkpoint must belong to this run and state version.
    other_checkpoint = world.checkpoint_id
    run(lambda: _bump_checkpoint_state_version(world, other_checkpoint))
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: uow(world).commit_cancel(
                cancel_request(
                    world,
                    command_id=receipt.command_id,
                    evidence_payload=verified_evidence(world, [seal], enumerated=3),
                )
            )
        )
    assert classify(error.value) == "VERSION_CONFLICT"
    run(lambda: _restore_checkpoint_state_version(world, other_checkpoint))
    # (c) the real bundle applies, keeping the convergence work claimable.
    applied = run(
        lambda: uow(world).commit_cancel(
            cancel_request(
                world,
                command_id=receipt.command_id,
                evidence_payload=verified_evidence(world, [seal], enumerated=3),
            )
        )
    )
    assert isinstance(applied, ControlApplied)
    assert applied.kind == "cancel"
    assert applied.status == RunStatus.CANCELLING
    assert live_works(world, run_id=world.run_id) >= 1
    assert (
        count(
            world,
            "work_item",
            "run_id = :run AND kind = 'advance_run' AND status IN ('pending', 'leased')",
            run=world.run_id,
        )
        == 0
    )
    assert (
        count(
            world,
            "work_item",
            "run_id = :run AND kind = 'reconcile_execution' AND status IN ('pending', 'leased')",
            run=world.run_id,
        )
        == 1
    )
    # The session slot stays with the cancelling run until its terminal commit, so the
    # convergence claim can still address the run and the frontend is not told it stopped.
    assert (
        scalar(
            world.schema,
            "SELECT owner_run_id FROM session_slot WHERE session_id = :id",
            id=world.session_id,
        )
        == world.run_id
    )
    # (d) replaying the applied command returns the recorded fact.
    replay = run(
        lambda: uow(world).commit_cancel(
            cancel_request(
                world,
                command_id=receipt.command_id,
                evidence_payload=verified_evidence(world, [seal], enumerated=3),
            )
        )
    )
    assert isinstance(replay, ControlApplied)
    assert replay.command_id == receipt.command_id


async def _set_run_status(world: World, status: str) -> None:
    """Move the stored run status without a guard (used to build a mismatch counterexample)."""
    engine = engine_for(world.schema)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text("UPDATE run SET status = :status WHERE id = :id"),
                {"status": status, "id": world.run_id},
            )
    finally:
        await engine.dispose()


async def _bump_checkpoint_state_version(world: World, checkpoint_id: uuid.UUID) -> None:
    engine = engine_for(world.schema)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text("UPDATE checkpoint SET state_version = state_version + 5 WHERE id = :id"),
                {"id": checkpoint_id},
            )
    finally:
        await engine.dispose()


async def _restore_checkpoint_state_version(world: World, checkpoint_id: uuid.UUID) -> None:
    engine = engine_for(world.schema)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text("UPDATE checkpoint SET state_version = state_version - 5 WHERE id = :id"),
                {"id": checkpoint_id},
            )
    finally:
        await engine.dispose()


# --------------------------------------------------------------------------- P1-4 exec


def test_sibling_run_execution_is_invisible_to_this_run(world):
    """Execution ownership is ``run_id``, not the shared task (review §11.2 P1-4)."""
    unit = uow(world)
    sibling_execution = run(
        lambda: _seed_execution(world, run_id=world.sender_run_id, status="running")
    )
    # The sibling's unresolved execution must not block this run's completion.
    result = run(
        lambda: unit.commit_terminal(
            terminal_request(world, result_status="completed", executions=())
        )
    )
    assert result.status == RunStatus.COMPLETED
    assert (
        scalar(world.schema, "SELECT status FROM execution WHERE id = :id", id=sibling_execution)
        == "running"
    )
    # Unsealed sibling deltas do not contradict this run's no-producer evidence either.
    item_id = run(lambda: _seed_item(world, run_id=world.sender_run_id))
    attempt_id = run(lambda: _seed_attempt(world, run_id=world.sender_run_id))
    run(
        lambda: _seed_event(
            world,
            sequence=1,
            item_id=item_id,
            attempt_id=attempt_id,
            run_id=world.sender_run_id,
        )
    )
    assert (
        count(world, "event", "run_id = :run", run=world.sender_run_id) == 1
        and count(world, "event", "run_id = :run", run=world.run_id) == 0
    )


def test_execution_supervision_identity_cannot_be_fabricated(world):
    unit = uow(world)
    # (a) a stored NULL supervision cannot be replaced by a request-supplied work id.
    bare = run(lambda: _seed_execution(world, status="running", supervisor_work_id=None))
    invented = run(lambda: _seed_work(world, kind="reconcile_execution", status="pending"))
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: unit.commit_terminal(
                terminal_request(
                    world,
                    result_status="failed",
                    executions=executions_payload((bare, "isolated", invented)),
                )
            )
        )
    assert classify(error.value) == "INVALID_INPUT"
    run(lambda: _delete_supervision(invented))
    run(lambda: _delete_execution(bare))

    # (b) a supervision work item owned by another run is refused.
    sibling_work = run(
        lambda: _seed_work(
            world, run_id=world.sender_run_id, kind="reconcile_execution", status="pending"
        )
    )
    foreign = run(lambda: _seed_execution(world, status="running", supervisor_work_id=sibling_work))
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: unit.commit_terminal(
                terminal_request(
                    world,
                    result_status="failed",
                    executions=executions_payload((foreign, "isolated", sibling_work)),
                )
            )
        )
    assert classify(error.value) == "INVALID_INPUT"
    run(lambda: _delete_execution(foreign))
    run(lambda: _delete_supervision(sibling_work))

    # (c) the wrong kind is refused. The run's own live ``advance_run`` work (the seeded
    # lease work) is the counterexample: the database allows only one live advance work per
    # run, so the transaction's own work item is the wrong-kind supervision candidate.
    wrong_kind = world.work_id
    with_kind = run(lambda: _seed_execution(world, status="running", supervisor_work_id=wrong_kind))
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: unit.commit_terminal(
                terminal_request(
                    world,
                    result_status="failed",
                    executions=executions_payload((with_kind, "isolated", wrong_kind)),
                )
            )
        )
    assert classify(error.value) == "INVALID_INPUT"
    run(lambda: _delete_execution(with_kind))

    dead = run(lambda: _seed_work(world, kind="recover_scope", status="completed"))
    with_dead = run(lambda: _seed_execution(world, status="unknown", supervisor_work_id=dead))
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: unit.commit_terminal(
                terminal_request(
                    world,
                    result_status="failed",
                    executions=executions_payload((with_dead, "unknown", dead)),
                )
            )
        )
    assert classify(error.value) == "INVALID_INPUT"
    run(lambda: _delete_execution(with_dead))
    run(lambda: _delete_supervision(dead))

    # (d) the exact stored live supervision work of this run is accepted.
    live = run(lambda: _seed_work(world, kind="reconcile_execution", status="pending"))
    attached = run(lambda: _seed_execution(world, status="running", supervisor_work_id=live))
    result = run(
        lambda: unit.commit_terminal(
            terminal_request(
                world,
                result_status="failed",
                executions=executions_payload((attached, "isolated", live)),
            )
        )
    )
    assert result.status == RunStatus.FAILED


async def _delete_supervision(work_id: uuid.UUID) -> None:
    """Remove seeded supervision work once its execution row is gone (FK order)."""
    async with _admin_engine() as engine:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text("DELETE FROM work_item WHERE id = :id"), {"id": work_id}
            )


async def _delete_execution(execution_id: uuid.UUID) -> None:
    async with _admin_engine() as engine:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text("DELETE FROM execution WHERE id = :id"), {"id": execution_id}
            )


class _admin_engine:
    """Small async context manager returning a lock-order-free admin engine."""

    def __init__(self) -> None:
        self._engine: AsyncEngine | None = None

    async def __aenter__(self) -> AsyncEngine:
        assert _SCHEMA[0] is not None
        self._engine = engine_for(_SCHEMA[0])
        return self._engine

    async def __aexit__(self, *exc: object) -> None:
        assert self._engine is not None
        await self._engine.dispose()


_SCHEMA: list[str | None] = [None]


# --------------------------------------------------------------------------- P1-5 live


def test_failed_terminal_keeps_supervision_work_alive(world):
    """P1-5: the database terminal state is not proof that a process stopped."""
    unit = uow(world)
    supervision = run(lambda: _seed_work(world, kind="reconcile_execution", status="pending"))
    execution = run(
        lambda: _seed_execution(world, status="running", supervisor_work_id=supervision)
    )
    result = run(
        lambda: unit.commit_terminal(
            terminal_request(
                world,
                result_status="failed",
                executions=executions_payload((execution, "isolated", supervision)),
            )
        )
    )
    assert result.status == RunStatus.FAILED
    assert (
        scalar(world.schema, "SELECT status FROM work_item WHERE id = :id", id=supervision)
        == "pending"
    )
    assert (
        count(
            world,
            "work_item",
            "run_id = :run AND kind = 'reconcile_execution' AND status IN ('pending', 'leased')",
            run=world.run_id,
        )
        == 1
    )
    assert (
        count(
            world,
            "work_item",
            "run_id = :run AND kind = 'advance_run' AND status IN ('pending', 'leased')",
            run=world.run_id,
        )
        == 0
    )


def test_cancelled_terminal_requires_stopped_executions_and_clears_advance_work(world):
    unit = uow(world)
    receipt = intake(world, "cancel", "cancel-terminal")
    supervision = run(lambda: _seed_work(world, kind="reconcile_execution", status="pending"))
    execution = run(
        lambda: _seed_execution(world, status="running", supervisor_work_id=supervision)
    )
    # (a) the database says running while the request claims stopped: version conflict.
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: unit.commit_terminal(
                terminal_request(
                    world,
                    result_status="cancelled",
                    status="cancelling",
                    desired_control="cancel",
                    control_command_id=receipt.command_id,
                    executions=executions_payload((execution, "stopped", None)),
                )
            )
        )
    assert classify(error.value) == "VERSION_CONFLICT"
    # (b) while the execution is unresolved, a truthful isolated resolution is refused by
    # the frozen contract itself: cancellation cannot be claimed before the stop fact.
    with pytest.raises(ValidationError):
        terminal_request(
            world,
            result_status="cancelled",
            status="cancelling",
            desired_control="cancel",
            control_command_id=receipt.command_id,
            executions=executions_payload((execution, "isolated", supervision)),
        )
    # (c) once the execution is stopped in the database, the terminal commit succeeds and
    # no advance work survives it.
    run(lambda: _set_execution_status(execution, "completed"))
    result = run(
        lambda: unit.commit_terminal(
            terminal_request(
                world,
                result_status="cancelled",
                status="cancelling",
                desired_control="cancel",
                control_command_id=receipt.command_id,
                executions=executions_payload((execution, "stopped", None)),
            )
        )
    )
    assert result.status == RunStatus.CANCELLED
    assert (
        count(
            world,
            "work_item",
            "run_id = :run AND kind = 'advance_run' AND status IN ('pending', 'leased')",
            run=world.run_id,
        )
        == 0
    )


async def _set_execution_status(execution_id: uuid.UUID, status: str) -> None:
    async with _admin_engine() as engine:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text("UPDATE execution SET status = :status WHERE id = :id"),
                {"status": status, "id": execution_id},
            )


# --------------------------------------------------------------------------- P1-6 resume


async def _claim_run_for_worker(world: World, work_id: uuid.UUID) -> None:
    """Simulate the DS-P02-04 claim (T2) so a queued run can be controlled again.

    DS-P02-02 only consumes the stored lease facts; the scheduler claim sets the run
    owner/epoch/lease, leases the pending advance work and takes the session slot. The claim
    walks the frozen lock order (slot -> run -> work) exactly like the real scheduler must.
    """
    engine = engine_for(world.schema)
    now = datetime.now(UTC)
    lease_until = now + timedelta(seconds=DEFAULT_LEASE_SECONDS)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    "UPDATE session_slot SET owner_run_id = :run, "
                    "slot_version = slot_version + 1 "
                    "WHERE scope_id = :scope AND session_id = :session"
                ),
                {"run": world.run_id, "scope": world.scope, "session": world.session_id},
            )
            await connection.execute(
                sa.text(
                    "UPDATE run SET status = 'running', owner_id = :owner, "
                    "lease_until = :lease, owner_epoch = owner_epoch + 1, updated_at = :now "
                    "WHERE id = :id"
                ),
                {"owner": world.owner, "lease": lease_until, "now": now, "id": world.run_id},
            )
            await connection.execute(
                sa.text(
                    "UPDATE work_item SET status = 'leased', owner_id = :owner, "
                    "lease_until = :lease, lease_epoch = lease_epoch + 1, updated_at = :now "
                    "WHERE id = :id"
                ),
                {"owner": world.owner, "lease": lease_until, "now": now, "id": work_id},
            )
            run_row = (
                (
                    await connection.execute(
                        sa.text("SELECT state_version, owner_epoch FROM run WHERE id = :id"),
                        {"id": world.run_id},
                    )
                )
                .mappings()
                .one()
            )
            # The worker cuts the checkpoint for the state version this claim will commit.
            checkpoint_id = uuid.uuid4()
            next_version = int(run_row["state_version"]) + 1
            await connection.execute(
                sa.text(
                    "INSERT INTO checkpoint (id, scope_id, run_id, state_version, "
                    "representation_revision, event_seq, schema_revision, runtime_build, "
                    "state_ref, content_hash) VALUES (:id, :scope, :run, :state_version, 1, 0, "
                    "1, 'test', :state_ref, :hash)"
                ),
                {
                    "id": checkpoint_id,
                    "scope": world.scope,
                    "run": world.run_id,
                    "state_version": next_version,
                    "state_ref": f"state:{next_version}",
                    "hash": "d" * 64,
                },
            )
            slot_version = (
                await connection.execute(
                    sa.text("SELECT slot_version FROM session_slot WHERE session_id = :id"),
                    {"id": world.session_id},
                )
            ).scalar_one()
            work_epoch = (
                await connection.execute(
                    sa.text("SELECT lease_epoch FROM work_item WHERE id = :id"), {"id": work_id}
                )
            ).scalar_one()
    finally:
        await engine.dispose()
    world.status = "running"
    world.state_version = int(run_row["state_version"])
    world.owner_epoch = int(run_row["owner_epoch"])
    world.slot_version = int(slot_version)
    world.work_epoch = int(work_epoch)
    world.work_id = work_id
    world.checkpoint_id = checkpoint_id


def test_resume_commands_never_share_a_work_item(world):
    unit = uow(world)
    receipt = intake(world, "pause", "resume-pause")
    run(lambda: unit.commit_pause(pause_request(world, command_id=receipt.command_id)))

    def resume(command_id: uuid.UUID, key: str, version: str) -> ResumedRun:
        return run(
            lambda: uow(world).resume(
                ResumeCommitRequest.model_validate(
                    {
                        "actor": actor(world.auth_revision, world=world),
                        "session_id": world.session_id,
                        "run_id": world.run_id,
                        "command_id": str(command_id),
                        "expected_state_version": version,
                        "expected_cancel_version": "0",
                        "checkpoint_id": world.checkpoint_id,
                        "idempotency_key": key,
                    }
                )
            )
        )

    first = resume(uuid.uuid4(), "resume-a", "2")
    assert resume(first.command_id, "resume-a", "3").work_id == first.work_id
    # Pause again: pause cancels live advance work, so the next command must create its own.
    # A queued run is first claimed by a worker (T2 belongs to DS-P02-04), which takes the
    # session slot and leases the pending advance work.
    run(lambda: _claim_run_for_worker(world, first.work_id))
    second_receipt = intake(world, "pause", "resume-pause-2")
    run(lambda: uow(world).commit_pause(pause_request(world, command_id=second_receipt.command_id)))
    second = resume(uuid.uuid4(), "resume-b", "4")
    assert second.work_id != first.work_id
    assert (
        scalar(world.schema, "SELECT dedup_key FROM work_item WHERE id = :id", id=second.work_id)
        == f"resume:{second.command_id}"
    )


def test_resume_refuses_a_live_work_from_another_command(world):
    unit = uow(world)
    receipt = intake(world, "pause", "resume-pause-3")
    run(lambda: unit.commit_pause(pause_request(world, command_id=receipt.command_id)))
    run(lambda: _insert_foreign_resume_work(world))
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: uow(world).resume(
                ResumeCommitRequest.model_validate(
                    {
                        "actor": actor(world.auth_revision, world=world),
                        "session_id": world.session_id,
                        "run_id": world.run_id,
                        "command_id": str(uuid.uuid4()),
                        "expected_state_version": "2",
                        "expected_cancel_version": "0",
                        "checkpoint_id": world.checkpoint_id,
                        "idempotency_key": "resume-c",
                    }
                )
            )
        )
    assert classify(error.value) == "VERSION_CONFLICT"


async def _insert_foreign_resume_work(world: World) -> None:
    engine = engine_for(world.schema)
    now = datetime.now(UTC)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    "INSERT INTO work_item (id, scope_id, run_id, subject_ref, kind, dedup_key, "
                    "status, payload_ref, wake_reason, priority, lease_epoch, attempt_count, "
                    "available_at, deadline_at) VALUES (:id, :scope, :run, '{}', 'advance_run', "
                    "'resume:other', 'pending', 'resume', 'resume', 100, 0, 0, :now, :deadline)"
                ),
                {
                    "id": uuid.uuid4(),
                    "scope": world.scope,
                    "run": world.run_id,
                    "now": now,
                    "deadline": now + timedelta(days=1),
                },
            )
    finally:
        await engine.dispose()


# --------------------------------------------------------------------------- misc


def test_desired_control_never_diverges_from_the_command_identity(world):
    """The database CHECK and the UoW agree: control fields move together."""
    receipt = intake(world, "pause", "pairing")
    row = run(
        lambda: _row(
            world,
            "SELECT desired_control, control_command_id FROM run WHERE id = :id",
            id=world.run_id,
        )
    )
    assert (row["desired_control"] is None) == (row["control_command_id"] is None)
    assert row["control_command_id"] == receipt.command_id
    assert row["desired_control"] == str(DesiredControl.PAUSE)


# --------------------------------------------------------------------------- identity


def test_flush_proof_identity_must_match_the_sealed_producer(world):
    """A request may not relabel scope/session/run/proof identity (review §11.2 P1-1)."""
    unit = uow(world)
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    seal = record_seal(
        world,
        item_id=item_id,
        attempt_id=attempt_id,
        content_digest="a" * 64,
        analysis_digest="b" * 64,
        content_head=(1, 2),
        analysis_head=(1, 2),
        durable_seq=2,
    )
    cases = {
        "scope": verified_evidence(world, [seal], scope_id="scope-somewhere-else"),
        "session": verified_evidence(world, [seal], session_id=uuid.uuid4()),
        "run": verified_evidence(world, [seal], run_id=uuid.uuid4()),
    }
    # The frozen request contract already binds every proof to the guard's lease.
    for label, payload in cases.items():
        with pytest.raises(ValidationError):
            advance_request(world, evidence=payload)
        assert label  # the case names the field that must be refused
    # Keeping the producer identity but swapping the persisted proof id is caught against
    # the sealed row inside the transaction.
    forged = verified_evidence(world, [{**seal, "proof_id": uuid.uuid4()}])
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: unit.commit_advance(advance_request(world, evidence=forged)))
    assert classify(error.value) == "INVALID_INPUT"
    assert (
        scalar(world.schema, "SELECT state_version FROM run WHERE id = :id", id=world.run_id) == 1
    )
    truthful = verified_evidence(world, [seal], enumerated=2)
    committed = run(lambda: unit.commit_advance(advance_request(world, evidence=truthful)))
    assert committed.state_version == "2"


def test_content_only_seal_persists_an_empty_analysis_channel_and_replays(world):
    """Non-thinking answers have no analysis deltas but still need a durable seal."""
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    proof_id = uuid.uuid4()
    empty_digest = hashlib.sha256(b"").hexdigest()
    seal = record_seal(
        world,
        item_id=item_id,
        attempt_id=attempt_id,
        proof_id=proof_id,
        content_digest="a" * 64,
        analysis_digest=empty_digest,
        content_head=(3, 4),
        analysis_head=None,
        durable_seq=4,
    )
    # A connection-ambiguous retry is a no-op, not a unique-constraint leak.
    record_seal(
        world,
        item_id=item_id,
        attempt_id=attempt_id,
        proof_id=proof_id,
        content_digest="a" * 64,
        analysis_digest=empty_digest,
        content_head=(3, 4),
        analysis_head=None,
        durable_seq=4,
    )
    row = run(
        lambda: _row(
            world,
            "SELECT analysis_last_delta_index, analysis_last_session_seq FROM output_seal "
            "WHERE proof_id = :proof",
            proof=proof_id,
        )
    )
    assert row == {"analysis_last_delta_index": None, "analysis_last_session_seq": None}
    assert count(world, "output_seal", "proof_id = :proof", proof=proof_id) == 1
    committed = run(
        lambda: uow(world).commit_advance(
            advance_request(world, evidence=verified_evidence(world, [seal], enumerated=4))
        )
    )
    assert committed.state_version == "2"


def test_seal_writer_rejects_cross_run_producers_and_changed_replay(world):
    item_id = run(lambda: _seed_item(world, run_id=world.sender_run_id))
    attempt_id = run(lambda: _seed_attempt(world, run_id=world.sender_run_id))
    with pytest.raises(AgentRuntimeError) as error:
        record_seal(
            world,
            item_id=item_id,
            attempt_id=attempt_id,
            content_digest="a" * 64,
            analysis_digest="b" * 64,
            content_head=(1, 2),
            analysis_head=(1, 2),
            durable_seq=2,
        )
    assert classify(error.value) == "INVALID_INPUT"
    assert count(world, "output_seal", "run_id = :run", run=world.run_id) == 0

    own_item = run(lambda: _seed_item(world))
    own_attempt = run(lambda: _seed_attempt(world))
    proof_id = uuid.uuid4()
    record_seal(
        world,
        item_id=own_item,
        attempt_id=own_attempt,
        proof_id=proof_id,
        content_digest="c" * 64,
        analysis_digest="d" * 64,
        content_head=(1, 2),
        analysis_head=(1, 2),
        durable_seq=2,
    )
    with pytest.raises(AgentRuntimeError) as error:
        record_seal(
            world,
            item_id=own_item,
            attempt_id=own_attempt,
            proof_id=proof_id,
            content_digest="e" * 64,
            analysis_digest="d" * 64,
            content_head=(1, 2),
            analysis_head=(1, 2),
            durable_seq=2,
        )
    assert classify(error.value) == "INVALID_INPUT"


def test_control_intake_rejects_stale_version_and_changed_replay(world):
    unit = uow(world)
    before = {
        "commands": count(world, "command", "run_id = :run", run=world.run_id),
        "works": live_works(world),
    }
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: unit.record_control(
                ControlRequest(
                    run_id=world.run_id,
                    kind="cancel",
                    idempotency_key="stale-control",
                    expected_version="0",
                )
            )
        )
    assert classify(error.value) == "VERSION_CONFLICT"
    assert count(world, "command", "run_id = :run", run=world.run_id) == before["commands"]
    assert live_works(world) == before["works"]
    assert (
        scalar(
            world.schema,
            "SELECT desired_control FROM run WHERE id = :id",
            id=world.run_id,
        )
        is None
    )

    accepted = intake(world, "pause", "control-payload", expected_version="1")
    with pytest.raises(AgentRuntimeError) as error:
        run(
            lambda: unit.record_control(
                ControlRequest(
                    run_id=world.run_id,
                    kind="pause",
                    idempotency_key="control-payload",
                    expected_version="2",
                )
            )
        )
    assert classify(error.value) == "INVALID_INPUT"
    assert count(world, "command", "run_id = :run", run=world.run_id) == 1
    assert accepted.command_id == scalar(
        world.schema, "SELECT control_command_id FROM run WHERE id = :id", id=world.run_id
    )


def _output_batch(
    world: World,
    *,
    attempt_id: uuid.UUID,
    item_id: uuid.UUID,
    content: str = "hello",
) -> ProducerBatch:
    now = datetime.now(UTC)
    return ProducerBatch.model_validate(
        {
            "lease": lease(world),
            "turnId": str(world.turn_id),
            "attemptId": str(attempt_id),
            "itemId": str(item_id),
            "traceId": "trace-journal",
            "events": [
                {
                    "kind": "assistant.delta",
                    "occurredAt": now,
                    "payload": {
                        "channel": "content",
                        "deltaIndex": "0",
                        "itemRevision": "1",
                        "text": content,
                    },
                },
                {
                    "kind": "analysis.delta",
                    "occurredAt": now,
                    "payload": {
                        "channel": "analysis",
                        "deltaIndex": "0",
                        "itemRevision": "1",
                        "text": "private reasoning",
                    },
                },
            ],
        }
    )


def test_journal_outbox_replay_visibility_and_idempotency(world):
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    engine = engine_for(world.schema, search_path=False)
    journal = SqlJournal(engine, world.schema, actor(world=world))
    reader = SqlJournalReader(engine, world.schema)
    outbox = SqlOutboxStore(engine, world.schema, world.scope)
    batch = _output_batch(world, attempt_id=attempt_id, item_id=item_id)

    first = run(lambda: journal.append(batch))
    replay = run(lambda: journal.append(batch))
    assert [entry.disposition for entry in first.entries] == ["created", "created"]
    assert [entry.disposition for entry in replay.entries] == ["reused", "reused"]
    assert first.entries[0].event_id == replay.entries[0].event_id
    assert count(world, "event", "session_id = :session", session=world.session_id) == 2
    assert count(world, "outbox") == 2
    assert (
        scalar(world.schema, "SELECT event_seq FROM session WHERE id = :id", id=world.session_id)
        == 2
    )

    request = StreamRequest(session_id=world.session_id, after_seq="0", last_event_id=None)
    public = run(lambda: reader.read_public(actor(world=world), request))
    authorized = run(lambda: reader.read_authorized(actor(world=world), request))
    assert isinstance(public, JournalPage)
    assert isinstance(authorized, JournalPage)
    assert public.cursor == "2" and [event.kind for event in public.events] == ["assistant.delta"]
    assert [event.kind for event in authorized.events] == ["assistant.delta", "analysis.delta"]

    claim = run(lambda: outbox.claim(OutboxClaimRequest(limit=8, lease_seconds=30)))
    assert claim is not None and len(claim.notices) == 2
    wrong = OutboxDelivery(
        claim_token=uuid.uuid4(), event_ids=tuple(notice.event_id for notice in claim.notices)
    )
    assert run(lambda: outbox.delivered(wrong)) == 0
    delivery = OutboxDelivery(
        claim_token=claim.claim_token,
        event_ids=tuple(notice.event_id for notice in claim.notices),
    )
    assert run(lambda: outbox.delivered(delivery)) == 2
    assert run(lambda: outbox.claim(OutboxClaimRequest(limit=8, lease_seconds=30))) is None
    run(engine.dispose)


@pytest.mark.parametrize(
    "scenario",
    [
        "respond",
        "finish",
        "partial",
        "rollback_respond",
        "rollback_finish",
        "unsealed",
        "empty",
        "pending",
        "unknown_execution",
        "active_child",
        "unverified_child",
        "revoked",
        "unpublished",
        "other_work",
    ],
)
def test_answer_action_atomic_commit_and_recovery(world, monkeypatch, scenario):
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    snapshot_id = run(
        lambda: _row(
            world,
            "SELECT context_snapshot_id FROM model_attempt WHERE id=:id",
            id=attempt_id,
        )
    )["context_snapshot_id"]
    base_state, base_context, _ = decision_facts()
    state = base_state.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "state_version": str(world.state_version),
            "checkpoint_ref": f"state:{world.state_version + 1}",
        }
    )
    context = base_context.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "snapshot_id": snapshot_id,
        }
    )
    responding = scenario in {"respond", "rollback_respond", "other_work"}

    async def register_primary():
        engine = engine_for(world.schema)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text("UPDATE turn SET primary_run_id=:run WHERE id=:turn"),
                    {"run": world.run_id, "turn": world.turn_id},
                )
                if responding:
                    await connection.execute(
                        sa.text(
                            "INSERT INTO budget_allocation (id, scope_id, root_run_id, limits, "
                            "reserved, unknown_reserved, settled, usage_status, "
                            "unsettled_count, version) "
                            "VALUES (:id,:scope,:run,'{}','{}','{}','{}','complete',0,1)"
                        ),
                        {"id": uuid.uuid4(), "scope": world.scope, "run": world.run_id},
                    )
                    await connection.execute(
                        sa.text(
                            "UPDATE timeline_item SET status='streaming', final_revision=0 "
                            "WHERE id=:item"
                        ),
                        {"item": item_id},
                    )
        finally:
            await engine.dispose()

    run(register_primary)
    action = (
        {"kind": "respond", "payload": {"answerRequestRef": "answer:request"}}
        if responding
        else {
            "kind": "finish",
            "payload": {
                "outcome": "partial" if scenario == "partial" else "success",
                "evidenceRefs": [],
                "finalItemIds": [str(item_id)],
            },
        }
    )
    frame = DecisionFrame(
        actor=actor(world=world),
        context=context,
        attempt_id=attempt_id,
        action=decision_adapter.validate_python(action),
    )

    class Context:
        async def load(self, scope_id, requested_id):
            return context, {}

    class Access:
        async def verify(self, current_actor, prepared):
            assert current_actor == frame.actor and prepared == context

        async def authorize_session(self, current_actor, session_id):
            assert current_actor == frame.actor and session_id == world.session_id
            if scenario == "revoked":
                from agent_runtime.storage.uow import errors

                errors.forbidden("session access revoked")

        async def authorize_session_in_transaction(self, session, current_actor, session_id):
            await self.authorize_session(current_actor, session_id)

        async def authorize_resource(self, current_actor, resource):
            raise AssertionError("no arbitrary resource access expected")

        async def authorize_resource_in_transaction(
            self, session, current_actor, resource, *, session_id
        ):
            await self.authorize_resource(current_actor, resource)

    class Publisher:
        calls = 0

        async def publish(self, current, requested):
            assert current == staged and requested == frame
            self.calls += 1
            return AnswerPublication((item_id,))

    engine = engine_for(world.schema, pool_size=1, search_path=False)
    journal = SqlJournal(engine, world.schema, frame.actor)
    payloads = BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema))
    store = SqlDecisionStore(
        engine,
        world.schema,
        actor=frame.actor,
        lease=lease(world),
        payloads=payloads,
        context=Context(),
        access=Access(),
        journal=journal,
    )
    run(
        lambda: journal.append(
            _output_batch(
                world,
                attempt_id=attempt_id,
                item_id=item_id,
                content="" if scenario == "empty" else "A verified answer.",
            )
        )
    )
    if scenario != "unsealed":
        proof = run(
            lambda: journal.seal_output(
                SealOutputRequest(
                    lease=lease(world),
                    attempt_id=attempt_id,
                    item_id=item_id,
                    proof_id=uuid.uuid4(),
                    sealed_revision="1",
                )
            )
        )
        if not responding and scenario != "unpublished":

            async def publish_completion():
                sessions = async_sessionmaker(engine, expire_on_commit=False)
                async with sessions() as session, session.begin():
                    await journal.append_state_in_transaction(
                        session,
                        StateEventBatch(
                            authority=MainEventAuthority(kind="main", guard=state_guard(world)),
                            session_id=world.session_id,
                            trace_id="test-answer-publication",
                            events=(
                                ItemCompletedDraft(
                                    kind="item.completed",
                                    producer_key=f"test-answer:{item_id}",
                                    occurred_at=datetime.now(UTC),
                                    turn_id=world.turn_id,
                                    run_id=world.run_id,
                                    parent_run_id=None,
                                    step_id=None,
                                    item_id=item_id,
                                    model_attempt_id=attempt_id,
                                    payload=ItemCompleted.model_validate(
                                        {
                                            "itemRevision": "1",
                                            "status": "completed",
                                            "channelDigests": [
                                                {"channel": c.channel, "digest": c.digest}
                                                for c in proof.channels
                                            ],
                                            "lastDeltaIndexes": [
                                                {
                                                    "channel": c.channel,
                                                    "lastDeltaIndex": c.last_delta_index,
                                                }
                                                for c in proof.channels
                                            ],
                                        }
                                    ),
                                ),
                            ),
                        ),
                    )

            run(publish_completion)
    staged = run(lambda: store.persist(state, frame))
    publisher = Publisher()
    committer = SqlAnswerActionCommitter(
        actor=frame.actor,
        lease=lease(world),
        decisions=store,
        state_store=SqlStateUnitOfWork(engine, world.schema, frame.actor),
        journal=journal,
        publisher=publisher,
        resources=(
            Access()
            if scenario == "revoked"
            else SqlRuntimeSourceReader(
                engine, world.schema, blobs=SqlContextBlobBackend(engine, world.schema)
            )
        ),
        live=SqlLiveRunGuard(
            engine,
            world.schema,
            actor=frame.actor,
            lease=lease(world),
            context=Context(),
            access=Access(),
        ),
    )
    if scenario == "pending":
        run(lambda: _seed_messages(world, (1,), allocated=1, claim_token=uuid.uuid4()))
    if scenario == "unknown_execution":
        run(lambda: _seed_execution(world, status="unknown"))
    if scenario in {"active_child", "unverified_child"}:

        async def child_fact():
            async with engine.begin() as conn:
                if scenario == "active_child":
                    await conn.execute(
                        sa.text(
                            f'UPDATE "{world.schema}".run SET parent_run_id=:parent WHERE id=:id'
                        ),
                        {"parent": world.run_id, "id": world.sender_run_id},
                    )
                else:
                    await conn.execute(
                        sa.text(
                            f'INSERT INTO "{world.schema}".agent_message '
                            "(id,scope_id,sender_run_id,recipient_run_id,task_id,correlation_id,"
                            "recipient_seq,protocol_revision,payload,kind,payload_hash,status,"
                            "trace_id,deadline_at) "
                            "VALUES (:id,:scope,:sender,:recipient,:task,:task,1,1,'{}',"
                            "'result.proposed',:hash,'pending','test',:deadline)"
                        ),
                        {
                            "id": uuid.uuid4(),
                            "scope": world.scope,
                            "sender": world.sender_run_id,
                            "recipient": world.run_id,
                            "task": world.task_id,
                            "hash": "a" * 64,
                            "deadline": datetime.now(UTC) + timedelta(hours=1),
                        },
                    )

        run(child_fact)
    if scenario == "other_work":
        with pytest.raises(sa.exc.IntegrityError):
            run(lambda: _seed_work(world, kind="advance_run"))
    initial_events, initial_outbox = count(world, "event"), count(world, "outbox")
    if scenario.startswith("rollback"):
        original = journal.append_state_in_transaction

        async def fail_after_events(*args):
            await original(*args)
            raise RuntimeError("forced answer rollback")

        monkeypatch.setattr(journal, "append_state_in_transaction", fail_after_events)
        with pytest.raises(RuntimeError, match="forced answer rollback"):
            run(lambda: committer.commit(staged, frame))
        assert count(world, "event") == initial_events
        assert count(world, "outbox") == initial_outbox
        restored = run(store.recover)
        assert restored is not None and restored.frame.action == frame.action
        unchanged = run(
            lambda: _row(
                world, "SELECT status,state_version FROM run WHERE id=:id", id=world.run_id
            )
        )
        assert unchanged == {"status": "running", "state_version": world.state_version}
        monkeypatch.setattr(journal, "append_state_in_transaction", original)
    if scenario in {
        "unsealed",
        "empty",
        "pending",
        "unknown_execution",
        "active_child",
        "unverified_child",
        "revoked",
        "unpublished",
    }:
        with pytest.raises(AgentRuntimeError):
            run(lambda: committer.commit(staged, frame))
        assert count(world, "event") == initial_events
        assert count(world, "outbox") == initial_outbox
        assert count(world, "work_item", "status='leased'") == 1
        run(engine.dispose)
        return
    committed = run(lambda: committer.commit(staged, frame))
    assert committed.state_version == str(world.state_version + 1)
    assert committed.status == (RunStatus.QUEUED if responding else RunStatus.COMPLETED)
    assert count(world, "event", "kind='run.status'") == 1
    assert count(world, "event", "kind='run.completed'") == (0 if responding else 1)
    assert count(world, "event", "kind='item.completed'") == 1
    assert count(world, "outbox") == initial_outbox + 2
    assert count(world, "work_item", "status='pending'") == (1 if responding else 0)
    assert count(world, "work_item", "status='leased'") == 0
    final_item = run(
        lambda: _row(
            world, "SELECT status,final_revision FROM timeline_item WHERE id=:id", id=item_id
        )
    )
    assert final_item == {"status": "completed", "final_revision": 1}
    assert run(lambda: _row(world, "SELECT owner_run_id FROM session_slot"))["owner_run_id"] is None
    if not responding:
        turn = run(
            lambda: _row(
                world, "SELECT outcome_run_id,outcome FROM turn WHERE id=:id", id=world.turn_id
            )
        )
        assert turn["outcome_run_id"] == world.run_id
        assert turn["outcome"] == ("partial" if scenario == "partial" else "success")
        terminal = run(lambda: _row(world, "SELECT payload FROM event WHERE kind='run.completed'"))
        assert terminal["payload"]["publicMessage"]
        if scenario == "partial":
            assert terminal["payload"]["nextSteps"]
    with pytest.raises(AgentRuntimeError):
        run(lambda: committer.commit(staged, frame))
    if scenario == "respond":
        from agent_runtime.contracts.scheduler import ClaimedWork
        from agent_runtime.contracts.work_claims import WorkClaimRequest

        request = WorkClaimRequest.model_validate(
            {
                "scopeId": world.scope,
                "worker": {
                    "workerId": "answer-continuation-worker",
                    "runtimeRevision": {
                        "key": {"namespace": "builtin", "key": "runtime"},
                        "revision": "1",
                    },
                    "capabilities": [],
                },
                "leaseSeconds": 30,
                "lanes": ["main"],
                "maintenanceKinds": [],
            }
        )
        claimed = run(lambda: SqlScheduler(engine, world.schema).claim_work(request))
        assert isinstance(claimed, ClaimedWork)
        assert claimed.state_version == committed.state_version
        assert claimed.lease.run_id == world.run_id
        assert claimed.lease.work_id != lease(world).work_id
    run(engine.dispose)


@pytest.mark.parametrize(
    "scenario",
    [
        "create",
        "update",
        "stale",
        "missing",
        "replace_identity",
        "bad_run",
        "bad_evidence",
        "pending",
        "control",
        "revoked",
        "rollback_create",
        "rollback_update",
        "race",
        "foreign_task",
    ],
)
def test_plan_action_cas_atomicity_and_continuation(world, monkeypatch, scenario):
    from agent_runtime.storage.plan_commit import SqlPlanActionCommitter

    attempt_id = run(lambda: _seed_attempt(world))
    snapshot_id = run(
        lambda: _row(
            world, "SELECT context_snapshot_id FROM model_attempt WHERE id=:id", id=attempt_id
        )
    )["context_snapshot_id"]
    base_state, base_context, _ = decision_facts()
    state = base_state.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "state_version": str(world.state_version),
            "checkpoint_ref": f"state:{world.state_version + 1}",
        }
    )
    context = base_context.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "snapshot_id": snapshot_id,
        }
    )
    plan_id, original_revision_id, step_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    existing = scenario in {
        "update",
        "stale",
        "replace_identity",
        "rollback_update",
        "foreign_task",
    }
    expected = "1" if scenario in {"update", "rollback_update", "missing", "foreign_task"} else "0"
    original_steps = [
        {
            "id": str(step_id),
            "parentStepId": None,
            "label": "Initial step",
            "status": "pending",
            "evidenceRefs": [],
            "runRefs": [],
        }
    ]

    async def seed_plan():
        engine = engine_for(world.schema)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    sa.text(
                        "INSERT INTO budget_allocation (id,scope_id,root_run_id,limits,reserved,"
                        "unknown_reserved,settled,usage_status,unsettled_count,version) "
                        "VALUES (:id,:scope,:run,'{}','{}','{}','{}','complete',0,1)"
                    ),
                    {"id": uuid.uuid4(), "scope": world.scope, "run": world.run_id},
                )
                if existing:
                    task_id = world.task_id
                    if scenario == "foreign_task":
                        task_id = uuid.uuid4()
                        await conn.execute(
                            sa.text(
                                "INSERT INTO task "
                                "(id,scope_id,root_run_id,acceptance_spec_ref,deadline_at) "
                                "VALUES (:id,:scope,:run,'acceptance:foreign',:deadline)"
                            ),
                            {
                                "id": task_id,
                                "scope": world.scope,
                                "run": world.sender_run_id,
                                "deadline": datetime.now(UTC) + timedelta(days=1),
                            },
                        )
                    await conn.execute(
                        sa.text(
                            "INSERT INTO plan (id,scope_id,task_id,current_revision) "
                            "VALUES (:id,:scope,:task,1)"
                        ),
                        {"id": plan_id, "scope": world.scope, "task": task_id},
                    )
                    await conn.execute(
                        sa.text(
                            "INSERT INTO plan_revision "
                            "(id,scope_id,plan_id,revision,steps,public_reason) "
                            "VALUES (:id,:scope,:plan,1,CAST(:steps AS jsonb),'Initial plan')"
                        ),
                        {
                            "id": original_revision_id,
                            "scope": world.scope,
                            "plan": plan_id,
                            "steps": json.dumps(original_steps),
                        },
                    )
        finally:
            await engine.dispose()

    run(seed_plan)
    steps = [
        {
            **original_steps[0],
            "label": "Updated step",
            "status": "running",
            "runRefs": [str(world.run_id), str(world.sender_run_id)],
        }
    ]
    if scenario == "bad_run":
        steps[0]["runRefs"] = [str(uuid.uuid4())]
    if scenario == "update":
        steps[0]["evidenceRefs"] = [
            {
                "resourceType": "plan_revision",
                "resourceId": str(original_revision_id),
                "resourceVersion": "1",
                "scope": {"scopeId": world.scope, "sessionId": None, "ownerId": None},
            }
        ]
    if scenario == "bad_evidence":
        steps[0]["evidenceRefs"] = [
            {
                "resourceType": "plan_revision",
                "resourceId": str(uuid.uuid4()),
                "resourceVersion": "1",
                "scope": {"scopeId": world.scope, "sessionId": None, "ownerId": None},
            }
        ]
    frame = DecisionFrame(
        actor=actor(world=world),
        context=context,
        attempt_id=attempt_id,
        action=decision_adapter.validate_python(
            {
                "kind": "update_plan",
                "payload": {
                    "planId": str(uuid.uuid4() if scenario == "replace_identity" else plan_id),
                    "expectedRevision": expected,
                    "steps": steps,
                },
            }
        ),
    )

    class Context:
        async def load(self, scope_id, requested_id):
            return context, {}

    class Access:
        async def verify(self, current_actor, prepared):
            assert current_actor == frame.actor and prepared == context

    class Resources(SqlRuntimeSourceReader):
        async def authorize_session_in_transaction(self, session, current_actor, session_id):
            await super().authorize_session_in_transaction(session, current_actor, session_id)
            if scenario == "revoked":
                from agent_runtime.storage.uow import errors

                errors.forbidden("session access revoked")

    engine = engine_for(world.schema, pool_size=2 if scenario == "race" else 1, search_path=False)
    journal = SqlJournal(engine, world.schema, frame.actor)
    store = SqlDecisionStore(
        engine,
        world.schema,
        actor=frame.actor,
        lease=lease(world),
        payloads=BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema)),
        context=Context(),
        access=Access(),
        journal=journal,
    )
    staged = run(lambda: store.persist(state, frame))
    committer = SqlPlanActionCommitter(
        actor=frame.actor,
        lease=lease(world),
        decisions=store,
        state_store=SqlStateUnitOfWork(engine, world.schema, frame.actor),
        journal=journal,
        resources=Resources(
            engine, world.schema, blobs=SqlContextBlobBackend(engine, world.schema)
        ),
        live=SqlLiveRunGuard(
            engine,
            world.schema,
            actor=frame.actor,
            lease=lease(world),
            context=Context(),
            access=Access(),
        ),
    )
    if scenario == "pending":
        run(lambda: _seed_messages(world, (1,), allocated=1, claim_token=uuid.uuid4()))
    if scenario == "control":
        run(
            lambda: SqlStateUnitOfWork(engine, world.schema, frame.actor).record_control(
                ControlRequest(
                    run_id=world.run_id,
                    kind="pause",
                    idempotency_key="plan-pause",
                    expected_version=str(world.state_version),
                )
            )
        )
    initial_events, initial_outbox = count(world, "event"), count(world, "outbox")

    def unchanged():
        assert count(world, "plan") == int(existing)
        assert count(world, "plan_revision") == int(existing)
        assert count(world, "event") == initial_events
        assert count(world, "outbox") == initial_outbox
        assert count(world, "work_item", "status='leased'") == 1
        stored = run(
            lambda: _row(
                world, "SELECT status,state_version FROM run WHERE id=:id", id=world.run_id
            )
        )
        assert stored == {"status": "running", "state_version": world.state_version}
        if existing:
            assert (
                run(
                    lambda: _row(
                        world, "SELECT current_revision FROM plan WHERE id=:id", id=plan_id
                    )
                )["current_revision"]
                == 1
            )

    if scenario.startswith("rollback"):
        original = journal.append_state_in_transaction

        async def fail_after_events(*args):
            await original(*args)
            raise RuntimeError("forced plan rollback")

        monkeypatch.setattr(journal, "append_state_in_transaction", fail_after_events)
        with pytest.raises(RuntimeError, match="forced plan rollback"):
            run(lambda: committer.commit(staged, frame))
        unchanged()
        assert run(store.recover).frame == frame
        monkeypatch.setattr(journal, "append_state_in_transaction", original)
    if scenario in {
        "stale",
        "missing",
        "replace_identity",
        "bad_run",
        "bad_evidence",
        "pending",
        "control",
        "revoked",
        "foreign_task",
    }:
        with pytest.raises(AgentRuntimeError) as error:
            run(lambda: committer.commit(staged, frame))
        if scenario in {"stale", "missing", "replace_identity"}:
            assert classify(error.value) == "VERSION_CONFLICT"
        unchanged()
        run(engine.dispose)
        return
    if scenario == "race":

        async def compete():
            return await asyncio.gather(
                committer.commit(staged, frame),
                committer.commit(staged, frame),
                return_exceptions=True,
            )

        outcomes = run(compete)
        successes = [value for value in outcomes if not isinstance(value, BaseException)]
        failures = [value for value in outcomes if isinstance(value, BaseException)]
        assert len(successes) == len(failures) == 1
        assert isinstance(failures[0], AgentRuntimeError)
        result = successes[0]
    else:
        result = run(lambda: committer.commit(staged, frame))
    assert result.status == RunStatus.QUEUED
    assert result.state_version == str(world.state_version + 1)
    assert count(world, "plan") == 1
    assert count(world, "plan_revision") == int(existing) + 1
    assert count(world, "outbox") == initial_outbox + 2
    assert count(world, "event", "kind='plan.updated'") == 1
    assert count(world, "event", "kind='run.status'") == 1
    saved = run(
        lambda: _row(
            world,
            "SELECT revision,steps FROM plan_revision WHERE plan_id=:id "
            "ORDER BY revision DESC LIMIT 1",
            id=plan_id,
        )
    )
    assert saved == {"revision": int(expected) + 1, "steps": steps}
    assert (
        run(lambda: _row(world, "SELECT current_revision FROM plan WHERE id=:id", id=plan_id))[
            "current_revision"
        ]
        == int(expected) + 1
    )
    if existing:
        assert (
            run(
                lambda: _row(
                    world, "SELECT steps FROM plan_revision WHERE id=:id", id=original_revision_id
                )
            )["steps"]
            == original_steps
        )
    assert count(world, "work_item", "status='leased'") == 0
    assert count(world, "work_item", "status='pending'") == 1
    assert run(lambda: _row(world, "SELECT owner_run_id FROM session_slot"))["owner_run_id"] is None
    with pytest.raises(AgentRuntimeError):
        run(lambda: committer.commit(staged, frame))
    if scenario == "create":
        from agent_runtime.contracts.scheduler import ClaimedWork
        from agent_runtime.contracts.work_claims import WorkClaimRequest

        request = WorkClaimRequest.model_validate(
            {
                "scopeId": world.scope,
                "worker": {
                    "workerId": "plan-continuation-worker",
                    "runtimeRevision": {
                        "key": {"namespace": "builtin", "key": "runtime"},
                        "revision": "1",
                    },
                    "capabilities": [],
                },
                "leaseSeconds": 30,
                "lanes": ["main"],
                "maintenanceKinds": [],
            }
        )
        claimed = run(lambda: SqlScheduler(engine, world.schema).claim_work(request))
        assert isinstance(claimed, ClaimedWork)
        assert claimed.state_version == result.state_version
        assert claimed.lease.run_id == world.run_id
    run(engine.dispose)


def test_model_event_sink_publishes_only_committed_output(world):
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    engine = engine_for(world.schema, search_path=False)
    request = make_request().model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "attempt_id": attempt_id,
        }
    )
    sink = SqlModelEventSink(
        SqlJournal(engine, world.schema, actor(world=world)),
        lease=lease(world),
        turn_id=world.turn_id,
        attempt_id=attempt_id,
        item_id=item_id,
        item_revision="1",
        trace_id="model-stream",
    )
    run(
        lambda: sink.record(
            request,
            ProviderContentDelta(
                attempt_id=attempt_id, sequence="0", kind="content.delta", text="hello"
            ),
        )
    )
    run(
        lambda: sink.record(
            request,
            ProviderAnalysisDelta(
                attempt_id=attempt_id, sequence="1", kind="analysis.delta", text="reason"
            ),
        )
    )
    assert count(world, "event", "session_id = :session", session=world.session_id) == 2
    assert count(world, "outbox") == 2
    reader = SqlJournalReader(engine, world.schema)
    page = run(
        lambda: reader.read_public(
            actor(world=world),
            StreamRequest(session_id=world.session_id, after_seq="0", last_event_id=None),
        )
    )
    assert [event.payload.text for event in page.events] == ["hello"]
    run(engine.dispose)


def test_journal_derives_replays_and_fences_output_seal(world):
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    engine = engine_for(world.schema, search_path=False)
    journal = SqlJournal(engine, world.schema, actor(world=world))
    batch = _output_batch(world, attempt_id=attempt_id, item_id=item_id)
    receipt = run(lambda: journal.append(batch))
    request = SealOutputRequest(
        lease=lease(world),
        attempt_id=attempt_id,
        item_id=item_id,
        proof_id=uuid.uuid4(),
        sealed_revision="1",
    )

    proof = run(lambda: journal.seal_output(request))
    channels = {channel.channel: channel for channel in proof.channels}
    assert channels["content"].digest == hashlib.sha256(b"hello").hexdigest()
    assert channels["analysis"].digest == hashlib.sha256(b"private reasoning").hexdigest()
    assert channels["content"].last_delta_index == "0"
    assert channels["content"].last_session_seq == receipt.entries[0].session_seq
    assert channels["analysis"].last_session_seq == receipt.entries[1].session_seq
    assert proof.durable_seq == "2" and count(world, "output_seal") == 1

    other_item = run(lambda: _seed_item(world))
    other_attempt = run(lambda: _seed_attempt(world))
    run(lambda: journal.append(_output_batch(world, attempt_id=other_attempt, item_id=other_item)))
    replay = run(lambda: journal.seal_output(request))
    assert replay == proof and replay.durable_seq == "2"
    assert all(
        entry.disposition == "reused" for entry in run(lambda: journal.append(batch)).entries
    )

    next_batch = ProducerBatch.model_validate(
        {
            **batch.model_dump(mode="python"),
            "events": [
                {
                    "kind": "assistant.delta",
                    "occurredAt": datetime.now(UTC),
                    "payload": {
                        "channel": "content",
                        "deltaIndex": "1",
                        "itemRevision": "1",
                        "text": "too late",
                    },
                }
            ],
        }
    )
    with pytest.raises(AgentRuntimeError) as sealed_error:
        run(lambda: journal.append(next_batch))
    assert classify(sealed_error.value) == "VERSION_CONFLICT"
    assert count(world, "event") == 4 and count(world, "outbox") == 4
    run(engine.dispose)


def test_journal_changed_replay_and_outbox_failure_are_atomic(world):
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    engine = engine_for(world.schema, search_path=False)
    journal = SqlJournal(engine, world.schema, actor(world=world))
    batch = _output_batch(world, attempt_id=attempt_id, item_id=item_id)
    run(lambda: journal.append(batch))

    changed = _output_batch(
        world, attempt_id=attempt_id, item_id=item_id, content="changed payload"
    )
    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: journal.append(changed))
    assert classify(error.value) == "INVALID_INPUT"
    assert count(world, "event") == 2
    assert (
        scalar(world.schema, "SELECT event_seq FROM session WHERE id = :id", id=world.session_id)
        == 2
    )

    next_batch = ProducerBatch.model_validate(
        {
            **batch.model_dump(mode="python"),
            "events": [
                {
                    "kind": "assistant.delta",
                    "occurredAt": datetime.now(UTC),
                    "payload": {
                        "channel": "content",
                        "deltaIndex": "1",
                        "itemRevision": "1",
                        "text": "next",
                    },
                }
            ],
        }
    )
    journal._sql = replace(journal._sql, outbox_insert=sa.text("SELECT 1 / 0"))
    with pytest.raises(sa.exc.DBAPIError):
        run(lambda: journal.append(next_batch))
    assert count(world, "event") == 2
    assert count(world, "outbox") == 2
    assert (
        scalar(world.schema, "SELECT event_seq FROM session WHERE id = :id", id=world.session_id)
        == 2
    )
    run(engine.dispose)


def test_journal_rejects_cross_batch_delta_gap_without_writes(world):
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    engine = engine_for(world.schema, search_path=False)
    journal = SqlJournal(engine, world.schema, actor(world=world))
    initial = _output_batch(world, attempt_id=attempt_id, item_id=item_id)
    run(lambda: journal.append(initial))
    changed_revision = ProducerBatch.model_validate(
        {
            **initial.model_dump(mode="python"),
            "events": [
                {
                    "kind": "assistant.delta",
                    "occurredAt": datetime.now(UTC),
                    "payload": {
                        "channel": "content",
                        "deltaIndex": "1",
                        "itemRevision": "2",
                        "text": "wrong revision",
                    },
                }
            ],
        }
    )
    with pytest.raises(AgentRuntimeError) as revision_error:
        run(lambda: journal.append(changed_revision))
    assert classify(revision_error.value) == "INVALID_INPUT"
    gap = ProducerBatch.model_validate(
        {
            **initial.model_dump(mode="python"),
            "events": [
                {
                    "kind": "assistant.delta",
                    "occurredAt": datetime.now(UTC),
                    "payload": {
                        "channel": "content",
                        "deltaIndex": "2",
                        "itemRevision": "1",
                        "text": "missing delta one",
                    },
                }
            ],
        }
    )

    with pytest.raises(AgentRuntimeError) as error:
        run(lambda: journal.append(gap))
    assert classify(error.value) == "INVALID_INPUT"
    assert count(world, "event") == 2
    assert count(world, "outbox") == 2
    assert (
        scalar(world.schema, "SELECT event_seq FROM session WHERE id = :id", id=world.session_id)
        == 2
    )
    run(engine.dispose)


def test_journal_concurrent_appends_allocate_one_contiguous_session_sequence(world):
    first_item = run(lambda: _seed_item(world))
    first_attempt = run(lambda: _seed_attempt(world))
    second_item = run(lambda: _seed_item(world))
    second_attempt = run(lambda: _seed_attempt(world))
    engine = engine_for(world.schema, search_path=False)
    first = SqlJournal(engine, world.schema, actor(world=world))
    second = SqlJournal(engine, world.schema, actor(world=world))

    async def append_both():
        return await asyncio.gather(
            first.append(_output_batch(world, attempt_id=first_attempt, item_id=first_item)),
            second.append(_output_batch(world, attempt_id=second_attempt, item_id=second_item)),
        )

    receipts = run(append_both)
    seqs = sorted(int(entry.session_seq) for receipt in receipts for entry in receipt.entries)
    assert seqs == [1, 2, 3, 4]
    assert count(world, "event") == 4 and count(world, "outbox") == 4
    assert (
        scalar(world.schema, "SELECT event_seq FROM session WHERE id = :id", id=world.session_id)
        == 4
    )
    run(engine.dispose)


def test_journal_revalidates_lease_producer_authority_and_terminal_state(world):
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    foreign_item = run(lambda: _seed_item(world, run_id=world.sender_run_id))
    foreign_attempt = run(lambda: _seed_attempt(world, run_id=world.sender_run_id))
    engine = engine_for(world.schema, search_path=False)
    journal = SqlJournal(engine, world.schema, actor(world=world))
    batch = _output_batch(world, attempt_id=attempt_id, item_id=item_id)

    stale = batch.model_copy(
        update={
            "lease": lease(
                world,
                run_lease_until=datetime.now(UTC) - timedelta(seconds=1),
                work_lease_until=datetime.now(UTC) - timedelta(seconds=1),
            )
        }
    )
    with pytest.raises(AgentRuntimeError) as stale_error:
        run(lambda: journal.append(stale))
    assert classify(stale_error.value) == "FENCED"

    for invalid in (
        _output_batch(world, attempt_id=foreign_attempt, item_id=item_id),
        _output_batch(world, attempt_id=attempt_id, item_id=foreign_item),
    ):
        with pytest.raises(AgentRuntimeError) as owner_error:
            run(lambda invalid=invalid: journal.append(invalid))
        assert classify(owner_error.value) == "INVALID_INPUT"

    async def set_authority_revision(revision: int) -> None:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    f'UPDATE "{world.schema}".run SET authority_revision = :revision '  # noqa: S608
                    "WHERE id = :id"
                ),
                {"revision": revision, "id": world.run_id},
            )

    run(lambda: set_authority_revision(2))
    with pytest.raises(AgentRuntimeError) as authority_error:
        run(lambda: journal.append(batch))
    assert classify(authority_error.value) == "FORBIDDEN"
    run(lambda: set_authority_revision(1))

    async def set_deleted_at(value: datetime | None) -> None:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    f'UPDATE "{world.schema}".session SET deleted_at = :value '  # noqa: S608
                    "WHERE id = :id"
                ),
                {"value": value, "id": world.session_id},
            )

    run(lambda: set_deleted_at(datetime.now(UTC)))
    with pytest.raises(AgentRuntimeError) as deleted_error:
        run(lambda: journal.append(batch))
    assert classify(deleted_error.value) == "VERSION_CONFLICT"
    run(lambda: set_deleted_at(None))

    async def complete_run() -> None:
        async with engine.begin() as connection:
            await connection.execute(
                sa.text(
                    f"UPDATE \"{world.schema}\".run SET status = 'completed', "  # noqa: S608
                    "phase = 'terminal', outcome = 'success', owner_id = NULL, "
                    "lease_until = NULL WHERE id = :id"
                ),
                {"id": world.run_id},
            )

    run(complete_run)
    with pytest.raises(AgentRuntimeError) as terminal_error:
        run(lambda: journal.append(batch))
    assert classify(terminal_error.value) in {"FENCED", "VERSION_CONFLICT"}
    assert count(world, "event") == 0 and count(world, "outbox") == 0
    run(engine.dispose)


def test_mixed_run_work_lease_is_fenced_for_uow_and_journal(world):
    other = run(lambda: _seed(world.schema, suffix="mixed-work"))
    mixed_lease = lease(
        world,
        work_id=other.work_id,
        work_epoch=str(other.work_epoch),
    )
    mixed_guard = state_guard(world, lease=mixed_lease)

    with pytest.raises(AgentRuntimeError) as uow_error:
        run(lambda: uow(world).commit_advance(advance_request(world, guard=mixed_guard)))
    assert classify(uow_error.value) == "FENCED"
    assert (
        scalar(world.schema, "SELECT state_version FROM run WHERE id = :id", id=world.run_id) == 1
    )
    assert (
        scalar(world.schema, "SELECT status FROM work_item WHERE id = :id", id=other.work_id)
        == "leased"
    )

    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    engine = engine_for(world.schema, search_path=False)
    journal = SqlJournal(engine, world.schema, actor(world=world))
    mixed_batch = _output_batch(world, attempt_id=attempt_id, item_id=item_id).model_copy(
        update={"lease": mixed_lease}
    )
    with pytest.raises(AgentRuntimeError) as journal_error:
        run(lambda: journal.append(mixed_batch))
    assert classify(journal_error.value) == "FENCED"
    assert count(world, "event") == 0
    assert count(world, "outbox") == 0
    assert (
        scalar(world.schema, "SELECT status FROM work_item WHERE id = :id", id=other.work_id)
        == "leased"
    )
    run(engine.dispose)


def test_journal_reader_resyncs_when_tail_event_is_missing(world):
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    engine = engine_for(world.schema, search_path=False)
    journal = SqlJournal(engine, world.schema, actor(world=world))
    reader = SqlJournalReader(engine, world.schema)
    receipt = run(
        lambda: journal.append(_output_batch(world, attempt_id=attempt_id, item_id=item_id))
    )
    tail_id = receipt.entries[-1].event_id

    async def remove_tail() -> None:
        async with engine.begin() as connection:
            await connection.execute(  # noqa: S608 - random test schema, not user input
                sa.text(f'DELETE FROM "{world.schema}".outbox WHERE event_id = :id'),
                {"id": tail_id},
            )
            await connection.execute(  # noqa: S608 - random test schema, not user input
                sa.text(f'DELETE FROM "{world.schema}".event WHERE id = :id'),
                {"id": tail_id},
            )

    run(remove_tail)
    request = StreamRequest(session_id=world.session_id, after_seq="1", last_event_id="1")
    result = run(lambda: reader.read_public(actor(world=world), request))
    assert result.kind == "resync_required" and result.reason == "projection_changed"
    run(engine.dispose)


def test_journal_reader_requires_resync_on_gap_or_auth_change(world):
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    engine = engine_for(world.schema, search_path=False)
    journal = SqlJournal(engine, world.schema, actor(world=world))
    reader = SqlJournalReader(engine, world.schema)
    receipt = run(
        lambda: journal.append(_output_batch(world, attempt_id=attempt_id, item_id=item_id))
    )

    stale_actor = actor(auth_revision=2, world=world)
    request = StreamRequest(session_id=world.session_id, after_seq="0", last_event_id=None)
    changed = run(lambda: reader.read_authorized(stale_actor, request))
    assert changed.kind == "resync_required" and changed.reason == "authorization_changed"

    first_id = receipt.entries[0].event_id

    async def remove_first() -> None:
        async with engine.begin() as connection:
            await connection.execute(  # noqa: S608 - random test schema, not user input
                sa.text(f'DELETE FROM "{world.schema}".outbox WHERE event_id = :id'),
                {"id": first_id},
            )
            await connection.execute(  # noqa: S608 - random test schema, not user input
                sa.text(f'DELETE FROM "{world.schema}".event WHERE id = :id'),
                {"id": first_id},
            )

    run(remove_first)
    gap = run(lambda: reader.read_public(actor(world=world), request))
    assert gap.kind == "resync_required" and gap.reason == "cursor_expired"
    run(engine.dispose)


def test_state_event_joins_caller_transaction_and_rolls_back(world):
    engine = engine_for(world.schema, search_path=False)
    journal = SqlJournal(engine, world.schema, actor(world=world))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    batch = StateEventBatch.model_validate(
        {
            "authority": {
                "kind": "service",
                "actor": actor(world=world),
                "purpose": "resource",
                "source": {
                    "resourceType": "artifact",
                    "resourceId": "artifact-1",
                    "resourceVersion": "1",
                    "scope": {"scopeId": world.scope},
                },
                "transactionId": str(uuid.uuid4()),
            },
            "sessionId": str(world.session_id),
            "traceId": "trace-state",
            "events": [
                {
                    "producerKey": "resource/artifact-1/revoked/2",
                    "kind": "resource.revoked",
                    "occurredAt": datetime.now(UTC),
                    "turnId": None,
                    "runId": None,
                    "parentRunId": None,
                    "stepId": None,
                    "itemId": None,
                    "modelAttemptId": None,
                    "payload": {"resourceId": "artifact-1", "authRevision": "2"},
                }
            ],
        }
    )

    async def rollback() -> None:
        with pytest.raises(RuntimeError, match="rollback"):
            async with session_factory() as session:
                async with session.begin():
                    await session.execute(
                        sa.text(
                            f'UPDATE "{world.schema}".session SET title = :title '  # noqa: S608
                            "WHERE id = :id"
                        ),
                        {"title": "must rollback", "id": world.session_id},
                    )
                    await journal.append_state_in_transaction(session, batch)
                    raise RuntimeError("rollback")

    run(rollback)
    assert count(world, "event") == 0 and count(world, "outbox") == 0
    assert (
        scalar(world.schema, "SELECT title FROM session WHERE id = :id", id=world.session_id)
        is None
    )

    async def commit() -> None:
        async with session_factory() as session:
            async with session.begin():
                await session.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".session SET title = :title '  # noqa: S608
                        "WHERE id = :id"
                    ),
                    {"title": "committed", "id": world.session_id},
                )
                receipt = await journal.append_state_in_transaction(session, batch)
                assert receipt.entries[0].session_seq == "1"

    run(commit)
    assert count(world, "event") == 1 and count(world, "outbox") == 1
    assert (
        scalar(world.schema, "SELECT title FROM session WHERE id = :id", id=world.session_id)
        == "committed"
    )
    run(engine.dispose)


def test_outbox_claims_are_disjoint_and_expired_claim_is_recoverable(world):
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    engine = engine_for(world.schema, search_path=False)
    journal = SqlJournal(engine, world.schema, actor(world=world))
    now = datetime.now(UTC)
    batch = ProducerBatch.model_validate(
        {
            "lease": lease(world),
            "turnId": str(world.turn_id),
            "attemptId": str(attempt_id),
            "itemId": str(item_id),
            "traceId": "trace-outbox-race",
            "events": [
                {
                    "kind": "assistant.delta",
                    "occurredAt": now,
                    "payload": {
                        "channel": "content",
                        "deltaIndex": str(index),
                        "itemRevision": "1",
                        "text": str(index),
                    },
                }
                for index in range(4)
            ],
        }
    )
    run(lambda: journal.append(batch))
    clock = [datetime.now(UTC)]
    first_store = SqlOutboxStore(engine, world.schema, world.scope, clock=lambda: clock[0])
    second_store = SqlOutboxStore(engine, world.schema, world.scope, clock=lambda: clock[0])

    async def claim_both():
        request = OutboxClaimRequest(limit=2, lease_seconds=5)
        return await asyncio.gather(first_store.claim(request), second_store.claim(request))

    claims = run(claim_both)
    assert all(claim is not None for claim in claims)
    event_sets = [{notice.event_id for notice in claim.notices} for claim in claims if claim]
    assert len(event_sets) == 2 and event_sets[0].isdisjoint(event_sets[1])
    clock[0] = clock[0] + timedelta(seconds=6)
    reclaimed = run(lambda: first_store.claim(OutboxClaimRequest(limit=4, lease_seconds=5)))
    assert reclaimed is not None and len(reclaimed.notices) == 4
    assert scalar(world.schema, "SELECT max(attempt_count) FROM outbox") == 2
    run(engine.dispose)


def test_live_run_guard_rechecks_lease_state_and_context(world):
    base_state, base_context, _ = decision_facts()
    current_actor = actor(world=world)
    state = base_state.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "state_version": str(world.state_version),
            "checkpoint_ref": f"state:{world.state_version + 1}",
        }
    )
    context = base_context.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
        }
    )
    frame = DecisionFrame(
        actor=current_actor,
        context=context,
        attempt_id=uuid.uuid4(),
        action=decision_adapter.validate_python(
            {"kind": "respond", "payload": {"answerRequestRef": "answer:1"}}
        ),
    )

    class Context:
        async def load(self, scope_id, snapshot_id):
            assert scope_id == world.scope and snapshot_id == context.snapshot_id
            return context, {}

    class Access:
        async def verify(self, actor_context, snapshot):
            assert actor_context == current_actor and snapshot == context

    engine = engine_for(world.schema, search_path=False)
    live = SqlLiveRunGuard(
        engine,
        world.schema,
        actor=current_actor,
        lease=lease(world),
        context=Context(),
        access=Access(),
    )
    run(lambda: live.require_running(state, frame))
    changed = state.model_copy(update={"state_version": "999"})
    with pytest.raises(AgentRuntimeError) as wrong_version:
        run(lambda: live.require_running(changed, frame))
    assert classify(wrong_version.value) == "VERSION_CONFLICT"

    unit = SqlStateUnitOfWork(engine, world.schema, current_actor)
    run(
        lambda: unit.record_control(
            ControlRequest(
                run_id=world.run_id,
                kind="pause",
                idempotency_key="guard-race-pause",
                expected_version=str(world.state_version),
            )
        )
    )
    with pytest.raises(AgentRuntimeError) as cancelled:
        run(lambda: live.require_running(state, frame))
    assert classify(cancelled.value) == "VERSION_CONFLICT"
    run(engine.dispose)


def test_staged_decision_observation_and_recovery_are_durable(world, monkeypatch):
    from agent_runtime.contracts.tools import FullResult, ToolCompleted

    attempt_id = run(lambda: _seed_attempt(world))
    snapshot_id = run(
        lambda: _row(
            world,
            "SELECT context_snapshot_id FROM model_attempt WHERE id = :id",
            id=attempt_id,
        )
    )["context_snapshot_id"]
    base_state, base_context, _ = decision_facts()
    state = base_state.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "state_version": str(world.state_version),
            "checkpoint_ref": f"state:{world.state_version + 1}",
        }
    )
    context = base_context.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "snapshot_id": snapshot_id,
        }
    )
    frame = DecisionFrame(
        actor=actor(world=world),
        context=context,
        attempt_id=attempt_id,
        action=decision_adapter.validate_python(tool_action()),
    )

    class Context:
        async def load(self, scope_id, requested_id):
            assert scope_id == world.scope and requested_id == snapshot_id
            return context, {}

    class Access:
        async def verify(self, current_actor, prepared):
            assert current_actor == frame.actor and prepared == context

    engine = engine_for(world.schema, pool_size=1, search_path=False)
    journal = SqlJournal(engine, world.schema, frame.actor)
    store = SqlDecisionStore(
        engine,
        world.schema,
        actor=frame.actor,
        lease=lease(world),
        payloads=BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema)),
        context=Context(),
        access=Access(),
        journal=journal,
    )
    call_id = frame.action.payload.validated_calls[0].tool_call_id
    original = journal.append_state_in_transaction

    async def fail_event(*args):
        raise RuntimeError("forced journal rollback")

    monkeypatch.setattr(journal, "append_state_in_transaction", fail_event)
    with pytest.raises(RuntimeError, match="forced journal rollback"):
        run(lambda: store.persist(state, frame))
    assert count(world, "tool_call", "run_id = :run", run=world.run_id) == 0
    assert count(world, "checkpoint", "run_id = :run", run=world.run_id) == 1
    assert count(world, "event", "kind = 'tool.started'") == 0
    monkeypatch.setattr(journal, "append_state_in_transaction", original)

    staged = run(lambda: store.persist(state, frame))
    assert staged.checkpoint_ref.startswith("context-")
    assert count(world, "tool_call", "run_id = :run", run=world.run_id) == 1
    assert count(world, "event", "kind = 'tool.started'") == 1
    assert count(world, "outbox", "1=1") == 1
    pending = run(store.recover)
    assert pending is not None and pending.unresolved_call_ids == (call_id,)
    assert pending.outcomes == ()
    assert pending.observation_refs == ()
    with pytest.raises(AgentRuntimeError) as unresolved:
        pending.require_next_model_sources()
    assert classify(unresolved.value) == "SIDE_EFFECT_UNKNOWN"
    assert run(lambda: store.persist(state, frame)).checkpoint_ref == staged.checkpoint_ref
    assert count(world, "tool_call", "run_id = :run", run=world.run_id) == 1

    outcome = ToolCompleted(
        tool_call_id=call_id,
        run_id=world.run_id,
        operation_id=None,
        status="completed",
        effect="none",
        effect_refs=(),
        result=FullResult(policy="full", data={"value": 391}),
    )
    monkeypatch.setattr(journal, "append_state_in_transaction", fail_event)
    with pytest.raises(RuntimeError, match="forced journal rollback"):
        run(lambda: store.record(frame, outcome))
    assert run(store.recover).unresolved_call_ids == (call_id,)
    assert count(world, "event", "kind = 'tool.finished'") == 0
    monkeypatch.setattr(journal, "append_state_in_transaction", original)

    run(lambda: store.record(frame, outcome))
    run(lambda: store.record(frame, outcome))
    recovered = run(store.recover)
    assert recovered is not None and recovered.unresolved_call_ids == ()
    assert recovered.outcomes == (outcome,)
    assert len(recovered.observation_refs) == 1
    assert recovered.observation_refs[0].resource_id == str(call_id)
    assert recovered.require_next_model_sources() == recovered.observation_refs
    assert count(world, "event", "kind = 'tool.finished'") == 1
    assert count(world, "outbox", "1=1") == 2
    source = ResourceRef.model_validate(
        {
            "resourceType": "tool_call_result",
            "resourceId": str(call_id),
            "resourceVersion": "1",
            "scope": {
                "scopeId": world.scope,
                "sessionId": str(world.session_id),
                "ownerId": world.actor_id,
            },
        }
    )
    req = context_request(refs=0).model_copy(
        update={
            "actor": frame.actor,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "resource_refs": (source,),
        }
    )
    reader = SqlRuntimeSourceReader(
        engine, world.schema, blobs=SqlContextBlobBackend(engine, world.schema)
    )
    assert "391" in run(lambda: reader.read(req, source)).content
    with pytest.raises(AgentRuntimeError):
        run(lambda: reader.read(req, source.model_copy(update={"resource_version": "2"})))
    run(engine.dispose)


def test_wait_action_commits_checkpoint_state_and_public_event_atomically(world):
    attempt_id = run(lambda: _seed_attempt(world))
    snapshot_id = run(
        lambda: _row(
            world,
            "SELECT context_snapshot_id FROM model_attempt WHERE id = :id",
            id=attempt_id,
        )
    )["context_snapshot_id"]
    base_state, base_context, _ = decision_facts()
    state = base_state.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "state_version": str(world.state_version),
            "checkpoint_ref": f"state:{world.state_version + 1}",
        }
    )
    context = base_context.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "snapshot_id": snapshot_id,
        }
    )
    predicate = CapabilityRef.model_validate(
        {"key": {"namespace": "builtin", "key": "deadline"}, "revision": "1"}
    )
    wait_deadline = datetime.now(UTC) + timedelta(minutes=10)
    frame = DecisionFrame(
        actor=actor(world=world),
        context=context,
        attempt_id=attempt_id,
        action=decision_adapter.validate_python(
            {
                "kind": "wait",
                "payload": {
                    "waitSpec": {
                        "kind": "timer",
                        "subjectId": "timer:1",
                        "deadlineAt": wait_deadline,
                        "resumePredicate": predicate.model_dump(mode="json", by_alias=True),
                    }
                },
            }
        ),
    )

    class Context:
        async def load(self, scope_id, requested_id):
            assert scope_id == world.scope and requested_id == snapshot_id
            return context, {}

    class Access:
        async def verify(self, current_actor, prepared):
            assert current_actor == frame.actor and prepared == context

    engine = engine_for(world.schema, pool_size=1, search_path=False)
    journal = SqlJournal(engine, world.schema, frame.actor)
    current_lease = lease(world)
    payloads = BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema))
    store = SqlDecisionStore(
        engine,
        world.schema,
        actor=frame.actor,
        lease=current_lease,
        payloads=payloads,
        context=Context(),
        access=Access(),
        journal=journal,
    )
    staged = run(lambda: store.persist(state, frame))
    committer = SqlWaitActionCommitter(
        actor=frame.actor,
        lease=current_lease,
        decisions=store,
        scheduler=SqlScheduler(engine, world.schema, journal=journal),
        allowed_predicates=(predicate,),
    )
    for invalid_kind, invalid_subject in (
        ("message", "another-run"),
        ("tool", "not-a-tool-call-id"),
        ("approval", "not-an-approval-id"),
    ):
        invalid_action = decision_adapter.validate_python(
            {
                "kind": "wait",
                "payload": {
                    "waitSpec": {
                        "kind": invalid_kind,
                        "subjectId": invalid_subject,
                        "deadlineAt": wait_deadline,
                        "resumePredicate": predicate.model_dump(mode="json", by_alias=True),
                    }
                },
            }
        )
        with pytest.raises(AgentRuntimeError) as invalid_wait:
            run(
                lambda action=invalid_action: committer.commit(
                    staged, replace(frame, action=action)
                )
            )
        assert invalid_wait.value.failure.code == "INVALID_INPUT"
    assert count(world, "event", "kind = 'run.status'") == 0
    committed = run(lambda: committer.commit(staged, frame))
    assert committed.status == RunStatus.WAITING_TOOL
    assert committed.state_version == str(world.state_version + 1)
    assert count(world, "event", "kind = 'run.status'") == 1
    assert count(world, "outbox") == 1
    run_row = run(
        lambda: _row(world, "SELECT status, waiting_spec FROM run WHERE id=:id", id=world.run_id)
    )
    work_row = run(
        lambda: _row(world, "SELECT status FROM work_item WHERE id=:id", id=world.work_id)
    )
    assert run_row["status"] == "waiting_tool"
    assert work_row["status"] == "completed"
    with pytest.raises(AgentRuntimeError):
        run(lambda: committer.commit(staged, frame))
    wake_request = WakeRequest(
        actor=frame.actor,
        session_id=world.session_id,
        run_id=world.run_id,
        expected_state_version=int(committed.state_version),
        kind="timer",
        subject_id="timer:1",
    )
    with pytest.raises(AgentRuntimeError):
        run(
            lambda: SqlWaitWaker(
                engine, world.schema, actor=frame.actor, journal=journal, payloads=payloads
            ).wake(wake_request)
        )
    assert count(world, "event", "kind = 'run.status'") == 1
    waker = SqlWaitWaker(
        engine,
        world.schema,
        actor=frame.actor,
        journal=journal,
        payloads=payloads,
        clock=lambda: wait_deadline + timedelta(seconds=1),
    )
    receipt = run(lambda: waker.wake(wake_request))
    assert receipt.state_version == world.state_version + 2
    assert count(world, "event", "kind = 'run.status'") == 2
    assert count(world, "outbox") == 2
    assert (
        run(lambda: _row(world, "SELECT status FROM run WHERE id=:id", id=world.run_id))["status"]
        == "queued"
    )
    assert (
        run(lambda: _row(world, "SELECT status FROM work_item WHERE id=:id", id=receipt.work_id))[
            "status"
        ]
        == "pending"
    )
    with pytest.raises(AgentRuntimeError):
        run(lambda: waker.wake(wake_request))
    run(engine.dispose)


def test_main_run_loader_requires_claimed_root_and_current_epoch(world: World) -> None:
    from agent_runtime.contracts.scheduler import ClaimedWork
    from agent_runtime.worker import LeaseHandle

    engine = engine_for(world.schema, search_path=False)
    budget_id = uuid.uuid4()

    async def align_checkpoint() -> None:
        async with engine.begin() as db:
            await db.execute(
                sa.text(
                    f"UPDATE {world.schema}.checkpoint SET state_version=:version "
                    "WHERE scope_id=:scope AND id=:id"
                ),
                {"version": world.state_version, "scope": world.scope, "id": world.checkpoint_id},
            )
            await db.execute(
                sa.text(
                    f"INSERT INTO {world.schema}.budget_allocation "
                    "(id,scope_id,root_run_id,limits,reserved,unknown_reserved,settled,"
                    "usage_status,unsettled_count,version) VALUES "
                    "(:id,:scope,:run,CAST(:limits AS jsonb),CAST(:reserved AS jsonb),"
                    "CAST(:unknown AS jsonb),CAST(:settled AS jsonb),'complete',0,1)"
                ),
                {
                    "id": budget_id,
                    "scope": world.scope,
                    "run": world.run_id,
                    "limits": json.dumps({"model_calls": 8, "tool_calls": 5, "tokens": 1000}),
                    "reserved": json.dumps({"model_calls": 2, "tool_calls": 1, "tokens": 200}),
                    "unknown": json.dumps({"model_calls": 0, "tool_calls": 0, "tokens": 0}),
                    "settled": json.dumps({"model_calls": 1, "tool_calls": 0, "tokens": 100}),
                },
            )

    run(align_checkpoint)

    class Setup:
        async def resolve(self, *_):
            raise AssertionError("loading the Run must not call the model")

    agent = CapabilityRef.model_validate(
        {
            "key": {"namespace": "test", "key": f"agent-{world.suffix}"},
            "revision": "1",
        }
    )
    claimed = ClaimedWork(
        disposition="claimed",
        lease=lease(world),
        state_version=str(world.state_version),
        checkpoint_id=world.checkpoint_id,
        budget_allocation_id=budget_id,
    )
    handle = LeaseHandle(claimed.lease)
    loader = SqlMainRunLoader(
        engine,
        world.schema,
        actor=actor(world.auth_revision, world=world),
        agents={world.agent_revision_id: MainAgentBinding(agent, Setup())},
    )
    loaded = run(lambda: loader.load(claimed, handle))
    assert loaded.state.run_id == world.run_id
    assert loaded.state.checkpoint_ref == f"state:{world.state_version + 1}"
    assert loaded.inputs.applied_seq == "0"
    assert loaded.binding.agent == agent
    assert loaded.turn_id == world.turn_id
    budget_reader = SqlMainBudgetReader(
        engine,
        world.schema,
        actor=actor(world.auth_revision, world=world),
        lease=handle.current,
        allocation_id=budget_id,
    )
    balance = run(lambda: budget_reader.read(loaded.state))
    assert balance.model_calls.available == 5
    assert balance.tool_calls.available == 4
    assert balance.tokens.available == 700
    run(lambda: _seed_attempt(world))
    unknown_usage = run(lambda: budget_reader.read(loaded.state))
    assert unknown_usage.model_calls.available == 0
    assert unknown_usage.tokens.available == 0
    assert unknown_usage.tool_calls.available == 4

    uninstalled = SqlMainRunLoader(
        engine, world.schema, actor=actor(world.auth_revision, world=world), agents={}
    )
    with pytest.raises(AgentRuntimeError) as missing:
        run(lambda: uninstalled.load(claimed, handle))
    assert missing.value.failure.code == "FORBIDDEN"

    async def advance_work_epoch() -> None:
        async with engine.begin() as db:
            await db.execute(
                sa.text(
                    f"UPDATE {world.schema}.work_item SET lease_epoch=lease_epoch+1 "
                    "WHERE scope_id=:scope AND id=:id"
                ),
                {"scope": world.scope, "id": world.work_id},
            )

    run(advance_work_epoch)
    with pytest.raises(AgentRuntimeError) as stale:
        run(lambda: loader.load(claimed, handle))
    assert stale.value.failure.code == "FENCED"
    with pytest.raises(AgentRuntimeError) as stale_budget:
        run(lambda: budget_reader.read(loaded.state))
    assert stale_budget.value.failure.code == "FENCED"
    run(engine.dispose)


@pytest.mark.parametrize("admitted", [False, True])
def test_main_stream_seals_and_publishes_root_answer(world: World, admitted: bool) -> None:
    from agent_runtime.contracts.canonical import content_digest
    from agent_runtime.contracts.loop import AppliedInputs
    from agent_runtime.contracts.payloads import TurnAccepted
    from agent_runtime.contracts.state_events import ServiceEventAuthority, TurnAcceptedDraft
    from agent_runtime.contracts.views import TimelineItem
    from agent_runtime.worker import LeaseHandle

    engine = engine_for(world.schema, search_path=False)
    reserved_id = uuid.uuid4()
    if admitted:
        runtime_actor = actor(world.auth_revision, world=world)
        run(
            lambda: SqlJournal(engine, world.schema, runtime_actor).append_state(
                StateEventBatch(
                    authority=ServiceEventAuthority(
                        kind="service",
                        actor=runtime_actor,
                        purpose="admission",
                        transaction_id=world.turn_id,
                        source=ResourceRef.model_validate(
                            {
                                "resourceType": "turn",
                                "resourceId": str(world.turn_id),
                                "resourceVersion": "0",
                                "scope": {
                                    "scopeId": world.scope,
                                    "sessionId": str(world.session_id),
                                },
                            }
                        ),
                    ),
                    session_id=world.session_id,
                    trace_id=str(world.turn_id),
                    events=(
                        TurnAcceptedDraft(
                            kind="turn.accepted",
                            producer_key=f"admission:{world.turn_id}",
                            occurred_at=datetime.now(UTC),
                            turn_id=world.turn_id,
                            run_id=world.run_id,
                            parent_run_id=None,
                            step_id=None,
                            item_id=None,
                            model_attempt_id=None,
                            payload=TurnAccepted(
                                turn_id=world.turn_id,
                                run_id=world.run_id,
                                assistant_item_id=reserved_id,
                                user_item=TimelineItem(
                                    id=uuid.uuid4(),
                                    type="message",
                                    role="user",
                                    created_seq="1",
                                    revision="0",
                                    status="completed",
                                    channels=(),
                                    resource_refs=(),
                                ),
                            ),
                        ),
                    ),
                )
            )
        )
    attempt_id = run(lambda: _seed_attempt(world))
    snapshot_id = run(
        lambda: _row(
            world,
            "SELECT context_snapshot_id FROM model_attempt WHERE id=:id",
            id=attempt_id,
        )
    )["context_snapshot_id"]
    state = decision_facts()[0].model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "state_version": str(world.state_version),
        }
    )
    context = decision_facts()[1].model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "snapshot_id": snapshot_id,
        }
    )

    class Setup:
        async def resolve(self, *_):
            raise AssertionError("stream publication must not call the model")

    loaded = LoadedMainRun(
        state=state,
        inputs=AppliedInputs(
            run_id=world.run_id,
            checkpoint_ref=state.checkpoint_ref,
            previous_seq="0",
            applied_seq="0",
            messages=(),
        ),
        binding=MainAgentBinding(state.agent, Setup()),
        turn_id=world.turn_id,
        budget_allocation_id=uuid.uuid4(),
    )
    stream = SqlMainStream(
        engine,
        world.schema,
        actor=actor(world.auth_revision, world=world),
        loaded=loaded,
        lease=LeaseHandle(lease(world)),
    )
    request = make_request().model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "attempt_id": attempt_id,
            "context_snapshot_id": snapshot_id,
        }
    )
    run(
        lambda: stream.record(
            request,
            ProviderContentDelta(
                attempt_id=attempt_id, sequence="0", kind="content.delta", text="hello"
            ),
        )
    )
    run(
        lambda: stream.record(
            request,
            ProviderAnalysisDelta(
                attempt_id=attempt_id, sequence="1", kind="analysis.delta", text="private"
            ),
        )
    )
    digest = content_digest({"runId": str(world.run_id), "text": "hello"})
    ref = f"answer-{digest}"
    run(lambda: stream.save(world.scope, str(world.run_id), ref, "hello"))
    frame = DecisionFrame(
        actor=actor(world.auth_revision, world=world),
        context=context,
        attempt_id=attempt_id,
        action=decision_adapter.validate_python(
            {"kind": "respond", "payload": {"answerRequestRef": ref}}
        ),
    )
    published = run(lambda: stream.publish(state, frame))
    assert len(published.item_ids) == 1
    if admitted:
        assert published.item_ids == (reserved_id,)
    assert run(lambda: stream.publish(state, frame)) == published
    assert count(world, "event") == (5 if admitted else 4)
    assert count(world, "output_seal") == 1
    item = run(
        lambda: _row(
            world, "SELECT status FROM timeline_item WHERE id=:id", id=published.item_ids[0]
        )
    )
    assert item["status"] == "completed"
    run(engine.dispose)


def _exercise_main_worker_answer(
    world: World,
    *,
    live: bool,
    interrupt: str | None = None,
    retry_once: bool = False,
    failure_kind: str = "transient",
    live_fault: str | None = None,
) -> None:
    import httpx
    from runtime_provider_faults import InterruptingTransport

    from agent_runtime.agent_loop import DecisionSetup
    from agent_runtime.contracts.context import ContextRequest
    from agent_runtime.contracts.failure import FailureEnvelope
    from agent_runtime.contracts.model import ModelConfig, ProviderCapabilities
    from agent_runtime.contracts.provider import ProviderError, ProviderFinish, ProviderUsage
    from agent_runtime.contracts.results import Usage
    from agent_runtime.contracts.scheduler import ClaimedWork
    from agent_runtime.contracts.work_claims import WorkClaimRequest
    from agent_runtime.providers.context_input import ContextOpenAIInputResolver
    from agent_runtime.providers.openai_compatible import (
        OpenAIAdapterProfile,
        OpenAICompatibleProvider,
        ProviderConnection,
    )
    from agent_runtime.storage.context_factory import build_sql_context_components
    from agent_runtime.storage.host_worker import (
        SqlClaimActorResolver,
        SqlClaimMainHandler,
        SqlWorkerRecoverySink,
    )
    from agent_runtime.storage.main_stream import SqlMainOutputFactory
    from agent_runtime.storage.main_worker import SqlMainWorkerHandler
    from agent_runtime.storage.recovery import RecoveryConfig, SqlRecovery
    from agent_runtime.storage.subagents import SqlSubagentRuntime
    from agent_runtime.worker import LeaseHandle

    base_url = os.environ.get("BASE_URL", "")
    model_name = os.environ.get("MODEL_NAME", "") if live else "scripted-provider"
    if live and (not base_url or not model_name):
        pytest.skip("BASE_URL and MODEL_NAME are required for live Provider test")

    engine = engine_for(world.schema, search_path=False)
    budget_id = uuid.uuid4()

    async def seed() -> None:
        async with engine.begin() as db:
            if retry_once or live_fault is not None:
                await db.execute(
                    sa.text(f"UPDATE {world.schema}.work_item SET attempt_count=1 WHERE id=:id"),
                    {"id": world.work_id},
                )
            await db.execute(
                sa.text(
                    f"UPDATE {world.schema}.turn SET primary_run_id=:run "
                    "WHERE scope_id=:scope AND id=:turn"
                ),
                {"run": world.run_id, "scope": world.scope, "turn": world.turn_id},
            )
            await db.execute(
                sa.text(
                    f"UPDATE {world.schema}.checkpoint SET state_version=:version "
                    "WHERE scope_id=:scope AND id=:id"
                ),
                {"version": world.state_version, "scope": world.scope, "id": world.checkpoint_id},
            )
            await db.execute(
                sa.text(
                    f"INSERT INTO {world.schema}.budget_allocation "
                    "(id,scope_id,root_run_id,limits,reserved,unknown_reserved,settled,"
                    "usage_status,unsettled_count,version) VALUES "
                    "(:id,:scope,:run,CAST(:limits AS jsonb),'{}','{}','{}','complete',0,1)"
                ),
                {
                    "id": budget_id,
                    "scope": world.scope,
                    "run": world.run_id,
                    "limits": json.dumps({"model_calls": 4, "tool_calls": 4, "tokens": 12000}),
                },
            )

    run(seed)
    runtime_actor = actor(world.auth_revision, world=world)
    agent = CapabilityRef.model_validate(
        {"key": {"namespace": "test", "key": f"agent-{world.suffix}"}, "revision": "1"}
    )

    def capability(name: str) -> CapabilityRef:
        return CapabilityRef.model_validate(
            {"key": {"namespace": "live", "key": name}, "revision": "1"}
        )

    model_ref = capability("model")
    finish_item_id: uuid.UUID | None = None
    question = "What is 17 multiplied by 23?"
    prompt = "Answer the user's arithmetic question accurately and briefly."
    if interrupt is not None:
        question = "Write a long numbered list of at least 200 practical database safety checks."
        prompt = "Answer at length. Do not call tools."
    model_config = ModelConfig(
        ref=model_ref,
        connection=capability("connection"),
        model_name=model_name,
        max_output_tokens=1024,
        thinking="disabled",
        provider_options={"chat_template_kwargs": {"enable_thinking": False}},
    )

    class Estimator:
        def count(self, text: str) -> int:
            return max(1, len(text) // 3)

    class Setup:
        async def resolve(self, state, inputs, budget):
            turn_prompt = (
                prompt
                if finish_item_id is None
                else (
                    "The answer was durably published. Return only this JSON decision: "
                    + json.dumps(
                        {
                            "kind": "finish",
                            "payload": {
                                "outcome": "success",
                                "evidenceRefs": [],
                                "finalItemIds": [str(finish_item_id)],
                            },
                        }
                    )
                )
            )
            request = ContextRequest.model_validate(
                {
                    "actor": runtime_actor.model_dump(mode="json", by_alias=True),
                    "sessionId": str(world.session_id),
                    "runId": str(world.run_id),
                    "profile": capability("profile").model_dump(mode="json", by_alias=True),
                    "model": model_ref.model_dump(mode="json", by_alias=True),
                    "prompt": capability("prompt").model_dump(mode="json", by_alias=True),
                    "tools": [],
                    "resourceRefs": [],
                    "budget": {
                        "allocationId": str(budget_id),
                        "maxInputTokens": 4096,
                        "reservedOutputTokens": 1024,
                    },
                    "promptText": turn_prompt,
                    "userInput": question
                    if finish_item_id is None
                    else (
                        "Complete the already published answer, without answering again. "
                        "Use exactly the finalItemIds from the supplied finish decision."
                    ),
                }
            )
            return DecisionSetup(
                request, model_config, "structured" if finish_item_id is not None else "native"
            )

    class Definitions:
        async def definition(self, _tool):
            raise AssertionError("direct-answer scenario must not resolve a tool")

    class Unexpected:
        def __getattr__(self, name):
            raise AssertionError(f"direct-answer scenario called {name}")

    context = build_sql_context_components(engine, world.schema, estimator=Estimator())
    resolver = ContextOpenAIInputResolver(
        context=context.runtime, actor=runtime_actor, access=context.access, tools=Definitions()
    )
    profile = OpenAIAdapterProfile.qwen()

    async def connections(_request):
        return ProviderConnection(base_url=base_url.rstrip("/"), api_key=os.environ.get("API_KEY"))

    async def capabilities(_request):
        return ProviderCapabilities(
            model=model_ref,
            streaming="supported",
            tool_calls="supported",
            structured_output="unknown",
            thinking_control="supported",
            tools_with_structured_output="unknown",
            tools_with_thinking="supported",
            usage="unknown",
            cancellation="supported",
            context_window=262144,
            max_output_tokens=65536,
            allowed_option_keys=("chat_template_kwargs",),
        )

    journal = SqlJournal(engine, world.schema, runtime_actor)
    reader = SqlRuntimeSourceReader(
        engine, world.schema, blobs=SqlContextBlobBackend(engine, world.schema)
    )
    predicate = capability("message-predicate")
    subagents = SqlSubagentRuntime(
        engine,
        world.schema,
        actor=runtime_actor,
        journal=journal,
        resources=reader,
        policies={},
        message_predicate=predicate,
        verifier=Unexpected(),
    )
    claimed = ClaimedWork(
        disposition="claimed",
        lease=lease(world),
        state_version=str(world.state_version),
        checkpoint_id=world.checkpoint_id,
        budget_allocation_id=budget_id,
    )

    statuses: list[int] = []
    request_shapes: list[dict[str, object]] = []
    injected_faults: list[str] = []

    async def note_request(request: httpx.Request) -> None:
        body = json.loads(request.content)
        request_shapes.append(
            {
                "model": body.get("model"),
                "messageCount": len(body.get("messages", [])),
                "messageChars": sum(
                    len(str(m.get("content", ""))) for m in body.get("messages", [])
                ),
                "toolCount": len(body.get("tools", [])),
                "parameterKeys": sorted(set(body) - {"messages", "tools"}),
            }
        )

    async def note_status(response: httpx.Response) -> None:
        statuses.append(response.status_code)

    class FixedProvider:
        failed_once = False

        async def stream(self, request, *, cancel=None):
            fail = retry_once and not self.failed_once
            self.failed_once = True
            content = (
                "391"
                if finish_item_id is None
                else json.dumps(
                    {
                        "kind": "finish",
                        "payload": {
                            "outcome": "success",
                            "evidenceRefs": [],
                            "finalItemIds": [str(finish_item_id)],
                        },
                    }
                )
            )
            yield ProviderContentDelta(
                attempt_id=request.attempt_id,
                sequence="0",
                kind="content.delta",
                text=(" \n" if failure_kind == "empty" else "obsolete partial output")
                if fail
                else content,
            )
            yield ProviderUsage(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="usage",
                usage=Usage(
                    status="complete",
                    reason="provider_reported",
                    input_tokens="10",
                    output_tokens="10",
                    total_tokens="20",
                ),
            )
            if fail:
                if failure_kind in {"length", "empty"}:
                    yield ProviderFinish(
                        attempt_id=request.attempt_id,
                        sequence="2",
                        kind="finish",
                        reason="length" if failure_kind == "length" else "stop",
                        raw_reason=failure_kind,
                    )
                    return
                yield ProviderError(
                    attempt_id=request.attempt_id,
                    sequence="2",
                    kind="error",
                    failure=FailureEnvelope(
                        code="DEPENDENCY_UNAVAILABLE",
                        category="provider",
                        retryable=True,
                        public_message="Injected temporary provider failure after partial output.",
                        attempt_id=str(request.attempt_id),
                    ),
                )
                return
            yield ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="2",
                kind="finish",
                reason="stop",
                raw_reason=None,
            )

    class FaultInjectedLiveProvider:
        """Real upstream inference; explicit normalized-event fault injection, not an outage."""

        def __init__(self, upstream):
            self.upstream = upstream
            self.injected = False

        async def stream(self, request, *, cancel=None):
            inject = not self.injected
            self.injected = True
            upstream = self.upstream.stream(request, cancel=cancel)
            try:
                async for event in upstream:
                    async for output in self.inject(event, request, inject):
                        yield output
            finally:
                await upstream.aclose()

        async def inject(self, event, request, inject):
            if not inject:
                yield event
                return
            assert not isinstance(event, ProviderError), "upstream failed before fault injection"
            if isinstance(event, ProviderContentDelta):
                if live_fault == "empty":
                    yield event.model_copy(update={"text": " "})
                else:
                    yield event
                return
            if isinstance(event, ProviderFinish):
                assert event.reason == "stop", "upstream must finish before terminal injection"
                injected_faults.append(live_fault)
                yield event.model_copy(
                    update={"reason": "stop" if live_fault == "empty" else "length"}
                )
            else:
                yield event

    async def scenario() -> None:
        nonlocal finish_item_id
        transport = (
            InterruptingTransport(httpx.AsyncHTTPTransport(), live_fault, injected_faults)
            if live_fault in {"timeout", "disconnect"}
            else None
        )
        async with httpx.AsyncClient(
            trust_env=False,
            transport=transport,
            timeout=httpx.Timeout(100.0),
            event_hooks={"request": [note_request], "response": [note_status]},
        ) as client:
            provider = (
                OpenAICompatibleProvider(
                    client, inputs=resolver, connections=connections, profile=profile
                )
                if live
                else FixedProvider()
            )
            if live_fault is not None and live_fault not in {"timeout", "disconnect"}:
                assert live
                provider = FaultInjectedLiveProvider(provider)
            handler = SqlMainWorkerHandler(
                engine,
                world.schema,
                actor=runtime_actor,
                estimator=Estimator(),
                provider=provider,
                capabilities=capabilities,
                validate_provider_options=profile.validate_options,
                agents={world.agent_revision_id: MainAgentBinding(agent, Setup())},
                specs=Unexpected(),
                tool_resources=Unexpected(),
                policy=Unexpected(),
                tools=Unexpected(),
                resources=reader,
                outputs=SqlMainOutputFactory(engine, world.schema, runtime_actor),
                scheduler=SqlScheduler(engine, world.schema, journal=journal),
                subagents=subagents,
                allowed_predicates=(predicate,),
                max_decisions=2,
            )

            class Factory:
                async def open(self, resolved, work, lease):
                    assert resolved.actor_id == runtime_actor.actor_id
                    assert resolved.auth_revision == runtime_actor.auth_revision
                    return handler

            host_handler = SqlClaimMainHandler(
                SqlClaimActorResolver(engine, world.schema), Factory()
            )
            async with asyncio.timeout(120):
                if interrupt is not None:
                    task = asyncio.create_task(host_handler(claimed, LeaseHandle(claimed.lease)))
                    try:
                        while True:
                            async with engine.connect() as db:
                                delta_count = (
                                    await db.execute(
                                        sa.text(
                                            f"SELECT count(*) FROM {world.schema}.event "
                                            "WHERE run_id=:run AND kind='assistant.delta'"
                                        ),
                                        {"run": world.run_id},
                                    )
                                ).scalar_one()
                            if delta_count:
                                break
                            if task.done():
                                await task
                                raise AssertionError("model completed before interruption")
                            await asyncio.sleep(0.05)
                        await SqlStateUnitOfWork(
                            engine, world.schema, runtime_actor
                        ).record_control(
                            ControlRequest(
                                run_id=world.run_id,
                                kind=interrupt,
                                expected_version=str(world.state_version),
                                idempotency_key=f"interrupt-{interrupt}",
                            )
                        )
                        with pytest.raises(AgentRuntimeError) as interrupted:
                            await task
                    finally:
                        if not task.done():
                            task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                    await SqlRecovery(engine, world.schema).handle_worker_failure(
                        claimed, interrupted.value
                    )
                    from agent_runtime.contracts.work_claims import ControlClaimedWork

                    control = await SqlScheduler(engine, world.schema).claim_work(
                        WorkClaimRequest.model_validate(
                            {
                                "scopeId": world.scope,
                                "worker": {
                                    "workerId": "interruption-control-worker",
                                    "runtimeRevision": {
                                        "key": {"namespace": "builtin", "key": "runtime"},
                                        "revision": "1",
                                    },
                                    "capabilities": [],
                                },
                                "leaseSeconds": 30,
                                "lanes": ["control"],
                                "maintenanceKinds": [],
                            }
                        )
                    )
                    assert isinstance(control, ControlClaimedWork)
                    await host_handler(control, LeaseHandle(control.lease))
                    async with engine.connect() as db:
                        output = (
                            (
                                await db.execute(
                                    sa.text(
                                        "SELECT status,content_ref FROM "
                                        f"{world.schema}.timeline_item "
                                        "WHERE run_id=:run"
                                    ),
                                    {"run": world.run_id},
                                )
                            )
                            .mappings()
                            .one()
                        )
                        assert output["status"] == "cancelled"
                        assert output["content_ref"] != "pending-main-output"
                        watermark = (
                            await db.execute(
                                sa.text(
                                    f"SELECT event_seq FROM {world.schema}.session WHERE id=:id"
                                ),
                                {"id": world.session_id},
                            )
                        ).scalar_one()
                        checkpoint_version = (
                            await db.execute(
                                sa.text(
                                    "SELECT c.state_version=r.state_version "
                                    f"FROM {world.schema}.run r "
                                    f"JOIN {world.schema}.checkpoint c ON c.id=r.checkpoint_id "
                                    "WHERE r.id=:run"
                                ),
                                {"run": world.run_id},
                            )
                        ).scalar_one()
                        assert checkpoint_version is True
                    await asyncio.sleep(0.3)
                    async with engine.connect() as db:
                        assert (
                            await db.execute(
                                sa.text(
                                    f"SELECT event_seq FROM {world.schema}.session WHERE id=:id"
                                ),
                                {"id": world.session_id},
                            )
                        ).scalar_one() == watermark
                    return
                if live_fault in {"timeout", "disconnect", "exhausted"}:
                    with pytest.raises(AgentRuntimeError) as failed:
                        await host_handler(claimed, LeaseHandle(claimed.lease))
                    await SqlWorkerRecoverySink(
                        SqlRecovery(
                            engine,
                            world.schema,
                            config=RecoveryConfig(
                                maximum_attempts=1 if live_fault == "exhausted" else 5
                            ),
                        )
                    )(claimed, failed.value)
                    return
                if retry_once:
                    with pytest.raises(AgentRuntimeError) as failed:
                        await host_handler(claimed, LeaseHandle(claimed.lease))
                    recovery = SqlRecovery(
                        engine, world.schema, config=RecoveryConfig(base_retry_seconds=1)
                    )
                    await SqlWorkerRecoverySink(recovery)(claimed, failed.value)
                    async with engine.connect() as db:
                        available = (
                            await db.execute(
                                sa.text(
                                    f"SELECT available_at FROM {world.schema}.work_item "
                                    "WHERE id=:id"
                                ),
                                {"id": world.work_id},
                            )
                        ).scalar_one()
                    retry_claim = await SqlScheduler(
                        engine, world.schema, clock=lambda: available + timedelta(microseconds=1)
                    ).claim_work(
                        WorkClaimRequest.model_validate(
                            {
                                "scopeId": world.scope,
                                "worker": {
                                    "workerId": "retry-worker",
                                    "runtimeRevision": {
                                        "key": {"namespace": "builtin", "key": "runtime"},
                                        "revision": "1",
                                    },
                                    "capabilities": [],
                                },
                                "leaseSeconds": 300,
                                "lanes": ["main"],
                                "maintenanceKinds": [],
                            }
                        )
                    )
                    assert isinstance(retry_claim, ClaimedWork)
                    await host_handler(retry_claim, LeaseHandle(retry_claim.lease))
                else:
                    await host_handler(claimed, LeaseHandle(claimed.lease))
                async with engine.connect() as db:
                    finish_item_id = (
                        await db.execute(
                            sa.text(
                                f"SELECT id FROM {world.schema}.timeline_item "
                                "WHERE scope_id=:scope AND run_id=:run AND status='completed'"
                            ),
                            {"scope": world.scope, "run": world.run_id},
                        )
                    ).scalar_one()
                next_claim = await SqlScheduler(engine, world.schema).claim_work(
                    WorkClaimRequest.model_validate(
                        {
                            "scopeId": world.scope,
                            "worker": {
                                "workerId": "answer-continuation-worker",
                                "runtimeRevision": {
                                    "key": {"namespace": "builtin", "key": "runtime"},
                                    "revision": "1",
                                },
                                "capabilities": [],
                            },
                            "leaseSeconds": 30,
                            "lanes": ["main"],
                            "maintenanceKinds": [],
                        }
                    )
                )
                assert isinstance(next_claim, ClaimedWork)
                await host_handler(next_claim, LeaseHandle(next_claim.lease))

    try:
        try:
            run(scenario)
        except AgentRuntimeError as exc:
            raise AssertionError(
                f"live root worker failed with {exc.failure.code}; "
                f"provider HTTP {statuses}; request {request_shapes}"
            ) from exc
        if live_fault is not None:
            assert injected_faults == [live_fault]
            assert statuses and all(status == 200 for status in statuses)
        if interrupt is not None:
            assert scalar(world.schema, "SELECT status FROM run WHERE id=:id", id=world.run_id) == (
                "paused" if interrupt == "pause" else "cancelled"
            )
            assert (
                count(world, "model_attempt", "run_id=:id AND status='cancelled'", id=world.run_id)
                == 1
            )
            assert count(world, "output_seal", "run_id=:id", id=world.run_id) == 1
            assert (
                count(world, "event", "run_id=:id AND kind='item.completed'", id=world.run_id) == 1
            )
            return
        if live_fault in {"timeout", "disconnect", "exhausted"}:
            assert (
                scalar(world.schema, "SELECT status FROM run WHERE id=:id", id=world.run_id)
                == "failed"
            )
            assert count(world, "model_attempt", "run_id=:id", id=world.run_id) == 1
            assert count(world, "output_seal", "run_id=:id", id=world.run_id) == 1
            terminal = run(
                lambda: _row(
                    world,
                    "SELECT payload FROM event WHERE run_id=:id AND kind='run.failed'",
                    id=world.run_id,
                )
            )["payload"]
            assert terminal["publicMessage"].strip()
            assert terminal["nextSteps"]
            if live_fault in {"timeout", "disconnect"}:
                assert terminal["failure"]["code"] == (
                    "PROVIDER_TIMEOUT" if live_fault == "timeout" else "DEPENDENCY_UNAVAILABLE"
                )
                assert (
                    scalar(
                        world.schema,
                        "SELECT usage_status FROM model_attempt WHERE run_id=:id",
                        id=world.run_id,
                    )
                    == "unknown"
                )
            return
        expected_status = "completed"
        expected_models = 2
        assert (
            scalar(world.schema, "SELECT status FROM run WHERE id=:id", id=world.run_id)
            == expected_status
        )
        assert (
            count(world, "model_attempt", "run_id=:id AND status='completed'", id=world.run_id)
            == expected_models
        )
        assert (
            count(world, "timeline_item", "run_id=:id AND status='completed'", id=world.run_id) == 1
        )
        assert count(world, "timeline_item", "run_id=:id", id=world.run_id) == 1 + retry_once
        assert count(world, "output_seal", "run_id=:id", id=world.run_id) == 1 + retry_once
        if retry_once:
            assert (
                count(world, "model_attempt", "run_id=:id AND status='failed'", id=world.run_id)
                == 1
            )
            assert (
                count(world, "timeline_item", "run_id=:id AND status='cancelled'", id=world.run_id)
                == 1
            )
            assert (
                scalar(
                    world.schema,
                    "SELECT count(DISTINCT model_attempt_id) FROM output_seal WHERE run_id=:id",
                    id=world.run_id,
                )
                == 2
            )
        assert count(world, "event", "run_id=:id AND kind='assistant.delta'", id=world.run_id) > 0
        if interrupt is None:

            async def final_answer_text() -> str:
                async with engine.connect() as db:
                    content = (
                        await db.execute(
                            sa.text(
                                f"SELECT b.content FROM {world.schema}.timeline_item i "
                                f"JOIN {world.schema}.context_blob b "
                                "ON b.scope_id=i.scope_id AND b.blob_key=i.content_ref "
                                "WHERE i.run_id=:run AND i.status='completed'"
                            ),
                            {"run": world.run_id},
                        )
                    ).scalar_one()
                    return json.loads(bytes(content))["text"]

            answer = run(final_answer_text)
            if live:
                assert "391" in answer, "real model answer must solve 17 * 23 correctly"
            else:
                assert answer == "391"
            turn = run(
                lambda: _row(
                    world,
                    "SELECT outcome_run_id,outcome FROM turn WHERE id=:id",
                    id=world.turn_id,
                )
            )
            assert turn == {"outcome_run_id": world.run_id, "outcome": "success"}
    finally:
        run(engine.dispose)


def test_main_worker_commits_answer(world: World) -> None:
    _exercise_main_worker_answer(world, live=False)


@pytest.mark.parametrize("failure_kind", ("transient", "length", "empty"))
def test_main_worker_retries_with_new_attempt_and_separate_output(
    world: World, failure_kind: str
) -> None:
    _exercise_main_worker_answer(world, live=False, retry_once=True, failure_kind=failure_kind)


@pytest.mark.skipif(
    os.environ.get("AGENT_RUNTIME_LIVE_PROVIDER") != "1",
    reason="requires an explicitly enabled live Provider",
)
def test_main_worker_commits_live_qwen_answer(world: World) -> None:
    _exercise_main_worker_answer(world, live=True)


@pytest.mark.skipif(
    os.environ.get("AGENT_RUNTIME_LIVE_PROVIDER") != "1",
    reason="requires an explicitly enabled live Provider",
)
@pytest.mark.parametrize("kind", ("pause", "cancel"))
def test_main_worker_interrupts_live_qwen_and_recovers_output(world: World, kind: str) -> None:
    _exercise_main_worker_answer(world, live=True, interrupt=kind)


@pytest.mark.skipif(
    os.environ.get("AGENT_RUNTIME_LIVE_PROVIDER") != "1",
    reason="requires an explicitly enabled live Provider",
)
@pytest.mark.parametrize("fault", ("length", "empty", "timeout", "disconnect", "exhausted"))
def test_main_worker_live_qwen_fault_recovery(world: World, fault: str) -> None:
    _exercise_main_worker_answer(
        world, live=True, live_fault=fault, retry_once=fault in {"length", "empty"}
    )


@pytest.mark.parametrize("change", ("none", "revoked", "deleted", "epoch", "owner"))
def test_host_resolves_claim_identity_from_current_rows(world: World, change: str) -> None:
    from agent_runtime.storage.host_worker import SqlClaimActorResolver

    engine = engine_for(world.schema, search_path=False)

    async def alter() -> None:
        statements = {
            "revoked": "UPDATE capability_grant SET revoked_at=now() WHERE run_id=:id",
            "deleted": "UPDATE session SET deleted_at=now() WHERE id=:id",
            "epoch": "UPDATE run SET owner_epoch=owner_epoch+1 WHERE id=:id",
            "owner": "UPDATE session SET owner_actor_id='different-user' WHERE id=:id",
        }
        if change != "none":
            async with engine.begin() as db:
                await db.execute(
                    sa.text(statements[change].replace("UPDATE ", f"UPDATE {world.schema}.")),
                    {"id": world.run_id if change in {"revoked", "epoch"} else world.session_id},
                )

    try:
        run(alter)
        resolver = SqlClaimActorResolver(engine, world.schema)
        if change == "none":
            resolved = run(lambda: resolver.resolve(lease(world)))
            assert resolved.actor_id == world.actor_id
            assert resolved.scope_id == world.scope
        else:
            with pytest.raises(AgentRuntimeError) as error:
                run(lambda: resolver.resolve(lease(world)))
            assert classify(error.value) == ("FENCED" if change == "epoch" else "FORBIDDEN")
    finally:
        run(engine.dispose)


@pytest.mark.parametrize("kind", ("pause", "cancel"))
@pytest.mark.parametrize("fail_publication", (False, True))
def test_host_control_applies_safe_point_without_model(
    world: World, kind: str, fail_publication: bool, monkeypatch
) -> None:
    from agent_runtime.contracts.work_claims import ControlClaimedWork
    from agent_runtime.storage.main_control import SqlMainControlHandler
    from agent_runtime.worker import LeaseHandle

    receipt = intake(world, kind, f"host-{kind}")
    row = run(lambda: _row(world, "SELECT * FROM run WHERE id=:id", id=world.run_id))
    claimed = ControlClaimedWork(
        disposition="control_claimed",
        lease=lease(world),
        state_version=str(row["state_version"]),
        command_id=receipt.command_id,
        desired_control=kind,
        status=row["status"],
        cancel_version=str(row["cancel_version"]),
        checkpoint_id=row["checkpoint_id"],
    )
    engine = engine_for(world.schema, search_path=False)
    try:
        handler = SqlMainControlHandler(engine, world.schema, actor(world=world))
        if fail_publication:
            original = SqlJournal.append_state_in_transaction

            async def broken_journal(self, *args, **kwargs):
                await original(self, *args, **kwargs)
                raise RuntimeError("control publication failed after append")

            monkeypatch.setattr(SqlJournal, "append_state_in_transaction", broken_journal)
            with pytest.raises(RuntimeError, match="control publication failed"):
                run(lambda: handler(claimed, LeaseHandle(claimed.lease)))
            assert (
                scalar(world.schema, "SELECT status FROM run WHERE id=:id", id=world.run_id)
                == row["status"]
            )
            assert (
                scalar(
                    world.schema, "SELECT status FROM command WHERE id=:id", id=receipt.command_id
                )
                == "accepted"
            )
            assert count(world, "event", "run_id=:id", id=world.run_id) == 0
            assert count(world, "outbox") == 0
            return
        run(lambda: handler(claimed, LeaseHandle(claimed.lease)))
        assert scalar(world.schema, "SELECT status FROM run WHERE id=:id", id=world.run_id) == (
            "paused" if kind == "pause" else "cancelled"
        )
        assert (
            scalar(world.schema, "SELECT status FROM command WHERE id=:id", id=receipt.command_id)
            == "applied"
        )
        events = 2 if kind == "pause" else 3
        assert count(world, "event", "run_id=:id", id=world.run_id) == events
        assert count(world, "outbox") == events
        if kind == "cancel":
            terminal = run(
                lambda: _row(world, "SELECT payload FROM event WHERE kind='run.cancelled'")
            )
            assert terminal["payload"]["publicMessage"]
            assert terminal["payload"]["nextSteps"]

        async def verify_non_output_events() -> None:
            from agent_runtime.storage.uow.evidence import (
                build_evidence_statements,
                verify_flush_evidence,
            )

            async with async_sessionmaker(engine)() as db:
                await verify_flush_evidence(
                    db,
                    build_evidence_statements(world.schema),
                    {"id": world.run_id, "scope_id": world.scope},
                    producers_confirmed(run_id=world.run_id, enumerated=events),
                )

        run(verify_non_output_events)
    finally:
        run(engine.dispose)


@pytest.mark.parametrize("blocker", ("model", "execution", "unsealed"))
def test_host_control_does_not_invent_safe_point_evidence(world: World, blocker: str) -> None:
    from agent_runtime.contracts.work_claims import ControlClaimedWork
    from agent_runtime.storage.main_control import SqlMainControlHandler
    from agent_runtime.worker import LeaseHandle

    engine = engine_for(world.schema, search_path=False)

    async def arrange() -> None:
        if blocker == "execution":
            await _seed_execution(world)
        else:
            attempt = await _seed_attempt(world)
            if blocker == "model":
                async with engine.begin() as db:
                    await db.execute(
                        sa.text(
                            f"UPDATE {world.schema}.model_attempt "
                            "SET status='running',finished_at=NULL "
                            "WHERE id=:id"
                        ),
                        {"id": attempt},
                    )
            else:
                item = await _seed_item(world)
                await _seed_event(world, sequence=1, item_id=item, attempt_id=attempt)

    try:
        run(arrange)
        receipt = intake(world, "cancel", f"blocked-{blocker}")
        row = run(lambda: _row(world, "SELECT * FROM run WHERE id=:id", id=world.run_id))
        claimed = ControlClaimedWork(
            disposition="control_claimed",
            lease=lease(world),
            state_version=str(row["state_version"]),
            command_id=receipt.command_id,
            desired_control="cancel",
            status="cancelling",
            cancel_version=str(row["cancel_version"]),
            checkpoint_id=row["checkpoint_id"],
        )
        handler = SqlMainControlHandler(engine, world.schema, actor(world=world))
        with pytest.raises(AgentRuntimeError) as error:
            run(lambda: handler(claimed, LeaseHandle(claimed.lease)))
        assert error.value.failure.code == "CONTROL_PENDING"
        assert error.value.failure.retryable is True
        assert (
            scalar(world.schema, "SELECT status FROM run WHERE id=:id", id=world.run_id)
            == "cancelling"
        )
        assert (
            scalar(world.schema, "SELECT status FROM command WHERE id=:id", id=receipt.command_id)
            == "accepted"
        )
    finally:
        run(engine.dispose)


@pytest.mark.parametrize(
    ("crash_after_seal", "empty"), ((False, False), (True, False), (False, True))
)
@pytest.mark.parametrize("normal_claim", (False, True))
def test_control_recovers_partial_channels_idempotently(
    world, monkeypatch, crash_after_seal, empty, normal_claim
):
    from agent_runtime.contracts.scheduler import ClaimedWork
    from agent_runtime.contracts.work_claims import ControlClaimedWork
    from agent_runtime.storage.main_output_recovery import SqlMainOutputRecovery
    from agent_runtime.worker import LeaseHandle

    engine = engine_for(world.schema, search_path=False)
    item_id = run(lambda: _seed_item(world))
    attempt_id = run(lambda: _seed_attempt(world))
    journal = SqlJournal(engine, world.schema, actor(world=world))
    if empty:
        from agent_runtime.contracts.payloads import ItemStarted
        from agent_runtime.contracts.state_events import ItemStartedDraft

        run(
            lambda: journal.append_state(
                StateEventBatch(
                    authority=MainEventAuthority(kind="main", guard=state_guard(world)),
                    session_id=world.session_id,
                    trace_id="empty-item-recovery",
                    events=(
                        ItemStartedDraft(
                            kind="item.started",
                            producer_key=f"empty-item:{item_id}",
                            occurred_at=datetime.now(UTC),
                            turn_id=world.turn_id,
                            run_id=world.run_id,
                            parent_run_id=None,
                            step_id=None,
                            item_id=item_id,
                            model_attempt_id=attempt_id,
                            payload=ItemStarted(
                                item_type="message",
                                role="assistant",
                                channels=("content", "analysis"),
                                item_revision="1",
                            ),
                        ),
                    ),
                )
            )
        )
    else:
        run(
            lambda: journal.append(
                _output_batch(
                    world, attempt_id=attempt_id, item_id=item_id, content="partial answer"
                )
            )
        )

    async def arrange():
        async with engine.begin() as db:
            await db.execute(
                sa.text(f"UPDATE {world.schema}.timeline_item SET status='streaming' WHERE id=:id"),
                {"id": item_id},
            )
            await db.execute(
                sa.text(f"UPDATE {world.schema}.model_attempt SET status='cancelled' WHERE id=:id"),
                {"id": attempt_id},
            )

    run(arrange)
    receipt = None if normal_claim else intake(world, "cancel", "recover-output")
    row = run(lambda: _row(world, "SELECT * FROM run WHERE id=:id", id=world.run_id))
    control = (
        ClaimedWork(
            disposition="claimed",
            lease=lease(world),
            state_version=str(row["state_version"]),
            checkpoint_id=row["checkpoint_id"],
            budget_allocation_id=uuid.uuid4(),
        )
        if normal_claim
        else ControlClaimedWork(
            disposition="control_claimed",
            lease=lease(world),
            state_version=str(row["state_version"]),
            command_id=receipt.command_id,
            desired_control="cancel",
            status="cancelling",
            cancel_version=str(row["cancel_version"]),
            checkpoint_id=row["checkpoint_id"],
        )
    )
    recovery = SqlMainOutputRecovery(engine, world.schema, actor(world=world))
    handle = LeaseHandle(control.lease)
    try:
        if normal_claim:
            run(lambda: recovery.recover(control, handle, protected_attempt_id=attempt_id))
            assert count(world, "output_seal") == 0
            assert count(world, "event", "kind='item.completed'") == 0
        if crash_after_seal:
            original = SqlJournal.append_state_in_transaction

            async def fail_after_append(self, *args, **kwargs):
                await original(self, *args, **kwargs)
                raise RuntimeError("crash after seal before publication")

            with monkeypatch.context() as patch:
                patch.setattr(SqlJournal, "append_state_in_transaction", fail_after_append)
                with pytest.raises(RuntimeError, match="crash after seal"):
                    run(lambda: recovery.recover(control, handle))
            assert count(world, "output_seal") == 1
            assert count(world, "event", "kind='item.completed'") == 0
            assert (
                scalar(world.schema, "SELECT status FROM timeline_item WHERE id=:id", id=item_id)
                == "streaming"
            )
        run(lambda: recovery.recover(control, handle))
        run(lambda: recovery.recover(control, handle))
        assert count(world, "output_seal") == (0 if empty else 1)
        assert count(world, "event", "kind='item.completed'") == 1
        item = run(
            lambda: _row(
                world, "SELECT status,content_ref FROM timeline_item WHERE id=:id", id=item_id
            )
        )
        assert item["status"] == "cancelled"
        payload = run(
            lambda: BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema)).get(
                world.scope, item["content_ref"]
            )
        )
        assert payload["text"] == ("" if empty else "partial answer")
        assert "private reasoning" not in str(payload)
        assert (
            scalar(world.schema, "SELECT payload->>'status' FROM event WHERE kind='item.completed'")
            == "cancelled"
        )
        assert count(world, "outbox") == (2 if empty else 3)
    finally:
        run(engine.dispose)


def test_recovery_fences_abandoned_model_with_pending_pause(world):
    from agent_runtime.storage.recovery import SqlRecovery

    engine = engine_for(world.schema, search_path=False)
    attempt_id = run(lambda: _seed_attempt(world))
    intake(world, "pause", "pause-before-worker-loss")

    async def arrange():
        async with engine.begin() as db:
            await db.execute(
                sa.text(
                    f"UPDATE {world.schema}.model_attempt SET status='running',finished_at=NULL "
                    "WHERE id=:id"
                ),
                {"id": attempt_id},
            )
            for table, identity in (("run", world.run_id), ("work_item", world.work_id)):
                await db.execute(
                    sa.text(
                        f"UPDATE {world.schema}.{table} SET lease_until=now()-interval '1 second' "
                        "WHERE id=:id"
                    ),
                    {"id": identity},
                )
            await db.execute(
                sa.text(f"UPDATE {world.schema}.work_item SET attempt_count=99 WHERE id=:id"),
                {"id": world.work_id},
            )

    try:
        run(arrange)
        report = run(lambda: SqlRecovery(engine, world.schema).recover_expired_leases(world.scope))
        assert report.requeued == 1
        assert (
            scalar(world.schema, "SELECT status FROM model_attempt WHERE id=:id", id=attempt_id)
            == "interrupted"
        )
        assert (
            scalar(
                world.schema, "SELECT usage_status FROM model_attempt WHERE id=:id", id=attempt_id
            )
            == "unknown"
        )
        assert (
            scalar(world.schema, "SELECT desired_control FROM run WHERE id=:id", id=world.run_id)
            == "pause"
        )
        assert (
            scalar(
                world.schema,
                "SELECT owner_run_id FROM session_slot WHERE session_id=:id",
                id=world.session_id,
            )
            == world.run_id
        )
    finally:
        run(engine.dispose)


@pytest.mark.parametrize(
    "scenario",
    (
        "waiting",
        "pending_input",
        "journal_failure",
        "human_reply",
        "human_rollback",
        "human_race",
    ),
)
def test_ask_user_publishes_question_and_wakes_only_after_new_input(world, monkeypatch, scenario):
    attempt_id = run(lambda: _seed_attempt(world))
    snapshot_id = run(
        lambda: _row(
            world,
            "SELECT context_snapshot_id FROM model_attempt WHERE id=:id",
            id=attempt_id,
        )
    )["context_snapshot_id"]
    run_deadline = run(
        lambda: _row(world, "SELECT deadline_at FROM run WHERE id=:id", id=world.run_id)
    )["deadline_at"]
    base_state, base_context, _ = decision_facts()
    state = base_state.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "state_version": str(world.state_version),
            "checkpoint_ref": f"state:{world.state_version + 1}",
        }
    )
    context = base_context.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "snapshot_id": snapshot_id,
        }
    )
    frame = DecisionFrame(
        actor=actor(world=world),
        context=context,
        attempt_id=attempt_id,
        action=decision_adapter.validate_python(
            {
                "kind": "ask_user",
                "payload": {
                    "question": "Which reporting period should I use?",
                    "requiredFacts": ["reporting period"],
                },
            }
        ),
    )

    class Context:
        async def load(self, scope_id, requested_id):
            assert scope_id == world.scope and requested_id == snapshot_id
            return context, {}

    class Access:
        async def verify(self, current_actor, prepared):
            assert current_actor == frame.actor and prepared == context

    engine = engine_for(world.schema, pool_size=1, search_path=False)
    journal = SqlJournal(engine, world.schema, frame.actor)
    payloads = BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema))
    store = SqlDecisionStore(
        engine,
        world.schema,
        actor=frame.actor,
        lease=lease(world),
        payloads=payloads,
        context=Context(),
        access=Access(),
        journal=journal,
    )
    staged = run(lambda: store.persist(state, frame))
    predicate = CapabilityRef.model_validate(
        {"key": {"namespace": "builtin", "key": "user_reply"}, "revision": "1"}
    )
    committer = SqlWaitActionCommitter(
        actor=frame.actor,
        lease=lease(world),
        decisions=store,
        scheduler=SqlScheduler(engine, world.schema, journal=journal),
        allowed_predicates=(predicate,),
        message_predicate=predicate,
        message_deadline_at=run_deadline,
    )
    if scenario == "waiting":
        unconfigured = SqlWaitActionCommitter(
            actor=frame.actor,
            lease=lease(world),
            decisions=store,
            scheduler=SqlScheduler(engine, world.schema, journal=journal),
            allowed_predicates=(predicate,),
        )
        with pytest.raises(AgentRuntimeError) as missing_wait_policy:
            run(lambda: unconfigured.commit(staged, frame))
        assert missing_wait_policy.value.failure.code == "FORBIDDEN"
        late_wait = SqlWaitActionCommitter(
            actor=frame.actor,
            lease=lease(world),
            decisions=store,
            scheduler=SqlScheduler(engine, world.schema, journal=journal),
            allowed_predicates=(predicate,),
            message_predicate=predicate,
            message_deadline_at=run_deadline + timedelta(seconds=1),
        )
        with pytest.raises(AgentRuntimeError) as invalid_deadline:
            run(lambda: late_wait.commit(staged, frame))
        assert invalid_deadline.value.failure.code == "INVALID_INPUT"
        assert count(world, "event") == 0
        assert count(world, "outbox") == 0
    if scenario == "pending_input":
        run(lambda: _seed_messages(world, (1,), allocated=1, claim_token=uuid.uuid4()))
        staged = staged.model_copy(update={"pending_input_seq": "1"})
    if scenario == "journal_failure":
        original = journal.append_state_in_transaction

        async def fail_after_events(*args):
            await original(*args)
            raise RuntimeError("forced ask-user rollback")

        monkeypatch.setattr(journal, "append_state_in_transaction", fail_after_events)
        with pytest.raises(RuntimeError, match="forced ask-user rollback"):
            run(lambda: committer.commit(staged, frame))
        assert count(world, "event") == 0
        assert count(world, "outbox") == 0
        unchanged = run(
            lambda: _row(
                world, "SELECT status, state_version FROM run WHERE id=:id", id=world.run_id
            )
        )
        assert unchanged["status"] == "running"
        assert unchanged["state_version"] == world.state_version
        assert (
            run(lambda: _row(world, "SELECT status FROM work_item WHERE id=:id", id=world.work_id))[
                "status"
            ]
            == "leased"
        )
        recovered = run(store.recover)
        assert recovered is not None and recovered.frame.action == frame.action
        monkeypatch.setattr(journal, "append_state_in_transaction", original)
    committed = run(lambda: committer.commit(staged, frame))
    if scenario == "pending_input":
        assert committed.status == RunStatus.QUEUED
        assert count(world, "event", "kind = 'input.required'") == 0
        assert count(world, "event", "kind = 'run.status'") == 1
        assert count(world, "outbox") == 1
        assert count(world, "work_item", "run_id=:id AND status='pending'", id=world.run_id) == 1
        run(engine.dispose)
        return
    assert committed.status == RunStatus.WAITING_USER
    if scenario.startswith("human_"):
        from agent_runtime.contracts.requests import InteractionRequest
        from agent_runtime.storage.interactions import SqlInteractionService
        from agent_runtime.storage.run_backend import SqlRunBackend

        reply_engine = engine_for(world.schema, pool_size=4, search_path=False)
        service = SqlInteractionService(reply_engine, world.schema)
        reply = InteractionRequest(
            session_id=world.session_id,
            run_id=world.run_id,
            expected_version=committed.state_version,
            idempotency_key="human-reply",
            text="2026年第三季度",
        )
        before = count(world, "event")
        with pytest.raises(AgentRuntimeError) as denied:
            run(lambda: service.submit(frame.actor.model_copy(update={"actor_id": "other"}), reply))
        assert denied.value.failure.code == "FORBIDDEN"
        if scenario == "human_rollback":
            append = SqlJournal.append_state_in_transaction

            async def fail_reply(self, *args):
                await append(self, *args)
                raise RuntimeError("reply commit interrupted")

            monkeypatch.setattr(SqlJournal, "append_state_in_transaction", fail_reply)
            with pytest.raises(RuntimeError, match="reply commit interrupted"):
                run(lambda: service.submit(frame.actor, reply))
            assert count(world, "event") == before
            assert count(world, "timeline_item", "item_type='user_reply'") == 0
            assert (
                count(world, "work_item", "run_id=:id AND status='pending'", id=world.run_id) == 0
            )
            monkeypatch.setattr(SqlJournal, "append_state_in_transaction", append)
        if scenario == "human_race":

            async def race():
                return await asyncio.gather(
                    service.submit(frame.actor, reply),
                    service.submit(
                        frame.actor, reply.model_copy(update={"idempotency_key": "racer"})
                    ),
                    return_exceptions=True,
                )

            outcomes = run(race)
            assert sum(isinstance(value, AgentRuntimeError) for value in outcomes) == 1
            accepted = next(value for value in outcomes if not isinstance(value, BaseException))
        else:
            accepted = run(lambda: service.submit(frame.actor, reply))
            assert run(lambda: service.submit(frame.actor, reply)) == accepted
            with pytest.raises(AgentRuntimeError) as reused:
                run(
                    lambda: service.submit(
                        frame.actor, reply.model_copy(update={"text": "changed"})
                    )
                )
            assert reused.value.failure.code == "VERSION_CONFLICT"
        assert count(world, "event") == before + 2
        assert count(world, "timeline_item", "item_type='user_reply'") == 1
        assert count(world, "work_item", "run_id=:id AND status='pending'", id=world.run_id) == 1
        current = run(
            lambda: _row(
                world,
                "SELECT status,pending_input_version,applied_input_version FROM run WHERE id=:id",
                id=world.run_id,
            )
        )
        assert current["status"] == "queued"
        assert current["pending_input_version"] == current["applied_input_version"] == 0
        snapshot = run(
            lambda: SqlRunBackend(reply_engine, world.schema).snapshot(
                frame.actor, world.session_id
            )
        )
        assert not snapshot.input_requests
        assert any(item.id == accepted.input_id for item in snapshot.items)
        ref = ResourceRef(
            resource_type="interaction_input",
            resource_id=str(accepted.input_id),
            resource_version="1",
            scope={"scopeId": world.scope, "sessionId": str(world.session_id)},
        )
        reader = SqlRuntimeSourceReader(
            reply_engine, world.schema, blobs=SqlContextBlobBackend(reply_engine, world.schema)
        )
        run(lambda: reader.authorize_resource(frame.actor, ref))
        if scenario == "human_reply":
            with pytest.raises(RuntimeError, match="cannot downgrade while human replies exist"):
                run(
                    lambda: execute(
                        "downgrade", DSN, world.schema, "0007_capability_superseded", 30
                    )
                )
            assert migrate("current", world.schema).current_revision == "0008_human_input"
        run(reply_engine.dispose)
        run(engine.dispose)
        return
    assert count(world, "event", "kind = 'run.status'") == 1
    assert count(world, "event", "kind = 'input.required'") == 1
    assert count(world, "outbox") == 2
    question = run(
        lambda: _row(
            world,
            "SELECT payload, visibility FROM event WHERE kind='input.required'",
        )
    )
    assert question["visibility"] == "public"
    assert question["payload"] == {
        "stateVersion": str(world.state_version + 1),
        "question": "Which reporting period should I use?",
        "requiredFacts": ["reporting period"],
        "replyToRunId": str(world.run_id),
    }
    with pytest.raises(AgentRuntimeError):
        run(lambda: committer.commit(staged, frame))
    wake_request = WakeRequest(
        actor=frame.actor,
        session_id=world.session_id,
        run_id=world.run_id,
        expected_state_version=int(committed.state_version),
        kind="message",
        subject_id=str(world.run_id),
    )
    waker = SqlWaitWaker(
        engine, world.schema, actor=frame.actor, journal=journal, payloads=payloads
    )
    with pytest.raises(AgentRuntimeError):
        run(lambda: waker.wake(wake_request))
    run(lambda: _seed_messages(world, (1,), allocated=1, claim_token=uuid.uuid4()))
    receipt = run(lambda: waker.wake(wake_request))
    assert receipt.state_version == world.state_version + 2
    assert count(world, "event", "kind = 'input.required'") == 1
    assert count(world, "event", "kind = 'run.status'") == 2
    assert count(world, "outbox") == 3
    assert (
        run(lambda: _row(world, "SELECT status FROM run WHERE id=:id", id=world.run_id))["status"]
        == "queued"
    )
    with pytest.raises(AgentRuntimeError):
        run(lambda: waker.wake(wake_request))
    run(engine.dispose)

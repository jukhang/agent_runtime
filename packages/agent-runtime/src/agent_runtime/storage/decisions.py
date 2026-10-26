"""Lease-fenced decision and tool-observation persistence.

The current WorkItem stays leased while a decision is staged. A checkpoint
representation revision records the proposed action without advancing the run state
version. Each tool outcome is committed with its Journal event; an unresolved call is
never automatically re-executed after a crash.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.context_runtime import ContextAccessVerifier, ContextRuntime
from agent_runtime.contracts.canonical import canonical_bytes, content_digest
from agent_runtime.contracts.decisions import InvokeTools, decision_adapter
from agent_runtime.contracts.loop import RunState
from agent_runtime.contracts.ownership import ChildCommitGuard, ChildLeaseToken
from agent_runtime.contracts.payloads import (
    ApprovalRequired,
    RunStatusPayload,
    ToolFinished,
    ToolStarted,
)
from agent_runtime.contracts.references import CapabilityRef, ResourceRef, ResourceScope
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.state_events import (
    ApprovalRequiredDraft,
    ChildEventAuthority,
    MainEventAuthority,
    RunStatusDraft,
    StateEventBatch,
    ToolFinishedDraft,
    ToolStartedDraft,
)
from agent_runtime.contracts.tasks import WaitSpec
from agent_runtime.contracts.tools import (
    PendingExecution,
    ToolCompleted,
    ToolOutcome,
    ToolWaitingApproval,
    tool_outcome_adapter,
)
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.contracts.transitions import StateWriteGuard
from agent_runtime.loop_runner import RecoveredDecision
from agent_runtime.storage.child_authority import require_child
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.uow import errors, facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped

Clock = Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class TransitionCheckpoint:
    checkpoint_id: UUID
    state_ref: str


class SqlDecisionStore:
    """Implements decision and observation ports for a single claimed worker.

    Tool execution remains the separate ToolHost's responsibility. In particular, an
    unresolved call after a crash requires explicit reconciliation, even for a read tool.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        lease: LeaseToken | ChildLeaseToken | Callable[[], LeaseToken | ChildLeaseToken],
        payloads: BlobContextPayloadStore,
        context: ContextRuntime,
        access: ContextAccessVerifier,
        journal: SqlJournal,
        clock: Clock = lambda: datetime.now(UTC),
        child_guard: Callable[[], ChildCommitGuard] | None = None,
    ) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._schema = require_schema(schema)
        self._actor = actor
        self._lease_source = lease if callable(lease) else lambda: lease
        self._payloads = payloads
        self._context = context
        self._access = access
        self._journal = journal
        self._clock = clock
        self._child_guard = child_guard
        self._locks = locks.build_lock_statements(schema)
        self._checkpoint = scoped(
            "SELECT id, run_id, state_version, representation_revision, state_ref, "
            "content_hash FROM {t}checkpoint WHERE scope_id = :scope_id AND id = :id",
            schema,
        )
        self._attempt = scoped(
            "SELECT run_id, context_snapshot_id, status FROM {t}model_attempt "
            "WHERE scope_id = :scope_id AND id = :id",
            schema,
        )
        self._blob_lock = scoped(
            "SELECT digest FROM {t}context_blob WHERE scope_id = :scope_id "
            "AND blob_key = :ref FOR KEY SHARE",
            schema,
        )
        self._blob_read = scoped(
            "SELECT digest, size_bytes, content FROM {t}context_blob "
            "WHERE scope_id = :scope_id AND blob_key = :ref",
            schema,
        )
        self._insert_checkpoint = scoped(
            "INSERT INTO {t}checkpoint (id, scope_id, run_id, source_checkpoint_id, "
            "state_version, representation_revision, event_seq, schema_revision, "
            "runtime_build, state_ref, content_hash) VALUES (:id, :scope_id, :run_id, "
            ":source_id, :state_version, :representation_revision, :event_seq, 1, "
            "'agent-runtime', :state_ref, :content_hash)",
            schema,
        )
        self._insert_transition_checkpoint = scoped(
            "INSERT INTO {t}checkpoint (id, scope_id, run_id, source_checkpoint_id, "
            "state_version, representation_revision, event_seq, schema_revision, "
            "runtime_build, state_ref, content_hash) VALUES (:id, :scope_id, :run_id, "
            ":source_id, :state_version, 0, :event_seq, 1, 'agent-runtime', "
            ":state_ref, :content_hash) ON CONFLICT DO NOTHING",
            schema,
        )
        self._set_checkpoint = scoped(
            "UPDATE {t}run SET checkpoint_id = :new_id, updated_at = :now "
            "WHERE scope_id = :scope_id AND id = :run_id AND checkpoint_id = :old_id "
            "AND state_version = :state_version",
            schema,
        )
        self._insert_call = scoped(
            "INSERT INTO {t}tool_call (id, scope_id, run_id, attempt_id, "
            "provider_call_id, spec_revision, input_hash, status) "
            "VALUES (:id, :scope_id, :run_id, :attempt_id, :provider_call_id, "
            "CAST(:spec_revision AS jsonb), :input_hash, 'pending')",
            schema,
        )
        self._calls = scoped(
            "SELECT id, attempt_id, provider_call_id, input_hash, spec_revision, "
            "status, result_ref FROM {t}tool_call WHERE scope_id = :scope_id "
            "AND run_id = :run_id AND attempt_id = :attempt_id ORDER BY id FOR UPDATE",
            schema,
        )
        self._set_call = scoped(
            "UPDATE {t}tool_call SET status = :status, result_ref = :result_ref, "
            "failure = CAST(:failure AS jsonb), finished_at = :finished, updated_at = :now "
            "WHERE scope_id = :scope_id AND id = :id AND status = 'pending'",
            schema,
        )
        self._finish_approved_call = scoped(
            "UPDATE {t}tool_call SET status=:status,result_ref=:result_ref,"
            "failure=CAST(:failure AS jsonb),finished_at=:finished,updated_at=:now "
            "WHERE scope_id=:scope_id AND id=:id AND status='waiting_approval' "
            "AND result_ref=:previous_ref",
            schema,
        )
        self._session_seq = scoped(
            "SELECT event_seq FROM {t}session WHERE scope_id = :scope_id AND id = :session_id",
            schema,
        )

    @property
    def _lease(self) -> LeaseToken | ChildLeaseToken:
        return self._lease_source()

    async def _lock_run(self, session: AsyncSession) -> dict[str, Any]:
        lease = self._lease
        now = self._clock()
        if isinstance(lease, ChildLeaseToken):
            guard = self._child_authority().guard
            return await require_child(session, self._schema, guard, now=now)
        facts.verify_lease(lease, now=now)
        facts.verify_slot(
            await locks.lock_slot(session, self._locks, lease.scope_id, lease.session_id),
            lease,
        )
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
        if run["status"] != "running" or run["desired_control"] is not None:
            errors.control_pending("decision or observation")
        return run

    def _guard(self, run: dict[str, Any]) -> StateWriteGuard:
        lease = self._lease
        if not isinstance(lease, LeaseToken):
            errors.forbidden("a child cannot acquire main state authority")
        return StateWriteGuard(
            actor=self._actor,
            lease=lease,
            expected_state_version=str(run["state_version"]),
            status=RunStatus.RUNNING,
            desired_control=None,
            control_command_id=None,
            cancel_version=str(run["cancel_version"]),
        )

    def _child_authority(self) -> ChildEventAuthority:
        if self._child_guard is None:
            errors.forbidden("child decision requires explicit child authority")
        guard = self._child_guard()
        if guard.actor != self._actor or guard.lease != self._lease:
            errors.forbidden("child decision guard changed owner or lease")
        return ChildEventAuthority(kind="child", guard=guard)

    def _authority(self, run: dict[str, Any]) -> MainEventAuthority | ChildEventAuthority:
        if isinstance(self._lease, ChildLeaseToken):
            return self._child_authority()
        return MainEventAuthority(kind="main", guard=self._guard(run))

    @staticmethod
    def _payload(state: RunState, frame: DecisionFrame) -> dict[str, object]:
        return {
            "kind": "staged_decision",
            "runId": str(state.run_id),
            "stateVersion": state.state_version,
            "attemptId": str(frame.attempt_id),
            "contextSnapshotId": str(frame.context.snapshot_id),
            "action": frame.action.model_dump(mode="json", by_alias=True),
        }

    @staticmethod
    def _stage_id(frame: DecisionFrame, digest: str) -> UUID:
        return uuid5(NAMESPACE_URL, f"decision:{frame.context.run_id}:{frame.attempt_id}:{digest}")

    async def _lock_blob(self, session: AsyncSession, ref: str, digest: str) -> None:
        stored = (
            await session.execute(self._blob_lock, {"scope_id": self._lease.scope_id, "ref": ref})
        ).scalar_one_or_none()
        if stored != digest:
            errors.version_conflict("decision or observation blob changed before commit")

    async def _read_blob(self, session: AsyncSession, ref: str) -> dict[str, Any]:
        scope_id = self._lease.scope_id
        prefix = f"context-{hashlib.sha256(scope_id.encode('utf-8')).hexdigest()}-"
        if not ref.startswith(prefix) or len(ref) != len(prefix) + 64:
            errors.forbidden("checkpoint or observation blob is out of scope")
        row = (
            (await session.execute(self._blob_read, {"scope_id": scope_id, "ref": ref}))
            .mappings()
            .one_or_none()
        )
        if row is None:
            errors.version_conflict("checkpoint or observation blob is missing")
        content = bytes(row["content"])
        digest = hashlib.sha256(content).hexdigest()
        if row["size_bytes"] != len(content) or row["digest"] != digest or ref != prefix + digest:
            errors.version_conflict("checkpoint or observation blob is corrupt")
        try:
            payload = json.loads(content)
            if not isinstance(payload, dict) or canonical_bytes(payload) != content:
                errors.version_conflict("checkpoint or observation blob is not canonical")
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            errors.version_conflict("checkpoint or observation blob is not valid JSON")
            raise AssertionError("unreachable") from exc
        return payload

    async def persist(self, state: RunState, frame: DecisionFrame) -> RunState:
        if (
            frame.actor != self._actor
            or state.scope_id != self._lease.scope_id
            or state.session_id != self._lease.session_id
            or state.run_id != self._lease.run_id
        ):
            errors.forbidden("decision does not belong to this worker claim")
        stored_context, _ = await self._context.load(
            self._lease.scope_id, frame.context.snapshot_id
        )
        if stored_context != frame.context:
            errors.version_conflict("decision uses a different context snapshot")
        await self._access.verify(self._actor, stored_context)
        payload = self._payload(state, frame)
        digest = content_digest(payload)
        ref = await self._payloads.put(state.scope_id, payload, digest)
        stage_id = self._stage_id(frame, digest)
        async with self._sessions() as session, session.begin():
            run = await self._lock_run(session)
            if int(run["state_version"]) != int(state.state_version):
                errors.version_conflict("run version changed before decision commit")
            if run["checkpoint_id"] == stage_id:
                return state.model_copy(update={"checkpoint_ref": ref})
            previous = (
                (
                    await session.execute(
                        self._checkpoint,
                        {"scope_id": state.scope_id, "id": run["checkpoint_id"]},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if previous is None or previous["state_ref"] != state.checkpoint_ref:
                errors.version_conflict("run checkpoint changed before decision commit")
            attempt = (
                (
                    await session.execute(
                        self._attempt,
                        {"scope_id": state.scope_id, "id": frame.attempt_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if (
                attempt is None
                or attempt["run_id"] != state.run_id
                or attempt["context_snapshot_id"] != frame.context.snapshot_id
                or attempt["status"] != "completed"
            ):
                errors.version_conflict("decision has no completed, bound model attempt")
            await self._lock_blob(session, ref, digest)
            action = frame.action
            if isinstance(action, InvokeTools):
                for call in action.payload.validated_calls:
                    await session.execute(
                        self._insert_call,
                        {
                            "id": call.tool_call_id,
                            "scope_id": state.scope_id,
                            "run_id": state.run_id,
                            "attempt_id": frame.attempt_id,
                            "provider_call_id": str(call.tool_call_id),
                            "spec_revision": json.dumps(
                                call.tool.model_dump(mode="json", by_alias=True)
                            ),
                            "input_hash": content_digest(call.arguments),
                        },
                    )
                await self._journal.append_state_in_transaction(
                    session,
                    StateEventBatch(
                        authority=self._authority(run),
                        session_id=state.session_id,
                        trace_id=str(frame.attempt_id),
                        events=tuple(
                            ToolStartedDraft(
                                kind="tool.started",
                                producer_key=f"tool-start:{call.tool_call_id}",
                                occurred_at=self._clock(),
                                turn_id=run["turn_id"],
                                run_id=state.run_id,
                                parent_run_id=run["parent_run_id"],
                                step_id=None,
                                item_id=None,
                                model_attempt_id=frame.attempt_id,
                                payload=ToolStarted(
                                    tool_call_id=call.tool_call_id,
                                    name=f"{call.tool.key.namespace}.{call.tool.key.key}",
                                    public_arguments={},
                                    started_at=self._clock(),
                                ),
                            )
                            for call in action.payload.validated_calls
                        ),
                    ),
                )
            event_seq = (
                await session.execute(
                    self._session_seq,
                    {"scope_id": state.scope_id, "session_id": state.session_id},
                )
            ).scalar_one()
            await session.execute(
                self._insert_checkpoint,
                {
                    "id": stage_id,
                    "scope_id": state.scope_id,
                    "run_id": state.run_id,
                    "source_id": run["checkpoint_id"],
                    "state_version": int(state.state_version),
                    "representation_revision": int(previous["representation_revision"]) + 1,
                    "event_seq": event_seq,
                    "state_ref": ref,
                    "content_hash": digest,
                },
            )
            changed = cast(
                CursorResult[Any],
                await session.execute(
                    self._set_checkpoint,
                    {
                        "new_id": stage_id,
                        "old_id": run["checkpoint_id"],
                        "scope_id": state.scope_id,
                        "run_id": state.run_id,
                        "state_version": int(state.state_version),
                        "now": self._clock(),
                    },
                ),
            )
            if changed.rowcount != 1:
                errors.fenced("decision checkpoint lost its run compare-and-swap")
        return state.model_copy(update={"checkpoint_ref": ref})

    async def prepare_next_checkpoint(
        self, state: RunState, frame: DecisionFrame, *, result: dict[str, object] | None = None
    ) -> TransitionCheckpoint:
        """Create an immutable next-version checkpoint; no Run transition occurs yet.

        An interrupted caller may recreate the same identity and retry the actual
        state transition. The orphan checkpoint is never treated as committed state.
        """
        if frame.actor != self._actor or state.run_id != self._lease.run_id:
            errors.forbidden("action transition belongs to another worker")
        if isinstance(frame.action, InvokeTools):
            errors.invalid_input("tool decisions must be observed before state transition")
        payload: dict[str, object] = {
            "kind": "action_transition",
            "runId": str(state.run_id),
            "stateVersion": str(int(state.state_version) + 1),
            "attemptId": str(frame.attempt_id),
            "contextSnapshotId": str(frame.context.snapshot_id),
            "action": frame.action.model_dump(mode="json", by_alias=True),
        }
        if result is not None:
            payload["result"] = result
        digest = content_digest(payload)
        ref = await self._payloads.put(state.scope_id, payload, digest)
        checkpoint_id = uuid5(
            NAMESPACE_URL, f"transition:{state.run_id}:{frame.attempt_id}:{digest}"
        )
        async with self._sessions() as session, session.begin():
            run = await self._lock_run(session)
            if int(run["state_version"]) != int(state.state_version):
                errors.version_conflict("run advanced before action transition")
            staged = (
                (
                    await session.execute(
                        self._checkpoint,
                        {"scope_id": state.scope_id, "id": run["checkpoint_id"]},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if staged is None or staged["state_ref"] != state.checkpoint_ref:
                errors.version_conflict("action transition lost its staged checkpoint")
            staged_payload = await self._read_blob(session, staged["state_ref"])
            if (
                staged_payload.get("kind") != "staged_decision"
                or staged_payload.get("attemptId") != str(frame.attempt_id)
                or staged_payload.get("contextSnapshotId") != str(frame.context.snapshot_id)
                or staged_payload.get("action") != payload["action"]
            ):
                errors.version_conflict("action transition differs from staged decision")
            await self._lock_blob(session, ref, digest)
            seq = (
                await session.execute(
                    self._session_seq,
                    {"scope_id": state.scope_id, "session_id": state.session_id},
                )
            ).scalar_one()
            await session.execute(
                self._insert_transition_checkpoint,
                {
                    "id": checkpoint_id,
                    "scope_id": state.scope_id,
                    "run_id": state.run_id,
                    "source_id": run["checkpoint_id"],
                    "state_version": int(state.state_version) + 1,
                    "event_seq": seq,
                    "state_ref": ref,
                    "content_hash": digest,
                },
            )
            recorded = (
                (
                    await session.execute(
                        self._checkpoint,
                        {"scope_id": state.scope_id, "id": checkpoint_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if (
                recorded is None
                or recorded["run_id"] != state.run_id
                or int(recorded["state_version"]) != int(state.state_version) + 1
                or recorded["state_ref"] != ref
                or recorded["content_hash"] != digest
            ):
                errors.version_conflict("transition checkpoint identity changed")
        return TransitionCheckpoint(checkpoint_id, ref)

    async def record(self, frame: DecisionFrame, outcome: ToolOutcome) -> None:
        if (
            frame.actor != self._actor
            or frame.context.run_id != self._lease.run_id
            or outcome.run_id != self._lease.run_id
        ):
            errors.forbidden("tool observation does not belong to this worker claim")
        if isinstance(outcome, ToolCompleted):
            failure = None
        elif isinstance(outcome, ToolWaitingApproval):
            failure = None
        elif isinstance(outcome, PendingExecution):
            failure = None
        else:
            failure = outcome.failure.model_dump(mode="json", by_alias=True)
        proposed_call = (
            next(
                (
                    call
                    for call in frame.action.payload.validated_calls
                    if call.tool_call_id == outcome.tool_call_id
                ),
                None,
            )
            if isinstance(frame.action, InvokeTools)
            else None
        )
        if proposed_call is None:
            errors.invalid_input("tool observation has no candidate call")
        payload: dict[str, object] = {
            "call": proposed_call.model_dump(mode="json", by_alias=True),
            "outcome": outcome.model_dump(mode="json", by_alias=True),
        }
        digest = content_digest(payload)
        ref = await self._payloads.put(self._lease.scope_id, payload, digest)
        async with self._sessions() as session, session.begin():
            run = await self._lock_run(session)
            checkpoint = (
                (
                    await session.execute(
                        self._checkpoint,
                        {"scope_id": self._lease.scope_id, "id": run["checkpoint_id"]},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if checkpoint is None:
                errors.version_conflict("decision checkpoint is missing")
            staged = await self._read_blob(session, checkpoint["state_ref"])
            if (
                staged.get("kind") != "staged_decision"
                or staged.get("attemptId") != str(frame.attempt_id)
                or staged.get("contextSnapshotId") != str(frame.context.snapshot_id)
                or checkpoint["content_hash"] != content_digest(staged)
            ):
                errors.version_conflict("tool result does not belong to the current decision")
            action = decision_adapter.validate_python(staged["action"])
            call = (
                next(
                    (
                        item
                        for item in action.payload.validated_calls
                        if item.tool_call_id == outcome.tool_call_id
                    ),
                    None,
                )
                if isinstance(action, InvokeTools)
                else None
            )
            if call is None:
                errors.invalid_input("tool result has no staged call")
            if call != proposed_call:
                errors.version_conflict("tool observation candidate differs from staged call")
            rows = (
                (
                    await session.execute(
                        self._calls,
                        {
                            "scope_id": self._lease.scope_id,
                            "run_id": self._lease.run_id,
                            "attempt_id": frame.attempt_id,
                        },
                    )
                )
                .mappings()
                .all()
            )
            row = next((item for item in rows if item["id"] == outcome.tool_call_id), None)
            if row is None:
                errors.version_conflict("tool call row is missing")
            if (
                row["provider_call_id"] != str(call.tool_call_id)
                or row["input_hash"] != content_digest(call.arguments)
                or row["spec_revision"] != call.tool.model_dump(mode="json", by_alias=True)
            ):
                errors.version_conflict("tool call row conflicts with the staged decision")
            status = "running" if isinstance(outcome, PendingExecution) else outcome.status
            if isinstance(outcome, PendingExecution):
                await self._verify_execution(session, outcome)
            if isinstance(outcome, ToolWaitingApproval):
                approval = (
                    await session.execute(
                        scoped(
                            "SELECT * FROM {t}approval WHERE scope_id=:scope AND id=:id "
                            "FOR KEY SHARE",
                            self._schema,
                        ),
                        {"scope": self._actor.scope_id, "id": outcome.approval.approval_id},
                    )
                ).mappings().one_or_none()
                binding = outcome.approval.binding
                if (
                    approval is None
                    or approval["status"] != "pending"
                    or approval["expires_at"] <= self._clock()
                    or approval["run_id"] != self._lease.run_id
                    or approval["actor_id"] != self._actor.actor_id
                    or approval["auth_revision"] != int(self._actor.auth_revision)
                    or approval["request_key"] != outcome.approval.idempotency_key
                    or approval["request_key"] != f"tool:{outcome.operation_id}"
                    or approval["input_hash"] != binding.arguments_digest
                    or approval["policy_ref"]
                    != binding.policy.model_dump(mode="json", by_alias=True)
                    or approval["tool_revision"]
                    != binding.tool.model_dump(mode="json", by_alias=True)
                    or approval["resource_refs"]
                    != [ref.model_dump(mode="json", by_alias=True) for ref in binding.resources]
                ):
                    errors.version_conflict("waiting approval has no matching durable request")
            resume_approval = (
                row["status"] == "waiting_approval"
                and not isinstance(outcome, ToolWaitingApproval)
                and staged.get("approvalResumed") is True
            )
            if resume_approval:
                if row["result_ref"] is None:
                    errors.version_conflict("waiting approval observation is missing")
                previous = await self._payloads.get(self._lease.scope_id, row["result_ref"])
                if not isinstance(previous, dict):
                    errors.version_conflict("waiting approval observation is invalid")
                waiting = tool_outcome_adapter.validate_python(previous.get("outcome"))
                if (
                    not isinstance(waiting, ToolWaitingApproval)
                    or waiting.tool_call_id != outcome.tool_call_id
                    or waiting.operation_id != outcome.operation_id
                ):
                    errors.version_conflict("approval resume lost its original operation")
                approval_status = (
                    await session.execute(
                        scoped(
                            "SELECT status FROM {t}approval WHERE scope_id=:scope AND id=:id",
                            self._schema,
                        ),
                        {"scope": self._actor.scope_id, "id": waiting.approval.approval_id},
                    )
                ).scalar_one_or_none()
                if approval_status not in {"approved", "rejected", "cancelled", "expired"}:
                    errors.version_conflict("approval has no terminal decision")
                if (
                    isinstance(outcome, (PendingExecution, ToolCompleted))
                    and approval_status != "approved"
                ):
                    errors.forbidden("unapproved tool cannot commit or dispatch an effect")
            if row["status"] != "pending" and not resume_approval:
                if row["result_ref"] == ref and row["status"] == status:
                    return
                errors.version_conflict("tool call already has another outcome")
            await self._lock_blob(session, ref, digest)
            changed = cast(
                CursorResult[Any],
                await session.execute(
                    self._finish_approved_call if resume_approval else self._set_call,
                    {
                        "scope_id": self._lease.scope_id,
                        "id": outcome.tool_call_id,
                        "status": status,
                        "result_ref": ref,
                        "failure": json.dumps(failure) if failure is not None else None,
                        "now": self._clock(),
                        "finished": None
                        if isinstance(outcome, PendingExecution)
                        else self._clock(),
                        "previous_ref": row["result_ref"],
                    },
                ),
            )
            if changed.rowcount != 1:
                errors.fenced("tool observation lost its status compare-and-swap")
            if not isinstance(outcome, (ToolWaitingApproval, PendingExecution)):
                await self._journal.append_state_in_transaction(
                    session,
                    StateEventBatch(
                        authority=self._authority(run),
                        session_id=self._lease.session_id,
                        trace_id=str(frame.attempt_id),
                        events=(
                            ToolFinishedDraft(
                                kind="tool.finished",
                                producer_key=f"tool-finish:{outcome.tool_call_id}",
                                occurred_at=self._clock(),
                                turn_id=run["turn_id"],
                                run_id=self._lease.run_id,
                                parent_run_id=run["parent_run_id"],
                                step_id=None,
                                item_id=None,
                                model_attempt_id=frame.attempt_id,
                                payload=ToolFinished(
                                    tool_call_id=outcome.tool_call_id,
                                    status=outcome.status,
                                    result_refs=outcome.effect_refs,
                                    failure=(
                                        None
                                        if isinstance(outcome, ToolCompleted)
                                        else outcome.failure
                                    ),
                                    finished_at=self._clock(),
                                ),
                            ),
                        ),
                    ),
                )

    async def recover(self) -> RecoveredDecision | None:
        """Read staged facts; unresolved calls are reconciliation work, not replay work."""
        async with self._sessions() as session, session.begin():
            run = await self._lock_run(session)
            checkpoint = (
                (
                    await session.execute(
                        self._checkpoint,
                        {"scope_id": self._lease.scope_id, "id": run["checkpoint_id"]},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if checkpoint is None:
                return None
            ref = checkpoint["state_ref"]
            if not str(ref).startswith("context-"):
                return None
            payload = await self._read_blob(session, ref)
            if content_digest(payload) != checkpoint["content_hash"]:
                errors.version_conflict("staged decision checkpoint is corrupt")
            controlled = payload.get("kind") == "control_checkpoint"
            payload = await self._unwrap_control(session, payload)
            if payload.get("kind") != "staged_decision":
                return None
            if (
                payload.get("runId") != str(self._lease.run_id)
                or not 0 <= int(str(payload.get("stateVersion", "-1"))) <= int(run["state_version"])
                or (
                    not controlled
                    and int(str(payload.get("stateVersion", "-1"))) != int(run["state_version"])
                )
            ):
                errors.version_conflict("staged decision checkpoint is corrupt")
            attempt_id = UUID(str(payload["attemptId"]))
            rows = (
                (
                    await session.execute(
                        self._calls,
                        {
                            "scope_id": self._lease.scope_id,
                            "run_id": self._lease.run_id,
                            "attempt_id": attempt_id,
                        },
                    )
                )
                .mappings()
                .all()
            )
        context, _ = await self._context.load(
            self._lease.scope_id, UUID(str(payload["contextSnapshotId"]))
        )
        await self._access.verify(self._actor, context)
        action = decision_adapter.validate_python(payload["action"])
        expected_calls = (
            {call.tool_call_id: call for call in action.payload.validated_calls}
            if isinstance(action, InvokeTools)
            else {}
        )
        if {row["id"] for row in rows} != set(expected_calls):
            errors.version_conflict("staged tool call set is incomplete")
        outcomes: list[ToolOutcome] = []
        pending: list[UUID] = []
        refs: list[ResourceRef] = []
        for row in rows:
            call = expected_calls[row["id"]]
            if (
                row["provider_call_id"] != str(call.tool_call_id)
                or row["input_hash"] != content_digest(call.arguments)
                or row["spec_revision"] != call.tool.model_dump(mode="json", by_alias=True)
            ):
                errors.version_conflict("staged tool call identity changed")
            if row["status"] == "pending":
                pending.append(row["id"])
            elif row["result_ref"] is not None:
                stored = await self._payloads.get(self._lease.scope_id, row["result_ref"])
                if not isinstance(stored, dict) or stored.get("call") != call.model_dump(
                    mode="json", by_alias=True
                ):
                    errors.version_conflict("stored observation call identity changed")
                outcome = tool_outcome_adapter.validate_python(stored.get("outcome"))
                if (
                    outcome.tool_call_id != row["id"]
                    or outcome.run_id != self._lease.run_id
                    or ("running" if isinstance(outcome, PendingExecution) else outcome.status)
                    != row["status"]
                ):
                    errors.version_conflict("stored observation identity changed")
                outcomes.append(outcome)
                if row["status"] in {"completed", "failed", "cancelled", "not_executed"}:
                    refs.append(
                        ResourceRef(
                            resource_type="tool_call_result",
                            resource_id=str(row["id"]),
                            resource_version="1",
                            scope=ResourceScope(
                                scope_id=self._lease.scope_id,
                                session_id=str(self._lease.session_id),
                                owner_id=self._actor.actor_id,
                            ),
                        )
                    )
            else:
                errors.version_conflict("tool call has a status without an observation")
        return RecoveredDecision(
            frame=DecisionFrame(
                actor=self._actor, context=context, attempt_id=attempt_id, action=action
            ),
            checkpoint_ref=str(ref),
            outcomes=tuple(outcomes),
            unresolved_call_ids=tuple(pending),
            observation_refs=tuple(refs),
        )

    async def _unwrap_control(self, db: AsyncSession, payload: dict[str, Any]) -> dict[str, Any]:
        seen: set[str] = set()
        for _ in range(64):
            if payload.get("kind") != "control_checkpoint":
                return payload
            source = str(payload.get("sourceStateRef", ""))
            if payload.get("runId") != str(self._lease.run_id) or source in seen:
                errors.version_conflict("control checkpoint lineage is corrupt")
            seen.add(source)
            payload = await self._read_blob(db, source)
        errors.version_conflict("control checkpoint lineage exceeds recovery limit")
        raise AssertionError("unreachable")

    async def _verify_execution(self, db: AsyncSession, outcome: PendingExecution) -> None:
        ref = outcome.execution_ref
        if ref.resource_type != "execution" or ref.scope.scope_id != self._actor.scope_id:
            errors.forbidden("pending execution belongs to another scope")
        try:
            identity = UUID(ref.resource_id)
        except ValueError:
            errors.invalid_input("execution identity must be a UUID")
        row = (
            await db.execute(
                scoped(
                    "SELECT e.id FROM {t}execution e JOIN {t}tool_call c "
                    "ON c.scope_id=e.scope_id AND c.operation_id=e.operation_id "
                    "JOIN {t}work_item w ON w.scope_id=e.scope_id AND w.id=e.supervisor_work_id "
                    "WHERE e.scope_id=:scope AND e.id=:id AND e.run_id=:run AND c.run_id=e.run_id "
                    "AND c.id=:call AND e.operation_id=:op "
                    "AND CAST(e.generation AS text)=:generation "
                    "AND w.run_id=e.run_id AND w.kind='reconcile_execution' "
                    "AND w.status IN ('pending','leased') "
                    "AND (e.cleanup_status<>'completed' "
                    "OR c.status IN ('completed','failed','cancelled'))",
                    self._schema,
                ),
                {
                    "scope": self._actor.scope_id,
                    "id": identity,
                    "run": outcome.run_id,
                    "call": outcome.tool_call_id,
                    "op": outcome.operation_id,
                    "generation": ref.resource_version,
                },
            )
        ).first()
        if row is None:
            errors.version_conflict("pending execution has no bound durable supervisor")

    async def park_execution(
        self, state: RunState, decision: RecoveredDecision, predicate: CapabilityRef
    ) -> RunState:
        """Persist a staged-decision wait for either a main or child claim."""
        pending = [item for item in decision.outcomes if isinstance(item, PendingExecution)]
        approvals = [item for item in decision.outcomes if isinstance(item, ToolWaitingApproval)]
        if len(pending) + len(approvals) != 1 or decision.unresolved_call_ids:
            errors.invalid_input("tool wait requires exactly one fully observed pending call")
        waiting_approval = approvals[0] if approvals else None
        wait_status = RunStatus.WAITING_USER if waiting_approval else RunStatus.WAITING_TOOL
        version = int(state.state_version) + 1
        payload = self._payload(state, decision.frame)
        payload["stateVersion"] = str(version)
        payload["approvalWait" if waiting_approval else "executionWait"] = True
        digest = content_digest(payload)
        ref = await self._payloads.put(state.scope_id, payload, digest)
        cp = uuid5(NAMESPACE_URL, f"execution-wait:{state.run_id}:{version}:{digest}")
        now = self._clock()
        async with self._sessions() as db, db.begin():
            run = await self._lock_run(db)
            if int(run["state_version"]) != int(state.state_version):
                errors.version_conflict("execution wait lost run version")
            previous = (
                (
                    await db.execute(
                        self._checkpoint, {"scope_id": state.scope_id, "id": run["checkpoint_id"]}
                    )
                )
                .mappings()
                .one()
            )
            staged = await self._read_blob(db, previous["state_ref"])
            staged = await self._unwrap_control(db, staged)
            if previous["state_ref"] != decision.checkpoint_ref or staged != self._payload(
                state, decision.frame
            ):
                expected = self._payload(state, decision.frame)
                if (
                    previous["state_ref"] != decision.checkpoint_ref
                    or any(
                        staged.get(key) != expected[key]
                        for key in ("kind", "runId", "attemptId", "contextSnapshotId", "action")
                    )
                    or int(str(staged.get("stateVersion", "-1"))) > int(state.state_version)
                ):
                    errors.version_conflict("execution wait lost staged decision")
            rows = (
                (
                    await db.execute(
                        self._calls,
                        {
                            "scope_id": state.scope_id,
                            "run_id": state.run_id,
                            "attempt_id": decision.frame.attempt_id,
                        },
                    )
                )
                .mappings()
                .all()
            )
            if len(rows) != len(decision.outcomes) or any(
                row["status"] == "pending" for row in rows
            ):
                errors.version_conflict("execution wait has unobserved tool calls")
            for item in pending:
                await self._verify_execution(db, item)
            if waiting_approval is not None:
                approval_status = (
                    await db.execute(
                        scoped(
                            "SELECT status,revision,expires_at FROM {t}approval "
                            "WHERE scope_id=:scope AND run_id=:run AND id=:id FOR KEY SHARE",
                            self._schema,
                        ),
                        {
                            "scope": state.scope_id,
                            "run": state.run_id,
                            "id": waiting_approval.approval.approval_id,
                        },
                    )
                ).mappings().one_or_none()
                if (
                    approval_status is None
                    or approval_status["status"] != "pending"
                    or approval_status["expires_at"] <= now
                ):
                    errors.version_conflict("approval wait has no pending request")
            await self._lock_blob(db, ref, digest)
            spec = WaitSpec(
                kind="approval" if waiting_approval else "tool",
                subject_id=str(
                    waiting_approval.approval.approval_id
                    if waiting_approval is not None
                    else pending[0].tool_call_id
                ),
                deadline_at=(
                    min(run["deadline_at"], approval_status["expires_at"])
                    if waiting_approval is not None and approval_status is not None
                    else run["deadline_at"]
                ),
                resume_predicate=predicate,
            )
            await self._journal.append_state_in_transaction(
                db,
                StateEventBatch(
                    authority=self._authority(run),
                    session_id=state.session_id,
                    trace_id=str(decision.frame.attempt_id),
                    events=(
                        RunStatusDraft(
                            kind="run.status",
                            producer_key=f"execution-wait:{state.run_id}:{version}",
                            occurred_at=now,
                            turn_id=run["turn_id"],
                            run_id=state.run_id,
                            parent_run_id=run["parent_run_id"],
                            step_id=None,
                            item_id=None,
                            model_attempt_id=None,
                            payload=RunStatusPayload(
                                state_version=str(version),
                                status=wait_status,
                                phase="waiting",
                                phase_started_at=now,
                                public_reason=(
                                    "Waiting for user approval."
                                    if waiting_approval
                                    else "Waiting for background tool results."
                                ),
                            ),
                        ),
                    )
                    + (
                        (
                            ApprovalRequiredDraft(
                                kind="approval.required",
                                producer_key=f"approval-required:{waiting_approval.approval.approval_id}",
                                occurred_at=now,
                                turn_id=run["turn_id"],
                                run_id=state.run_id,
                                parent_run_id=run["parent_run_id"],
                                step_id=None,
                                item_id=None,
                                model_attempt_id=decision.frame.attempt_id,
                                payload=ApprovalRequired(
                                    approval_ref=ResourceRef(
                                        resource_type="approval",
                                        resource_id=str(waiting_approval.approval.approval_id),
                                        resource_version=str(approval_status["revision"]),
                                        scope=ResourceScope(
                                            scope_id=state.scope_id,
                                            session_id=str(state.session_id),
                                            owner_id=self._actor.actor_id,
                                        ),
                                    ),
                                    summary=waiting_approval.approval.public_summary,
                                    expires_at=approval_status["expires_at"],
                                    allowed_decisions=("approved", "rejected", "cancelled"),
                                ),
                            ),
                        )
                        if waiting_approval is not None and approval_status is not None
                        else ()
                    ),
                ),
            )
            seq = (
                await db.execute(
                    self._session_seq, {"scope_id": state.scope_id, "session_id": state.session_id}
                )
            ).scalar_one()
            await db.execute(
                self._insert_transition_checkpoint,
                {
                    "id": cp,
                    "scope_id": state.scope_id,
                    "run_id": state.run_id,
                    "source_id": run["checkpoint_id"],
                    "state_version": version,
                    "event_seq": seq,
                    "state_ref": ref,
                    "content_hash": digest,
                },
            )
            await db.execute(
                scoped(
                    "UPDATE {t}run SET status=:status,phase='waiting',phase_started_at=:now,"
                    "state_version=:version,checkpoint_id=:cp,waiting_spec=CAST(:wait AS jsonb),"
                    "owner_id=NULL,lease_until=NULL,owner_epoch=owner_epoch+1,updated_at=:now "
                    "WHERE scope_id=:scope AND id=:run",
                    self._schema,
                ),
                {
                    "scope": state.scope_id,
                    "run": state.run_id,
                    "now": now,
                    "status": wait_status.value,
                    "version": version,
                    "cp": cp,
                    "wait": spec.model_dump_json(by_alias=True),
                },
            )
            await db.execute(
                scoped(
                    "UPDATE {t}work_item SET status='completed',owner_id=NULL,lease_until=NULL,"
                    "lease_epoch=lease_epoch+1,updated_at=:now WHERE scope_id=:scope AND id=:id",
                    self._schema,
                ),
                {"scope": state.scope_id, "id": self._lease.work_id, "now": now},
            )
            if isinstance(self._lease, LeaseToken):
                await db.execute(
                    scoped(
                        "UPDATE {t}session_slot SET owner_run_id=NULL,slot_version=slot_version+1,"
                        "updated_at=:now WHERE scope_id=:scope AND session_id=:session "
                        "AND owner_run_id=:run",
                        self._schema,
                    ),
                    {
                        "scope": state.scope_id,
                        "session": state.session_id,
                        "run": state.run_id,
                        "now": now,
                    },
                )
        return state.model_copy(
            update={
                "state_version": str(version),
                "checkpoint_ref": ref,
                "status": wait_status,
            }
        )


class SqlExecutionWaiter:
    def __init__(self, decisions: SqlDecisionStore, predicate: CapabilityRef) -> None:
        self._decisions, self._predicate = decisions, predicate

    async def park(self, state: RunState, decision: RecoveredDecision) -> RunState:
        return await self._decisions.park_execution(state, decision, self._predicate)


__all__ = ["RecoveredDecision", "SqlDecisionStore", "SqlExecutionWaiter"]

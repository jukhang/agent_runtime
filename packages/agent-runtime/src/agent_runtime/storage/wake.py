"""Evidence-backed wake of a persisted waiting Run."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.payloads import RunStatusPayload
from agent_runtime.contracts.references import CapabilityRef, ResourceRef, ResourceScope
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.state_events import (
    RunStatusDraft,
    ServiceEventAuthority,
    StateEventBatch,
)
from agent_runtime.contracts.tasks import WaitSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.work_claims import MaintenanceClaimedWork
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.uow import errors, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped

type WakeKind = Literal["tool", "approval", "message", "timer", "external"]


class ExternalWaitEvidence(Protocol):
    async def __call__(self, session: AsyncSession, request: WakeRequest, spec: WaitSpec) -> bool:
        """Check durable evidence and current resource ACL in the wake transaction."""
        ...


@dataclass(frozen=True, slots=True)
class WakeRequest:
    actor: ActorContext
    session_id: UUID
    run_id: UUID
    expected_state_version: int
    kind: WakeKind
    subject_id: str
    maintenance: MaintenanceClaimedWork | None = None


@dataclass(frozen=True, slots=True)
class WakeReceipt:
    run_id: UUID
    work_id: UUID
    checkpoint_id: UUID
    state_version: int


class SqlWaitWaker:
    """Only a stored, matching DB fact can queue a waiting Run."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        journal: SqlJournal,
        payloads: BlobContextPayloadStore,
        approval_resumer_enabled: bool = False,
        external_predicates: dict[CapabilityRef, ExternalWaitEvidence] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._schema = require_schema(schema)
        self._actor = actor
        self._journal = journal
        self._payloads = payloads
        self._approval_resumer_enabled = approval_resumer_enabled
        self._external_predicates = dict(external_predicates or {})
        self._clock = clock
        self._locks = locks.build_lock_statements(schema)
        self._checkpoint = scoped(
            "SELECT id, state_ref, content_hash FROM {t}checkpoint "
            "WHERE scope_id=:scope AND run_id=:run AND id=:id",
            schema,
        )
        self._blob_lock = scoped(
            "SELECT digest FROM {t}context_blob WHERE scope_id=:scope "
            "AND blob_key=:ref FOR KEY SHARE",
            schema,
        )
        self._insert_checkpoint = scoped(
            "INSERT INTO {t}checkpoint (id, scope_id, run_id, source_checkpoint_id, "
            "state_version, representation_revision, event_seq, schema_revision, "
            "runtime_build, state_ref, content_hash) VALUES "
            "(:id, :scope, :run, :source, :version, 0, :event_seq, 1, "
            "'agent-runtime', :state_ref, :content_hash)",
            schema,
        )
        self._evidence_tool = scoped(
            "SELECT status FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run AND id=:id",
            schema,
        )
        self._evidence_approval = scoped(
            "SELECT status FROM {t}approval WHERE scope_id=:scope AND run_id=:run AND id=:id",
            schema,
        )
        self._set_run = scoped(
            "UPDATE {t}run SET status='queued', phase='queued', state_version=:version, "
            "checkpoint_id=:checkpoint, waiting_spec=NULL, wake_generation=wake_generation+1, "
            "phase_started_at=:now, updated_at=:now "
            "WHERE scope_id=:scope AND id=:run AND status IN ('waiting_tool','waiting_user') "
            "AND state_version=:expected",
            schema,
        )
        self._insert_work = scoped(
            "INSERT INTO {t}work_item (id, scope_id, run_id, subject_ref, kind, dedup_key, "
            "status, payload_ref, wake_reason, priority, lease_epoch, attempt_count, "
            "available_at, deadline_at) VALUES (:id, :scope, :run, CAST(:subject AS jsonb), "
            "'advance_run', :dedup, 'pending', :payload, :reason, 1000, 0, 0, :now, :deadline)",
            schema,
        )
        self._seq = scoped(
            "SELECT event_seq FROM {t}session WHERE scope_id=:scope AND id=:session",
            schema,
        )

    async def _verify_evidence(
        self, session: AsyncSession, request: WakeRequest, spec: WaitSpec, now: datetime
    ) -> None:
        if spec.kind != request.kind or spec.subject_id != request.subject_id:
            errors.version_conflict("wake signal does not match the persisted wait")
        if request.kind == "external":
            verify = self._external_predicates.get(spec.resume_predicate)
            if verify is None:
                errors.forbidden("external wait predicate is not installed")
            if now >= spec.deadline_at:
                errors.version_conflict("external wait deadline has expired")
            if await verify(session, request, spec) is not True:
                errors.version_conflict("external wait has no authorized durable result")
            return
        if request.kind == "timer":
            if now < spec.deadline_at:
                errors.version_conflict("wait timer has not reached its deadline")
            return
        if request.kind == "message":
            if request.subject_id != str(request.run_id):
                errors.invalid_input("message wait subject must be its recipient run")
            return
        try:
            subject = UUID(request.subject_id)
        except ValueError as exc:
            errors.invalid_input("wake subject is not a UUID")
            raise AssertionError("unreachable") from exc
        query = self._evidence_tool if request.kind == "tool" else self._evidence_approval
        status = (
            await session.execute(
                query,
                {"scope": request.actor.scope_id, "run": request.run_id, "id": subject},
            )
        ).scalar_one_or_none()
        terminal = (
            {"completed", "failed", "cancelled", "not_executed"}
            if request.kind == "tool"
            else {"approved", "rejected", "expired", "cancelled"}
        )
        if status not in terminal:
            errors.version_conflict("wake source has no terminal result in this run")
        if request.kind == "tool":
            unresolved = (
                await session.execute(
                    scoped(
                        "SELECT e.id FROM {t}tool_call tc JOIN {t}execution e "
                        "ON e.scope_id=tc.scope_id AND e.operation_id=tc.operation_id "
                        "WHERE tc.scope_id=:scope AND tc.run_id=:run AND tc.id=:id "
                        "AND e.status NOT IN ('completed','failed','cancelled') LIMIT 1",
                        self._schema,
                    ),
                    {"scope": request.actor.scope_id, "run": request.run_id, "id": subject},
                )
            ).scalar_one_or_none()
            if unresolved is not None:
                errors.version_conflict("tool's external execution is still unresolved")

    async def wake(self, request: WakeRequest) -> WakeReceipt:
        if request.actor != self._actor:
            errors.forbidden("wake actor is not the authenticated actor")
        if request.kind == "approval" and not self._approval_resumer_enabled:
            errors.control_pending("approval resumer is not installed in this host")
        payload: dict[str, object] = {
            "kind": "wait_woken",
            "runId": str(request.run_id),
            "stateVersion": str(request.expected_state_version + 1),
            "wakeKind": request.kind,
            "subjectId": request.subject_id,
        }
        # Preserve an execution decision across wait/wake. A takeover must still
        # observe its complete call set before asking the model for another action.
        staged_ref: str | None = None
        if request.kind in {"tool", "approval"}:
            async with self._sessions() as db:
                staged_ref = (
                    await db.execute(
                        scoped(
                            "SELECT c.state_ref FROM {t}run r JOIN {t}checkpoint c "
                            "ON c.scope_id=r.scope_id AND c.id=r.checkpoint_id "
                            "WHERE r.scope_id=:scope AND r.id=:run",
                            self._schema,
                        ),
                        {"scope": request.actor.scope_id, "run": request.run_id},
                    )
                ).scalar_one_or_none()
            if staged_ref is not None and staged_ref.startswith("context-"):
                staged = await self._payloads.get(request.actor.scope_id, staged_ref)
                if staged.get("kind") == "staged_decision":
                    resumed_key = (
                        "approvalResumed" if request.kind == "approval" else "executionResumed"
                    )
                    payload = {
                        **staged,
                        "stateVersion": str(request.expected_state_version + 1),
                        resumed_key: True,
                    }
                else:
                    staged_ref = None
            else:
                staged_ref = None
        digest = content_digest(payload)
        state_ref = await self._payloads.put(request.actor.scope_id, payload, digest)
        now = self._clock()
        async with self._sessions() as session, session.begin():
            slot = await locks.lock_slot(
                session, self._locks, request.actor.scope_id, request.session_id
            )
            if slot is None:
                errors.fenced("session slot does not exist")
            parent_id = (
                await session.execute(
                    scoped(
                        "SELECT parent_run_id FROM {t}run WHERE scope_id=:scope AND id=:run",
                        self._schema,
                    ),
                    {"scope": request.actor.scope_id, "run": request.run_id},
                )
            ).scalar_one_or_none()
            identities = {request.run_id} if parent_id is None else {request.run_id, parent_id}
            locked = {
                identity: await locks.lock_run(
                    session, self._locks, request.actor.scope_id, identity
                )
                for identity in sorted(identities, key=str)
            }
            run = locked[request.run_id]
            if (
                run is None
                or run["session_id"] != request.session_id
                or run["status"] not in {"waiting_tool", "waiting_user"}
                or int(run["state_version"]) != request.expected_state_version
                or run["owner_id"] is not None
                or run["desired_control"] is not None
                or run["deadline_at"] <= now
            ):
                errors.version_conflict("waiting run is no longer eligible to wake")
            await revalidate_authority(session, self._locks, request.actor, run=run, now=now)
            if run["parent_run_id"] is not None:
                root = locked.get(run["root_run_id"])
                if (
                    root is None
                    or root["desired_control"] is not None
                    or root["status"]
                    in {"paused", "cancelling", "cancelled", "completed", "failed"}
                ):
                    errors.control_pending("child wake blocked by root control")
                await revalidate_authority(session, self._locks, request.actor, run=root, now=now)
            if request.maintenance is not None:
                claim = request.maintenance
                work = await locks.lock_work_item(
                    session, self._locks, request.actor.scope_id, claim.lease.work_id
                )
                if (
                    claim.lease.scope_id != request.actor.scope_id
                    or claim.kind != "reconcile_execution"
                    or work is None
                    or work["run_id"] != request.run_id
                    or work["status"] != "leased"
                    or work["owner_id"] != claim.lease.owner_id
                    or work["lease_epoch"] != int(claim.lease.work_epoch)
                    or work["lease_until"] <= now
                    or ResourceRef.model_validate(work["subject_ref"]) != claim.subject_ref
                ):
                    errors.fenced("execution wake maintenance lease changed")
            spec = WaitSpec.model_validate(run["waiting_spec"])
            await self._verify_evidence(session, request, spec, now)
            if request.kind == "message" and int(run["pending_input_version"]) <= int(
                run["applied_input_version"]
            ):
                errors.version_conflict("no new input is pending for the waiting run")
            previous = (
                (
                    await session.execute(
                        self._checkpoint,
                        {
                            "scope": request.actor.scope_id,
                            "run": request.run_id,
                            "id": run["checkpoint_id"],
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            if previous is None:
                errors.version_conflict("waiting checkpoint is missing")
            if staged_ref is not None:
                if previous["state_ref"] != staged_ref:
                    errors.version_conflict("execution wait checkpoint changed")
                if request.kind == "tool":
                    incomplete = (
                        await session.execute(
                            scoped(
                                "SELECT id FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run "
                                "AND attempt_id=:attempt AND status NOT IN "
                                "('completed','failed','cancelled','not_executed') LIMIT 1",
                                self._schema,
                            ),
                            {
                                "scope": request.actor.scope_id,
                                "run": request.run_id,
                                "attempt": UUID(str(payload["attemptId"])),
                            },
                        )
                    ).first()
                    if incomplete is not None:
                        errors.version_conflict(
                            "other calls in the execution decision are still pending"
                        )
                elif not (
                    await session.execute(
                        scoped(
                            "SELECT 1 FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run "
                            "AND attempt_id=:attempt AND status='waiting_approval' "
                            "AND id IN (SELECT c.id FROM {t}tool_call c "
                            "JOIN {t}context_blob b ON b.scope_id=c.scope_id "
                            "AND b.blob_key=c.result_ref "
                            "JOIN {t}approval a ON a.scope_id=c.scope_id "
                            "AND a.request_key='tool:' || "
                            "(convert_from(b.content,'UTF8')::jsonb #>> '{{outcome,operationId}}') "
                            "WHERE a.scope_id=:scope AND a.id=:approval "
                            "AND (convert_from(b.content,'UTF8')::jsonb "
                            "#>> '{{outcome,approval,approvalId}}')=CAST(a.id AS text)) LIMIT 1",
                            self._schema,
                        ),
                        {
                            "scope": request.actor.scope_id,
                            "run": request.run_id,
                            "attempt": UUID(str(payload["attemptId"])),
                            "approval": UUID(request.subject_id),
                        },
                    )
                ).first():
                    errors.version_conflict("approval wake lost its staged tool call")
            stored_digest = (
                await session.execute(
                    self._blob_lock,
                    {"scope": request.actor.scope_id, "ref": state_ref},
                )
            ).scalar_one_or_none()
            if stored_digest != digest:
                errors.version_conflict("wake checkpoint blob changed before commit")
            next_version = request.expected_state_version + 1
            checkpoint_id = uuid5(NAMESPACE_URL, f"wake-checkpoint:{request.run_id}:{next_version}")
            work_id = uuid5(NAMESPACE_URL, f"wake-work:{request.run_id}:{next_version}")
            event_seq = (
                await session.execute(
                    self._seq,
                    {"scope": request.actor.scope_id, "session": request.session_id},
                )
            ).scalar_one()
            await session.execute(
                self._insert_checkpoint,
                {
                    "id": checkpoint_id,
                    "scope": request.actor.scope_id,
                    "run": request.run_id,
                    "source": previous["id"],
                    "version": next_version,
                    "event_seq": event_seq,
                    "state_ref": state_ref,
                    "content_hash": digest,
                },
            )
            await self._journal.append_state_in_transaction(
                session,
                StateEventBatch(
                    authority=ServiceEventAuthority(
                        kind="service",
                        actor=request.actor,
                        purpose="wake",
                        source=ResourceRef(
                            resource_type="run",
                            resource_id=str(request.run_id),
                            resource_version=str(request.expected_state_version),
                            scope=ResourceScope(
                                scope_id=request.actor.scope_id,
                                session_id=str(request.session_id),
                                owner_id=request.actor.actor_id,
                            ),
                        ),
                        transaction_id=work_id,
                    ),
                    session_id=request.session_id,
                    trace_id=str(work_id),
                    events=(
                        RunStatusDraft(
                            kind="run.status",
                            producer_key=f"run-status:{request.run_id}:{next_version}",
                            occurred_at=now,
                            turn_id=run["turn_id"],
                            run_id=request.run_id,
                            parent_run_id=run["parent_run_id"],
                            step_id=None,
                            item_id=None,
                            model_attempt_id=None,
                            payload=RunStatusPayload(
                                state_version=str(next_version),
                                status=RunStatus.QUEUED,
                                phase="queued",
                                phase_started_at=now,
                                public_reason="The requested input or result is ready.",
                            ),
                        ),
                    ),
                ),
            )
            changed = cast(
                CursorResult[Any],
                await session.execute(
                    self._set_run,
                    {
                        "version": next_version,
                        "checkpoint": checkpoint_id,
                        "now": now,
                        "scope": request.actor.scope_id,
                        "run": request.run_id,
                        "expected": request.expected_state_version,
                    },
                ),
            )
            if changed.rowcount != 1:
                errors.fenced("wait wake lost its Run compare-and-swap")
            await session.execute(
                self._insert_work,
                {
                    "id": work_id,
                    "scope": request.actor.scope_id,
                    "run": request.run_id,
                    "subject": json.dumps({"kind": request.kind, "subjectId": request.subject_id}),
                    "dedup": f"wake:{request.run_id}:{next_version}",
                    "payload": f"checkpoint:{checkpoint_id}",
                    "reason": (
                        "message"
                        if request.kind == "message"
                        else "tool_result"
                        if request.kind in {"tool", "approval"}
                        else "recovery"
                    ),
                    "now": now,
                    "deadline": run["deadline_at"],
                },
            )
        return WakeReceipt(request.run_id, work_id, checkpoint_id, next_version)


__all__ = ["ExternalWaitEvidence", "SqlWaitWaker", "WakeReceipt", "WakeRequest"]

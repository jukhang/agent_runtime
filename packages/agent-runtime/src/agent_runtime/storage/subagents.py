"""Durable, one-level delegation with isolated grants and acceptance evidence."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.agent import Budget as AgentBudget
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.message_payloads import (
    ResultProposed,
    ResultVerification,
    TaskAssign,
    TaskFailed,
)
from agent_runtime.contracts.messages import (
    ResultProposedMessage,
    TaskAssignMessage,
    message_adapter,
)
from agent_runtime.contracts.ownership import ChildCommitGuard, ChildProposalRequest
from agent_runtime.contracts.payloads import (
    CommandUpdated,
    RunCancelled,
    RunCompleted,
    RunFailed,
    RunStatusPayload,
)
from agent_runtime.contracts.references import CapabilityRef, ResourceRef, ResourceScope
from agent_runtime.contracts.results import Usage
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.state_events import (
    ChildEventAuthority,
    CommandUpdatedDraft,
    MainEventAuthority,
    RunCancelledDraft,
    RunCompletedDraft,
    RunFailedDraft,
    RunStatusDraft,
    ServiceEventAuthority,
    StateEventBatch,
)
from agent_runtime.contracts.subagents import (
    ChildRunRef,
    CollectRequest,
    SpawnRequest,
    VerifiedResult,
)
from agent_runtime.contracts.tasks import WaitSpec
from agent_runtime.contracts.transactions import ActorContext, CommitGuard
from agent_runtime.contracts.transitions import StateWriteGuard
from agent_runtime.contracts.views import Phase
from agent_runtime.contracts.work_claims import ChildClaimedWork, MaintenanceClaimedWork
from agent_runtime.storage.child_access import require_child_resources
from agent_runtime.storage.child_authority import require_child
from agent_runtime.storage.child_recovery import seal_child_output
from agent_runtime.storage.context_access import TransactionalResourceAccess
from agent_runtime.storage.delegation_sql import checkpoint, message, queue_work
from agent_runtime.storage.delegation_types import AcceptanceVerifier, DelegationPolicy
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.uow import SqlStateUnitOfWork, errors, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.worker import ChildLeaseHandle


class SqlSubagentRuntime:
    """Owns transactions, not model execution or the host's acceptance decision."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        journal: SqlJournal,
        resources: TransactionalResourceAccess,
        policies: Mapping[CapabilityRef, DelegationPolicy],
        message_predicate: CapabilityRef,
        verifier: AcceptanceVerifier,
        failure_notice_ttl_seconds: int = 86400,
    ) -> None:
        self._schema = require_schema(schema)
        if (
            type(failure_notice_ttl_seconds) is not int
            or not 1 <= failure_notice_ttl_seconds <= 604800
        ):
            raise ValueError("failure notice TTL must be between 1 and 604800 seconds")
        self._failure_notice_ttl_seconds = failure_notice_ttl_seconds
        if journal.schema != schema:
            raise ValueError("subagent Journal uses another schema")
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._actor, self._journal, self._resources = actor, journal, resources
        self._policies = dict(policies)
        if any(key != policy.agent for key, policy in self._policies.items()):
            raise ValueError("delegation policy target identity mismatch")
        self._predicate, self._verifier = message_predicate, verifier
        self._state = SqlStateUnitOfWork(engine, schema, actor)
        self._locks = locks.build_lock_statements(schema)

    @property
    def actor(self) -> ActorContext:
        return self._actor

    @property
    def schema(self) -> str:
        return self._schema

    async def _one(self, session: AsyncSession, sql: str, **params: Any) -> dict[str, Any] | None:
        row = (await session.execute(scoped(sql, self._schema), params)).mappings().one_or_none()
        return None if row is None else dict(row)

    async def _execute(self, db: AsyncSession, sql: str, **params: Any) -> None:
        await db.execute(scoped(sql, self._schema), params)

    def _guard(self, guard: CommitGuard, run: Mapping[str, Any]) -> MainEventAuthority:
        return MainEventAuthority(
            kind="main",
            guard=StateWriteGuard(
                actor=guard.actor,
                lease=guard.lease,
                expected_state_version=guard.expected_state_version,
                status=RunStatus.RUNNING,
                desired_control=None,
                control_command_id=None,
                cancel_version=str(run["cancel_version"]),
            ),
        )

    async def _parent(self, session: AsyncSession, guard: CommitGuard) -> Any:
        if guard.actor != self._actor:
            errors.forbidden("delegation actor changed")
        guarded = await self._state._lock_guard(session, guard)
        run = guarded.run
        if (
            guarded.work["kind"] != "advance_run"
            or run["parent_run_id"] is not None
            or run["root_run_id"] != run["id"]
        ):
            errors.forbidden("this adapter only delegates from the primary run")
        if run["state_version"] != int(guard.expected_state_version):
            errors.version_conflict("delegation parent state changed")
        if run["status"] != "running" or run["desired_control"] is not None:
            errors.control_pending("delegation parent has pending control")
        if run["deadline_at"] <= guarded.now:
            errors.version_conflict("delegation parent deadline elapsed")
        await self._resources.authorize_session_in_transaction(
            session, self._actor, run["session_id"]
        )
        return guarded

    async def _refs(
        self, session: AsyncSession, session_id: UUID, refs: tuple[ResourceRef, ...]
    ) -> None:
        for ref in refs:
            if ref.scope.scope_id != self._actor.scope_id or ref.resource_version is None:
                errors.forbidden("delegation requires same-scope versioned resources")
            await self._resources.authorize_resource_in_transaction(
                session, self._actor, ref, session_id=session_id
            )

    async def _status(
        self,
        session: AsyncSession,
        run: Mapping[str, Any],
        version: int,
        status: RunStatus,
        authority: Any,
        now: datetime,
        reason: str,
    ) -> None:
        phase: Phase = (
            "queued"
            if status == RunStatus.QUEUED
            else (
                "terminal"
                if status in {RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED}
                else "waiting"
            )
        )
        await self._journal.append_state_in_transaction(
            session,
            StateEventBatch(
                authority=authority,
                session_id=run["session_id"],
                trace_id=str(run["id"]),
                events=(
                    RunStatusDraft(
                        kind="run.status",
                        producer_key=f"run-status:{run['id']}:{version}",
                        occurred_at=now,
                        turn_id=run["turn_id"],
                        run_id=run["id"],
                        parent_run_id=run["parent_run_id"],
                        step_id=None,
                        item_id=None,
                        model_attempt_id=None,
                        payload=RunStatusPayload(
                            state_version=str(version),
                            status=status,
                            phase=phase,
                            phase_started_at=now,
                            public_reason=reason,
                        ),
                    ),
                ),
            ),
        )

    async def spawn(
        self, request: SpawnRequest, *, checkpoint_id: UUID | None = None
    ) -> ChildRunRef:
        try:
            return await self._spawn(request, checkpoint_id=checkpoint_id)
        except IntegrityError:
            errors.version_conflict("delegation identity or concurrent state conflicts")

    async def _spawn(self, request: SpawnRequest, *, checkpoint_id: UUID | None) -> ChildRunRef:
        task = request.task
        policy = self._policies.get(task.target_agent)
        if policy is None or task.output_schema_ref != policy.output_schema_ref:
            errors.forbidden("unregistered delegation target or output contract")
        expected_acceptance = (
            f"{policy.acceptance_spec.key.namespace}:{policy.acceptance_spec.key.key}:"
            f"{policy.acceptance_spec.revision}"
        )
        if task.acceptance_spec_ref != expected_acceptance:
            errors.forbidden("task acceptance contract is not registered")
        async with self._sessions() as db, db.begin():
            guarded = await self._parent(db, request.parent_guard)
            parent, now = guarded.run, guarded.now
            if parent["pending_input_version"] != parent["applied_input_version"]:
                errors.version_conflict("new input must be applied before delegation")
            if task.deadline_at <= now:
                errors.invalid_input("child deadline has expired")
            if checkpoint_id is None:
                checkpoint_id = await checkpoint(
                    db,
                    self._schema,
                    scope=self._actor.scope_id,
                    run_id=parent["id"],
                    version=parent["state_version"] + 1,
                    source=parent["checkpoint_id"],
                    event_seq=await self._state._session_seq(db, parent["session_id"]),
                    payload={
                        "kind": "delegation",
                        "task": task.model_dump(mode="json", by_alias=True),
                    },
                )
            else:
                await self._state._verify_checkpoint(
                    db, parent, checkpoint_id, maximum_state_version=parent["state_version"] + 1
                )
            target = await self._one(
                db,
                "SELECT revision,manifest FROM {t}agent_revision WHERE scope_id=:scope AND id=:id",
                scope=self._actor.scope_id,
                id=policy.agent_revision_id,
            )
            if target is None or target["revision"] != int(policy.agent.revision):
                errors.forbidden("pinned target agent revision is unavailable")
            if not isinstance(target["manifest"], dict):
                errors.forbidden("pinned target agent manifest is invalid")
            try:
                agent_budget = AgentBudget.model_validate(target["manifest"].get("budget", {}))
                deadline = min(
                    task.deadline_at,
                    parent["deadline_at"],
                    now + timedelta(seconds=agent_budget.max_runtime_seconds),
                )
            except (ValueError, TypeError, OverflowError):
                errors.forbidden("pinned target agent budget is invalid")
            if deadline <= now:
                errors.invalid_input("effective child deadline has expired")
            # The checkpoint above retains the requested task; all child state
            # below shares this one admission-time intersection, never a renewed TTL.
            task = task.model_copy(update={"deadline_at": deadline})
            grant = await locks.current_grant(
                db, self._locks, self._actor.scope_id, parent["id"], self._actor.actor_id
            )
            assert grant is not None
            allowed = grant["allowed_scope"]
            target_ref = task.target_agent.model_dump(mode="json", by_alias=True)
            tools = [t.model_dump(mode="json", by_alias=True) for t in policy.tools]
            refs = [r.model_dump(mode="json", by_alias=True) for r in task.resource_refs]
            if (
                not isinstance(allowed, dict)
                or target_ref not in allowed.get("agents", [])
                or any(t not in allowed.get("tools", []) for t in tools)
                or any(r not in allowed.get("resources", []) for r in refs)
            ):
                errors.forbidden("child capabilities exceed the parent grant")
            await self._refs(db, parent["session_id"], task.resource_refs)
            active = await self._one(
                db,
                "SELECT count(*) AS total FROM {t}run "
                "WHERE scope_id=:scope AND parent_run_id=:parent "
                "AND status NOT IN ('completed','failed','cancelled')",
                scope=self._actor.scope_id,
                parent=parent["id"],
            )
            if active is None or active["total"] >= policy.max_active_children:
                errors.capacity_unavailable("active child concurrency limit reached")
            budget = await locks.lock_domain_resource(
                db,
                self._schema,
                "budget_allocation",
                self._actor.scope_id,
                str(policy.parent_allocation_id),
            )
            if (
                budget is None
                or budget["root_run_id"] != parent["id"]
                or budget["parent_id"] is not None
            ):
                errors.forbidden("delegation parent budget has another owner")
            limits = policy.limits
            reserved = dict(budget["reserved"])
            for key, amount in limits.items():
                values = [
                    budget["limits"].get(key),
                    reserved.get(key, 0),
                    budget["settled"].get(key, 0),
                ]
                if any(type(value) is not int or value < 0 for value in values):
                    errors.invalid_input("parent budget quantities are not valid counters")
                if values[0] - values[1] - values[2] < amount:
                    errors.capacity_unavailable("insufficient parent budget for child")
                reserved[key] = values[1] + amount
            key = f"delegate:{parent['id']}:{request.idempotency_key}"
            child_id, workspace_id, goal_id = (
                uuid5(NAMESPACE_URL, f"{key}:{suffix}") for suffix in ("run", "workspace", "goal")
            )
            child = ChildRunRef(
                scope_id=self._actor.scope_id,
                session_id=parent["session_id"],
                parent_run_id=parent["id"],
                child_run_id=child_id,
                task_id=task.task_id,
                agent=policy.agent,
                budget_allocation_id=task.budget_allocation_id,
                workspace_id=workspace_id,
            )
            for table, identity in (
                ("run", child_id),
                ("task", task.task_id),
                ("budget_allocation", task.budget_allocation_id),
            ):
                if await self._one(db, f"SELECT id FROM {{t}}{table} WHERE id=:id", id=identity):
                    errors.version_conflict("delegation identity already exists")
            await self._execute(
                db,
                "INSERT INTO {t}task "
                "(id,scope_id,root_run_id,parent_task_id,current_goal_revision_id,"
                "acceptance_spec_ref,deadline_at) "
                "VALUES (:id,:scope,:run,:parent,:goal,:acceptance,:deadline)",
                id=task.task_id,
                scope=self._actor.scope_id,
                run=child_id,
                parent=parent["task_id"],
                goal=goal_id,
                acceptance=task.acceptance_spec_ref,
                deadline=task.deadline_at,
            )
            await self._execute(
                db,
                "INSERT INTO {t}goal_revision "
                "(id,scope_id,task_id,revision,objective,constraints,focus_refs) "
                "VALUES (:id,:scope,:task,1,:objective,"
                "CAST(:constraints AS jsonb),CAST(:refs AS jsonb))",
                id=goal_id,
                scope=self._actor.scope_id,
                task=task.task_id,
                objective=task.objective,
                constraints=json.dumps(task.constraints),
                refs=json.dumps(refs),
            )
            await self._execute(
                db,
                "INSERT INTO {t}run "
                "(id,scope_id,session_id,turn_id,root_run_id,parent_run_id,agent_revision_id,task_id,"
                "status,phase,state_version,owner_epoch,cancel_version,wake_generation,"
                "pending_input_version,applied_input_version,authority_revision,"
                "phase_started_at,deadline_at) "
                "VALUES (:id,:scope,:session,:turn,:root,:parent,:agent,:task,'queued','queued',"
                "0,0,0,0,1,1,:auth,:now,:deadline)",
                id=child_id,
                scope=self._actor.scope_id,
                session=parent["session_id"],
                turn=parent["turn_id"],
                root=parent["id"],
                parent=parent["id"],
                agent=policy.agent_revision_id,
                task=task.task_id,
                auth=int(self._actor.auth_revision),
                now=now,
                deadline=task.deadline_at,
            )
            await self._execute(
                db,
                "INSERT INTO {t}workspace "
                "(id,scope_id,task_id,owner_run_id,generation,revision) "
                "VALUES (:id,:scope,:task,:run,1,1)",
                id=workspace_id,
                scope=self._actor.scope_id,
                task=task.task_id,
                run=child_id,
            )
            child_refs = refs + [
                ResourceRef(
                    resource_type="goal_revision",
                    resource_id=str(goal_id),
                    resource_version="1",
                    scope=ResourceScope(scope_id=self._actor.scope_id),
                ).model_dump(mode="json", by_alias=True)
            ]
            await self._execute(
                db,
                "INSERT INTO {t}capability_grant "
                "(id,scope_id,run_id,actor_id,revision,allowed_scope,issued_at,expires_at) "
                "VALUES (:id,:scope,:run,:actor,:revision,CAST(:allowed AS jsonb),:now,:expires)",
                id=uuid5(NAMESPACE_URL, key + ":grant"),
                scope=self._actor.scope_id,
                run=child_id,
                actor=self._actor.actor_id,
                revision=int(self._actor.auth_revision),
                allowed=json.dumps({"resources": child_refs, "tools": tools, "agents": []}),
                now=now,
                expires=min(task.deadline_at, grant["expires_at"]),
            )
            await self._execute(
                db,
                "INSERT INTO {t}budget_allocation "
                "(id,scope_id,root_run_id,parent_id,limits,reserved,unknown_reserved,settled,"
                "usage_status,unsettled_count,version) VALUES (:id,:scope,:root,:parent,"
                "CAST(:limits AS jsonb),'{{}}','{{}}','{{}}','complete',0,0)",
                id=task.budget_allocation_id,
                scope=self._actor.scope_id,
                root=parent["id"],
                parent=budget["id"],
                limits=json.dumps(limits),
            )
            await self._execute(
                db,
                "UPDATE {t}budget_allocation SET reserved=CAST(:reserved AS jsonb),"
                "version=version+1,updated_at=:now WHERE id=:id AND scope_id=:scope",
                reserved=json.dumps(reserved),
                now=now,
                id=budget["id"],
                scope=self._actor.scope_id,
            )
            await self._execute(
                db,
                "INSERT INTO {t}budget_entry "
                "(id,scope_id,allocation_id,operation_key,kind,usage_status,quantities) "
                "VALUES (:id,:scope,:allocation,:key,'reserve','complete',CAST(:limits AS jsonb))",
                id=uuid5(NAMESPACE_URL, key + ":reserve"),
                scope=self._actor.scope_id,
                allocation=budget["id"],
                key=key,
                limits=json.dumps(limits),
            )
            cp = await checkpoint(
                db,
                self._schema,
                scope=self._actor.scope_id,
                run_id=child_id,
                version=0,
                source=None,
                event_seq=await self._state._session_seq(db, parent["session_id"]),
                payload={
                    "kind": "child_assignment",
                    "task": task.model_dump(mode="json", by_alias=True),
                    "child": child.model_dump(mode="json", by_alias=True),
                },
            )
            await self._execute(
                db, "UPDATE {t}run SET checkpoint_id=:cp WHERE id=:id", cp=cp, id=child_id
            )
            await message(
                db,
                self._schema,
                scope=self._actor.scope_id,
                status="applied",
                applied_checkpoint=cp,
                envelope=TaskAssignMessage(
                    protocol_revision=1,
                    message_id=uuid5(NAMESPACE_URL, key + ":assign"),
                    correlation_id=task.task_id,
                    causation_id=None,
                    sender_run_id=parent["id"],
                    recipient_run_id=child_id,
                    task_id=task.task_id,
                    recipient_seq="1",
                    created_at=now,
                    deadline_at=task.deadline_at,
                    resource_refs=task.resource_refs,
                    trace_id=str(request.parent_guard.lease.work_id),
                    kind="task.assign",
                    payload=TaskAssign(
                        objective=task.objective,
                        constraints=task.constraints,
                        acceptance_spec_ref=task.acceptance_spec_ref,
                        output_schema_ref=task.output_schema_ref,
                        budget_allocation_id=task.budget_allocation_id,
                    ),
                ),
            )
            await queue_work(
                db,
                self._schema,
                scope=self._actor.scope_id,
                run_id=child_id,
                checkpoint_id=cp,
                key=key + ":work",
                now=now,
                deadline=task.deadline_at,
            )
            version = parent["state_version"] + 1
            await self._status(
                db,
                parent,
                version,
                RunStatus.WAITING_USER,
                self._guard(request.parent_guard, parent),
                now,
                "Waiting for delegated result.",
            )
            child_row = {**parent, "id": child_id, "parent_run_id": parent["id"]}
            await self._status(
                db,
                child_row,
                0,
                RunStatus.QUEUED,
                ServiceEventAuthority(
                    kind="service",
                    actor=self._actor,
                    purpose="wake",
                    source=ResourceRef(
                        resource_type="run",
                        resource_id=str(child_id),
                        resource_version="0",
                        scope=ResourceScope(
                            scope_id=self._actor.scope_id, session_id=str(parent["session_id"])
                        ),
                    ),
                    transaction_id=child_id,
                ),
                now,
                "Delegated task queued.",
            )
            wait = WaitSpec(
                kind="message",
                subject_id=str(parent["id"]),
                deadline_at=task.deadline_at,
                resume_predicate=self._predicate,
            )
            await self._execute(
                db,
                "UPDATE {t}run SET status='waiting_user',phase='waiting',"
                "phase_started_at=:now,state_version=:version,checkpoint_id=:cp,"
                "waiting_spec=CAST(:wait AS jsonb),owner_id=NULL,lease_until=NULL,"
                "owner_epoch=owner_epoch+1,updated_at=:now WHERE id=:id",
                now=now,
                version=version,
                cp=checkpoint_id,
                wait=wait.model_dump_json(by_alias=True),
                id=parent["id"],
            )
            await self._state._finish_work(
                db, parent, guarded.work["id"], status="completed", now=now
            )
            await self._state._release_slot(db, parent, now)
            return child

    async def propose(self, request: ChildProposalRequest) -> UUID:
        """Persist a child result and wake only a still-waiting, uncontrolled parent."""
        try:
            return await self._propose(request)
        except IntegrityError:
            errors.version_conflict("proposal identity or concurrent state conflicts")

    async def _propose(self, request: ChildProposalRequest) -> UUID:
        guard = request.guard
        if guard.actor != self._actor:
            errors.forbidden("proposal actor changed")
        async with self._sessions() as db, db.begin():
            # The slot serializes this transaction with parent writes, never grants child execution.
            await locks.lock_slot(db, self._locks, guard.lease.scope_id, guard.lease.session_id)
            rows = {}
            for identity in sorted((guard.root_run_id, guard.lease.run_id), key=str):
                rows[identity] = await locks.lock_run(
                    db, self._locks, guard.lease.scope_id, identity
                )
            parent, child = rows[guard.root_run_id], rows[guard.lease.run_id]
            if (
                parent is None
                or child is None
                or child["parent_run_id"] != parent["id"]
                or child["task_id"] != request.task_id
                or parent["status"] in {"completed", "failed", "cancelled"}
                or parent["desired_control"] == "cancel"
                or child["desired_control"] is not None
                or child["status"] != "running"
                or child["deadline_at"] <= datetime.now(UTC)
                or parent["deadline_at"] <= datetime.now(UTC)
            ):
                errors.version_conflict("child proposal parent or execution is no longer eligible")
            if child["pending_input_version"] != child["applied_input_version"]:
                errors.version_conflict("child assignment or input has not been applied")
            await revalidate_authority(
                db, self._locks, self._actor, run=parent, now=datetime.now(UTC)
            )
            for table, terminal in (
                ("model_attempt", "'completed','failed','cancelled','interrupted'"),
                ("tool_call", "'completed','failed','cancelled','not_executed'"),
                ("execution", "'completed','failed','cancelled'"),
            ):
                active = await self._one(
                    db,
                    f"SELECT count(*) AS total FROM {{t}}{table} "
                    f"WHERE scope_id=:scope AND run_id=:run AND status NOT IN ({terminal})",
                    scope=self._actor.scope_id,
                    run=child["id"],
                )
                if active and active["total"]:
                    errors.version_conflict("child has unresolved calls or processes")
            unsealed = await self._one(
                db,
                "SELECT count(*) AS total FROM {t}event e "
                "WHERE e.scope_id=:scope AND e.run_id=:run "
                "AND e.kind IN ('assistant.delta','analysis.delta') "
                "AND NOT EXISTS (SELECT 1 FROM {t}output_seal s WHERE s.scope_id=e.scope_id "
                "AND s.run_id=e.run_id AND s.model_attempt_id=e.model_attempt_id "
                "AND s.item_id=e.item_id)",
                scope=self._actor.scope_id,
                run=child["id"],
            )
            if unsealed and unsealed["total"]:
                errors.version_conflict("child output is not sealed")
            proposal = request.proposal
            await require_child_resources(
                db,
                self._schema,
                self._actor,
                child["id"],
                (
                    *proposal.artifact_refs,
                    *proposal.evidence_refs,
                    *proposal.validation.evidence_refs,
                ),
            )
            await self._refs(
                db,
                parent["session_id"],
                (
                    *proposal.artifact_refs,
                    *proposal.evidence_refs,
                    *proposal.validation.evidence_refs,
                ),
            )
            now = datetime.now(UTC)
            version = child["state_version"] + 1
            await self._status(
                db,
                child,
                version,
                RunStatus.COMPLETED,
                ChildEventAuthority(kind="child", guard=guard),
                now,
                "Child result proposed.",
            )
            cp = await checkpoint(
                db,
                self._schema,
                scope=self._actor.scope_id,
                run_id=child["id"],
                version=version,
                source=child["checkpoint_id"],
                event_seq=await self._state._session_seq(db, parent["session_id"]),
                payload={
                    "kind": "child_proposal",
                    "proposal": proposal.model_dump(mode="json", by_alias=True),
                },
            )
            await self._journal.append_state_in_transaction(
                db,
                StateEventBatch(
                    authority=ChildEventAuthority(kind="child", guard=guard),
                    session_id=parent["session_id"],
                    trace_id=str(proposal.result_id),
                    events=(
                        RunCompletedDraft(
                            kind="run.completed",
                            producer_key=f"run-completed:{child['id']}:{version}",
                            occurred_at=now,
                            turn_id=child["turn_id"],
                            run_id=child["id"],
                            parent_run_id=parent["id"],
                            step_id=None,
                            item_id=None,
                            model_attempt_id=None,
                            payload=RunCompleted(
                                state_version=str(version),
                                final_item_ids=(),
                                partial_result_refs=proposal.artifact_refs,
                                outcome="success",
                                failure=None,
                            ),
                        ),
                    ),
                ),
            )
            seq = parent["pending_input_version"] + 1
            await message(
                db,
                self._schema,
                scope=self._actor.scope_id,
                envelope=ResultProposedMessage(
                    protocol_revision=1,
                    message_id=proposal.result_id,
                    correlation_id=request.task_id,
                    causation_id=None,
                    sender_run_id=child["id"],
                    recipient_run_id=parent["id"],
                    task_id=request.task_id,
                    recipient_seq=str(seq),
                    created_at=now,
                    deadline_at=parent["deadline_at"],
                    resource_refs=proposal.artifact_refs,
                    trace_id=str(proposal.result_id),
                    kind="result.proposed",
                    payload=proposal,
                ),
            )
            await self._execute(
                db,
                "UPDATE {t}run SET status='completed',phase='terminal',outcome='success',"
                "state_version=:version,checkpoint_id=:cp,owner_id=NULL,lease_until=NULL,owner_epoch=owner_epoch+1,"
                "phase_started_at=:now,updated_at=:now WHERE id=:id",
                version=version,
                cp=cp,
                now=now,
                id=child["id"],
            )
            await self._state._finish_work(
                db, child, guard.lease.work_id, status="completed", now=now
            )
            await self._execute(
                db,
                "UPDATE {t}run SET pending_input_version=:seq WHERE id=:id",
                seq=seq,
                id=parent["id"],
            )
            if (
                parent["status"] == "waiting_user"
                and parent["deadline_at"] > now
                and parent["desired_control"] is None
            ):
                spec = WaitSpec.model_validate(parent["waiting_spec"])
                if spec.kind == "message" and spec.subject_id == str(parent["id"]):
                    await self._wake_parent(db, parent, proposal.result_id, now)
            return proposal.result_id

    async def report_failure(
        self, guard: ChildCommitGuard, failure: FailureEnvelope, *, cancelled: bool = False
    ) -> UUID:
        """Terminal child failure is not a Python task-cancellation acknowledgement."""
        if guard.actor != self._actor:
            errors.forbidden("child failure actor changed")
        async with self._sessions() as db, db.begin():
            await locks.lock_slot(db, self._locks, guard.lease.scope_id, guard.lease.session_id)
            rows = {}
            for identity in sorted((guard.root_run_id, guard.lease.run_id), key=str):
                rows[identity] = await locks.lock_run(
                    db, self._locks, self._actor.scope_id, identity
                )
            parent, child = rows[guard.root_run_id], rows[guard.lease.run_id]
            if (
                parent is None
                or child is None
                or child["parent_run_id"] != parent["id"]
                or child["root_run_id"] != parent["id"]
                or child["session_id"] != parent["session_id"]
            ):
                errors.forbidden("child failure has another parent")
            if cancelled and not (
                child["desired_control"] == "cancel"
                or parent["desired_control"] == "cancel"
                or parent["status"] == "cancelled"
            ):
                errors.control_pending("child cancellation needs persisted control")
            if not cancelled and (
                child["desired_control"] == "cancel"
                or parent["desired_control"] == "cancel"
                or parent["status"] == "cancelled"
            ):
                errors.control_pending("pending cancellation cannot become ordinary failure")
            await self._resources.authorize_session_in_transaction(
                db, self._actor, parent["session_id"]
            )
            for table, terminals in (
                ("model_attempt", "'completed','failed','cancelled','interrupted'"),
                ("tool_call", "'completed','failed','cancelled','not_executed'"),
                ("execution", "'completed','failed','cancelled'"),
            ):
                active = await self._one(
                    db,
                    f"SELECT id FROM {{t}}{table} WHERE scope_id=:scope "
                    f"AND run_id=:run AND status NOT IN ({terminals}) LIMIT 1",
                    scope=self._actor.scope_id,
                    run=child["id"],
                )
                if active is not None:
                    errors.version_conflict("child failure still has unresolved execution")
            now = datetime.now(UTC)
            version = child["state_version"] + 1
            identity = uuid5(NAMESPACE_URL, f"child-failed:{child['id']}:{version}")
            status = RunStatus.CANCELLED if cancelled else RunStatus.FAILED
            authority = ChildEventAuthority(kind="child", guard=guard)
            await require_child(db, self._schema, guard, now=now, allow_control=True)
            partial_refs = await seal_child_output(
                db,
                self._schema,
                self._journal,
                authority,
                child,
                now,
                status="cancelled" if cancelled else "failed",
            )
            await self._status(db, child, version, status, authority, now, failure.public_message)
            event_args: dict[str, Any] = dict(
                producer_key=f"child-terminal:{child['id']}:{version}",
                occurred_at=now,
                turn_id=child["turn_id"],
                run_id=child["id"],
                parent_run_id=parent["id"],
                step_id=None,
                item_id=None,
                model_attempt_id=None,
            )
            terminal_args: dict[str, Any] = dict(
                state_version=str(version),
                final_item_ids=(),
                partial_result_refs=partial_refs,
                outcome=None,
            )
            event = (
                RunCancelledDraft(
                    kind="run.cancelled",
                    **event_args,
                    payload=RunCancelled(**terminal_args, failure=None),
                )
                if cancelled
                else RunFailedDraft(
                    kind="run.failed",
                    **event_args,
                    payload=RunFailed(**terminal_args, failure=failure),
                )
            )
            await self._journal.append_state_in_transaction(
                db,
                StateEventBatch(
                    authority=authority,
                    session_id=parent["session_id"],
                    trace_id=str(identity),
                    events=(event,),
                ),
            )
            if child["control_command_id"] is not None:
                command_status: Literal["applied", "rejected"] = (
                    "applied" if cancelled and child["desired_control"] == "cancel" else "rejected"
                )
                command_id = child["control_command_id"]
                await self._execute(
                    db,
                    "UPDATE {t}command SET status=:status,result_ref=:ref,updated_at=:now "
                    "WHERE scope_id=:scope AND id=:id AND run_id=:run AND status='accepted'",
                    status=command_status,
                    ref=str(identity),
                    now=now,
                    scope=self._actor.scope_id,
                    id=command_id,
                    run=child["id"],
                )
                await self._journal.append_state_in_transaction(
                    db,
                    StateEventBatch(
                        authority=authority,
                        session_id=parent["session_id"],
                        trace_id=str(identity),
                        events=(
                            CommandUpdatedDraft(
                                kind="command.updated",
                                **{
                                    **event_args,
                                    "producer_key": f"child-command:{command_id}:{command_status}",
                                },
                                payload=CommandUpdated(
                                    command_id=command_id,
                                    status=command_status,
                                    public_reason=failure.public_message,
                                ),
                            ),
                        ),
                    ),
                )
            cp = await checkpoint(
                db,
                self._schema,
                scope=self._actor.scope_id,
                run_id=child["id"],
                version=version,
                source=child["checkpoint_id"],
                event_seq=await self._state._session_seq(db, parent["session_id"]),
                payload={
                    "kind": "child_failed",
                    "cancelled": cancelled,
                    "failure": failure.model_dump(mode="json", by_alias=True),
                },
            )
            seq = parent["pending_input_version"] + 1
            envelope = message_adapter.validate_python(
                dict(
                    protocol_revision=1,
                    message_id=identity,
                    correlation_id=child["task_id"],
                    causation_id=None,
                    sender_run_id=child["id"],
                    recipient_run_id=parent["id"],
                    task_id=child["task_id"],
                    recipient_seq=str(seq),
                    created_at=now,
                    deadline_at=parent["deadline_at"]
                    if parent["deadline_at"] > now
                    else now + timedelta(seconds=self._failure_notice_ttl_seconds),
                    resource_refs=(),
                    trace_id=str(identity),
                    kind="task.failed",
                    payload=TaskFailed(failure=failure, partial_result_refs=partial_refs),
                )
            )
            await message(
                db,
                self._schema,
                scope=self._actor.scope_id,
                envelope=envelope,
                status="pending" if parent["deadline_at"] > now else "expired",
            )
            await self._execute(
                db,
                "UPDATE {t}run SET status=:status,phase='terminal',outcome=NULL,"
                "failure=CAST(:failure AS jsonb),state_version=:version,checkpoint_id=:cp,"
                "desired_control=NULL,control_command_id=NULL,owner_id=NULL,lease_until=NULL,"
                "owner_epoch=owner_epoch+1,phase_started_at=:now,updated_at=:now WHERE id=:id",
                status=status.value,
                failure=None if cancelled else failure.model_dump_json(by_alias=True),
                version=version,
                cp=cp,
                now=now,
                id=child["id"],
            )
            await self._state._finish_work(
                db, child, guard.lease.work_id, status="completed", now=now
            )
            await self._execute(
                db,
                "UPDATE {t}work_item SET status='cancelled',owner_id=NULL,lease_until=NULL,"
                "lease_epoch=lease_epoch+1,updated_at=:now WHERE scope_id=:scope "
                "AND run_id=:run AND (kind='advance_run' OR "
                "(kind='reconcile_execution' AND payload_ref='cancel-convergence' "
                "AND dedup_key=:control_key)) "
                "AND status IN ('pending','leased')",
                scope=self._actor.scope_id,
                run=child["id"],
                control_key=f"cancel:{child['control_command_id']}",
                now=now,
            )
            await self._execute(
                db,
                "UPDATE {t}run SET pending_input_version=:seq WHERE id=:id",
                seq=seq,
                id=parent["id"],
            )
            if (
                parent["status"] == "waiting_user"
                and parent["desired_control"] is None
                and parent["deadline_at"] > now
            ):
                spec = WaitSpec.model_validate(parent["waiting_spec"])
                if spec.kind == "message" and spec.subject_id == str(parent["id"]):
                    await self._wake_parent(db, parent, identity, now)
            return identity

    async def park_unknown_execution(self, guard: ChildCommitGuard) -> UUID:
        """Release a child lease while an external effect is being reconciled."""
        if guard.actor != self._actor:
            errors.forbidden("child park actor changed")
        now = datetime.now(UTC)
        async with self._sessions() as db, db.begin():
            child = await require_child(db, self._schema, guard, now=now)
            pending = await self._one(
                db,
                "SELECT tc.id AS tool_call_id FROM {t}tool_call tc "
                "JOIN {t}execution e ON e.scope_id=tc.scope_id "
                "AND e.operation_id=tc.operation_id "
                "WHERE tc.scope_id=:scope AND tc.run_id=:run AND tc.status='failed' "
                "AND e.status NOT IN ('completed','failed','cancelled') "
                "ORDER BY tc.created_at DESC LIMIT 1",
                scope=self._actor.scope_id,
                run=child["id"],
            )
            if pending is None:
                errors.version_conflict("child has no unresolved external effect to park")
            version = child["state_version"] + 1
            authority = ChildEventAuthority(kind="child", guard=guard)
            await self._status(
                db,
                child,
                version,
                RunStatus.WAITING_TOOL,
                authority,
                now,
                "Reconciling a delegated external tool effect.",
            )
            cp = await checkpoint(
                db,
                self._schema,
                scope=self._actor.scope_id,
                run_id=child["id"],
                version=version,
                source=child["checkpoint_id"],
                event_seq=await self._state._session_seq(db, child["session_id"]),
                payload={
                    "kind": "child_external_effect_wait",
                    "toolCallId": str(pending["tool_call_id"]),
                },
            )
            wait = WaitSpec(
                kind="tool",
                subject_id=str(pending["tool_call_id"]),
                deadline_at=child["deadline_at"],
                resume_predicate=self._predicate,
            )
            await self._execute(
                db,
                "UPDATE {t}run SET status='waiting_tool',phase='waiting',"
                "phase_started_at=:now,state_version=:version,checkpoint_id=:cp,"
                "waiting_spec=CAST(:wait AS jsonb),owner_id=NULL,lease_until=NULL,"
                "owner_epoch=owner_epoch+1,updated_at=:now WHERE id=:id",
                now=now,
                version=version,
                cp=cp,
                wait=wait.model_dump_json(by_alias=True),
                id=child["id"],
            )
            await self._state._finish_work(
                db, child, guard.lease.work_id, status="completed", now=now
            )
            return UUID(str(pending["tool_call_id"]))

    async def fail_reconciled_external(
        self, work: MaintenanceClaimedWork, operation_id: UUID
    ) -> UUID:
        """Finish a parked child only after a fenced supervisor confirms remote state."""
        if work.kind != "reconcile_execution" or work.lease.scope_id != self._actor.scope_id:
            errors.forbidden("child reconciliation work belongs to another scope")
        now = datetime.now(UTC)
        async with self._sessions() as db, db.begin():
            subject = await self._one(
                db,
                "SELECT e.run_id,r.session_id FROM {t}execution e JOIN {t}run r "
                "ON r.scope_id=e.scope_id AND r.id=e.run_id "
                "WHERE e.scope_id=:scope AND e.id=:execution",
                scope=self._actor.scope_id,
                execution=UUID(work.subject_ref.resource_id),
            )
            if subject is None:
                errors.version_conflict("reconciled execution disappeared")
            run_id = UUID(str(subject["run_id"]))
            await locks.lock_slot(db, self._locks, work.lease.scope_id, subject["session_id"])
            candidate = await self._one(
                db,
                "SELECT parent_run_id FROM {t}run WHERE scope_id=:scope AND id=:run",
                scope=work.lease.scope_id,
                run=run_id,
            )
            if candidate is None or candidate["parent_run_id"] is None:
                errors.forbidden("reconciled execution does not belong to a child")
            rows = {}
            for identity in sorted((run_id, candidate["parent_run_id"]), key=str):
                rows[identity] = await locks.lock_run(
                    db, self._locks, work.lease.scope_id, identity
                )
            child, parent = rows[run_id], rows[candidate["parent_run_id"]]
            if (
                child is None
                or parent is None
                or child["parent_run_id"] != parent["id"]
                or child["root_run_id"] != parent["id"]
                or child["session_id"] != parent["session_id"]
                or child["session_id"] != subject["session_id"]
                or child["status"] != "waiting_tool"
            ):
                errors.version_conflict("child is no longer parked for reconciliation")
            await self._resources.authorize_session_in_transaction(
                db, self._actor, child["session_id"]
            )
            leased = await locks.lock_work_item(
                db, self._locks, work.lease.scope_id, work.lease.work_id
            )
            if (
                leased is None
                or leased["status"] != "leased"
                or leased["owner_id"] != work.lease.owner_id
                or int(leased["lease_epoch"]) != int(work.lease.work_epoch)
                or leased["lease_until"] is None
                or leased["lease_until"] <= now
                or leased["run_id"] != child["id"]
            ):
                errors.fenced("child reconciliation work lease changed")
            execution = await self._one(
                db,
                "SELECT id,status,supervisor_work_id,generation FROM {t}execution "
                "WHERE scope_id=:scope AND operation_id=:operation AND run_id=:run FOR UPDATE",
                scope=self._actor.scope_id,
                operation=operation_id,
                run=child["id"],
            )
            if (
                execution is None
                or execution["id"] != UUID(work.subject_ref.resource_id)
                or str(execution["generation"]) != work.subject_ref.resource_version
                or execution["supervisor_work_id"] != leased["id"]
                or execution["status"] not in {"completed", "failed", "cancelled"}
            ):
                errors.version_conflict("external execution has not been reconciled")
            tool = await self._one(
                db,
                "SELECT id,status FROM {t}tool_call WHERE scope_id=:scope "
                "AND run_id=:run AND operation_id=:operation",
                scope=self._actor.scope_id,
                run=child["id"],
                operation=operation_id,
            )
            wait = WaitSpec.model_validate(child["waiting_spec"])
            if tool is None or tool["status"] != "failed" or wait.subject_id != str(tool["id"]):
                errors.version_conflict("child wait no longer matches the failed tool")
            failure = FailureEnvelope(
                code="DEPENDENCY_UNAVAILABLE",
                category="tool",
                retryable=False,
                public_message=(
                    "The delegated external effect was reconciled, but its original "
                    "result is unavailable. The task stopped without repeating the write."
                ),
            )
            version = child["state_version"] + 1
            identity = uuid5(NAMESPACE_URL, f"child-reconciled:{child['id']}:{version}")
            authority = ServiceEventAuthority(
                kind="service",
                actor=self._actor,
                purpose="recovery",
                source=ResourceRef(
                    resource_type="run",
                    resource_id=str(child["id"]),
                    resource_version=str(child["state_version"]),
                    scope=ResourceScope(
                        scope_id=self._actor.scope_id,
                        session_id=str(child["session_id"]),
                    ),
                ),
                transaction_id=identity,
            )
            await self._status(
                db, child, version, RunStatus.FAILED, authority, now, failure.public_message
            )
            await self._journal.append_state_in_transaction(
                db,
                StateEventBatch(
                    authority=authority,
                    session_id=child["session_id"],
                    trace_id=str(identity),
                    events=(
                        RunFailedDraft(
                            kind="run.failed",
                            producer_key=f"child-terminal:{child['id']}:{version}",
                            occurred_at=now,
                            turn_id=child["turn_id"],
                            run_id=child["id"],
                            parent_run_id=parent["id"],
                            step_id=None,
                            item_id=None,
                            model_attempt_id=None,
                            payload=RunFailed(
                                state_version=str(version),
                                final_item_ids=(),
                                partial_result_refs=(),
                                outcome=None,
                                failure=failure,
                            ),
                        ),
                    ),
                ),
            )
            cp = await checkpoint(
                db,
                self._schema,
                scope=self._actor.scope_id,
                run_id=child["id"],
                version=version,
                source=child["checkpoint_id"],
                event_seq=await self._state._session_seq(db, child["session_id"]),
                payload={
                    "kind": "child_external_effect_reconciled",
                    "operationId": str(operation_id),
                    "failure": failure.model_dump(mode="json", by_alias=True),
                },
            )
            seq = parent["pending_input_version"] + 1
            await message(
                db,
                self._schema,
                scope=self._actor.scope_id,
                envelope=message_adapter.validate_python(
                    dict(
                        protocol_revision=1,
                        message_id=identity,
                        correlation_id=child["task_id"],
                        causation_id=None,
                        sender_run_id=child["id"],
                        recipient_run_id=parent["id"],
                        task_id=child["task_id"],
                        recipient_seq=str(seq),
                        created_at=now,
                        deadline_at=(
                            parent["deadline_at"]
                            if parent["deadline_at"] > now
                            else now + timedelta(seconds=self._failure_notice_ttl_seconds)
                        ),
                        resource_refs=(),
                        trace_id=str(identity),
                        kind="task.failed",
                        payload=TaskFailed(failure=failure, partial_result_refs=()),
                    )
                ),
                status="pending" if parent["deadline_at"] > now else "expired",
            )
            await self._execute(
                db,
                "UPDATE {t}run SET status='failed',phase='terminal',outcome=NULL,"
                "failure=CAST(:failure AS jsonb),state_version=:version,checkpoint_id=:cp,"
                "waiting_spec=NULL,desired_control=NULL,control_command_id=NULL,"
                "owner_id=NULL,lease_until=NULL,owner_epoch=owner_epoch+1,"
                "phase_started_at=:now,updated_at=:now WHERE id=:id",
                failure=failure.model_dump_json(by_alias=True),
                version=version,
                cp=cp,
                now=now,
                id=child["id"],
            )
            await self._execute(
                db,
                "UPDATE {t}work_item SET status='completed',owner_id=NULL,"
                "lease_until=NULL,lease_epoch=lease_epoch+1,updated_at=:now "
                "WHERE scope_id=:scope AND id=:id",
                now=now,
                scope=self._actor.scope_id,
                id=leased["id"],
            )
            await self._execute(
                db,
                "UPDATE {t}run SET pending_input_version=:seq WHERE id=:id",
                seq=seq,
                id=parent["id"],
            )
            if (
                parent["status"] == "waiting_user"
                and parent["desired_control"] is None
                and parent["deadline_at"] > now
            ):
                parent_wait = WaitSpec.model_validate(parent["waiting_spec"])
                if parent_wait.kind == "message" and parent_wait.subject_id == str(parent["id"]):
                    await self._wake_parent(db, parent, identity, now)
            return identity

    async def handle_worker_failure(
        self, work: ChildClaimedWork, handle: ChildLeaseHandle, error: BaseException
    ) -> None:
        """Worker child_error_sink; stale authority is never upgraded on takeover."""
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise error
        lease = handle.current()
        if lease.run_id != work.lease.run_id or lease.work_id != work.lease.work_id:
            errors.forbidden("child error lease changed identity")
        async with self._sessions() as db:
            child = await self._one(
                db,
                "SELECT * FROM {t}run WHERE scope_id=:scope AND id=:id",
                scope=self._actor.scope_id,
                id=lease.run_id,
            )
            root = await self._one(
                db,
                "SELECT * FROM {t}run WHERE scope_id=:scope AND id=:id",
                scope=self._actor.scope_id,
                id=work.root_run_id,
            )
        if child is None or root is None:
            errors.forbidden("child error run disappeared")
        guard = ChildCommitGuard(
            actor=self._actor,
            lease=lease,
            expected_state_version=str(child["state_version"]),
            root_run_id=work.root_run_id,
            expected_root_cancel_version=str(root["cancel_version"]),
            workspace_ref=work.workspace_ref,
            workspace_owner_run_id=lease.run_id,
            workspace_generation=work.workspace_generation,
        )
        failure = (
            error.failure
            if isinstance(error, AgentRuntimeError)
            else FailureEnvelope(
                code="INTERNAL",
                category="internal",
                retryable=False,
                public_message="The child worker could not complete its task.",
            )
        )
        await self.report_failure(
            guard,
            failure,
            cancelled=(
                child["desired_control"] == "cancel"
                or root["desired_control"] == "cancel"
                or root["status"] == "cancelled"
            ),
        )

    async def _wake_parent(
        self, db: AsyncSession, parent: Mapping[str, Any], proposal_id: UUID, now: datetime
    ) -> None:
        version = parent["state_version"] + 1
        cp = await checkpoint(
            db,
            self._schema,
            scope=self._actor.scope_id,
            run_id=parent["id"],
            version=version,
            source=parent["checkpoint_id"],
            event_seq=await self._state._session_seq(db, parent["session_id"]),
            payload={"kind": "child_result_ready", "proposalId": str(proposal_id)},
        )
        authority = ServiceEventAuthority(
            kind="service",
            actor=self._actor,
            purpose="wake",
            source=ResourceRef(
                resource_type="run",
                resource_id=str(parent["id"]),
                resource_version=str(parent["state_version"]),
                scope=ResourceScope(
                    scope_id=self._actor.scope_id, session_id=str(parent["session_id"])
                ),
            ),
            transaction_id=proposal_id,
        )
        await self._status(
            db,
            parent,
            version,
            RunStatus.QUEUED,
            authority,
            now,
            "Delegated result ready for verification.",
        )
        await self._execute(
            db,
            "UPDATE {t}run SET status='queued',phase='queued',waiting_spec=NULL,"
            "state_version=:version,checkpoint_id=:cp,wake_generation=wake_generation+1,phase_started_at=:now,"
            "updated_at=:now WHERE id=:id",
            version=version,
            cp=cp,
            now=now,
            id=parent["id"],
        )
        await queue_work(
            db,
            self._schema,
            scope=self._actor.scope_id,
            run_id=parent["id"],
            checkpoint_id=cp,
            key=f"child-result:{parent['id']}:{version}",
            now=now,
            deadline=parent["deadline_at"],
        )

    async def collect(self, request: CollectRequest) -> VerifiedResult:
        async with self._sessions() as db, db.begin():
            guarded = await self._parent(db, request.parent_guard)
            parent, now = guarded.run, guarded.now
            child = await self._one(
                db,
                "SELECT * FROM {t}run WHERE scope_id=:scope AND id=:id",
                scope=self._actor.scope_id,
                id=request.child.child_run_id,
            )
            policy = self._policies.get(request.child.agent)
            if (
                child is None
                or policy is None
                or request.child.scope_id != self._actor.scope_id
                or request.child.session_id != parent["session_id"]
                or request.child.parent_run_id != parent["id"]
                or child["parent_run_id"] != parent["id"]
                or child["root_run_id"] != parent["id"]
                or child["task_id"] != request.child.task_id
                or child["session_id"] != parent["session_id"]
                or child["status"] != "completed"
                or child["agent_revision_id"] != policy.agent_revision_id
                or request.acceptance_spec != policy.acceptance_spec
            ):
                errors.forbidden(
                    "collection target or acceptance contract is not owned by this parent"
                )
            binding = await self._one(
                db,
                "SELECT payload FROM {t}agent_message WHERE scope_id=:scope "
                "AND recipient_run_id=:child AND sender_run_id=:parent "
                "AND task_id=:task AND kind='task.assign'",
                scope=self._actor.scope_id,
                child=child["id"],
                parent=parent["id"],
                task=child["task_id"],
            )
            workspace = await self._one(
                db,
                "SELECT id FROM {t}workspace WHERE scope_id=:scope "
                "AND owner_run_id=:child AND id=:id",
                scope=self._actor.scope_id,
                child=child["id"],
                id=request.child.workspace_id,
            )
            if (
                binding is None
                or workspace is None
                or binding["payload"].get("payload", binding["payload"])["budgetAllocationId"]
                != str(request.child.budget_allocation_id)
            ):
                errors.forbidden("collection budget or workspace binding changed")
            proposed = await self._one(
                db,
                "SELECT * FROM {t}agent_message WHERE scope_id=:scope "
                "AND id=:id AND sender_run_id=:child AND recipient_run_id=:parent "
                "AND task_id=:task "
                "AND kind='result.proposed'",
                scope=self._actor.scope_id,
                id=request.proposal_id,
                child=child["id"],
                parent=parent["id"],
                task=child["task_id"],
            )
            if proposed is None or request.expected_proposal_revision != "1":
                errors.version_conflict("proposal identity or immutable revision changed")
            verification_id = uuid5(NAMESPACE_URL, f"collect:{request.proposal_id}")
            if await self._one(
                db, "SELECT id FROM {t}agent_message WHERE id=:id", id=verification_id
            ):
                errors.version_conflict("proposal has already been verified")
            pending, applied, seq = (
                parent["pending_input_version"],
                parent["applied_input_version"],
                proposed["recipient_seq"],
            )
            cursor = await self._one(
                db,
                "SELECT allocated_seq,applied_seq FROM {t}mailbox_cursor "
                "WHERE scope_id=:scope AND recipient_run_id=:run FOR UPDATE",
                scope=self._actor.scope_id,
                run=parent["id"],
            )
            if (
                cursor is None
                or cursor["allocated_seq"] != pending
                or cursor["applied_seq"] != applied
            ):
                errors.version_conflict("collection mailbox watermarks disagree")
            if not (
                (
                    proposed["status"] == "applied"
                    and seq <= applied <= pending
                    and proposed["applied_checkpoint_id"] is not None
                )
                or (proposed["status"] == "pending" and seq == applied + 1 <= pending)
            ):
                errors.version_conflict("proposal input is not ready for ordered collection")
            proposal = ResultProposed.model_validate(
                proposed["payload"].get("payload", proposed["payload"])
            )
            result = await self._verifier.verify_in_transaction(db, request, proposal)
            if (
                result.child != request.child
                or result.proposal_id != request.proposal_id
                or result.proposal_revision != request.expected_proposal_revision
                or result.acceptance_spec != request.acceptance_spec
            ):
                errors.invalid_input("acceptance verifier returned another proposal identity")
            if any(ref not in proposal.artifact_refs for ref in result.result_refs):
                errors.invalid_input("verifier returned a result outside the proposed artifacts")
            await self._refs(db, parent["session_id"], (*result.evidence_refs, *result.result_refs))
            await self._settle_budget(db, request.child, request.proposal_id, now)
            child_seq = child["pending_input_version"] + 1
            ack = message_adapter.validate_python(
                dict(
                    protocol_revision=1,
                    message_id=verification_id,
                    correlation_id=child["task_id"],
                    causation_id=proposal.result_id,
                    sender_run_id=parent["id"],
                    recipient_run_id=child["id"],
                    task_id=child["task_id"],
                    recipient_seq=str(child_seq),
                    created_at=now,
                    deadline_at=parent["deadline_at"],
                    resource_refs=result.result_refs,
                    trace_id=str(verification_id),
                    kind="result.accepted"
                    if result.disposition == "accepted"
                    else "result.rejected",
                    payload=ResultVerification(
                        result_id=proposal.result_id,
                        verification_ref=str(verification_id),
                        public_reason=result.public_reason,
                    ),
                )
            )
            await message(db, self._schema, scope=self._actor.scope_id, envelope=ack)
            await self._execute(
                db,
                "UPDATE {t}run SET pending_input_version=:seq WHERE id=:id",
                seq=child_seq,
                id=child["id"],
            )
            version = parent["state_version"] + 1
            cp = await checkpoint(
                db,
                self._schema,
                scope=self._actor.scope_id,
                run_id=parent["id"],
                version=version,
                source=parent["checkpoint_id"],
                event_seq=await self._state._session_seq(db, parent["session_id"]),
                payload={
                    "kind": "child_verified",
                    "result": result.model_dump(mode="json", by_alias=True),
                },
            )
            if proposed["status"] == "pending":
                await self._execute(
                    db,
                    "UPDATE {t}agent_message SET status='applied',"
                    "applied_checkpoint_id=:cp WHERE id=:id",
                    cp=cp,
                    id=proposed["id"],
                )
                await self._execute(
                    db,
                    "UPDATE {t}mailbox_cursor SET applied_seq=:seq "
                    "WHERE scope_id=:scope AND recipient_run_id=:id",
                    seq=proposed["recipient_seq"],
                    scope=self._actor.scope_id,
                    id=parent["id"],
                )
                await self._execute(
                    db,
                    "UPDATE {t}run SET applied_input_version=:seq WHERE id=:id",
                    seq=proposed["recipient_seq"],
                    id=parent["id"],
                )
            await self._status(
                db,
                parent,
                version,
                RunStatus.QUEUED,
                self._guard(request.parent_guard, parent),
                now,
                result.public_reason,
            )
            await self._execute(
                db,
                "UPDATE {t}run SET status='queued',phase='queued',state_version=:version,"
                "checkpoint_id=:cp,owner_id=NULL,lease_until=NULL,owner_epoch=owner_epoch+1,phase_started_at=:now,"
                "updated_at=:now WHERE id=:id",
                version=version,
                cp=cp,
                now=now,
                id=parent["id"],
            )
            await self._state._finish_work(
                db, parent, guarded.work["id"], status="completed", now=now
            )
            await self._state._release_slot(db, parent, now)
            await queue_work(
                db,
                self._schema,
                scope=self._actor.scope_id,
                run_id=parent["id"],
                checkpoint_id=cp,
                key=f"child-collected:{parent['id']}:{version}",
                now=now,
                deadline=parent["deadline_at"],
            )
            return result

    async def _settle_budget(
        self, db: AsyncSession, child_ref: ChildRunRef, evidence_id: UUID, now: datetime
    ) -> None:
        policy = self._policies[child_ref.agent]
        parent = await locks.lock_domain_resource(
            db,
            self._schema,
            "budget_allocation",
            self._actor.scope_id,
            str(policy.parent_allocation_id),
        )
        child = await locks.lock_domain_resource(
            db,
            self._schema,
            "budget_allocation",
            self._actor.scope_id,
            str(child_ref.budget_allocation_id),
        )
        if (
            parent is None
            or child is None
            or child["parent_id"] != parent["id"]
            or parent["root_run_id"] != child_ref.parent_run_id
            or child["root_run_id"] != child_ref.parent_run_id
            or child["limits"] != policy.limits
        ):
            errors.forbidden("child budget allocation has another parent")
        operation_key = f"delegate-settle:{child_ref.child_run_id}"
        if await self._one(
            db,
            "SELECT id FROM {t}budget_entry WHERE scope_id=:scope "
            "AND allocation_id=:allocation AND operation_key=:key AND kind='settle'",
            scope=self._actor.scope_id,
            allocation=parent["id"],
            key=operation_key,
        ):
            return
        # Failed/cancelled children may never reach the normal post-model budget
        # read. Rebuild their accounting from terminal audit facts before refunding.
        attempts = (
            (
                await db.execute(
                    scoped(
                        "SELECT status,usage FROM {t}model_attempt "
                        "WHERE scope_id=:scope AND run_id=:run",
                        self._schema,
                    ),
                    {"scope": self._actor.scope_id, "run": child_ref.child_run_id},
                )
            )
            .mappings()
            .all()
        )
        if attempts:
            tokens, unknown = 0, 0
            for attempt in attempts:
                usage = Usage.model_validate(attempt["usage"]) if attempt["usage"] else None
                if (
                    usage is None
                    or usage.status != "complete"
                    or attempt["status"] in {"pending", "running"}
                ):
                    unknown += 1
                if usage is not None:
                    tokens += int(usage.total_tokens or "0")
            tool_count = (
                await db.execute(
                    scoped(
                        "SELECT count(*) FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run",
                        self._schema,
                    ),
                    {"scope": self._actor.scope_id, "run": child_ref.child_run_id},
                )
            ).scalar_one()
            spent = {"model_calls": len(attempts), "tool_calls": tool_count, "tokens": tokens}
            held = {
                key: max(0, int(child["limits"][key]) - spent[key]) if unknown else 0
                for key in ("model_calls", "tokens")
            }
            child = {
                **child,
                "settled": spent,
                "reserved": held,
                "unknown_reserved": held,
                "usage_status": "unknown" if unknown else "complete",
                "unsettled_count": unknown,
            }
            await self._execute(
                db,
                "UPDATE {t}budget_allocation SET settled=CAST(:spent AS jsonb),"
                "reserved=CAST(:held AS jsonb),unknown_reserved=CAST(:held AS jsonb),"
                "usage_status=:status,unsettled_count=:unknown,version=version+1,"
                "updated_at=:now WHERE scope_id=:scope AND id=:id",
                spent=json.dumps(spent),
                held=json.dumps(held),
                status=child["usage_status"],
                unknown=unknown,
                now=now,
                scope=self._actor.scope_id,
                id=child_ref.budget_allocation_id,
            )
        if (
            child["usage_status"] != "complete"
            or child["unsettled_count"]
            or any(child["reserved"].values())
            or any(child["unknown_reserved"].values())
        ):
            return  # Unknown usage remains reserved until a later reconciliation transaction.
        reserved, settled = dict(parent["reserved"]), dict(parent["settled"])
        for key, limit in child["limits"].items():
            actual = child["settled"].get(key, 0)
            if any(
                type(v) is not int or v < 0
                for v in (limit, actual, reserved.get(key), settled.get(key, 0))
            ):
                errors.invalid_input("invalid child budget settlement counters")
            if reserved[key] < limit:
                errors.version_conflict("child budget reservation already released")
            reserved[key] -= limit
            settled[key] = settled.get(key, 0) + actual
        await self._execute(
            db,
            "UPDATE {t}budget_allocation SET reserved=CAST(:reserved AS jsonb),"
            "settled=CAST(:settled AS jsonb),version=version+1,updated_at=:now WHERE id=:id",
            reserved=json.dumps(reserved),
            settled=json.dumps(settled),
            now=now,
            id=parent["id"],
        )
        await self._execute(
            db,
            "INSERT INTO {t}budget_entry "
            "(id,scope_id,allocation_id,operation_key,kind,usage_status,usage_reason,"
            "quantities,reconciliation_ref) "
            "VALUES (:id,:scope,:allocation,:key,'settle','complete','provider_reported',"
            "CAST(:quantities AS jsonb),:ref)",
            id=uuid5(NAMESPACE_URL, operation_key),
            scope=self._actor.scope_id,
            allocation=parent["id"],
            key=operation_key,
            quantities=json.dumps(child["settled"]),
            ref=str(evidence_id),
        )

    async def reconcile_budget(self, child_ref: ChildRunRef) -> bool:
        """Release only audited terminal usage; repeated reconciliation is a no-op."""
        policy = self._policies.get(child_ref.agent)
        if policy is None or child_ref.scope_id != self._actor.scope_id:
            errors.forbidden("unregistered child reconciliation target")
        async with self._sessions() as db, db.begin():
            await locks.lock_slot(db, self._locks, self._actor.scope_id, child_ref.session_id)
            rows = {}
            for identity in sorted((child_ref.parent_run_id, child_ref.child_run_id), key=str):
                rows[identity] = await locks.lock_run(
                    db, self._locks, self._actor.scope_id, identity
                )
            parent, child = rows[child_ref.parent_run_id], rows[child_ref.child_run_id]
            if (
                parent is None
                or child is None
                or parent["session_id"] != child_ref.session_id
                or child["parent_run_id"] != parent["id"]
                or child["root_run_id"] != parent["id"]
                or child["task_id"] != child_ref.task_id
                or child["agent_revision_id"] != policy.agent_revision_id
                or child["status"] not in {"completed", "failed", "cancelled"}
            ):
                errors.forbidden("budget reconciliation requires an owned terminal child")
            await self._resources.authorize_session_in_transaction(
                db, self._actor, child_ref.session_id
            )
            binding = await self._one(
                db,
                "SELECT payload FROM {t}agent_message WHERE scope_id=:scope "
                "AND sender_run_id=:parent AND recipient_run_id=:child "
                "AND task_id=:task AND kind='task.assign'",
                scope=self._actor.scope_id,
                parent=parent["id"],
                child=child["id"],
                task=child["task_id"],
            )
            if binding is None or binding["payload"].get("payload", binding["payload"]).get(
                "budgetAllocationId"
            ) != str(child_ref.budget_allocation_id):
                errors.forbidden("child allocation binding changed")
            for table, terminals in (
                ("model_attempt", "'completed','failed','cancelled','interrupted'"),
                ("tool_call", "'completed','failed','cancelled','not_executed'"),
                ("execution", "'completed','failed','cancelled'"),
            ):
                if await self._one(
                    db,
                    f"SELECT id FROM {{t}}{table} WHERE scope_id=:scope "
                    f"AND run_id=:run AND status NOT IN ({terminals}) LIMIT 1",
                    scope=self._actor.scope_id,
                    run=child["id"],
                ):
                    return False
            await self._settle_budget(db, child_ref, child["checkpoint_id"], datetime.now(UTC))
            return (
                await self._one(
                    db,
                    "SELECT id FROM {t}budget_entry WHERE scope_id=:scope "
                    "AND allocation_id=:allocation AND operation_key=:key AND kind='settle'",
                    scope=self._actor.scope_id,
                    allocation=policy.parent_allocation_id,
                    key=f"delegate-settle:{child['id']}",
                )
                is not None
            )


__all__ = ["SqlSubagentRuntime"]

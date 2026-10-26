"""Plan revision CAS, checkpoint selection and continuation in one transaction."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from uuid import NAMESPACE_URL, UUID, uuid5

from agent_runtime.contracts.decisions import UpdatePlanPayload
from agent_runtime.contracts.payloads import PlanUpdated, RunStatusPayload
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.state_events import (
    MainEventAuthority,
    PlanUpdatedDraft,
    RunStatusDraft,
    StateEventBatch,
)
from agent_runtime.contracts.transactions import CommitGuard
from agent_runtime.contracts.transitions import StateWriteGuard, UpdatedStateRef
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import scoped

if TYPE_CHECKING:
    from agent_runtime.storage.context_access import TransactionalResourceAccess
    from agent_runtime.storage.journal import SqlJournal
    from agent_runtime.storage.uow.state import SqlStateUnitOfWork


async def commit_plan(
    store: SqlStateUnitOfWork,
    *,
    guard: CommitGuard,
    checkpoint_id: UUID,
    plan: UpdatePlanPayload,
    revision_id: UUID,
    journal: SqlJournal,
    resources: TransactionalResourceAccess,
) -> UpdatedStateRef:
    if journal.schema != store.schema:
        errors.invalid_input("plan Journal uses another schema")
    # Revalidate even when a caller constructed its DTO with model_copy/model_construct.
    plan = UpdatePlanPayload.model_validate(plan.model_dump())
    async with store._session() as session, session.begin():
        guarded = await store._lock_guard(session, guard)
        run, now = guarded.run, guarded.now
        if guarded.work["kind"] != "advance_run":
            errors.forbidden("plan commit requires main advance work")
        if int(run["state_version"]) != int(guard.expected_state_version):
            errors.version_conflict("plan run state changed")
        if run["status"] != "running" or run["desired_control"] is not None:
            errors.control_pending("plan commit requires uncontrolled running state")
        if run["deadline_at"] <= now:
            errors.version_conflict("plan run deadline elapsed")
        if run["pending_input_version"] != run["applied_input_version"]:
            errors.version_conflict("new input must be applied before planning")
        version = int(run["state_version"]) + 1
        await store._verify_checkpoint(session, run, checkpoint_id, maximum_state_version=version)
        await resources.authorize_session_in_transaction(session, guard.actor, run["session_id"])
        for step in plan.steps:
            for ref in step.evidence_refs:
                if ref.scope.scope_id != run["scope_id"] or ref.resource_version is None:
                    errors.forbidden("plan evidence must be same-scope and versioned")
                await resources.authorize_resource_in_transaction(
                    session, guard.actor, ref, session_id=run["session_id"]
                )
            for run_id in step.run_refs:
                target = await session.execute(
                    scoped(
                        "SELECT id FROM {t}run WHERE scope_id=:scope AND id=:id "
                        "AND session_id=:session AND task_id=:task",
                        store.schema,
                    ),
                    {
                        "scope": run["scope_id"],
                        "id": run_id,
                        "session": run["session_id"],
                        "task": run["task_id"],
                    },
                )
                if target.scalar_one_or_none() is None:
                    errors.forbidden("plan run reference is outside the current task")

        params = {
            "scope": run["scope_id"],
            "plan": plan.plan_id,
            "task": run["task_id"],
            "expected": int(plan.expected_revision),
            "now": now,
        }
        if int(plan.expected_revision) == 0:
            await session.execute(
                scoped(
                    "INSERT INTO {t}plan (id,scope_id,task_id,current_revision) "
                    "VALUES (:plan,:scope,:task,0) ON CONFLICT DO NOTHING",
                    store.schema,
                ),
                params,
            )
        stored = (
            (
                await session.execute(
                    scoped(
                        "SELECT task_id,current_revision FROM {t}plan WHERE scope_id=:scope "
                        "AND id=:plan",
                        store.schema,
                    ),
                    params,
                )
            )
            .mappings()
            .one_or_none()
        )
        if stored is None:
            errors.version_conflict("plan does not exist or task already has another plan")
        if stored["task_id"] != run["task_id"]:
            errors.forbidden("plan belongs to another task")
        if int(stored["current_revision"]) != int(plan.expected_revision):
            errors.version_conflict("plan revision changed")
        revision = int(plan.expected_revision) + 1
        reason = "Plan updated."
        # UPDATE locks and rechecks the version after any concurrent writer commits.
        changed = await session.execute(
            scoped(
                "UPDATE {t}plan SET current_revision=:revision,updated_at=:now "
                "WHERE scope_id=:scope AND id=:plan AND task_id=:task "
                "AND current_revision=:expected RETURNING id",
                store.schema,
            ),
            {**params, "revision": revision},
        )
        if changed.scalar_one_or_none() is None:
            errors.version_conflict("plan compare-and-swap lost")
        inserted = await session.execute(
            scoped(
                "INSERT INTO {t}plan_revision (id,scope_id,plan_id,revision,steps,public_reason) "
                "VALUES (:id,:scope,:plan,:revision,CAST(:steps AS jsonb),:reason) "
                "ON CONFLICT DO NOTHING RETURNING id",
                store.schema,
            ),
            {
                **params,
                "id": revision_id,
                "revision": revision,
                "reason": reason,
                "steps": json.dumps([s.model_dump(mode="json", by_alias=True) for s in plan.steps]),
            },
        )
        if inserted.scalar_one_or_none() is None:
            errors.version_conflict("immutable plan revision already exists")
        common = {
            "occurred_at": now,
            "turn_id": run["turn_id"],
            "run_id": run["id"],
            "parent_run_id": run["parent_run_id"],
            "step_id": None,
            "item_id": None,
            "model_attempt_id": None,
        }
        await journal.append_state_in_transaction(
            session,
            StateEventBatch(
                authority=MainEventAuthority(
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
                ),
                session_id=run["session_id"],
                trace_id=str(guard.lease.work_id),
                events=(
                    PlanUpdatedDraft(
                        **common,
                        kind="plan.updated",
                        producer_key=f"plan-updated:{plan.plan_id}:{revision}",
                        payload=PlanUpdated(
                            plan_id=plan.plan_id,
                            revision=str(revision),
                            steps=plan.steps,
                            public_reason=reason,
                        ),
                    ),
                    RunStatusDraft(
                        **common,
                        kind="run.status",
                        producer_key=f"run-status:{run['id']}:{version}",
                        payload=RunStatusPayload(
                            state_version=str(version),
                            status=RunStatus.QUEUED,
                            phase="queued",
                            phase_started_at=now,
                            public_reason=reason,
                        ),
                    ),
                ),
            ),
        )
        await session.execute(
            scoped(
                "UPDATE {t}run SET status='queued',phase='queued',phase_started_at=:now, "
                "state_version=:version,checkpoint_id=:checkpoint,owner_id=NULL,lease_until=NULL, "
                "owner_epoch=owner_epoch+1,updated_at=:now WHERE scope_id=:scope AND id=:run",
                store.schema,
            ),
            {
                "scope": run["scope_id"],
                "run": run["id"],
                "now": now,
                "version": version,
                "checkpoint": checkpoint_id,
            },
        )
        await store._finish_work(session, run, guarded.work["id"], status="completed", now=now)
        key = f"plan:{run['id']}:state:{version}"
        await session.execute(
            scoped(
                "INSERT INTO {t}work_item (id,scope_id,run_id,subject_ref,kind,dedup_key,status,"
                "payload_ref,wake_reason,priority,lease_epoch,attempt_count,"
                "available_at,deadline_at) "
                "VALUES (:id,:scope,:run,CAST(:subject AS jsonb),'advance_run',:key,'pending',"
                ":payload,'recovery',100,0,0,:now,:deadline)",
                store.schema,
            ),
            {
                "id": uuid5(NAMESPACE_URL, key),
                "scope": run["scope_id"],
                "run": run["id"],
                "key": key,
                "subject": json.dumps(
                    {"kind": "plan", "planId": str(plan.plan_id), "revision": str(revision)}
                ),
                "payload": f"checkpoint:{checkpoint_id}",
                "now": now,
                "deadline": run["deadline_at"],
            },
        )
        await store._release_slot(session, run, now)
        return store._updated_ref(
            run,
            state_version=version,
            status="queued",
            checkpoint_id=checkpoint_id,
            committed_seq=await store._session_seq(session, run["session_id"]),
        )

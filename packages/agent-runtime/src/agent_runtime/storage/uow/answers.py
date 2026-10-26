"""Answer state transitions using the standard UoW lock/authority boundary."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from agent_runtime.contracts.payloads import (
    ChannelDigest,
    ChannelIndex,
    ItemCompleted,
    RunCompleted,
    RunStatusPayload,
)
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.state_events import (
    ItemCompletedDraft,
    MainEventAuthority,
    RunCompletedDraft,
    RunStatusDraft,
    StateEventBatch,
    StateEventDraft,
)
from agent_runtime.contracts.transactions import CommitGuard
from agent_runtime.contracts.transitions import StateWriteGuard, UpdatedStateRef
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import scoped
from agent_runtime.terminal_guidance import terminal_guidance

_OUTPUT_CHANNELS: tuple[Literal["content", "analysis"], ...] = ("content", "analysis")

if TYPE_CHECKING:
    from agent_runtime.storage.context_access import TransactionalResourceAccess
    from agent_runtime.storage.journal import SqlJournal
    from agent_runtime.storage.uow.state import SqlStateUnitOfWork


async def commit_answer(
    store: SqlStateUnitOfWork,
    *,
    guard: CommitGuard,
    checkpoint_id: UUID,
    item_ids: tuple[UUID, ...],
    outcome: Literal["success", "partial"] | None,
    evidence_refs: tuple[ResourceRef, ...],
    journal: SqlJournal,
    resources: TransactionalResourceAccess,
) -> UpdatedStateRef:
    if journal.schema != store.schema:
        errors.invalid_input("answer Journal uses another schema")
    if not item_ids:
        errors.invalid_input("an answer requires nonempty final items")
    if len(set(item_ids)) != len(item_ids):
        errors.invalid_input("an answer contains duplicate final items")
    if outcome is None and len(item_ids) > 63:
        errors.invalid_input("a native answer exceeds the 63 final item limit")
    async with store._session() as session, session.begin():
        guarded = await store._lock_guard(session, guard)
        run, now = guarded.run, guarded.now
        if guarded.work["kind"] != "advance_run":
            errors.forbidden("answer commit requires main advance work")
        if int(run["state_version"]) != int(guard.expected_state_version):
            errors.version_conflict("answer state version changed")
        if run["status"] != "running" or run["desired_control"] is not None:
            errors.control_pending("answer requires uncontrolled running state")
        if run["deadline_at"] <= now:
            errors.version_conflict("answer run deadline elapsed")
        if run["pending_input_version"] != run["applied_input_version"]:
            errors.version_conflict("new user input must be applied before answering")
        version = int(run["state_version"]) + 1
        await store._verify_checkpoint(session, run, checkpoint_id, maximum_state_version=version)
        await resources.authorize_session_in_transaction(session, guard.actor, run["session_id"])
        for ref in evidence_refs:
            if ref.scope.scope_id != run["scope_id"] or ref.resource_version is None:
                errors.forbidden("answer evidence must be same-scope and versioned")
            await resources.authorize_resource_in_transaction(
                session, guard.actor, ref, session_id=run["session_id"]
            )
        params = {"scope": run["scope_id"], "run": run["id"]}

        if outcome is None:
            other_work = await session.execute(
                scoped(
                    "SELECT id FROM {t}work_item WHERE scope_id=:scope AND run_id=:run "
                    "AND kind='advance_run' AND status IN ('pending','leased') "
                    "AND id<>:work LIMIT 1",
                    store.schema,
                ),
                {**params, "work": guard.lease.work_id},
            )
            if other_work.scalar_one_or_none() is not None:
                errors.version_conflict("run already has another live continuation")

        async def rows(sql: str) -> list[dict[str, Any]]:
            result = await session.execute(scoped(sql, store.schema), params)
            return [dict(row) for row in result.mappings()]

        seals = await rows(
            "SELECT s.*, a.status AS attempt_status FROM {t}output_seal s "
            "JOIN {t}model_attempt a ON a.scope_id=s.scope_id AND a.id=s.model_attempt_id "
            "WHERE s.scope_id=:scope AND s.run_id=:run"
        )
        selected: dict[UUID, dict[str, Any]] = {}
        for seal in seals:
            if seal["item_id"] in item_ids:
                if seal["item_id"] in selected:
                    errors.invalid_input("answer item has ambiguous producers")
                selected[seal["item_id"]] = seal
        if set(selected) != set(item_ids):
            errors.invalid_input("answer item is not sealed in this run")
        if outcome is not None:
            if await rows(
                "SELECT id FROM {t}run WHERE scope_id=:scope AND parent_run_id=:run "
                "AND status NOT IN ('completed','failed','cancelled')"
            ):
                errors.version_conflict("a delegated child is still active")
            if await rows(
                "SELECT p.id FROM {t}agent_message p WHERE p.scope_id=:scope "
                "AND p.recipient_run_id=:run AND p.kind='result.proposed' AND NOT EXISTS "
                "(SELECT 1 FROM {t}agent_message v WHERE v.scope_id=p.scope_id "
                "AND v.sender_run_id=:run AND v.recipient_run_id=p.sender_run_id "
                "AND v.causation_id=p.id AND v.kind IN ('result.accepted','result.rejected'))"
            ):
                errors.version_conflict("a child result has not been independently verified")
            if await rows(
                "SELECT id FROM {t}model_attempt WHERE scope_id=:scope AND run_id=:run "
                "AND status NOT IN ('completed','failed','interrupted','cancelled')"
            ):
                errors.version_conflict("a model attempt is still active")
            if await rows(
                "SELECT id FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run "
                "AND status NOT IN ('completed','failed','cancelled','not_executed')"
            ):
                errors.version_conflict("a tool call is not reconciled")
            if await rows(
                "SELECT id FROM {t}execution WHERE scope_id=:scope AND run_id=:run "
                "AND status NOT IN ('completed','failed','cancelled')"
            ):
                errors.version_conflict("execution processes are not confirmed stopped")
            if await rows(
                "SELECT e.id FROM {t}event e WHERE e.scope_id=:scope AND e.run_id=:run "
                "AND e.kind IN ('assistant.delta','analysis.delta') AND NOT EXISTS "
                "(SELECT 1 FROM {t}output_seal s WHERE s.scope_id=e.scope_id "
                "AND s.run_id=e.run_id AND s.model_attempt_id=e.model_attempt_id "
                "AND s.item_id=e.item_id)"
            ):
                errors.version_conflict("output has not finished flushing")

        events: list[StateEventDraft] = []
        for item_id in item_ids:
            seal = selected[item_id]
            result = await session.execute(
                scoped(
                    "SELECT * FROM {t}timeline_item WHERE scope_id=:scope AND id=:item",
                    store.schema,
                ),
                {"scope": run["scope_id"], "item": item_id},
            )
            item = result.mappings().one_or_none()
            if (
                item is None
                or item["run_id"] != run["id"]
                or item["session_id"] != run["session_id"]
                or item["item_type"] != "message"
                or item["status"] not in {"streaming", "completed"}
                or (
                    int(item["final_revision"]) != int(seal["sealed_revision"])
                    and not (
                        outcome is None
                        and item["status"] == "streaming"
                        and int(item["final_revision"]) == 0
                    )
                )
                or seal["attempt_status"] != "completed"
                or seal["content_last_delta_index"] is None
            ):
                errors.invalid_input("answer item is incomplete or has another owner")
            # A seal proves durability, but does not prove nonempty user-visible content.
            text = await session.execute(
                scoped(
                    "SELECT 1 FROM {t}event WHERE scope_id=:scope AND run_id=:run "
                    "AND item_id=:item AND model_attempt_id=:attempt "
                    "AND kind='assistant.delta' AND length(btrim(payload->>'text')) > 0 LIMIT 1",
                    store.schema,
                ),
                {**params, "item": item_id, "attempt": seal["model_attempt_id"]},
            )
            if text.scalar_one_or_none() is None:
                errors.invalid_input("answer has no visible content")
            if outcome is None:
                events.append(
                    ItemCompletedDraft(
                        kind="item.completed",
                        producer_key=f"answer-completed:{item_id}:{seal['sealed_revision']}",
                        occurred_at=now,
                        turn_id=run["turn_id"],
                        run_id=run["id"],
                        parent_run_id=run["parent_run_id"],
                        step_id=None,
                        item_id=item_id,
                        model_attempt_id=seal["model_attempt_id"],
                        payload=ItemCompleted(
                            item_revision=str(seal["sealed_revision"]),
                            status="completed",
                            channel_digests=tuple(
                                ChannelDigest(channel=channel, digest=seal[f"{channel}_digest"])
                                for channel in _OUTPUT_CHANNELS
                            ),
                            last_delta_indexes=tuple(
                                ChannelIndex(
                                    channel=channel,
                                    last_delta_index=(
                                        None
                                        if seal[f"{channel}_last_delta_index"] is None
                                        else str(seal[f"{channel}_last_delta_index"])
                                    ),
                                )
                                for channel in _OUTPUT_CHANNELS
                            ),
                        ),
                    )
                )
            elif item["status"] != "completed":
                errors.invalid_input("final answer has not been published as completed")
            else:
                published = await session.execute(
                    scoped(
                        "SELECT id FROM {t}event WHERE scope_id=:scope AND run_id=:run "
                        "AND item_id=:item AND model_attempt_id=:attempt "
                        "AND kind='item.completed' AND payload->>'status'='completed' "
                        "AND payload->>'itemRevision'=:revision LIMIT 1",
                        store.schema,
                    ),
                    {
                        **params,
                        "item": item_id,
                        "attempt": seal["model_attempt_id"],
                        "revision": str(seal["sealed_revision"]),
                    },
                )
                if published.scalar_one_or_none() is None:
                    errors.invalid_input("final answer has no durable completion event")
        status = RunStatus.COMPLETED if outcome is not None else RunStatus.QUEUED
        guidance = (
            terminal_guidance("partial", retained_count=len(item_ids))
            if outcome == "partial"
            else None
        )
        phase: Literal["terminal", "queued"] = "terminal" if outcome is not None else "queued"
        events.append(
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
                    public_reason=(
                        guidance.message
                        if guidance is not None
                        else "Task completed."
                        if outcome is not None
                        else "Answer published."
                    ),
                ),
            )
        )
        if outcome is not None:
            events.append(
                RunCompletedDraft(
                    kind="run.completed",
                    producer_key=f"run-completed:{run['id']}:{version}",
                    occurred_at=now,
                    turn_id=run["turn_id"],
                    run_id=run["id"],
                    parent_run_id=run["parent_run_id"],
                    step_id=None,
                    item_id=None,
                    model_attempt_id=None,
                    payload=RunCompleted(
                        state_version=str(version),
                        final_item_ids=item_ids,
                        partial_result_refs=evidence_refs if outcome == "partial" else (),
                        outcome=outcome,
                        failure=None,
                        public_message=guidance.message if guidance else "Task completed.",
                        next_steps=guidance.next_steps if guidance else (),
                    ),
                )
            )
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
                events=tuple(events),
            ),
        )
        for item_id in item_ids:
            await session.execute(
                scoped(
                    "UPDATE {t}timeline_item SET status='completed', final_revision=:revision, "
                    "updated_at=:now "
                    "WHERE scope_id=:scope AND id=:item",
                    store.schema,
                ),
                {
                    "scope": run["scope_id"],
                    "item": item_id,
                    "now": now,
                    "revision": selected[item_id]["sealed_revision"],
                },
            )
        await session.execute(
            scoped(
                "UPDATE {t}run SET status=:status, phase=:phase, phase_started_at=:now, "
                "state_version=:version, checkpoint_id=:checkpoint, outcome=:outcome, "
                "owner_id=NULL, lease_until=NULL, owner_epoch=owner_epoch+1, updated_at=:now "
                "WHERE scope_id=:scope AND id=:run",
                store.schema,
            ),
            {
                **params,
                "status": status.value,
                "phase": phase,
                "now": now,
                "version": version,
                "checkpoint": checkpoint_id,
                "outcome": outcome,
            },
        )
        await store._finish_work(session, run, guarded.work["id"], status="completed", now=now)
        if outcome is None:
            key = f"answer:{run['id']}:state:{version}"
            await session.execute(
                scoped(
                    "INSERT INTO {t}work_item (id,scope_id,run_id,subject_ref,kind,"
                    "dedup_key,status,payload_ref,wake_reason,priority,lease_epoch,"
                    "attempt_count,available_at,deadline_at) "
                    "VALUES (:id,:scope,:run,CAST(:subject AS jsonb),'advance_run',:key,'pending',"
                    ":payload,'recovery',100,0,0,:now,:deadline)",
                    store.schema,
                ),
                {
                    **params,
                    "id": uuid5(NAMESPACE_URL, key),
                    "key": key,
                    "subject": json.dumps(
                        {"kind": "answer", "itemIds": [str(i) for i in item_ids]}
                    ),
                    "payload": f"checkpoint:{checkpoint_id}",
                    "now": now,
                    "deadline": run["deadline_at"],
                },
            )
        else:
            await store._finish_advance_works(session, run, status="cancelled", now=now)
            turn_write = await session.execute(
                scoped(
                    "UPDATE {t}turn SET outcome_run_id=:run, outcome=:outcome, updated_at=:now "
                    "WHERE scope_id=:scope AND id=:turn AND primary_run_id=:run "
                    "AND outcome_run_id IS NULL RETURNING id",
                    store.schema,
                ),
                {**params, "turn": run["turn_id"], "outcome": outcome, "now": now},
            )
            if run["parent_run_id"] is None and turn_write.scalar_one_or_none() is None:
                errors.version_conflict("primary turn outcome changed before completion")
        await store._release_slot(session, run, now)
        return store._updated_ref(
            run,
            state_version=version,
            status=status.value,
            checkpoint_id=checkpoint_id,
            committed_seq=await store._session_seq(session, run["session_id"]),
        )

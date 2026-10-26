"""Transaction-local recovery of delegated Runs, without worker authority upgrades."""

import hashlib
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from agent_runtime.contracts.canonical import canonical_bytes, content_digest
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.message_payloads import TaskFailed
from agent_runtime.contracts.messages import message_adapter
from agent_runtime.contracts.payloads import (
    ChannelDigest,
    ChannelIndex,
    CommandUpdated,
    ItemCompleted,
    ItemStarted,
    RunCancelled,
    RunStatusPayload,
)
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.state_events import (
    CommandUpdatedDraft,
    EventAuthority,
    ItemCompletedDraft,
    RunCancelledDraft,
    RunStatusDraft,
    ServiceEventAuthority,
    StateEventBatch,
    StateEventDraft,
)
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.delegation_sql import checkpoint, message, queue_work
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.terminal_failure import finalize_root_failure
from agent_runtime.storage.uow import locks
from agent_runtime.storage.uow.schema import scoped


async def lock_recovery_runs(
    db: AsyncSession, schema: str, sql: locks.LockStatements, scope: str, run_id: UUID
) -> tuple[Mapping[str, Any] | None, Mapping[str, Any] | None, Mapping[str, Any] | None]:
    identity = (
        await db.execute(
            scoped(
                "SELECT session_id,parent_run_id FROM {t}run WHERE scope_id=:scope AND id=:id",
                schema,
            ),
            {"scope": scope, "id": run_id},
        )
    ).one_or_none()
    if identity is None:
        return None, None, None
    slot = await locks.lock_slot(db, sql, scope, identity.session_id)
    ids = {run_id}
    if identity.parent_run_id is not None:
        ids.add(identity.parent_run_id)
    rows = {key: await locks.lock_run(db, sql, scope, key) for key in sorted(ids, key=str)}
    return slot, rows[run_id], rows.get(identity.parent_run_id)


async def unresolved_child_effects(
    db: AsyncSession,
    schema: str,
    run: Mapping[str, Any],
    *,
    cancelling: bool = False,
) -> bool:
    # Stopped execution is cancellation evidence, not a recovered effect/result.
    # Bind it to the completed supervisor's current generation; bare terminal
    # execution rows and old-generation work cannot discharge an unknown effect.
    return bool(
        (
            await db.execute(
                scoped(
                    "SELECT EXISTS(SELECT 1 FROM {t}execution "
                    "WHERE scope_id=:scope AND run_id=:run "
                    "AND status NOT IN ('completed','failed','cancelled')) "
                    "OR EXISTS(SELECT 1 FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run "
                    "AND status='running') OR EXISTS(SELECT 1 FROM {t}operation o "
                    "JOIN {t}tool_call t "
                    "ON t.scope_id=o.scope_id AND t.operation_id=o.id "
                    "WHERE t.scope_id=:scope AND t.run_id=:run "
                    "AND o.status IN ('prepared','dispatching','unknown') "
                    "AND NOT (:cancelling AND o.status='unknown' "
                    "AND COALESCE(o.failure->>'category','')='reconciled_external_effect' "
                    "AND EXISTS(SELECT 1 FROM {t}execution e JOIN {t}work_item w "
                    "ON w.scope_id=e.scope_id AND w.id=e.supervisor_work_id "
                    "WHERE e.scope_id=t.scope_id AND e.run_id=t.run_id "
                    "AND e.operation_id=o.id AND e.failure IS NULL "
                    "AND ((e.status='completed' AND o.failure->>'remoteStatus'='completed') "
                    "OR (e.status='cancelled' AND o.failure->>'remoteStatus'='stopped')) "
                    "AND w.run_id=e.run_id AND w.kind='reconcile_execution' "
                    "AND w.status='completed' AND w.owner_id IS NULL AND w.lease_until IS NULL "
                    "AND w.subject_ref->>'resourceType'='execution' "
                    "AND w.subject_ref->>'resourceId'=CAST(e.id AS text) "
                    "AND w.subject_ref->>'resourceVersion'=CAST(e.generation AS text) "
                    "AND w.subject_ref->'scope'->>'scopeId'=e.scope_id)))",
                    schema,
                ),
                {"scope": run["scope_id"], "run": run["id"], "cancelling": cancelling},
            )
        ).scalar_one()
    )


async def finalize_child_failure(
    db: AsyncSession,
    engine: AsyncEngine,
    schema: str,
    child: Mapping[str, Any],
    parent: Mapping[str, Any],
    failure: FailureEnvelope,
    now: datetime,
) -> FailureEnvelope:
    """Caller holds Slot -> sorted parent/child Runs -> Work; all writes stay atomic."""
    # The shared projector also seals interrupted output and retains recovery guidance.
    # Its Turn update is guarded by primary_run_id, so a child cannot replace its parent.
    failure = await finalize_root_failure(db, engine, schema, child, failure, now)
    await notify_parent(db, engine, schema, child, parent, failure, now)
    return failure


async def notify_parent(
    db: AsyncSession,
    engine: AsyncEngine,
    schema: str,
    child: Mapping[str, Any],
    parent: Mapping[str, Any],
    failure: FailureEnvelope,
    now: datetime,
) -> None:
    session_row = (
        await db.execute(
            scoped(
                "SELECT owner_actor_id,auth_revision,event_seq FROM {t}session "
                "WHERE scope_id=:scope AND id=:session FOR UPDATE",
                schema,
            ),
            {"scope": child["scope_id"], "session": child["session_id"]},
        )
    ).one()
    actor = ActorContext(
        scope_id=child["scope_id"],
        actor_id=session_row.owner_actor_id,
        auth_revision=str(session_row.auth_revision),
        grants_ref="recovery:terminal",
    )
    version = int(child["state_version"]) + 1
    identity = uuid5(NAMESPACE_URL, f"child-recovered-failure:{child['id']}:{version}")
    seq = int(parent["pending_input_version"]) + 1
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
            deadline_at=max(parent["deadline_at"], now + timedelta(days=1)),
            resource_refs=(),
            trace_id=str(identity),
            kind="task.failed",
            payload=TaskFailed(failure=failure, partial_result_refs=failure.partial_result_refs),
        )
    )
    await message(
        db,
        schema,
        scope=child["scope_id"],
        envelope=envelope,
        status="pending"
        if parent["deadline_at"] > now
        and parent["status"] not in {"completed", "failed", "cancelled"}
        else "expired",
    )
    await db.execute(
        scoped(
            "UPDATE {t}run SET pending_input_version=:seq WHERE scope_id=:scope AND id=:id", schema
        ),
        {"seq": seq, "scope": child["scope_id"], "id": parent["id"]},
    )
    spec = parent["waiting_spec"]
    if not (
        parent["status"] == "waiting_user"
        and parent["desired_control"] is None
        and parent["owner_id"] is None
        and parent["deadline_at"] > now
        and isinstance(spec, dict)
        and spec.get("kind") == "message"
        and spec.get("subjectId") == str(parent["id"])
        and datetime.fromisoformat(spec["deadlineAt"]) > now
    ):
        return
    next_version = int(parent["state_version"]) + 1
    cp = await checkpoint(
        db,
        schema,
        scope=child["scope_id"],
        run_id=parent["id"],
        version=next_version,
        source=parent["checkpoint_id"],
        payload={"kind": "child_result_ready", "proposalId": str(identity)},
        event_seq=session_row.event_seq,
    )
    await SqlJournal(engine, schema, actor, clock=lambda: now).append_state_in_transaction(
        db,
        StateEventBatch(
            authority=ServiceEventAuthority(
                kind="service",
                actor=actor,
                purpose="recovery",
                transaction_id=identity,
                source=ResourceRef(
                    resource_type="run",
                    resource_id=str(parent["id"]),
                    resource_version=str(parent["state_version"]),
                    scope=ResourceScope(
                        scope_id=actor.scope_id, session_id=str(parent["session_id"])
                    ),
                ),
            ),
            session_id=parent["session_id"],
            trace_id=str(identity),
            events=(
                RunStatusDraft(
                    kind="run.status",
                    producer_key=f"run-status:{parent['id']}:{next_version}",
                    occurred_at=now,
                    turn_id=parent["turn_id"],
                    run_id=parent["id"],
                    parent_run_id=parent["parent_run_id"],
                    step_id=None,
                    item_id=None,
                    model_attempt_id=None,
                    payload=RunStatusPayload(
                        state_version=str(next_version),
                        status=RunStatus.QUEUED,
                        phase="queued",
                        phase_started_at=now,
                        public_reason="The delegated task failed.",
                    ),
                ),
            ),
        ),
    )
    await db.execute(
        scoped(
            "UPDATE {t}run SET status='queued',phase='queued',waiting_spec=NULL,"
            "state_version=:version,checkpoint_id=:cp,wake_generation=wake_generation+1,"
            "phase_started_at=:now,updated_at=:now "
            "WHERE scope_id=:scope AND id=:id",
            schema,
        ),
        {
            "version": next_version,
            "cp": cp,
            "now": now,
            "scope": actor.scope_id,
            "id": parent["id"],
        },
    )
    await queue_work(
        db,
        schema,
        scope=actor.scope_id,
        run_id=parent["id"],
        checkpoint_id=cp,
        key=f"child-result:{parent['id']}:{next_version}",
        now=now,
        deadline=parent["deadline_at"],
    )


async def cancel_child_in_transaction(
    db: AsyncSession,
    engine: AsyncEngine,
    schema: str,
    child: Mapping[str, Any],
    parent: Mapping[str, Any],
    now: datetime,
) -> bool:
    """Stop intent for live owners; service cancellation only after their fence is lost.

    Caller locks the Slot, both Runs (sorted), and every live child work before Session.
    Unknown effects remain supervised and budget reservations are never refunded here.
    """
    params = {"scope": child["scope_id"], "run": child["id"], "now": now}
    session_row = (
        await db.execute(
            scoped(
                "SELECT owner_actor_id,auth_revision,event_seq FROM {t}session "
                "WHERE scope_id=:scope AND id=:session FOR UPDATE",
                schema,
            ),
            {**params, "session": child["session_id"]},
        )
    ).one()
    actor = ActorContext(
        scope_id=child["scope_id"],
        actor_id=session_row.owner_actor_id,
        auth_revision=str(session_row.auth_revision),
        grants_ref="recovery:terminal",
    )
    journal = SqlJournal(engine, schema, actor, clock=lambda: now)
    authority = ServiceEventAuthority(
        kind="service",
        actor=actor,
        purpose="recovery",
        transaction_id=uuid5(NAMESPACE_URL, f"child-cancel:{child['id']}:{child['state_version']}"),
        source=ResourceRef(
            resource_type="run",
            resource_id=str(child["id"]),
            resource_version=str(child["state_version"]),
            scope=ResourceScope(scope_id=actor.scope_id, session_id=str(child["session_id"])),
        ),
    )
    common = dict(
        occurred_at=now,
        turn_id=child["turn_id"],
        run_id=child["id"],
        parent_run_id=parent["id"],
        step_id=None,
        item_id=None,
        model_attempt_id=None,
    )
    child = dict(child)
    command = child["control_command_id"]
    if child["desired_control"] != "cancel":
        old_command = command
        command = uuid5(
            NAMESPACE_URL, f"child-cancel-command:{child['id']}:{child['state_version']}"
        )
        await db.execute(
            scoped(
                "INSERT INTO {t}command (id,scope_id,run_id,actor_id,request_key,request_hash,kind,"
                "status,expected_state_version) VALUES (:id,:scope,:run,:actor,:key,:hash,'cancel',"
                "'accepted',:version)",
                schema,
            ),
            {
                **params,
                "id": command,
                "actor": actor.actor_id,
                "key": f"recovery:{command}",
                "hash": content_digest({"child": str(child["id"]), "parent": str(parent["id"])}),
                "version": child["state_version"],
            },
        )
        events: list[Any] = []
        if old_command is not None:
            await db.execute(
                scoped(
                    "UPDATE {t}command SET status='rejected',updated_at=:now "
                    "WHERE scope_id=:scope AND id=:id AND status='accepted'",
                    schema,
                ),
                {**params, "id": old_command},
            )
            events.append(
                CommandUpdatedDraft(
                    kind="command.updated",
                    **common,
                    producer_key=f"child-superseded:{old_command}",
                    payload=CommandUpdated(
                        command_id=old_command,
                        status="rejected",
                        public_reason="Parent cancellation superseded this control.",
                    ),
                )
            )
        version = child["state_version"] + 1
        cp = await checkpoint(
            db,
            schema,
            scope=actor.scope_id,
            run_id=child["id"],
            version=version,
            source=child["checkpoint_id"],
            event_seq=session_row.event_seq,
            payload={"kind": "child_cancel_requested", "parentId": str(parent["id"])},
        )
        events.extend(
            (
                CommandUpdatedDraft(
                    kind="command.updated",
                    **common,
                    producer_key=f"child-cancel-accepted:{command}",
                    payload=CommandUpdated(
                        command_id=command,
                        status="accepted",
                        public_reason="Parent requested cancellation.",
                    ),
                ),
                RunStatusDraft(
                    kind="run.status",
                    **common,
                    producer_key=f"run-status:{child['id']}:{version}",
                    payload=RunStatusPayload(
                        state_version=str(version),
                        status=RunStatus.CANCELLING,
                        phase="cancelling",
                        phase_started_at=now,
                        public_reason="Stopping the delegated task.",
                    ),
                ),
            )
        )
        await append_cancel_events(db, journal, authority, child, tuple(events))
        await db.execute(
            scoped(
                "UPDATE {t}run SET status='cancelling',phase='cancelling',desired_control='cancel',"
                "control_command_id=:command,cancel_version=cancel_version+1,state_version=:version,"
                "checkpoint_id=:cp,updated_at=:now,phase_started_at=:now "
                "WHERE scope_id=:scope AND id=:run",
                schema,
            ),
            {**params, "command": command, "version": version, "cp": cp},
        )
        child.update(
            state_version=version,
            checkpoint_id=cp,
            desired_control="cancel",
            control_command_id=command,
            status="cancelling",
        )
    if (
        child["owner_id"] is not None
        and child["lease_until"] is not None
        and child["lease_until"] > now
    ):
        return False
    # An expired/no owner can no longer publish. Interrupted inference cost remains unknown.
    await db.execute(
        scoped(
            "UPDATE {t}model_attempt SET status='interrupted',finish_reason='cancelled',"
            "finished_at=:now,"
            "usage_status='unknown',usage_reason='collection_failed',usage=CAST(:usage AS jsonb) "
            "WHERE scope_id=:scope AND run_id=:run AND status IN ('pending','running')",
            schema,
        ),
        {
            **params,
            "usage": '{"status":"unknown","reason":"collection_failed",'
            '"inputTokens":null,"outputTokens":null,"totalTokens":null}',
        },
    )
    if await unresolved_child_effects(db, schema, child, cancelling=True):
        await db.execute(
            scoped(
                "UPDATE {t}execution SET cleanup_status='pending',updated_at=:now "
                "WHERE scope_id=:scope AND run_id=:run "
                "AND status NOT IN ('completed','failed','cancelled')",
                schema,
            ),
            params,
        )
        await db.execute(
            scoped("UPDATE {t}run SET updated_at=:now WHERE scope_id=:scope AND id=:run", schema),
            params,
        )
        return False
    partial = await seal_child_output(
        db, schema, journal, authority, child, now, status="cancelled"
    )
    version = child["state_version"] + 1
    cp = await checkpoint(
        db,
        schema,
        scope=actor.scope_id,
        run_id=child["id"],
        version=version,
        source=child["checkpoint_id"],
        event_seq=(
            await db.execute(
                scoped(
                    "SELECT event_seq FROM {t}session WHERE scope_id=:scope AND id=:session", schema
                ),
                {**params, "session": child["session_id"]},
            )
        ).scalar_one(),
        payload={"kind": "child_cancelled", "parentId": str(parent["id"])},
    )
    await append_cancel_events(
        db,
        journal,
        authority,
        child,
        (
            RunStatusDraft(
                kind="run.status",
                **common,
                producer_key=f"run-status:{child['id']}:{version}",
                payload=RunStatusPayload(
                    state_version=str(version),
                    status=RunStatus.CANCELLED,
                    phase="terminal",
                    phase_started_at=now,
                    public_reason="The delegated task was cancelled.",
                ),
            ),
            RunCancelledDraft(
                kind="run.cancelled",
                **common,
                producer_key=f"child-cancelled:{child['id']}:{version}",
                payload=RunCancelled(
                    state_version=str(version),
                    final_item_ids=(),
                    partial_result_refs=partial,
                    outcome=None,
                    failure=None,
                ),
            ),
            CommandUpdatedDraft(
                kind="command.updated",
                **common,
                producer_key=f"child-cancel-applied:{command}",
                payload=CommandUpdated(
                    command_id=command,
                    status="applied",
                    public_reason="The delegated task stopped.",
                ),
            ),
        ),
    )
    await db.execute(
        scoped(
            "UPDATE {t}command SET status='applied',result_ref=:ref,updated_at=:now "
            "WHERE scope_id=:scope AND id=:id AND status='accepted'",
            schema,
        ),
        {**params, "id": command, "ref": str(cp)},
    )
    await db.execute(
        scoped(
            "UPDATE {t}run SET status='cancelled',phase='terminal',waiting_spec=NULL,"
            "desired_control=NULL,"
            "control_command_id=NULL,owner_id=NULL,lease_until=NULL,owner_epoch=owner_epoch+1,"
            "state_version=:version,checkpoint_id=:cp,failure=NULL,"
            "phase_started_at=:now,updated_at=:now "
            "WHERE scope_id=:scope AND id=:run",
            schema,
        ),
        {**params, "version": version, "cp": cp},
    )
    await db.execute(
        scoped(
            "UPDATE {t}work_item SET status='cancelled',owner_id=NULL,lease_until=NULL,"
            "lease_epoch=lease_epoch+1,updated_at=:now WHERE scope_id=:scope AND run_id=:run "
            "AND status IN ('pending','leased') "
            "AND (kind='advance_run' OR payload_ref='cancel-convergence')",
            schema,
        ),
        params,
    )
    await notify_parent(
        db,
        engine,
        schema,
        child,
        parent,
        FailureEnvelope(
            code="CONTROL_PENDING",
            category="concurrency",
            retryable=False,
            public_message="The delegated task was cancelled.",
            partial_result_refs=partial,
        ),
        now,
    )
    return True


async def append_cancel_events(
    db: AsyncSession,
    journal: SqlJournal,
    authority: ServiceEventAuthority,
    child: Mapping[str, Any],
    events: tuple[StateEventDraft, ...],
) -> None:
    for event in events:
        current = authority
        if isinstance(event, CommandUpdatedDraft):
            # Command events use their existing control authority, not recovery's
            # narrower event whitelist. Deleted sessions retain SQL audit only;
            # their cleanup must not reacquire user control authority.
            deleted = (
                await db.execute(
                    scoped(
                        "SELECT deleted_at FROM {t}session WHERE scope_id=:scope AND id=:session",
                        journal._schema,
                    ),
                    {"scope": child["scope_id"], "session": child["session_id"]},
                )
            ).scalar_one()
            if deleted is not None:
                continue
            current = authority.model_copy(
                update={
                    "purpose": "control",
                    "source": ResourceRef(
                        resource_type="command",
                        resource_id=str(event.payload.command_id),
                        resource_version="1",
                        scope=authority.source.scope,
                    ),
                }
            )
        await journal.append_state_in_transaction(
            db,
            StateEventBatch(
                authority=current,
                session_id=child["session_id"],
                trace_id=str(authority.transaction_id),
                events=(event,),
            ),
        )


async def seal_child_output(
    db: AsyncSession,
    schema: str,
    journal: SqlJournal,
    authority: EventAuthority,
    child: Mapping[str, Any],
    now: datetime,
    *,
    status: Literal["failed", "cancelled"],
) -> tuple[ResourceRef, ...]:
    """Seal durable bytes after the caller fences/stops producers, before Run terminal.

    The caller holds Slot -> sorted Runs -> Work -> Session in this transaction.
    Both recovery services and an authenticated child failure sink can reuse this;
    it emits only item.completed, never a second Run terminal or parent notice.
    """
    params = {"scope": child["scope_id"], "run": child["id"]}
    items = (
        (
            await db.execute(
                scoped(
                    "SELECT * FROM {t}timeline_item WHERE scope_id=:scope AND run_id=:run "
                    "AND status IN ('pending','streaming') ORDER BY source_seq,id FOR UPDATE",
                    schema,
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    refs = []
    for item in items:
        item_params = {**params, "item": item["id"]}
        events = [
            dict(row)
            for row in (
                await db.execute(
                    scoped(
                        "SELECT kind,model_attempt_id,payload,session_seq FROM {t}event "
                        "WHERE scope_id=:scope AND run_id=:run AND item_id=:item "
                        "AND kind IN ('item.started','assistant.delta','analysis.delta') "
                        "ORDER BY session_seq",
                        schema,
                    ),
                    item_params,
                )
            ).mappings()
        ]
        starts = [row for row in events if row["kind"] == "item.started"]
        if len(starts) > 1:
            raise ValueError("stopped child output has ambiguous declarations")
        started = ItemStarted.model_validate(starts[0]["payload"]) if starts else None
        outputs = [row for row in events if row["kind"] != "item.started"]
        attempt = outputs[0]["model_attempt_id"] if outputs else None
        proof = None
        revision = max(1, int(item["final_revision"]))
        if started is not None:
            attempt = starts[0]["model_attempt_id"]
            revision = int(started.item_revision)
            if any(
                row["model_attempt_id"] != attempt
                or row["payload"]["itemRevision"] != started.item_revision
                or row["payload"]["channel"] not in started.channels
                or row["session_seq"] <= starts[0]["session_seq"]
                for row in outputs
            ):
                raise ValueError("stopped child output disagrees with its declaration")
        if outputs:
            if attempt is None or any(row["model_attempt_id"] != attempt for row in outputs):
                raise ValueError("stopped child output has ambiguous producers")
            durable_seq = (
                await db.execute(
                    scoped(
                        "SELECT event_seq FROM {t}session WHERE scope_id=:scope AND id=:session",
                        schema,
                    ),
                    {**params, "session": child["session_id"]},
                )
            ).scalar_one()
            proof = journal.derive_flush_proof(
                outputs,
                durable_seq=durable_seq,
                proof_id=uuid5(NAMESPACE_URL, f"child-stop-seal:{attempt}"),
                scope_id=child["scope_id"],
                session_id=child["session_id"],
                run_id=child["id"],
                attempt_id=attempt,
                item_id=item["id"],
            )
            revision = int(outputs[0]["payload"]["itemRevision"])
            stored = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}output_seal WHERE scope_id=:scope AND run_id=:run "
                            "AND item_id=:item AND model_attempt_id=:attempt",
                            schema,
                        ),
                        {**item_params, "attempt": attempt},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if stored is None:
                await db.execute(
                    journal._sql.seal_insert,
                    {
                        "id": proof.proof_id,
                        **journal._seal_payload(proof, revision),
                        "sealed_at": now,
                    },
                )
            else:
                saved = journal._stored_flush_proof(dict(stored))
                if saved.channels != proof.channels:
                    raise ValueError("stopped output disagrees with its seal")
                proof, revision = saved, int(stored["sealed_revision"])
        # Empty declared channels are completion facts, not fabricated producer seals.
        flushed = {} if proof is None else {part.channel: part for part in proof.channels}
        channels = tuple(flushed) if started is None else started.channels
        empty_digest = hashlib.sha256(b"").hexdigest()
        text = "".join(
            row["payload"]["text"] for row in outputs if row["payload"]["channel"] == "content"
        )
        payload = {"runId": str(child["id"]), "text": text}
        scope_digest = hashlib.sha256(child["scope_id"].encode()).hexdigest()
        ref = f"context-{scope_digest}-{content_digest(payload)}"
        await SqlContextBlobBackend(journal._engine, schema).put_in_transaction(
            db, child["scope_id"], ref, canonical_bytes(payload)
        )
        await journal.append_state_in_transaction(
            db,
            StateEventBatch(
                authority=authority,
                session_id=child["session_id"],
                trace_id=f"child-output:{child['id']}:{child['state_version']}",
                events=(
                    ItemCompletedDraft(
                        kind="item.completed",
                        producer_key=f"child-{status}-item:{item['id']}",
                        occurred_at=now,
                        turn_id=child["turn_id"],
                        run_id=child["id"],
                        parent_run_id=child["parent_run_id"],
                        step_id=None,
                        item_id=item["id"],
                        model_attempt_id=attempt,
                        payload=ItemCompleted(
                            item_revision=str(revision),
                            status=status,
                            channel_digests=tuple(
                                ChannelDigest(
                                    channel=channel,
                                    digest=flushed[channel].digest
                                    if channel in flushed
                                    else empty_digest,
                                )
                                for channel in channels
                            ),
                            last_delta_indexes=tuple(
                                ChannelIndex(
                                    channel=channel,
                                    last_delta_index=flushed[channel].last_delta_index
                                    if channel in flushed
                                    else None,
                                )
                                for channel in channels
                            ),
                        ),
                    ),
                ),
            ),
        )
        await db.execute(
            scoped(
                "UPDATE {t}timeline_item SET status=:status,final_revision=:revision,"
                "content_ref=:ref,updated_at=:now WHERE scope_id=:scope AND id=:item",
                schema,
            ),
            {**item_params, "revision": revision, "ref": ref, "now": now, "status": status},
        )
        if text.strip():
            refs.append(
                ResourceRef(
                    resource_type="timeline_item",
                    resource_id=str(item["id"]),
                    resource_version=str(revision),
                    scope=ResourceScope(
                        scope_id=child["scope_id"], session_id=str(child["session_id"])
                    ),
                )
            )
    return tuple(refs)

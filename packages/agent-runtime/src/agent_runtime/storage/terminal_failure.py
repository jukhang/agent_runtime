"""Root failure projection inside Recovery's locked Slot/Run/Work transaction."""

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from agent_runtime.contracts.canonical import canonical_bytes, content_digest
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.payloads import (
    ChannelDigest,
    ChannelIndex,
    ItemCompleted,
    ItemStarted,
    RunFailed,
    RunStatusPayload,
)
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.state_events import (
    ItemCompletedDraft,
    RunFailedDraft,
    RunStatusDraft,
    ServiceEventAuthority,
    StateEventBatch,
)
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.control_checkpoints import transition_checkpoint
from agent_runtime.storage.uow.schema import scoped
from agent_runtime.terminal_guidance import terminal_guidance


async def finalize_root_failure(
    db: AsyncSession,
    engine: AsyncEngine,
    schema: str,
    run: Mapping[str, Any],
    failure: FailureEnvelope,
    now: datetime,
) -> FailureEnvelope:
    """Caller has validated the current/expired fence and holds Slot -> Run -> Work.

    No external effect is retried or declared stopped here. Epoch invalidation and
    the failure are committed together with the captured output and notifications.
    """
    params = {"scope": run["scope_id"], "run": run["id"], "session": run["session_id"]}
    session_row = (
        (
            await db.execute(
                scoped(
                    "SELECT * FROM {t}session WHERE scope_id=:scope AND id=:session FOR UPDATE",
                    schema,
                ),
                params,
            )
        )
        .mappings()
        .one()
    )
    actor = ActorContext(
        scope_id=run["scope_id"],
        actor_id=session_row["owner_actor_id"],
        auth_revision=str(session_row["auth_revision"]),
        grants_ref="recovery:terminal",
    )
    journal = SqlJournal(engine, schema, actor, clock=lambda: now)
    blobs = SqlContextBlobBackend(engine, schema)
    authority = ServiceEventAuthority(
        kind="service",
        actor=actor,
        purpose="recovery",
        source=ResourceRef(
            resource_type="run",
            resource_id=str(run["id"]),
            resource_version=str(run["state_version"]),
            scope=ResourceScope(scope_id=run["scope_id"], session_id=str(run["session_id"])),
        ),
        transaction_id=uuid5(
            NAMESPACE_URL, f"failed:{run['scope_id']}:{run['id']}:{run['state_version']}"
        ),
    )
    common = dict(
        occurred_at=now,
        turn_id=run["turn_id"],
        run_id=run["id"],
        parent_run_id=run["parent_run_id"],
        step_id=None,
    )
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
            errors.version_conflict("interrupted item has ambiguous declarations")
        started = ItemStarted.model_validate(starts[0]["payload"]) if starts else None
        outputs = [row for row in events if row["kind"] != "item.started"]
        attempt_id = outputs[0]["model_attempt_id"] if outputs else None
        revision = max(1, int(item["final_revision"]))
        if started is not None:
            attempt_id = starts[0]["model_attempt_id"]
            revision = int(started.item_revision)
            if any(
                row["model_attempt_id"] != attempt_id
                or row["payload"]["itemRevision"] != started.item_revision
                or row["payload"]["channel"] not in started.channels
                or row["session_seq"] <= starts[0]["session_seq"]
                for row in outputs
            ):
                errors.version_conflict("interrupted output disagrees with its declaration")
        if any(row["model_attempt_id"] != attempt_id for row in outputs):
            errors.version_conflict("interrupted item has ambiguous output producers")
        proof = None
        if outputs:
            if attempt_id is None:
                errors.invalid_input("output has no model producer")
            proof = journal.derive_flush_proof(
                outputs,
                durable_seq=int(session_row["event_seq"]),
                proof_id=uuid5(NAMESPACE_URL, f"main-seal:{attempt_id}"),
                scope_id=run["scope_id"],
                session_id=run["session_id"],
                run_id=run["id"],
                attempt_id=attempt_id,
                item_id=item["id"],
            )
            revision = int(outputs[0]["payload"]["itemRevision"])
            existing = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}output_seal WHERE scope_id=:scope AND run_id=:run "
                            "AND model_attempt_id=:attempt AND item_id=:item",
                            schema,
                        ),
                        {**item_params, "attempt": attempt_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing is None:
                await db.execute(
                    journal._sql.seal_insert,
                    {
                        "id": uuid5(NAMESPACE_URL, f"failed-seal:{run['scope_id']}:{item['id']}"),
                        **journal._seal_payload(proof, revision),
                        "sealed_at": now,
                    },
                )
            else:
                stored = journal._stored_flush_proof(dict(existing))
                if (
                    stored.channels != proof.channels
                    or int(existing["sealed_revision"]) != revision
                ):
                    errors.version_conflict("stored seal disagrees with interrupted output")
                proof = stored
        # A declaration without deltas needs completion digests, not a producer seal.
        flushed = {} if proof is None else {part.channel: part for part in proof.channels}
        channels = tuple(flushed) if started is None else started.channels
        empty_digest = hashlib.sha256(b"").hexdigest()
        text = "".join(
            row["payload"]["text"] for row in outputs if row["payload"]["channel"] == "content"
        )
        payload = {"runId": str(run["id"]), "text": text}
        data, digest = canonical_bytes(payload), content_digest(payload)
        key = f"context-{hashlib.sha256(run['scope_id'].encode()).hexdigest()}-{digest}"
        await blobs.put_in_transaction(db, run["scope_id"], key, data)
        await journal.append_state_in_transaction(
            db,
            StateEventBatch(
                authority=authority,
                session_id=run["session_id"],
                trace_id=str(authority.transaction_id),
                events=(
                    ItemCompletedDraft(
                        kind="item.completed",
                        producer_key=f"failed-item:{item['id']}",
                        item_id=item["id"],
                        model_attempt_id=attempt_id,
                        payload=ItemCompleted(
                            item_revision=str(revision),
                            status="failed",
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
                        **common,
                    ),
                ),
            ),
        )
        await db.execute(
            scoped(
                "UPDATE {t}timeline_item SET status='failed',final_revision=:revision,"
                "content_ref=:ref,updated_at=:now WHERE scope_id=:scope AND id=:item",
                schema,
            ),
            {**item_params, "revision": revision, "ref": key, "now": now},
        )

    retained = (
        (
            await db.execute(
                scoped(
                    "SELECT i.id,i.status,i.final_revision,"
                    "(SELECT string_agg(e.payload->>'text','' "
                    "ORDER BY e.session_seq) FROM {t}event e WHERE e.scope_id=i.scope_id "
                    "AND e.item_id=i.id AND e.kind='assistant.delta') AS text "
                    "FROM {t}timeline_item i JOIN {t}output_seal s "
                    "ON s.scope_id=i.scope_id AND s.item_id=i.id AND s.run_id=i.run_id "
                    "WHERE i.scope_id=:scope AND i.run_id=:run "
                    "AND i.status IN ('completed','failed','cancelled') "
                    "AND s.content_last_delta_index IS NOT NULL ORDER BY i.source_seq,i.id",
                    schema,
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    refs = tuple(
        ResourceRef(
            resource_type="timeline_item",
            resource_id=str(i["id"]),
            resource_version=str(i["final_revision"]),
            scope=ResourceScope(scope_id=run["scope_id"], session_id=str(run["session_id"])),
        )
        for i in retained
        if i["text"] and i["text"].strip()
    )
    artifacts = (
        (
            await db.execute(
                scoped(
                    "SELECT payload->'resourceRef' AS ref FROM {t}event "
                    "WHERE scope_id=:scope AND run_id=:run AND kind='artifact.created' "
                    "ORDER BY session_seq",
                    schema,
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    artifact_refs = tuple(ResourceRef.model_validate(i["ref"]) for i in artifacts)
    refs = tuple(dict.fromkeys((*refs, *artifact_refs)))
    effects = bool(
        (
            await db.execute(
                scoped(
                    "SELECT EXISTS(SELECT 1 FROM {t}execution "
                    "WHERE scope_id=:scope AND run_id=:run "
                    "AND status NOT IN ('completed','failed','cancelled')) OR EXISTS(SELECT 1 FROM "
                    "{t}operation o JOIN {t}tool_call t ON t.scope_id=o.scope_id "
                    "AND t.operation_id=o.id "
                    "WHERE t.scope_id=:scope AND t.run_id=:run "
                    "AND o.status IN ('prepared','dispatching','unknown'))",
                    schema,
                ),
                params,
            )
        ).scalar_one()
    )
    guidance = terminal_guidance(
        "failed", failure=failure, retained_count=len(refs), unresolved_effects=effects
    )
    failure = failure.model_copy(
        update={
            "partial_result_refs": refs,
            "recovery_options": guidance.next_steps,
            "public_message": guidance.message,
        }
    )
    version = int(run["state_version"]) + 1
    checkpoint = run["checkpoint_id"]
    if checkpoint is not None:
        checkpoint = await transition_checkpoint(
            db, schema, run, checkpoint, kind="failure", status="failed", target_version=version
        )
    await journal.append_state_in_transaction(
        db,
        StateEventBatch(
            authority=authority,
            session_id=run["session_id"],
            trace_id=str(authority.transaction_id),
            events=(
                RunStatusDraft(
                    kind="run.status",
                    producer_key=f"failed-status:{run['id']}:{version}",
                    item_id=None,
                    model_attempt_id=None,
                    payload=RunStatusPayload(
                        state_version=str(version),
                        status=RunStatus.FAILED,
                        phase="terminal",
                        phase_started_at=now,
                        public_reason=guidance.message,
                    ),
                    **common,
                ),
                RunFailedDraft(
                    kind="run.failed",
                    producer_key=f"failed-terminal:{run['id']}:{version}",
                    item_id=None,
                    model_attempt_id=None,
                    payload=RunFailed(
                        state_version=str(version),
                        final_item_ids=tuple(
                            i["id"]
                            for i in retained
                            if i["status"] == "completed" and i["text"].strip()
                        ),
                        partial_result_refs=refs,
                        outcome=None,
                        failure=failure,
                        public_message=guidance.message,
                        next_steps=guidance.next_steps,
                    ),
                    **common,
                ),
            ),
        ),
    )
    encoded = json.dumps(failure.model_dump(mode="json", by_alias=True))
    await db.execute(
        scoped(
            "UPDATE {t}run SET state_version=:version,checkpoint_id=:checkpoint "
            "WHERE scope_id=:scope AND id=:run",
            schema,
        ),
        {**params, "version": version, "checkpoint": checkpoint},
    )
    await db.execute(
        scoped(
            "UPDATE {t}turn SET outcome_run_id=:run,outcome=NULL,failure=CAST(:failure AS jsonb),"
            "updated_at=:now WHERE scope_id=:scope AND primary_run_id=:run "
            "AND outcome_run_id IS NULL",
            schema,
        ),
        {**params, "failure": encoded, "now": now},
    )
    await db.execute(
        scoped(
            "UPDATE {t}work_item SET status='cancelled',owner_id=NULL,lease_until=NULL,"
            "lease_epoch=lease_epoch+1,updated_at=:now WHERE scope_id=:scope AND run_id=:run "
            "AND kind='advance_run' AND status IN ('pending','leased')",
            schema,
        ),
        {**params, "now": now},
    )
    return failure

"""Transaction-local checkpoint and mailbox persistence for delegation."""

import hashlib
import json
from datetime import datetime
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.canonical import canonical_bytes, content_digest
from agent_runtime.contracts.messages import AgentEnvelope
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import scoped


async def checkpoint(
    session: AsyncSession,
    schema: str,
    *,
    scope: str,
    run_id: UUID,
    version: int,
    source: UUID | None,
    payload: dict[str, object],
    event_seq: int,
) -> UUID:
    data = canonical_bytes(payload)
    digest = content_digest(payload)
    if len(data) > 16_777_216:
        errors.invalid_input("delegation checkpoint exceeds the blob limit")
    ref = f"context-{hashlib.sha256(scope.encode()).hexdigest()}-{digest}"
    await session.execute(
        scoped(
            "INSERT INTO {t}context_blob (scope_id,blob_key,digest,size_bytes,content) "
            "VALUES (:scope,:ref,:digest,:size,:data) ON CONFLICT DO NOTHING",
            schema,
        ),
        {"scope": scope, "ref": ref, "digest": digest, "size": len(data), "data": data},
    )
    stored = (
        await session.execute(
            scoped(
                "SELECT content FROM {t}context_blob WHERE scope_id=:scope AND blob_key=:ref",
                schema,
            ),
            {"scope": scope, "ref": ref},
        )
    ).scalar_one()
    if bytes(stored) != data:
        errors.version_conflict("delegation checkpoint blob conflicts")
    identity = uuid5(NAMESPACE_URL, f"delegation-checkpoint:{run_id}:{version}:{digest}")
    inserted = await session.execute(
        scoped(
            "INSERT INTO {t}checkpoint (id,scope_id,run_id,source_checkpoint_id,state_version,"
            "representation_revision,event_seq,schema_revision,runtime_build,"
            "state_ref,content_hash) "
            "VALUES (:id,:scope,:run,:source,:version,0,:seq,1,'agent-runtime',:ref,:digest) "
            "ON CONFLICT DO NOTHING RETURNING id",
            schema,
        ),
        {
            "id": identity,
            "scope": scope,
            "run": run_id,
            "source": source,
            "version": version,
            "seq": event_seq,
            "ref": ref,
            "digest": digest,
        },
    )
    if inserted.scalar_one_or_none() is None:
        errors.version_conflict("delegation checkpoint version already exists")
    return identity


async def message(
    session: AsyncSession,
    schema: str,
    *,
    scope: str,
    envelope: AgentEnvelope,
    status: str = "pending",
    applied_checkpoint: UUID | None = None,
) -> None:
    await session.execute(
        scoped(
            "INSERT INTO {t}mailbox_cursor (scope_id,recipient_run_id,allocated_seq,applied_seq) "
            "VALUES (:scope,:run,0,0) ON CONFLICT (scope_id,recipient_run_id) DO NOTHING",
            schema,
        ),
        {"scope": scope, "run": envelope.recipient_run_id},
    )
    allocated = await session.execute(
        scoped(
            "UPDATE {t}mailbox_cursor "
            "SET allocated_seq=:seq,updated_at=CURRENT_TIMESTAMP "
            "WHERE scope_id=:scope AND recipient_run_id=:run "
            "AND allocated_seq=:previous RETURNING allocated_seq",
            schema,
        ),
        {
            "scope": scope,
            "run": envelope.recipient_run_id,
            "seq": int(envelope.recipient_seq),
            "previous": int(envelope.recipient_seq) - 1,
        },
    )
    if allocated.scalar_one_or_none() is None:
        errors.version_conflict("mailbox sequence changed before delegation message")
    if status == "applied":
        changed = await session.execute(
            scoped(
                "UPDATE {t}mailbox_cursor SET applied_seq=:seq "
                "WHERE scope_id=:scope AND recipient_run_id=:run AND applied_seq=:previous "
                "RETURNING applied_seq",
                schema,
            ),
            {
                "scope": scope,
                "run": envelope.recipient_run_id,
                "seq": int(envelope.recipient_seq),
                "previous": int(envelope.recipient_seq) - 1,
            },
        )
        if changed.scalar_one_or_none() is None:
            errors.version_conflict("assignment checkpoint skips mailbox input")
    await session.execute(
        scoped(
            "INSERT INTO {t}agent_message (id,scope_id,sender_run_id,recipient_run_id,task_id,"
            "correlation_id,causation_id,recipient_seq,protocol_revision,payload,kind,payload_hash,"
            "status,trace_id,deadline_at,applied_checkpoint_id,created_at,updated_at) "
            "VALUES (:id,:scope,:sender,:recipient,"
            ":task,:correlation,:causation,:seq,1,CAST(:payload AS jsonb),:kind,:hash,:status,"
            ":trace,:deadline,:checkpoint,:created,:created)",
            schema,
        ),
        {
            "id": envelope.message_id,
            "scope": scope,
            "sender": envelope.sender_run_id,
            "recipient": envelope.recipient_run_id,
            "task": envelope.task_id,
            "correlation": envelope.correlation_id,
            "causation": envelope.causation_id,
            "seq": int(envelope.recipient_seq),
            "payload": json.dumps(
                {
                    "payload": envelope.payload.model_dump(mode="json", by_alias=True),
                    "resourceRefs": [
                        r.model_dump(mode="json", by_alias=True) for r in envelope.resource_refs
                    ],
                }
            ),
            "kind": envelope.kind,
            "hash": content_digest(envelope.model_dump(mode="json", by_alias=True)),
            "status": status,
            "trace": envelope.trace_id,
            "deadline": envelope.deadline_at,
            "checkpoint": applied_checkpoint,
            "created": envelope.created_at,
        },
    )


async def queue_work(
    session: AsyncSession,
    schema: str,
    *,
    scope: str,
    run_id: UUID,
    checkpoint_id: UUID,
    key: str,
    now: datetime,
    deadline: datetime,
) -> UUID:
    identity = uuid5(NAMESPACE_URL, key)
    await session.execute(
        scoped(
            "INSERT INTO {t}work_item (id,scope_id,run_id,subject_ref,kind,dedup_key,status,"
            "payload_ref,wake_reason,priority,lease_epoch,attempt_count,available_at,deadline_at) "
            "VALUES (:id,:scope,:run,CAST(:subject AS jsonb),'advance_run',:key,'pending',"
            ":payload,'recovery',100,0,0,:now,:deadline)",
            schema,
        ),
        {
            "id": identity,
            "scope": scope,
            "run": run_id,
            "subject": json.dumps({"kind": "delegate"}),
            "key": key,
            "payload": f"checkpoint:{checkpoint_id}",
            "now": now,
            "deadline": deadline,
        },
    )
    return identity

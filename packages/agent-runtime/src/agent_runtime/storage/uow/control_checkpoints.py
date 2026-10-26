"""Immutable control transition checkpoints created in the owning state transaction."""

import hashlib
from collections.abc import Mapping
from typing import Any, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.canonical import canonical_bytes, content_digest
from agent_runtime.storage.uow import errors, locks
from agent_runtime.storage.uow.schema import scoped


async def transition_checkpoint(
    db: AsyncSession,
    schema: str,
    run: Mapping[str, Any],
    source_id: UUID,
    *,
    kind: str,
    status: str,
    target_version: int,
) -> UUID:
    """Caller owns the state transaction and Slot; never commit a pointer alone."""
    current = await locks.lock_run(
        db, locks.build_lock_statements(schema), run["scope_id"], run["id"]
    )
    if (
        current is None
        or current["checkpoint_id"] != run["checkpoint_id"]
        or current["state_version"] != run["state_version"]
    ):
        errors.version_conflict("control checkpoint lost its current Run source")
    params = {"scope": run["scope_id"], "run": run["id"], "source": source_id}
    source = (
        (
            await db.execute(
                scoped(
                    "SELECT * FROM {t}checkpoint "
                    "WHERE scope_id=:scope AND run_id=:run AND id=:source",
                    schema,
                ),
                params,
            )
        )
        .mappings()
        .one_or_none()
    )
    if source is None:
        errors.invalid_input("control checkpoint does not belong to this run")
    if int(source["state_version"]) == target_version:
        await _select_checkpoint(db, schema, run, source_id)
        return source_id
    if source_id != run["checkpoint_id"] or int(source["state_version"]) != run["state_version"]:
        errors.version_conflict("control checkpoint is not the current committed state")
    payload = {
        "kind": "control_checkpoint",
        "runId": str(run["id"]),
        "stateVersion": str(target_version),
        "control": kind,
        "status": status,
        "sourceCheckpointId": str(source_id),
        "sourceStateRef": source["state_ref"],
        "appliedInputSeq": str(run["applied_input_version"]),
    }
    data, digest = canonical_bytes(payload), content_digest(payload)
    scope_digest = hashlib.sha256(run["scope_id"].encode()).hexdigest()
    ref = f"context-{scope_digest}-{digest}"
    await db.execute(
        scoped(
            "INSERT INTO {t}context_blob (scope_id,blob_key,digest,size_bytes,content) "
            "VALUES (:scope,:ref,:digest,:size,:content) ON CONFLICT DO NOTHING",
            schema,
        ),
        {**params, "ref": ref, "digest": digest, "size": len(data), "content": data},
    )
    blob = (
        (
            await db.execute(
                scoped(
                    "SELECT digest,content FROM {t}context_blob "
                    "WHERE scope_id=:scope AND blob_key=:ref "
                    "FOR KEY SHARE",
                    schema,
                ),
                {**params, "ref": ref},
            )
        )
        .mappings()
        .one_or_none()
    )
    if blob is None or blob["digest"] != digest or bytes(blob["content"]) != data:
        errors.version_conflict("control checkpoint blob is missing or corrupt")
    # A prepared action can already occupy revision zero without being selected.
    # Run serialization protects allocation; no existing representation is rewritten.
    matching_id = (
        await db.execute(
            scoped(
                "SELECT id FROM {t}checkpoint WHERE scope_id=:scope AND run_id=:run "
                "AND state_version=:version AND source_checkpoint_id=:source "
                "AND state_ref=:ref AND content_hash=:digest "
                "ORDER BY representation_revision LIMIT 1",
                schema,
            ),
            {**params, "version": target_version, "ref": ref, "digest": digest},
        )
    ).scalar_one_or_none()
    if matching_id is not None:
        checkpoint_id = cast(UUID, matching_id)
        await _select_checkpoint(db, schema, run, checkpoint_id)
        return checkpoint_id
    revision = (
        await db.execute(
            scoped(
                "SELECT COALESCE(MAX(representation_revision),-1)+1 FROM {t}checkpoint "
                "WHERE scope_id=:scope AND run_id=:run AND state_version=:version",
                schema,
            ),
            {**params, "version": target_version},
        )
    ).scalar_one()
    checkpoint_id = uuid5(
        NAMESPACE_URL, f"control-checkpoint:{run['id']}:{target_version}:{kind}:{digest}"
    )
    await db.execute(
        scoped(
            "INSERT INTO {t}checkpoint (id,scope_id,run_id,source_checkpoint_id,state_version,"
            "representation_revision,event_seq,schema_revision,"
            "runtime_build,state_ref,content_hash) "
            "SELECT :id,:scope,:run,:source,:version,:revision,s.event_seq,1,"
            "'agent-runtime',:ref,:digest "
            "FROM {t}session s WHERE s.scope_id=:scope AND s.id=:session "
            "ON CONFLICT DO NOTHING",
            schema,
        ),
        {
            **params,
            "id": checkpoint_id,
            "version": target_version,
            "revision": revision,
            "ref": ref,
            "digest": digest,
            "session": run["session_id"],
        },
    )
    existing = (
        (
            await db.execute(
                scoped(
                    "SELECT state_ref,content_hash FROM {t}checkpoint "
                    "WHERE scope_id=:scope AND id=:id",
                    schema,
                ),
                {**params, "id": checkpoint_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if existing is None or existing["state_ref"] != ref or existing["content_hash"] != digest:
        errors.version_conflict("control checkpoint identity conflicts with another transition")
    await _select_checkpoint(db, schema, run, checkpoint_id)
    return checkpoint_id


async def _select_checkpoint(
    db: AsyncSession, schema: str, run: Mapping[str, Any], checkpoint_id: UUID
) -> None:
    selected = (
        await db.execute(
            scoped(
                "UPDATE {t}run SET checkpoint_id=:checkpoint "
                "WHERE scope_id=:scope AND id=:run AND state_version=:version "
                "AND checkpoint_id=:source RETURNING id",
                schema,
            ),
            {
                "checkpoint": checkpoint_id,
                "scope": run["scope_id"],
                "run": run["id"],
                "version": run["state_version"],
                "source": run["checkpoint_id"],
            },
        )
    ).scalar_one_or_none()
    if selected is None:
        errors.version_conflict("control checkpoint lost its Run compare-and-swap")

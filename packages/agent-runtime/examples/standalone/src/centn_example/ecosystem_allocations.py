"""Durable create intents and fenced cleanup, independent of Runtime migrations."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.failure import RecoveryOption, runtime_failure
from agent_runtime.storage.host_tool_access import SqlHostToolAccess
from agent_runtime.storage.uow import errors, locks
from agent_runtime.storage.uow.schema import scoped
from centn_example.resource_migrate import resource_schema


@dataclass(frozen=True)
class Allocation:
    id: UUID
    scope_id: str
    metadata: dict[str, str]
    sandbox_id: str | None


class SqlSandboxAllocations:
    def __init__(self, engine, runtime_schema, *, deployment_id, resources):
        self.engine, self.runtime_schema = engine, runtime_schema
        self.schema = resource_schema(runtime_schema)
        self.deployment_id, self.resources = deployment_id, resources
        self.sessions = resources.resources.sessions
        self.owner = uuid4()
        self.locks = locks.build_lock_statements(runtime_schema)

    def view(self, row):
        return Allocation(
            row["id"], row["scope_id"], dict(row["remote_metadata"]), row["sandbox_id"]
        )

    async def reserve(self, invocation, lease):
        identity = uuid5(
            NAMESPACE_URL, f"allocation:{invocation.actor.scope_id}:{invocation.operation_id}"
        )
        metadata = {
            "centn.ecosystem": self.deployment_id,
            "centn.allocation": str(identity),
            "centn.scope": invocation.actor.scope_id,
            "centn.run": str(invocation.run_id),
            "centn.operation": str(invocation.operation_id),
            "centn.claim": str(lease.work_id),
            "centn.claim-owner": lease.owner_id,
            "centn.run-epoch": lease.run_epoch,
            "centn.work-epoch": lease.work_epoch,
        }
        async with self.sessions() as db, db.begin():
            await SqlHostToolAccess(
                self.runtime_schema, invocation.actor, self.resources, lease=lambda: lease
            ).authorize(db, invocation, ())
            exists = (
                await db.execute(
                    scoped(
                        "SELECT id FROM {t}sandbox_allocation WHERE scope_id=:scope "
                        "AND operation_id=:operation",
                        self.schema,
                    ),
                    {"scope": invocation.actor.scope_id, "operation": invocation.operation_id},
                )
            ).first()
            if exists is not None:
                raise runtime_failure(
                    "DEPENDENCY_UNAVAILABLE",
                    category="tool",
                    public_message=(
                        "An earlier sandbox allocation was not replayed. "
                        "Inspect its cleanup record and submit a new task."
                    ),
                    recovery_options=(
                        RecoveryOption(
                            action="manual_attention",
                            label="Inspect allocation cleanup before submitting a new task",
                        ),
                    ),
                )
            await db.execute(
                scoped(
                    "INSERT INTO {t}sandbox_allocation (scope_id,id,deployment_id,owner_actor_id,"
                    "session_id,run_id,operation_id,work_id,claim_owner,run_epoch,work_epoch,"
                    "invocation_hash,remote_metadata,status,expires_at) VALUES "
                    "(:scope,:id,:deployment,:actor,:session,:run,:operation,:work,:owner,:run_epoch,"
                    ":work_epoch,:digest,CAST(:metadata AS jsonb),'reserved',:expires)",
                    self.schema,
                ),
                {
                    "scope": invocation.actor.scope_id,
                    "id": identity,
                    "deployment": self.deployment_id,
                    "actor": invocation.actor.actor_id,
                    "session": invocation.session_id,
                    "run": invocation.run_id,
                    "operation": invocation.operation_id,
                    "work": lease.work_id,
                    "owner": lease.owner_id,
                    "run_epoch": int(lease.run_epoch),
                    "work_epoch": int(lease.work_epoch),
                    "digest": content_digest(invocation.model_dump(mode="json", by_alias=True)),
                    "metadata": json.dumps(metadata),
                    "expires": invocation.deadline_at,
                },
            )
        return Allocation(identity, invocation.actor.scope_id, metadata, None)

    async def row(self, db, identity, scope_id):
        row = (
            (
                await db.execute(
                    scoped(
                        "SELECT * FROM {t}sandbox_allocation WHERE scope_id=:scope AND id=:id "
                        "AND deployment_id=:deployment FOR UPDATE",
                        self.schema,
                    ),
                    {"scope": scope_id, "id": identity, "deployment": self.deployment_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            errors.forbidden("Allocation is outside this deployment")
        return row

    async def created(self, allocation, sandbox_id):
        async with self.sessions() as db, db.begin():
            row = await self.row(db, allocation.id, allocation.scope_id)
            if (
                row["scope_id"] != allocation.scope_id
                or row["remote_metadata"] != allocation.metadata
            ):
                errors.forbidden("Remote allocation ownership changed")
            if row["status"] == "cleaned" or row["sandbox_id"] not in {None, sandbox_id}:
                errors.version_conflict(
                    "Allocation is already closed or has another remote identity"
                )
            await db.execute(
                scoped(
                    "UPDATE {t}sandbox_allocation SET sandbox_id=:sandbox,"
                    "status=CASE WHEN status='reserved' THEN 'created' ELSE status END,"
                    "updated_at=CURRENT_TIMESTAMP WHERE scope_id=:scope AND id=:id "
                    "AND deployment_id=:deployment",
                    self.schema,
                ),
                {
                    "scope": allocation.scope_id,
                    "id": allocation.id,
                    "sandbox": sandbox_id,
                    "deployment": self.deployment_id,
                },
            )

    async def bind_in_transaction(self, db, allocation, invocation, request):
        if allocation.scope_id != invocation.actor.scope_id:
            errors.forbidden("Allocation belongs to another scope")
        row = await self.row(db, allocation.id, allocation.scope_id)
        if (
            row["status"] not in {"created", "bound"}
            or row["sandbox_id"] != request.binding.sandbox_id
            or row["run_id"] != invocation.run_id
            or row["operation_id"] != invocation.operation_id
            or row["invocation_hash"]
            != content_digest(invocation.model_dump(mode="json", by_alias=True))
            or row["execution_id"] not in {None, request.binding.execution_id}
        ):
            errors.forbidden("Allocation is no longer authorized for dispatch")
        await db.execute(
            scoped(
                "UPDATE {t}sandbox_allocation SET status='bound',execution_id=:execution,"
                "updated_at=CURRENT_TIMESTAMP WHERE scope_id=:scope AND id=:id "
                "AND deployment_id=:deployment",
                self.schema,
            ),
            {
                "scope": allocation.scope_id,
                "id": allocation.id,
                "execution": request.binding.execution_id,
                "deployment": self.deployment_id,
            },
        )

    async def cleanup_candidates(self):
        async with self.sessions() as db:
            rows = (
                (
                    await db.execute(
                        scoped(
                            "SELECT a.* FROM {t}sandbox_allocation a "
                            "WHERE deployment_id=:deployment AND status<>'cleaned' "
                            "AND (cleanup_until IS NULL OR cleanup_until<CURRENT_TIMESTAMP) "
                            "ORDER BY updated_at,id LIMIT 1000",
                            self.schema,
                        ),
                        {"deployment": self.deployment_id},
                    )
                )
                .mappings()
                .all()
            )
            return tuple(self.view(row) for row in rows)

    async def claim_cleanup(self, identity, scope_id):
        async with self.sessions() as db, db.begin():
            # Discover immutable lock identities, then use the same Slot -> Run -> Work
            # -> allocation order as dispatch. Never lock an execution in reverse order.
            initial = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}sandbox_allocation WHERE scope_id=:scope AND id=:id "
                            "AND deployment_id=:deployment",
                            self.schema,
                        ),
                        {"scope": scope_id, "id": identity, "deployment": self.deployment_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if initial is None:
                return None
            await locks.lock_slot(db, self.locks, initial["scope_id"], initial["session_id"])
            run = await locks.lock_run(db, self.locks, initial["scope_id"], initial["run_id"])
            work = await locks.lock_work_item(
                db, self.locks, initial["scope_id"], initial["work_id"]
            )
            row = await self.row(db, identity, scope_id)
            now = datetime.now(UTC)
            if row["status"] == "cleaned" or (row["cleanup_until"] and row["cleanup_until"] > now):
                return None
            if row["execution_id"] is not None:
                cleaned = (
                    await db.execute(
                        scoped(
                            "SELECT cleanup_status FROM {t}execution "
                            "WHERE scope_id=:scope AND id=:id",
                            self.runtime_schema,
                        ),
                        {"scope": row["scope_id"], "id": row["execution_id"]},
                    )
                ).scalar_one_or_none()
                if cleaned != "completed":
                    return None
            else:
                active = (
                    run is not None
                    and work is not None
                    and run["status"] == "running"
                    and run["desired_control"] is None
                    and run["owner_id"] == row["claim_owner"]
                    and int(run["owner_epoch"]) == row["run_epoch"]
                    and run["lease_until"] is not None
                    and run["lease_until"] > now
                    and run["deadline_at"] > now
                    and work["status"] == "leased"
                    and work["owner_id"] == row["claim_owner"]
                    and int(work["lease_epoch"]) == row["work_epoch"]
                    and work["lease_until"] is not None
                    and work["lease_until"] > now
                    and row["expires_at"] > now
                )
                if active:
                    return None
            await db.execute(
                scoped(
                    "UPDATE {t}sandbox_allocation SET status='cleaning',cleanup_owner=:owner,"
                    "cleanup_until=:until,updated_at=:now WHERE scope_id=:scope AND id=:id "
                    "AND deployment_id=:deployment",
                    self.schema,
                ),
                {
                    "id": identity,
                    "scope": scope_id,
                    "deployment": self.deployment_id,
                    "owner": self.owner,
                    "until": now + timedelta(seconds=40),
                    "now": now,
                },
            )
            return self.view(row)

    async def complete_cleanup(self, identity, scope_id):
        async with self.sessions() as db, db.begin():
            row = await self.row(db, identity, scope_id)
            if (
                row["status"] != "cleaning"
                or row["cleanup_owner"] != self.owner
                or row["cleanup_until"] <= datetime.now(UTC)
            ):
                errors.fenced("Allocation cleanup lease was lost")
            if row["sandbox_id"] is None:
                errors.version_conflict(
                    "Unknown create cannot be declared cleaned without a remote identity"
                )
            changed = await db.execute(
                scoped(
                    "UPDATE {t}sandbox_allocation SET status='cleaned',"
                    "cleanup_until=NULL,cleanup_owner=NULL,"
                    "updated_at=CURRENT_TIMESTAMP WHERE scope_id=:scope AND id=:id "
                    "AND deployment_id=:deployment AND status='cleaning' "
                    "AND cleanup_owner=:owner AND cleanup_until>:now",
                    self.schema,
                ),
                {
                    "scope": scope_id,
                    "id": identity,
                    "deployment": self.deployment_id,
                    "owner": self.owner,
                    "now": datetime.now(UTC),
                },
            )
            if changed.rowcount != 1:
                errors.fenced("Allocation cleanup compare-and-swap was lost")

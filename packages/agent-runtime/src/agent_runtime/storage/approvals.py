"""Durable, operation-bound human approvals for SQL-backed tool execution."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.policy import PolicyRequest
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.tools import (
    ApprovalBinding,
    ApprovalDecision,
    ApprovalRequest,
    ToolSpec,
)
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.approval_events import append_approval_update
from agent_runtime.storage.uow import errors, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped


class ApprovalAccess(Protocol):
    async def require_request(
        self, db: AsyncSession, request: PolicyRequest, spec: ToolSpec
    ) -> None:
        """Recheck tool permission and pinned resource versions in this transaction."""

    async def require_decision(
        self,
        db: AsyncSession,
        approver: ActorContext,
        request_actor: str,
        run_id: UUID,
        session_id: UUID,
        resources: tuple[ResourceRef, ...],
    ) -> None:
        """Authorize this approver and recheck the target resources in this transaction."""


@dataclass(frozen=True, slots=True)
class StoredApproval:
    approval_id: UUID
    request: ApprovalRequest
    status: str
    revision: int


@dataclass(frozen=True, slots=True)
class ApprovalResolution:
    approval_id: UUID
    run_id: UUID
    status: str
    revision: int
    grant_ref: str | None


class SqlApprovalService:
    """Approval state is SQL-owned; a grant reference is never sufficient by itself.

    The caller supplies a trusted ACL/resource verifier. Both request and decision
    recheck it under the same transaction that writes the approval row. Wake is a
    separate idempotent operation; callers must use ``SqlWaitWaker`` and recovery.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        access: ApprovalAccess,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._engine = engine
        self._schema = require_schema(schema)
        self._access = access
        self._clock = clock
        self._locks = locks.build_lock_statements(schema)
        self._by_key = scoped(
            "SELECT * FROM {t}approval WHERE scope_id=:scope AND request_key=:key FOR UPDATE",
            schema,
        )
        self._by_id = scoped(
            "SELECT * FROM {t}approval WHERE scope_id=:scope AND id=:id FOR UPDATE", schema
        )
        self._insert = scoped(
            "INSERT INTO {t}approval (id,scope_id,run_id,operation_id,actor_id,request_key,"
            "input_hash,status,decision,revision,auth_revision,policy_ref,tool_revision,"
            "resource_refs,expires_at) VALUES (:id,:scope,:run,NULL,:actor,:key,:hash,"
            "'pending',NULL,0,:auth,CAST(:policy AS jsonb),CAST(:tool AS jsonb),"
            "CAST(:resources AS jsonb),:expires) ON CONFLICT (scope_id,request_key) DO NOTHING",
            schema,
        )
        self._resolve = scoped(
            "UPDATE {t}approval SET status=:decision,decision=:decision,"
            "decision_actor_id=:actor,revision=revision+1,"
            "approved_at=CASE WHEN :decision='approved' "
            "THEN CAST(:now AS timestamptz) ELSE NULL END,"
            "updated_at=:now WHERE scope_id=:scope AND id=:id AND status='pending' "
            "AND revision=:expected RETURNING revision",
            schema,
        )
        self._expire = scoped(
            "UPDATE {t}approval SET status='expired',revision=revision+1,updated_at=:now "
            "WHERE scope_id=:scope AND id=:id AND status='pending' AND expires_at<=:now",
            schema,
        )

    async def request(
        self,
        policy_request: PolicyRequest,
        spec: ToolSpec,
        *,
        public_summary: str,
        expires_at: datetime,
    ) -> StoredApproval:
        invocation = policy_request.invocation
        operation_id = invocation.operation_id
        if operation_id is None or spec.ref != invocation.call.tool:
            errors.invalid_input("approval requires a pinned effectful tool operation")
        if spec.approval_policy != policy_request.policy:
            errors.version_conflict("tool approval policy changed")
        now = self._clock()
        if expires_at.tzinfo is None or expires_at <= now:
            errors.invalid_input("approval expiry must be in the future")
        key = f"tool:{operation_id}"
        approval_id = uuid5(NAMESPACE_URL, f"approval:{invocation.actor.scope_id}:{key}")
        binding = ApprovalBinding(
            scope_id=invocation.actor.scope_id,
            actor_id=invocation.actor.actor_id,
            session_id=invocation.session_id,
            tool=spec.ref,
            operation_id=operation_id,
            arguments_digest=content_digest(invocation.call.arguments),
            policy=policy_request.policy,
            auth_revision=invocation.actor.auth_revision,
            resources=policy_request.resources,
        )
        approval_request = ApprovalRequest(
            approval_id=approval_id,
            binding=binding,
            idempotency_key=key,
            public_summary=public_summary,
            expires_at=expires_at,
        )
        async with self._sessions() as db, db.begin():
            slot = await locks.lock_slot(
                db, self._locks, invocation.actor.scope_id, invocation.session_id
            )
            run = await locks.lock_run(
                db, self._locks, invocation.actor.scope_id, invocation.run_id
            )
            if (
                slot is None
                or run is None
                or run["session_id"] != invocation.session_id
                or run["status"] != "running"
                or run["desired_control"] is not None
                or run["deadline_at"] <= now
                or expires_at > run["deadline_at"]
            ):
                errors.version_conflict("run is not eligible for approval")
            await revalidate_authority(db, self._locks, invocation.actor, run=run, now=now)
            await self._access.require_request(db, policy_request, spec)
            await db.execute(
                self._insert,
                {
                    "id": approval_id,
                    "scope": invocation.actor.scope_id,
                    "run": invocation.run_id,
                    "actor": invocation.actor.actor_id,
                    "key": key,
                    "hash": binding.arguments_digest,
                    "auth": int(invocation.actor.auth_revision),
                    "policy": json.dumps(
                        policy_request.policy.model_dump(mode="json", by_alias=True)
                    ),
                    "tool": json.dumps(spec.ref.model_dump(mode="json", by_alias=True)),
                    "resources": json.dumps(
                        [ref.model_dump(mode="json", by_alias=True) for ref in binding.resources]
                    ),
                    "expires": expires_at,
                },
            )
            row = (
                (await db.execute(self._by_key, {"scope": binding.scope_id, "key": key}))
                .mappings()
                .one()
            )
            if (
                not self._matches(row, binding, invocation.run_id)
                or row["status"] != "pending"
                or row["expires_at"] <= now
            ):
                errors.version_conflict(
                    "operation approval already has another binding or decision"
                )
            return StoredApproval(
                row["id"],
                approval_request.model_copy(update={"expires_at": row["expires_at"]}),
                row["status"],
                row["revision"],
            )

    async def resolve(
        self, approver: ActorContext, decision: ApprovalDecision
    ) -> ApprovalResolution:
        if decision.idempotency_key != (
            f"approval:{decision.approval_id}:{decision.expected_version}:{decision.decision}"
        ):
            errors.invalid_input("approval decision idempotency key is not canonical")
        now = self._clock()
        async with self._sessions() as db, db.begin():
            # Resolve the parent run before the approval row to keep lock ordering stable.
            identity = (
                (
                    await db.execute(
                        scoped(
                            "SELECT run_id,session_id FROM {t}approval a JOIN {t}run r "
                            "ON r.scope_id=a.scope_id AND r.id=a.run_id "
                            "WHERE a.scope_id=:scope AND a.id=:id",
                            self._schema,
                        ),
                        {"scope": approver.scope_id, "id": decision.approval_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if identity is None:
                errors.forbidden("approval is not in the caller scope")
            await locks.lock_slot(db, self._locks, approver.scope_id, identity["session_id"])
            run = await locks.lock_run(db, self._locks, approver.scope_id, identity["run_id"])
            row = (
                (
                    await db.execute(
                        self._by_id, {"scope": approver.scope_id, "id": decision.approval_id}
                    )
                )
                .mappings()
                .one()
            )
            if run is None or run["status"] != "waiting_user" or run["desired_control"] is not None:
                errors.version_conflict("approval run is no longer waiting for a user")
            if run["deadline_at"] <= now or int(run["authority_revision"]) != row["auth_revision"]:
                errors.forbidden("approval run deadline or authority changed")
            if int(decision.expected_version) != int(row["revision"]):
                errors.version_conflict("approval revision changed")
            if row["status"] != "pending":
                errors.version_conflict("approval was already decided")
            # Lock the Journal session before resource ACL locks to avoid a
            # Session-SHARE/resource-UPDATE cycle with a concurrent revoker.
            await db.execute(
                scoped(
                    "SELECT id FROM {t}session WHERE scope_id=:scope AND id=:id FOR UPDATE",
                    self._schema,
                ),
                {"scope": approver.scope_id, "id": identity["session_id"]},
            )
            if row["expires_at"] <= now:
                resources = tuple(ResourceRef.model_validate(item) for item in row["resource_refs"])
                await self._access.require_decision(
                    db, approver, row["actor_id"], row["run_id"], identity["session_id"], resources
                )
                await db.execute(
                    self._expire,
                    {"scope": approver.scope_id, "id": decision.approval_id, "now": now},
                )
                await append_approval_update(
                    db,
                    self._engine,
                    self._schema,
                    approver,
                    run,
                    dict(row),
                    status="expired",
                    revision=int(row["revision"]) + 1,
                    now=now,
                    reason="审批已过期，本次操作未执行。",
                )
                return ApprovalResolution(
                    decision.approval_id, row["run_id"], "expired", int(row["revision"]) + 1, None
                )
            resources = tuple(ResourceRef.model_validate(item) for item in row["resource_refs"])
            await self._access.require_decision(
                db, approver, row["actor_id"], row["run_id"], identity["session_id"], resources
            )
            revised = (
                await db.execute(
                    self._resolve,
                    {
                        "scope": approver.scope_id,
                        "id": decision.approval_id,
                        "expected": int(decision.expected_version),
                        "decision": decision.decision,
                        "actor": approver.actor_id,
                        "now": now,
                    },
                )
            ).scalar_one_or_none()
            if revised is None:
                errors.version_conflict("approval decision lost its compare-and-swap")
            await append_approval_update(
                db,
                self._engine,
                self._schema,
                approver,
                run,
                dict(row),
                status=decision.decision,
                revision=int(revised),
                now=now,
                reason={
                    "approved": "已批准，等待执行原操作。",
                    "rejected": "已拒绝，操作不会执行。",
                    "cancelled": "已取消审批，操作不会执行。",
                }[decision.decision],
            )
            return ApprovalResolution(
                decision.approval_id,
                row["run_id"],
                decision.decision,
                int(revised),
                f"approval:{decision.approval_id}:{revised}"
                if decision.decision == "approved"
                else None,
            )

    async def read_for_resume(
        self, request: PolicyRequest, spec: ToolSpec, approval_id: UUID
    ) -> ApprovalResolution:
        """Recheck an approval and its current authority before reusing the original call."""
        invocation = request.invocation
        now = self._clock()
        if invocation.operation_id is None or spec.ref != invocation.call.tool:
            errors.invalid_input("approval resume requires the original operation")
        binding = ApprovalBinding(
            scope_id=invocation.actor.scope_id,
            actor_id=invocation.actor.actor_id,
            session_id=invocation.session_id,
            tool=spec.ref,
            operation_id=invocation.operation_id,
            arguments_digest=content_digest(invocation.call.arguments),
            policy=request.policy,
            auth_revision=invocation.actor.auth_revision,
            resources=request.resources,
        )
        async with self._sessions() as db, db.begin():
            await locks.lock_slot(db, self._locks, binding.scope_id, binding.session_id)
            run = await locks.lock_run(db, self._locks, binding.scope_id, invocation.run_id)
            if (
                run is None
                or run["session_id"] != binding.session_id
                or run["status"] != "running"
                or run["desired_control"] is not None
                or run["deadline_at"] <= now
            ):
                errors.version_conflict("approval resume run is no longer active")
            row = (
                (await db.execute(self._by_id, {"scope": binding.scope_id, "id": approval_id}))
                .mappings()
                .one_or_none()
            )
            if row is None or not self._matches(row, binding, invocation.run_id):
                errors.forbidden("approval no longer matches the original tool call")
            status = row["status"]
            if row["expires_at"] <= now and status in {"pending", "approved"}:
                status = "expired"
            if status == "approved":
                await revalidate_authority(db, self._locks, invocation.actor, run=run, now=now)
                await self._access.require_request(db, request, spec)
            return ApprovalResolution(
                approval_id,
                invocation.run_id,
                status,
                int(row["revision"]),
                f"approval:{approval_id}:{row['revision']}" if status == "approved" else None,
            )

    @staticmethod
    def _matches(row: Any, binding: ApprovalBinding, run_id: UUID) -> bool:
        return cast(
            bool,
            (
                row["id"]
                == uuid5(NAMESPACE_URL, f"approval:{binding.scope_id}:tool:{binding.operation_id}")
                and row["actor_id"] == binding.actor_id
                and row["run_id"] == run_id
                and row["request_key"] == f"tool:{binding.operation_id}"
                and row["input_hash"] == binding.arguments_digest
                and row["auth_revision"] == int(binding.auth_revision)
                and row["policy_ref"] == binding.policy.model_dump(mode="json", by_alias=True)
                and row["tool_revision"] == binding.tool.model_dump(mode="json", by_alias=True)
                and row["resource_refs"]
                == [ref.model_dump(mode="json", by_alias=True) for ref in binding.resources]
            ),
        )


__all__ = ["ApprovalAccess", "ApprovalResolution", "SqlApprovalService", "StoredApproval"]

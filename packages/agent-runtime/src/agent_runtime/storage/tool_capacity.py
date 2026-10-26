"""Durable tool-group slots shared by every Worker in a deployment."""

import hashlib
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.capacity import (
    CapacityKey,
    CapacityObserveRequest,
    CapacityReleaseRequest,
    CapacityReservation,
    CapacityReserveRequest,
)
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.registry import CapabilityManifest, ToolCapability
from agent_runtime.contracts.tools import ToolInvocation, ToolSpec
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.capacity import SqlSystemCapacityStore
from agent_runtime.storage.uow.schema import scoped


class SqlToolCapacity:
    """The host declares group limits at bootstrap; runtime never creates a pool."""

    def __init__(self, store: SqlSystemCapacityStore, *, deployment_id: str) -> None:
        if not deployment_id:
            raise ValueError("deployment id is required for tool capacity")
        self._store, self._deployment_id = store, deployment_id

    @property
    def engine(self) -> AsyncEngine:
        return self._store.engine

    @property
    def schema(self) -> str:
        return self._store.schema

    def key(self, scope_id: str, spec: ToolSpec) -> CapacityKey:
        group = f"{scope_id}\0{spec.concurrency_group}".encode()
        return CapacityKey(
            deployment_id=self._deployment_id,
            resource_kind="tool_slot",
            resource_version="1",
            resource_key="tool:" + hashlib.sha256(group).hexdigest(),
        )

    async def reserve(self, invocation: ToolInvocation, spec: ToolSpec) -> CapacityReservation:
        key = self.key(invocation.actor.scope_id, spec)
        view = await self._store.observe(CapacityObserveRequest(key=key))
        if int(view.capacity) != spec.max_concurrency:
            errors.version_conflict("declared tool group capacity differs from its pinned spec")
        return await self._store.reserve(
            CapacityReserveRequest(
                key=key,
                amount=1,
                holder_ref=str(invocation.run_id),
                request_id=(
                    f"tool:{invocation.operation_id}"
                    if spec.side_effect == "external" and invocation.operation_id is not None
                    else f"tool:{uuid4()}"
                ),
                lease_until=invocation.deadline_at,
                deadline_at=invocation.deadline_at,
            )
        )

    async def reserve_in_transaction(
        self, db: AsyncSession, invocation: ToolInvocation, spec: ToolSpec
    ) -> CapacityReservation:
        key = self.key(invocation.actor.scope_id, spec)
        if invocation.operation_id is None:
            errors.invalid_input("background capacity requires an operation identity")
        return await self._store.reserve_in_transaction(
            db,
            CapacityReserveRequest(
                key=key,
                amount=1,
                holder_ref=str(invocation.run_id),
                request_id=f"tool:{invocation.operation_id}",
                lease_until=invocation.deadline_at,
                deadline_at=invocation.deadline_at,
            ),
            expected_capacity=spec.max_concurrency,
        )

    async def release(self, invocation: ToolInvocation, reservation: CapacityReservation) -> None:
        result = await self._store.release(
            CapacityReleaseRequest(
                key=reservation.key,
                reservation_id=reservation.reservation_id,
                request_id=f"tool-release:{reservation.reservation_id}",
                reason="settled",
                holder_state="terminal",
            )
        )
        if result.outcome == "rejected":
            errors.version_conflict("tool capacity release was refused")

    async def settle_committed(self, invocation: ToolInvocation, spec: ToolSpec) -> None:
        """Retry a lost slot-release response without reserving a second slot."""
        if spec.side_effect != "external" or invocation.operation_id is None:
            return
        key = self.key(invocation.actor.scope_id, spec)
        row = await self._store.reservation_by_request(key, f"tool:{invocation.operation_id}")
        if row is None or row["status"] != "reserved":
            return
        reservation = CapacityReservation(
            key=key,
            reservation_id=row["id"],
            amount=int(row["amount"]),
            status="reserved",
            version=str(row["version"]),
            lease_until=row["lease_until"],
            holder_ref=str(row["holder_ref"]),
        )
        await self.release(invocation, reservation)

    async def settle_in_transaction(
        self, db: AsyncSession, invocation: ToolInvocation, spec: ToolSpec
    ) -> None:
        """Release a terminal external effect in the same transaction as its result."""
        if spec.side_effect != "external" or invocation.operation_id is None:
            errors.invalid_input("terminal tool capacity needs an external operation")
        await self.settle_operation_in_transaction(
            db, invocation.actor.scope_id, invocation.operation_id, spec
        )

    async def settle_operation_in_transaction(
        self, db: AsyncSession, scope_id: str, operation_id: UUID, spec: ToolSpec
    ) -> None:
        """Release one verified terminal operation, without a live Run lease."""
        key = self.key(scope_id, spec)
        row = await self._store.reservation_by_request_in_transaction(
            db, key, f"tool:{operation_id}"
        )
        if row is None:
            errors.version_conflict("terminal external tool has no capacity reservation")
        result = await self._store.release_in_transaction(
            db,
            CapacityReleaseRequest(
                key=key,
                reservation_id=row["id"],
                request_id=f"tool-release:{row['id']}",
                reason="settled",
                holder_state="terminal",
            ),
        )
        if result.outcome == "rejected":
            errors.version_conflict("terminal tool capacity release was refused")

    async def settle_execution_in_transaction(
        self, db: AsyncSession, scope_id: str, run_id: UUID, operation_id: UUID
    ) -> None:
        """Resolve the immutable ToolSpec from persisted facts after remote exit evidence."""
        call = (
            (
                await db.execute(
                    scoped(
                        "SELECT spec_revision FROM {t}tool_call WHERE scope_id=:scope "
                        "AND run_id=:run AND operation_id=:operation",
                        self._store.schema,
                    ),
                    {"scope": scope_id, "run": run_id, "operation": operation_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if call is None:
            errors.version_conflict("terminal execution has no bound tool call")
        ref = CapabilityRef.model_validate(call["spec_revision"])
        manifest_row = (
            (
                await db.execute(
                    scoped(
                        "SELECT r.manifest,r.content_digest,c.kind FROM {t}capability c "
                        "JOIN {t}capability_revision r ON r.scope_id=c.scope_id "
                        "AND r.capability_id=c.id WHERE c.scope_id=:scope "
                        "AND c.namespace=:namespace AND c.key=:key AND r.revision=:revision",
                        self._store.schema,
                    ),
                    {
                        "scope": scope_id,
                        "namespace": ref.key.namespace,
                        "key": ref.key.key,
                        "revision": int(ref.revision),
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        if manifest_row is None or manifest_row["kind"] != "tool":
            errors.version_conflict("terminal tool revision is unavailable")
        manifest = CapabilityManifest.model_validate(manifest_row["manifest"])
        if (
            manifest.ref != ref
            or not isinstance(manifest.spec, ToolCapability)
            or manifest_row["content_digest"] != manifest.content_digest
            or content_digest(manifest.spec.spec.model_dump(mode="json", by_alias=True))
            != manifest.content_digest
        ):
            errors.version_conflict("terminal tool revision differs from its persisted manifest")
        await self.settle_operation_in_transaction(db, scope_id, operation_id, manifest.spec.spec)


__all__ = ["SqlToolCapacity"]

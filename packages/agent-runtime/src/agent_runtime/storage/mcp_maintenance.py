"""Bind a claimed maintenance lease to pinned MCP effect inspection."""

from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.credentials import GrantRef, MaintenanceCredentialLease, ScopeRequest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.tools import ToolSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.work_claims import MaintenanceClaimedWork
from agent_runtime.credentials.broker import CredentialBroker
from agent_runtime.credentials.ports import BrokerBlobs
from agent_runtime.mcp.host import McpMaintenanceInspector
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlMcpMaintenanceInspectorFactory:
    """Create one inspector per claim; all grants are fenced again by the SQL store."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        spec: ToolSpec,
        broker: CredentialBroker,
        blobs: BrokerBlobs,
    ) -> None:
        if spec.mcp is None or spec.mcp.lookup_destination is None:
            raise ValueError("MCP maintenance needs a fixed lookup")
        self._schema = require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._actor, self._spec = actor, spec
        self._broker, self._blobs = broker, blobs

    def __call__(self, work: MaintenanceClaimedWork) -> McpMaintenanceInspector:
        if work.kind != "reconcile_execution" or work.lease.scope_id != self._actor.scope_id:
            errors.forbidden("MCP maintenance claim is not in scope")
        binding = self._spec.mcp
        assert binding is not None

        async def issue(key: str, destination: CapabilityRef, action: str) -> GrantRef:
            if (
                (action == "read" and destination != binding.lookup_destination)
                or (action == "write" and destination != binding.cancel_destination)
                or action not in {"read", "write"}
            ):
                errors.forbidden("MCP maintenance destination differs from its pinned tool")
            async with self._sessions() as db:
                row = (
                    (
                        await db.execute(
                            scoped(
                                "SELECT e.operation_id,e.generation,e.run_id,r.session_id "
                                "FROM {t}execution e JOIN {t}run r "
                                "ON r.scope_id=e.scope_id AND r.id=e.run_id "
                                "WHERE e.scope_id=:s AND e.id=:i "
                                "AND e.supervisor_work_id=:w AND e.remote_request_key=:k",
                                self._schema,
                            ),
                            {
                                "s": self._actor.scope_id,
                                "i": UUID(work.subject_ref.resource_id),
                                "w": work.lease.work_id,
                                "k": key,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
            if row is None or str(row["generation"]) != work.subject_ref.resource_version:
                errors.fenced("MCP maintenance effect identity changed")
            expires = min(work.lease.work_lease_until, datetime.now(UTC) + timedelta(seconds=60))
            if expires <= datetime.now(UTC):
                errors.fenced("MCP maintenance work lease expired")
            scope = ScopeRequest(
                actor=self._actor,
                session_id=row["session_id"],
                run_id=row["run_id"],
                execution_id=UUID(work.subject_ref.resource_id),
                execution_generation=str(row["generation"]),
                operation_id=row["operation_id"],
                maintenance_lease=MaintenanceCredentialLease(
                    work_id=work.lease.work_id,
                    owner_id=work.lease.owner_id,
                    epoch=work.lease.work_epoch,
                    lease_until=work.lease.work_lease_until,
                ),
                destination=destination,
                actions=(action,),
                policy_decision_id=uuid5(
                    NAMESPACE_URL,
                    f"mcp-maintenance:{self._actor.scope_id}:{work.lease.work_id}:"
                    f"{work.lease.work_epoch}:{destination.key.key}",
                ),
                input_digest=content_digest({"operation_key": key}),
                expires_at=expires,
                max_requests=2,
                max_response_bytes=4096,
            )
            return await self._broker.authorize(self._actor, scope)

        return McpMaintenanceInspector(
            spec=self._spec,
            actor=self._actor,
            broker=self._broker,
            blobs=self._blobs,
            issue_grant=issue,
        )


__all__ = ["SqlMcpMaintenanceInspectorFactory"]

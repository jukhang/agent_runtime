"""Durable sandbox dispatch intents. No ambiguous intent is dispatched twice."""

import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.policy import PolicyAllow, PolicyRequest
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.tools import ToolInvocation, ToolSpec
from agent_runtime.policy_validation import validate_policy_decision
from agent_runtime.sandbox.runtime import ExecutionHandle, ExecutionStart
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.tool_host import TransactionAuthorizer
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.tool_validation import validate_tool_invocation

ExecutionAuthorizer = Callable[[AsyncSession, ToolInvocation, ExecutionStart], Awaitable[None]]


def execution_payload(request: ExecutionStart) -> dict[str, object]:
    binding = request.binding
    return {
        "kind": "sandbox_dispatch",
        "scopeId": binding.scope_id,
        "executionId": str(binding.execution_id),
        "generation": binding.generation,
        "workspaceGeneration": binding.workspace_generation,
        "imageDigest": binding.image_digest,
        "sandboxId": binding.sandbox_id,
        "command": request.command,
        "workingDirectory": request.working_directory,
        "timeoutSeconds": request.timeout_seconds,
        "deadlineAt": request.deadline_at.isoformat(),
        **({"limits": request.limits.payload()} if request.limits is not None else {}),
    }


class SqlSandboxStartAuthority:
    """One invocation's authority, with a host-owned live transactional authorizer.

    The authorizer must lock/revalidate the current main/child lease, scope, ACL,
    workspace generation/image, concurrency and execution budget. It must also
    verify that the trusted command was derived from this invocation. This port
    deliberately has no permissive default or automatic resource discovery.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        invocation: ToolInvocation,
        spec: ToolSpec,
        grant: PolicyAllow,
        resources: tuple[ResourceRef, ...],
        authorize: TransactionAuthorizer,
        authorize_execution: ExecutionAuthorizer,
        payloads: BlobContextPayloadStore,
        capacity: SqlToolCapacity | None = None,
    ) -> None:
        if invocation.operation_id is None or spec.side_effect == "none":
            raise ValueError("sandbox execution requires an effectful operation")
        self._schema = require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._invocation, self._spec, self._grant = invocation, spec, grant
        self._resources, self._authorize, self._payloads = resources, authorize, payloads
        self._authorize_execution = authorize_execution
        if capacity is not None and (
            capacity.engine is not engine or capacity.schema != self._schema
        ):
            raise ValueError("sandbox dispatch capacity must use the same engine and schema")
        self._capacity = capacity

    @property
    def capacity(self) -> SqlToolCapacity | None:
        return self._capacity

    def _payload(self, request: ExecutionStart) -> dict[str, object]:
        return {
            **execution_payload(request),
            "runId": str(self._invocation.run_id),
            "toolCallId": str(self._invocation.call.tool_call_id),
            "attemptId": str(self._invocation.model_attempt_id),
            "contextSnapshotId": str(self._invocation.context_snapshot_id),
            "call": self._invocation.call.model_dump(mode="json", by_alias=True),
            "invocation": self._invocation.model_dump(mode="json", by_alias=True),
            "spec": self._spec.model_dump(mode="json", by_alias=True),
        }

    def _identity(self, request: ExecutionStart) -> tuple[str, UUID, str]:
        invocation = self._invocation
        if request.binding.scope_id != invocation.actor.scope_id:
            errors.forbidden("execution belongs to another scope")
        expected = uuid5(
            NAMESPACE_URL, f"execution:{invocation.actor.scope_id}:{invocation.operation_id}"
        )
        if request.binding.execution_id != expected:
            errors.invalid_input("execution identity is not derived from its operation")
        if request.deadline_at > invocation.deadline_at:
            errors.forbidden("sandbox deadline exceeds the tool grant")
        if request.timeout_seconds > self._spec.timeout_seconds:
            errors.forbidden("sandbox timeout exceeds the tool specification")
        key = str(invocation.operation_id)
        work = uuid5(NAMESPACE_URL, f"sandbox-supervise:{expected}:{request.binding.generation}")
        return key, work, content_digest(self._payload(request))

    async def _check(
        self, db: AsyncSession, request: ExecutionStart, *, verify_execution: bool = True
    ) -> str:
        invocation, spec = self._invocation, self._spec
        now = datetime.now(UTC)
        validate_tool_invocation(spec, invocation, now=now)
        validate_policy_decision(
            PolicyRequest(
                invocation=invocation, policy=spec.approval_policy, resources=self._resources
            ),
            self._grant,
            now=now,
        )
        await self._authorize(db, invocation, self._resources)
        if verify_execution:
            await self._authorize_execution(db, invocation, request)
        row = (
            (
                await db.execute(
                    scoped(
                        "SELECT r.task_id FROM {t}run r JOIN {t}tool_call c "
                        "ON c.scope_id=r.scope_id AND c.run_id=r.id "
                        "JOIN {t}model_attempt a ON a.scope_id=c.scope_id AND a.id=c.attempt_id "
                        "WHERE r.scope_id=:scope AND r.id=:run AND c.id=:call "
                        "AND c.status='pending' AND c.attempt_id=:attempt AND a.status='completed' "
                        "AND c.provider_call_id=:provider "
                        "AND (c.operation_id IS NULL OR c.operation_id=:operation) "
                        "AND a.run_id=r.id AND a.context_snapshot_id=:snapshot "
                        "AND c.spec_revision=CAST(:spec AS jsonb) "
                        "AND c.input_hash=:hash FOR UPDATE OF c",
                        self._schema,
                    ),
                    {
                        "scope": invocation.actor.scope_id,
                        "run": invocation.run_id,
                        "call": invocation.call.tool_call_id,
                        "attempt": invocation.model_attempt_id,
                        "snapshot": invocation.context_snapshot_id,
                        "provider": str(invocation.call.tool_call_id),
                        "operation": invocation.operation_id,
                        "spec": json.dumps(spec.ref.model_dump(mode="json", by_alias=True)),
                        "hash": content_digest(invocation.call.arguments),
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            errors.forbidden("sandbox has no current persisted tool decision")
        return str(row["task_id"])

    async def reserve(self, request: ExecutionStart) -> ExecutionHandle | None:
        key, work, digest = self._identity(request)
        if request.deadline_at <= datetime.now(UTC):
            errors.version_conflict("sandbox deadline expired before reservation")
        ref = await self._payloads.put(request.binding.scope_id, self._payload(request), digest)
        invocation, binding = self._invocation, request.binding
        now = datetime.now(UTC)
        async with self._sessions() as db, db.begin():
            task = await self._check(db, request, verify_execution=False)
            if self._capacity is not None:
                await self._capacity.reserve_in_transaction(db, invocation, self._spec)
            await self._authorize_execution(db, invocation, request)
            # Serialize all sandbox ownership assignments, including cross-scope
            # mistakes by a host. A sandbox ID is never reassigned to another execution.
            await db.execute(
                scoped("SELECT pg_advisory_xact_lock(hashtextextended(:sandbox,0))", self._schema),
                {"sandbox": binding.sandbox_id},
            )
            shared = (
                await db.execute(
                    scoped(
                        "SELECT id FROM {t}execution WHERE sandbox_id=:sandbox AND id<>:id LIMIT 1",
                        self._schema,
                    ),
                    {"sandbox": binding.sandbox_id, "id": binding.execution_id},
                )
            ).first()
            if shared is not None:
                errors.forbidden("sandbox identity is already owned by another execution")
            await db.execute(
                scoped(
                    "INSERT INTO {t}operation "
                    "(id,scope_id,task_id,idempotency_key,input_hash,status) "
                    "VALUES (:id,:scope,:task,:key,:hash,'prepared') "
                    "ON CONFLICT (scope_id,task_id,idempotency_key) DO NOTHING",
                    self._schema,
                ),
                {
                    "id": invocation.operation_id,
                    "scope": binding.scope_id,
                    "task": task,
                    "key": key,
                    "hash": digest,
                },
            )
            operation = (
                (
                    await db.execute(
                        scoped(
                            "SELECT id,input_hash,status FROM {t}operation WHERE scope_id=:scope "
                            "AND task_id=:task AND idempotency_key=:key FOR UPDATE",
                            self._schema,
                        ),
                        {"scope": binding.scope_id, "task": task, "key": key},
                    )
                )
                .mappings()
                .one()
            )
            if operation["id"] != invocation.operation_id or operation["input_hash"] != digest:
                errors.version_conflict("sandbox operation input changed")
            existing = (
                (
                    await db.execute(
                        scoped(
                            "SELECT e.*,w.payload_ref FROM {t}execution e JOIN {t}work_item w "
                            "ON w.scope_id=e.scope_id AND w.id=e.supervisor_work_id "
                            "WHERE e.scope_id=:scope AND e.remote_request_key=:key FOR UPDATE OF e",
                            self._schema,
                        ),
                        {"scope": binding.scope_id, "key": key},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if existing is not None:
                if (
                    existing["id"] != binding.execution_id
                    or existing["run_id"] != invocation.run_id
                    or existing["operation_id"] != invocation.operation_id
                    or existing["supervisor_work_id"] != work
                    or existing["sandbox_id"] != binding.sandbox_id
                    or existing["generation"] != binding.generation
                    or existing["payload_ref"] != ref
                ):
                    errors.version_conflict("sandbox dispatch binding changed")
                if (
                    existing["remote_execution_id"] is not None
                    and existing["cleanup_status"] == "pending"
                    and existing["status"]
                    in {"running", "completed", "failed", "cancelled", "unknown"}
                ):
                    return ExecutionHandle(binding, existing["remote_execution_id"])
                errors.version_conflict("sandbox dispatch is unresolved; refusing replay")
            if operation["status"] != "prepared":
                errors.version_conflict("sandbox operation cannot be dispatched again")
            subject = {
                "resourceType": "execution",
                "resourceId": str(binding.execution_id),
                "resourceVersion": str(binding.generation),
                "scope": {"scopeId": binding.scope_id},
            }
            await db.execute(
                scoped(
                    "INSERT INTO {t}work_item "
                    "(id,scope_id,run_id,subject_ref,kind,dedup_key,status,payload_ref,"
                    "wake_reason,priority,lease_epoch,attempt_count,available_at,deadline_at) "
                    "VALUES (:id,:scope,:run,CAST(:subject AS jsonb),'reconcile_execution',:dedup,"
                    "'pending',:ref,NULL,1000,0,0,:available,:deadline)",
                    self._schema,
                ),
                {
                    "id": work,
                    "scope": binding.scope_id,
                    "run": invocation.run_id,
                    "subject": json.dumps(subject),
                    "dedup": f"sandbox:{binding.execution_id}:{binding.generation}",
                    "ref": ref,
                    "available": now + timedelta(seconds=15),
                    "deadline": request.deadline_at + timedelta(minutes=10),
                },
            )
            await db.execute(
                scoped(
                    "INSERT INTO {t}execution (id,scope_id,operation_id,run_id,supervisor_work_id,"
                    "remote_request_key,status,log_cursor,cleanup_status,"
                    "sandbox_id,generation,deadline_at) "
                    "VALUES (:id,:scope,:operation,:run,:work,:key,"
                    "'starting','0','pending',:sandbox,"
                    ":generation,:deadline)",
                    self._schema,
                ),
                {
                    "id": binding.execution_id,
                    "scope": binding.scope_id,
                    "operation": invocation.operation_id,
                    "run": invocation.run_id,
                    "work": work,
                    "key": key,
                    "sandbox": binding.sandbox_id,
                    "generation": binding.generation,
                    "deadline": request.deadline_at,
                },
            )
            await db.execute(
                scoped(
                    "UPDATE {t}operation SET status='dispatching',updated_at=:now "
                    "WHERE scope_id=:scope AND id=:id",
                    self._schema,
                ),
                {"scope": binding.scope_id, "id": invocation.operation_id, "now": now},
            )
            await db.execute(
                scoped(
                    "UPDATE {t}tool_call SET operation_id=:operation,updated_at=:now "
                    "WHERE scope_id=:scope AND id=:id",
                    self._schema,
                ),
                {
                    "scope": binding.scope_id,
                    "id": invocation.call.tool_call_id,
                    "operation": invocation.operation_id,
                    "now": now,
                },
            )
        return None

    async def started(self, request: ExecutionStart, handle: ExecutionHandle) -> None:
        _, work, digest = self._identity(request)
        if handle.binding != request.binding:
            errors.forbidden("sandbox returned another execution binding")
        async with self._sessions() as db, db.begin():
            await self._check(db, request)
            changed = await db.execute(
                scoped(
                    "UPDATE {t}execution e SET remote_execution_id=:command,status='running',"
                    "updated_at=:now FROM {t}work_item w,{t}operation o "
                    "WHERE e.scope_id=:scope AND e.id=:id AND e.run_id=:run "
                    "AND e.generation=:generation AND e.supervisor_work_id=:work "
                    "AND e.sandbox_id=:sandbox AND e.status IN ('starting','unknown') "
                    "AND e.remote_execution_id IS NULL AND w.scope_id=e.scope_id "
                    "AND w.id=e.supervisor_work_id AND w.status IN ('pending','leased') "
                    "AND o.scope_id=e.scope_id AND o.id=e.operation_id AND o.input_hash=:digest "
                    "AND o.status IN ('dispatching','unknown')",
                    self._schema,
                ),
                {
                    "command": handle.command_id,
                    "scope": request.binding.scope_id,
                    "id": request.binding.execution_id,
                    "run": self._invocation.run_id,
                    "generation": request.binding.generation,
                    "work": work,
                    "sandbox": request.binding.sandbox_id,
                    "digest": digest,
                    "now": datetime.now(UTC),
                },
            )
            if getattr(changed, "rowcount", None) != 1:
                errors.version_conflict("sandbox handle save lost its dispatch fence")

    async def unknown(self, request: ExecutionStart) -> None:
        key, work, digest = self._identity(request)
        async with self._sessions() as db, db.begin():
            changed = await db.execute(
                scoped(
                    "UPDATE {t}execution e SET status='unknown',updated_at=:now "
                    "FROM {t}operation o WHERE e.scope_id=:scope AND e.id=:id "
                    "AND e.operation_id=:operation AND e.run_id=:run AND e.generation=:generation "
                    "AND e.remote_request_key=:key AND e.supervisor_work_id=:work "
                    "AND e.status IN ('starting','running','unknown') "
                    "AND o.scope_id=e.scope_id AND o.id=e.operation_id AND o.input_hash=:digest "
                    "AND o.status IN ('dispatching','unknown')",
                    self._schema,
                ),
                {
                    "scope": request.binding.scope_id,
                    "id": request.binding.execution_id,
                    "operation": self._invocation.operation_id,
                    "run": self._invocation.run_id,
                    "generation": request.binding.generation,
                    "key": key,
                    "work": work,
                    "digest": digest,
                    "now": datetime.now(UTC),
                },
            )
            if getattr(changed, "rowcount", None) == 1:
                await db.execute(
                    scoped(
                        "UPDATE {t}operation SET status='unknown',updated_at=:now "
                        "WHERE scope_id=:scope AND id=:id AND status='dispatching'",
                        self._schema,
                    ),
                    {
                        "scope": request.binding.scope_id,
                        "id": self._invocation.operation_id,
                        "now": datetime.now(UTC),
                    },
                )

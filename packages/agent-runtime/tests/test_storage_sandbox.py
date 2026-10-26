"""Real isolated SQL sandbox intents and maintenance, not a live Docker gate."""

import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

pytest.importorskip("sqlalchemy")
import sqlalchemy as sa  # noqa: E402
from test_decision_validation import tool_action  # noqa: E402
from test_storage_subagents import claim_request  # noqa: E402
from test_storage_uow import _row, _seed_attempt, actor, engine_for, lease, run  # noqa: E402
from test_storage_uow import no_leftover_schemas as no_leftover_schemas  # noqa: E402
from test_storage_uow import schema as schema  # noqa: E402
from test_storage_uow import world as world  # noqa: E402

from agent_runtime.context_payloads import BlobContextPayloadStore  # noqa: E402
from agent_runtime.contracts.canonical import content_digest  # noqa: E402
from agent_runtime.contracts.decisions import decision_adapter  # noqa: E402
from agent_runtime.contracts.failure import AgentRuntimeError  # noqa: E402
from agent_runtime.contracts.policy import PolicyAllow, PolicyRequest  # noqa: E402
from agent_runtime.contracts.references import ResourceRef, ResourceScope  # noqa: E402
from agent_runtime.contracts.requests import ControlRequest  # noqa: E402
from agent_runtime.contracts.tools import ToolInvocation, ToolSpec  # noqa: E402
from agent_runtime.contracts.work_claims import MaintenanceClaimedWork  # noqa: E402
from agent_runtime.policy_validation import policy_input_digest  # noqa: E402
from agent_runtime.sandbox.runtime import (  # noqa: E402
    ExecutionBinding,
    ExecutionStart,
    RemoteStatus,
    SandboxExecutionRuntime,
)
from agent_runtime.sandbox.supervision import ExecutionLimits, ExecutionUsage  # noqa: E402
from agent_runtime.storage.context_blobs import SqlContextBlobBackend  # noqa: E402
from agent_runtime.storage.sandbox_start import SqlSandboxStartAuthority  # noqa: E402
from agent_runtime.storage.sandbox_worker import (  # noqa: E402
    SqlExecutionMaintenanceRouter,
    SqlSandboxMaintenanceWorker,
)
from agent_runtime.storage.scheduler import SqlScheduler  # noqa: E402
from agent_runtime.storage.uow import SqlStateUnitOfWork, errors, facts, locks  # noqa: E402
from agent_runtime.storage.uow.authority import revalidate_authority  # noqa: E402

pytestmark = pytest.mark.skipif(not os.getenv("AGENT_RUNTIME_TEST_DSN"), reason="real PG required")


async def setup(world, engine, *, image_digest="python@sha256:" + "a" * 64):
    attempt = await _seed_attempt(world)
    snapshot = (
        await _row(world, "SELECT context_snapshot_id FROM model_attempt WHERE id=:id", id=attempt)
    )["context_snapshot_id"]
    call = decision_adapter.validate_python(tool_action()).payload.validated_calls[0]
    spec = ToolSpec(
        ref=call.tool,
        description="sandbox",
        source=call.tool,
        kind="background",
        side_effect="external",
        input_schema=call.tool,
        output_schema=call.tool,
        permissions=(),
        approval_policy=call.tool,
        idempotency="reconcile",
        timeout_seconds=60,
        concurrency_group="sandbox",
        max_concurrency=1,
        cancellation="stop",
        public_result_policy="full",
    )
    invocation = ToolInvocation(
        actor=actor(world=world),
        session_id=world.session_id,
        run_id=world.run_id,
        model_attempt_id=attempt,
        context_snapshot_id=snapshot,
        context_version="1",
        call=call,
        operation_id=uuid4(),
        deadline_at=datetime.now(UTC) + timedelta(minutes=2),
    )
    policy = PolicyRequest(invocation=invocation, policy=spec.approval_policy, resources=())
    grant = PolicyAllow(
        decision="allow",
        decision_id=uuid4(),
        policy=spec.approval_policy,
        input_digest=policy_input_digest(policy),
        evidence_refs=(
            ResourceRef(
                resource_type="workspace",
                resource_id=str(world.workspace_id),
                resource_version="1",
                scope=ResourceScope(
                    scope_id=world.scope, session_id=str(world.session_id), owner_id=world.actor_id
                ),
            ),
        ),
        evaluated_at=datetime.now(UTC),
        expires_at=invocation.deadline_at,
        grant_ref="sandbox-test-grant",
    )
    async with engine.begin() as db:
        await db.execute(
            sa.text(
                "INSERT INTO tool_call (id,scope_id,run_id,attempt_id,provider_call_id,"
                "spec_revision,input_hash,status) VALUES (:id,:scope,:run,:attempt,:provider,"
                "CAST(:spec AS jsonb),:hash,'pending')"
            ),
            {
                "id": call.tool_call_id,
                "scope": world.scope,
                "run": world.run_id,
                "attempt": attempt,
                "provider": str(call.tool_call_id),
                "spec": json.dumps(call.tool.model_dump(mode="json", by_alias=True)),
                "hash": content_digest(call.arguments),
            },
        )
    statements = locks.build_lock_statements(world.schema)
    token = lease(world)

    async def authorize(db, current, resources):
        assert current == invocation and resources == ()
        facts.verify_slot(
            await locks.lock_slot(db, statements, world.scope, world.session_id), token
        )
        row = facts.verify_run(
            await locks.lock_run(db, statements, world.scope, world.run_id),
            current.actor,
            token,
            now=datetime.now(UTC),
        )
        await revalidate_authority(db, statements, current.actor, run=row, now=datetime.now(UTC))
        facts.verify_work(
            await locks.lock_work_item(db, statements, world.scope, world.work_id),
            token,
            now=datetime.now(UTC),
        )
        if row["status"] != "running" or row["desired_control"] is not None:
            errors.control_pending("test sandbox main run is controlled")

    async def authorize_execution(db, current, request):
        generation = (
            await db.execute(
                sa.text(
                    "SELECT generation FROM workspace WHERE scope_id=:scope AND id=:id FOR UPDATE"
                ),
                {"scope": world.scope, "id": world.workspace_id},
            )
        ).scalar_one()
        if request.binding.workspace_generation != generation:
            errors.version_conflict("workspace changed")
        assert request.binding.image_digest == image_digest

    payloads = BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema))
    authority = SqlSandboxStartAuthority(
        engine,
        world.schema,
        invocation=invocation,
        spec=spec,
        grant=grant,
        resources=(),
        authorize=authorize,
        authorize_execution=authorize_execution,
        payloads=payloads,
    )
    request = ExecutionStart(
        ExecutionBinding(
            world.scope,
            uuid5(NAMESPACE_URL, f"execution:{world.scope}:{invocation.operation_id}"),
            1,
            1,
            image_digest,
            f"sandbox-{uuid4()}",
        ),
        "python -c 'print(391)'",
        "/tmp",
        30,
        invocation.deadline_at,
    )
    return authority, request, payloads


class Backend:
    sends = 0
    kills = 0
    closes = 0
    interrupts = 0
    fail_start = False
    observation = RemoteStatus(True, None)

    def __init__(self, sandbox_id):
        self.sandbox_id = sandbox_id

    async def start(self, request, timeout_seconds):
        self.sends += 1
        if self.fail_start:
            raise TimeoutError("injected lost dispatch response")
        return "command-1"

    async def status(self, command):
        return self.observation

    async def interrupt(self, command):
        self.interrupts += 1
        if hasattr(self, "before_interrupt"):
            await self.before_interrupt()

    async def destroy(self):
        self.kills += 1
        return self.kills > 1

    async def aclose(self):
        self.closes += 1


@pytest.mark.parametrize(
    "scenario",
    ["reuse", "ambiguous", "input_conflict", "fenced_save", "revoked", "workspace_conflict"],
)
def test_sql_sandbox_intents(world, scenario):
    async def check():
        engine = engine_for(world.schema)
        try:
            authority, request, _ = await setup(world, engine)
            backend = Backend(request.binding.sandbox_id)
            runtime = SandboxExecutionRuntime(backend, authority)
            if scenario in {"revoked", "workspace_conflict"}:
                async with engine.begin() as db:
                    await db.execute(
                        sa.text("UPDATE run SET authority_revision=authority_revision+1")
                        if scenario == "revoked"
                        else sa.text("UPDATE workspace SET generation=generation+1")
                    )
                with pytest.raises(AgentRuntimeError):
                    await runtime.start(request)
                assert backend.sends == 0
                assert (await _row(world, "SELECT count(*) AS n FROM execution"))["n"] == 0
                return
            if scenario == "ambiguous":
                backend.fail_start = True
                with pytest.raises(TimeoutError):
                    await runtime.start(request)
                with pytest.raises(AgentRuntimeError):
                    await runtime.start(request)
                assert backend.sends == 1
            elif scenario == "fenced_save":
                assert await authority.reserve(request) is None
                async with engine.begin() as db:
                    await db.execute(sa.text("UPDATE run SET owner_epoch=owner_epoch+1"))
                from agent_runtime.sandbox.runtime import ExecutionHandle

                with pytest.raises(AgentRuntimeError):
                    await authority.started(request, ExecutionHandle(request.binding, "lost"))
                await authority.unknown(request)
                assert (await _row(world, "SELECT status FROM execution"))["status"] == "unknown"
            else:
                handle = await runtime.start(request)
                if scenario == "reuse":
                    assert await runtime.start(request) == handle
                else:
                    with pytest.raises(AgentRuntimeError):
                        await runtime.start(replace(request, command="echo different"))
                assert backend.sends == 1
            assert (await _row(world, "SELECT count(*) AS n FROM execution"))["n"] == 1
            assert (
                await _row(
                    world, "SELECT count(*) AS n FROM work_item WHERE kind='reconcile_execution'"
                )
            )["n"] == 1
        finally:
            await engine.dispose()

    run(check)


@pytest.mark.parametrize(
    "scenario",
    ["cleanup", "stale", "missing_handle", "quota", "inspection_failure", "cancel", "deadline"],
)
def test_sql_sandbox_maintenance(world, scenario):
    async def check():
        engine = engine_for(world.schema)
        try:
            authority, request, payloads = await setup(world, engine)
            if scenario in {"quota", "inspection_failure"}:
                request = replace(request, limits=ExecutionLimits(10, 20, 30, 40))
            backend = Backend(request.binding.sandbox_id)
            if scenario == "missing_handle":
                await authority.reserve(request)
            else:
                await SandboxExecutionRuntime(backend, authority).start(request)
            scheduler = SqlScheduler(engine, world.schema)

            async def claim():
                async with engine.begin() as db:
                    await db.execute(
                        sa.text(
                            "UPDATE work_item SET available_at=:available "
                            "WHERE kind='reconcile_execution'"
                        ),
                        {"available": datetime.now(UTC) - timedelta(minutes=10)},
                    )
                work = await scheduler.claim_work(claim_request(world.scope, "maintenance"))
                assert isinstance(work, MaintenanceClaimedWork)
                return work

            async def connect(sandbox):
                assert sandbox == request.binding.sandbox_id
                return backend

            async def confirm_deleted(sandbox):
                return False

            tree_stopped = False

            async def inspect(remote, payload):
                assert payload["remoteExecutionId"] == "command-1"
                if scenario == "inspection_failure":
                    raise TimeoutError("injected measurement failure")
                return ExecutionUsage(
                    remote.sandbox_id,
                    "command-1",
                    11 if backend.interrupts == 0 else 0,
                    0,
                    0,
                    0,
                    tree_stopped,
                )

            worker = SqlSandboxMaintenanceWorker(
                engine,
                world.schema,
                connect=connect,
                confirm_deleted=confirm_deleted,
                payloads=payloads,
                inspect_resources=inspect,
            )

            async def unexpected_legacy(work):
                raise AssertionError("sandbox work was incorrectly sent to legacy inspector")

            worker = SqlExecutionMaintenanceRouter(
                engine, world.schema, sandbox=worker, external=unexpected_legacy
            )
            work = await claim()
            if scenario in {"quota", "inspection_failure", "cancel", "deadline"}:
                persisted_stops = []

                async def before_interrupt():
                    persisted = await _row(world, "SELECT failure FROM execution")
                    assert persisted["failure"]["category"] in {
                        "quota_exceeded",
                        "quota_inspection_unavailable",
                        "execution_cancelled",
                        "execution_deadline",
                    }
                    persisted_stops.append(persisted["failure"]["category"])

                backend.before_interrupt = before_interrupt
                if scenario == "cancel":
                    await SqlStateUnitOfWork(
                        engine, world.schema, actor(world=world)
                    ).record_control(
                        ControlRequest(
                            run_id=world.run_id,
                            kind="cancel",
                            idempotency_key="sandbox-cancel",
                            expected_version=str(world.state_version),
                        )
                    )
                elif scenario == "deadline":
                    async with engine.begin() as db:
                        await db.execute(
                            sa.text("UPDATE execution SET deadline_at=:expired"),
                            {"expired": datetime.now(UTC) - timedelta(seconds=1)},
                        )
                await worker(work)
                execution = await _row(world, "SELECT status,failure FROM execution")
                assert execution["status"] == "unknown" and backend.interrupts == 1
                assert len(persisted_stops) == backend.interrupts
                reason = {
                    "quota": "quota_exceeded",
                    "inspection_failure": "quota_inspection_unavailable",
                    "cancel": "execution_cancelled",
                    "deadline": "execution_deadline",
                }[scenario]
                assert execution["failure"]["category"] == reason
                # New Worker instance; memory state is not the source of the stop intent.
                worker = SqlSandboxMaintenanceWorker(
                    engine,
                    world.schema,
                    connect=connect,
                    confirm_deleted=confirm_deleted,
                    payloads=payloads,
                    inspect_resources=inspect,
                )
                # The command exits zero, but a child remains alive. Preserve unknown.
                backend.observation = RemoteStatus(False, 0)
                await worker(await claim())
                execution = await _row(world, "SELECT status,failure FROM execution")
                if scenario in {"quota", "inspection_failure"}:
                    assert execution["status"] == "unknown"
                    tree_stopped = True
                    if scenario == "quota":
                        await worker(await claim())
                        execution = await _row(world, "SELECT status,failure FROM execution")
                        assert execution["status"] == "failed"
                else:
                    assert execution["status"] == "failed"
                assert execution["failure"]["category"] == reason
                assert backend.kills == 0
                assert backend.interrupts >= 2
                assert len(persisted_stops) == backend.interrupts
                return
            if scenario == "stale":
                async with engine.begin() as db:
                    await db.execute(
                        sa.text(
                            "UPDATE work_item SET lease_epoch=lease_epoch+1 "
                            "WHERE kind='reconcile_execution'"
                        )
                    )
                with pytest.raises(AgentRuntimeError):
                    await worker(work)
                assert backend.closes == 0 and backend.kills == 0
                return
            if scenario == "missing_handle":
                await worker(work)
                assert (await _row(world, "SELECT status FROM execution"))["status"] == "unknown"
                assert backend.sends == 0 and backend.kills == 0
                return
            await worker(work)
            assert (await _row(world, "SELECT status FROM execution"))["status"] == "running"
            backend.observation = RemoteStatus(False, 7)
            await worker(await claim())
            execution = await _row(world, "SELECT status,cleanup_status,failure FROM execution")
            assert execution["status"] == "failed" and execution["cleanup_status"] == "pending"
            assert execution["failure"]["exitCode"] == 7
            await worker(await claim())
            assert backend.kills == 0  # Output not collected; do not delete the workspace.
            # Simulate a separately verified collector; this is not collector acceptance.
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        "UPDATE operation SET status='committed',result_ref='sealed:test-result'"
                    )
                )
            await worker(await claim())
            assert (await _row(world, "SELECT cleanup_status FROM execution"))[
                "cleanup_status"
            ] == "failed"
            await worker(await claim())
            assert (await _row(world, "SELECT cleanup_status FROM execution"))[
                "cleanup_status"
            ] == "completed"
            assert (
                await _row(world, "SELECT status FROM work_item WHERE kind='reconcile_execution'")
            )["status"] == "completed"
        finally:
            await engine.dispose()

    run(check)


def test_late_handle_can_be_saved_after_supervision_marks_unknown(world):
    async def check():
        from agent_runtime.sandbox.runtime import ExecutionHandle

        engine = engine_for(world.schema)
        try:
            authority, request, payloads = await setup(world, engine)
            assert await authority.reserve(request) is None
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        "UPDATE work_item SET available_at=:available "
                        "WHERE kind='reconcile_execution'"
                    ),
                    {"available": datetime.now(UTC) - timedelta(minutes=10)},
                )
            claimed = await SqlScheduler(engine, world.schema).claim_work(
                claim_request(world.scope, "maintenance")
            )
            backend = Backend(request.binding.sandbox_id)

            async def connect(sandbox):
                return backend

            async def absent(sandbox):
                return False

            worker = SqlSandboxMaintenanceWorker(
                engine, world.schema, connect=connect, confirm_deleted=absent, payloads=payloads
            )
            await worker(claimed)
            assert (await _row(world, "SELECT status FROM execution"))["status"] == "unknown"
            handle = ExecutionHandle(request.binding, "late-command")
            await authority.started(request, handle)
            assert await authority.reserve(request) == handle
            assert backend.sends == 0
        finally:
            await engine.dispose()

    run(check)


@pytest.mark.parametrize(
    "scenario", ["success", "nonzero", "paused", "journal_rollback", "before_wait"]
)
def test_pending_execution_seal_wait_and_automatic_wake(world, scenario, monkeypatch):
    from test_decision_validation import facts as decision_facts

    from agent_runtime.agent_loop import DecisionFrame
    from agent_runtime.contracts.tools import PendingExecution, ToolInvocation
    from agent_runtime.storage.decisions import SqlDecisionStore
    from agent_runtime.storage.journal import SqlJournal
    from agent_runtime.storage.sandbox_results import SqlSandboxResultCollector
    from agent_runtime.storage.tool_host import ToolEffect
    from agent_runtime.storage.wake import SqlWaitWaker

    async def check():
        engine = engine_for(world.schema)
        try:
            authority, request, payloads = await setup(world, engine)
            backend = Backend(request.binding.sandbox_id)
            await SandboxExecutionRuntime(backend, authority).start(request)
            dispatch = await _row(
                world, "SELECT payload_ref FROM work_item WHERE kind='reconcile_execution'"
            )
            payload = await payloads.get(world.scope, dispatch["payload_ref"])
            invocation = ToolInvocation.model_validate(payload["invocation"])
            state, context, _ = decision_facts()
            state = state.model_copy(
                update={
                    "scope_id": world.scope,
                    "session_id": world.session_id,
                    "run_id": world.run_id,
                    "state_version": str(world.state_version),
                    "checkpoint_ref": f"state:{world.state_version + 1}",
                }
            )
            context = context.model_copy(
                update={
                    "scope_id": world.scope,
                    "session_id": world.session_id,
                    "run_id": world.run_id,
                    "snapshot_id": invocation.context_snapshot_id,
                }
            )
            candidate = tool_action()
            candidate["payload"]["validatedCalls"] = [payload["call"]]
            frame = DecisionFrame(
                actor=invocation.actor,
                context=context,
                attempt_id=invocation.model_attempt_id,
                action=decision_adapter.validate_python(candidate),
            )

            class Context:
                async def load(self, *args):
                    return context, {}

            class Access:
                async def verify(self, *args):
                    pass

            journal = SqlJournal(engine, world.schema, invocation.actor)
            store = SqlDecisionStore(
                engine,
                world.schema,
                actor=invocation.actor,
                lease=lease(world),
                payloads=payloads,
                context=Context(),
                access=Access(),
                journal=journal,
            )
            # Replace the fixture's bare call with a genuine staged decision before
            # recording the async observation. Restore its already-reserved op binding.
            async with engine.begin() as db:
                await db.execute(sa.text("DELETE FROM tool_call"))
            state = await store.persist(state, frame)
            async with engine.begin() as db:
                await db.execute(
                    sa.text("UPDATE tool_call SET operation_id=:op"),
                    {"op": invocation.operation_id},
                )
            pending = PendingExecution(
                status="pending_execution",
                tool_call_id=invocation.call.tool_call_id,
                run_id=world.run_id,
                operation_id=invocation.operation_id,
                execution_ref=ResourceRef(
                    resource_type="execution",
                    resource_id=str(request.binding.execution_id),
                    resource_version="1",
                    scope=ResourceScope(scope_id=world.scope),
                ),
            )
            with pytest.raises(AgentRuntimeError):
                await store.record(
                    frame,
                    pending.model_copy(
                        update={
                            "execution_ref": pending.execution_ref.model_copy(
                                update={"resource_version": "2"}
                            )
                        }
                    ),
                )
            await store.record(frame, pending)
            await store.record(frame, pending)
            tool = await _row(world, "SELECT status,finished_at FROM tool_call")
            assert tool["status"] == "running" and tool["finished_at"] is None
            recovered = await store.recover()
            assert recovered.outcomes == (pending,)
            if scenario != "before_wait":
                state = await store.park_execution(state, recovered, invocation.call.tool)
                assert state.status.value == "waiting_tool"
                assert (
                    await _row(
                        world,
                        "SELECT owner_run_id FROM session_slot WHERE session_id=:id",
                        id=world.session_id,
                    )
                )["owner_run_id"] is None

            class Schemas:
                def output(self, spec, data):
                    assert data == {"value": 391}

            async def read(remote, payload):
                assert backend.kills == 0
                return ToolEffect({"value": 391}, "Execution output collected.")

            async def authorize(db, current):
                assert current == invocation
                current_run = (
                    (
                        await db.execute(
                            sa.text("SELECT * FROM run WHERE id=:id"), {"id": world.run_id}
                        )
                    )
                    .mappings()
                    .one()
                )
                await revalidate_authority(
                    db,
                    locks.build_lock_statements(world.schema),
                    invocation.actor,
                    run=current_run,
                    now=datetime.now(UTC),
                )

            results = SqlSandboxResultCollector(
                engine,
                world.schema,
                actor=invocation.actor,
                payloads=payloads,
                journal=journal,
                schemas=Schemas(),
                reader=read,
                authorize=authorize,
                waker=SqlWaitWaker(
                    engine, world.schema, actor=invocation.actor, journal=journal, payloads=payloads
                ),
            )

            async def connect(sandbox):
                return backend

            async def absent(sandbox):
                return False

            worker = SqlSandboxMaintenanceWorker(
                engine,
                world.schema,
                connect=connect,
                confirm_deleted=absent,
                payloads=payloads,
                results=results,
            )
            scheduler = SqlScheduler(engine, world.schema)

            async def tick():
                async with engine.begin() as db:
                    await db.execute(
                        sa.text(
                            "UPDATE work_item SET available_at=:now "
                            "WHERE kind='reconcile_execution'"
                        ),
                        {"now": datetime.now(UTC) - timedelta(minutes=10)},
                    )
                claimed = await scheduler.claim_work(claim_request(world.scope, "maintenance"))
                assert isinstance(claimed, MaintenanceClaimedWork)
                await worker(claimed)

            backend.observation = RemoteStatus(False, 7 if scenario == "nonzero" else 0)
            await tick()  # Stop evidence is not yet a ToolResult.
            assert backend.kills == 0
            if scenario == "journal_rollback":
                original = journal.append_state_in_transaction

                async def fail(*args):
                    raise RuntimeError("injected seal event failure")

                monkeypatch.setattr(journal, "append_state_in_transaction", fail)
                with pytest.raises(RuntimeError, match="seal event"):
                    await tick()
                assert (await _row(world, "SELECT status FROM tool_call"))["status"] == "running"
                assert (await _row(world, "SELECT status FROM operation"))["status"] == "unknown"
                async with engine.begin() as db:
                    await db.execute(
                        sa.text(
                            "UPDATE work_item SET status='pending',owner_id=NULL,lease_until=NULL "
                            "WHERE kind='reconcile_execution'"
                        )
                    )
                monkeypatch.setattr(journal, "append_state_in_transaction", original)
            await tick()  # Seal only; no deletion before the commit.
            assert backend.kills == 0
            assert (await _row(world, "SELECT status FROM tool_call"))["status"] == (
                "failed" if scenario == "nonzero" else "completed"
            )
            if scenario == "paused":
                async with engine.begin() as db:
                    await db.execute(
                        sa.text("UPDATE run SET status='paused' WHERE id=:id"), {"id": world.run_id}
                    )
            await tick()  # Wake and first (unconfirmed) deletion.
            if scenario == "before_wait":
                assert (await _row(world, "SELECT status FROM run WHERE id=:id", id=world.run_id))[
                    "status"
                ] == "running"
                await tick()  # Even cleanup can complete before the wait is committed.
                assert (
                    await _row(
                        world, "SELECT status FROM work_item WHERE kind='reconcile_execution'"
                    )
                )["status"] == "pending"
                state = await store.park_execution(state, recovered, invocation.call.tool)
                await tick()
            assert (await _row(world, "SELECT status FROM run WHERE id=:id", id=world.run_id))[
                "status"
            ] == ("paused" if scenario == "paused" else "queued")
            if scenario != "before_wait":
                await tick()  # Cleanup retry does not duplicate ToolFinished or wake work.
            assert (await _row(world, "SELECT cleanup_status FROM execution"))[
                "cleanup_status"
            ] == "completed"
            assert (
                await _row(world, "SELECT count(*) AS n FROM event WHERE kind='tool.finished'")
            )["n"] == 1
            assert (
                await _row(world, "SELECT status FROM work_item WHERE kind='reconcile_execution'")
            )["status"] == ("pending" if scenario == "paused" else "completed")
            if scenario != "paused":
                async with engine.begin() as db:
                    await db.execute(
                        sa.text(
                            "INSERT INTO budget_allocation "
                            "(id,scope_id,root_run_id,limits,reserved,"
                            "unknown_reserved,settled,usage_status,unsettled_count,version) "
                            "VALUES (:id,:scope,:run,'{}','{}','{}','{}','complete',0,1)"
                        ),
                        {"id": uuid4(), "scope": world.scope, "run": world.run_id},
                    )
                next_work = await scheduler.claim_work(claim_request(world.scope, "main"))
                assert next_work.lease.run_id == world.run_id
                resumed = SqlDecisionStore(
                    engine,
                    world.schema,
                    actor=invocation.actor,
                    lease=next_work.lease,
                    payloads=payloads,
                    context=Context(),
                    access=Access(),
                    journal=journal,
                )
                observed = await resumed.recover()
                assert observed.frame.attempt_id == frame.attempt_id
                assert observed.require_next_model_sources()[0].resource_id == str(
                    invocation.call.tool_call_id
                )
        finally:
            await engine.dispose()

    run(check)

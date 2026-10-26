"""Approval lifecycle against a disposable PostgreSQL schema."""

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa
from test_storage_uow import _row, _seed_attempt, actor, engine_for, lease, run
from test_storage_uow import no_leftover_schemas as no_leftover_schemas
from test_storage_uow import schema as schema
from test_storage_uow import world as world

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.policy import PolicyRequest
from agent_runtime.contracts.tools import ApprovalDecision, ToolSpec
from agent_runtime.storage.approvals import SqlApprovalService

pytestmark = pytest.mark.skipif(
    not os.getenv("AGENT_RUNTIME_TEST_DSN"), reason="requires real PostgreSQL"
)


class Access:
    requests = 0
    decisions = 0

    async def require_request(self, db, request, spec):
        self.requests += 1
        assert request.invocation.call.tool == spec.ref

    async def require_decision(self, db, approver, request_actor, run_id, session_id, resources):
        self.decisions += 1
        assert approver.actor_id == request_actor and resources == ()


def test_sql_approval_lifecycle(world):
    async def scenario():
        import json
        from importlib.resources import files

        from agent_runtime.contracts.tools import ToolInvocation

        data = json.loads(
            files("agent_runtime.contracts").joinpath("fixtures/tools.json").read_text()
        )
        spec = ToolSpec.model_validate(data["spec"])
        now = datetime.now(UTC)
        operation_id = uuid4()
        invocation = ToolInvocation.model_validate(
            {
                **data["invocation"],
                "actor": actor(world=world).model_dump(mode="json", by_alias=True),
                "sessionId": str(world.session_id),
                "runId": str(world.run_id),
                "modelAttemptId": None,
                "operationId": str(operation_id),
                "deadlineAt": (now + timedelta(seconds=60)).isoformat(),
            }
        )
        request = PolicyRequest(invocation=invocation, policy=spec.approval_policy, resources=())
        access = Access()
        engine = engine_for(world.schema)
        service = SqlApprovalService(engine, world.schema, access=access)
        first = await service.request(
            request,
            spec,
            public_summary="Approve artifact revision",
            expires_at=now + timedelta(hours=1),
        )
        duplicate = await service.request(
            request,
            spec,
            public_summary="Approve artifact revision",
            expires_at=now + timedelta(hours=2),
        )
        assert first.approval_id == duplicate.approval_id
        assert duplicate.request.expires_at == first.request.expires_at
        changed_call = invocation.call.model_copy(update={"arguments": {"changed": True}})
        changed = request.model_copy(
            update={"invocation": invocation.model_copy(update={"call": changed_call})}
        )
        with pytest.raises(AgentRuntimeError) as conflict:
            await service.request(
                changed, spec, public_summary="Approve change", expires_at=now + timedelta(hours=1)
            )
        assert conflict.value.failure.code == "VERSION_CONFLICT"
        async with engine.begin() as db:
            await db.execute(
                sa.text(f"UPDATE \"{world.schema}\".run SET status='waiting_user' WHERE id=:id"),
                {"id": world.run_id},
            )
        decision = ApprovalDecision(
            approval_id=first.approval_id,
            expected_version="0",
            decision="approved",
            idempotency_key=f"approval:{first.approval_id}:0:approved",
        )
        receipt = await service.resolve(actor(world=world), decision)
        assert receipt.grant_ref == f"approval:{first.approval_id}:1"
        assert access.requests == 3 and access.decisions == 1
        with pytest.raises(AgentRuntimeError) as repeated:
            await service.resolve(actor(world=world), decision)
        assert repeated.value.failure.code == "VERSION_CONFLICT"
        from agent_runtime.storage.approval_cleanup import SqlApprovalCleanup

        assert await SqlApprovalCleanup(engine, world.schema).reconcile(world.scope) == 0
        async with engine.connect() as db:
            events = (
                await db.execute(
                    sa.text(
                        f'SELECT e.payload FROM "{world.schema}".event e '
                        f'JOIN "{world.schema}".outbox o ON o.event_id=e.id '
                        "WHERE e.kind='approval.updated'"
                    )
                )
            ).scalars().all()
        assert len(events) == 1 and events[0]["status"] == "approved"
        await engine.dispose()

    run(scenario)


@pytest.mark.parametrize(
    "decision_name", ["approved", "rejected", "expired", "revoked", "revoked_before_wake"]
)
def test_approval_wake_reuses_staged_operation_and_commits_one_effect(world, decision_name):
    async def scenario():
        from test_decision_validation import facts, tool_action

        from agent_runtime.agent_loop import DecisionFrame
        from agent_runtime.context_payloads import BlobContextPayloadStore
        from agent_runtime.contracts.decisions import decision_adapter
        from agent_runtime.contracts.state import RunStatus
        from agent_runtime.contracts.tools import ToolInvocation, ToolWaitingApproval
        from agent_runtime.contracts.work_claims import WorkClaimRequest
        from agent_runtime.storage.approval_resume import SqlApprovalResumer
        from agent_runtime.storage.approval_wake import SqlApprovalWakeScanner
        from agent_runtime.storage.context_blobs import SqlContextBlobBackend
        from agent_runtime.storage.decisions import SqlDecisionStore
        from agent_runtime.storage.journal import SqlJournal
        from agent_runtime.storage.recovery import SqlRecovery
        from agent_runtime.storage.scheduler import SqlScheduler
        from agent_runtime.storage.tool_capacity import SqlToolCapacity
        from agent_runtime.storage.tool_execution_gate import SqlToolExecutionGate
        from agent_runtime.storage.tool_host import SqlToolHost, ToolEffect
        from agent_runtime.storage.uow.capacity import SqlSystemCapacityStore

        engine = engine_for(world.schema)
        attempt_id = await _seed_attempt(world)
        snapshot = await _row(
            world, "SELECT context_snapshot_id FROM model_attempt WHERE id=:id", id=attempt_id
        )
        initial = await _row(
            world, "SELECT state_ref FROM checkpoint WHERE id=:id", id=world.checkpoint_id
        )
        state, context, budget = facts()
        state = state.model_copy(
            update={
                "scope_id": world.scope,
                "session_id": world.session_id,
                "run_id": world.run_id,
                "checkpoint_ref": initial["state_ref"],
            }
        )
        context = context.model_copy(
            update={
                "scope_id": world.scope,
                "session_id": world.session_id,
                "run_id": world.run_id,
                "snapshot_id": snapshot["context_snapshot_id"],
            }
        )
        budget = budget.model_copy(update={"deadline_at": datetime.now(UTC) + timedelta(hours=1)})
        action = decision_adapter.validate_python(tool_action())
        frame = DecisionFrame(
            actor=actor(world=world), context=context, attempt_id=attempt_id, action=action
        )
        call = action.payload.validated_calls[0]
        spec = ToolSpec(
            ref=call.tool,
            description="Approved database write",
            source=call.tool,
            kind="write",
            side_effect="database",
            input_schema=call.tool,
            output_schema=call.tool,
            permissions=(),
            approval_policy=call.tool,
            idempotency="transactional",
            timeout_seconds=60,
            concurrency_group="approved-effect",
            max_concurrency=1,
            cancellation="stop",
            public_result_policy="summary",
        )

        class Context:
            async def load(self, scope_id, requested_id):
                assert scope_id == world.scope and requested_id == context.snapshot_id
                return context, {}

        class ContextAccess:
            async def verify(self, current_actor, prepared):
                assert current_actor == frame.actor and prepared == context

        class Registry:
            async def get(self, requested):
                assert requested.tool == call.tool
                return spec

            async def resources(self, invocation, pinned):
                return ()

        journal = SqlJournal(engine, world.schema, frame.actor)
        payloads = BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema))
        store = SqlDecisionStore(
            engine,
            world.schema,
            actor=frame.actor,
            lease=lease(world),
            payloads=payloads,
            context=Context(),
            access=ContextAccess(),
            journal=journal,
        )
        staged = await store.persist(state, frame)
        operation_id = uuid4()
        invocation = ToolInvocation(
            actor=frame.actor,
            session_id=world.session_id,
            run_id=world.run_id,
            model_attempt_id=attempt_id,
            context_snapshot_id=context.snapshot_id,
            context_version=context.context_version,
            call=call,
            operation_id=operation_id,
            deadline_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        approvals = SqlApprovalService(engine, world.schema, access=Access())
        request = PolicyRequest(invocation=invocation, policy=spec.approval_policy, resources=())
        issued = await approvals.request(
            request,
            spec,
            public_summary="Approve the write",
            expires_at=datetime.now(UTC) + timedelta(minutes=30),
        )
        waiting = ToolWaitingApproval(
            status="waiting_approval",
            tool_call_id=call.tool_call_id,
            run_id=world.run_id,
            operation_id=operation_id,
            approval=issued.request,
        )
        await store.record(frame, waiting)
        observed = await store.recover()
        assert observed is not None and observed.outcomes == (waiting,)
        parked = await store.park_execution(staged, observed, spec.ref)
        assert parked.status == RunStatus.WAITING_USER
        required = await _row(
            world, "SELECT count(*) AS total FROM event WHERE kind='approval.required'"
        )
        assert required["total"] == 1
        if decision_name != "expired":
            decision = ApprovalDecision(
                approval_id=issued.approval_id,
                expected_version="0",
                decision="approved" if decision_name.startswith("revoked") else decision_name,
                idempotency_key=(
                    f"approval:{issued.approval_id}:0:approved"
                    if decision_name.startswith("revoked")
                    else f"approval:{issued.approval_id}:0:{decision_name}"
                ),
            )
            await approvals.resolve(frame.actor, decision)
        if decision_name == "expired":
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".approval SET expires_at=:expired '
                        "WHERE id=:id AND status='pending'"
                    ),
                    {"expired": datetime.now(UTC) - timedelta(seconds=1), "id": issued.approval_id},
                )
        if decision_name == "revoked_before_wake":
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".capability_grant SET revoked_at=:now '
                        "WHERE scope_id=:scope AND run_id=:run"
                    ),
                    {"now": datetime.now(UTC), "scope": world.scope, "run": world.run_id},
                )
            scanner = SqlApprovalWakeScanner(engine, world.schema)
            assert await scanner.wake_ready(world.scope) == 0
            assert await scanner.wake_ready(world.scope) == 0
            failed = await _row(
                world, "SELECT status,failure FROM run WHERE id=:id", id=world.run_id
            )
            assert failed["status"] == "failed" and failed["failure"]["code"] == "FORBIDDEN"
            assert (await _row(world, "SELECT count(*) AS total FROM artifact"))["total"] == 0
            assert (await _row(world, "SELECT count(*) AS total FROM event "
                             "WHERE kind='run.failed'"))["total"] == 1
            await engine.dispose()
            return
        scanner = SqlApprovalWakeScanner(engine, world.schema)
        assert await scanner.wake_ready(world.scope) == 1
        assert await scanner.wake_ready(world.scope) == 0
        wake = await _row(world, "SELECT state_version FROM run WHERE id=:id", id=world.run_id)
        async with engine.begin() as db:
            await db.execute(
                sa.text(
                    f'INSERT INTO "{world.schema}".budget_allocation '
                    "(id,scope_id,root_run_id,limits,reserved,unknown_reserved,settled,"
                    "usage_status,unsettled_count,version) VALUES "
                    "(:id,:scope,:run,CAST(:limits AS jsonb),'{}','{}','{}','complete',0,1)"
                ),
                {
                    "id": uuid4(),
                    "scope": world.scope,
                    "run": world.run_id,
                    "limits": '{"tool_calls":4}',
                },
            )
        claim = await SqlScheduler(engine, world.schema).claim_work(
            WorkClaimRequest.model_validate(
                {
                    "scopeId": world.scope,
                    "worker": {
                        "workerId": "approval-resumer",
                        "runtimeRevision": {
                            "key": {"namespace": "builtin", "key": "runtime"},
                            "revision": "1",
                        },
                        "capabilities": [],
                    },
                    "leaseSeconds": 300,
                    "lanes": ["main"],
                    "maintenanceKinds": [],
                }
            )
        )
        assert claim.disposition == "claimed"
        if decision_name == "revoked":
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".capability_grant SET revoked_at=:now '
                        "WHERE scope_id=:scope AND run_id=:run"
                    ),
                    {"now": datetime.now(UTC), "scope": world.scope, "run": world.run_id},
                )
        resumed_store = SqlDecisionStore(
            engine,
            world.schema,
            actor=frame.actor,
            lease=claim.lease,
            payloads=payloads,
            context=Context(),
            access=ContextAccess(),
            journal=journal,
        )
        if decision_name == "revoked":
            with pytest.raises(AgentRuntimeError) as revoked:
                await resumed_store.recover()
            assert revoked.value.failure.code == "FORBIDDEN"
            await SqlRecovery(engine, world.schema).handle_worker_failure(claim, revoked.value)
            assert (await _row(world, "SELECT status FROM run WHERE id=:id", id=world.run_id))[
                "status"
            ] == "failed"
            assert (await _row(world, "SELECT count(*) AS total FROM artifact"))["total"] == 0
            await engine.dispose()
            return
        recovered = await resumed_store.recover()
        assert recovered is not None and recovered.outcomes == (waiting,)
        capacity_store = SqlSystemCapacityStore(engine, world.schema)
        capacity = SqlToolCapacity(capacity_store, deployment_id="approval-test")
        await capacity_store.seed(capacity.key(world.scope, spec), capacity=1)

        class Schemas:
            def input(self, pinned, arguments):
                assert arguments == call.arguments

            def output(self, pinned, data):
                assert data == {"ok": True}

        class Effect:
            calls = 0

            async def invoke(self, db, current):
                self.calls += 1
                await db.execute(
                    sa.text(
                        f'INSERT INTO "{world.schema}".artifact '
                        "(id,scope_id,owner_actor_id,task_id) "
                        "VALUES (:id,:scope,:actor,:task)"
                    ),
                    {
                        "id": uuid4(),
                        "scope": world.scope,
                        "actor": frame.actor.actor_id,
                        "task": world.task_id,
                    },
                )
                return ToolEffect(data={"ok": True}, summary="Saved")

        effect = Effect()

        async def authorize(db, current, refs):
            assert current.actor == frame.actor and refs == ()

        host = SqlToolHost(
            engine,
            world.schema,
            specs=Registry(),
            resources=Registry(),
            schemas=Schemas(),
            authorize=authorize,
            capacity=capacity,
            execution_gate=SqlToolExecutionGate(
                world.schema, approval_required=lambda current, pinned, refs: True
            ),
            read_tools={},
            database_tools={spec.ref: effect},
            external_tools={},
        )
        next_state = parked.model_copy(
            update={
                "status": RunStatus.RUNNING,
                "state_version": str(wake["state_version"]),
                "checkpoint_ref": recovered.checkpoint_ref,
            }
        )
        class FlakyDecisionStore:
            def __init__(self):
                self.fail_once = decision_name == "approved"

            async def record(self, current_frame, outcome):
                if self.fail_once:
                    self.fail_once = False
                    raise RuntimeError("worker died before recording the observation")
                await resumed_store.record(current_frame, outcome)

            async def recover(self):
                return await resumed_store.recover()

        flaky = FlakyDecisionStore()
        resumer = SqlApprovalResumer(
            approvals=approvals,
            specs=Registry(),
            resources=Registry(),
            tools=host,
            decisions=flaky,
        )
        if decision_name == "approved":
            with pytest.raises(RuntimeError, match="before recording"):
                await resumer.resume(next_state, budget, recovered)
            assert effect.calls == 1
            assert (await resumed_store.recover()).outcomes == (waiting,)
        complete = await resumer.resume(next_state, budget, recovered)
        expected = "completed" if decision_name == "approved" else "not_executed"
        assert complete.outcomes[0].status == expected
        assert effect.calls == (1 if decision_name == "approved" else 0)
        artifacts = await _row(
            world,
            "SELECT count(*) AS total FROM artifact WHERE owner_actor_id=:actor",
            actor=frame.actor.actor_id,
        )
        assert artifacts["total"] == (1 if decision_name == "approved" else 0)
        call_row = await _row(
            world, "SELECT status FROM tool_call WHERE id=:id", id=call.tool_call_id
        )
        assert call_row["status"] == expected
        await engine.dispose()

    run(scenario)

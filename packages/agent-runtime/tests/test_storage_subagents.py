"""Real SQL delegation/collection transactions; random schema, no business data."""

import asyncio
import json
import os
import signal
import sys
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest

pytest.importorskip("sqlalchemy")

import sqlalchemy as sa  # noqa: E402
from test_context_runtime import request as context_request  # noqa: E402
from test_decision_validation import (
    facts,  # noqa: E402
    tool_action,  # noqa: E402
)
from test_mcp_broker_integration import mcp_server as mcp_server  # noqa: E402
from test_storage_uow import (  # noqa: E402
    _row,
    _seed_attempt,
    _seed_execution,
    _seed_item,
    actor,
    count,
    engine_for,
    lease,
    run,
)
from test_storage_uow import (
    no_leftover_schemas as no_leftover_schemas,
)
from test_storage_uow import (
    schema as schema,
)
from test_storage_uow import (
    world as world,
)

from agent_runtime.agent_loop import DecisionFrame  # noqa: E402
from agent_runtime.context_payloads import BlobContextPayloadStore  # noqa: E402
from agent_runtime.contracts.canonical import content_digest  # noqa: E402
from agent_runtime.contracts.decisions import decision_adapter  # noqa: E402
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope  # noqa: E402
from agent_runtime.contracts.message_payloads import ResultProposed  # noqa: E402
from agent_runtime.contracts.ownership import ChildCommitGuard, ChildProposalRequest  # noqa: E402
from agent_runtime.contracts.references import (  # noqa: E402
    CapabilityRef,
    ResourceRef,
    ResourceScope,
)
from agent_runtime.contracts.registry import CapabilityManifest, ToolCapability  # noqa: E402
from agent_runtime.contracts.requests import ControlRequest  # noqa: E402
from agent_runtime.contracts.scheduler import ClaimedWork  # noqa: E402
from agent_runtime.contracts.subagents import (  # noqa: E402
    ChildRunRef,
    CollectRequest,
    VerifiedResult,
)
from agent_runtime.contracts.tasks import ValidationEvidence  # noqa: E402
from agent_runtime.contracts.transactions import CommitGuard  # noqa: E402
from agent_runtime.contracts.work_claims import (  # noqa: E402
    ChildClaimedWork,
    MaintenanceClaimedWork,
    WorkClaimRequest,
)
from agent_runtime.storage.context_blobs import SqlContextBlobBackend  # noqa: E402
from agent_runtime.storage.context_sources import SqlRuntimeSourceReader  # noqa: E402
from agent_runtime.storage.decisions import SqlDecisionStore  # noqa: E402
from agent_runtime.storage.delegate_commit import SqlDelegateActionCommitter  # noqa: E402
from agent_runtime.storage.delegation_types import DelegationPolicy  # noqa: E402
from agent_runtime.storage.execution_supervisor import SqlExecutionSupervisor  # noqa: E402
from agent_runtime.storage.journal import SqlJournal  # noqa: E402
from agent_runtime.storage.live_run import SqlLiveRunGuard  # noqa: E402
from agent_runtime.storage.scheduler import SqlScheduler  # noqa: E402
from agent_runtime.storage.subagents import SqlSubagentRuntime  # noqa: E402
from agent_runtime.storage.uow import SqlStateUnitOfWork  # noqa: E402
from agent_runtime.worker import ChildLeaseHandle  # noqa: E402

pytestmark = pytest.mark.skipif(
    not os.getenv("AGENT_RUNTIME_TEST_DSN"), reason="requires real PostgreSQL"
)


def claim_request(scope, lane):
    return WorkClaimRequest.model_validate(
        {
            "scopeId": scope,
            "worker": {
                "workerId": f"delegate-{lane}",
                "runtimeRevision": {
                    "key": {"namespace": "builtin", "key": "runtime"},
                    "revision": "1",
                },
                "capabilities": [],
            },
            "leaseSeconds": 300,
            "lanes": [lane],
            "maintenanceKinds": ["reconcile_execution"] if lane == "maintenance" else [],
        }
    )


async def install_tool_revision(engine, world, spec):
    manifest = CapabilityManifest(
        ref=spec.ref,
        status="active",
        content_digest=content_digest(spec.model_dump(mode="json", by_alias=True)),
        spec=ToolCapability(kind="tool", spec=spec),
    )
    capability_id = uuid4()
    async with engine.begin() as db:
        await db.execute(
            sa.text(
                f'INSERT INTO "{world.schema}".capability '
                "(id,scope_id,namespace,key,kind) VALUES (:id,:scope,:namespace,:key,'tool')"
            ),
            {
                "id": capability_id,
                "scope": world.scope,
                "namespace": spec.ref.key.namespace,
                "key": spec.ref.key.key,
            },
        )
        await db.execute(
            sa.text(
                f'INSERT INTO "{world.schema}".capability_revision '
                "(id,scope_id,capability_id,revision,content_digest,status,manifest) "
                "VALUES (:id,:scope,:capability,:revision,:digest,'active',"
                "CAST(:manifest AS jsonb))"
            ),
            {
                "id": uuid4(),
                "scope": world.scope,
                "capability": capability_id,
                "revision": int(spec.ref.revision),
                "digest": manifest.content_digest,
                "manifest": manifest.model_dump_json(by_alias=True),
            },
        )


@pytest.mark.parametrize(
    "scenario",
    [
        "accepted",
        "rejected",
        "rollback_spawn",
        "rollback_proposal",
        "rollback_collect",
        "insufficient",
        "forbidden",
        "deadline",
        "unknown_usage",
        "bad_verifier",
        "pause",
        "execution",
        "race",
        "child_failed",
        "child_cancelled",
        "failure_rollback",
        "failure_execution",
        "child_adapters",
        "applied_later_input",
        "pending_later_input",
        "notification_prefix",
        "failure_expired_parent",
        "child_worker",
        "child_worker_revoked_claim",
        "child_worker_tool",
        "child_worker_resume",
        "child_worker_uncommitted",
        "child_worker_write_denied",
        "child_worker_network",
        "child_worker_effect",
        "child_worker_external_unknown",
        "child_tool_host_database",
        "child_tool_host_read",
        "child_tool_host_rollback",
        "child_tool_host_approval_budget",
        "child_tool_host_resource_changed",
        "child_tool_host_external",
        "child_tool_host_mcp",
        "child_tool_host_mcp_supervisor",
        "child_tool_host_mcp_broker",
        "child_tool_host_mcp_broker_read",
        "child_tool_host_mcp_broker_resource_changed",
        "child_tool_host_mcp_broker_access_revoked",
        "child_tool_host_mcp_broker_approval_budget",
        "child_tool_host_supervisor",
        "child_tool_host_cancel_supervisor",
        "child_tool_host_process_supervisor",
        "child_tool_host_process_cancel_supervisor",
        "child_tool_host_process_takeover_supervisor",
        "child_tool_host_process_mcp_supervisor",
        "child_tool_host_process_mcp_cancel_supervisor",
        "child_tool_host_process_mcp_takeover_supervisor",
        "child_execution_wait",
    ],
)
def test_durable_delegate_propose_collect(world, monkeypatch, scenario, request):
    if scenario == "child_worker_network" and os.getenv("AGENT_RUNTIME_LIVE_MODEL") != "1":
        pytest.skip("opt in to real model and SQL Worker integration")
    if (
        scenario in {"child_tool_host_mcp", "child_tool_host_mcp_supervisor"}
        and os.getenv("AGENT_RUNTIME_LIVE_MCP") != "1"
    ):
        pytest.skip("opt in to real MCP and SQL external effect integration")
    attempt = run(lambda: _seed_attempt(world))
    snapshot = run(
        lambda: _row(
            world, "SELECT context_snapshot_id FROM model_attempt WHERE id=:id", id=attempt
        )
    )["context_snapshot_id"]
    state, context, _ = facts()
    state = state.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "state_version": "1",
            "checkpoint_ref": "state:2",
        }
    )
    context = context.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": world.run_id,
            "snapshot_id": snapshot,
        }
    )
    target = state.agent
    acceptance = CapabilityRef.model_validate(
        {"key": {"namespace": "builtin", "key": "acceptance"}, "revision": "1"}
    )
    predicate = CapabilityRef.model_validate(
        {"key": {"namespace": "builtin", "key": "message"}, "revision": "1"}
    )
    parent_budget, child_budget = uuid4(), uuid4()
    policy = DelegationPolicy(
        agent=target,
        agent_revision_id=world.agent_revision_id,
        parent_allocation_id=parent_budget,
        model_calls=5,
        tool_calls=8,
        tokens=1000,
        tools=(CapabilityRef.model_validate(tool_action()["payload"]["validatedCalls"][0]["tool"]),)
        if scenario
        in {
            "child_adapters",
            "child_worker_tool",
            "child_worker_write_denied",
            "child_tool_host_database",
            "child_tool_host_read",
            "child_tool_host_rollback",
            "child_tool_host_approval_budget",
            "child_tool_host_resource_changed",
            "child_tool_host_external",
            "child_tool_host_mcp",
            "child_tool_host_mcp_supervisor",
            "child_tool_host_mcp_broker",
            "child_tool_host_mcp_broker_read",
            "child_tool_host_mcp_broker_resource_changed",
            "child_tool_host_mcp_broker_access_revoked",
            "child_tool_host_mcp_broker_approval_budget",
            "child_tool_host_supervisor",
            "child_tool_host_cancel_supervisor",
            "child_tool_host_process_supervisor",
            "child_tool_host_process_cancel_supervisor",
            "child_tool_host_process_takeover_supervisor",
            "child_tool_host_process_mcp_supervisor",
            "child_tool_host_process_mcp_cancel_supervisor",
            "child_tool_host_process_mcp_takeover_supervisor",
            "child_worker_effect",
            "child_worker_external_unknown",
            "child_execution_wait",
        }
        else (),
        acceptance_spec=acceptance,
        output_schema_ref="builtin:report:1",
    )
    task = {
        "taskId": str(uuid4()),
        "targetAgent": target.model_dump(mode="json", by_alias=True),
        "objective": "Analyze the assigned evidence",
        "constraints": [],
        "acceptanceSpecRef": "builtin:acceptance:1",
        "outputSchemaRef": "builtin:report:1",
        "resourceRefs": [],
        "budgetAllocationId": str(child_budget),
        "deadlineAt": datetime.now(UTC) + timedelta(hours=2 if scenario != "deadline" else 48),
    }
    frame = DecisionFrame(
        actor=actor(world=world),
        context=context,
        attempt_id=attempt,
        action=decision_adapter.validate_python(
            {"kind": "delegate", "payload": {"taskPackage": task}}
        ),
    )
    engine = engine_for(
        world.schema,
        pool_size=2 if scenario == "race" else 1,
        search_path=scenario == "child_execution_wait",
    )

    async def seed():
        async with engine.begin() as conn:
            await conn.execute(
                sa.text(
                    f'INSERT INTO "{world.schema}".budget_allocation '
                    "(id,scope_id,root_run_id,limits,reserved,unknown_reserved,settled,"
                    "usage_status,unsettled_count,version) "
                    "VALUES (:id,:scope,:run,CAST(:limits AS jsonb),'{}','{}','{}','complete',0,0)"
                ),
                {
                    "id": parent_budget,
                    "scope": world.scope,
                    "run": world.run_id,
                    "limits": json.dumps(
                        {
                            "model_calls": 10,
                            "tool_calls": 16,
                            "tokens": 500 if scenario == "insufficient" else 2000,
                        }
                    ),
                },
            )
            await conn.execute(
                sa.text(
                    f'UPDATE "{world.schema}".capability_grant '
                    "SET allowed_scope=CAST(:allowed AS jsonb) "
                    "WHERE run_id=:run"
                ),
                {
                    "run": world.run_id,
                    "allowed": json.dumps(
                        {
                            "agents": [] if scenario == "forbidden" else [task["targetAgent"]],
                            "tools": [
                                tool.model_dump(mode="json", by_alias=True) for tool in policy.tools
                            ],
                            "resources": [],
                        }
                    ),
                },
            )

    run(seed)

    class Context:
        async def load(self, scope_id, requested_id):
            return context, {}

    class Access:
        async def verify(self, current, prepared):
            assert current == frame.actor and prepared == context

    class Verifier:
        calls = 0

        async def verify_in_transaction(self, db, request, proposal):
            self.calls += 1
            return VerifiedResult(
                child=request.child,
                proposal_id=uuid4() if scenario == "bad_verifier" else request.proposal_id,
                proposal_revision="1",
                acceptance_spec=acceptance,
                disposition="rejected" if scenario == "rejected" else "accepted",
                evidence_refs=proposal.evidence_refs,
                result_refs=proposal.artifact_refs,
                public_reason="Verified independently."
                if scenario != "rejected"
                else "Evidence does not meet acceptance.",
            )

    reader = SqlRuntimeSourceReader(
        engine, world.schema, blobs=SqlContextBlobBackend(engine, world.schema)
    )
    journal = SqlJournal(engine, world.schema, frame.actor)
    verifier = Verifier()
    runtime = SqlSubagentRuntime(
        engine,
        world.schema,
        actor=frame.actor,
        journal=journal,
        resources=reader,
        policies={target: policy},
        message_predicate=predicate,
        verifier=verifier,
    )
    store = SqlDecisionStore(
        engine,
        world.schema,
        actor=frame.actor,
        lease=lease(world),
        payloads=BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema)),
        context=Context(),
        access=Access(),
        journal=journal,
    )
    staged = run(lambda: store.persist(state, frame))
    committer = SqlDelegateActionCommitter(
        actor=frame.actor,
        lease=lease(world),
        decisions=store,
        subagents=runtime,
        live=SqlLiveRunGuard(
            engine,
            world.schema,
            actor=frame.actor,
            lease=lease(world),
            context=Context(),
            access=Access(),
        ),
    )
    original = journal.append_state_in_transaction

    async def fail_after_events(*args):
        await original(*args)
        raise RuntimeError("forced delegation rollback")

    baseline = (
        count(world, "run"),
        count(world, "task"),
        count(world, "event"),
        count(world, "outbox"),
    )
    if scenario == "rollback_spawn":
        monkeypatch.setattr(journal, "append_state_in_transaction", fail_after_events)
        with pytest.raises(RuntimeError, match="forced delegation rollback"):
            run(lambda: committer.commit(staged, frame))
        assert (
            count(world, "run"),
            count(world, "task"),
            count(world, "event"),
            count(world, "outbox"),
        ) == baseline
        assert count(world, "budget_entry") == 0
        assert run(store.recover).frame == frame
        monkeypatch.setattr(journal, "append_state_in_transaction", original)
    if scenario in {"insufficient", "forbidden"}:
        with pytest.raises(AgentRuntimeError):
            run(lambda: committer.commit(staged, frame))
        assert (
            count(world, "run"),
            count(world, "task"),
            count(world, "event"),
            count(world, "outbox"),
        ) == baseline
        run(engine.dispose)
        return
    if scenario == "race":

        async def compete():
            return await asyncio.gather(
                committer.commit(staged, frame),
                committer.commit(staged, frame),
                return_exceptions=True,
            )

        results = run(compete)
        assert sum(isinstance(value, AgentRuntimeError) for value in results) == 1
    else:
        committed = run(lambda: committer.commit(staged, frame))
        assert committed.status == "waiting_user" and committed.state_version == "2"
    assert count(world, "run") == baseline[0] + 1
    assert count(world, "budget_allocation") == 2
    assert count(world, "budget_entry") == 1
    assert count(world, "agent_message", "kind='task.assign' AND status='applied'") == 1
    assert count(world, "outbox") == baseline[3] + 2
    with pytest.raises(AgentRuntimeError):
        run(lambda: committer.commit(staged, frame))
    claimed = run(
        lambda: SqlScheduler(engine, world.schema).claim_work(claim_request(world.scope, "child"))
    )
    assert isinstance(claimed, ChildClaimedWork)
    assert claimed.budget_allocation_id == child_budget and child_budget != parent_budget
    assert run(lambda: _row(world, "SELECT owner_run_id FROM session_slot"))["owner_run_id"] is None
    child_row = run(lambda: _row(world, "SELECT * FROM run WHERE id=:id", id=claimed.lease.run_id))
    if scenario == "deadline":
        # A long requested deadline is narrowed, not rejected; an empty pinned
        # manifest still applies the standard Agent Budget default of 1800 seconds.
        assert child_row["deadline_at"] < task["deadlineAt"]
        assert child_row["deadline_at"] <= child_row["phase_started_at"] + timedelta(seconds=1800)
    goal = run(
        lambda: _row(
            world, "SELECT id FROM goal_revision WHERE task_id=:task", task=child_row["task_id"]
        )
    )["id"]
    ref = ResourceRef(
        resource_type="goal_revision",
        resource_id=str(goal),
        resource_version="1",
        scope=ResourceScope(scope_id=world.scope),
    )
    if scenario == "child_worker_revoked_claim":
        from agent_runtime.storage.host_worker import SqlClaimChildHandler

        class DeniedHandlers:
            async def open(self, actor, work, lease):
                pytest.fail("revoked claim opened the child handler")

        async def revoked_claim():
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".capability_grant '
                        "SET revoked_at=now() WHERE scope_id=:scope AND run_id=:run"
                    ),
                    {"scope": world.scope, "run": claimed.lease.run_id},
                )
            with pytest.raises(AgentRuntimeError) as denied:
                await SqlClaimChildHandler(engine, world.schema, DeniedHandlers())(
                    claimed, ChildLeaseHandle(claimed.lease)
                )
            assert denied.value.failure.code == "FORBIDDEN"

        run(revoked_claim)
        assert count(world, "model_attempt", f"run_id='{claimed.lease.run_id}'") == 0
        run(engine.dispose)
        return
    child_request = context_request().model_copy(
        update={"actor": frame.actor, "session_id": world.session_id, "run_id": child_row["id"]}
    )
    assert run(lambda: reader.read(child_request, ref)).content
    with pytest.raises(AgentRuntimeError):
        run(
            lambda: reader.read(child_request, ref.model_copy(update={"resource_id": str(uuid4())}))
        )
    proposal = ResultProposed(
        result_id=uuid4(),
        summary="Child produced a candidate.",
        artifact_refs=(ref,),
        evidence_refs=(ref,),
        validation=ValidationEvidence(status="passed", evidence_refs=(ref,)),
        remaining_issues=(),
    )
    child_guard = ChildCommitGuard(
        actor=frame.actor,
        lease=claimed.lease,
        expected_state_version=claimed.state_version,
        root_run_id=world.run_id,
        expected_root_cancel_version=claimed.root_cancel_version,
        workspace_ref=claimed.workspace_ref,
        workspace_owner_run_id=claimed.lease.run_id,
        workspace_generation=claimed.workspace_generation,
    )
    proposed = ChildProposalRequest(
        guard=child_guard, task_id=child_row["task_id"], proposal=proposal
    )
    if scenario == "child_execution_wait":
        run(lambda: _check_child_execution_wait(world, engine, frame.actor, child_row, child_guard))
        run(engine.dispose)
        return
    if scenario.startswith("child_tool_host_"):
        server = (
            request.getfixturevalue("mcp_server")
            if scenario.startswith("child_tool_host_mcp_broker")
            or scenario.startswith("child_tool_host_process_mcp_")
            else None
        )
        run(
            lambda: _check_child_tool_host(
                world,
                engine,
                frame.actor,
                child_row,
                child_guard,
                ref,
                scenario=scenario,
                mcp_server=server,
            )
        )
        run(engine.dispose)
        return
    if scenario == "child_adapters":
        run(
            lambda: _check_child_adapters(
                world,
                engine,
                journal,
                reader,
                state,
                context,
                child_row,
                child_guard,
                ref,
                child_budget,
            )
        )
        run(engine.dispose)
        return
    if scenario in {
        "child_worker",
        "child_worker_tool",
        "child_worker_resume",
        "child_worker_network",
        "child_worker_effect",
        "child_worker_external_unknown",
    }:
        run(
            lambda: _run_child_worker(
                world,
                engine,
                runtime,
                frame.actor,
                claimed,
                child_row,
                child_guard,
                ref,
                with_tool=scenario
                in {"child_worker_tool", "child_worker_effect", "child_worker_external_unknown"},
                retry_proposal=scenario == "child_worker_resume",
                live_model=scenario == "child_worker_network",
                effect_mode=scenario in {"child_worker_effect", "child_worker_external_unknown"},
                external_mode=scenario == "child_worker_external_unknown",
                resolve_claim_actor=scenario == "child_worker",
            )
        )
        if scenario == "child_worker_external_unknown":
            assert count(world, "agent_message", "kind='result.proposed'") == 0
            run(engine.dispose)
            return
        assert count(world, "agent_message", "kind='result.proposed'") == 1
        assert count(world, "output_seal", "run_id IS NOT NULL") >= 1
        run(engine.dispose)
        return
    if scenario == "child_worker_uncommitted":
        run(lambda: _seed_attempt(world, run_id=child_row["id"]))
        with pytest.raises(AgentRuntimeError) as uncommitted:
            run(
                lambda: _run_child_worker(
                    world,
                    engine,
                    runtime,
                    frame.actor,
                    claimed,
                    child_row,
                    child_guard,
                    ref,
                    with_tool=False,
                    retry_proposal=False,
                )
            )
        assert uncommitted.value.failure.code == "VERSION_CONFLICT"
        assert count(world, "agent_message", "kind='result.proposed'") == 0
        run(engine.dispose)
        return
    if scenario == "child_worker_write_denied":
        with pytest.raises(AgentRuntimeError) as denied:
            run(
                lambda: _run_child_worker(
                    world,
                    engine,
                    runtime,
                    frame.actor,
                    claimed,
                    child_row,
                    child_guard,
                    ref,
                    with_tool=True,
                    retry_proposal=False,
                    write_spec=True,
                )
            )
        assert denied.value.failure.code == "FORBIDDEN"
        assert count(world, "tool_call", "run_id IS NOT NULL") == 0
        assert count(world, "agent_message", "kind='result.proposed'") == 0
        run(engine.dispose)
        return
    if scenario.startswith("child_") or scenario.startswith("failure_"):
        failure = FailureEnvelope(
            code="PROVIDER_ERROR",
            category="provider",
            retryable=False,
            public_message="Child model could not complete the task.",
        )
        if scenario == "failure_execution":
            run(lambda: _seed_execution(replace(world, run_id=child_row["id"]), status="unknown"))
            with pytest.raises(AgentRuntimeError):
                run(lambda: runtime.report_failure(child_guard, failure))
            assert count(world, "agent_message", "kind='task.failed'") == 0
            run(engine.dispose)
            return
        if scenario == "failure_expired_parent":

            async def expire_parent():
                async with engine.begin() as db:
                    await db.execute(
                        sa.text(
                            f'UPDATE "{world.schema}".run SET deadline_at=:deadline WHERE id=:id'
                        ),
                        {"deadline": datetime.now(UTC) - timedelta(seconds=1), "id": world.run_id},
                    )

            run(expire_parent)
        if scenario == "child_cancelled":
            with pytest.raises(AgentRuntimeError):
                run(lambda: runtime.report_failure(child_guard, failure, cancelled=True))

            run(
                lambda: SqlStateUnitOfWork(engine, world.schema, frame.actor).record_control(
                    ControlRequest(
                        run_id=child_row["id"],
                        kind="cancel",
                        idempotency_key="child-cancel",
                        expected_version="0",
                    )
                )
            )
        if scenario == "failure_rollback":
            monkeypatch.setattr(journal, "append_state_in_transaction", fail_after_events)
            with pytest.raises(RuntimeError):
                run(lambda: runtime.report_failure(child_guard, failure))
            assert count(world, "agent_message", "kind='task.failed'") == 0
            assert (
                run(lambda: _row(world, "SELECT status FROM run WHERE id=:id", id=child_row["id"]))[
                    "status"
                ]
                == "running"
            )
            monkeypatch.setattr(journal, "append_state_in_transaction", original)
        if scenario == "child_failed":
            run(
                lambda: runtime.handle_worker_failure(
                    claimed, ChildLeaseHandle(claimed.lease), AgentRuntimeError(failure)
                )
            )
        else:
            run(
                lambda: runtime.report_failure(
                    child_guard, failure, cancelled=scenario == "child_cancelled"
                )
            )
        assert count(world, "agent_message", "kind='task.failed'") == 1
        if scenario == "failure_expired_parent":
            notice = run(
                lambda: _row(
                    world,
                    "SELECT status,created_at,deadline_at FROM agent_message "
                    "WHERE kind='task.failed'",
                )
            )
            assert notice["status"] == "expired" and notice["deadline_at"] > notice["created_at"]
            assert (
                run(lambda: _row(world, "SELECT status FROM run WHERE id=:id", id=world.run_id))[
                    "status"
                ]
                == "waiting_user"
            )
            assert (
                count(world, "work_item", "run_id=:id AND status='pending'", id=world.run_id) == 0
            )
            with pytest.raises(AgentRuntimeError):
                run(lambda: runtime.report_failure(child_guard, failure))
            run(engine.dispose)
            return
        assert (
            count(
                world,
                "work_item",
                "run_id=:id AND status IN ('pending','leased')",
                id=child_row["id"],
            )
            == 0
        )
        if scenario == "child_cancelled":
            assert (
                count(world, "command", "run_id=:id AND status='applied'", id=child_row["id"]) == 1
            )
        assert (
            run(lambda: _row(world, "SELECT status FROM run WHERE id=:id", id=world.run_id))[
                "status"
            ]
            == "queued"
        )
        with pytest.raises(AgentRuntimeError):
            run(lambda: runtime.report_failure(child_guard, failure))
        run(engine.dispose)
        return
    if scenario == "pause":
        run(
            lambda: SqlStateUnitOfWork(engine, world.schema, frame.actor).record_control(
                ControlRequest(
                    run_id=world.run_id,
                    kind="pause",
                    idempotency_key="pause-parent",
                    expected_version="2",
                )
            )
        )
    if scenario == "execution":
        child_world = replace(world, run_id=child_row["id"])
        run(lambda: _seed_execution(child_world, status="unknown"))
        with pytest.raises(AgentRuntimeError):
            run(lambda: runtime.propose(proposed))
        assert count(world, "agent_message", "kind='result.proposed'") == 0
        run(engine.dispose)
        return
    before = (count(world, "event"), count(world, "outbox"))
    if scenario == "rollback_proposal":
        monkeypatch.setattr(journal, "append_state_in_transaction", fail_after_events)
        with pytest.raises(RuntimeError):
            run(lambda: runtime.propose(proposed))
        assert (count(world, "event"), count(world, "outbox")) == before
        assert count(world, "agent_message", "kind='result.proposed'") == 0
        monkeypatch.setattr(journal, "append_state_in_transaction", original)
    if scenario == "notification_prefix":

        async def progress_before_proposal():
            from agent_runtime.contracts.messages import message_adapter
            from agent_runtime.storage.delegation_sql import message

            async with engine.begin() as db:
                await message(
                    db,
                    world.schema,
                    scope=world.scope,
                    envelope=message_adapter.validate_python(
                        dict(
                            protocol_revision=1,
                            message_id=uuid4(),
                            correlation_id=child_row["task_id"],
                            causation_id=None,
                            sender_run_id=child_row["id"],
                            recipient_run_id=world.run_id,
                            task_id=child_row["task_id"],
                            recipient_seq="1",
                            created_at=datetime.now(UTC),
                            deadline_at=datetime.now(UTC) + timedelta(hours=1),
                            resource_refs=(),
                            trace_id="prefix-progress",
                            kind="progress.updated",
                            payload={
                                "stepRef": None,
                                "publicSummary": "Reviewing",
                                "evidenceRefs": [],
                            },
                        )
                    ),
                )
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".run SET pending_input_version=1 WHERE id=:id'
                    ),
                    {"id": world.run_id},
                )

        run(progress_before_proposal)
    assert run(lambda: runtime.propose(proposed)) == proposal.result_id
    with pytest.raises(AgentRuntimeError):
        run(lambda: runtime.propose(proposed))
    parent = run(lambda: _row(world, "SELECT * FROM run WHERE id=:id", id=world.run_id))
    if scenario == "pause":
        assert parent["status"] == "waiting_user" and parent["desired_control"] == "pause"
        assert count(world, "work_item", "run_id=:id AND status='pending'", id=world.run_id) == 0
        run(engine.dispose)
        return
    assert parent["status"] == "queued" and parent["state_version"] == 3
    resumed = run(
        lambda: SqlScheduler(engine, world.schema).claim_work(claim_request(world.scope, "main"))
    )
    assert isinstance(resumed, ClaimedWork)
    assert resumed.lease.run_id == world.run_id
    child = ChildRunRef(
        scope_id=world.scope,
        session_id=world.session_id,
        parent_run_id=world.run_id,
        child_run_id=child_row["id"],
        task_id=child_row["task_id"],
        agent=target,
        budget_allocation_id=child_budget,
        workspace_id=UUID(claimed.workspace_ref.resource_id),
    )
    collect = CollectRequest(
        parent_guard=CommitGuard(
            actor=frame.actor, lease=resumed.lease, expected_state_version="3"
        ),
        child=child,
        proposal_id=proposal.result_id,
        expected_proposal_revision="1",
        acceptance_spec=acceptance,
    )
    if scenario == "notification_prefix":
        from agent_runtime.storage.main_run_loader import SqlMainRunLoader

        run(
            lambda: SqlMainRunLoader(
                engine, world.schema, actor=frame.actor, agents={}
            ).apply_notifications(collect.parent_guard)
        )
        assert (
            run(
                lambda: _row(
                    world, "SELECT applied_input_version FROM run WHERE id=:id", id=world.run_id
                )
            )["applied_input_version"]
            == 1
        )
    with pytest.raises(AgentRuntimeError):
        run(
            lambda: runtime.collect(
                collect.model_copy(
                    update={"child": child.model_copy(update={"parent_run_id": uuid4()})}
                )
            )
        )
    if scenario == "unknown_usage":

        async def unknown():
            async with engine.begin() as conn:
                await conn.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".budget_allocation '
                        "SET usage_status='unknown',unsettled_count=1 WHERE id=:id"
                    ),
                    {"id": child_budget},
                )

        run(unknown)
    else:

        async def known_usage():
            async with engine.begin() as conn:
                await conn.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".budget_allocation '
                        "SET settled=CAST(:usage AS jsonb) WHERE id=:id"
                    ),
                    {
                        "id": child_budget,
                        "usage": json.dumps({"model_calls": 1, "tool_calls": 1, "tokens": 42}),
                    },
                )

        run(known_usage)
    before = (count(world, "event"), count(world, "outbox"))
    if scenario == "rollback_collect":
        monkeypatch.setattr(journal, "append_state_in_transaction", fail_after_events)
        with pytest.raises(RuntimeError):
            run(lambda: runtime.collect(collect))
        assert (count(world, "event"), count(world, "outbox")) == before
        assert count(world, "agent_message", "kind='result.accepted'") == 0
        assert count(world, "budget_entry", "kind='settle'") == 0
        monkeypatch.setattr(journal, "append_state_in_transaction", original)
    if scenario == "bad_verifier":
        with pytest.raises(AgentRuntimeError):
            run(lambda: runtime.collect(collect))
        assert count(world, "agent_message", "kind='result.accepted'") == 0
        run(engine.dispose)
        return
    if scenario in {"applied_later_input", "pending_later_input"}:

        async def later_input():
            from agent_runtime.contracts.messages import message_adapter
            from agent_runtime.storage.delegation_sql import message

            async with engine.begin() as db:
                await message(
                    db,
                    world.schema,
                    scope=world.scope,
                    envelope=message_adapter.validate_python(
                        dict(
                            protocol_revision=1,
                            message_id=uuid4(),
                            correlation_id=child.task_id,
                            causation_id=None,
                            sender_run_id=child.child_run_id,
                            recipient_run_id=world.run_id,
                            task_id=child.task_id,
                            recipient_seq="2",
                            created_at=datetime.now(UTC),
                            deadline_at=datetime.now(UTC) + timedelta(hours=1),
                            resource_refs=(),
                            trace_id="later-input",
                            kind="progress.updated",
                            payload={
                                "stepRef": None,
                                "publicSummary": "Later child progress",
                                "evidenceRefs": [],
                            },
                        )
                    ),
                )
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".run SET pending_input_version=2 WHERE id=:id'
                    ),
                    {"id": world.run_id},
                )

        run(later_input)

        async def apply_all():
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".agent_message '
                        "SET status='applied',applied_checkpoint_id=:cp WHERE recipient_run_id=:id"
                    ),
                    {"cp": resumed.checkpoint_id, "id": world.run_id},
                )
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".mailbox_cursor '
                        "SET applied_seq=2 WHERE recipient_run_id=:id"
                    ),
                    {"id": world.run_id},
                )
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".run SET applied_input_version=2 WHERE id=:id'
                    ),
                    {"id": world.run_id},
                )

        if scenario == "applied_later_input":
            run(apply_all)
    result = run(lambda: runtime.collect(collect))
    assert result.disposition == ("rejected" if scenario == "rejected" else "accepted")
    assert (
        verifier.calls >= 1
    )  # Child's own validation=passed did not bypass the independent verifier.
    assert (
        count(
            world,
            "agent_message",
            "kind='result.rejected'" if scenario == "rejected" else "kind='result.accepted'",
        )
        == 1
    )
    final = run(
        lambda: _row(
            world,
            "SELECT status,state_version,applied_input_version FROM run WHERE id=:id",
            id=world.run_id,
        )
    )
    assert final == {
        "status": "queued",
        "state_version": 4,
        "applied_input_version": 2
        if scenario in {"applied_later_input", "notification_prefix"}
        else 1,
    }
    balance = run(
        lambda: _row(world, "SELECT reserved FROM budget_allocation WHERE id=:id", id=parent_budget)
    )["reserved"]
    assert (
        balance == policy.limits
        if scenario == "unknown_usage"
        else all(value == 0 for value in balance.values())
    )
    if scenario != "unknown_usage":
        assert run(
            lambda: _row(
                world, "SELECT settled FROM budget_allocation WHERE id=:id", id=parent_budget
            )
        )["settled"] == {"model_calls": 1, "tool_calls": 1, "tokens": 42}
    assert count(world, "work_item", "run_id=:id AND status='pending'", id=world.run_id) == 1
    if scenario == "unknown_usage":
        assert run(lambda: runtime.reconcile_budget(child)) is False

        async def audit_usage():
            async with engine.begin() as conn:
                await conn.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".budget_allocation '
                        "SET usage_status='complete',unsettled_count=0,"
                        "settled=CAST(:usage AS jsonb) WHERE id=:id"
                    ),
                    {
                        "id": child_budget,
                        "usage": json.dumps({"model_calls": 1, "tool_calls": 1, "tokens": 42}),
                    },
                )

        run(audit_usage)
        execute = runtime._execute

        async def fail_after_settle(db, sql, **params):
            await execute(db, sql, **params)
            if "INSERT INTO {t}budget_entry" in sql:
                raise RuntimeError("fault after reconciliation ledger write")

        monkeypatch.setattr(runtime, "_execute", fail_after_settle)
        with pytest.raises(RuntimeError):
            run(lambda: runtime.reconcile_budget(child))
        assert count(world, "budget_entry", "kind='settle'") == 0
        monkeypatch.setattr(runtime, "_execute", execute)

    async def reconcile_twice():
        return await asyncio.gather(
            runtime.reconcile_budget(child), runtime.reconcile_budget(child)
        )

    assert run(reconcile_twice) == [True, True]
    assert run(lambda: runtime.reconcile_budget(child)) is True
    assert count(world, "budget_entry", "kind='settle'") == 1
    with pytest.raises(AgentRuntimeError):
        run(lambda: runtime.collect(collect))
    run(engine.dispose)


async def _check_child_execution_wait(world, engine, current_actor, child, guard):
    from agent_runtime.contracts.state import RunStatus
    from agent_runtime.contracts.tools import FullResult, PendingExecution, ToolCompleted
    from agent_runtime.storage.wake import SqlWaitWaker, WakeRequest

    attempt = await _seed_attempt(world, run_id=child["id"])
    snapshot = (
        await _row(world, "SELECT context_snapshot_id FROM model_attempt WHERE id=:id", id=attempt)
    )["context_snapshot_id"]
    state, context, _ = facts()
    state = state.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": child["id"],
            "state_version": str(child["state_version"]),
            "checkpoint_ref": (
                await _row(
                    world,
                    "SELECT state_ref FROM checkpoint WHERE id=:id",
                    id=child["checkpoint_id"],
                )
            )["state_ref"],
        }
    )
    context = context.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": child["id"],
            "snapshot_id": snapshot,
        }
    )
    frame = DecisionFrame(
        actor=current_actor,
        context=context,
        attempt_id=attempt,
        action=decision_adapter.validate_python(tool_action()),
    )

    class Context:
        async def load(self, *args):
            return context, {}

    class Access:
        async def verify(self, *args):
            pass

    blobs = BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema))
    journal = SqlJournal(engine, world.schema, current_actor)
    store = SqlDecisionStore(
        engine,
        world.schema,
        actor=current_actor,
        lease=guard.lease,
        payloads=blobs,
        context=Context(),
        access=Access(),
        journal=journal,
        child_guard=lambda: guard,
    )
    state = await store.persist(state, frame)
    op, execution, work = uuid4(), uuid4(), uuid4()
    call = frame.action.payload.validated_calls[0]
    execution_ref = ResourceRef(
        resource_type="execution",
        resource_id=str(execution),
        resource_version="1",
        scope=ResourceScope(scope_id=world.scope),
    )
    async with engine.begin() as db:
        await db.execute(
            sa.text(
                "INSERT INTO operation (id,scope_id,task_id,idempotency_key,input_hash,status) "
                "VALUES (:id,:scope,:task,:key,:hash,'dispatching')"
            ),
            {
                "id": op,
                "scope": world.scope,
                "task": child["task_id"],
                "key": str(op),
                "hash": "a" * 64,
            },
        )
        await db.execute(
            sa.text(
                "INSERT INTO work_item (id,scope_id,run_id,subject_ref,kind,dedup_key,status,"
                "payload_ref,wake_reason,priority,lease_epoch,attempt_count,"
                "available_at,deadline_at) "
                "VALUES (:id,:scope,:run,CAST(:ref AS jsonb),'reconcile_execution',"
                ":key,'pending','child-test',NULL,"
                "1,0,0,:now,:deadline)"
            ),
            {
                "id": work,
                "scope": world.scope,
                "run": child["id"],
                "ref": execution_ref.model_dump_json(by_alias=True),
                "key": str(work),
                "now": datetime.now(UTC),
                "deadline": child["deadline_at"],
            },
        )
        await db.execute(
            sa.text(
                "INSERT INTO execution (id,scope_id,operation_id,run_id,supervisor_work_id,"
                "remote_request_key,status,log_cursor,cleanup_status,generation,deadline_at) "
                "VALUES "
                "(:id,:scope,:op,:run,:work,:key,'running','0','pending',1,:deadline)"
            ),
            {
                "id": execution,
                "scope": world.scope,
                "op": op,
                "run": child["id"],
                "work": work,
                "key": str(op),
                "deadline": child["deadline_at"],
            },
        )
        await db.execute(
            sa.text("UPDATE tool_call SET operation_id=:op WHERE id=:id"),
            {"op": op, "id": call.tool_call_id},
        )
    pending = PendingExecution(
        status="pending_execution",
        tool_call_id=call.tool_call_id,
        run_id=child["id"],
        operation_id=op,
        execution_ref=execution_ref,
    )
    await store.record(frame, pending)
    # Async output collection can temporarily leave Operation unknown. This is
    # not the older failed/unknown-effect path that terminates a reconciled child.
    async with engine.begin() as db:
        await db.execute(sa.text("UPDATE operation SET status='unknown' WHERE id=:id"), {"id": op})
    from agent_runtime.storage.child_worker import SqlChildWorkerHandler

    handler = SqlChildWorkerHandler(
        engine,
        world.schema,
        actor=current_actor,
        subagents=None,
        estimator=None,
        provider=None,
        capabilities=None,
        agents={},
        specs=None,
        resources=None,
        policy=None,
        read_handlers={},
    )
    assert await handler._unknown_external_effect(child["id"]) is None
    recovered = await store.recover()
    parked = await store.park_execution(state, recovered, call.tool)
    assert parked.status == RunStatus.WAITING_TOOL
    assert (await _row(world, "SELECT status FROM run WHERE id=:id", id=world.run_id))[
        "status"
    ] == "waiting_user"
    completed = ToolCompleted(
        status="completed",
        tool_call_id=call.tool_call_id,
        run_id=child["id"],
        operation_id=op,
        effect="committed",
        effect_refs=(),
        result=FullResult(policy="full", data={"ok": True}),
    )
    payload = {
        "call": call.model_dump(mode="json", by_alias=True),
        "outcome": completed.model_dump(mode="json", by_alias=True),
    }
    from agent_runtime.contracts.canonical import content_digest

    result_ref = await blobs.put(world.scope, payload, content_digest(payload))
    async with engine.begin() as db:
        await db.execute(
            sa.text("UPDATE execution SET status='completed' WHERE id=:id"), {"id": execution}
        )
        await db.execute(
            sa.text("UPDATE tool_call SET status='completed',result_ref=:ref WHERE id=:id"),
            {"id": call.tool_call_id, "ref": result_ref},
        )
        await db.execute(
            sa.text("UPDATE operation SET status='committed',result_ref=:ref WHERE id=:id"),
            {"id": op, "ref": result_ref},
        )
        await db.execute(
            sa.text("UPDATE run SET status='paused' WHERE id=:id"), {"id": world.run_id}
        )
    waker = SqlWaitWaker(engine, world.schema, actor=current_actor, journal=journal, payloads=blobs)
    request = WakeRequest(
        current_actor,
        world.session_id,
        child["id"],
        int(parked.state_version),
        "tool",
        str(call.tool_call_id),
    )
    with pytest.raises(AgentRuntimeError) as blocked:
        await waker.wake(request)
    assert blocked.value.failure.code == "CONTROL_PENDING"
    async with engine.begin() as db:
        await db.execute(
            sa.text("UPDATE run SET status='waiting_user' WHERE id=:id"), {"id": world.run_id}
        )
    await waker.wake(request)
    next_work = await SqlScheduler(engine, world.schema).claim_work(
        claim_request(world.scope, "child")
    )
    assert isinstance(next_work, ChildClaimedWork) and next_work.lease.run_id == child["id"]
    next_guard = guard.model_copy(
        update={"lease": next_work.lease, "expected_state_version": next_work.state_version}
    )
    resumed = SqlDecisionStore(
        engine,
        world.schema,
        actor=current_actor,
        lease=next_work.lease,
        payloads=blobs,
        context=Context(),
        access=Access(),
        journal=journal,
        child_guard=lambda: next_guard,
    )
    observed = await resumed.recover()
    assert observed.outcomes == (completed,)
    assert observed.require_next_model_sources()[0].resource_id == str(call.tool_call_id)


async def _run_child_worker(
    world,
    engine,
    runtime,
    current_actor,
    claimed,
    child_row,
    guard,
    ref,
    *,
    with_tool,
    retry_proposal,
    write_spec=False,
    live_model=False,
    effect_mode=False,
    external_mode=False,
    resolve_claim_actor=False,
):
    from test_context_runtime import Estimator
    from test_provider_invoker import capabilities, make_request

    from agent_runtime.agent_loop import DecisionSetup
    from agent_runtime.contracts.policy import PolicyAllow
    from agent_runtime.contracts.provider import (
        ContentDelta,
        ProviderFinish,
        ProviderUsage,
        ToolCallComplete,
    )
    from agent_runtime.contracts.results import Usage
    from agent_runtime.contracts.tools import ToolSpec
    from agent_runtime.policy_validation import policy_input_digest
    from agent_runtime.storage.child_worker import ChildAgentBinding, SqlChildWorkerHandler
    from agent_runtime.storage.tool_assembly import SqlToolRuntimeAssembly
    from agent_runtime.storage.tool_capacity import SqlToolCapacity
    from agent_runtime.storage.tool_execution_gate import SqlToolExecutionGate
    from agent_runtime.storage.tool_host import SqlToolHost, ToolEffect
    from agent_runtime.storage.uow.capacity import SqlSystemCapacityStore
    from agent_runtime.tool_names import provider_tool_name

    model = make_request().model
    tool_ref = CapabilityRef.model_validate(tool_action()["payload"]["validatedCalls"][0]["tool"])
    spec = ToolSpec(
        ref=tool_ref,
        description="Read assigned evidence",
        source=tool_ref,
        kind="write" if write_spec or effect_mode else "read",
        side_effect=(
            "external" if external_mode else "database" if write_spec or effect_mode else "none"
        ),
        input_schema=tool_ref,
        output_schema=tool_ref,
        permissions=(),
        approval_policy=tool_ref,
        idempotency=(
            "provider_key"
            if external_mode
            else "transactional"
            if write_spec or effect_mode
            else "read_only"
        ),
        timeout_seconds=30,
        concurrency_group="child-read",
        max_concurrency=1,
        cancellation="stop",
        public_result_policy="summary" if effect_mode else "full",
    )
    req = context_request(refs=0).model_copy(
        update={
            "actor": current_actor,
            "session_id": world.session_id,
            "run_id": child_row["id"],
            "model": model.ref,
            "budget": context_request(refs=0).budget.model_copy(
                update={"allocation_id": claimed.budget_allocation_id}
            ),
            "resource_refs": (ref,),
            "tools": (tool_ref,) if with_tool else (),
            "user_input": "Analyze the assigned evidence",
            "prompt_text": "Answer the delegated task using the assigned evidence.",
        }
    )

    class Setup:
        async def resolve(self, state, inputs, budget):
            assert inputs.messages[0].kind == "task.assign"
            return DecisionSetup(req, model)

    class Backend:
        calls = 0

        async def stream(self, request, *, cancel=None):
            self.calls += 1
            if with_tool and self.calls == 1:
                yield ToolCallComplete(
                    attempt_id=request.attempt_id,
                    sequence="0",
                    kind="tool.complete",
                    call_id="child-read-1",
                    name=provider_tool_name(tool_ref),
                    arguments={"query": "assigned evidence"},
                )
                reason = "tool_calls"
            else:
                yield ContentDelta(
                    attempt_id=request.attempt_id,
                    sequence="0",
                    kind="content.delta",
                    text="The evidence supports the delegated finding.",
                )
                reason = "stop"
            yield ProviderUsage(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="usage",
                usage=Usage(
                    status="complete",
                    reason="provider_reported",
                    input_tokens="10",
                    output_tokens="10",
                    total_tokens="20",
                ),
            )
            yield ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="2",
                kind="finish",
                reason=reason,
                raw_reason=reason,
            )

    class Tools:
        async def get(self, call):
            assert with_tool and call.tool == tool_ref
            return spec

        async def resources(self, invocation, spec):
            assert with_tool
            return (ref,)

        async def evaluate(self, request):
            assert with_tool
            return PolicyAllow(
                decision="allow",
                decision_id=uuid4(),
                policy=request.policy,
                input_digest=policy_input_digest(request),
                evidence_refs=(ref,),
                evaluated_at=datetime.now(UTC),
                expires_at=request.invocation.deadline_at,
                grant_ref="child-read-grant",
            )

    class ReadHandler:
        async def invoke(self, db, invocation):
            return {"finding": "assigned evidence"}

    class InterceptProposal:
        failed = False

        async def propose(self, request):
            if not self.failed:
                self.failed = True
                raise RuntimeError("worker stopped before proposal commit")
            return await runtime.propose(request)

    tools = Tools()
    backend = Backend()
    handle = ChildLeaseHandle(claimed.lease)
    effect_tools = None
    tool_runtime = None
    if effect_mode:
        capacity_store = SqlSystemCapacityStore(engine, world.schema)
        capacity = SqlToolCapacity(capacity_store, deployment_id="test-tool-host")
        await capacity_store.seed(
            capacity.key(current_actor.scope_id, spec), capacity=spec.max_concurrency
        )
        if external_mode:
            await install_tool_revision(engine, world, spec)
        from agent_runtime.storage.child_tools import SqlChildToolAuthorizer

        authorizer = SqlChildToolAuthorizer(
            world.schema,
            current_actor,
            SqlRuntimeSourceReader(
                engine, world.schema, blobs=SqlContextBlobBackend(engine, world.schema)
            ),
        )

        async def authorize(db, current, resources):
            await authorizer.require_in_transaction(
                db,
                guard.model_copy(update={"lease": handle.current()}),
                current,
                resources,
            )

        class Schemas:
            def input(self, pinned, arguments):
                assert pinned == spec and isinstance(arguments.get("query"), str)

            def output(self, pinned, data):
                assert pinned == spec and isinstance(data, dict) and data.get("ok") is True

        class WriteHandler:
            async def invoke(self, db, current):
                await db.execute(
                    sa.text(
                        f'INSERT INTO "{world.schema}".artifact '
                        "(id,scope_id,owner_actor_id,task_id) "
                        "VALUES (:id,:scope,:actor,:task)"
                    ),
                    {
                        "id": uuid4(),
                        "scope": world.scope,
                        "actor": current_actor.actor_id,
                        "task": child_row["task_id"],
                    },
                )
                return ToolEffect(data={"ok": True}, summary="Artifact created")

        class ExternalHandler:
            sends = 0

            async def dispatch(self, current, *, key):
                self.sends += 1
                raise OSError("remote response lost")

            async def lookup(self, current, *, key):
                return None

        remote = ExternalHandler()

        effect_tools = SqlToolHost(
            engine,
            world.schema,
            specs=tools,
            resources=tools,
            schemas=Schemas(),
            authorize=authorize,
            capacity=capacity,
            execution_gate=SqlToolExecutionGate(
                world.schema, approval_required=lambda invocation, pinned, resources: False
            ),
            database_tools={} if external_mode else {tool_ref: WriteHandler()},
            external_tools={tool_ref: remote} if external_mode else {},
        )

        class ExternalInspector:
            async def inspect(self, key):
                assert key and remote.sends == 1
                return "completed"

            async def cancel(self, key):
                pytest.fail("completed remote effect should not be cancelled")

        tool_runtime = SqlToolRuntimeAssembly(
            effect_tools,
            capacity,
            supervisor=SqlExecutionSupervisor(
                engine,
                world.schema,
                inspector=ExternalInspector(),
                capacity=capacity,
                subagents=runtime,
            )
            if external_mode
            else None,
        )
    client = None
    if live_model:
        import httpx
        from dotenv import dotenv_values

        from agent_runtime.providers.context_input import ContextOpenAIInputResolver
        from agent_runtime.providers.openai_compatible import (
            OpenAIAdapterProfile,
            OpenAICompatibleProvider,
            ProviderConnection,
        )
        from agent_runtime.storage.context_factory import build_sql_context_components

        config = {key: value for key, value in dotenv_values(".env").items() if value is not None}
        config.update(os.environ)
        assert config.get("BASE_URL") and config.get("MODEL_NAME")
        model = model.model_copy(update={"model_name": config["MODEL_NAME"]})
        components = build_sql_context_components(engine, world.schema, estimator=Estimator())

        class NoTools:
            async def definition(self, ref):
                raise AssertionError("live child test has no tools")

        async def connection(request):
            return ProviderConnection(
                base_url=config["BASE_URL"], api_key=config.get("API_KEY") or None
            )

        client = httpx.AsyncClient(trust_env=False)
        backend = OpenAICompatibleProvider(
            client,
            inputs=ContextOpenAIInputResolver(
                context=components.runtime,
                actor=current_actor,
                access=components.access,
                tools=NoTools(),
            ),
            connections=connection,
            profile=OpenAIAdapterProfile.qwen(),
        )
        req = req.model_copy(
            update={
                "prompt_text": "Answer the assigned task in one short sentence. Do not call tools.",
                "user_input": "State one finding about the assigned evidence.",
            }
        )
    worker = SqlChildWorkerHandler(
        engine,
        world.schema,
        actor=current_actor,
        subagents=InterceptProposal() if retry_proposal else runtime,
        estimator=Estimator(),
        provider=backend,
        capabilities=capabilities,
        agents={world.agent_revision_id: ChildAgentBinding(facts()[0].agent, Setup())},
        specs=tools,
        resources=tools,
        policy=tools,
        read_handlers={tool_ref: ReadHandler()} if with_tool and not effect_mode else {},
        effect_tools=effect_tools,
        tool_runtime=tool_runtime,
    )
    if retry_proposal:
        with pytest.raises(RuntimeError, match="before proposal commit"):
            await worker(claimed, handle)
    if live_model:
        from agent_runtime.worker import WorkerConfig, WorkerRunner

        errors_seen = []

        async def on_error(work, lease, error):
            errors_seen.append(error)
            await runtime.handle_worker_failure(work, lease, error)

        runner = WorkerRunner(
            SqlScheduler(engine, world.schema),
            claim_request(world.scope, "child"),
            lambda work: None,
            child_handler=worker,
            child_error_sink=on_error,
            config=WorkerConfig(concurrency=1),
        )
        try:
            await runner._execute(claimed)
        finally:
            await client.aclose()
        assert not errors_seen, [str(error) for error in errors_seen]
    else:
        if external_mode:
            await worker(claimed, handle)
            assert backend.calls == 1 and remote.sends == 1
            assert (await _row(world, "SELECT status FROM run WHERE id=:id", id=child_row["id"]))[
                "status"
            ] == "waiting_tool"
            assert (
                await _row(
                    world,
                    "SELECT status FROM operation WHERE task_id=:id",
                    id=child_row["task_id"],
                )
            )["status"] == "unknown"
            assert (
                await _row(
                    world, "SELECT status FROM tool_call WHERE run_id=:id", id=child_row["id"]
                )
            )["status"] == "failed"
            execution = await _row(
                world,
                "SELECT id,operation_id,supervisor_work_id FROM execution WHERE run_id=:id",
                id=child_row["id"],
            )
            maintenance = await SqlScheduler(engine, world.schema).claim_work(
                claim_request(world.scope, "maintenance")
            )
            assert isinstance(maintenance, MaintenanceClaimedWork)
            assert maintenance.lease.work_id == execution["supervisor_work_id"]

            assert tool_runtime is not None and tool_runtime.supervisor is not None
            await tool_runtime.supervisor(maintenance)
            assert (await _row(world, "SELECT status FROM run WHERE id=:id", id=child_row["id"]))[
                "status"
            ] == "failed"
            assert (await _row(world, "SELECT status FROM run WHERE id=:id", id=world.run_id))[
                "status"
            ] == "queued"
            assert (
                await _row(
                    world,
                    "SELECT status FROM work_item WHERE id=:id",
                    id=execution["supervisor_work_id"],
                )
            )["status"] == "completed"
            with pytest.raises(AgentRuntimeError):
                await runtime.fail_reconciled_external(maintenance, execution["operation_id"])
            assert (
                await _row(
                    world,
                    "SELECT status FROM agent_message WHERE sender_run_id=:id "
                    "AND kind='task.failed'",
                    id=child_row["id"],
                )
            )["status"] == "pending"
            assert backend.calls == 1 and remote.sends == 1
            return
        if resolve_claim_actor:
            from agent_runtime.storage.host_worker import SqlClaimChildHandler

            class ChildHandlers:
                async def open(self, actor, work, lease):
                    assert (actor.scope_id, actor.actor_id, actor.auth_revision) == (
                        current_actor.scope_id,
                        current_actor.actor_id,
                        current_actor.auth_revision,
                    )
                    return worker

            await SqlClaimChildHandler(engine, world.schema, ChildHandlers())(claimed, handle)
        else:
            await worker(claimed, handle)
        assert backend.calls == (2 if with_tool else 1)
    assert (await _row(world, "SELECT status FROM run WHERE id=:id", id=child_row["id"]))[
        "status"
    ] == "completed"
    if with_tool:
        assert (
            await _row(world, "SELECT status FROM tool_call WHERE run_id=:id", id=child_row["id"])
        )["status"] == "completed"
    if effect_mode:
        assert (
            await _row(
                world,
                "SELECT status FROM operation WHERE task_id=:id",
                id=child_row["task_id"],
            )
        )["status"] == "committed"
        assert (
            await _row(
                world,
                "SELECT count(*) AS n FROM artifact WHERE task_id=:id",
                id=child_row["task_id"],
            )
        )["n"] == 1
    allocation = await _row(
        world,
        "SELECT settled FROM budget_allocation WHERE id=:id",
        id=claimed.budget_allocation_id,
    )
    assert allocation["settled"]["model_calls"] == (2 if with_tool else 1)
    if live_model:
        assert 0 < allocation["settled"]["tokens"] <= 1000
        assert (
            await _row(
                world,
                "SELECT count(*) AS n FROM model_attempt WHERE run_id=:id AND status='completed'",
                id=child_row["id"],
            )
        )["n"] == 1
    else:
        assert allocation["settled"]["tokens"] == (40 if with_tool else 20)


async def _mcp_broker_tool(
    world, engine, invocation, grant, resource, authorizer, guard, server, spec, *, read_only=False
):
    from uuid import NAMESPACE_URL, uuid5

    from agent_runtime.contracts.credentials import EgressDestination, ScopeRequest
    from agent_runtime.credentials import CredentialBroker
    from agent_runtime.credentials.https import OutboundProtocolProfile, PinnedHttpsTransport
    from agent_runtime.credentials.ports import FilteredBody
    from agent_runtime.mcp.host import McpBrokerEffectTool, McpBrokerReadTool
    from agent_runtime.storage.credential_broker import SqlCredentialGrantStore

    port, tls, server_state = server
    server_state["drop_next_effect"] = not read_only
    effect_ref = CapabilityRef.model_validate(
        {"key": {"namespace": "test.mcp", "key": "effect"}, "revision": "1"}
    )
    lookup_ref = CapabilityRef.model_validate(
        {"key": {"namespace": "test.mcp", "key": "lookup"}, "revision": "1"}
    )
    search_ref = CapabilityRef.model_validate(
        {"key": {"namespace": "test.mcp", "key": "search"}, "revision": "1"}
    )
    destination_refs = (search_ref,) if read_only else (effect_ref, lookup_ref)
    targets = {
        ref: EgressDestination(
            destination=ref,
            hostname="mcp.test",
            port=port,
            action="write" if ref == effect_ref else "read",
            method="POST",
            path="/mcp",
            private_addresses=("127.0.0.1",),
        )
        for ref in destination_refs
    }

    async def authorize(db, caller, scope, request, phase):
        assert caller == invocation.actor
        assert scope.policy_decision_id == grant.decision_id
        assert scope.destination in targets
        assert scope.actions == (targets[scope.destination].action,)
        assert scope.input_digest == content_digest(invocation.call.arguments)
        await authorizer.require_in_transaction(db, guard, invocation, (resource,))

    class Blobs:
        def __init__(self):
            self.items = {}

        async def read(self, caller, key, limit):
            assert caller == invocation.actor and len(self.items[key]) <= limit
            return self.items[key]

        async def put_private(self, caller, request_id, data):
            assert caller == invocation.actor
            key = str(uuid4())
            self.items[key] = data
            return key

    class Registry:
        def resolve(self, ref):
            return targets[ref]

    class Vault:
        async def bearer(self, scope_id, ref):
            assert scope_id == world.scope and ref in targets
            return server_state["token"]

    class Resolver:
        async def resolve(self, hostname, port_number):
            assert hostname == "mcp.test" and port_number == port
            return ("127.0.0.1",)

    class Filter:
        async def apply(self, target, reply):
            return FilteredBody(reply.body, target.destination, "allowed")

    blobs = Blobs()
    transport = PinnedHttpsTransport(
        tls=tls,
        protocol_profiles={
            ref: OutboundProtocolProfile(
                kind="mcp_tool",
                tool_name={
                    effect_ref: "increment_counter",
                    lookup_ref: "lookup_effect",
                    search_ref: "search",
                }[ref],
            )
            for ref in destination_refs
        },
    )
    broker = CredentialBroker(
        store=SqlCredentialGrantStore(engine, world.schema, authorize=authorize),
        vault=Vault(),
        blobs=blobs,
        registry=Registry(),
        resolver=Resolver(),
        transport=transport,
        response_filter=Filter(),
    )

    async def issue(current, destination, action):
        assert current == invocation and (read_only or current.operation_id is not None)
        identity = (
            {"tool_call_id": current.call.tool_call_id}
            if read_only
            else {
                "execution_id": uuid5(
                    NAMESPACE_URL, f"execution:{world.scope}:{current.operation_id}"
                ),
                "execution_generation": "0",
                "operation_id": current.operation_id,
            }
        )
        scope = ScopeRequest(
            actor=current.actor,
            session_id=current.session_id,
            run_id=current.run_id,
            destination=destination,
            actions=(action,),
            policy_decision_id=grant.decision_id,
            input_digest=content_digest(current.call.arguments),
            expires_at=current.deadline_at,
            max_requests=3,
            max_response_bytes=4096,
            **identity,
        )
        return await broker.authorize(current.actor, scope)

    if read_only:
        return McpBrokerReadTool(
            spec=spec, source=spec.source, broker=broker, blobs=blobs, issue_grant=issue
        ), transport
    tool = McpBrokerEffectTool(
        spec=spec,
        source=spec.source,
        broker=broker,
        blobs=blobs,
        issue_grant=issue,
    )

    class ObservedTool:
        last_error = None

        async def dispatch(self, current, *, key):
            try:
                return await tool.dispatch(current, key=key)
            except Exception as exc:
                self.last_error = exc
                raise

        async def lookup(self, current, *, key):
            try:
                return await tool.lookup(current, key=key)
            except Exception as exc:
                self.last_error = exc
                raise

    return ObservedTool(), transport


async def _check_child_tool_host(
    world, engine, current_actor, child_row, guard, resource, *, scenario, mcp_server=None
):
    from agent_runtime.contracts.capacity import CapacityObserveRequest
    from agent_runtime.contracts.policy import PolicyAllow, PolicyRequest
    from agent_runtime.contracts.tools import ToolInvocation, ToolSpec
    from agent_runtime.policy_validation import policy_input_digest
    from agent_runtime.storage.child_tools import SqlChildToolAuthorizer
    from agent_runtime.storage.tool_execution_gate import SqlToolExecutionGate
    from agent_runtime.storage.tool_host import SqlToolHost, ToolEffect

    child_id = guard.lease.run_id
    attempt_id = await _seed_attempt(world, run_id=child_id)
    snapshot_id = (
        await _row(
            world, "SELECT context_snapshot_id FROM model_attempt WHERE id=:id", id=attempt_id
        )
    )["context_snapshot_id"]
    checkpoint_ref = (
        await _row(
            world, "SELECT state_ref FROM checkpoint WHERE id=:id", id=child_row["checkpoint_id"]
        )
    )["state_ref"]
    state, context, _ = facts()
    state = state.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": child_id,
            "state_version": guard.expected_state_version,
            "checkpoint_ref": checkpoint_ref,
            "cancel_version": str(child_row["cancel_version"]),
            "applied_input_seq": str(child_row["applied_input_version"]),
            "pending_input_seq": str(child_row["pending_input_version"]),
        }
    )
    context = context.model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": child_id,
            "snapshot_id": snapshot_id,
        }
    )
    frame = DecisionFrame(
        actor=current_actor,
        context=context,
        attempt_id=attempt_id,
        action=decision_adapter.validate_python(tool_action()),
    )

    class Context:
        async def load(self, scope_id, requested_id):
            assert scope_id == world.scope and requested_id == snapshot_id
            return context, {}

    class Access:
        async def verify(self, actor_context, prepared):
            assert actor_context == current_actor and prepared == context

    store = SqlDecisionStore(
        engine,
        world.schema,
        actor=current_actor,
        lease=guard.lease,
        payloads=BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema)),
        context=Context(),
        access=Access(),
        journal=SqlJournal(engine, world.schema, current_actor),
        child_guard=lambda: guard,
    )
    await store.persist(state, frame)
    call = frame.action.payload.validated_calls[0]
    external = scenario in {
        "child_tool_host_external",
        "child_tool_host_mcp",
        "child_tool_host_mcp_supervisor",
        "child_tool_host_mcp_broker",
        "child_tool_host_mcp_broker_approval_budget",
        "child_tool_host_supervisor",
        "child_tool_host_cancel_supervisor",
        "child_tool_host_process_supervisor",
        "child_tool_host_process_cancel_supervisor",
        "child_tool_host_process_takeover_supervisor",
        "child_tool_host_process_mcp_supervisor",
        "child_tool_host_process_mcp_cancel_supervisor",
        "child_tool_host_process_mcp_takeover_supervisor",
    }
    read_only = scenario in {
        "child_tool_host_read",
        "child_tool_host_mcp_broker_read",
        "child_tool_host_mcp_broker_resource_changed",
        "child_tool_host_mcp_broker_access_revoked",
    }
    spec = ToolSpec(
        ref=call.tool,
        description="Effectful test capability",
        source=call.tool,
        kind="read" if read_only else "write",
        side_effect="none" if read_only else ("external" if external else "database"),
        input_schema=call.tool,
        output_schema=call.tool,
        permissions=(),
        approval_policy=call.tool,
        idempotency="read_only" if read_only else ("provider_key" if external else "transactional"),
        timeout_seconds=30,
        concurrency_group="test-effect",
        max_concurrency=1,
        cancellation="stop",
        public_result_policy="summary",
    )
    if scenario.startswith("child_tool_host_mcp_broker"):
        from agent_runtime.contracts.tools import McpToolBinding
        from agent_runtime.mcp.install import McpToolSchemas

        input_schema = {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": False,
        }
        output_schema = (
            {
                "type": "object",
                "required": ["resultType", "content"],
                "properties": {"resultType": {"const": "complete"}},
            }
            if read_only
            else {
                "type": "object",
                "required": ["operation_key", "ok"],
                "properties": {"ok": {"const": True}},
            }
        )
        spec = ToolSpec.model_validate(
            {
                **spec.model_dump(mode="json", by_alias=True),
                "source": {"key": {"namespace": "test.mcp", "key": "server"}, "revision": "1"},
                "inputSchema": {
                    "key": {"namespace": "mcp.input", "key": content_digest(input_schema)},
                    "revision": "1",
                },
                "outputSchema": {
                    "key": {"namespace": "mcp.output", "key": content_digest(output_schema)},
                    "revision": "1",
                },
                "mcp": McpToolBinding(
                    protocol_version="2026-07-28",
                    tool_name="search" if read_only else "increment_counter",
                    destination=CapabilityRef.model_validate(
                        {
                            "key": {
                                "namespace": "test.mcp",
                                "key": "search" if read_only else "effect",
                            },
                            "revision": "1",
                        }
                    ),
                    input_schema=input_schema,
                    output_schema=output_schema,
                    lookup_name=None if read_only else "lookup_effect",
                    lookup_destination=None
                    if read_only
                    else CapabilityRef.model_validate(
                        {"key": {"namespace": "test.mcp", "key": "lookup"}, "revision": "1"}
                    ),
                ).model_dump(mode="json", by_alias=True),
            }
        )
    invocation = ToolInvocation(
        actor=current_actor,
        session_id=world.session_id,
        run_id=child_id,
        model_attempt_id=attempt_id,
        context_snapshot_id=snapshot_id,
        context_version="1",
        call=call,
        operation_id=None if read_only else uuid4(),
        deadline_at=datetime.now(UTC) + timedelta(minutes=2),
    )
    request = PolicyRequest(
        invocation=invocation, policy=spec.approval_policy, resources=(resource,)
    )
    grant = PolicyAllow(
        decision="allow",
        decision_id=uuid4(),
        policy=spec.approval_policy,
        input_digest=policy_input_digest(request),
        evidence_refs=(resource,),
        evaluated_at=datetime.now(UTC),
        expires_at=invocation.deadline_at,
        grant_ref="test-effect-grant",
    )

    class Resolver:
        async def get(self, call_request):
            assert call_request.tool == spec.ref
            return spec

        async def resources(self, current, pinned):
            assert current == invocation and pinned == spec
            return (resource,)

    class Schemas:
        def input(self, pinned, arguments):
            assert pinned == spec and isinstance(arguments.get("query"), str)

        def output(self, pinned, data):
            assert pinned == spec and isinstance(data, dict)
            if scenario == "child_tool_host_mcp_broker_read":
                assert data.get("resultType") == "complete" and data.get("content")
            else:
                assert data.get("ok") is True

    authorizer = SqlChildToolAuthorizer(
        world.schema,
        current_actor,
        SqlRuntimeSourceReader(
            engine, world.schema, blobs=SqlContextBlobBackend(engine, world.schema)
        ),
    )

    async def authorize(db, current, refs):
        await authorizer.require_in_transaction(db, guard, current, refs)

    class DatabaseTool:
        calls = 0

        async def invoke(self, db, current):
            self.calls += 1
            await db.execute(
                sa.text(
                    f'INSERT INTO "{world.schema}".artifact '
                    "(id,scope_id,owner_actor_id,task_id) VALUES (:id,:scope,:actor,:task)"
                ),
                {
                    "id": uuid4(),
                    "scope": world.scope,
                    "actor": current_actor.actor_id,
                    "task": child_row["task_id"],
                },
            )
            if scenario == "child_tool_host_rollback" and self.calls == 1:
                raise RuntimeError("effect rolled back")
            return ToolEffect(data={"ok": True}, summary="Artifact created")

    class ReadHandler:
        calls = 0

        async def invoke(self, current):
            self.calls += 1
            assert current == invocation
            return ToolEffect(data={"ok": True}, summary="Read completed")

    class ExternalTool:
        sends = 0
        saved = None

        async def dispatch(self, current, *, key):
            self.sends += 1
            assert key == str(invocation.operation_id)
            self.saved = ToolEffect(data={"ok": True}, summary="Remote result stored")
            raise OSError("response lost after remote commit")

        async def lookup(self, current, *, key):
            assert key == str(invocation.operation_id)
            return self.saved

    class McpExternalTool:
        sends = 0

        async def dispatch(self, current, *, key):
            from scripts.verify_runtime_mcp_effect import invoke

            self.sends += 1
            result = await invoke(
                os.environ["AGENT_RUNTIME_MCP_URL"],
                os.environ["AGENT_RUNTIME_MCP_TOKEN"],
                "increment_counter",
                {"operation_key": key, "counter": f"run-{current.run_id}", "amount": 3},
            )
            assert result["ok"] and result["value"] == 3
            raise OSError("response lost after MCP committed the effect")

        async def lookup(self, current, *, key):
            from scripts.verify_runtime_mcp_effect import invoke

            result = await invoke(
                os.environ["AGENT_RUNTIME_MCP_URL"],
                os.environ["AGENT_RUNTIME_MCP_TOKEN"],
                "lookup_effect",
                {"operation_key": key},
            )
            assert result["found"] and result["value"] == 3
            return ToolEffect(data={"ok": True}, summary="MCP write reconciled")

    database = DatabaseTool()
    read_handler = ReadHandler()
    remote = (
        McpExternalTool()
        if scenario in {"child_tool_host_mcp", "child_tool_host_mcp_supervisor"}
        else ExternalTool()
    )
    broker_transport = None
    if scenario.startswith("child_tool_host_mcp_broker"):
        broker_tool, broker_transport = await _mcp_broker_tool(
            world,
            engine,
            invocation,
            grant,
            resource,
            authorizer,
            guard,
            mcp_server,
            spec,
            read_only=read_only,
        )
        if read_only:
            read_handler = broker_tool
        else:
            remote = broker_tool
    from agent_runtime.storage.tool_capacity import SqlToolCapacity
    from agent_runtime.storage.uow.capacity import SqlSystemCapacityStore

    capacity_store = SqlSystemCapacityStore(engine, world.schema)
    capacity = SqlToolCapacity(capacity_store, deployment_id="test-tool-host")
    await capacity_store.seed(capacity.key(current_actor.scope_id, spec), capacity=1)
    if external:
        await install_tool_revision(engine, world, spec)
    host = SqlToolHost(
        engine,
        world.schema,
        specs=Resolver(),
        resources=Resolver(),
        schemas=McpToolSchemas() if spec.mcp is not None else Schemas(),
        authorize=authorize,
        capacity=capacity,
        execution_gate=SqlToolExecutionGate(
            world.schema,
            approval_required=lambda invocation, pinned, resources: (
                scenario
                in {"child_tool_host_approval_budget", "child_tool_host_mcp_broker_approval_budget"}
            ),
        ),
        read_tools={call.tool: read_handler} if read_only else {},
        database_tools={call.tool: database} if not external and not read_only else {},
        external_tools={call.tool: remote} if external else {},
    )
    before = await _row(world, "SELECT count(*) AS n FROM artifact")
    mcp_seen_before = len(mcp_server[2]["seen"]) if mcp_server is not None else 0
    mcp_effect_before = mcp_server[2]["effect_count"] if mcp_server is not None else 0
    if scenario in {
        "child_tool_host_resource_changed",
        "child_tool_host_mcp_broker_resource_changed",
    }:
        assert resource.resource_type == "goal_revision"
        async with engine.begin() as db:
            await db.execute(
                sa.text(
                    f'UPDATE "{world.schema}".goal_revision SET revision=revision+1 WHERE id=:id'
                ),
                {"id": UUID(resource.resource_id)},
            )
        with pytest.raises(AgentRuntimeError) as stale:
            await host.execute(invocation, grant)
        assert stale.value.failure.code == "VERSION_CONFLICT"
        assert database.calls == 0
        assert (await _row(world, "SELECT count(*) AS n FROM artifact"))["n"] == before["n"]
        assert (
            await _row(
                world,
                "SELECT count(*) AS n FROM operation WHERE id=:id",
                id=invocation.operation_id,
            )
        )["n"] == 0
        assert (
            await capacity_store.observe(
                CapacityObserveRequest(key=capacity.key(current_actor.scope_id, spec))
            )
        ).reserved == "0"
        if scenario == "child_tool_host_mcp_broker_resource_changed":
            assert len(mcp_server[2]["seen"]) == mcp_seen_before
            await broker_transport.aclose()
        return
    if scenario == "child_tool_host_mcp_broker_access_revoked":
        async with engine.begin() as db:
            await db.execute(
                sa.text(
                    f'UPDATE "{world.schema}".capability_grant '
                    "SET allowed_scope=jsonb_set(allowed_scope,'{tools}','[]'::jsonb) "
                    "WHERE run_id=:run AND actor_id=:actor "
                    "AND allowed_scope->>'kind' IS DISTINCT FROM 'credential_proxy'"
                ),
                {"run": child_id, "actor": current_actor.actor_id},
            )
        with pytest.raises(AgentRuntimeError) as revoked:
            await host.execute(invocation, grant)
        assert revoked.value.failure.code == "FORBIDDEN"
        assert len(mcp_server[2]["seen"]) == mcp_seen_before
        assert (await _row(world, "SELECT count(*) AS n FROM artifact"))["n"] == before["n"]
        await broker_transport.aclose()
        return
    if scenario in {
        "child_tool_host_approval_budget",
        "child_tool_host_mcp_broker_approval_budget",
    }:
        with pytest.raises(AgentRuntimeError) as missing:
            await host.execute(invocation, grant)
        assert missing.value.failure.code == "FORBIDDEN"
        if scenario == "child_tool_host_mcp_broker_approval_budget":
            assert len(mcp_server[2]["seen"]) == mcp_seen_before
        approval_id = uuid4()
        async with engine.begin() as db:
            await db.execute(
                sa.text(
                    f'INSERT INTO "{world.schema}".approval '
                    "(id,scope_id,run_id,actor_id,request_key,input_hash,status,decision,"
                    "revision,auth_revision,policy_ref,tool_revision,resource_refs,expires_at,"
                    "approved_at) VALUES (:id,:scope,:run,:actor,:key,:hash,'approved',"
                    "'approved',1,:auth,CAST(:policy AS jsonb),CAST(:tool AS jsonb),"
                    "CAST(:refs AS jsonb),:expires,:now)"
                ),
                {
                    "id": approval_id,
                    "scope": world.scope,
                    "run": child_id,
                    "actor": current_actor.actor_id,
                    "key": f"tool:{invocation.operation_id}",
                    "hash": content_digest(invocation.call.arguments),
                    "auth": int(current_actor.auth_revision),
                    "policy": json.dumps(
                        spec.approval_policy.model_dump(mode="json", by_alias=True)
                    ),
                    "tool": json.dumps(spec.ref.model_dump(mode="json", by_alias=True)),
                    "refs": json.dumps([resource.model_dump(mode="json", by_alias=True)]),
                    "expires": invocation.deadline_at,
                    "now": datetime.now(UTC),
                },
            )
        grant = grant.model_copy(update={"grant_ref": f"approval:{approval_id}:1"})
        allocation = await _row(
            world,
            "SELECT b.id,b.limits FROM budget_allocation b "
            "JOIN agent_message m ON m.scope_id=b.scope_id "
            "WHERE m.recipient_run_id=:id AND m.kind='task.assign' "
            "AND m.payload->'payload'->>'budgetAllocationId'=CAST(b.id AS text)",
            id=child_id,
        )
        async with engine.begin() as db:
            await db.execute(
                sa.text(
                    f'UPDATE "{world.schema}".budget_allocation '
                    "SET limits=jsonb_set(limits,'{tool_calls}','0'::jsonb) WHERE id=:id"
                ),
                {"id": allocation["id"]},
            )
        with pytest.raises(AgentRuntimeError) as exhausted:
            await host.execute(invocation, grant)
        assert exhausted.value.failure.code == "BUDGET_EXHAUSTED"
        if scenario == "child_tool_host_mcp_broker_approval_budget":
            assert len(mcp_server[2]["seen"]) == mcp_seen_before
        assert database.calls == 0
        assert (await _row(world, "SELECT count(*) AS n FROM artifact"))["n"] == before["n"]
        assert (
            await _row(
                world,
                "SELECT count(*) AS n FROM operation WHERE id=:id",
                id=invocation.operation_id,
            )
        )["n"] == 0
        async with engine.begin() as db:
            await db.execute(
                sa.text(
                    f'UPDATE "{world.schema}".budget_allocation '
                    "SET limits=CAST(:limits AS jsonb) WHERE id=:id"
                ),
                {"id": allocation["id"], "limits": json.dumps(allocation["limits"])},
            )
    if scenario == "child_tool_host_rollback":
        with pytest.raises(RuntimeError, match="rolled back"):
            await host.execute(invocation, grant)
        assert (await _row(world, "SELECT count(*) AS n FROM artifact"))["n"] == before["n"]
        assert (
            await _row(
                world,
                "SELECT count(*) AS n FROM operation WHERE id=:id",
                id=invocation.operation_id,
            )
        )["n"] == 0
    if external:
        unknown = await host.execute(invocation, grant)
        assert unknown.status == "failed" and unknown.effect == "unknown"
        capacity_key = capacity.key(current_actor.scope_id, spec)
        assert (
            await capacity_store.observe(CapacityObserveRequest(key=capacity_key))
        ).reserved == "1"
        assert (
            await _row(
                world, "SELECT status FROM operation WHERE id=:id", id=invocation.operation_id
            )
        )["status"] == "unknown"
        execution = await _row(
            world,
            "SELECT id,status,supervisor_work_id FROM execution WHERE operation_id=:id",
            id=invocation.operation_id,
        )
        assert execution["status"] == "unknown"
        assert (
            await _row(
                world,
                "SELECT status,kind FROM work_item WHERE id=:id",
                id=execution["supervisor_work_id"],
            )
        ) == {"status": "pending", "kind": "reconcile_execution"}
        if scenario in {"child_tool_host_mcp_broker", "child_tool_host_mcp_broker_approval_budget"}:
            try:
                completed = await host.execute(invocation, grant)
                assert completed.status == "completed" and completed.effect == "committed", (
                    completed,
                    mcp_server[2]["seen"],
                    remote.last_error,
                )
                assert mcp_server[2]["effect_count"] == mcp_effect_before + 1
                assert [item[0] for item in mcp_server[2]["seen"][-2:]] == [
                    "tools/call",
                    "tools/call",
                ]
                assert (await host.execute(invocation, grant)) == completed
                assert mcp_server[2]["effect_count"] == mcp_effect_before + 1
                await store.record(frame, completed)
                assert (
                    await _row(
                        world, "SELECT status FROM tool_call WHERE id=:id", id=call.tool_call_id
                    )
                )["status"] == "completed"
            finally:
                await broker_transport.aclose()
            return
        if scenario in {
            "child_tool_host_process_supervisor",
            "child_tool_host_process_cancel_supervisor",
            "child_tool_host_process_takeover_supervisor",
            "child_tool_host_process_mcp_supervisor",
            "child_tool_host_process_mcp_cancel_supervisor",
            "child_tool_host_process_mcp_takeover_supervisor",
        }:
            import threading
            from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

            root = Path(__file__).resolve().parents[3]
            remote_calls = []
            cancelling = scenario in {
                "child_tool_host_process_cancel_supervisor",
                "child_tool_host_process_mcp_cancel_supervisor",
            }
            takeover = scenario in {
                "child_tool_host_process_takeover_supervisor",
                "child_tool_host_process_mcp_takeover_supervisor",
            }
            mcp_process = "_process_mcp_" in scenario
            mcp_fixture = mcp_server if mcp_process else None
            if mcp_fixture is not None:
                mcp_state = mcp_fixture[2]
                mcp_state["effects"][str(invocation.operation_id)] = {
                    "operation_key": str(invocation.operation_id),
                    "found": True,
                    "status": "running" if cancelling else "completed",
                }
                mcp_state["delay_next_lookup"] = takeover
                mcp_seen_before = len(mcp_state["seen"])
                mcp_cancel_before = mcp_state["cancel_count"]
            if cancelling:
                async with engine.begin() as db:
                    command_id = uuid4()
                    state_version = (
                        await db.execute(
                            sa.text(f'SELECT state_version FROM "{world.schema}".run WHERE id=:id'),
                            {"id": invocation.run_id},
                        )
                    ).scalar_one()
                    await db.execute(
                        sa.text(
                            f'INSERT INTO "{world.schema}".command '
                            "(id,scope_id,run_id,actor_id,request_key,request_hash,kind,"
                            "status,expected_state_version) "
                            "VALUES (:id,:scope,:run,:actor,:request,:hash,'cancel',"
                            "'accepted',:version)"
                        ),
                        {
                            "id": command_id,
                            "scope": world.scope,
                            "run": invocation.run_id,
                            "actor": current_actor.actor_id,
                            "request": f"cancel:{command_id}",
                            "hash": content_digest({"runId": str(invocation.run_id)}),
                            "version": state_version,
                        },
                    )
                    await db.execute(
                        sa.text(
                            f"UPDATE \"{world.schema}\".run SET status='cancelling',"
                            "phase='cancelling',desired_control='cancel',"
                            "control_command_id=:command WHERE id=:id"
                        ),
                        {"id": invocation.run_id, "command": command_id},
                    )

            class RemoteEndpoint(BaseHTTPRequestHandler):
                def do_GET(self):
                    if self.path != f"/executions/{invocation.operation_id}":
                        self.send_error(404)
                        return
                    remote_calls.append("inspect")
                    if takeover and remote_calls.count("inspect") == 1:
                        time.sleep(10)
                    state = (
                        "running"
                        if cancelling and remote_calls.count("inspect") == 1
                        else ("stopped" if cancelling else "completed")
                    )
                    body = json.dumps({"state": state}).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                def do_POST(self):
                    if not cancelling or self.path != (
                        f"/executions/{invocation.operation_id}/cancel"
                    ):
                        self.send_error(409)
                        return
                    remote_calls.append("cancel")
                    self.send_response(204)
                    self.end_headers()

                def log_message(self, format, *args):
                    return

            remote_server = ThreadingHTTPServer(("127.0.0.1", 0), RemoteEndpoint)
            remote_thread = threading.Thread(target=remote_server.serve_forever, daemon=True)
            remote_thread.start()
            environment = {
                **os.environ,
                "RUNTIME_TEST_SCHEMA": world.schema,
                "RUNTIME_TEST_SCOPE": world.scope,
                "RUNTIME_TEST_DEPLOYMENT": "test-tool-host",
                "RUNTIME_TEST_REMOTE_URL": (f"http://127.0.0.1:{remote_server.server_port}"),
                "RUNTIME_TEST_LEASE_SECONDS": "20" if takeover else "30",
            }
            if mcp_fixture is not None:
                environment.update(
                    {
                        "RUNTIME_TEST_MCP_PORT": str(mcp_fixture[0]),
                        "RUNTIME_TEST_MCP_CERT": mcp_fixture[2]["cert_file"],
                        "RUNTIME_TEST_MCP_TOKEN": mcp_fixture[2]["token"],
                        "RUNTIME_TEST_MCP_ACTOR": current_actor.model_dump_json(by_alias=True),
                    }
                )
            processes = []
            try:
                roles = (
                    ("main", "child", "maintenance")
                    if takeover
                    else ("main", "child", "maintenance", "maintenance")
                )
                for role in roles:
                    processes.append(
                        await asyncio.create_subprocess_exec(
                            sys.executable,
                            "-m",
                            "scripts.runtime_worker",
                            "--factory",
                            "scripts.verify_runtime_p04_maintenance_worker:bootstrap",
                            cwd=root,
                            env={**environment, "RUNTIME_TEST_ROLE": role},
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                        )
                    )
                if takeover:
                    first_inspection = time.monotonic() + 40
                    while (
                        not (
                            len(mcp_fixture[2]["seen"]) > mcp_seen_before
                            if mcp_fixture is not None
                            else remote_calls
                        )
                        and time.monotonic() < first_inspection
                    ):
                        assert all(process.returncode is None for process in processes)
                        await asyncio.sleep(0.1)
                    if mcp_fixture is not None and len(mcp_fixture[2]["seen"]) == mcp_seen_before:
                        pytest.fail("maintenance worker did not reach the pinned MCP lookup")
                    if mcp_fixture is None:
                        assert remote_calls == ["inspect"]
                    else:
                        assert [item[2] for item in mcp_fixture[2]["seen"][mcp_seen_before:]] == [
                            "lookup_effect"
                        ]
                    processes[-1].kill()
                    await processes[-1].communicate()
                    processes.pop()
                    processes.append(
                        await asyncio.create_subprocess_exec(
                            sys.executable,
                            "-m",
                            "scripts.runtime_worker",
                            "--factory",
                            "scripts.verify_runtime_p04_maintenance_worker:bootstrap",
                            cwd=root,
                            env={**environment, "RUNTIME_TEST_ROLE": "maintenance"},
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                        )
                    )
                limit = time.monotonic() + (75 if takeover else 25)
                while time.monotonic() < limit:
                    for process in processes:
                        if process.returncode is not None:
                            _, stderr = await process.communicate()
                            pytest.fail(f"maintenance lane exited early: {stderr.decode()}")
                    row = await _row(
                        world, "SELECT status FROM execution WHERE id=:id", id=execution["id"]
                    )
                    if row["status"] == ("cancelled" if cancelling else "completed"):
                        break
                    await asyncio.sleep(0.1)
                else:
                    pytest.fail("maintenance lane did not settle external execution")
                assert all(process.returncode is None for process in processes)
                if mcp_fixture is None:
                    assert remote_calls.count("inspect") == (2 if cancelling or takeover else 1)
                    assert remote_calls.count("cancel") >= (1 if cancelling else 0)
                else:
                    names = [item[2] for item in mcp_fixture[2]["seen"][mcp_seen_before:]]
                    assert names.count("lookup_effect") >= (2 if takeover else 1)
                    assert mcp_fixture[2]["cancel_count"] == mcp_cancel_before + int(cancelling)
                    assert all(valid for _, valid, _ in mcp_fixture[2]["seen"][mcp_seen_before:])
                assert (
                    await capacity_store.observe(CapacityObserveRequest(key=capacity_key))
                ).reserved == "0"
                assert (
                    await _row(
                        world,
                        "SELECT status FROM work_item WHERE id=:id",
                        id=execution["supervisor_work_id"],
                    )
                )["status"] == "completed"
                assert remote.sends == 1
            finally:
                for process in processes:
                    if process.returncode is None:
                        process.send_signal(signal.SIGTERM)
                    try:
                        await asyncio.wait_for(process.communicate(), timeout=10)
                    except TimeoutError:
                        process.kill()
                        await process.communicate()
                remote_server.shutdown()
                remote_server.server_close()
                remote_thread.join(timeout=5)
            return
        supervisor_claim = await SqlScheduler(engine, world.schema).claim_work(
            claim_request(world.scope, "maintenance")
        )
        assert isinstance(supervisor_claim, MaintenanceClaimedWork)
        assert supervisor_claim.lease.work_id == execution["supervisor_work_id"]
        if scenario == "child_tool_host_mcp_supervisor":

            class McpInspector:
                async def inspect(self, key):
                    from scripts.verify_runtime_mcp_effect import invoke

                    result = await invoke(
                        os.environ["AGENT_RUNTIME_MCP_URL"],
                        os.environ["AGENT_RUNTIME_MCP_TOKEN"],
                        "lookup_effect",
                        {"operation_key": key},
                    )
                    return "completed" if result.get("found") else "unknown"

                async def cancel(self, key):
                    pytest.fail("the immediate MCP effect should not need process cancellation")

            await SqlExecutionSupervisor(
                engine, world.schema, inspector=McpInspector(), capacity=capacity
            )(supervisor_claim)
            confirmed = await _row(
                world,
                "SELECT status,cleanup_status FROM execution WHERE id=:id",
                id=execution["id"],
            )
            assert confirmed == {"status": "completed", "cleanup_status": "pending"}
            assert (
                await _row(
                    world, "SELECT status FROM operation WHERE id=:id", id=invocation.operation_id
                )
            )["status"] == "unknown"
            await store.record(frame, unknown)
            assert remote.sends == 1
            return
        if scenario in {"child_tool_host_supervisor", "child_tool_host_cancel_supervisor"}:
            cancelling = scenario == "child_tool_host_cancel_supervisor"
            if cancelling:
                async with engine.begin() as db:
                    command_id = uuid4()
                    state_version = (
                        await db.execute(
                            sa.text(f'SELECT state_version FROM "{world.schema}".run WHERE id=:id'),
                            {"id": invocation.run_id},
                        )
                    ).scalar_one()
                    await db.execute(
                        sa.text(
                            f'INSERT INTO "{world.schema}".command '
                            "(id,scope_id,run_id,actor_id,request_key,request_hash,kind,"
                            "status,expected_state_version) "
                            "VALUES (:id,:scope,:run,:actor,:request,:hash,'cancel',"
                            "'accepted',:version)"
                        ),
                        {
                            "id": command_id,
                            "scope": world.scope,
                            "run": invocation.run_id,
                            "actor": current_actor.actor_id,
                            "request": f"cancel:{command_id}",
                            "hash": content_digest({"runId": str(invocation.run_id)}),
                            "version": state_version,
                        },
                    )
                    await db.execute(
                        sa.text(
                            f"UPDATE \"{world.schema}\".run SET status='cancelling',"
                            "phase='cancelling',desired_control='cancel',"
                            "control_command_id=:command WHERE id=:id"
                        ),
                        {"id": invocation.run_id, "command": command_id},
                    )

            class Inspector:
                calls = 0
                cancellations = 0

                async def inspect(self, key):
                    assert key == str(invocation.operation_id)
                    self.calls += 1
                    if self.calls == 1:
                        return "running"
                    return "stopped" if cancelling else "completed"

                async def cancel(self, key):
                    assert cancelling and key == str(invocation.operation_id)
                    self.cancellations += 1

            inspector = Inspector()
            supervisor = SqlExecutionSupervisor(
                engine, world.schema, inspector=inspector, capacity=capacity, retry_seconds=1
            )
            await supervisor(supervisor_claim)
            pending = await _row(
                world,
                "SELECT status,attempt_count,lease_epoch FROM work_item WHERE id=:id",
                id=execution["supervisor_work_id"],
            )
            assert pending["status"] == "pending" and pending["attempt_count"] >= 2
            assert pending["lease_epoch"] > int(supervisor_claim.lease.work_epoch)
            assert (
                await _row(
                    world, "SELECT status,failure FROM execution WHERE id=:id", id=execution["id"]
                )
            ) == {"status": "running", "failure": None}
            assert (
                await capacity_store.observe(CapacityObserveRequest(key=capacity_key))
            ).reserved == "1"
            with pytest.raises(AgentRuntimeError):
                await supervisor(supervisor_claim)
            await asyncio.sleep(1.1)
            resumed = await SqlScheduler(engine, world.schema).claim_work(
                claim_request(world.scope, "maintenance")
            )
            assert isinstance(resumed, MaintenanceClaimedWork)
            if not cancelling:
                settle = capacity.settle_execution_in_transaction

                async def fail_after_release(db, scope_id, run_id, operation_id):
                    await settle(db, scope_id, run_id, operation_id)
                    raise RuntimeError("supervisor stopped before terminal commit")

                capacity.settle_execution_in_transaction = fail_after_release
                with pytest.raises(RuntimeError, match="before terminal commit"):
                    await supervisor(resumed)
                capacity.settle_execution_in_transaction = settle
                assert (
                    await _row(
                        world, "SELECT status FROM execution WHERE id=:id", id=execution["id"]
                    )
                )["status"] == "running"
                assert (
                    await capacity_store.observe(CapacityObserveRequest(key=capacity_key))
                ).reserved == "1"
            await supervisor(resumed)
            assert (
                await _row(
                    world,
                    "SELECT status,cleanup_status FROM execution WHERE id=:id",
                    id=execution["id"],
                )
            ) == {
                "status": "cancelled" if cancelling else "completed",
                "cleanup_status": "pending",
            }
            assert (
                await capacity_store.observe(CapacityObserveRequest(key=capacity_key))
            ).reserved == "0"
            assert inspector.cancellations == (2 if cancelling else 0)
            assert (
                await _row(
                    world,
                    "SELECT status FROM operation WHERE id=:id",
                    id=invocation.operation_id,
                )
            )["status"] == "unknown"
            assert (
                await _row(
                    world,
                    "SELECT status FROM work_item WHERE id=:id",
                    id=execution["supervisor_work_id"],
                )
            )["status"] == "completed"
            if cancelling:
                assert remote.sends == 1
                return
            await store.record(frame, unknown)
            return
    completed = await host.execute(invocation, grant)
    assert completed.status == "completed" and completed.result.policy == "summary"
    assert (await host.execute(invocation, grant)) == completed
    if scenario in {
        "child_tool_host_approval_budget",
        "child_tool_host_mcp_broker_approval_budget",
    }:
        async with engine.begin() as db:
            await db.execute(
                sa.text(
                    f"UPDATE \"{world.schema}\".approval SET status='cancelled',"
                    "decision='cancelled' WHERE id=:id"
                ),
                {"id": approval_id},
            )
        with pytest.raises(AgentRuntimeError) as revoked:
            await host.execute(invocation, grant)
        assert revoked.value.failure.code == "FORBIDDEN"
    if read_only:
        assert completed.effect == "none"
        if scenario == "child_tool_host_mcp_broker_read":
            assert [item[0] for item in mcp_server[2]["seen"][-2:]] == ["tools/call", "tools/call"]
            await broker_transport.aclose()
        else:
            assert read_handler.calls == 2
        assert (await _row(world, "SELECT count(*) AS n FROM artifact"))["n"] == before["n"]
        await store.record(frame, completed)
        return
    if external:
        assert remote.sends == 1
        assert (
            await _row(
                world,
                "SELECT status FROM execution WHERE operation_id=:id",
                id=invocation.operation_id,
            )
        )["status"] == "completed"
        assert (
            await _row(
                world,
                "SELECT status FROM work_item WHERE id=:id",
                id=execution["supervisor_work_id"],
            )
        )["status"] == "completed"
    else:
        assert database.calls == (2 if scenario == "child_tool_host_rollback" else 1)
        assert (await _row(world, "SELECT count(*) AS n FROM artifact"))["n"] == before["n"] + 1
    operation = await _row(
        world, "SELECT status,result_ref FROM operation WHERE id=:id", id=invocation.operation_id
    )
    assert operation["status"] == "committed" and operation["result_ref"]
    call_row = await _row(
        world, "SELECT operation_id,status FROM tool_call WHERE id=:id", id=call.tool_call_id
    )
    assert call_row["operation_id"] == invocation.operation_id and call_row["status"] == "pending"
    await store.record(frame, completed)
    assert (await _row(world, "SELECT status FROM tool_call WHERE id=:id", id=call.tool_call_id))[
        "status"
    ] == "completed"


async def _check_child_adapters(
    world, engine, journal, reader, state, context, child_row, guard, resource, child_budget
):
    from sqlalchemy.ext.asyncio import async_sessionmaker
    from test_provider_invoker import make_request

    from agent_runtime.contracts.provider import AnalysisDelta, ContentDelta
    from agent_runtime.contracts.tools import FullResult, ToolCompleted, ToolInvocation
    from agent_runtime.storage.child_budget import SqlChildBudgetReader
    from agent_runtime.storage.child_output import ChildProducerBatch, ChildSealOutputRequest
    from agent_runtime.storage.child_stream import SqlChildStream
    from agent_runtime.storage.child_tools import SqlChildToolAuthorizer
    from agent_runtime.storage.model_events import SqlModelEventSink

    child_id = guard.lease.run_id
    attempt_id = await _seed_attempt(world, run_id=child_id)
    snapshot_id = (
        await _row(
            world, "SELECT context_snapshot_id FROM model_attempt WHERE id=:id", id=attempt_id
        )
    )["context_snapshot_id"]
    checkpoint_ref = (
        await _row(
            world, "SELECT state_ref FROM checkpoint WHERE id=:id", id=child_row["checkpoint_id"]
        )
    )["state_ref"]
    child_context = context.model_copy(update={"run_id": child_id, "snapshot_id": snapshot_id})
    child_state = state.model_copy(
        update={
            "run_id": child_id,
            "state_version": guard.expected_state_version,
            "checkpoint_ref": checkpoint_ref,
            "cancel_version": str(child_row["cancel_version"]),
            "applied_input_seq": str(child_row["applied_input_version"]),
            "pending_input_seq": str(child_row["pending_input_version"]),
        }
    )
    frame = DecisionFrame(
        actor=guard.actor,
        context=child_context,
        attempt_id=attempt_id,
        action=decision_adapter.validate_python(tool_action(count=2)),
    )

    class Context:
        async def load(self, scope_id, requested_id):
            assert scope_id == world.scope and requested_id == snapshot_id
            return child_context, {}

    class Access:
        async def verify(self, current, prepared):
            assert current == guard.actor and prepared == child_context

    shared = dict(
        actor=guard.actor,
        lease=guard.lease,
        context=Context(),
        access=Access(),
        child_guard=lambda: guard,
    )
    store = SqlDecisionStore(
        engine,
        world.schema,
        **shared,
        payloads=BlobContextPayloadStore(SqlContextBlobBackend(engine, world.schema)),
        journal=journal,
    )
    live = SqlLiveRunGuard(engine, world.schema, **shared)
    await live.require_running(child_state, frame)
    missing = SqlLiveRunGuard(
        engine,
        world.schema,
        actor=guard.actor,
        lease=guard.lease,
        context=Context(),
        access=Access(),
    )
    with pytest.raises(AgentRuntimeError):
        await missing.require_running(child_state, frame)
    staged = await store.persist(child_state, frame)
    assert (await store.persist(child_state, frame)).checkpoint_ref == staged.checkpoint_ref
    await live.require_running(staged, frame)
    recovered = await store.recover()
    assert set(recovered.unresolved_call_ids) == {
        call.tool_call_id for call in frame.action.payload.validated_calls
    }

    invocation = ToolInvocation(
        actor=guard.actor,
        session_id=world.session_id,
        run_id=child_id,
        model_attempt_id=attempt_id,
        context_snapshot_id=snapshot_id,
        context_version="1",
        call=frame.action.payload.validated_calls[0],
        operation_id=None,
        deadline_at=datetime.now(UTC) + timedelta(minutes=2),
    )
    authorizer = SqlChildToolAuthorizer(world.schema, guard.actor, reader)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def authorize(candidate=invocation, refs=(resource,), current_guard=guard):
        async with sessions() as db, db.begin():
            await authorizer.require_in_transaction(db, current_guard, candidate, refs)

    await authorize()
    current_checkpoint = (
        await _row(world, "SELECT checkpoint_id FROM run WHERE id=:id", id=child_id)
    )["checkpoint_id"]
    async with engine.begin() as db:
        await db.execute(
            sa.text(f'UPDATE "{world.schema}".run SET checkpoint_id=:checkpoint WHERE id=:id'),
            {"checkpoint": child_row["checkpoint_id"], "id": child_id},
        )
    with pytest.raises(AgentRuntimeError):
        await authorize()
    async with engine.begin() as db:
        await db.execute(
            sa.text(f'UPDATE "{world.schema}".run SET checkpoint_id=:checkpoint WHERE id=:id'),
            {"checkpoint": current_checkpoint, "id": child_id},
        )
    bad_call = invocation.call.model_copy(update={"arguments": {"query": "changed"}})
    for candidate in (
        invocation.model_copy(update={"call": bad_call}),
        invocation.model_copy(update={"context_version": "99"}),
        invocation.model_copy(update={"model_attempt_id": uuid4()}),
        invocation.model_copy(update={"run_id": world.run_id}),
    ):
        with pytest.raises(AgentRuntimeError):
            await authorize(candidate)
    with pytest.raises(AgentRuntimeError):
        await authorize(refs=(resource.model_copy(update={"resource_id": str(uuid4())}),))
    for changed in (
        {"lease": guard.lease.model_copy(update={"run_epoch": "99"})},
        {"lease": guard.lease.model_copy(update={"work_epoch": "99"})},
        {"workspace_generation": "99"},
        {"expected_state_version": "99"},
        {"root_run_id": uuid4()},
    ):
        with pytest.raises(AgentRuntimeError):
            await authorize(current_guard=guard.model_copy(update=changed))

    # Effect boundary shares the same transaction: rollback also rolls back the host write.
    async with sessions() as db:
        with pytest.raises(RuntimeError, match="rollback effect"):
            async with db.begin():
                await authorizer.require_in_transaction(db, guard, invocation, (resource,))
                await db.execute(
                    sa.text(
                        f'UPDATE "{world.schema}".workspace '
                        "SET revision=revision+1 WHERE owner_run_id=:id"
                    ),
                    {"id": child_id},
                )
                raise RuntimeError("rollback effect")
    assert (
        await _row(world, "SELECT revision FROM workspace WHERE owner_run_id=:id", id=child_id)
    )["revision"] == int(guard.workspace_ref.resource_version)

    item_id = await _seed_item(world, run_id=child_id)
    request = make_request().model_copy(
        update={
            "scope_id": world.scope,
            "session_id": world.session_id,
            "run_id": child_id,
            "attempt_id": attempt_id,
        }
    )
    sink = SqlModelEventSink(
        journal,
        lease=guard.lease,
        child_guard=lambda: guard,
        turn_id=child_row["turn_id"],
        attempt_id=attempt_id,
        item_id=item_id,
        item_revision="1",
        trace_id="child-output",
    )
    await sink.record(
        request,
        ContentDelta(
            attempt_id=attempt_id, sequence="0", kind="content.delta", text="child answer"
        ),
    )
    await sink.record(
        request,
        AnalysisDelta(
            attempt_id=attempt_id, sequence="1", kind="analysis.delta", text="child reasoning"
        ),
    )
    seal_request = ChildSealOutputRequest(
        guard=guard,
        attempt_id=attempt_id,
        item_id=item_id,
        proof_id=uuid4(),
        sealed_revision="1",
    )
    proof = await journal.seal_output(seal_request)
    assert {channel.channel for channel in proof.channels} == {"content", "analysis"}
    assert await journal.seal_output(seal_request) == proof
    stream_attempt = await _seed_attempt(world, run_id=child_id)
    stream_snapshot = (
        await _row(
            world, "SELECT context_snapshot_id FROM model_attempt WHERE id=:id", id=stream_attempt
        )
    )["context_snapshot_id"]
    stream_request = request.model_copy(
        update={"attempt_id": stream_attempt, "context_snapshot_id": stream_snapshot}
    )
    stream = SqlChildStream(engine, world.schema, journal, lambda: guard)
    await stream.record(
        stream_request,
        AnalysisDelta(
            attempt_id=stream_attempt, sequence="0", kind="analysis.delta", text="reasoning"
        ),
    )
    await stream.record(
        stream_request,
        ContentDelta(attempt_id=stream_attempt, sequence="1", kind="content.delta", text="result"),
    )
    from agent_runtime.contracts.canonical import content_digest

    answer_ref = f"answer-{content_digest({'runId': str(child_id), 'text': 'result'})}"
    await stream.save(world.scope, str(child_id), answer_ref, "result")
    response = DecisionFrame(
        actor=guard.actor,
        context=child_context.model_copy(update={"snapshot_id": stream_snapshot}),
        attempt_id=stream_attempt,
        action=decision_adapter.validate_python(
            {"kind": "respond", "payload": {"answerRequestRef": answer_ref}}
        ),
    )
    output_ref = await stream.publish(response)
    assert output_ref is not None
    output_request = context_request().model_copy(
        update={"actor": guard.actor, "session_id": world.session_id, "run_id": child_id}
    )
    assert (await reader.read(output_request, output_ref)).content
    assert await stream.publish(response) == output_ref
    budget = await SqlChildBudgetReader(engine, world.schema, child_budget, lambda: guard).read(
        child_state
    )
    assert int(budget.model_calls.spent) >= 2
    assert budget.model_calls.available == 0
    delta = {
        "kind": "assistant.delta",
        "occurredAt": datetime.now(UTC),
        "payload": {"channel": "content", "deltaIndex": "1", "itemRevision": "1", "text": "late"},
    }
    batch = ChildProducerBatch(
        guard=guard,
        turn_id=child_row["turn_id"],
        attempt_id=attempt_id,
        item_id=item_id,
        trace_id="child-output",
        events=(delta,),
    )
    with pytest.raises(AgentRuntimeError):
        await journal.append(batch)
    with pytest.raises(AgentRuntimeError):
        await journal.append(batch.model_copy(update={"turn_id": uuid4()}))
    row = await _row(
        world,
        "SELECT parent_run_id,visibility FROM event WHERE run_id=:id "
        "AND model_attempt_id=:attempt AND kind='analysis.delta'",
        id=child_id,
        attempt=attempt_id,
    )
    assert row["parent_run_id"] == world.run_id and row["visibility"] == "restricted"
    outcome = ToolCompleted(
        tool_call_id=invocation.call.tool_call_id,
        run_id=child_id,
        operation_id=None,
        status="completed",
        effect="none",
        effect_refs=(),
        result=FullResult(policy="full", data={"answer": 42}),
    )
    await store.record(frame, outcome)
    await store.record(frame, outcome)
    recovered = await store.recover()
    assert recovered.outcomes == (outcome,)
    assert len(recovered.unresolved_call_ids) == 1
    with pytest.raises(AgentRuntimeError):
        await authorize()
    pending = invocation.model_copy(update={"call": frame.action.payload.validated_calls[1]})
    await authorize(pending)
    async with engine.begin() as db:
        await db.execute(
            sa.text(
                f'UPDATE "{world.schema}".run SET cancel_version=cancel_version+1 WHERE id=:id'
            ),
            {"id": world.run_id},
        )
    for operation in (
        lambda: live.require_running(staged, frame),
        lambda: store.persist(staged, frame),
        lambda: journal.append(batch),
        lambda: journal.seal_output(seal_request),
        lambda: authorize(pending),
    ):
        with pytest.raises(AgentRuntimeError) as cancelled:
            await operation()
        assert cancelled.value.failure.code == "CONTROL_PENDING"
    assert (await _row(world, "SELECT owner_run_id FROM session_slot"))["owner_run_id"] is None

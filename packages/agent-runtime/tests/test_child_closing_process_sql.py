"""A real child Worker stack dies after the SQL paid-call claim, before a result.

Input and Provider are synthetic; SQL, delegation, process death and recovery are real.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_storage_uow import engine_for, run

pytestmark = pytest.mark.skipif(
    not os.getenv("AGENT_RUNTIME_TEST_DSN"), reason="requires PostgreSQL"
)

BOOT = """
import sys, pytest
from test_storage_uow import migrate, run, _seed
from test_storage_subagents import test_durable_delegate_propose_collect
schema, marker=sys.argv[1:]
assert migrate('upgrade',schema).status=='applied'
world=run(lambda:_seed(schema))
test_durable_delegate_propose_collect(world,pytest.MonkeyPatch(),'child_worker_closing',None,
    closing_floors=True,closing_crash_marker=marker)
"""

MAINTAIN = """
import asyncio,json,sys
from datetime import timedelta
from agent_runtime.contracts.ownership import ChildCommitGuard
from agent_runtime.storage.recovery import SqlRecovery
from test_storage_uow import engine_for
async def main():
 data=json.loads(sys.stdin.read())
 engine=engine_for(data['schema'])
 try:
  guard=ChildCommitGuard.model_validate(data['guard'])
  now=max(guard.lease.run_lease_until,guard.lease.work_lease_until)+timedelta(seconds=1)
  recovery=SqlRecovery(engine,data['schema'],clock=lambda:now)
  report=await recovery.recover_expired_leases(data['scope'])
  print(json.dumps({'requeued':report.requeued,'failed':report.failed}),flush=True)
 finally: await engine.dispose()
asyncio.run(main())
"""


def test_child_closing_claim_survives_sigkill_and_independent_maintenance(tmp_path):
    schema = "p03_child_loss_" + uuid4().hex[:12]
    marker = tmp_path / "claimed.json"
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(Path(__file__).parent), env.get("PYTHONPATH", "")])
    log = (tmp_path / "worker.log").open("w")
    child = subprocess.Popen(
        [sys.executable, "-c", BOOT, schema, str(marker)], env=env, stdout=log, stderr=log
    )
    try:
        deadline = time.monotonic() + 240
        while not marker.exists():
            assert child.poll() is None, (tmp_path / "worker.log").read_text()
            assert time.monotonic() < deadline, "child never reached the committed closing claim"
            time.sleep(0.2)
        child.kill()
        child.wait(timeout=10)
        packet = json.loads(marker.read_text())
        result = subprocess.run(
            [sys.executable, "-c", MAINTAIN],
            input=json.dumps(packet),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            check=True,
        )
        assert json.loads(result.stdout) == {"requeued": 1, "failed": 0}

        async def verify():
            from datetime import timedelta

            from test_storage_subagents import claim_request

            from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
            from agent_runtime.contracts.model import ProviderRequest
            from agent_runtime.contracts.ownership import ChildCommitGuard
            from agent_runtime.contracts.references import CapabilityRef
            from agent_runtime.contracts.work_claims import ChildClaimedWork
            from agent_runtime.storage.context_blobs import SqlContextBlobBackend
            from agent_runtime.storage.context_sources import SqlRuntimeSourceReader
            from agent_runtime.storage.journal import SqlJournal
            from agent_runtime.storage.model_attempts import SqlModelAttemptStore
            from agent_runtime.storage.scheduler import SqlScheduler
            from agent_runtime.storage.subagents import SqlSubagentRuntime

            engine = engine_for(schema)
            try:
                request = ProviderRequest.model_validate(packet["request"])
                guard = ChildCommitGuard.model_validate(packet["guard"])
                async with engine.begin() as db:
                    row = (
                        (
                            await db.execute(
                                text("SELECT status,usage_status FROM model_attempt WHERE id=:id"),
                                {"id": request.attempt_id},
                            )
                        )
                        .mappings()
                        .one()
                    )
                    assert dict(row) == {"status": "interrupted", "usage_status": "unknown"}
                    entries = (
                        await db.execute(
                            text(
                                "SELECT kind,count(*) FROM budget_entry "
                                "WHERE operation_key=:key GROUP BY kind"
                            ),
                            {"key": f"runtime.finalization:{request.run_id}"},
                        )
                    ).all()
                    assert dict(entries) == {"reserve": 1, "adjust": 1}
                    assert (
                        await db.execute(
                            text("SELECT count(*) FROM model_attempt WHERE run_id=:id"),
                            {"id": request.run_id},
                        )
                    ).scalar_one() == 1
                    assert (
                        await db.execute(
                            text("SELECT status FROM run WHERE id=:id"), {"id": request.run_id}
                        )
                    ).scalar_one() == "queued"
                store = SqlModelAttemptStore(engine, schema, child_guard=lambda: guard)
                from test_storage_model_attempts import completed

                with pytest.raises(AgentRuntimeError):
                    await store.finish(request, completed())
                async with engine.connect() as db:
                    available = (
                        await db.execute(
                            text(
                                "SELECT min(available_at) FROM work_item "
                                "WHERE run_id=:id AND status='pending'"
                            ),
                            {"id": request.run_id},
                        )
                    ).scalar_one()
                assert available is not None
                resumed = await SqlScheduler(
                    engine, schema, clock=lambda: available + timedelta(seconds=1)
                ).claim_work(claim_request(packet["scope"], "child"))
                assert isinstance(resumed, ChildClaimedWork)
                fresh = guard.model_copy(
                    update={"lease": resumed.lease, "expected_state_version": resumed.state_version}
                )
                fresh_store = SqlModelAttemptStore(engine, schema, child_guard=lambda: fresh)
                from datetime import UTC, datetime

                with pytest.raises(AgentRuntimeError) as denied:
                    await fresh_store.begin(
                        request.model_copy(update={"attempt_id": uuid4()}),
                        started_at=datetime.now(UTC),
                    )
                assert denied.value.failure.code == "USAGE_UNRESOLVED"
                resources = SqlRuntimeSourceReader(
                    engine, schema, blobs=SqlContextBlobBackend(engine, schema)
                )
                runtime = SqlSubagentRuntime(
                    engine,
                    schema,
                    actor=guard.actor,
                    journal=SqlJournal(engine, schema, guard.actor),
                    resources=resources,
                    policies={},
                    message_predicate=CapabilityRef.model_validate(
                        {"key": {"namespace": "builtin", "key": "message"}, "revision": "1"}
                    ),
                    verifier=None,
                )
                await runtime.report_failure(
                    fresh,
                    FailureEnvelope(
                        code="USAGE_UNRESOLVED",
                        category="budget",
                        retryable=False,
                        public_message="Previous model usage is unknown.",
                    ),
                )
                async with engine.connect() as db:
                    assert (
                        await db.execute(
                            text("SELECT status FROM run WHERE id=:id"), {"id": request.run_id}
                        )
                    ).scalar_one() == "failed"
                    assert (
                        await db.execute(
                            text(
                                "SELECT count(*) FROM work_item WHERE run_id=:id "
                                "AND status IN ('pending','leased')"
                            ),
                            {"id": request.run_id},
                        )
                    ).scalar_one() == 0
                    assert (
                        await db.execute(
                            text("SELECT count(*) FROM model_attempt WHERE run_id=:id"),
                            {"id": request.run_id},
                        )
                    ).scalar_one() == 1
                    assert (
                        await db.execute(
                            text(
                                "SELECT count(*) FROM agent_message "
                                "WHERE sender_run_id=:id AND kind='task.failed'"
                            ),
                            {"id": request.run_id},
                        )
                    ).scalar_one() == 1
            finally:
                await engine.dispose()

        run(verify)
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=10)
        log.close()

        async def drop():
            engine = engine_for(schema)
            try:
                async with engine.begin() as db:
                    await db.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            finally:
                await engine.dispose()

        run(drop)

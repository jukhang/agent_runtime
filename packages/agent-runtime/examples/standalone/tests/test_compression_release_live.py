"""Independent API/Worker/maintenance process performance and hard-crash probes."""

import asyncio
import json
import math
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from test_live import deployment, port, submit, wait_result  # noqa: F401

from agent_runtime.storage.recovery import RecoveryConfig

pytestmark = pytest.mark.skipif(
    os.environ.get("CENTN_LIVE") != "1", reason="Real services required"
)


def persist(context, name, data):
    directory = Path(os.environ.get("CENTN_RELEASE_EVIDENCE_DIR", str(context["logs"])))
    directory.mkdir(parents=True, exist_ok=True)
    body = json.dumps(data, default=str, indent=2)
    (directory / name).write_text(body)
    (context["logs"] / name).write_text(body)


@pytest.fixture
def fault_deployment(request, monkeypatch, tmp_path):
    mode = request.param
    listen = port()
    endpoint = os.environ["CENTN_PROVIDER_URL"]
    transcript = tmp_path / "proxy.jsonl"
    log = (tmp_path / "proxy.log").open("wb")
    proxy = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).with_name("compression_faults.py")),
            "--port",
            str(listen),
            "--upstream",
            endpoint,
            "--mode",
            mode,
            "--evidence",
            str(transcript),
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    monkeypatch.setenv("CENTN_PROVIDER_URL", f"http://127.0.0.1:{listen}/v1")
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert proxy.poll() is None
            try:
                if (
                    httpx.get(f"http://127.0.0.1:{listen}/health", trust_env=False).status_code
                    == 200
                ):
                    break
            except httpx.TransportError:
                pass
            time.sleep(0.1)
        else:
            pytest.fail("Real Provider fault proxy did not start")
        host = request.getfixturevalue("deployment")
        yield {**host, "faultMode": mode, "proxyTranscript": transcript, "proxy": proxy}
    finally:
        if proxy.poll() is None:
            proxy.terminate()
            proxy.wait(10)
        log.close()
        (tmp_path / "proxy-cleanup.json").write_text(
            json.dumps(
                {
                    "pid": proxy.pid,
                    "exitCode": proxy.returncode,
                    "alive": proxy.poll() is None,
                }
            )
        )


@pytest.mark.parametrize(
    "fault_deployment", ["empty", "truncated", "disconnect", "timeout"], indirect=True
)
def test_independent_host_provider_fault_terminal_contract(fault_deployment):
    host = fault_deployment
    mode = host["faultMode"]
    host["worker"].terminate()
    host["worker"].wait(30)
    model_limit = 2 if mode == "empty" else 1
    run_timeout = 45 if mode == "timeout" else 120
    terminal_grace = 60
    task = (
        "Reply exactly with validation-ok. Do not use tools."
        if mode == "empty"
        else "Write 20 numbered, detailed engineering checks. Do not use any tools."
    )
    receipt = submit(host, task)

    async def set_test_budget():
        engine = create_async_engine(host["dsn"])
        try:
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f"UPDATE {host['schema']}.budget_allocation "
                        "SET limits=jsonb_set(limits,'{model_calls}',CAST(:limit AS jsonb)) "
                        "WHERE root_run_id=:run"
                    ),
                    {"run": receipt["runId"], "limit": str(model_limit)},
                )
                for table, selector in (("run", "id"), ("task", "root_run_id")):
                    await db.execute(
                        sa.text(
                            f"UPDATE {host['schema']}.{table} "
                            "SET deadline_at=now()+:seconds*interval '1 second' "
                            f"WHERE {selector}=:run"
                        ),
                        {"run": receipt["runId"], "seconds": run_timeout},
                    )
        finally:
            await engine.dispose()

    asyncio.run(set_test_budget())
    replacement = host["launch"]("worker")
    retry_history = []

    async def wait_with_retry_audit():
        engine = create_async_engine(host["dsn"])
        deadline = time.monotonic() + run_timeout + terminal_grace
        try:
            while time.monotonic() < deadline:
                async with engine.connect() as db:
                    state = dict(
                        (
                            await db.execute(
                                sa.text(
                                    "SELECT r.status,r.state_version,r.owner_epoch,r.deadline_at,"
                                    "w.status AS work_status,w.attempt_count,w.lease_epoch,"
                                    "w.available_at,w.deadline_at AS work_deadline,"
                                    "b.limits,b.version AS budget_version,"
                                    "b.reserved,b.unknown_reserved,"
                                    f"(SELECT count(*) FROM {host['schema']}.model_attempt m "
                                    "WHERE m.run_id=r.id) AS model_attempts "
                                    f"FROM {host['schema']}.run r "
                                    f"JOIN {host['schema']}.work_item w "
                                    "ON w.run_id=r.id AND w.kind='advance_run' "
                                    f"JOIN {host['schema']}.budget_allocation b "
                                    "ON b.root_run_id=r.id "
                                    "AND b.parent_id IS NULL WHERE r.id=:run"
                                ),
                                {"run": receipt["runId"]},
                            )
                        )
                        .mappings()
                        .one()
                    )
                state["retryCount"] = max(0, state["attempt_count"] - 1)
                state["next_attempt"] = (
                    state["attempt_count"] + 1 if state["work_status"] == "pending" else None
                )
                if not retry_history or state != retry_history[-1]:
                    retry_history.append(state)
                    persist(host, f"host-fault-{mode}-retry.json", retry_history)
                response = await asyncio.to_thread(
                    host["client"].get,
                    f"/api/sessions/{receipt['sessionId']}/runs/{receipt['runId']}/result",
                )
                if response.status_code == 200:
                    if state["status"] == "failed":
                        return response.json()
                    # The GET can see a newer commit than the preceding SQL read.
                    continue
                assert response.status_code == 409
                await asyncio.sleep(0.5)
            pytest.fail("Bounded fault task did not reach terminal; retry audit retained")
        finally:
            await engine.dispose()

    result = asyncio.run(wait_with_retry_audit())

    async def audit():
        engine = create_async_engine(host["dsn"])
        try:
            async with engine.connect() as db:
                attempts = [
                    dict(row)
                    for row in (
                        await db.execute(
                            sa.text(
                                "SELECT id,status,usage_status,usage,failure,finish_reason "
                                f"FROM {host['schema']}.model_attempt "
                                "WHERE run_id=:run ORDER BY started_at"
                            ),
                            {"run": receipt["runId"]},
                        )
                    )
                    .mappings()
                    .all()
                ]
                counts = {}
                for table in ("tool_call", "operation", "execution"):
                    selector = (
                        "run_id=:run"
                        if table != "operation"
                        else (
                            f"id IN (SELECT operation_id FROM {host['schema']}.tool_call "
                            "WHERE run_id=:run)"
                        )
                    )
                    counts[table] = (
                        await db.execute(
                            sa.text(
                                f"SELECT count(*) FROM {host['schema']}.{table} WHERE {selector}"
                            ),
                            {"run": receipt["runId"]},
                        )
                    ).scalar_one()
                return attempts, counts
        finally:
            await engine.dispose()

    attempts, counts = asyncio.run(audit())
    transcript = [json.loads(line) for line in host["proxyTranscript"].read_text().splitlines()]
    record = {
        "layer": "independent API + Worker + maintenance + real upstream TCP proxy",
        "mode": mode,
        "result": result,
        "attempts": attempts,
        "effectCounts": counts,
        "proxyTranscript": transcript,
        "proxyAliveBeforeCleanup": host["proxy"].poll() is None,
        "replacementAliveBeforeCleanup": replacement.poll() is None,
        "modelCallLimit": model_limit,
        "retryAudit": retry_history,
        "configuredMaxWorkAttempts": RecoveryConfig().maximum_attempts,
        "observedFailedModelAttempts": sum(row["status"] == "failed" for row in attempts),
        "runTaskDeadlineSeconds": run_timeout,
        "terminalGraceSeconds": terminal_grace,
        "timeoutBound": "test-only Run/Task deadline of 45s" if mode == "timeout" else None,
    }
    persist(host, f"host-fault-{mode}.json", record)
    assert any(row.get("status") == 200 for row in transcript)
    assert result["status"] == "failed" and not result["finalItems"]
    assert result["failure"]["publicMessage"] and result["failure"]["recoveryOptions"]
    assert not any(
        option["action"] == "retry_same" for option in result["failure"]["recoveryOptions"]
    )
    assert counts == {"tool_call": 0, "operation": 0, "execution": 0}
    assert len(attempts) == model_limit and all(
        row["status"] not in {"running", "pending", "completed"} for row in attempts
    )
    assert retry_history[-1]["status"] == "failed"
    assert max(row["model_attempts"] for row in retry_history) <= model_limit
    assert max(row["attempt_count"] for row in retry_history) <= RecoveryConfig().maximum_attempts
    if mode == "empty":
        assert retry_history[-1]["attempt_count"] == 2
    if mode in {"empty", "truncated"}:
        assert result["failure"]["code"] == "INCOMPLETE_MODEL_OUTPUT"
        assert result["usage"]["status"] == "complete"
    else:
        assert any(row["stage"] == "after_real_content" for row in transcript)
        assert result["partialResults"]
        assert result["usage"]["status"] == "unknown" and result["usage"]["totalTokens"] is None
        expected = "PROVIDER_TIMEOUT" if mode == "timeout" else "DEPENDENCY_UNAVAILABLE"
        assert any(row["failure"] and row["failure"]["code"] == expected for row in attempts)
    record["passed"] = True
    persist(host, f"host-fault-{mode}.json", record)


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def test_first_persistent_delta_and_completion_by_session(deployment):  # noqa: F811
    async def measure():
        engine = create_async_engine(deployment["dsn"])
        results = []
        subscriptions = []
        async with httpx.AsyncClient(
            base_url=deployment["url"],
            timeout=30,
            trust_env=False,
            headers={"Authorization": "Bearer " + deployment["token"].get_secret_value()},
        ) as client:

            async def one(cohort, index):
                response = await client.post("/api/sessions")
                response.raise_for_status()
                session = response.json()["id"]
                nonce = uuid4().hex[:10]
                ready = asyncio.Event()
                started = 0.0

                async def first_sse_delta():
                    async with client.stream(
                        "GET", f"/api/sessions/{session}/events", timeout=180
                    ) as stream:
                        stream.raise_for_status()
                        ready.set()
                        async for line in stream.aiter_lines():
                            if not line.startswith("data:"):
                                continue
                            frame = json.loads(line[5:].strip())
                            for event in frame.get("events", []):
                                if event.get("kind") == "assistant.delta":
                                    return (time.monotonic() - started, event["runId"])
                    raise AssertionError("GET SSE ended without an assistant delta")

                subscription = asyncio.create_task(first_sse_delta())
                subscriptions.append(subscription)
                await asyncio.wait_for(ready.wait(), 30)
                started = time.monotonic()
                submitted_at = datetime.now(UTC)
                response = await client.post(
                    "/api/turns",
                    json={
                        "sessionId": session,
                        "agent": deployment["agent"],
                        "idempotencyKey": str(uuid4()),
                        "inputs": [
                            {"kind": "text", "text": "Reply exactly with this identifier: " + nonce}
                        ],
                    },
                )
                response.raise_for_status()
                receipt = response.json()
                first = None
                recorded = None
                while time.monotonic() - started < 180:
                    async with engine.connect() as db:
                        row = (
                            (
                                await db.execute(
                                    sa.text(
                                        "SELECT r.status,(SELECT min(e.recorded_at) "
                                        f"FROM {deployment['schema']}.event e "
                                        "WHERE e.run_id=r.id AND e.kind='assistant.delta') "
                                        "AS first_delta "
                                        f"FROM {deployment['schema']}.run r WHERE r.id=:id"
                                    ),
                                    {"id": receipt["runId"]},
                                )
                            )
                            .mappings()
                            .one()
                        )
                    now = time.monotonic()
                    if first is None and row["first_delta"] is not None:
                        first = now - started
                        recorded = (row["first_delta"] - submitted_at).total_seconds()
                    if row["status"] in {"completed", "failed", "cancelled"}:
                        assert row["status"] == "completed", row["status"]
                        assert first is not None
                        async with engine.connect() as db:
                            text = (
                                await db.execute(
                                    sa.text(
                                        "SELECT string_agg(payload->>'text','' "
                                        "ORDER BY session_seq) "
                                        f"FROM {deployment['schema']}.event "
                                        "WHERE run_id=:id AND kind='assistant.delta'"
                                    ),
                                    {"id": receipt["runId"]},
                                )
                            ).scalar_one()
                        assert nonce in text
                        sse_first, sse_run = await asyncio.wait_for(subscription, 30)
                        assert sse_run == receipt["runId"]
                        return {
                            "cohort": cohort,
                            "index": index,
                            "sessionId": session,
                            "runId": receipt["runId"],
                            "submittedAt": submitted_at.isoformat(),
                            "firstPersistentDeltaObservedSeconds": first,
                            "firstDeltaRecordedSeconds": recorded,
                            "actualSSEFirstDeltaSeconds": sse_first,
                            "completionObservedSeconds": now - started,
                            "status": row["status"],
                            "nonce": nonce,
                            "answer": text,
                        }
                    await asyncio.sleep(0.02)
                raise AssertionError("Performance sample exceeded scenario deadline")

            try:
                for cohort in (1, 2, 4, 8):
                    samples = await asyncio.gather(*(one(cohort, i) for i in range(cohort)))
                    results.append(
                        {
                            "sessions": cohort,
                            "n": len(samples),
                            "samples": samples,
                            "durableTTFTP50": percentile(
                                [s["firstPersistentDeltaObservedSeconds"] for s in samples], 0.5
                            ),
                            "durableTTFTP95": percentile(
                                [s["firstPersistentDeltaObservedSeconds"] for s in samples], 0.95
                            ),
                            "actualSSEFirstDeltaP50": percentile(
                                [s["actualSSEFirstDeltaSeconds"] for s in samples], 0.5
                            ),
                            "actualSSEFirstDeltaP95": percentile(
                                [s["actualSSEFirstDeltaSeconds"] for s in samples], 0.95
                            ),
                            "completionP50": percentile(
                                [s["completionObservedSeconds"] for s in samples], 0.5
                            ),
                            "completionP95": percentile(
                                [s["completionObservedSeconds"] for s in samples], 0.95
                            ),
                        }
                    )
                    persist(
                        deployment,
                        "performance.json",
                        {
                            "model": os.environ["CENTN_MODEL"],
                            "workerConcurrency": 4,
                            "observation": (
                                "durable=SQL visibility polling; SSE=actual GET data receipt, "
                                "authenticated per-session GET established before POST; "
                                "neither is browser paint latency"
                            ),
                            "quantile": "nearest rank; n<=8, not an SLO estimate",
                            "cohorts": results,
                        },
                    )
            finally:
                for subscription in subscriptions:
                    if not subscription.done():
                        subscription.cancel()
                await asyncio.gather(*subscriptions, return_exceptions=True)
                await engine.dispose()

    asyncio.run(measure())


def test_sigkill_after_durable_output_keeps_prefix_and_stops_replay(deployment):  # noqa: F811
    receipt = submit(
        deployment, "Write a detailed numbered list of 300 distinct engineering checks."
    )

    async def inspect():
        engine = create_async_engine(deployment["dsn"])
        try:
            async with engine.connect() as db:
                rows = (
                    (
                        await db.execute(
                            sa.text(
                                f"SELECT session_seq,payload FROM {deployment['schema']}.event "
                                "WHERE run_id=:run AND kind='assistant.delta' ORDER BY session_seq"
                            ),
                            {"run": receipt["runId"]},
                        )
                    )
                    .mappings()
                    .all()
                )
                return [(row["session_seq"], row["payload"]) for row in rows]
        finally:
            await engine.dispose()

    deadline = time.monotonic() + 90
    before = []
    while time.monotonic() < deadline:
        before = asyncio.run(inspect())
        if before:
            break
        time.sleep(0.02)
    assert before, "Need a genuinely persisted assistant delta before SIGKILL"
    worker = deployment["worker"]
    worker.kill()
    worker.wait(10)
    assert worker.returncode == -9
    replacement = deployment["launch"]("worker")
    result = wait_result(deployment, receipt, timeout=180)
    after = asyncio.run(inspect())

    async def attempts():
        engine = create_async_engine(deployment["dsn"])
        try:
            async with engine.connect() as db:
                return [
                    dict(row)
                    for row in (
                        await db.execute(
                            sa.text(
                                "SELECT id,status,usage_status,usage "
                                f"FROM {deployment['schema']}.model_attempt "
                                "WHERE run_id=:run ORDER BY started_at"
                            ),
                            {"run": receipt["runId"]},
                        )
                    )
                    .mappings()
                    .all()
                ]
        finally:
            await engine.dispose()

    audit = asyncio.run(attempts())
    record = {
        "injection": "SIGKILL after SQL-visible assistant.delta",
        "killedPid": worker.pid,
        "killedExit": worker.returncode,
        "replacementAlive": replacement.poll() is None,
        "apiAlive": deployment["client"].get("/api/config").status_code == 200,
        "maintenanceAlive": deployment["maintenance"].poll() is None,
        "committedDeltaCountBefore": len(before),
        "deltaCountAfter": len(after),
        "prefixUnchanged": after[: len(before)] == before,
        "result": result,
        "modelAttempts": audit,
        "externalEffect": "none in this read-only scenario; effect crash windows untested",
    }
    persist(deployment, "sigkill-output.json", record)
    assert record["prefixUnchanged"]
    assert record["replacementAlive"] and record["apiAlive"] and record["maintenanceAlive"]
    assert result["status"] == "failed"
    assert result["failure"]["code"] == "USAGE_UNRESOLVED"
    assert result["failure"]["publicMessage"]
    assert any(
        option["action"] == "manual_attention" for option in result["failure"]["recoveryOptions"]
    )
    assert result["partialResults"] and not result["finalItems"]
    assert result["usage"]["status"] == "unknown"
    assert result["usage"]["reason"] == "collection_failed"
    assert result["usage"]["totalTokens"] is None
    assert len(audit) == 1, "Unknown usage must prevent automatic model replay"
    assert audit[0]["status"] == "interrupted" and audit[0]["usage_status"] == "unknown"
    assert audit[0]["usage"]["totalTokens"] is None
    record["passed"] = True
    persist(deployment, "sigkill-output.json", record)

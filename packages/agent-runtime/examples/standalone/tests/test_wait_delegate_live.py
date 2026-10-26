"""R05: SQL evidence, independent restarts, terminal children and audited budgets."""

import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from test_live import answer, deployment, port, submit, wait_result  # noqa: F401, F811

pytestmark = pytest.mark.skipif(
    os.environ.get("CENTN_LIVE") != "1", reason="Real services required"
)


def rows(host, query, **params):
    async def read():
        engine = create_async_engine(host["dsn"])
        try:
            async with engine.connect() as db:
                return [
                    dict(row)
                    for row in (
                        await db.execute(
                            sa.text(query.replace("{t}", f'"{host["schema"]}".')), params
                        )
                    ).mappings()
                ]
        finally:
            await engine.dispose()

    return asyncio.run(read())


def inject_sql(host, query, **params):
    """Explicit scenario fault injection, only in the fixture's disposable schema."""

    async def write():
        engine = create_async_engine(host["dsn"])
        try:
            async with engine.begin() as db:
                await db.execute(sa.text(query.replace("{t}", f'"{host["schema"]}".')), params)
        finally:
            await engine.dispose()

    asyncio.run(write())


def until(check, timeout=150):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(0.3)
    pytest.fail("Scenario state did not converge within its explicit bound")


def evidence(host, name, body):
    folder = Path(os.environ.get("CENTN_RELEASE_EVIDENCE_DIR", str(host["logs"])))
    folder.mkdir(parents=True, exist_ok=True)
    (folder / name).write_text(json.dumps(body, indent=2, ensure_ascii=False, default=str))


def signal(host, receipt, text):
    return host["client"].post(
        f"/api/sessions/{receipt['sessionId']}/runs/{receipt['runId']}/external-result",
        json={"text": text},
    )


@pytest.mark.parametrize("when", ["before_wait", "after_restart", "cancel"])
def test_external_completion_wait_restarts_and_duplicate_signal(deployment, when):  # noqa: F811
    host = deployment
    assert host["environment"]["CENTN_AGENT_PROFILE"] == "external_wait"
    # Keep result insertion and the first durable wait on opposite sides of a restart.
    host["maintenance"].terminate()
    host["maintenance"].wait(30)
    if when == "before_wait":
        host["worker"].terminate()
        host["worker"].wait(30)
    receipt = submit(host, "等待外部工厂数据后回答开工率，并指出90%的说法是否正确。不要用工具。")
    text = f"计划工时1000，实际800，正确开工率80%不是90%。批次 {uuid4().hex[:8]}。"
    if when == "before_wait":
        response = signal(host, receipt, text)
        assert response.status_code == 201, response.text
        host["launch"]("worker")
    waiting = until(
        lambda: rows(
            host,
            "SELECT status,state_version,waiting_spec FROM {t}run WHERE id=:run "
            "AND status='waiting_tool'",
            run=receipt["runId"],
        )
    )[0]
    assert waiting["waiting_spec"]["kind"] == "external"
    if when != "before_wait":
        host["worker"].kill()
        host["worker"].wait(10)
        response = signal(host, receipt, text)
        assert response.status_code == 201, response.text
    duplicate = signal(host, receipt, text)
    assert duplicate.status_code == 201 and duplicate.json() == response.json()
    mismatch = signal(host, receipt, text + "different")
    assert mismatch.status_code == 409, mismatch.text
    wrong = {**receipt, "sessionId": str(uuid4())}
    assert signal(host, wrong, text).status_code == 409
    url = (
        f"{host['url'].rstrip('/')}/api/sessions/"
        f"{receipt['sessionId']}/runs/{receipt['runId']}/external-result"
    )
    assert httpx.post(url, json={}, trust_env=False).status_code == 401
    if when == "cancel":
        control = host["client"].post(
            f"/api/sessions/{receipt['sessionId']}/controls",
            json={
                "runId": receipt["runId"],
                "kind": "cancel",
                "idempotencyKey": str(uuid4()),
                "expectedVersion": str(waiting["state_version"]),
            },
        )
        assert control.status_code in {200, 202}, control.text
    host["launch"]("worker") if when != "before_wait" else None
    host["launch"]("maintenance")
    result = wait_result(host, receipt, timeout=180)
    assert result["status"] == ("cancelled" if when == "cancel" else "completed"), result
    if when != "cancel":
        assert "80" in answer(result) and "90" in answer(result), answer(result)
    time.sleep(2)
    runs = rows(
        host, "SELECT status,wake_generation FROM {t}run WHERE id=:run", run=receipt["runId"]
    )
    assert runs[0]["wake_generation"] == (0 if when == "cancel" else 1), runs
    assert not rows(
        host,
        "SELECT id FROM {t}work_item WHERE run_id=:run AND status IN ('pending','leased')",
        run=receipt["runId"],
    )
    assert signal(host, receipt, text).status_code == 409
    evidence(host, f"external-{when}.json", {"runs": runs, "result": result})


@pytest.mark.parametrize("outcome", ["failed", "cancelled", "cancelled_running"])
def test_delegate_terminal_failure_cancel_and_budget(deployment, outcome, tmp_path):  # noqa: F811
    host = deployment
    assert host["environment"]["CENTN_AGENT_PROFILE"] == "delegate"
    host["child"].terminate()
    host["child"].wait(30)
    receipt = submit(host, "独立复核计划1000小时实际800小时，开工率应该是80%而非90%。")
    child = until(
        lambda: rows(
            host, "SELECT id,status FROM {t}run WHERE parent_run_id=:run", run=receipt["runId"]
        )
    )[0]
    proxy = None
    proxy_log = None
    try:
        if outcome == "failed":
            host["worker"].terminate()
            host["worker"].wait(30)
        if outcome in {"failed", "cancelled_running"}:
            listen = port()
            transcript = tmp_path / "child-proxy.jsonl"
            proxy_log = (tmp_path / "child-proxy.log").open("wb")
            proxy = subprocess.Popen(
                [
                    sys.executable,
                    str(Path(__file__).with_name("compression_faults.py")),
                    "--port",
                    str(listen),
                    "--upstream",
                    host["environment"]["CENTN_PROVIDER_URL"],
                    "--mode",
                    "empty" if outcome == "failed" else "timeout",
                    "--evidence",
                    str(transcript),
                ],
                stdout=proxy_log,
                stderr=subprocess.STDOUT,
            )

            def healthy():
                try:
                    return (
                        httpx.get(f"http://127.0.0.1:{listen}/", trust_env=False).status_code == 200
                    )
                except httpx.TransportError:
                    return False

            until(healthy, 15)
            host["launch"]("child", {"CENTN_PROVIDER_URL": f"http://127.0.0.1:{listen}/v1"})
        if outcome != "failed":
            if outcome == "cancelled_running":
                until(
                    lambda: rows(
                        host,
                        "SELECT id FROM {t}model_attempt WHERE run_id=:run AND status='running'",
                        run=child["id"],
                    )
                )
            response = host["client"].post(
                f"/api/sessions/{receipt['sessionId']}/controls",
                json={
                    "runId": receipt["runId"],
                    "kind": "cancel",
                    "idempotencyKey": str(uuid4()),
                    "expectedVersion": str(
                        rows(
                            host,
                            "SELECT state_version FROM {t}run WHERE id=:run",
                            run=receipt["runId"],
                        )[0]["state_version"]
                    ),
                },
            )
            assert response.status_code in {200, 202}, response.text
            if outcome == "cancelled":
                host["launch"]("child")
        if outcome == "failed":
            until(
                lambda: rows(
                    host, "SELECT id FROM {t}run WHERE id=:run AND status='failed'", run=child["id"]
                )
            )
            host["launch"]("worker")
        result = wait_result(host, receipt, timeout=240)
        terminal = until(
            lambda: rows(
                host,
                "SELECT id,status,failure FROM {t}run WHERE id=:run "
                "AND status IN ('failed','cancelled')",
                run=child["id"],
            )
        )[0]
        assert terminal["status"] == ("failed" if outcome == "failed" else "cancelled"), terminal
        if outcome != "failed":
            assert result["status"] == "cancelled", result
        else:
            notices = rows(
                host,
                "SELECT kind,status,payload FROM {t}agent_message "
                "WHERE sender_run_id=:child AND recipient_run_id=:run AND kind='task.failed'",
                child=child["id"],
                run=receipt["runId"],
            )
            assert len(notices) == 1 and notices[0]["status"] == "applied", notices
            assert result["status"] == "completed" and result["outcome"] == "partial", result
            assert "80" in answer(result), result

        def settled():
            return rows(
                host,
                "SELECT operation_key,quantities FROM {t}budget_entry "
                "WHERE operation_key=:key AND kind='settle'",
                key=f"delegate-settle:{child['id']}",
            )

        if outcome == "cancelled_running":
            # A cancelled network inference has no trustworthy final usage receipt.
            # Conservatively retain its reservation instead of inventing a refund.
            until(
                lambda: rows(
                    host,
                    "SELECT id FROM {t}budget_allocation "
                    "WHERE root_run_id=:run AND parent_id IS NOT NULL AND usage_status='unknown'",
                    run=receipt["runId"],
                ),
                30,
            )
            settlement = settled()
            assert not settlement
            assert result["usage"]["status"] == "unknown", result["usage"]
        else:
            settlement = until(settled, 60)
            assert len(settlement) == 1
            if outcome == "failed":
                audit = rows(
                    host,
                    "SELECT count(*) AS calls,"
                    "sum(COALESCE((usage->>'totalTokens')::bigint,0)) AS tokens "
                    "FROM {t}model_attempt WHERE run_id=:run",
                    run=child["id"],
                )[0]
                assert audit["tokens"] > 0
                assert settlement[0]["quantities"]["model_calls"] == audit["calls"]
                assert settlement[0]["quantities"]["tokens"] == audit["tokens"]
        budgets = rows(
            host,
            "SELECT limits,reserved,unknown_reserved,settled,usage_status,parent_id "
            "FROM {t}budget_allocation WHERE root_run_id=:run",
            run=receipt["runId"],
        )
        assert len(budgets) == 2
        if outcome != "cancelled_running":
            assert all(
                not any(b["reserved"].values()) and not any(b["unknown_reserved"].values())
                for b in budgets
            ), budgets
        else:
            parent = next(b for b in budgets if b["parent_id"] is None)
            child_budget = next(b for b in budgets if b["parent_id"] is not None)
            assert parent["reserved"] == child_budget["limits"], budgets
            assert any(child_budget["unknown_reserved"].values()), budgets
        assert (
            len(rows(host, "SELECT id FROM {t}run WHERE parent_run_id=:run", run=receipt["runId"]))
            == 1
        )
        assert not rows(
            host,
            "SELECT id FROM {t}work_item WHERE run_id IN "
            "(SELECT id FROM {t}run WHERE id=:run OR parent_run_id=:run) "
            "AND status IN ('pending','leased')",
            run=receipt["runId"],
        )
        host["maintenance"].terminate()
        host["maintenance"].wait(30)
        host["launch"]("maintenance")
        time.sleep(3)
        assert settled() == settlement
        evidence(
            host,
            f"delegate-{outcome}.json",
            {
                "result": result,
                "child": terminal,
                "budgets": budgets,
                "settlement": settlement,
            },
        )
    finally:
        if proxy is not None:
            proxy.terminate()
            proxy.wait(10)
        if proxy_log is not None:
            proxy_log.close()


@pytest.mark.parametrize("fault", ["revoked", "session_revoked", "expired"])
def test_external_wait_without_result_is_converged(deployment, fault):  # noqa: F811
    host = deployment
    assert host["environment"]["CENTN_AGENT_PROFILE"] == "external_wait"
    receipt = submit(host, "等待外部生产数据后再分析，不要自行编造数据。")
    until(
        lambda: rows(
            host,
            "SELECT id FROM {t}run WHERE id=:run AND status='waiting_tool'",
            run=receipt["runId"],
        )
    )
    if fault == "revoked":
        inject_sql(
            host,
            "UPDATE {t}capability_grant SET revoked_at=now() WHERE run_id=:run",
            run=receipt["runId"],
        )
    elif fault == "session_revoked":
        inject_sql(
            host,
            "UPDATE {t}session SET deleted_at=now() WHERE id=:session",
            session=receipt["sessionId"],
        )
    else:
        # Deliberately inject an elapsed deadline; no completion fact is submitted.
        inject_sql(
            host,
            "UPDATE {t}run SET deadline_at=now()-interval '1 second' WHERE id=:run",
            run=receipt["runId"],
        )
    if fault == "session_revoked":
        state = until(
            lambda: rows(
                host,
                "SELECT status,failure FROM {t}run WHERE id=:run AND status='failed'",
                run=receipt["runId"],
            ),
            90,
        )[0]
        forbidden = host["client"].get(f"/api/runtime/sessions/{receipt['sessionId']}/snapshot")
        assert forbidden.status_code == 409 and forbidden.json()["code"] == "FORBIDDEN"
        result = state
    else:
        result = wait_result(host, receipt, timeout=90)
    assert result["status"] == "failed", result
    assert result["failure"]["code"] == ("DELIVERY_EXPIRED" if fault == "expired" else "FORBIDDEN")
    assert result["failure"]["recoveryOptions"], result
    assert not rows(
        host,
        "SELECT id FROM {t}work_item WHERE run_id=:run AND status IN ('pending','leased')",
        run=receipt["runId"],
    )
    assert not rows(
        host, "SELECT session_id FROM {t}session_slot WHERE owner_run_id=:run", run=receipt["runId"]
    )
    evidence(
        host, f"external-{fault}.json", {"result": result, "deadlineInjected": fault == "expired"}
    )


@pytest.mark.parametrize("phase", ["queued", "running"])
def test_delegate_pause_safe_point_and_resume(deployment, phase):  # noqa: F811
    host = deployment
    assert host["environment"]["CENTN_AGENT_PROFILE"] == "delegate"
    host["child"].terminate()
    host["child"].wait(30)
    receipt = submit(host, "独立复核1000小时计划、800小时实际，解释为何开工率80%不是90%。")
    child = until(
        lambda: rows(
            host, "SELECT id,status FROM {t}run WHERE parent_run_id=:run", run=receipt["runId"]
        )
    )[0]
    deadlines = rows(
        host,
        "SELECT c.deadline_at AS run_deadline,t.deadline_at AS task_deadline,"
        "p.deadline_at AS parent_deadline,m.created_at AS assigned_at,"
        "m.deadline_at AS assignment_deadline,target.manifest->'budget' AS target_budget "
        "FROM {t}run c JOIN {t}run p ON p.scope_id=c.scope_id AND p.id=c.parent_run_id "
        "JOIN {t}task t ON t.scope_id=c.scope_id AND t.id=c.task_id "
        "JOIN {t}agent_revision target ON target.scope_id=c.scope_id "
        "AND target.id=c.agent_revision_id "
        "JOIN {t}agent_message m ON m.scope_id=c.scope_id AND m.recipient_run_id=c.id "
        "AND m.sender_run_id=p.id AND m.task_id=c.task_id AND m.kind='task.assign' "
        "WHERE c.id=:run",
        run=child["id"],
    )
    assert len(deadlines) == 1, deadlines
    deadlines = deadlines[0]
    runtime_seconds = deadlines["target_budget"]["maxRuntimeSeconds"]
    assert type(runtime_seconds) is int and runtime_seconds > 0, deadlines
    latest_deadline = min(
        deadlines["assigned_at"] + timedelta(seconds=runtime_seconds),
        deadlines["parent_deadline"],
    )
    work_deadlines = rows(
        host,
        "SELECT id,deadline_at FROM {t}work_item WHERE run_id=:run AND kind='advance_run'",
        run=child["id"],
    )
    assert len(work_deadlines) == 1, work_deadlines
    for deadline in (
        deadlines["run_deadline"],
        deadlines["task_deadline"],
        deadlines["assignment_deadline"],
        *(work["deadline_at"] for work in work_deadlines),
    ):
        assert deadlines["assigned_at"] < deadline <= latest_deadline, deadlines
        assert deadline == deadlines["run_deadline"], (deadlines, work_deadlines)
    if phase == "running":
        host["launch"]("child")
        until(
            lambda: rows(
                host,
                "SELECT id FROM {t}model_attempt WHERE run_id=:run AND status='running'",
                run=child["id"],
            )
        )

    def control(kind):
        current = rows(
            host, "SELECT state_version FROM {t}run WHERE id=:run", run=receipt["runId"]
        )[0]
        response = host["client"].post(
            f"/api/sessions/{receipt['sessionId']}/controls",
            json={
                "runId": receipt["runId"],
                "kind": kind,
                "idempotencyKey": str(uuid4()),
                "expectedVersion": str(current["state_version"]),
            },
        )
        assert response.status_code == 200, response.text

    control("pause")
    remaining = rows(
        host,
        "SELECT deadline_at,GREATEST(0,EXTRACT(EPOCH FROM "
        "(deadline_at-clock_timestamp()))) AS remaining_seconds FROM {t}run WHERE id=:run",
        run=child["id"],
    )[0]
    assert remaining["deadline_at"] == deadlines["run_deadline"], remaining
    until(
        lambda: rows(
            host, "SELECT id FROM {t}run WHERE id=:run AND status='paused'", run=receipt["runId"]
        ),
        float(remaining["remaining_seconds"]) + 60,
    )
    if phase == "queued":
        host["launch"]("child")
    before = rows(host, "SELECT id,status FROM {t}model_attempt WHERE run_id=:run", run=child["id"])
    assert all(row["status"] not in {"pending", "running"} for row in before)
    time.sleep(3)
    assert (
        rows(host, "SELECT id,status FROM {t}model_attempt WHERE run_id=:run", run=child["id"])
        == before
    )
    if phase == "queued":
        assert not before
        state = rows(host, "SELECT status FROM {t}run WHERE id=:run", run=child["id"])[0]
        assert state["status"] == "queued"
    control("resume")
    # Includes the child's 300-second budget and the parent's sealed-result review.
    # This is a convergence bound, not an asserted latency SLA.
    result = wait_result(host, receipt, timeout=420)
    assert result["status"] == "completed" and "80" in answer(result), result
    assert (
        len(rows(host, "SELECT id FROM {t}run WHERE parent_run_id=:run", run=receipt["runId"])) == 1
    )
    evidence(
        host,
        f"delegate-pause-{phase}.json",
        {
            "result": result,
            "attemptsAtPaused": before,
            "childDeadlines": deadlines,
            "childWorkDeadlines": work_deadlines,
        },
    )

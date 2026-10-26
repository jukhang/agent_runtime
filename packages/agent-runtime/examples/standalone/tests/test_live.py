"""Opt-in real SQL/Provider/API/independent Worker acceptance, never mock answers."""

import json
import os
import signal
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import sqlalchemy as sa
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import create_async_engine

from agent_runtime.storage.migrate import execute

pytestmark = pytest.mark.skipif(
    os.environ.get("CENTN_LIVE") != "1", reason="Real services required"
)
ROOT = Path(__file__).resolve().parents[1]


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def deployment(tmp_path):
    import asyncio

    from centn_example.config import Settings
    from centn_example.resource_migrate import resource_schema
    from centn_example.resource_migrate import upgrade as upgrade_resources

    settings = Settings.from_env()
    assert sa.make_url(settings.dsn).database == "aletai_wangxh"
    schema = "p05_live_" + uuid4().hex[:12]
    environment = {**os.environ, "CENTN_SCHEMA": schema}
    cleanup_record = tmp_path / "cleanup.json"
    cleanup_record.write_text(
        json.dumps({"database": "aletai_wangxh", "schema": schema, "status": "pending"})
    )
    listen = port()
    logs = []
    children = []
    client = None

    def launch(role, overrides=None):
        log = (tmp_path / f"{role}-{len(children)}.log").open("wb")
        logs.append(log)
        child = subprocess.Popen(
            [sys.executable, "-m", "centn_example", role, "--port", str(listen)],
            cwd=ROOT,
            env={**environment, **(overrides or {})},
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        children.append(child)
        return child

    try:
        asyncio.run(execute("upgrade", settings.dsn, schema, "base", 60))
        asyncio.run(upgrade_resources(settings.dsn, schema))
        api = launch("api")
        worker = launch("worker")
        maintenance = launch("maintenance")
        child_worker = launch("child") if settings.agent_profile == "delegate" else None
        client = httpx.Client(
            base_url=f"http://127.0.0.1:{listen}",
            trust_env=False,
            timeout=30,
            headers={"Authorization": f"Bearer {settings.token}"},
        )
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            assert api.poll() is None and worker.poll() is None and maintenance.poll() is None, (
                "Read isolated host logs"
            )
            try:
                response = client.get("/api/config")
                if response.status_code == 200:
                    break
            except httpx.TransportError:
                pass
            time.sleep(0.5)
        else:
            pytest.fail("Example host did not become ready")
        yield {
            "client": client,
            "agent": response.json()["agent"],
            "url": str(client.base_url),
            "token": SecretStr(settings.token),
            "worker": worker,
            "maintenance": maintenance,
            "child": child_worker,
            "launch": launch,
            "environment": environment,
            "logs": tmp_path,
            "schema": schema,
            "dsn": settings.dsn,
        }
    finally:
        if client is not None:
            client.close()
        for child in children:
            if child.poll() is None:
                child.send_signal(signal.SIGTERM)
                try:
                    child.wait(30)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(10)
        for log in logs:
            log.close()

        async def cleanup():
            engine = create_async_engine(settings.dsn)
            try:
                async with engine.begin() as db:
                    await db.execute(
                        sa.schema.DropSchema(resource_schema(schema), cascade=True, if_exists=True)
                    )
                    await db.execute(sa.schema.DropSchema(schema, cascade=True, if_exists=True))
            finally:
                await engine.dispose()

        for attempt in range(3):
            try:
                asyncio.run(cleanup())
                cleanup_record.write_text(
                    json.dumps({"database": "aletai_wangxh", "schema": schema, "status": "cleaned"})
                )
                break
            except (OSError, sa.exc.OperationalError, TimeoutError) as exc:
                cleanup_record.write_text(
                    json.dumps(
                        {
                            "database": "aletai_wangxh",
                            "schema": schema,
                            "status": "pending",
                            "errorType": type(exc).__name__,
                        }
                    )
                )
                if attempt == 2:
                    raise
                time.sleep(2**attempt)


def submit(deployment, text, session=None):
    client = deployment["client"]
    if session is None:
        response = client.post("/api/sessions")
        response.raise_for_status()
        session = response.json()["id"]
    response = client.post(
        "/api/turns",
        json={
            "sessionId": session,
            "agent": deployment["agent"],
            "idempotencyKey": str(uuid4()),
            "inputs": [{"kind": "text", "text": text}],
        },
    )
    response.raise_for_status()
    return response.json()


def wait_result(deployment, receipt, timeout=180):
    deadline = time.monotonic() + timeout
    inspect_at = time.monotonic() + 15
    stop_reason = "Real task did not finish within the scenario bound"
    while time.monotonic() < deadline:
        response = deployment["client"].get(
            f"/api/sessions/{receipt['sessionId']}/runs/{receipt['runId']}/result"
        )
        if response.status_code == 503 and response.json().get("code") == "DEPENDENCY_UNAVAILABLE":
            time.sleep(1)
            continue
        if response.status_code == 200:
            result = response.json()
            (deployment["logs"] / f"{receipt['runId']}-result.json").write_text(
                json.dumps(result, ensure_ascii=False, indent=2)
            )
            return result
        assert response.status_code == 409 and response.json()["code"] == "WAIT_TIMEOUT", (
            response.text
        )
        if time.monotonic() >= inspect_at:
            status = deployment["client"].get(
                f"/api/runtime/sessions/{receipt['sessionId']}/snapshot"
            )
            if status.status_code == 503 and status.json().get("code") == "DEPENDENCY_UNAVAILABLE":
                time.sleep(1)
                continue
            status.raise_for_status()
            if any(row["runId"] == receipt["runId"] for row in status.json()["inputRequests"]):
                stop_reason = "Task unexpectedly requested human input"
                break
            inspect_at = time.monotonic() + 15
    snapshot = deployment["client"].get(f"/api/runtime/sessions/{receipt['sessionId']}/snapshot")
    diagnostic = (
        snapshot.json() if snapshot.status_code == 200 else {"httpStatus": snapshot.status_code}
    )
    (deployment["logs"] / f"{receipt['runId']}-blocked-snapshot.json").write_text(
        json.dumps(diagnostic, ensure_ascii=False, indent=2)
    )
    import asyncio

    async def sql_diagnostic():
        engine = create_async_engine(deployment["dsn"])
        data = {}
        try:
            async with engine.connect() as db:
                for table, columns in (
                    ("run", "id,status,state_version,phase,owner_id,owner_epoch,lease_until"),
                    ("work_item", "id,kind,status,attempt_count,owner_id,lease_epoch,lease_until"),
                    ("model_attempt", "id,status,finish_reason,started_at,finished_at,failure"),
                ):
                    # Identifiers are fixed here or fixture-generated, never request input.
                    rows = await db.execute(
                        sa.text(f'SELECT {columns} FROM "{deployment["schema"]}".{table}')
                    )
                    data[table] = [dict(row) for row in rows.mappings()]
            return data
        finally:
            await engine.dispose()

    try:
        sql_state = asyncio.run(sql_diagnostic())
    except Exception as exc:
        sql_state = {"diagnosticErrorType": type(exc).__name__}
    (deployment["logs"] / f"{receipt['runId']}-blocked-sql.json").write_text(
        json.dumps(sql_state, default=str, ensure_ascii=False, indent=2)
    )
    pytest.fail(
        stop_reason
        + "; "
        + json.dumps(
            {"runs": diagnostic.get("runs"), "inputRequests": diagnostic.get("inputRequests")},
            ensure_ascii=False,
        )
    )


def answer(result):
    return "\n".join(
        c["text"]
        for item in result["finalItems"]
        if item["role"] == "assistant"
        for c in item["channels"]
        if c["name"] == "content"
    )


def test_real_resource_preview_download_acl_and_revocation(deployment):
    import asyncio
    import hashlib

    from centn_example.config import Settings
    from centn_example.resources import Resources

    from agent_runtime.contracts.failure import AgentRuntimeError
    from agent_runtime.contracts.references import ResourceRef
    from agent_runtime.contracts.transactions import ActorContext

    client = deployment["client"]
    session = client.post("/api/sessions").json()["id"]
    text = "车间资料：计划工时1000，实际工时800，开工率80%。"
    body = {
        "sessionId": session,
        "idempotencyKey": str(uuid4()),
        "title": "车间资料.md",
        "text": text,
    }
    created = client.post("/api/resources", json=body)
    assert created.status_code == 201, created.text
    value = created.json()
    assert client.post("/api/resources", json=body).json() == value
    changed = client.post("/api/resources", json={**body, "text": "changed"})
    assert changed.status_code == 409 and changed.json()["code"] == "VERSION_CONFLICT"
    ref = value["resourceRef"]
    preview = client.post("/api/resources/preview", json={"resourceRef": ref})
    assert preview.status_code == 200 and preview.json()["text"] == text
    download = client.post("/api/resources/download", json={"resourceRef": ref})
    assert download.status_code == 200 and download.content == text.encode()
    assert hashlib.sha256(download.content).hexdigest() == value["digest"]
    assert download.headers["x-content-type-options"] == "nosniff"
    assert download.headers["cache-control"] == "no-store"
    assert download.headers["content-disposition"].startswith("attachment;")
    assert (
        client.post(
            "/api/resources/preview", json={"resourceRef": {**ref, "resourceVersion": "2"}}
        ).status_code
        == 409
    )
    other_session = client.post("/api/sessions").json()["id"]
    denied = client.post(
        "/api/turns",
        json={
            "sessionId": other_session,
            "agent": deployment["agent"],
            "idempotencyKey": str(uuid4()),
            "inputs": [{"kind": "resource", "resourceRef": ref}],
        },
    )
    assert denied.status_code == 409 and denied.json()["code"] == "FORBIDDEN"

    async def wrong_actor():
        settings = Settings.from_env()
        engine = create_async_engine(settings.dsn)
        try:
            store = Resources(engine, deployment["schema"])
            actor = ActorContext(
                scope_id=settings.scope,
                actor_id="other-user",
                auth_revision="1",
                grants_ref="other",
            )
            with pytest.raises(AgentRuntimeError) as error:
                await store.get(actor, ResourceRef.model_validate(ref))
            assert error.value.failure.code == "FORBIDDEN"
        finally:
            await engine.dispose()

    asyncio.run(wrong_actor())
    assert client.post("/api/resources/revoke", json={"resourceRef": ref}).status_code == 204
    assert client.get(f"/api/sessions/{session}/resources").json() == []
    for route in ("preview", "download"):
        response = client.post(f"/api/resources/{route}", json={"resourceRef": ref})
        assert response.status_code == 409 and response.json()["code"] == "FORBIDDEN"


def test_real_question_uses_uploaded_attachment_and_knowledge_source(deployment):
    client = deployment["client"]
    session = client.post("/api/sessions").json()["id"]
    refs = []
    for kind, title, text in (
        (
            "attachment",
            "模具车间工时.txt",
            "2026年第三季度，模具车间计划工时1000小时，实际开工工时800小时。",
        ),
        (
            "knowledge",
            "开工率口径.txt",
            "开工率=实际开工工时÷计划工时×100%。只按给定工时计算，不是订单履约率。",
        ),
    ):
        response = client.post(
            "/api/resources",
            json={
                "sessionId": session,
                "idempotencyKey": str(uuid4()),
                "kind": kind,
                "title": title,
                "text": text,
            },
        )
        assert response.status_code == 201, response.text
        refs.append(response.json()["resourceRef"])
    response = client.post(
        "/api/turns",
        json={
            "sessionId": session,
            "agent": deployment["agent"],
            "idempotencyKey": str(uuid4()),
            "inputs": [
                {
                    "kind": "text",
                    "text": "请依据上传的两份资料回答模具车间2026年第三季度的开工率，"
                    "给出计算过程，并引用资料名称。不要编造订单、收入或其他数据，三句话内回答。",
                },
                *({"kind": "resource", "resourceRef": ref} for ref in refs),
            ],
        },
    )
    assert response.status_code == 202, response.text
    result = wait_result(deployment, response.json(), timeout=240)
    assert result["status"] == "completed", result.get("failure")
    text = answer(result)
    assert "80" in text and "800" in text and "1000" in text, text
    assert "模具车间工时" in text and "开工率口径" in text, text


@pytest.mark.parametrize("decision", ["approved", "rejected", "revoked"])
def test_real_report_approval_survives_worker_restart(deployment, decision):
    client = deployment["client"]
    text = "请调用 save-report 工具保存一份报告，标题为开工核对报告.txt，" + (
        "正文严格为：计划工时1000小时，实际工时800小时，开工率80%。"
        "请先申请保存审批，不要只在聊天中输出正文。"
    )
    source = None
    if decision == "revoked":
        session = client.post("/api/sessions").json()["id"]
        source = client.post(
            "/api/resources",
            json={
                "sessionId": session,
                "idempotencyKey": str(uuid4()),
                "title": "工时事实.txt",
                "text": text,
            },
        ).json()
        submitted = client.post(
            "/api/turns",
            json={
                "sessionId": session,
                "agent": deployment["agent"],
                "idempotencyKey": str(uuid4()),
                "inputs": [
                    {"kind": "text", "text": text},
                    {"kind": "resource", "resourceRef": source["resourceRef"]},
                ],
            },
        )
        assert submitted.status_code == 202, submitted.text
        receipt = submitted.json()
    else:
        receipt = submit(deployment, text)
    session = receipt["sessionId"]
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        rows = client.get(f"/api/sessions/{session}/approvals").json()
        snapshot = client.get(f"/api/runtime/sessions/{session}/snapshot").json()
        if rows and snapshot["runs"][-1]["status"] == "waiting_user":
            break
        assert snapshot["runs"][-1]["status"] not in {"completed", "failed", "cancelled"}, snapshot[
            "runs"
        ][-1]
        time.sleep(0.5)
    else:
        pytest.fail("Real model did not request report approval")
    assert len(rows) == 1 and rows[0]["status"] == "pending"
    assert len(client.get(f"/api/sessions/{session}/resources").json()) == int(source is not None)
    deployment["worker"].terminate()
    deployment["worker"].wait(30)
    deployment["maintenance"].terminate()
    deployment["maintenance"].wait(30)
    row = rows[0]
    if decision == "revoked":
        response = client.post("/api/resources/revoke", json={"resourceRef": source["resourceRef"]})
        assert response.status_code == 204, response.text
        response = client.post(
            f"/api/sessions/{session}/approvals",
            json={
                "approvalId": row["id"],
                "expectedVersion": row["revision"],
                "decision": "approved",
                "idempotencyKey": f"approval:{row['id']}:{row['revision']}:approved",
            },
        )
        assert response.status_code == 409 and response.json()["code"] == "FORBIDDEN", response.text
        current = client.get(f"/api/runtime/sessions/{session}/snapshot").json()["runs"][-1]
        cancelled = client.post(
            f"/api/sessions/{session}/controls",
            json={
                "runId": receipt["runId"],
                "kind": "cancel",
                "idempotencyKey": str(uuid4()),
                "expectedVersion": current["stateVersion"],
            },
        )
        assert cancelled.status_code == 200, cancelled.text
        deployment["launch"]("maintenance")
        deployment["launch"]("worker")
        result = wait_result(deployment, receipt, timeout=120)
        assert result["status"] == "cancelled", result
        assert client.get(f"/api/sessions/{session}/resources").json() == []
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            approvals = client.get(f"/api/runtime/sessions/{session}/snapshot").json()["approvals"]
            if approvals and approvals[0].get("status") == "cancelled":
                break
            time.sleep(0.5)
        assert len(approvals) == 1 and approvals[0]["status"] == "cancelled", approvals
        assert approvals[0]["allowedDecisions"] == []
        return
    payload = {
        "approvalId": row["id"],
        "expectedVersion": row["revision"],
        "decision": decision,
        "idempotencyKey": f"approval:{row['id']}:{row['revision']}:{decision}",
    }
    if decision == "approved":
        from playwright.sync_api import sync_playwright

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True, channel="chrome")
            try:
                page = browser.new_page(viewport={"width": 1280, "height": 900})
                page.goto(deployment["url"].rstrip("/") + "/?session=" + session)
                page.locator("#token").fill(deployment["token"].get_secret_value())
                page.locator("#credentials button").click()
                page.locator("#approvals").get_by_role("button", name="批准").click(timeout=30000)
                page.wait_for_function(
                    "document.querySelector('#approvals').textContent.includes('approved')"
                )
                page.reload()
                # The example intentionally keeps bearer credentials in memory only.
                page.locator("#token").fill(deployment["token"].get_secret_value())
                page.locator("#credentials button").click()
                page.wait_for_function(
                    "document.querySelector('#approvals').textContent.includes('approved')"
                )
                assert page.locator("#approvals").get_by_role("button", name="批准").count() == 0
                assert page.locator("#error").inner_text() == ""
                directory = Path(os.environ.get("CENTN_SCREENSHOT_DIR", str(deployment["logs"])))
                directory.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(directory / "p05-approval.png"), full_page=True)
            finally:
                browser.close()
    response = client.post(f"/api/sessions/{session}/approvals", json=payload)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == decision
    replay = client.post(f"/api/sessions/{session}/approvals", json=payload)
    assert replay.status_code == 200 and replay.json() == response.json(), replay.text
    approval_views = client.get(f"/api/runtime/sessions/{session}/snapshot").json()["approvals"]
    assert len(approval_views) == 1 and approval_views[0]["status"] == decision, approval_views
    assert approval_views[0]["revision"] == "1" and approval_views[0]["allowedDecisions"] == []
    assert client.get(f"/api/sessions/{session}/resources").json() == []
    deployment["launch"]("maintenance")
    deployment["launch"]("worker")
    result = wait_result(deployment, receipt, timeout=240)
    assert result["status"] == "completed", result.get("failure")
    reports = client.get(f"/api/sessions/{session}/resources").json()
    assert len(reports) == (1 if decision == "approved" else 0), reports
    if reports:
        preview = client.post(
            "/api/resources/preview", json={"resourceRef": reports[0]["resourceRef"]}
        )
        assert "80%" in preview.json()["text"]
        # Repeated decisions after completion cannot create another operation or report.
        assert client.post(f"/api/sessions/{session}/approvals", json=payload).status_code == 200
        assert len(client.get(f"/api/sessions/{session}/resources").json()) == 1
    other = client.post("/api/sessions").json()["id"]
    assert client.post(f"/api/sessions/{other}/approvals", json=payload).status_code == 409
    changed = {
        **payload,
        "decision": "cancelled",
        "idempotencyKey": f"approval:{row['id']}:{row['revision']}:cancelled",
    }
    assert client.post(f"/api/sessions/{session}/approvals", json=changed).status_code == 409


@pytest.mark.skipif(
    os.environ.get("CENTN_AGENT_PROFILE") != "delegate", reason="Delegate profile required"
)
def test_real_delegate_wait_collect_and_budget_settlement(deployment):
    import asyncio

    receipt = submit(
        deployment,
        "独立复核以下数据，再给出简短结论：模具厂计划工时1000小时、实际800小时。"
        "有人声称开工率为90%，这是否正确？给出正确百分比，不要编造其他资料。",
    )
    result = wait_result(deployment, receipt, timeout=300)
    assert result["status"] == "completed", result.get("failure")
    assert "80" in answer(result) and "90" in answer(result), answer(result)
    snapshot = (
        deployment["client"].get(f"/api/runtime/sessions/{receipt['sessionId']}/snapshot").json()
    )
    children = [run for run in snapshot["runs"] if run.get("parentRunId") == receipt["runId"]]
    assert len(children) == 1 and children[0]["status"] == "completed", snapshot["runs"]

    async def verify():
        engine = create_async_engine(deployment["dsn"])
        try:
            async with engine.connect() as db:
                from agent_runtime.storage.uow.schema import scoped

                params = {"scope": os.environ["CENTN_SCOPE"], "run": receipt["runId"]}
                rows = (
                    (
                        await db.execute(
                            scoped(
                                "SELECT limits,reserved,unknown_reserved,parent_id "
                                "FROM {t}budget_allocation "
                                "WHERE scope_id=:scope AND root_run_id=CAST(:run AS uuid)",
                                deployment["schema"],
                            ),
                            params,
                        )
                    )
                    .mappings()
                    .all()
                )
                assert len(rows) == 2
                child = next(row for row in rows if row["parent_id"] is not None)
                assert child["limits"]["tool_calls"] == 0
                assert all(
                    not any(row["reserved"].values()) and not any(row["unknown_reserved"].values())
                    for row in rows
                )
                assert (
                    await db.execute(
                        scoped(
                            "SELECT count(*) FROM {t}agent_message WHERE scope_id=:scope "
                            "AND sender_run_id=CAST(:run AS uuid) AND kind='result.accepted'",
                            deployment["schema"],
                        ),
                        params,
                    )
                ).scalar_one() == 1
        finally:
            await engine.dispose()

    asyncio.run(verify())


def test_real_qa_tool_followup_and_replay(deployment):
    first = submit(deployment, "请使用乘法工具计算23乘17，告诉我准确结果。")
    result = wait_result(deployment, first)
    assert result["status"] == "completed", result.get("failure")
    assert "391" in answer(result), answer(result)
    snapshot = deployment["client"].get(f"/api/runtime/sessions/{first['sessionId']}/snapshot")
    snapshot.raise_for_status()
    assert any("391" in c["text"] for item in snapshot.json()["items"] for c in item["channels"])
    second = submit(deployment, "将刚才的结果再乘以19，同样使用工具核对。", first["sessionId"])
    second_result = wait_result(deployment, second)
    assert second_result["status"] == "completed", second_result.get("failure")
    assert "7429" in answer(second_result).replace(",", ""), answer(second_result)
    third = submit(
        deployment, "刚才两个计算结果之间是什么关系？直接解释，不要重新计算。", first["sessionId"]
    )
    third_result = wait_result(deployment, third)
    assert third_result["status"] == "completed" and "19" in answer(third_result), third_result


@pytest.mark.skipif(os.environ.get("CENTN_THINKING") != "true", reason="Thinking profile required")
def test_real_thinking_channels_are_persisted_separately(deployment):
    receipt = submit(deployment, "17是不是质数？用一句话给出答案，不需要工具。")
    result = wait_result(deployment, receipt)
    assert result["status"] == "completed", result.get("failure")
    assert "17" in answer(result) and "质数" in answer(result), answer(result)
    snapshot = deployment["client"].get(f"/api/runtime/sessions/{receipt['sessionId']}/snapshot")
    snapshot.raise_for_status()
    channels = [
        channel
        for item in snapshot.json()["items"]
        if item["role"] == "assistant"
        for channel in item["channels"]
    ]
    assert any(channel["name"] == "analysis" and channel["text"].strip() for channel in channels)
    assert any(channel["name"] == "content" and "质数" in channel["text"] for channel in channels)
    assert all(channel["name"] in {"content", "analysis"} for channel in channels)


def test_real_human_reply_survives_worker_restart(deployment):
    receipt = submit(
        deployment,
        "请先询问我报告使用哪个季度。必须等我明确回答后，再调用工具计算23乘17，"
        "并在最终答案写出我指定的季度和计算结果。现在不能先计算。",
    )
    client = deployment["client"]
    path = f"/api/runtime/sessions/{receipt['sessionId']}/snapshot"
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        response = client.get(path)
        response.raise_for_status()
        snapshot = response.json()
        if snapshot["inputRequests"]:
            break
        assert snapshot["runs"][-1]["status"] not in {"failed", "completed", "cancelled"}, snapshot[
            "runs"
        ][-1]
        time.sleep(0.3)
    else:
        pytest.fail("Agent did not publish its required clarification")
    assert not snapshot["tools"], "Model must wait before the requested computation"
    question = snapshot["inputRequests"][-1]
    deployment["worker"].terminate()
    deployment["worker"].wait(30)
    body = {
        "sessionId": receipt["sessionId"],
        "runId": receipt["runId"],
        "expectedVersion": question["stateVersion"],
        "idempotencyKey": str(uuid4()),
        "text": "使用2026年第三季度。",
    }
    accepted = client.post("/api/interactions", json=body)
    assert accepted.status_code == 202, accepted.text
    assert client.post("/api/interactions", json=body).json() == accepted.json()
    conflict = client.post("/api/interactions", json={**body, "text": "different payload"})
    assert conflict.status_code == 409 and conflict.json()["code"] == "VERSION_CONFLICT"
    queued = client.get(path).json()
    assert not queued["inputRequests"] and queued["runs"][-1]["status"] == "queued"
    assert any(
        "2026年第三季度" in channel["text"]
        for item in queued["items"]
        if item["role"] == "user"
        for channel in item["channels"]
    )
    deployment["launch"]("worker")
    result = wait_result(deployment, receipt)
    assert result["status"] == "completed", result.get("failure")
    text = answer(result)
    assert "391" in text and ("2026" in text or "第三季度" in text), text
    restored = client.get(path).json()
    assert restored["tools"] and all(tool["name"] for tool in restored["tools"])
    assert not restored["inputRequests"]


@pytest.mark.skipif(
    os.environ.get("CENTN_AGENT_PROFILE") != "planner", reason="Planner profile required"
)
def test_real_planner_tool_execution_and_plan_closure(deployment):
    receipt = submit(
        deployment,
        "请先列一个简短计划，然后使用乘法工具分别计算23乘17以及391乘19，"
        "核对两个结果之间的关系，给出准确结果和一句结论。完成后更新计划状态。",
    )
    result = wait_result(deployment, receipt, timeout=360)
    assert result["status"] == "completed" and result["outcome"] == "success", result.get("failure")
    text = answer(result).replace(",", "")
    assert "391" in text and "7429" in text and "19" in text, text
    response = deployment["client"].get(f"/api/runtime/sessions/{receipt['sessionId']}/snapshot")
    response.raise_for_status()
    snapshot = response.json()
    assert snapshot["plans"] and any(plan["steps"] for plan in snapshot["plans"])
    assert all(
        step["status"] == "completed" for plan in snapshot["plans"] for step in plan["steps"]
    )
    assert all(int(plan["revision"]) >= 2 for plan in snapshot["plans"])
    assert len(snapshot["tools"]) >= 2 and all(
        tool["status"] == "completed" for tool in snapshot["tools"]
    )


@pytest.mark.skipif(
    os.environ.get("CENTN_AGENT_PROFILE", "conversation") != "conversation",
    reason="Conversation control profile required",
)
def test_real_pause_resume_and_cancel_streaming_output(deployment):
    deployment["worker"].terminate()
    deployment["worker"].wait(30)
    receipt = submit(deployment, "请连续写出1到2000的数字，每个数字一行，不使用工具，不要省略。")
    client = deployment["client"]
    path = f"/api/runtime/sessions/{receipt['sessionId']}/snapshot"

    def snapshot():
        response = client.get(path)
        response.raise_for_status()
        return response.json()

    def control(kind):
        for _ in range(5):
            run = next(row for row in snapshot()["runs"] if row["id"] == receipt["runId"])
            response = client.post(
                f"/api/sessions/{receipt['sessionId']}/controls",
                json={
                    "runId": run["id"],
                    "kind": kind,
                    "expectedVersion": run["stateVersion"],
                    "idempotencyKey": str(uuid4()),
                },
            )
            if response.status_code == 200:
                return response.json()
            assert response.status_code == 409 and response.json()["code"] == "VERSION_CONFLICT", (
                response.text
            )
        pytest.fail("Control lost five version races")

    assert control("pause")["status"] == "accepted"
    deployment["launch"]("worker")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        current = snapshot()
        if current["runs"][-1]["status"] == "paused":
            break
        time.sleep(0.2)
    else:
        pytest.fail("Pause was not applied by the independent control Worker")
    assert not any(item["role"] == "assistant" for item in current["items"])
    assert control("resume")["status"] in {"accepted", "applied"}
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        current = snapshot()
        if any(
            item["status"] == "streaming" and any(channel["text"] for channel in item["channels"])
            for item in current["items"]
            if item["role"] == "assistant"
        ):
            break
        assert current["runs"][-1]["status"] not in {"completed", "failed", "cancelled"}, current[
            "runs"
        ]
        time.sleep(0.15)
    else:
        pytest.fail("No persisted partial output was observed before cancel")
    assert control("cancel")["status"] in {"accepted", "applied"}
    result = wait_result(deployment, receipt, timeout=90)
    assert result["status"] == "cancelled", result
    restored = snapshot()
    assert not any(item["status"] == "streaming" for item in restored["items"])
    assert any(
        channel["text"]
        for item in restored["items"]
        if item["role"] == "assistant"
        for channel in item["channels"]
    )


@pytest.mark.skipif(
    os.environ.get("CENTN_AGENT_PROFILE", "conversation") != "conversation",
    reason="Conversation throughput profile required",
)
def test_real_concurrency_and_session_isolation(deployment):
    measurements = []
    directory = Path(os.environ.get("CENTN_METRICS_DIR", str(deployment["logs"])))
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "concurrency.json").write_text("[]")
    for count in (1, 2, 4, 8):
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=count) as pool:
            receipts = list(
                pool.map(
                    lambda i, n=count: submit(
                        deployment, f"{17 + n}加{i + 1}是多少？只回答一个数字，不使用工具。"
                    ),
                    range(count),
                )
            )
            results = list(
                pool.map(lambda receipt: wait_result(deployment, receipt, timeout=240), receipts)
            )
        assert len({receipt["sessionId"] for receipt in receipts}) == count
        for i, (receipt, result) in enumerate(zip(receipts, results, strict=True)):
            assert result["runId"] == receipt["runId"] and result["status"] == "completed", (
                result.get("failure")
            )
            assert answer(result).strip().rstrip("。.") == str(17 + count + i + 1), answer(result)
        measurements.append(
            {
                "sessions": count,
                "completed": len(results),
                "durationSeconds": round(time.monotonic() - started, 3),
            }
        )
        (directory / "concurrency.json").write_text(json.dumps(measurements, indent=2))


def test_api_requires_authentication_and_no_worker_in_api(deployment):
    assert (
        httpx.get(deployment["url"].rstrip("/") + "/api/config", trust_env=False).status_code == 401
    )
    deployment["worker"].terminate()
    deployment["worker"].wait(30)
    receipt = submit(deployment, "你好，请简短回应。")
    time.sleep(1)
    snapshot = (
        deployment["client"].get(f"/api/runtime/sessions/{receipt['sessionId']}/snapshot").json()
    )
    assert snapshot["runs"][-1]["status"] == "queued"
    deployment["launch"]("worker")
    result = wait_result(deployment, receipt)
    assert result["status"] == "completed" and answer(result).strip(), result


def test_real_browser_resource_preview_download_and_revoke(deployment):
    from playwright.sync_api import sync_playwright

    directory = Path(os.environ["CENTN_SCREENSHOT_DIR"])
    directory.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            **(
                {"channel": os.environ["CENTN_BROWSER_CHANNEL"]}
                if os.environ.get("CENTN_BROWSER_CHANNEL")
                else {}
            ),
        )
        try:
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            errors = []
            page.on("pageerror", lambda exc: errors.append(str(exc)))
            page.goto(deployment["url"])
            page.locator("#token").fill(deployment["token"].get_secret_value())
            page.locator("#credentials button").click()
            page.wait_for_function("location.search.includes('session=')")
            page.locator("#resource-panel summary").click()
            raw = "计划工时1000小时，实际工时800小时。\n<script>not executable</script>".encode()
            page.locator("#resource-file").set_input_files(
                {"name": "工时数据.txt", "mimeType": "text/plain", "buffer": raw}
            )
            row = page.locator(".resource-row").filter(has_text="工时数据.txt")
            row.wait_for()
            row.get_by_role("button", name="预览").click()
            page.wait_for_function("document.querySelector('#resource-dialog').open")
            assert page.locator("#resource-text").inner_text() == raw.decode()
            assert page.locator("#resource-text script").count() == 0
            page.screenshot(path=str(directory / "p05-resource-preview.png"), full_page=True)
            page.locator("#resource-close").click()
            with page.expect_download() as downloaded:
                row.get_by_role("button", name="下载").click()
            assert Path(downloaded.value.path()).read_bytes() == raw
            assert downloaded.value.suggested_filename == "工时数据.txt"
            page.set_viewport_size({"width": 390, "height": 844})
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            page.screenshot(path=str(directory / "p05-resource-mobile.png"), full_page=True)
            row.get_by_role("button", name="撤销").click()
            row.wait_for(state="detached")
            page.reload()
            page.locator("#token").fill(deployment["token"].get_secret_value())
            page.locator("#credentials button").click()
            page.wait_for_function("document.querySelector('#login').open === false")
            assert page.locator(".resource-row").count() == 0
            assert not errors, errors
            assert page.locator("#error").inner_text() == ""
        finally:
            browser.close()


def test_real_browser_streaming_history_and_mobile(deployment):
    from playwright.sync_api import sync_playwright

    directory = Path(os.environ["CENTN_SCREENSHOT_DIR"])
    directory.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        options = {"headless": True}
        if os.environ.get("CENTN_BROWSER_CHANNEL"):
            options["channel"] = os.environ["CENTN_BROWSER_CHANNEL"]
        browser = playwright.chromium.launch(**options)
        try:
            page = browser.new_page(viewport={"width": 1280, "height": 900})
            errors = []
            page.on("pageerror", lambda exc: errors.append(str(exc)))
            page.goto(deployment["url"])
            page.locator("#token").fill(deployment["token"].get_secret_value())
            page.locator("#credentials button").click()
            page.wait_for_function("document.querySelector('#login').open === false")
            page.wait_for_function("location.search.includes('session=')")
            page.evaluate("""() => {
                window.visibleLengths = [];
                new MutationObserver(() => {
                    const selector = 'article[data-role=assistant] .content';
                    const nodes = document.querySelectorAll(selector);
                    if (nodes.length) {
                        window.visibleLengths.push(nodes[nodes.length-1].textContent.length);
                    }
                }).observe(document.querySelector('#messages'),{
                    subtree:true,childList:true,characterData:true
                });
            }""")
            page.locator("#input").fill(
                "请分十条介绍如何核对一份数据分析报告，每条说明原因，不需要工具。"
            )
            page.locator("#composer button").click()
            try:
                page.wait_for_function(
                    "['completed','failed','cancelled'].includes("
                    "document.querySelector('#status').dataset.state)",
                    timeout=240000,
                )
            except Exception:
                diagnostic = page.evaluate("""() => ({
                    status:document.querySelector('#status').textContent,
                    error:document.querySelector('#error').textContent,
                    lengths:window.visibleLengths,
                    visible:document.querySelector('#messages').innerText
                })""")
                (directory / "p05-browser-failure.json").write_text(
                    json.dumps(diagnostic, ensure_ascii=False)
                )
                page.screenshot(path=str(directory / "p05-browser-failure.png"), full_page=True)
                raise
            assert page.locator("#status").get_attribute("data-state") == "completed", page.locator(
                "#error"
            ).inner_text()
            text = page.locator("article[data-role=assistant] .content").last.inner_text()
            assert len(text) > 100
            lengths = page.evaluate("window.visibleLengths")
            assert len({n for n in lengths if n > 0}) > 2, "Browser did not show progressive output"
            assert page.locator("#error").inner_text() == ""
            page.screenshot(path=str(directory / "p05-desktop.png"), full_page=True)
            url = page.url
            page.reload()
            page.locator("#token").fill(deployment["token"].get_secret_value())
            page.locator("#credentials button").click()
            page.wait_for_function("document.querySelector('#login').open === false")
            page.wait_for_function(
                "document.querySelectorAll('article[data-role=assistant] .content').length > 0"
            )
            assert page.url == url
            assert page.locator("article[data-role=assistant] .content").last.inner_text() == text
            page.set_viewport_size({"width": 390, "height": 844})
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            assert page.locator("#input").is_visible()
            page.screenshot(path=str(directory / "p05-mobile.png"), full_page=True)
            assert not errors, errors
        finally:
            browser.close()

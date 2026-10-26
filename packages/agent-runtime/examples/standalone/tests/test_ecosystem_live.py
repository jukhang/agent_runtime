"""One actual model task through the new host, not historical component probes."""

import asyncio
import hashlib
import io
import json
import os
import time
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from test_live import answer, deployment, submit, wait_result  # noqa: F401

pytestmark = pytest.mark.skipif(
    os.environ.get("CENTN_ECOSYSTEM_LIVE") != "1", reason="Real ecosystem services required"
)


def test_model_mcp_git_skill_chart_report(deployment):  # noqa: F811
    from centn_example.resources import Resources
    from PIL import Image

    from agent_runtime.contracts.failure import AgentRuntimeError
    from agent_runtime.contracts.references import ResourceRef
    from agent_runtime.contracts.transactions import ActorContext

    evidence_dir = Path(os.environ["CENTN_ECOSYSTEM_EVIDENCE"])
    expected = json.loads((evidence_dir / "expected-data.json").read_text())
    receipt = submit(
        deployment,
        "请调用 read-metrics 获取车间最新数据，"
        "再将返回的 structuredContent 完整原样作为一个 JSON 字符串放入 "
        "chart-report 的 argv 数组，运行已安装的 Git Skill，生成 PNG 柱状图和报告。"
        "不要使用 save-report，不需要人工审批。必须完成沙箱生成后再回答，"
        "报告数据集编号、计划合计、实际合计及比例。",
    )
    result = wait_result(deployment, receipt, timeout=360)
    (evidence_dir / "model-result.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2)
    )
    assert result["status"] == "completed" and result["outcome"] == "success", answer(result)
    rows = deployment["client"].get(f"/api/sessions/{receipt['sessionId']}/resources").json()
    assert {row["title"] for row in rows} == {"chart.png", "report.txt"}, answer(result)
    png_row = next(row for row in rows if row["mime"] == "image/png")
    report_row = next(row for row in rows if row["title"] == "report.txt")
    client = deployment["client"]
    chart = client.post("/api/resources/preview", json={"resourceRef": png_row["resourceRef"]})
    assert chart.status_code == 200 and chart.headers["content-type"] == "image/png"
    download = client.post("/api/resources/download", json={"resourceRef": png_row["resourceRef"]})
    assert download.content == chart.content
    assert hashlib.sha256(download.content).hexdigest() == png_row["digest"]
    image = Image.open(io.BytesIO(download.content))
    image.load()
    assert image.size == (800, 480) and image.format == "PNG"
    assert json.loads(image.info["Dataset"]) == expected
    report = client.post(
        "/api/resources/preview", json={"resourceRef": report_row["resourceRef"]}
    ).json()
    details = json.loads(report["text"])
    assert details["data"] == expected and details["uid"] == 1000
    assert details["planned"] == sum(r["planned"] for r in expected["rows"])
    assert details["actual"] == sum(r["actual"] for r in expected["rows"])
    assert details["ratio"] == details["actual"] / details["planned"]
    for bar in details["bars"]:
        x0, y0, x1, y1 = bar["rect"]
        assert image.getpixel(((x0 + x1) // 2, (y0 + y1) // 2)) == (
            (30, 145, 160) if bar["series"] == "planned" else (220, 75, 100)
        )
    assert str(details["planned"]) in answer(result) and str(details["actual"]) in answer(result)
    assert expected["datasetId"] in answer(result)
    (evidence_dir / "chart.png").write_bytes(download.content)
    (evidence_dir / "report.json").write_text(report["text"])

    async def verify_sql_and_acl():
        engine = create_async_engine(deployment["dsn"])
        try:
            async with engine.connect() as db:
                from centn_example.resource_migrate import resource_schema

                allocations = [
                    dict(r)
                    for r in (
                        await db.execute(
                            sa.text(
                                "SELECT * FROM "
                                f'"{resource_schema(deployment["schema"])}".sandbox_allocation'
                            )
                        )
                    ).mappings()
                ]
                sql = (
                    "SELECT status,cleanup_status,sandbox_id,remote_execution_id "
                    f'FROM "{deployment["schema"]}".execution'
                )
                executions = [dict(r) for r in (await db.execute(sa.text(sql))).mappings()]
                tools = [
                    dict(r)
                    for r in (
                        await db.execute(
                            sa.text(
                                "SELECT status,spec_revision,result_ref "
                                f'FROM "{deployment["schema"]}".tool_call'
                            )
                        )
                    ).mappings()
                ]
                broker = (
                    await db.execute(
                        sa.text(f'SELECT count(*) FROM "{deployment["schema"]}".credential_request')
                    )
                ).scalar_one()
                dispatch_key = (
                    await db.execute(
                        sa.text(
                            f'SELECT w.payload_ref FROM "{deployment["schema"]}".execution e '
                            f'JOIN "{deployment["schema"]}".work_item w '
                            "ON w.id=e.supervisor_work_id "
                            "AND w.scope_id=e.scope_id"
                        )
                    )
                ).scalar_one()
            resources = Resources(engine, deployment["schema"])
            from agent_runtime.contracts.tools import SummaryResult
            from agent_runtime.storage.context_blobs import SqlContextBlobBackend
            from agent_runtime.tool_transcript import ToolTranscript

            blobs = SqlContextBlobBackend(engine, deployment["schema"])
            chart_observations = []
            for tool in tools:
                if tool["spec_revision"]["key"]["key"] == "chart-report":
                    raw = await blobs.get(os.environ["CENTN_SCOPE"], tool["result_ref"])
                    transcript = ToolTranscript.model_validate_json(raw)
                    assert isinstance(transcript.outcome.result, SummaryResult)
                    assert b"png_base64" not in raw and b"iVBOR" not in raw
                    assert len(raw) < 8192
                    chart_observations.append(json.loads(raw))
            assert len(chart_observations) == 1
            (evidence_dir / "model-chart-observation.json").write_text(
                json.dumps(chart_observations, ensure_ascii=False, indent=2)
            )
            ref = ResourceRef.model_validate(png_row["resourceRef"])
            actor = ActorContext(
                scope_id=ref.scope.scope_id,
                actor_id=ref.scope.owner_id,
                auth_revision="1",
                grants_ref="example-owner",
            )
            for principal, target in (
                (actor.model_copy(update={"actor_id": "other-owner"}), ref),
                (actor, ref.model_copy(update={"resource_version": "2"})),
                (
                    actor,
                    ref.model_copy(
                        update={
                            "scope": ref.scope.model_copy(
                                update={"session_id": "00000000-0000-4000-8000-000000000000"}
                            )
                        }
                    ),
                ),
            ):
                with pytest.raises(AgentRuntimeError):
                    await resources.get(principal, target)
            from dataclasses import replace

            from centn_example.config import Settings
            from centn_example.ecosystem import require_mcp_data
            from centn_example.host import Host

            from agent_runtime.contracts.tools import ToolInvocation

            host = Host(replace(Settings.from_env(), schema=deployment["schema"]))
            try:
                from centn_example.ecosystem_allocations import Allocation

                assert len(allocations) == 1
                row = allocations[0]
                wrong_scope = Allocation(
                    row["id"], "foreign-scope", row["remote_metadata"], row["sandbox_id"]
                )
                with pytest.raises(AgentRuntimeError):
                    await host.ecosystem.allocations.created(wrong_scope, row["sandbox_id"])
                assert (
                    await host.ecosystem.allocations.claim_cleanup(row["id"], "foreign-scope")
                    is None
                )
                with pytest.raises(AgentRuntimeError):
                    await host.ecosystem.allocations.complete_cleanup(row["id"], "foreign-scope")
                with pytest.raises(AgentRuntimeError):
                    await host.ecosystem.allocations.complete_cleanup(row["id"], row["scope_id"])
                dispatch = await host.ecosystem.payloads.get(actor.scope_id, dispatch_key)
                invocation = ToolInvocation.model_validate(dispatch["invocation"])
                changed = json.loads(json.dumps(expected))
                changed["rows"][0]["actual"] += 1
                forged = invocation.model_copy(
                    update={
                        "call": invocation.call.model_copy(
                            update={"arguments": {"argv": [json.dumps(changed)]}}
                        )
                    }
                )
                async with host.resources.sessions() as db, db.begin():
                    with pytest.raises(AgentRuntimeError):
                        await require_mcp_data(db, host, forged)
            finally:
                await host.aclose()
            return executions, tools, broker, allocations
        finally:
            await engine.dispose()

    for _ in range(60):
        executions, tools, broker, allocations = asyncio.run(verify_sql_and_acl())
        if (
            executions
            and all(e["cleanup_status"] == "completed" for e in executions)
            and all(row["status"] == "cleaned" for row in allocations)
        ):
            break
        time.sleep(1)
    assert len(executions) == 1 and executions[0]["status"] == "completed"
    assert executions[0]["cleanup_status"] == "completed" and broker >= 1
    assert len(allocations) == 1 and allocations[0]["status"] == "cleaned"
    assert {t["spec_revision"]["key"]["key"] for t in tools if t["status"] == "completed"} >= {
        "read-metrics",
        "chart-report",
    }
    # Browser uses the real host's authenticated resource route and decodes the actual PNG.
    from playwright.sync_api import sync_playwright

    screenshots = (
        Path(__file__).resolve().parents[5] / "output/playwright/p05-ecosystem" / evidence_dir.name
    )
    screenshots.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(channel=os.environ.get("CENTN_BROWSER_CHANNEL", "chrome"))
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(deployment["url"] + "?session=" + receipt["sessionId"])
        page.locator("#token").fill(deployment["token"].get_secret_value())
        page.locator("#credentials button").click()
        page.wait_for_function("document.getElementById('login').open === false")
        page.locator("#resource-panel summary").click()
        resource = page.locator(".resource-row").filter(has_text="chart.png")
        resource.get_by_role("button", name="预览").click()
        page.wait_for_function("document.getElementById('resource-chart')?.naturalWidth === 800")
        page.screenshot(path=str(screenshots / "chart-desktop.png"))
        page.set_viewport_size({"width": 390, "height": 844})
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        page.screenshot(path=str(screenshots / "chart-mobile.png"))
        page.locator("#resource-close").click()
        with page.expect_download() as file:
            resource.get_by_role("button", name="下载").click()
        assert hashlib.sha256(Path(file.value.path()).read_bytes()).hexdigest() == png_row["digest"]
        browser.close()
    for row in rows:
        body = {"resourceRef": row["resourceRef"]}
        assert client.post("/api/resources/revoke", json=body).status_code == 204
        assert client.post("/api/resources/preview", json=body).status_code == 409
        assert client.post("/api/resources/download", json=body).status_code == 409
    (evidence_dir / "host-checks.json").write_text(
        json.dumps(
            {
                "runId": receipt["runId"],
                "schema": deployment["schema"],
                "executions": executions,
                "tools": tools,
                "brokerRequests": broker,
                "allocations": allocations,
                "screenshots": str(screenshots),
                "checks": {
                    "dataProvenance": True,
                    "changedDatasetRejected": True,
                    "summaryOnlyModelObservation": True,
                    "allocationScopeAndCleanupOwnerDenied": True,
                    "decodedPngAndBarPixels": True,
                    "reportArithmetic": True,
                    "previewDownloadDigest": True,
                    "browserDesktopMobileDownload": True,
                    "crossOwnerSessionVersionDenied": True,
                    "revocationDenied": True,
                    "supervisedCleanup": True,
                },
            },
            default=str,
            indent=2,
        )
    )

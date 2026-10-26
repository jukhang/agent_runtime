"""SIGKILL after a real approved database effect, not merely after chat output."""

import asyncio
import json
import os
import time
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from test_live import deployment, submit, wait_result  # noqa: F401

pytestmark = pytest.mark.skipif(
    os.environ.get("CENTN_LIVE") != "1", reason="Real services required"
)


def test_sigkill_after_approved_report_commit_never_replays_effect(deployment):  # noqa: F811
    client = deployment["client"]
    receipt = submit(
        deployment,
        "请调用 save-report 工具保存报告，标题为恢复核验.txt，"
        "正文严格为：计划工时1000小时，实际800小时，开工率80%。"
        "需要审批时申请审批，不要只在聊天中输出。保存后解释计算方法。",
    )
    session = receipt["sessionId"]
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        response = client.get(f"/api/runtime/sessions/{session}/snapshot")
        response.raise_for_status()
        snapshot = response.json()
        pending = [row for row in snapshot["approvals"] if row["status"] == "pending"]
        if pending and snapshot["runs"][-1]["status"] == "waiting_user":
            break
        assert snapshot["runs"][-1]["status"] not in {"completed", "failed", "cancelled"}
        time.sleep(0.1)
    else:
        pytest.fail("Real model did not request report approval")
    approval = pending[0]
    approval_id = approval["approvalRef"]["resourceId"]
    decision = {
        "approvalId": approval_id,
        "expectedVersion": approval["revision"],
        "decision": "approved",
        "idempotencyKey": f"approval:{approval_id}:{approval['revision']}:approved",
    }
    response = client.post(f"/api/sessions/{session}/approvals", json=decision)
    assert response.status_code == 200, response.text

    async def wait_for_commit_and_kill():
        engine = create_async_engine(deployment["dsn"])
        try:
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                async with engine.connect() as db:
                    row = (
                        (
                            await db.execute(
                                sa.text(
                                'SELECT r.status,(SELECT count(*) FROM '
                                f'"{deployment["schema"]}".'
                                'operation o JOIN '
                                f'"{deployment["schema"]}".tool_call t '
                                "ON t.scope_id=o.scope_id AND t.operation_id=o.id "
                                "WHERE t.run_id=r.id AND o.status='committed') "
                                    "AS committed FROM "
                                    f'"{deployment["schema"]}".run r WHERE r.id=:id'
                                ),
                                {"id": receipt["runId"]},
                            )
                        )
                        .mappings()
                        .one()
                    )
                if row["committed"]:
                    assert row["committed"] == 1
                    assert row["status"] not in {"completed", "failed", "cancelled"}, (
                        "Injection missed the live post-commit window"
                    )
                    deployment["worker"].kill()
                    deployment["worker"].wait(10)
                    assert deployment["worker"].returncode == -9
                    return dict(row)
                await asyncio.sleep(0.02)
            raise AssertionError("Approved report effect was never durably committed")
        finally:
            await engine.dispose()

    committed_before = asyncio.run(wait_for_commit_and_kill())
    replacement = deployment["launch"]("worker")
    result = wait_result(deployment, receipt, timeout=180)
    assert result["status"] in {"completed", "failed"}, result
    if result["status"] == "failed":
        assert result["failure"]["publicMessage"].strip()
        assert result["failure"]["recoveryOptions"]
    reports = client.get(f"/api/sessions/{session}/resources").json()
    assert len(reports) == 1
    preview = client.post("/api/resources/preview", json={"resourceRef": reports[0]["resourceRef"]})
    assert preview.status_code == 200 and "80%" in preview.json()["text"], preview.text
    assert client.post(f"/api/sessions/{session}/approvals", json=decision).status_code == 200

    async def inspect_ledger():
        engine = create_async_engine(deployment["dsn"])
        try:
            async with engine.connect() as db:
                operations = (
                    (
                        await db.execute(
                            sa.text(
                                'SELECT id,status,result_ref FROM '
                                f'"{deployment["schema"]}".operation '
                                "WHERE id IN (SELECT operation_id FROM "
                                f'"{deployment["schema"]}".tool_call WHERE run_id=:id)'
                            ),
                            {"id": receipt["runId"]},
                        )
                    )
                    .mappings()
                    .all()
                )
                tools = (
                    (
                        await db.execute(
                            sa.text(
                                'SELECT id,status,operation_id FROM '
                                f'"{deployment["schema"]}".tool_call '
                                "WHERE run_id=:id"
                            ),
                            {"id": receipt["runId"]},
                        )
                    )
                    .mappings()
                    .all()
                )
            assert len(operations) == 1 and operations[0]["status"] == "committed"
            assert len(tools) == 1 and tools[0]["status"] == "completed"
            return {
                "operations": [dict(row) for row in operations],
                "tools": [dict(row) for row in tools],
            }
        finally:
            await engine.dispose()

    ledger = asyncio.run(inspect_ledger())
    evidence = {
        "injection": "SIGKILL after SQL-approved report effect commit before Run terminal",
        "committedBefore": committed_before,
        "replacementAlive": replacement.poll() is None,
        "reportCount": len(reports),
        "previewVerified": True,
        "result": result,
        **ledger,
    }
    destination = Path(os.environ.get("CENTN_RELEASE_EVIDENCE_DIR", str(deployment["logs"])))
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "sigkill-approved-write.json").write_text(
        json.dumps(evidence, ensure_ascii=False, default=str, indent=2)
    )

"""Real model approval invalidation after source revoke, without user cancellation."""

import asyncio
import json
import os
import time
from pathlib import Path
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from test_live import deployment, wait_result  # noqa: F401

pytestmark = pytest.mark.skipif(
    os.environ.get("CENTN_LIVE") != "1", reason="Real services required"
)


def test_pending_source_revoke_converges_without_cancel_or_clock_advance(deployment):  # noqa: F811
    client = deployment["client"]
    session = client.post("/api/sessions").json()["id"]
    instruction = (
        "请调用 save-report 工具保存报告，标题为撤权核验.txt，"
        "正文严格为：计划工时1000小时，实际800小时，开工率80%。"
        "需要审批时申请审批，不要只在聊天中输出。"
    )
    response = client.post(
        "/api/resources",
        json={
            "sessionId": session,
            "idempotencyKey": str(uuid4()),
            "title": "工时事实.txt",
            "text": instruction,
        },
    )
    response.raise_for_status()
    source = response.json()
    response = client.post(
        "/api/turns",
        json={
            "sessionId": session,
            "agent": deployment["agent"],
            "idempotencyKey": str(uuid4()),
            "inputs": [
                {"kind": "text", "text": instruction},
                {"kind": "resource", "resourceRef": source["resourceRef"]},
            ],
        },
    )
    assert response.status_code == 202, response.text
    receipt = response.json()
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        response = client.get(f"/api/runtime/sessions/{session}/snapshot")
        response.raise_for_status()
        snapshot = response.json()
        pending = [row for row in snapshot["approvals"] if row["status"] == "pending"]
        if pending and snapshot["runs"][-1]["status"] == "waiting_user":
            break
        assert snapshot["runs"][-1]["status"] not in {"completed", "failed", "cancelled"}, snapshot
        time.sleep(0.2)
    else:
        pytest.fail("Real model did not request approval")
    approval_id = pending[0]["approvalRef"]["resourceId"]
    started = time.monotonic()
    response = client.post("/api/resources/revoke", json={"resourceRef": source["resourceRef"]})
    assert response.status_code == 204, response.text
    result = wait_result(deployment, receipt, timeout=120)
    assert result["status"] in {"completed", "failed"}, result
    if result["status"] == "failed":
        assert result["failure"]["publicMessage"].strip()
    snapshot = client.get(f"/api/runtime/sessions/{session}/snapshot").json()
    rows = [row for row in snapshot["approvals"] if row["approvalRef"]["resourceId"] == approval_id]
    assert len(rows) == 1 and rows[0]["status"] == "cancelled", snapshot
    assert rows[0]["revision"] == "1" and rows[0]["allowedDecisions"] == []
    assert client.get(f"/api/sessions/{session}/resources").json() == []

    async def inspect():
        engine = create_async_engine(deployment["dsn"])
        schema = deployment["schema"]
        try:
            async with engine.connect() as db:
                approval = (
                    (
                        await db.execute(
                            sa.text(
                                "SELECT status,revision,resource_refs,"
                                "expires_at>CURRENT_TIMESTAMP AS before_expiry "
                                f'FROM "{schema}".approval WHERE id=:id'
                            ),
                            {"id": approval_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                assert approval["before_expiry"] and approval["status"] == "cancelled"
                assert source["resourceRef"] in approval["resource_refs"]
                events = (
                    (
                        await db.execute(
                            sa.text(
                                f'SELECT e.payload FROM "{schema}".event e '
                                f'JOIN "{schema}".outbox o '
                                "ON o.scope_id=e.scope_id AND o.event_id=e.id "
                                "WHERE e.run_id=:id AND e.kind='approval.updated'"
                            ),
                            {"id": receipt["runId"]},
                        )
                    )
                    .scalars()
                    .all()
                )
                assert len(events) == 1 and events[0]["status"] == "cancelled"
                counts = (
                    (
                        await db.execute(
                            sa.text(
                                f'SELECT (SELECT count(*) FROM "{schema}".operation o '
                                f'JOIN "{schema}".tool_call t '
                                "ON t.scope_id=o.scope_id AND t.operation_id=o.id "
                                "WHERE t.run_id=:id) AS operations,"
                                f'(SELECT count(*) FROM "{schema}".command '
                                "WHERE run_id=:id) AS controls,"
                                f'(SELECT owner_run_id FROM "{schema}".session_slot '
                                "WHERE session_id=:session) AS owner"
                            ),
                            {"id": receipt["runId"], "session": session},
                        )
                    )
                    .mappings()
                    .one()
                )
                assert (
                    counts["operations"] == 0
                    and counts["controls"] == 0
                    and counts["owner"] is None
                )
                return {"approval": dict(approval), "events": events, "counts": dict(counts)}
        finally:
            await engine.dispose()

    evidence = {
        "noUserCancel": True,
        "noClockAdvance": True,
        "convergenceSeconds": time.monotonic() - started,
        "result": result,
        **asyncio.run(inspect()),
    }
    destination = Path(os.environ.get("CENTN_RELEASE_EVIDENCE_DIR", str(deployment["logs"])))
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "approval-validity.json").write_text(
        json.dumps(evidence, ensure_ascii=False, default=str, indent=2)
    )

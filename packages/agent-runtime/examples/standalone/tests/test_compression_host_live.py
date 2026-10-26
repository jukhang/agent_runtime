"""Two real API turns force automatic history compression after Worker restart."""

import asyncio
import json
import os
import re
from uuid import UUID

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from test_compression_release_live import persist
from test_live import deployment, submit, wait_result  # noqa: F401

from agent_runtime.storage.context_blobs import SqlContextBlobBackend

pytestmark = pytest.mark.skipif(
    os.environ.get("CENTN_LIVE") != "1", reason="Real services required"
)

FACTS = (
    "ActiveAlpha planned hours: 1000.\n"
    "ActiveAlpha completed hours: 800.\n"
    "ActiveAlpha completion was incorrectly stated as 90%; corrected completion is 80%, not 90%.\n"
    "ActiveBeta approved budget: 2300 CNY.\n"
    "ActiveBeta must not be published without legal approval.\n"
    "ActiveBeta unfinished work: legal review and supplier signature are both still pending.\n"
)
QUESTION = (
    "Using the earlier ActiveAlpha and ActiveBeta facts, answer exactly six numbered lines: "
    "1 planned hours; 2 completed hours; 3 corrected completion percentage and the rejected "
    "old percentage; 4 approved budget and currency; 5 publication restriction; "
    "6 unfinished work. Do not use tools. Answer in English, concisely."
)


def test_real_host_automatically_compresses_history_after_worker_restart(deployment):  # noqa: F811
    host = deployment
    archived = (
        "ArchivedGamma is an unrelated closed project. Its old notebook describes blue "
        "labels and archived meeting rooms; no active work is tracked here.\n"
    ) * 50
    initial = (
        "Remember this project history for the next turn. Do not use tools. "
        "For this turn reply only: History received.\n" + FACTS + archived
    )
    first = submit(host, initial)
    first_result = wait_result(host, first, timeout=240)
    persist(host, "host-compression-first-result.json", first_result)
    assert first_result["status"] == "completed", first_result
    old = host["worker"]
    old.terminate()
    old.wait(30)
    host["environment"]["CENTN_CONTEXT_INPUT_TOKENS"] = "7000"
    replacement = host["launch"]("worker")
    second = submit(host, QUESTION, session=first["sessionId"])
    second_result = wait_result(host, second, timeout=300)
    persist(host, "host-compression-second-result.json", second_result)

    async def audit():
        engine = create_async_engine(host["dsn"])
        blobs = SqlContextBlobBackend(engine, host["schema"])
        try:
            async with engine.connect() as db:
                runs = [
                    dict(row)
                    for row in (
                        await db.execute(
                            sa.text(
                                f"SELECT id,scope_id,session_id,status FROM {host['schema']}.run "
                                "WHERE id IN (:first,:second) ORDER BY created_at"
                            ),
                            {"first": UUID(first["runId"]), "second": UUID(second["runId"])},
                        )
                    ).mappings()
                ]
                snapshots = [
                    dict(row)
                    for row in (
                        await db.execute(
                            sa.text(
                                "SELECT id,scope_id,run_id,context_version,payload_ref,source_refs,"
                                "estimated_input_tokens,budget "
                                f"FROM {host['schema']}.context_snapshot "
                                "WHERE run_id IN (:first,:second) ORDER BY created_at"
                            ),
                            {"first": UUID(first["runId"]), "second": UUID(second["runId"])},
                        )
                    ).mappings()
                ]
                attempts = [
                    dict(row)
                    for row in (
                        await db.execute(
                            sa.text(
                                "SELECT id,scope_id,run_id,context_snapshot_id,status,usage,"
                                "output_ref,input_ref,finish_reason "
                                f"FROM {host['schema']}.model_attempt "
                                "WHERE run_id IN (:first,:second) ORDER BY started_at"
                            ),
                            {"first": UUID(first["runId"]), "second": UUID(second["runId"])},
                        )
                    ).mappings()
                ]
                text = (
                    await db.execute(
                        sa.text(
                            "SELECT string_agg(payload->>'text','' ORDER BY session_seq) "
                            f"FROM {host['schema']}.event "
                            "WHERE run_id=:run AND kind='assistant.delta'"
                        ),
                        {"run": UUID(second["runId"])},
                    )
                ).scalar_one()
                tool_count = (
                    await db.execute(sa.text(f"SELECT count(*) FROM {host['schema']}.tool_call"))
                ).scalar_one()
            summaries = []
            required = []
            for snapshot in snapshots:
                payload = json.loads(await blobs.get(snapshot["scope_id"], snapshot["payload_ref"]))
                snapshot["payload"] = payload
                if str(snapshot["run_id"]) != second["runId"]:
                    continue
                for source in snapshot["source_refs"]:
                    if source["disposition"] == "summarized":
                        envelope = json.loads(
                            await blobs.get(
                                snapshot["scope_id"], source["summaryRef"]["resourceId"]
                            )
                        )
                        summaries.append({"source": source, "envelope": envelope})
                    if source["required"]:
                        entry = next(
                            item
                            for item in payload["sources"]
                            if item["sourceId"] == source["sourceId"]
                        )
                        original = None
                        if source["resource"]["resourceType"] == "turn_input":
                            async with engine.connect() as db:
                                ref = (
                                    await db.execute(
                                        sa.text(
                                            f"SELECT input_ref FROM {host['schema']}.turn "
                                            "WHERE id=:id"
                                        ),
                                        {"id": UUID(source["resource"]["resourceId"])},
                                    )
                                ).scalar_one()
                            original = (await blobs.get(snapshot["scope_id"], ref)).decode()
                        required.append({"source": source, "entry": entry, "original": original})
            return {
                "runs": runs,
                "snapshots": snapshots,
                "attempts": attempts,
                "summaries": summaries,
                "required": required,
                "answer": text,
                "toolCount": tool_count,
            }
        finally:
            await engine.dispose()

    evidence = asyncio.run(audit())
    evidence.update(
        {
            "firstReceipt": first,
            "secondReceipt": second,
            "firstResult": first_result,
            "secondResult": second_result,
            "inputBytes": len(initial.encode()),
            "archivedRepeats": 50,
            "initialInputLimit": 100000,
            "replacementInputLimit": 7000,
            "oldWorkerPid": old.pid,
            "oldWorkerExit": old.returncode,
            "replacementWorkerPid": replacement.pid,
            "replacementAlive": replacement.poll() is None,
            "historyCreation": "API submission only; no SQL seeds or manually built summaries",
        }
    )
    persist(host, "host-compression.json", evidence)
    assert second_result["status"] == "completed", second_result
    assert replacement.pid != old.pid and evidence["replacementAlive"]
    assert len(evidence["runs"]) == 2
    assert len({run["scope_id"] for run in evidence["runs"]}) == 1
    assert {str(run["session_id"]) for run in evidence["runs"]} == {first["sessionId"]}
    assert evidence["summaries"], "MainFactory must automatically summarize actual prior history"
    first_snapshots = [row for row in evidence["snapshots"] if str(row["run_id"]) == first["runId"]]
    assert first_snapshots and all(
        row["budget"]["maxInputTokens"] > 16384 for row in first_snapshots
    )
    assert not any(
        source["disposition"] == "summarized"
        for row in first_snapshots
        for source in row["source_refs"]
    )
    final_snapshots = [
        row
        for row in evidence["snapshots"]
        if str(row["run_id"]) == second["runId"]
        and any(source["disposition"] == "summarized" for source in row["source_refs"])
    ]
    assert all(row["budget"]["maxInputTokens"] == 7000 for row in final_snapshots)
    attempts = {str(row["id"]): row for row in evidence["attempts"]}
    for summary in evidence["summaries"]:
        source, envelope = summary["source"], summary["envelope"]
        assert not source["required"] and source["summaryStatus"] == "completed"
        assert source["resource"]["resourceType"] == "turn_input"
        attempt = attempts[envelope["attemptId"]]
        assert attempt["status"] == "completed" and attempt["usage"]["status"] == "complete"
        assert attempt["output_ref"] == source["summaryRef"]["resourceId"]
        assert str(attempt["context_snapshot_id"]) == envelope["inputSnapshotId"]
        assert envelope["renderedRequestDigest"]
    assert evidence["required"]
    current_inputs = []
    for required in evidence["required"]:
        assert required["source"]["disposition"] == "included"
        assert required["source"]["summaryRef"] is None
        if required["original"] is not None:
            assert required["entry"]["content"] == required["original"]
            assert QUESTION in required["original"]
            current_inputs.append(required)
    assert current_inputs
    lower = evidence["answer"].lower().replace(",", "")
    lines = dict(re.findall(r"(?m)^\s*([1-6])[.)]\s*(.+)$", lower))
    assert set(lines) == set("123456"), evidence["answer"]
    assert "1000" in lines["1"] and "800" in lines["2"]
    assert "80%" in lines["3"] and "90%" in lines["3"]
    assert any(word in lines["3"] for word in ("reject", "incorrect", "old", "not"))
    assert "2300" in lines["4"] and "cny" in lines["4"]
    assert "legal" in lines["5"] and "approval" in lines["5"]
    assert "not" in lines["5"] or "without" in lines["5"]
    assert all(word in lines["6"] for word in ("legal", "review", "supplier", "signature"))
    assert "pending" in lines["6"] or "unfinished" in lines["6"]
    assert evidence["toolCount"] == 0
    evidence["passed"] = True
    persist(host, "host-compression.json", evidence)

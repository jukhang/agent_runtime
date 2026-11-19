from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.contracts.canonical import canonical_bytes, content_digest
from agent_runtime.contracts.decisions import FinishPayload, RecoveredToolFailure
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.storage.tool_recovery_evidence import recorded_outcome, verify_tool_recoveries


def evidence_rows():
    run, failed, completed = uuid4(), uuid4(), uuid4()
    now = datetime.now(UTC)
    values = []
    for identity, status, finished in (
        (failed, "failed", now),
        (completed, "completed", now + timedelta(seconds=1)),
    ):
        outcome = {
            "toolCallId": str(identity),
            "runId": str(run),
            "operationId": None,
            "status": status,
            "effect": "none",
            "effectRefs": [],
        }
        outcome.update(
            {
                "failure": {
                    "code": "INVALID_INPUT",
                    "category": "tool",
                    "retryable": False,
                    "publicMessage": "Invalid original arguments.",
                }
            }
            if status == "failed"
            else {"result": {"policy": "full", "data": {"verified": True}}}
        )
        data = {"outcome": outcome}
        values.append(
            {
                "id": identity,
                "status": status,
                "operation_id": None,
                "finished_at": finished,
                "content": canonical_bytes(data),
                "digest": content_digest(data),
            }
        )
    return run, failed, completed, values


def assessment(failed, completed):
    return RecoveredToolFailure(
        failed_tool_call_id=failed,
        recovery_tool_call_ids=(completed,),
        reason="Corrected arguments, then read and verified the saved result.",
    )


@pytest.mark.asyncio
async def test_unexecuted_planned_operation_can_be_recovered_without_fabricating_ledger():
    import json

    run, failed, success, rows = evidence_rows()
    intent = uuid4()
    data = json.loads(rows[0]["content"])
    data["outcome"].update(status="not_executed", operationId=str(intent))
    data["outcome"]["failure"]["operationId"] = str(intent)
    rows[0].update(
        status="not_executed", content=canonical_bytes(data), digest=content_digest(data)
    )
    db = Database(rows)
    assert await verify_tool_recoveries(
        db,
        "test",
        scope_id="s",
        run_id=run,
        failures={failed},
        assessments=(assessment(failed, success),),
    )
    assert rows[0]["operation_id"] is None
    assert not any("operation o" in query for query, _ in db.queries)
    data["outcome"]["status"] = "failed"
    rows[0].update(status="failed", content=canonical_bytes(data), digest=content_digest(data))
    with pytest.raises(AgentRuntimeError):
        recorded_outcome(rows[0], run_id=run)


class Database:
    def __init__(self, calls):
        self.calls, self.queries = calls, []

    async def execute(self, query, params):
        self.queries.append((str(query), params))
        if "operation o" in str(query):
            return SimpleNamespace(mappings=lambda: [])
        return SimpleNamespace(mappings=lambda: self.calls)


@pytest.mark.asyncio
async def test_verified_later_success_recovers_failure_without_mutating_original():
    run, failed, success, rows = evidence_rows()
    db = Database(rows)
    assert await verify_tool_recoveries(
        db,
        "test",
        scope_id="scope",
        run_id=run,
        failures={failed},
        assessments=(assessment(failed, success),),
    )
    assert rows[0]["status"] == "failed" and "Invalid original" in rows[0]["content"].decode()
    assert db.queries[0][1]["run"] == run and db.queries[0][1]["scope"] == "scope"


@pytest.mark.asyncio
async def test_missing_or_incomplete_coverage_stays_partial():
    run, failed, success, rows = evidence_rows()
    assert not await verify_tool_recoveries(
        Database([]), "test", scope_id="s", run_id=run, failures={failed}, assessments=()
    )
    assert not await verify_tool_recoveries(
        Database(rows),
        "test",
        scope_id="s",
        run_id=run,
        failures={failed, uuid4()},
        assessments=(assessment(failed, success),),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    ["foreign", "missing", "old", "unfinished", "failed_verifier", "corrupt", "unknown_effect"],
)
async def test_unproven_recovery_is_rejected(fault):
    run, failed, success, rows = evidence_rows()
    if fault == "foreign":
        run = uuid4()
    elif fault == "missing":
        rows.pop()
    elif fault == "old":
        rows[1]["finished_at"] = rows[0]["finished_at"] - timedelta(seconds=1)
    elif fault == "unfinished":
        rows[1]["finished_at"] = None
    elif fault == "failed_verifier":
        rows[1]["status"] = "failed"
    elif fault == "corrupt":
        rows[0]["digest"] = "0" * 64
    else:
        import json

        data = json.loads(rows[0]["content"])
        operation = uuid4()
        data["outcome"].update(effect="unknown", operationId=str(operation))
        rows[0].update(
            operation_id=operation, content=canonical_bytes(data), digest=content_digest(data)
        )
    with pytest.raises(AgentRuntimeError) as error:
        await verify_tool_recoveries(
            Database(rows),
            "test",
            scope_id="s",
            run_id=run,
            failures={failed},
            assessments=(assessment(failed, success),),
        )
    expected = (
        "AUDIT_UNAVAILABLE" if fault in {"foreign", "failed_verifier", "corrupt"}
        else "SIDE_EFFECT_UNKNOWN" if fault in {"unfinished", "unknown_effect"}
        else "INVALID_EVIDENCE"
    )
    assert error.value.failure.code == expected


def test_recovery_contract_rejects_duplicates_self_reference_and_blank_claims():
    run, failed, success, rows = evidence_rows()
    for ids, reason in (((failed,), "self"), ((success, success), "duplicate"), ((success,), " ")):
        with pytest.raises(ValidationError):
            RecoveredToolFailure(
                failed_tool_call_id=failed, recovery_tool_call_ids=ids, reason=reason
            )
    entry = assessment(failed, success)
    with pytest.raises(ValidationError):
        FinishPayload(
            outcome="success",
            evidence_refs=(),
            final_item_ids=(),
            recovered_tool_failures=(entry, entry),
        )
    assert recorded_outcome(rows[0], run_id=run).effect == "none"

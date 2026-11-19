"""Validate completion-review evidence without rewriting unsuccessful ToolCalls."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any, NoReturn
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.canonical import canonical_bytes
from agent_runtime.contracts.decisions import RecoveredToolFailure
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.tools import TerminalToolOutcome, ToolNotExecuted, tool_outcome_adapter
from agent_runtime.storage.uow.schema import scoped

UNSUCCESSFUL_TOOL_STATES = frozenset({"failed", "cancelled", "not_executed"})


def invalid_evidence(detail: str) -> NoReturn:
    raise runtime_failure("INVALID_EVIDENCE", category="validation", public_message=detail)


def recorded_outcome(row: Mapping[str, Any], *, run_id: UUID) -> TerminalToolOutcome:
    """Require immutable, intact outcome bytes paired with the exact ledger identity."""
    raw = row["content"]
    try:
        data = json.loads(raw)
        if hashlib.sha256(raw).hexdigest() != row["digest"] or canonical_bytes(data) != raw:
            raise ValueError("outcome digest differs")
        value = data.get("outcome", data)
        result = tool_outcome_adapter.validate_python(value)
        unexecuted_intent = (
            isinstance(result, ToolNotExecuted)
            and row["operation_id"] is None
            and result.operation_id is not None
            and result.failure.operation_id == str(result.operation_id)
        )
        if (
            not isinstance(result, TerminalToolOutcome)
            or result.tool_call_id != row["id"]
            or result.run_id != run_id
            or (result.operation_id != row["operation_id"] and not unexecuted_intent)
            or getattr(result, "status", None) != row["status"]
        ):
            raise ValueError("outcome identity differs")
    except (ValueError, TypeError, AttributeError):
        raise runtime_failure(
            "AUDIT_UNAVAILABLE",
            category="storage",
            public_message="Recovery evidence has no intact terminal tool outcome.",
        ) from None
    return result


async def verify_tool_recoveries(
    db: AsyncSession,
    schema: str,
    *,
    scope_id: str,
    run_id: UUID,
    failures: set[UUID],
    assessments: tuple[RecoveredToolFailure, ...],
) -> bool:
    """Model owns semantic assessment; SQL proves provenance, ordering and effect safety."""
    if not assessments:
        return not failures
    ids = {
        identity
        for assessment in assessments
        for identity in (assessment.failed_tool_call_id, *assessment.recovery_tool_call_ids)
    }
    result = await db.execute(
        scoped(
            "SELECT t.id,t.status,t.operation_id,t.finished_at,b.content,b.digest "
            "FROM {t}tool_call t LEFT JOIN {t}context_blob b "
            "ON b.scope_id=t.scope_id AND b.blob_key=t.result_ref "
            "WHERE t.scope_id=:scope AND t.run_id=:run AND t.id=ANY(:ids)",
            schema,
        ),
        {"scope": scope_id, "run": run_id, "ids": list(ids)},
    )
    calls = {row["id"]: dict(row) for row in result.mappings()}
    if set(calls) != ids:
        invalid_evidence("Recovery evidence is not available in the current Run.")
    operation_ids = {row["operation_id"] for row in calls.values() if row["operation_id"]}
    operations = {}
    if operation_ids:
        result = await db.execute(
            scoped(
                "SELECT o.id,o.status,o.result_ref FROM {t}operation o JOIN {t}run r "
                "ON r.scope_id=o.scope_id AND r.task_id=o.task_id "
                "WHERE o.scope_id=:scope AND r.id=:run AND o.id=ANY(:ids) "
                "ORDER BY o.id FOR SHARE OF o",
                schema,
            ),
            {"scope": scope_id, "run": run_id, "ids": list(operation_ids)},
        )
        operations = {row["id"]: row for row in result.mappings()}
    outcomes = {identity: recorded_outcome(row, run_id=run_id) for identity, row in calls.items()}
    for identity, outcome in outcomes.items():
        if calls[identity]["finished_at"] is None or outcome.effect == "unknown":
            raise runtime_failure(
                "SIDE_EFFECT_UNKNOWN",
                category="tool",
                public_message="Recovery evidence has unfinished or unknown effects.",
            )
        if calls[identity]["operation_id"]:
            operation = operations.get(outcome.operation_id)
            expected = "committed" if outcome.effect == "committed" else "failed_safe"
            if (
                operation is None
                or operation["status"] != expected
                or (expected == "committed" and not operation["result_ref"])
            ):
                raise runtime_failure(
                    "SIDE_EFFECT_UNKNOWN",
                    category="tool",
                    public_message="Recovery operation has not been safely reconciled.",
                )
    for assessment in assessments:
        failed_id = assessment.failed_tool_call_id
        if failed_id not in failures or calls[failed_id]["status"] not in UNSUCCESSFUL_TOOL_STATES:
            invalid_evidence("Recovery assessment does not name an unsuccessful tool.")
        for identity in assessment.recovery_tool_call_ids:
            if calls[identity]["status"] != "completed" or (
                calls[identity]["finished_at"] <= calls[failed_id]["finished_at"]
            ):
                invalid_evidence("Recovery requires a later successfully completed tool.")
    return {entry.failed_tool_call_id for entry in assessments} == failures

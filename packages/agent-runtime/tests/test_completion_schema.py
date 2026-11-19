from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator
from pydantic import TypeAdapter

from agent_runtime.contracts.decisions import Finish
from agent_runtime.storage.completion_schema import (
    bind_completion_recoveries,
    completion_recovery_choices,
)


def candidate(failed, successful):
    return {
        "kind": "finish",
        "payload": {
            "outcome": "success",
            "evidenceRefs": [],
            "finalItemIds": [],
            "recoveredToolFailures": [
                {
                    "failedToolCallId": failed,
                    "recoveryToolCallIds": [successful],
                    "reason": "The later result verified the requested goal.",
                }
            ],
        },
    }


def test_completion_schema_keeps_failed_and_successful_menus_separate():
    failed, completed, pending = (str(uuid4()) for _ in range(3))
    schema = TypeAdapter(Finish).json_schema(by_alias=True)
    bind_completion_recoveries(
        schema, {failed: "not_executed", completed: "completed", pending: "running"}
    )
    # Public schemas use Unicode property escapes; Python re is not their regex engine.
    Draft202012Validator.check_schema(schema, format_checker=None)
    validator = Draft202012Validator(schema)
    validator.validate(candidate(failed, completed))
    for first, second in [(completed, failed), (pending, completed), (failed, str(uuid4()))]:
        assert not validator.is_valid(candidate(first, second))


@pytest.mark.parametrize("calls", [{}, {str(uuid4()): "completed"}, {str(uuid4()): "running"}])
def test_no_unsuccessful_visible_calls_means_no_recovery_claims(calls):
    schema = TypeAdapter(Finish).json_schema(by_alias=True)
    bind_completion_recoveries(schema, calls)
    Draft202012Validator.check_schema(schema, format_checker=None)
    value = {
        "kind": "finish",
        "payload": {"outcome": "success", "evidenceRefs": [], "finalItemIds": []},
    }
    Draft202012Validator(schema).validate(value)
    assert not Draft202012Validator(schema).is_valid(candidate(str(uuid4()), str(uuid4())))


@pytest.mark.asyncio
async def test_sql_menu_queries_only_delivered_tool_identities_in_this_run():
    visible, omitted, document = uuid4(), uuid4(), uuid4()
    sources = [
        SimpleNamespace(
            resource=SimpleNamespace(resource_type=kind, resource_id=str(identity)),
            disposition=disposition,
        )
        for identity, kind, disposition in [
            (visible, "tool_call_result", "included"),
            (omitted, "tool_call_result", "omitted"),
            (document, "artifact", "included"),
        ]
    ]
    result = SimpleNamespace(mappings=lambda: [{"id": visible, "status": "failed"}])
    db = SimpleNamespace(execute=AsyncMock(return_value=result))
    request = SimpleNamespace(scope_id="current-scope", run_id=uuid4())
    value = await completion_recovery_choices(db, "test", request, SimpleNamespace(sources=sources))
    assert value == {str(visible): "failed"}
    args = db.execute.await_args.args[1]
    assert args == {"scope": "current-scope", "run": request.run_id, "ids": [visible]}


@pytest.mark.asyncio
async def test_empty_context_never_queries_or_discloses_sql_tool_results():
    db = SimpleNamespace(execute=AsyncMock())
    assert (
        await completion_recovery_choices(db, "test", object(), SimpleNamespace(sources=[])) == {}
    )
    db.execute.assert_not_awaited()

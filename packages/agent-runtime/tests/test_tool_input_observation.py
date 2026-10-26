"""Invalid input is an observation only before any tool execution boundary."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import TypeAdapter
from test_tool_timeout_boundary import fake_host, grant_for, invocation_pair

from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.tools import ToolNotExecuted
from agent_runtime.storage.tool_host import ToolEffect
from agent_runtime.tool_schemas import PinnedToolSchemas


def rejecting_schemas(spec, mode):
    if mode == "pinned":
        return PinnedToolSchemas({spec.input_schema: TypeAdapter(dict[str, int])})
    if mode == "raw_pydantic":
        return SimpleNamespace(
            input=lambda spec, args: TypeAdapter(dict[str, int]).validate_python(args)
        )
    if mode == "typed":
        error = runtime_failure(
            "INVALID_INPUT", category="validation", public_message="safe detail"
        )
    else:
        error = (
            ValueError("PRIVATE_ARGUMENT_TEXT")
            if mode == "value"
            else TypeError("PRIVATE_ARGUMENT_TEXT")
        )
    return SimpleNamespace(input=Mock(side_effect=error))


@pytest.mark.asyncio
@pytest.mark.parametrize("effect", ["none", "external"])
@pytest.mark.parametrize("mode", ["value", "type", "raw_pydantic", "typed", "pinned"])
async def test_invalid_schema_input_returns_not_executed_without_side_effects(effect, mode):
    spec, invocation = invocation_pair(effect=effect)
    invocation = invocation.model_copy(
        update={
            "call": invocation.call.model_copy(
                update={"arguments": {"private": "PRIVATE_ARGUMENT_TEXT"}}
            )
        }
    )
    original = invocation.model_dump(mode="json")
    host = fake_host(spec)
    host._schemas = rejecting_schemas(spec, mode)
    hook = SimpleNamespace(before=AsyncMock(), after=AsyncMock())
    host._hooks = (hook,)
    outcome = await host.execute(invocation, grant_for(spec, invocation))
    assert isinstance(outcome, ToolNotExecuted)
    assert outcome.tool_call_id == invocation.call.tool_call_id
    assert outcome.run_id == invocation.run_id and outcome.operation_id == invocation.operation_id
    assert outcome.effect == "none" and outcome.effect_refs == ()
    assert outcome.failure.code == "INVALID_INPUT" and outcome.failure.category == "validation"
    assert outcome.failure.retryable is False
    assert "PRIVATE_ARGUMENT_TEXT" not in outcome.model_dump_json()
    if mode == "typed":
        assert outcome.failure is host._schemas.input.side_effect.failure
    assert invocation.model_dump(mode="json") == original
    host._specs.get.assert_awaited_once()
    host._resources.resources.assert_not_awaited()
    host._check.assert_not_awaited()
    host._cached.assert_not_awaited()
    host._capacity.reserve.assert_not_awaited()
    host._capacity.release.assert_not_awaited()
    host._capacity.settle_committed.assert_not_awaited()
    host._execute_authorized.assert_not_awaited()
    hook.before.assert_not_awaited()
    hook.after.assert_not_awaited()
    assert host._groups == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code,category",
    [
        ("FORBIDDEN", "authorization"),
        ("FENCED", "concurrency"),
        ("CONTROL_PENDING", "concurrency"),
        ("INVALID_INPUT", "authorization"),
        ("UNEXPECTED", "validation"),
    ],
)
async def test_non_validation_input_error_is_unchanged(code, category):
    spec, invocation = invocation_pair()
    host = fake_host(spec)
    error = runtime_failure(code, category=category, public_message="original")
    host._schemas.input = Mock(side_effect=error)
    with pytest.raises(type(error)) as caught:
        await host.execute(invocation, grant_for(spec, invocation))
    assert caught.value is error


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [asyncio.CancelledError(), RuntimeError("programming defect")])
async def test_input_cancel_and_unknown_exception_propagate(error):
    spec, invocation = invocation_pair()
    host = fake_host(spec)
    host._schemas.input = Mock(side_effect=error)
    with pytest.raises(type(error)) as caught:
        await host.execute(invocation, grant_for(spec, invocation))
    assert caught.value is error


@pytest.mark.asyncio
@pytest.mark.parametrize("position", ["handler", "output", "commit"])
async def test_execution_validation_errors_are_not_not_executed(position):
    spec, invocation = invocation_pair(effect="external" if position == "commit" else "none")
    if position == "commit":
        spec = spec.model_copy(update={"side_effect": "database", "idempotency": "transactional"})
    host = fake_host(spec)
    del host._execute_authorized
    error = ValueError("execution already started")
    handler = SimpleNamespace(invoke=AsyncMock(return_value=ToolEffect(data={}, summary="ok")))
    host._read = {spec.ref: handler}
    host._database = {spec.ref: handler}
    host._schemas.output = Mock()
    host._claim = AsyncMock(return_value={"status": "prepared"})
    host._commit = AsyncMock()
    if position == "handler":
        handler.invoke.side_effect = error
    elif position == "output":
        host._schemas.output.side_effect = error
    else:
        host._commit.side_effect = error
    with pytest.raises(ValueError) as caught:
        await host.execute(invocation, grant_for(spec, invocation))
    assert caught.value is error
    handler.invoke.assert_awaited_once()

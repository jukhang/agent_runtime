import json
from importlib.resources import files

import pytest
from pydantic import TypeAdapter

from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.registry import HookSpec
from agent_runtime.contracts.tools import ToolInvocation, ToolSpec
from agent_runtime.storage.tool_host import SqlToolHost, ToolEffect
from agent_runtime.tool_hooks import PinnedToolHooks
from agent_runtime.tool_schemas import PinnedToolSchemas

DATA = json.loads(files("agent_runtime.contracts").joinpath("fixtures/tools.json").read_text())


def objects():
    raw = DATA["spec"] | {
        "kind": "read",
        "sideEffect": "none",
        "idempotency": "read_only",
        "publicResultPolicy": "full",
    }
    invocation = DATA["invocation"] | {"operationId": None}
    return ToolSpec.model_validate(raw), ToolInvocation.model_validate(invocation)


@pytest.mark.asyncio
async def test_pinned_schemas_and_read_projection():
    spec, invocation = objects()
    schemas = PinnedToolSchemas(
        {
            spec.input_schema: TypeAdapter(dict[str, str]),
            spec.output_schema: TypeAdapter(dict[str, bool]),
        }
    )
    schemas.input(spec, {"query": "x"})
    schemas.output(spec, {"ok": True})
    with pytest.raises(Exception, match="pinned tool schema"):
        PinnedToolSchemas({}).input(spec, {})
    host = SqlToolHost.__new__(SqlToolHost)
    host._schemas = schemas
    host._hooks = ()
    outcome = await host._completed(spec, invocation, ToolEffect(data={"ok": True}, summary="ok"))
    assert outcome.effect == "none" and outcome.operation_id is None


@pytest.mark.asyncio
async def test_pinned_hooks_block_before_execution_and_require_failure_reporting():
    spec, invocation = objects()
    ref = CapabilityRef.model_validate(
        {"key": {"namespace": "hook", "key": "safety"}, "revision": "1"}
    )
    hook = HookSpec(
        ref=ref,
        handler=ref,
        stage="before_execute",
        order=0,
        timeout_seconds=1,
        failure_policy="block",
        replay="pure",
    )

    async def deny(current, pinned, outcome):
        raise ValueError("blocked")

    chain = PinnedToolHooks((hook,), {ref: deny})
    host = SqlToolHost.__new__(SqlToolHost)
    host._hooks = (chain,)
    assert host.installed_hook_refs == frozenset({ref})
    with pytest.raises(ValueError, match="blocked"):
        await chain.before(invocation, spec)
    terminal = hook.model_copy(update={"stage": "terminal", "failure_policy": "notify_retry"})
    with pytest.raises(ValueError, match="failure reporter"):
        PinnedToolHooks((terminal,), {ref: deny})

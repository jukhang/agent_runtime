"""Executable backend tutorials and the boundaries their explanations promise."""

import asyncio
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError
from runtime_examples import (
    agent_config,
    context,
    durable,
    human,
    knowledge,
    loop,
    mcp,
    memory,
    skill,
    subagents,
    tools,
)

from agent_runtime.context_runtime import LoadedSource
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.mcp.install import McpToolSchemas
from agent_runtime.skills.git import GitSkillInstaller, VerifiedSkillBundle


@pytest.mark.parametrize(
    "module",
    [
        "agent_config",
        "tools",
        "context",
        "loop",
        "knowledge",
        "memory",
        "mcp",
        "skill",
        "human",
        "subagents",
    ],
)
def test_offline_entrypoints(module: str) -> None:
    root = Path(__file__).resolve().parents[1]
    sdk = root.parents[1] / "src"
    result = subprocess.run(
        [sys.executable, "-m", f"runtime_examples.{module}"],
        cwd=root,
        env={**os.environ, "PYTHONPATH": os.pathsep.join((str(root), str(sdk)))},
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip()


def test_agent_bindings_and_budget() -> None:
    agent = agent_config.build_agent()
    assert agent.tools == (tools.tool_spec().ref,)
    assert agent.budget.max_tool_calls == 24 and agent.add_datetime_to_context
    assert agent_config.model_config(thinking=True).thinking == "enabled"
    assert knowledge.knowledge_binding().ref in knowledge.configured_agent().knowledge


def test_tool_schemas_and_business_result() -> None:
    spec = tools.tool_spec()
    tools.schemas().input(spec, {"left": 23.0, "right": 17.0})
    result = tools.calculate(tools.MultiplyInput(left=23.0, right=17.0))
    assert result.value == 391
    tools.schemas().output(spec, result.model_dump(mode="json"))
    with pytest.raises(AgentRuntimeError):
        tools.schemas().input(spec, {"left": "not-a-number", "right": 17.0})


@pytest.mark.asyncio
async def test_context_roundtrip_and_offline_loop() -> None:
    snapshot = await context.demo()
    assert snapshot.context_version == "1" and snapshot.sources[0].required
    result = await loop.demo()
    assert result["mode"] == "offline-scripted-provider"
    assert result["action"]["kind"] == "respond"
    assert result["answerTexts"] == ["Hello from the SDK."]
    assert result["eventKinds"] == ["content.delta", "content.delta", "finish"]


@pytest.mark.asyncio
async def test_loop_proposes_tool_without_executing_or_fabricating_an_answer() -> None:
    result = await loop.demo(tool_decision=True)
    assert result["action"]["kind"] == "invoke_tools"
    calls = result["action"]["payload"]["validatedCalls"]
    assert len(calls) == 1 and calls[0]["arguments"] == {"left": 23.0, "right": 17.0}
    assert result["answerTexts"] == []
    assert result["eventKinds"] == ["tool.complete", "finish"]


@pytest.mark.asyncio
async def test_context_revocation_and_required_budget_failure() -> None:
    runtime, sources = context.build_context()
    ref = ResourceRef(
        resource_type="attachment",
        resource_id="facts",
        resource_version="1",
        scope=ResourceScope(scope_id=context.actor().scope_id),
    )
    sources.sources[ref] = LoadedSource(resource=ref, content="Do not invent facts", required=True)
    req = context.request("Explain", (ref,))
    snapshot = await runtime.prepare(req)
    sources.sources.clear()
    with pytest.raises(AgentRuntimeError, match="revoked"):
        await sources.verify(context.actor(), snapshot)
    sources.sources[ref] = LoadedSource(resource=ref, content="x" * 30000, required=True)
    with pytest.raises(AgentRuntimeError) as error:
        await runtime.prepare(req)
    assert error.value.failure.code == "BUDGET_EXHAUSTED"
    wrong = req.model_copy(update={"actor": req.actor.model_copy(update={"actor_id": "other"})})
    with pytest.raises(AgentRuntimeError) as error:
        await runtime.prepare(wrong)
    assert error.value.failure.code == "FORBIDDEN"


def test_mcp_schema_binding_and_rejects_extra_arguments() -> None:
    spec = mcp.tool_spec()
    assert spec.ref in mcp.configured_agent().tools
    assert spec.mcp is not None and spec.mcp.protocol_version == "2026-07-28"
    McpToolSchemas().input(spec, {"factory": "A"})
    with pytest.raises(ValueError):
        McpToolSchemas().input(spec, {"factory": "A", "url": "https://untrusted.test"})
    value = spec.model_dump(mode="json", by_alias=True)
    value["mcp"]["inputSchema"]["required"] = []
    with pytest.raises(ValidationError):
        type(spec).model_validate(value)


def test_skill_bytes_and_tamper_rejection() -> None:
    bundle = skill.local_bundle()
    assert bundle.spec.ref in skill.configured_agent().skills
    assert bundle.spec.git_commit is None  # Local verification, not a fake online install.
    with pytest.raises(ValueError):
        GitSkillInstaller().verify_bundle(
            VerifiedSkillBundle(spec=bundle.spec, files={**bundle.files, "run.py": b"tampered"})
        )


def test_missing_skill_dependencies_fail_before_sandbox() -> None:
    from agent_runtime.skills.script import SkillScriptTool

    class NeverRun:
        async def start(self, *args: object, **kwargs: object) -> None:
            raise AssertionError("Must not start a sandbox")

    with pytest.raises(ValueError, match="dependencies"):
        SkillScriptTool(
            bundle=skill.local_bundle(),
            executable_path="run.py",
            sandbox=NeverRun(),
            available_tools=frozenset(),
        )


@pytest.mark.asyncio
async def test_memory_keeps_correction_and_pending_work() -> None:
    result = await memory.demo()
    assert "800, not 900" in result["summary"] and "verify workshop B" in result["summary"]


def test_subagent_and_human_bindings() -> None:
    parent, child = subagents.agents()
    assert agent_config.ref(child.id) in parent.subagents and not child.tools
    task_id, allocation = uuid4(), uuid4()
    package = subagents.task(task_id, allocation, datetime.now(UTC) + timedelta(minutes=3))
    assert package.budget_allocation_id == allocation and package.task_id == task_id
    decision = human.approval_decision(uuid4(), "3", "approved")
    assert decision.idempotency_key == f"approval:{decision.approval_id}:3:approved"


def test_durable_help_does_not_require_host_install_or_configuration() -> None:
    args = durable.parser().parse_args(
        [
            "submit",
            "--session",
            str(uuid4()),
            "--key",
            "request-001",
            "--text",
            "Hello",
        ]
    )
    assert args.after == "0" and args.timeout == 600
    assert durable.parser().parse_args(["maintenance"]).command == "maintenance"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["applied", "up-to-date"])
async def test_repeat_runtime_migration_still_upgrades_host(monkeypatch, status: str) -> None:
    import agent_runtime.storage.migrate as migration

    upgraded = []

    async def execute(*args):
        return SimpleNamespace(status=status)

    async def upgrade(dsn: str, schema: str) -> None:
        upgraded.append((dsn, schema))

    monkeypatch.setattr(migration, "execute", execute)
    await durable.migrate("postgresql+asyncpg://unused/unused", "tutorial", upgrade)
    assert upgraded == [("postgresql+asyncpg://unused/unused", "tutorial")]


@pytest.mark.asyncio
async def test_stream_failure_awaits_result_reader_cleanup(monkeypatch) -> None:
    ready, cleaned = asyncio.Event(), asyncio.Event()

    class Handle:
        async def result(self, **kwargs):
            ready.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0.05)
                cleaned.set()

    async def fail_stream(*args):
        await ready.wait()
        raise ValueError("Stream rejected")

    monkeypatch.setattr(durable, "stream", fail_stream)
    with pytest.raises(ValueError, match="Stream rejected"):
        await durable.watch(Handle(), after="0", timeout=1)
    assert cleaned.is_set()

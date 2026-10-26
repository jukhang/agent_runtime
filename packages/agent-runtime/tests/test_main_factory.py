"""Host factory rejects unsupported declared bindings before opening any client."""

import hashlib
import json
from importlib.resources import files
from uuid import uuid4

import httpx
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from agent_runtime.agent import Agent
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.model import ModelConfig
from agent_runtime.contracts.references import CapabilityKey, CapabilityRef
from agent_runtime.contracts.registry import HookSpec, SkillSpec
from agent_runtime.contracts.scheduler import ClaimedWork
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.providers.openai_compatible import OpenAIAdapterProfile
from agent_runtime.skills.git import VerifiedSkillBundle
from agent_runtime.storage.admission import AdmissionAgent
from agent_runtime.storage.main_factory import (
    CONTROL_DECISIONS,
    MainAgentProfile,
    MainHostPorts,
    SqlMainHandlerFactory,
)
from agent_runtime.storage.tool_host import SqlToolHost
from agent_runtime.tool_hooks import PinnedToolHooks


def test_control_profile_only_offers_actions_supported_by_its_channel():
    schema = CONTROL_DECISIONS.json_schema(by_alias=True)
    assert schema["properties"]["kind"]["const"] == "finish"
    assert "AskUser" not in schema["$defs"]
    assert "Respond" not in schema["$defs"]
    assert "InvokeTools" not in schema["$defs"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "binding",
    [
        "knowledge",
        "subagents",
        "skills",
        "hooks",
        "policy",
        "context",
        "memory",
        "input_schema",
        "output_schema",
    ],
)
async def test_unbound_agent_capabilities_fail_before_execution(binding):
    ref = CapabilityRef(key=CapabilityKey(namespace="host", key="assistant"), revision="1")
    agent = Agent(id="assistant", name="Assistant", model=ref)
    agent = agent.model_copy(
        update={
            binding: (ref,)
            if binding in {"knowledge", "subagents", "skills", "hooks", "policy"}
            else ref
        }
    )
    revision_id = uuid4()
    installed = AdmissionAgent(ref, revision_id, agent, 100000)
    profile = MainAgentProfile(
        installed,
        ModelConfig(
            ref=ref,
            connection=ref,
            model_name="test",
            max_output_tokens=100,
            thinking="disabled",
            provider_options={},
        ),
        ref,
        ref,
    )
    async with httpx.AsyncClient() as client:
        with pytest.raises(ValueError, match="host binding"):
            SqlMainHandlerFactory(
                None,
                "agent_runtime",
                client=client,
                estimator=None,
                agents={("scope", revision_id): profile},
                ports=None,
                connections=None,
                capabilities=None,
                adapter=OpenAIAdapterProfile.qwen(),
                allowed_predicates=(),
            )


@pytest.mark.asyncio
async def test_main_worker_installs_pinned_skill_and_rejects_unbound_hook(monkeypatch):
    ref = CapabilityRef(key=CapabilityKey(namespace="host", key="assistant"), revision="1")
    skill_ref = CapabilityRef(key=CapabilityKey(namespace="host", key="skill"), revision="1")
    hook_ref = CapabilityRef(key=CapabilityKey(namespace="host", key="hook"), revision="1")
    content = b"# Verified instructions\nCheck the computed result.\n"
    file = {
        "path": "SKILL.md",
        "blobRef": "private-skill",
        "sizeBytes": len(content),
        "digest": hashlib.sha256(content).hexdigest(),
        "executable": False,
    }
    manifest = [{key: file[key] for key in ("path", "digest", "sizeBytes", "executable")}]
    skill = SkillSpec.model_validate(
        {
            "ref": skill_ref.model_dump(mode="json", by_alias=True),
            "source": skill_ref.model_dump(mode="json", by_alias=True),
            "gitCommit": "a" * 40,
            "bundleDigest": content_digest(manifest),
            "instructionPath": "SKILL.md",
            "files": [file],
            "requiredTools": [],
        }
    )
    bundle = VerifiedSkillBundle(skill, {"SKILL.md": content})
    actor = ActorContext(scope_id="scope", actor_id="owner", auth_revision="1", grants_ref="g")
    revision_id = uuid4()
    agent = Agent(id="assistant", name="Assistant", model=ref, skills=(skill_ref,))
    approval_port = object()
    from agent_runtime.storage.context_factory import SqlContextComponents

    context_port = SqlContextComponents(runtime=object(), access=object())
    fixture = json.loads(
        files("agent_runtime.contracts").joinpath("fixtures/scheduler.json").read_text()
    )
    work = ClaimedWork.model_validate(fixture["claimResults"][0])

    class Lease:
        def current(self):
            return work.lease

    def open_handler(factory):
        return factory.open(actor, work, Lease())

    class Extensions:
        async def skills(self, current, run_id, profile):
            assert current == actor and profile.installed.revision_id == revision_id
            assert run_id == work.lease.run_id
            return {skill_ref: bundle}

    class Ports:
        async def open(self, current, lease):
            assert current == actor
            return MainHostPorts(*([object()] * 7), approvals=approval_port, context=context_port)

    model = ModelConfig(
        ref=ref,
        connection=ref,
        model_name="test",
        max_output_tokens=100,
        thinking="disabled",
        provider_options={},
    )
    engine = create_async_engine("postgresql+asyncpg://unused:unused@127.0.0.1/unused")
    try:
        async with httpx.AsyncClient() as client:

            def factory(configured_agent, configured_ports=None):
                profile = MainAgentProfile(
                    AdmissionAgent(ref, revision_id, configured_agent, 100000), model, ref, ref
                )
                result = SqlMainHandlerFactory(
                    engine,
                    "agent_runtime",
                    client=client,
                    estimator=object(),
                    agents={(actor.scope_id, revision_id): profile},
                    ports=configured_ports or Ports(),
                    connections=object(),
                    capabilities=object(),
                    adapter=OpenAIAdapterProfile.qwen(),
                    allowed_predicates=(),
                    extensions=Extensions(),
                )

                async def selected_profile(current, run_id):
                    assert current == actor and run_id == work.lease.run_id
                    return profile

                monkeypatch.setattr(result, "_profile_for_run", selected_profile)
                return result

            handler = await open_handler(factory(agent))
            assert handler._approvals is approval_port
            assert handler._context is context_port
            assert handler._provider._inputs._context is context_port.runtime
            setup = handler._loader._agents[revision_id].setup
            assert setup._skills == {skill_ref: content.decode()}
            with pytest.raises(ValueError, match="hooks do not match"):
                await open_handler(factory(agent.model_copy(update={"hooks": (hook_ref,)})))

            async def accept(current, pinned, outcome):
                return None

            hook_spec = HookSpec(
                ref=hook_ref,
                handler=hook_ref,
                stage="before_execute",
                order=0,
                timeout_seconds=1,
                failure_policy="block",
                replay="pure",
            )

            class HookedPorts:
                def __init__(self, assembled=True, approved=True):
                    self.assembled = assembled
                    self.approved = approved

                async def open(self, current, lease):
                    async def authorize(db, invocation, refs):
                        return None

                    from agent_runtime.storage.tool_assembly import SqlToolRuntimeAssembly
                    from agent_runtime.storage.tool_capacity import SqlToolCapacity
                    from agent_runtime.storage.tool_execution_gate import SqlToolExecutionGate
                    from agent_runtime.storage.uow.capacity import SqlSystemCapacityStore

                    capacity = SqlToolCapacity(
                        SqlSystemCapacityStore(engine, "agent_runtime"), deployment_id="test"
                    )
                    host = SqlToolHost(
                        engine,
                        "agent_runtime",
                        specs=object(),
                        resources=object(),
                        schemas=object(),
                        authorize=authorize,
                        capacity=capacity,
                        execution_gate=SqlToolExecutionGate(
                            "agent_runtime",
                            approval_required=lambda invocation, spec, resources: False,
                        ),
                        hooks=(PinnedToolHooks((hook_spec,), {hook_ref: accept}),),
                    )
                    return MainHostPorts(
                        *([object()] * 4),
                        host,
                        object(),
                        object(),
                        SqlToolRuntimeAssembly(host, capacity) if self.assembled else None,
                        approvals=approval_port if self.approved else None,
                    )

            hooked = agent.model_copy(update={"hooks": (hook_ref,)})
            with pytest.raises(ValueError, match="runtime assembly"):
                await open_handler(factory(hooked, HookedPorts(assembled=False)))
            with pytest.raises(ValueError, match="approval service"):
                await open_handler(factory(hooked, HookedPorts(approved=False)))
            assert await open_handler(factory(hooked, HookedPorts()))
    finally:
        await engine.dispose()

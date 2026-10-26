"""Installed Skill resolution against real Run authority and Worker configuration."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from uuid import uuid4

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from test_storage_capability_registry import Authority, ref, registry_schema  # noqa: F401
from test_storage_uow import DSN, engine_for, run

from agent_runtime.agent import Agent
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.model import ModelConfig
from agent_runtime.contracts.registry import SkillSpec
from agent_runtime.contracts.requests import SubmitRequest, TextInput
from agent_runtime.contracts.scheduler import ClaimedWork, ClaimRequest, WorkerCapabilities
from agent_runtime.contracts.tools import ToolInvocation
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.providers.openai_compatible import OpenAIAdapterProfile
from agent_runtime.sandbox.runtime import ExecutionBinding, ExecutionStart
from agent_runtime.skills.git import VerifiedSkillBundle
from agent_runtime.storage.admission import AdmissionAgent, SqlAdmission
from agent_runtime.storage.agent_install import SqlAgentInstaller
from agent_runtime.storage.capability_registry import SqlCapabilityRegistry
from agent_runtime.storage.main_budget import SqlMainBudgetReader
from agent_runtime.storage.main_factory import (
    MainAgentProfile,
    MainHostPorts,
    SqlMainHandlerFactory,
)
from agent_runtime.storage.scheduler import SqlScheduler
from agent_runtime.storage.skill_access import SqlRunSkillAccess
from agent_runtime.storage.skill_bundles import SqlSkillBundleStore
from agent_runtime.storage.skill_execution import SqlSkillExecutionAuthorizer
from agent_runtime.storage.skill_extensions import SqlSkillExtensionResolver
from agent_runtime.worker import LeaseHandle

pytestmark = pytest.mark.skipif(not DSN, reason="AGENT_RUNTIME_TEST_DSN is not configured")


def test_run_skill_authority_and_current_agent_worker_assembly(
    registry_schema: str,  # noqa: F811
) -> None:
    async def exercise() -> None:
        engine = engine_for(registry_schema, search_path=False)
        actor = ActorContext(scope_id="skill-run", actor_id="owner", auth_revision="1",
                             grants_ref="owner")
        source = b"# Skill\nCheck units and cite evidence.\n"
        entry = {
            "path": "SKILL.md", "blobRef": "install-source",
            "digest": hashlib.sha256(source).hexdigest(), "sizeBytes": len(source),
            "executable": False,
        }
        spec = SkillSpec.model_validate({
            "ref": ref("run-skill"), "source": ref("source"), "gitCommit": "a" * 40,
            "bundleDigest": content_digest([
                {key: entry[key] for key in ("path", "digest", "sizeBytes", "executable")}
            ]), "instructionPath": "SKILL.md", "files": [entry], "requiredTools": [],
        })
        registry = SqlCapabilityRegistry(engine, registry_schema, authority=Authority())
        store = SqlSkillBundleStore(
            engine, registry_schema, registry=registry,
            access=SqlRunSkillAccess(engine, registry_schema),
        )
        model = ModelConfig(
            ref=ref("model"), connection=ref("connection"), model_name="test",
            max_output_tokens=100, thinking="disabled", provider_options={},
        )
        configured = Agent(id="skill-assistant", name="Skill assistant", model=model.ref,
                           skills=(spec.ref,))
        unrelated = Agent(id="other-assistant", name="Other assistant", model=model.ref,
                          skills=(ref("not-installed"),))
        installer = SqlAgentInstaller(engine, registry_schema, namespace="host")
        try:
            await store.install(actor, VerifiedSkillBundle(spec, {"SKILL.md": source}))
            await registry.publish(actor, spec.ref)
            profiles = {}
            for agent in (configured, unrelated):
                revision_id = await installer.install(actor.scope_id, ref(agent.id), agent)
                installed = AdmissionAgent(ref(agent.id), revision_id, agent, 100000)
                profiles[(actor.scope_id, revision_id)] = MainAgentProfile(
                    installed, model, ref("profile"), ref("prompt")
                )
            session_id = uuid4()
            async with engine.begin() as db:
                await db.execute(sa.text(
                    f"INSERT INTO {registry_schema}.session "
                    "(id,scope_id,owner_actor_id,title_source,event_seq,auth_revision) "
                    "VALUES (:id,:scope,:actor,'none',0,1)"
                ), {"id": session_id, "scope": actor.scope_id, "actor": actor.actor_id})

            async def authorized(*args):
                return True

            admission = SqlAdmission(
                engine, registry_schema, authorize=authorized,
                agents={(actor.scope_id, ref(p.installed.agent.id)): p.installed
                        for p in profiles.values()},
            )
            accepted = await admission.accept(actor, SubmitRequest(
                session_id=session_id, agent=ref(configured.id), idempotency_key="skill-first",
                inputs=(TextInput(kind="text", text="Use the installed Skill."),),
            ))
            claim = await SqlScheduler(engine, registry_schema).claim(ClaimRequest(
                scope_id=actor.scope_id, lease_seconds=60,
                worker=WorkerCapabilities(worker_id="skill-worker", runtime_revision=ref("runtime"),
                                          capabilities=(ref(configured.id),)),
            ))
            assert isinstance(claim, ClaimedWork) and claim.lease.run_id == accepted.run_id

            class Ports:
                async def open(self, current, lease):
                    return MainHostPorts(*([object()] * 7))

            async with httpx.AsyncClient() as client:
                factory = SqlMainHandlerFactory(
                    engine, registry_schema, client=client, estimator=object(), agents=profiles,
                    ports=Ports(), connections=object(), capabilities=object(),
                    adapter=OpenAIAdapterProfile.qwen(), allowed_predicates=(),
                    extensions=SqlSkillExtensionResolver(store),
                )
                handler = await factory.open(actor, claim, LeaseHandle(claim.lease))
                assert len(handler._loader._agents) == 1
                setup = next(iter(handler._loader._agents.values())).setup
                assert setup._skills == {spec.ref: source.decode()}
                loaded = await handler._loader.load(claim, LeaseHandle(claim.lease))
                budget = await SqlMainBudgetReader(
                    engine, registry_schema, actor=actor, lease=claim.lease,
                    allocation_id=claim.budget_allocation_id,
                ).read(loaded.state)
                resolved = await setup.resolve(loaded.state, loaded.inputs, budget)
                assert source.decode() not in resolved.context_request.prompt_text
                assert "Check units and cite evidence." in resolved.context_request.user_input

            with pytest.raises(AgentRuntimeError, match="not bound"):
                await store.load(actor, accepted.run_id, ref("not-installed"))
            with pytest.raises(AgentRuntimeError):
                await store.load(actor.model_copy(update={"scope_id": "other-scope"}),
                                 accepted.run_id, spec.ref)
            fixture = json.loads(files("agent_runtime.contracts").joinpath(
                "fixtures/tools.json"
            ).read_text())
            invocation = ToolInvocation.model_validate({
                **fixture["invocation"], "actor": actor, "runId": accepted.run_id,
                "sessionId": session_id,
            })
            execution = ExecutionStart(
                ExecutionBinding(actor.scope_id, uuid4(), 1, 1,
                                 "python@sha256:" + "a" * 64, "sandbox"),
                "python report.py", "/workspace", 30,
                datetime.now(UTC) + timedelta(minutes=1),
            )

            async def authorized_execution(db, current, request):
                assert current == invocation and request == execution

            guard = SqlSkillExecutionAuthorizer(
                registry_schema, skill=spec.ref, authorize=authorized_execution
            )
            async with AsyncSession(engine) as db, db.begin():
                await guard(db, invocation, execution)
            await registry.revoke(actor, spec.ref)
            async with AsyncSession(engine) as db, db.begin():
                with pytest.raises(AgentRuntimeError, match="revoked"):
                    await guard(db, invocation, execution)
            async with engine.begin() as db:
                await db.execute(sa.text(
                    f"UPDATE {registry_schema}.run SET authority_revision=2 WHERE id=:id"
                ), {"id": accepted.run_id})
            with pytest.raises(AgentRuntimeError, match="authority revision"):
                await store.load(actor, accepted.run_id, spec.ref)
        finally:
            await engine.dispose()

    run(exercise)

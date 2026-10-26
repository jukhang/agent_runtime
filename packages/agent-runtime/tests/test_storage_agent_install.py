"""Installation and actual-input Setup against isolated PostgreSQL schemas."""

import asyncio
from uuid import uuid4

import pytest

pytest.importorskip("sqlalchemy")

import sqlalchemy as sa
from test_storage_uow import DSN, engine_for, run, schema  # noqa: F401

from agent_runtime.agent import Agent
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.model import ModelConfig
from agent_runtime.contracts.references import CapabilityKey, CapabilityRef
from agent_runtime.contracts.requests import SubmitRequest, TextInput
from agent_runtime.contracts.scheduler import ClaimedWork, ClaimRequest, WorkerCapabilities
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.admission import AdmissionAgent, SqlAdmission
from agent_runtime.storage.agent_install import SqlAgentInstaller
from agent_runtime.storage.main_budget import SqlMainBudgetReader
from agent_runtime.storage.main_run_loader import MainAgentBinding, SqlMainRunLoader
from agent_runtime.storage.main_setup import SqlMainLoopSetup
from agent_runtime.storage.scheduler import SqlScheduler
from agent_runtime.worker import LeaseHandle

pytestmark = pytest.mark.skipif(not DSN, reason="AGENT_RUNTIME_TEST_DSN is not configured")


def capability(key: str, revision: str = "1", namespace: str = "host") -> CapabilityRef:
    return CapabilityRef(key=CapabilityKey(namespace=namespace, key=key), revision=revision)


@pytest.mark.parametrize("case", ["immutable", "namespace", "setup"])
def test_installed_agent_setup(schema: str, case: str) -> None:  # noqa: F811
    async def exercise() -> None:
        engine = engine_for(schema, search_path=False)
        actor = ActorContext(
            scope_id="scope-a", actor_id="owner", auth_revision="1", grants_ref="host:owner"
        )
        ref = capability("assistant")
        model = ModelConfig(
            ref=capability("model"),
            connection=capability("connection"),
            model_name="qwen3.6-27b",
            max_output_tokens=1024,
            thinking="disabled",
            provider_options={},
        )
        agent = Agent(
            id="assistant",
            name="Assistant",
            model=model.ref,
            instructions=("Answer accurately.",),
            add_datetime_to_context=True,
            timezone="Asia/Shanghai",
        )
        installer = SqlAgentInstaller(engine, schema, namespace="host")
        try:
            ids = await asyncio.gather(
                *(installer.install(actor.scope_id, ref, agent) for _ in range(4))
            )
            assert len(set(ids)) == 1
            if case == "immutable":
                with pytest.raises(AgentRuntimeError, match="immutable"):
                    await installer.install(
                        actor.scope_id, ref, agent.model_copy(update={"name": "Changed"})
                    )
                newer = await installer.install(
                    actor.scope_id,
                    capability("assistant", "2"),
                    agent.model_copy(update={"name": "Newer"}),
                )
                await installer.install(actor.scope_id, ref, agent)
                async with engine.connect() as db:
                    assert (
                        await db.execute(
                            sa.text(f"SELECT published_revision_id FROM {schema}.agent")
                        )
                    ).scalar_one() == newer
                return
            if case == "namespace":
                other = SqlAgentInstaller(engine, schema, namespace="other")
                with pytest.raises(AgentRuntimeError, match="namespace"):
                    await other.install(
                        actor.scope_id, capability("assistant", namespace="other"), agent
                    )
                return
            session_id = uuid4()
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f"INSERT INTO {schema}.session "
                        "(id,scope_id,owner_actor_id,title_source,event_seq,auth_revision) "
                        "VALUES (:id,'scope-a','owner','none',0,1)"
                    ),
                    {"id": session_id},
                )

            async def authorized(*args: object) -> bool:
                return True

            admission = SqlAdmission(
                engine,
                schema,
                authorize=authorized,
                agents={(actor.scope_id, ref): AdmissionAgent(ref, ids[0], agent, 100000)},
            )
            first = await admission.accept(
                actor,
                SubmitRequest(
                    session_id=session_id,
                    agent=ref,
                    idempotency_key="first",
                    inputs=(TextInput(kind="text", text="My topic is water."),),
                ),
            )
            await admission.accept(
                actor,
                SubmitRequest(
                    session_id=session_id,
                    agent=ref,
                    idempotency_key="future",
                    inputs=(TextInput(kind="text", text="FUTURE SECRET"),),
                ),
            )
            claim = await SqlScheduler(engine, schema).claim(
                ClaimRequest(
                    scope_id=actor.scope_id,
                    lease_seconds=60,
                    worker=WorkerCapabilities(
                        worker_id="setup-test", runtime_revision=ref, capabilities=(ref,)
                    ),
                )
            )
            assert isinstance(claim, ClaimedWork)
            assert claim.lease.run_id == first.run_id
            setup = SqlMainLoopSetup(
                engine,
                schema,
                actor=actor,
                agent_ref=ref,
                agent=agent,
                model=model,
                profile=capability("profile"),
                prompt=capability("prompt"),
            )
            loaded = await SqlMainRunLoader(
                engine, schema, actor=actor, agents={ids[0]: MainAgentBinding(ref, setup)}
            ).load(claim, LeaseHandle(claim.lease))
            budget = await SqlMainBudgetReader(
                engine,
                schema,
                actor=actor,
                lease=claim.lease,
                allocation_id=claim.budget_allocation_id,
            ).read(loaded.state)
            resolved = await setup.resolve(loaded.state, loaded.inputs, budget)
            assert resolved.mode == "native"
            assert resolved.context_request.user_input == "My topic is water."
            assert "Current time:" in resolved.context_request.prompt_text
            assert [r.resource_id for r in resolved.context_request.resource_refs] == [
                str(first.turn_id)
            ]
            invalid = SqlMainLoopSetup(
                engine,
                schema,
                actor=actor.model_copy(update={"actor_id": "intruder"}),
                agent_ref=ref,
                agent=agent,
                model=model,
                profile=ref,
                prompt=ref,
            )
            with pytest.raises(AgentRuntimeError):
                await invalid.resolve(loaded.state, loaded.inputs, budget)
        finally:
            await engine.dispose()

    run(exercise)

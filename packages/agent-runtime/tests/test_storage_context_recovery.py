"""Real SQL context reduction retains explicit resources and immutable snapshots."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

pytest.importorskip("sqlalchemy")

import sqlalchemy as sa
from test_context_runtime import Estimator
from test_storage_agent_install import capability
from test_storage_uow import DSN, engine_for, run, schema  # noqa: F401

from agent_runtime.agent import Agent
from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope
from agent_runtime.contracts.loop import AppliedInputs, BudgetCounter, BudgetView, RunState
from agent_runtime.contracts.model import ModelConfig, ProviderRequest
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.requests import ResourceInput, SubmitRequest, TextInput
from agent_runtime.contracts.results import Usage
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.provider import AttemptTerminal
from agent_runtime.storage.admission import AdmissionAgent, SqlAdmission
from agent_runtime.storage.agent_install import SqlAgentInstaller
from agent_runtime.storage.context_factory import build_sql_context_components
from agent_runtime.storage.main_setup import SqlMainLoopSetup
from agent_runtime.storage.model_attempts import SqlModelAttemptStore

pytestmark = pytest.mark.skipif(not DSN, reason="AGENT_RUNTIME_TEST_DSN is not configured")


@pytest.mark.parametrize("explicit", (False, True))
def test_sql_context_limit_rebuilds_snapshot_and_preserves_required_history(
    schema: str,  # noqa: F811
    explicit: bool,
) -> None:
    async def exercise() -> None:
        engine = engine_for(schema, search_path=False)
        actor = ActorContext(
            scope_id="scope-a", actor_id="owner", auth_revision="1", grants_ref="host:owner"
        )
        ref, model_ref = capability("assistant"), capability("model")
        agent = Agent(id="assistant", name="Assistant", model=model_ref)
        model = ModelConfig(
            ref=model_ref,
            connection=model_ref,
            model_name="test",
            max_output_tokens=512,
            thinking="disabled",
            provider_options={},
        )
        session_id = uuid4()

        async def authorize(*args):
            return True

        try:
            revision = await SqlAgentInstaller(engine, schema, namespace="host").install(
                actor.scope_id, ref, agent
            )
            admission = SqlAdmission(
                engine,
                schema,
                authorize=authorize,
                agents={(actor.scope_id, ref): AdmissionAgent(ref, revision, agent, 100000)},
            )
            async with engine.begin() as db:
                await db.execute(
                    sa.text(
                        f"INSERT INTO {schema}.session "
                        "(id,scope_id,owner_actor_id,title_source,event_seq,auth_revision) "
                        "VALUES (:id,'scope-a','owner','none',0,1)"
                    ),
                    {"id": session_id},
                )
            old = await admission.accept(
                actor,
                SubmitRequest(
                    session_id=session_id,
                    agent=ref,
                    idempotency_key="old",
                    inputs=(TextInput(kind="text", text="x" * 14000),),
                ),
            )
            old_ref = ResourceRef(
                resource_type="turn_input",
                resource_id=str(old.turn_id),
                resource_version="0",
                scope=ResourceScope(
                    scope_id=actor.scope_id, session_id=str(session_id), owner_id=actor.actor_id
                ),
            )
            parts = (TextInput(kind="text", text="Explain the result."),)
            submitted = SubmitRequest(
                session_id=session_id,
                agent=ref,
                idempotency_key="current",
                inputs=(*parts, ResourceInput(kind="resource", resource_ref=old_ref))
                if explicit
                else parts,
            )
            current = await admission.accept(actor, submitted)
            async with engine.connect() as db:
                row = (
                    (
                        await db.execute(
                            sa.text(
                                f"SELECT r.*,c.state_ref FROM {schema}.run r "
                            f"JOIN {schema}.checkpoint c ON c.id=r.checkpoint_id "
                            "WHERE r.id=:run"
                            ),
                            {"run": current.run_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                allocation = (
                    await db.execute(
                        sa.text(
                            f"SELECT id FROM {schema}.budget_allocation WHERE root_run_id=:run"
                        ),
                        {"run": current.run_id},
                    )
                ).scalar_one()
            state = RunState(
                scope_id=actor.scope_id,
                session_id=session_id,
                run_id=current.run_id,
                state_version=str(row["state_version"]),
                checkpoint_ref=row["state_ref"],
                agent=ref,
                status=RunStatus.RUNNING,
                desired_control=None,
                cancel_version="0",
                pending_input_seq="0",
                applied_input_seq="0",
            )
            inputs = AppliedInputs(
                run_id=current.run_id,
                checkpoint_ref=state.checkpoint_ref,
                previous_seq="0",
                applied_seq="0",
                messages=(),
            )
            counter = BudgetCounter(limit="100000", spent="0", reserved="0", unknown_reserved="0")
            budget = BudgetView(
                allocation_id=allocation,
                revision="1",
                model_calls=counter,
                tool_calls=counter,
                tokens=counter,
                deadline_at=datetime.now(UTC) + timedelta(minutes=5),
            )
            setup = SqlMainLoopSetup(
                engine,
                schema,
                actor=actor,
                agent_ref=ref,
                agent=agent,
                model=model,
                profile=capability("profile"),
                prompt=capability("prompt"),
                max_input_tokens=24000,
            )
            context = build_sql_context_components(
                engine, schema, estimator=Estimator(), optional_history=True
            ).runtime
            initial = await setup.resolve(state, inputs, budget)
            before = await context.prepare(initial.context_request)
            old_source = next(
                s for s in before.sources if s.resource.resource_id == str(old.turn_id)
            )
            assert old_source.required == explicit
            request = ProviderRequest(
                scope_id=actor.scope_id,
                session_id=session_id,
                run_id=current.run_id,
                attempt_id=uuid4(),
                context_snapshot_id=before.snapshot_id,
                context_version=before.context_version,
                input_ref=before.payload_ref,
                input_digest=before.content_hash,
                estimated_input_tokens=before.estimated_input_tokens,
                budget_allocation_id=allocation,
                deadline_at=budget.deadline_at,
                model=model,
                tools=(),
                output_schema=None,
            )
            attempts = SqlModelAttemptStore(engine, schema)
            now = datetime.now(UTC)
            await attempts.begin(request, started_at=now)
            await attempts.finish(
                request,
                AttemptTerminal(
                    status="failed",
                    finish_reason=None,
                    usage=Usage(
                        status="complete",
                        reason="provider_reported",
                        input_tokens="0",
                        output_tokens="0",
                        total_tokens="0",
                    ),
                    failure=FailureEnvelope(
                        code="CONTEXT_LIMIT",
                        category="provider",
                        retryable=True,
                        public_message="Injected context rejection with explicitly reported usage.",
                        attempt_id=str(request.attempt_id),
                    ),
                    finished_at=now,
                ),
            )
            reduced = await setup.resolve(state, inputs, budget)
            assert reduced.context_request.budget.max_input_tokens == 12000
            if explicit:
                with pytest.raises(AgentRuntimeError) as error:
                    await context.prepare(reduced.context_request)
                assert error.value.failure.code == "BUDGET_EXHAUSTED"
            else:
                after = await context.prepare(reduced.context_request)
                assert after.snapshot_id != before.snapshot_id
                assert after.content_hash != before.content_hash
                omitted = next(
                    s for s in after.sources if s.resource.resource_id == str(old.turn_id)
                )
                assert omitted.disposition == "omitted"
                assert any(s.required and s.disposition == "included" for s in after.sources)
            assert (await context.load(actor.scope_id, before.snapshot_id))[0] == before
        finally:
            await engine.dispose()

    run(exercise)

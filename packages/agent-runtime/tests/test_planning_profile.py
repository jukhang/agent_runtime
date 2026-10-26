"""A permissive or schema-ignoring Provider cannot invent planning bindings."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from test_decision_validation import facts as loop_facts

from agent_runtime.contracts.decisions import (
    AskUser,
    AskUserPayload,
    Delegate,
    DelegatePayload,
    Finish,
    FinishPayload,
    UpdatePlan,
    UpdatePlanPayload,
    Wait,
    WaitPayload,
)
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.references import (
    CapabilityKey,
    CapabilityRef,
    ResourceRef,
    ResourceScope,
)
from agent_runtime.contracts.tasks import TaskPackage, WaitSpec
from agent_runtime.contracts.views import PlanStep
from agent_runtime.storage.planning import (
    PLAN_REVIEW_SCHEMA,
    PlanningFacts,
    PlanningOptions,
    SqlPlanningSetupFactory,
    _PlanningSetup,
)


def ref(key):
    return CapabilityRef(key=CapabilityKey(namespace="test", key=key), revision="1")


def bindings():
    child = TaskPackage(
        task_id=uuid4(),
        target_agent=ref("child"),
        objective="Verify the calculation",
        constraints=(),
        acceptance_spec_ref="test:check:1",
        output_schema_ref="text",
        resource_refs=(),
        budget_allocation_id=uuid4(),
        deadline_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    wait = WaitSpec(
        kind="timer",
        subject_id="registered-timer",
        deadline_at=child.deadline_at,
        resume_predicate=ref("timer"),
    )
    values = PlanningFacts(
        uuid4(), "0", None, (uuid4(),), (uuid4(),), PlanningOptions((child,), (wait,))
    )
    return values, loop_facts()[1]


def test_plan_identifiers_version_steps_and_evidence_are_host_bound():
    facts, snapshot = bindings()
    action = UpdatePlan(
        kind="update_plan",
        payload=UpdatePlanPayload(
            plan_id=facts.plan_id,
            expected_revision="0",
            steps=(
                PlanStep(
                    id=facts.step_ids[0],
                    parent_step_id=None,
                    label="Calculate",
                    status="pending",
                    evidence_refs=(),
                    run_refs=(),
                ),
            ),
        ),
    )
    facts.validate(action, snapshot, initial=True)
    for changes in (
        {"plan_id": uuid4()},
        {"expected_revision": "1"},
        {"steps": (action.payload.steps[0].model_copy(update={"id": uuid4()}),)},
    ):
        with pytest.raises(AgentRuntimeError):
            facts.validate(
                action.model_copy(update={"payload": action.payload.model_copy(update=changes)}),
                snapshot,
                initial=True,
            )


def test_delegate_keeps_task_budget_agent_deadline_and_resources_pinned():
    facts, snapshot = bindings()
    candidate = facts.options.delegations[0]
    action = Delegate(
        kind="delegate",
        payload=DelegatePayload(
            task_package=candidate.model_copy(
                update={"objective": "Independently verify the same subtask"}
            )
        ),
    )
    facts.validate(action, snapshot, initial=False)
    for field, value in (
        ("task_id", uuid4()),
        ("budget_allocation_id", uuid4()),
        ("target_agent", ref("other")),
        ("deadline_at", candidate.deadline_at + timedelta(days=1)),
    ):
        tampered = action.model_copy(
            update={
                "payload": DelegatePayload(
                    task_package=action.payload.task_package.model_copy(update={field: value})
                )
            }
        )
        with pytest.raises(AgentRuntimeError):
            facts.validate(tampered, snapshot, initial=False)
    with pytest.raises(AgentRuntimeError):
        facts.validate(action, snapshot, initial=True)


def test_waits_and_final_answers_require_current_registered_facts():
    facts, snapshot = bindings()
    wait = Wait(kind="wait", payload=WaitPayload(wait_spec=facts.options.waits[0]))
    facts.validate(wait, snapshot, initial=False)
    with pytest.raises(AgentRuntimeError):
        facts.validate(
            wait.model_copy(
                update={
                    "payload": WaitPayload(
                        wait_spec=wait.payload.wait_spec.model_copy(
                            update={"subject_id": "invented"}
                        )
                    )
                }
            ),
            snapshot,
            initial=False,
        )

    finish = Finish(
        kind="finish",
        payload=FinishPayload(outcome="success", evidence_refs=(), final_item_ids=facts.final_ids),
    )
    with pytest.raises(AgentRuntimeError):
        facts.validate(finish, snapshot, initial=False)
    closed = replace(facts, complete=True, successful=True)
    closed.validate(finish, snapshot, initial=False)
    with pytest.raises(AgentRuntimeError):
        closed.validate(
            finish.model_copy(
                update={"payload": finish.payload.model_copy(update={"final_item_ids": (uuid4(),)})}
            ),
            snapshot,
            initial=False,
        )


@pytest.mark.asyncio
async def test_planning_schema_pins_exact_evidence_and_excludes_unavailable_actions(monkeypatch):
    facts, snapshot = bindings()
    factory = SqlPlanningSetupFactory(None, "test")

    async def current(scope, run):
        return replace(facts, options=PlanningOptions())

    monkeypatch.setattr(factory, "facts", current)
    request = SimpleNamespace(
        scope_id=snapshot.scope_id, run_id=snapshot.run_id, output_schema=PLAN_REVIEW_SCHEMA
    )
    response = await factory.output_format(request, snapshot)
    schema = response["json_schema"]["schema"]
    assert set(schema["discriminator"]["mapping"]) == {"update_plan", "ask_user"}
    evidence = [
        source.resource.model_dump(mode="json", by_alias=True)
        for source in snapshot.sources
        if source.disposition in {"included", "summarized"}
    ]
    assert schema["$defs"]["PlanStep"]["properties"]["evidenceRefs"]["items"] == (
        {"enum": evidence} if evidence else False
    )

    async def complete(scope, run):
        return replace(facts, complete=True, successful=True)

    monkeypatch.setattr(factory, "facts", complete)
    schema = (await factory.output_format(request, snapshot))["json_schema"]["schema"]
    assert "finish" in schema["discriminator"]["mapping"]
    assert schema["$defs"]["FinishPayload"]["properties"]["finalItemIds"] == {
        "type": "array",
        "const": [str(item) for item in facts.final_ids],
    }


def final_facts():
    facts, snapshot = bindings()
    return replace(
        facts, revision="5", complete=True, successful=True, options=PlanningOptions()
    ), snapshot


@pytest.mark.asyncio
async def test_final_review_schema_and_validator_only_allow_finish(monkeypatch):
    facts, snapshot = final_facts()
    factory = SqlPlanningSetupFactory(None, "test")

    async def current(scope, run):
        return facts

    monkeypatch.setattr(factory, "facts", current)
    request = SimpleNamespace(
        scope_id=snapshot.scope_id, run_id=snapshot.run_id, output_schema=PLAN_REVIEW_SCHEMA
    )
    schema = (await factory.output_format(request, snapshot))["json_schema"]["schema"]
    assert schema["oneOf"] == [{"$ref": "#/$defs/Finish"}]
    assert schema["discriminator"]["mapping"] == {"finish": "#/$defs/Finish"}
    for outcome in ("success", "partial"):
        facts.validate(
            Finish(
                kind="finish",
                payload=FinishPayload(
                    outcome=outcome, final_item_ids=facts.final_ids, evidence_refs=()
                ),
            ),
            snapshot,
            initial=False,
        )
    for action in (
        UpdatePlan(
            kind="update_plan",
            payload=UpdatePlanPayload(
                plan_id=facts.plan_id, expected_revision=facts.revision, steps=()
            ),
        ),
        AskUser(
            kind="ask_user",
            payload=AskUserPayload(question="Recheck?", required_facts=()),
        ),
    ):
        with pytest.raises(AgentRuntimeError, match="Finish completion review"):
            facts.validate(action, snapshot, initial=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "boundary",
    ["initial", "incomplete", "failed_step", "no_answer", "reply", "pending", "delegate", "wait"],
)
async def test_finish_only_does_not_cross_unfinished_or_new_input_boundary(monkeypatch, boundary):
    facts, snapshot = final_facts()
    options = bindings()[0].options
    changes = {
        "initial": {"revision": "0"},
        "incomplete": {"complete": False},
        "failed_step": {"successful": False},
        "no_answer": {"final_ids": ()},
        "reply": {"after_reply": True},
        "pending": {"input_pending": True},
        "delegate": {"options": PlanningOptions(delegations=options.delegations)},
        "wait": {"options": PlanningOptions(waits=options.waits)},
    }
    facts = replace(facts, **changes[boundary])
    assert not facts.finish_only
    factory = SqlPlanningSetupFactory(None, "test")

    async def current(scope, run):
        return facts

    monkeypatch.setattr(factory, "facts", current)
    request = SimpleNamespace(
        scope_id=snapshot.scope_id, run_id=snapshot.run_id, output_schema=PLAN_REVIEW_SCHEMA
    )
    schema = (await factory.output_format(request, snapshot))["json_schema"]["schema"]
    assert "update_plan" in schema["discriminator"]["mapping"]
    facts.validate(
        UpdatePlan(
            kind="update_plan",
            payload=UpdatePlanPayload(
                plan_id=facts.plan_id, expected_revision=facts.revision, steps=()
            ),
        ),
        snapshot,
        initial=boundary == "initial",
    )


@pytest.mark.asyncio
async def test_final_setup_enters_structured_review_without_selecting_success(monkeypatch):
    from test_agent_loop import build

    loop, state, inputs, budget, _, _ = build("answer")
    facts, snapshot = final_facts()
    factory = SqlPlanningSetupFactory(None, "test")

    async def current(scope, run):
        return facts

    monkeypatch.setattr(factory, "facts", current)
    setup = await _PlanningSetup(loop._setup, factory).resolve(state, inputs, budget)
    assert setup.mode == "structured" and setup.output_schema == PLAN_REVIEW_SCHEMA
    assert "Select Finish only" in setup.context_request.prompt_text
    assert "partial if incomplete" in setup.context_request.prompt_text
    setup.validate_action(
        Finish(
            kind="finish",
            payload=FinishPayload(
                outcome="partial", final_item_ids=facts.final_ids, evidence_refs=()
            ),
        ),
        snapshot,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["incomplete", "no_answer", "after_reply"])
async def test_native_execution_protocol_preserves_prompt_plan_and_capabilities(
    monkeypatch, boundary
):
    from test_agent_loop import build

    loop, state, inputs, budget, _, _ = build("tool")
    base = await loop._setup.resolve(state, inputs, budget)
    facts, _ = final_facts()
    scope = ResourceScope(scope_id=state.scope_id, session_id=str(state.session_id))
    plan_ref = ResourceRef(
        resource_type="plan_revision",
        resource_id=str(uuid4()),
        resource_version=facts.revision,
        scope=scope,
    )
    existing = ResourceRef(
        resource_type="turn_input",
        resource_id=str(uuid4()),
        resource_version="0",
        scope=scope,
    )
    refs = (existing, plan_ref) if boundary == "no_answer" else (existing,)
    request = base.context_request.model_copy(update={"resource_refs": refs})
    base = replace(base, context_request=request)
    changes = {
        "incomplete": {"complete": False, "successful": False},
        "no_answer": {"final_ids": ()},
        "after_reply": {"after_reply": True},
    }
    facts = replace(facts, plan_ref=plan_ref, **changes[boundary])

    class Base:
        async def resolve(self, current, applied, available):
            assert (current, applied, available) == (state, inputs, budget)
            return base

    factory = SqlPlanningSetupFactory(None, "test")

    async def current(scope_id, run_id):
        assert (scope_id, run_id) == (state.scope_id, state.run_id)
        return facts

    monkeypatch.setattr(factory, "facts", current)
    setup = await _PlanningSetup(Base(), factory).resolve(state, inputs, budget)
    actual = setup.context_request
    assert setup.mode == "native" and setup.model == base.model
    assert setup.output_schema is None and setup.validate_action is None
    assert actual.prompt_text.startswith(request.prompt_text + "\n")
    assert "already been persisted by Runtime" in actual.prompt_text
    assert "later structured review stage" in actual.prompt_text
    assert "Do not ask the user for internal plan interfaces or permissions" in actual.prompt_text
    assert "AskUser remains available for genuinely missing business facts" in actual.prompt_text
    assert actual.resource_refs == (existing, plan_ref)
    assert actual.tools == request.tools and actual.tools
    assert actual.actor == request.actor
    assert actual.model_dump(exclude={"prompt_text", "resource_refs"}) == request.model_dump(
        exclude={"prompt_text", "resource_refs"}
    )

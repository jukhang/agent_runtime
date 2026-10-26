"""Final item structure is bounded before staging, without rewriting attempt audit."""

from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from jsonschema import Draft202012Validator
from test_agent_loop import build
from test_decision_validation import NOW
from test_decision_validation import facts as loop_facts
from test_planning_profile import final_facts
from test_provider_invoker import Attempts, capabilities

from agent_runtime.contracts.decisions import Finish, FinishPayload
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.provider import ContentDelta, ProviderFinish
from agent_runtime.decision_validation import validate_candidate_decision
from agent_runtime.provider import ProviderInvoker
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.model_retry import propose_model_retry
from agent_runtime.storage.planning import PLAN_REVIEW_SCHEMA, SqlPlanningSetupFactory
from agent_runtime.storage.uow.answers import commit_answer


def candidate(ids, *, outcome="partial"):
    return Finish(
        kind="finish",
        payload=FinishPayload(outcome=outcome, final_item_ids=ids, evidence_refs=()),
    )


@pytest.mark.asyncio
async def test_planning_schema_binds_the_complete_ordered_content_bundle(monkeypatch):
    facts, snapshot = final_facts()
    facts = replace(facts, final_ids=(uuid4(), uuid4(), uuid4()))
    factory = SqlPlanningSetupFactory(None, "test")

    async def current(*args):
        return facts

    monkeypatch.setattr(factory, "facts", current)
    response = await factory.output_format(
        SimpleNamespace(
            scope_id=snapshot.scope_id,
            run_id=snapshot.run_id,
            output_schema=PLAN_REVIEW_SCHEMA,
        ),
        snapshot,
    )
    schema = response["json_schema"]["schema"]
    ids = schema["$defs"]["FinishPayload"]["properties"]["finalItemIds"]
    assert ids == {"type": "array", "const": [str(item) for item in facts.final_ids]}
    assert "uniqueItems" not in str(schema)
    validator = Draft202012Validator(schema)
    for invalid in ((), (facts.final_ids[0],) * 2, facts.final_ids + (uuid4(),), (uuid4(),)):
        assert not validator.is_valid(candidate(invalid).model_dump(mode="json", by_alias=True))
    # Planning owns a publication bundle, unlike the generic/custom Finish contract.
    for subset in ((facts.final_ids[1],), facts.final_ids[::2], facts.final_ids[::-1]):
        for outcome in ("success", "partial"):
            action = candidate(subset, outcome=outcome)
            assert not validator.is_valid(action.model_dump(mode="json", by_alias=True))
            with pytest.raises(AgentRuntimeError) as exc:
                facts.validate(action, snapshot, initial=False)
            assert exc.value.failure.code == "INCOMPLETE_MODEL_OUTPUT"
            assert not exc.value.failure.retryable
    for outcome in ("success", "partial"):
        action = candidate(facts.final_ids, outcome=outcome)
        assert validator.is_valid(action.model_dump(mode="json", by_alias=True))
        facts.validate(action, snapshot, initial=False)


@pytest.mark.parametrize("outcome", ["success", "partial"])
def test_generic_custom_finish_still_accepts_a_unique_subset(outcome):
    state, snapshot, budget = loop_facts()
    bundle = (uuid4(), uuid4(), uuid4())
    for subset in ((bundle[1],), bundle[::2], bundle[::-1]):
        action = candidate(subset, outcome=outcome)
        assert Draft202012Validator(Finish.model_json_schema(by_alias=True)).is_valid(
            action.model_dump(mode="json", by_alias=True)
        )
        assert validate_candidate_decision(state, snapshot, budget, action, now=NOW) == action


@pytest.mark.asyncio
async def test_empty_bundle_does_not_open_finish(monkeypatch):
    facts, snapshot = final_facts()
    facts = replace(facts, final_ids=())
    factory = SqlPlanningSetupFactory(None, "test")

    async def current(*args):
        return facts

    monkeypatch.setattr(factory, "facts", current)
    response = await factory.output_format(
        SimpleNamespace(
            scope_id=snapshot.scope_id, run_id=snapshot.run_id, output_schema=PLAN_REVIEW_SCHEMA
        ),
        snapshot,
    )
    schema = response["json_schema"]["schema"]
    assert "finish" not in schema["discriminator"]["mapping"]
    assert {"$ref": "#/$defs/Finish"} not in schema["oneOf"]
    assert not facts.finish_only
    assert not Draft202012Validator(schema).is_valid(
        candidate(()).model_dump(mode="json", by_alias=True)
    )


@pytest.mark.asyncio
async def test_bundle_facts_query_requires_current_content_seal_and_stable_order(monkeypatch):
    session, run, task, plan, revision, step = (uuid4() for _ in range(6))
    items = (uuid4(), uuid4())
    queries = []

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def connection(self, *, execution_options):
            assert execution_options == {"isolation_level": "REPEATABLE READ"}

        async def execute(self, statement, parameters):
            sql = str(statement)
            queries.append(sql)
            assert parameters == {"scope": "test", "run": run}
            if "SELECT i.id,i.final_revision" in sql:
                assert "i.scope_id=:scope AND i.run_id=:run" in sql
                assert "i.item_type='message' AND i.status='completed'" in sql
                assert "os.sealed_revision=i.final_revision AND m.status='completed'" in sql
                assert "os.content_last_delta_index IS NOT NULL" in sql
                assert "ORDER BY i.source_seq,i.id" in sql
                rows = [{"id": item, "final_revision": 7} for item in items]
                return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))
            row = {
                "task_id": task,
                "session_id": session,
                "owner_actor_id": "owner",
                "plan_id": plan,
                "current_revision": 2,
                "revision_id": revision,
                "steps": [{"id": str(step), "status": "completed"}],
                "state_ref": "checkpoint",
                "pending_input_version": 0,
                "applied_input_version": 0,
            }
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))

    async def blob(self, scope, ref):
        assert scope == "test" and ref == "checkpoint"
        return b'{"kind":"action_transition"}'

    monkeypatch.setattr(SqlContextBlobBackend, "get", blob)
    factory = SqlPlanningSetupFactory(None, "test")
    factory._sessions = Session
    facts = await factory.facts("test", run)
    assert len(queries) == 2 and facts.final_ids == items
    assert tuple(ref.resource_id for ref in facts.output_refs) == tuple(str(item) for item in items)


@pytest.mark.parametrize("shape", ["empty", "duplicate", "excessive", "foreign"])
def test_malformed_structure_does_not_reclassify_authorization_errors(shape):
    facts, snapshot = final_facts()
    facts = replace(facts, final_ids=(uuid4(), uuid4()))
    ids = {
        "empty": (),
        "duplicate": (facts.final_ids[0],) * 2,
        "excessive": (*facts.final_ids, uuid4()),
        "foreign": (uuid4(),),
    }[shape]
    with pytest.raises(AgentRuntimeError) as exc:
        facts.validate(candidate(ids), snapshot, initial=False)
    failure = exc.value.failure
    assert failure.code == ("FORBIDDEN" if shape == "foreign" else "INCOMPLETE_MODEL_OUTPUT")
    assert failure.category == ("authorization" if shape == "foreign" else "provider")
    assert not failure.retryable and failure.attempt_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["empty", "duplicate", "excessive"])
async def test_after_finish_validation_keeps_completed_audit_and_has_no_retry_proof(shape):
    loop, state, inputs, budget, _, answers = build("ask_user")
    facts, _ = final_facts()
    ids = {
        "empty": (),
        "duplicate": facts.final_ids * 2,
        "excessive": (*facts.final_ids, uuid4()),
    }[shape]
    action = candidate(ids)
    attempts = Attempts()

    class Provider:
        calls = 0

        async def stream(self, request, *, cancel=None):
            self.calls += 1
            yield ContentDelta(
                attempt_id=request.attempt_id,
                sequence="0",
                kind="content.delta",
                text=action.model_dump_json(by_alias=True),
            )
            yield ProviderFinish(
                attempt_id=request.attempt_id,
                sequence="1",
                kind="finish",
                reason="stop",
                raw_reason="stop",
            )

    provider = Provider()
    loop._provider = ProviderInvoker(
        provider, capabilities=capabilities, attempts=attempts, clock=lambda: NOW
    )
    resolve = loop._setup.resolve

    class Setup:
        async def resolve(self, *args):
            return replace(
                await resolve(*args),
                validate_action=lambda action, snapshot: facts.validate(
                    action, snapshot, initial=False
                ),
            )

    loop._setup = Setup()
    with pytest.raises(AgentRuntimeError) as exc:
        await loop.decide(state, inputs, budget)
    assert exc.value.failure.code == "INCOMPLETE_MODEL_OUTPUT"
    assert not exc.value.failure.retryable and exc.value.failure.attempt_id is None
    assert provider.calls == 1 and not answers.saved
    assert len(attempts.terminals) == 1
    assert attempts.terminals[0].status == "completed" and attempts.terminals[0].failure is None

    class NoDatabase:
        async def execute(self, *args):
            raise AssertionError("A post-Finish validation error has no failed attempt proof")

    choice = await propose_model_retry(
        NoDatabase(),
        "test",
        SimpleNamespace(),
        {},
        {"kind": "advance_run"},
        exc.value.failure,
        budget_id=uuid4(),
        clock=lambda: NOW,
        maximum_attempts=3,
        base_backoff=timedelta(seconds=1),
        max_backoff=timedelta(seconds=30),
    )
    assert choice.kind == "stop" and choice.reason == "unproven_model_failure"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "shape,message",
    [
        ("empty", "nonempty final items"),
        ("duplicate", "duplicate final items"),
        ("count", "63 final item limit"),
    ],
)
async def test_sql_guardrails_remain_distinct_and_fail_before_database(shape, message):
    item = uuid4()
    ids = {"empty": (), "duplicate": (item, item), "count": tuple(uuid4() for _ in range(64))}[
        shape
    ]
    with pytest.raises(AgentRuntimeError, match=message) as exc:
        await commit_answer(
            SimpleNamespace(schema="test"),
            guard=None,
            checkpoint_id=uuid4(),
            item_ids=ids,
            outcome=None,
            evidence_refs=(),
            journal=SimpleNamespace(schema="test"),
            resources=None,
        )
    assert exc.value.failure.code == "INVALID_INPUT" and not exc.value.failure.retryable

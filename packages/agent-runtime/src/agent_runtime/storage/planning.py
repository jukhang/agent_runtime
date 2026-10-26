"""Registered plan/execute profile with host-bound candidates, not model-owned authority."""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import Field, JsonValue, TypeAdapter
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.agent_loop import DecisionSetup, LoopSetup
from agent_runtime.contracts.canonical import canonical_json
from agent_runtime.contracts.context import PreparedContext
from agent_runtime.contracts.decisions import (
    AskUser,
    Delegate,
    Finish,
    NextAction,
    UpdatePlan,
    Wait,
)
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.loop import AppliedInputs, BudgetView, RunState
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.contracts.references import (
    CapabilityKey,
    CapabilityRef,
    ResourceRef,
    ResourceScope,
)
from agent_runtime.contracts.tasks import TaskPackage, WaitSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.main_factory import MainAgentProfile
from agent_runtime.storage.main_setup import SqlMainLoopSetup
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped

INITIAL_PLAN_SCHEMA = CapabilityRef(
    key=CapabilityKey(namespace="runtime", key="initial-plan"), revision="1"
)
PLAN_REVIEW_SCHEMA = CapabilityRef(
    key=CapabilityKey(namespace="runtime", key="plan-review"), revision="1"
)
INITIAL_ACTIONS: TypeAdapter[UpdatePlan | AskUser] = TypeAdapter(
    Annotated[UpdatePlan | AskUser, Field(discriminator="kind")]
)
REVIEW_ACTIONS: TypeAdapter[UpdatePlan | AskUser | Finish | Delegate | Wait] = TypeAdapter(
    Annotated[UpdatePlan | AskUser | Finish | Delegate | Wait, Field(discriminator="kind")]
)


@dataclass(frozen=True, slots=True)
class PlanningOptions:
    delegations: tuple[TaskPackage, ...] = ()
    waits: tuple[WaitSpec, ...] = ()


class PlanningOptionsPort(Protocol):
    targets: frozenset[CapabilityRef]

    async def resolve(self, scope_id: str, run_id: UUID) -> PlanningOptions:
        """Return only installed children and currently persisted, wakeable wait conditions."""
        ...


@dataclass(frozen=True, slots=True)
class PlanningFacts:
    plan_id: UUID
    revision: str
    plan_ref: ResourceRef | None
    final_ids: tuple[UUID, ...]
    step_ids: tuple[UUID, ...]
    options: PlanningOptions
    complete: bool = False
    after_reply: bool = False
    output_refs: tuple[ResourceRef, ...] = ()
    successful: bool = False
    input_pending: bool = False

    @property
    def finish_only(self) -> bool:
        return (
            self.revision != "0"
            and self.complete
            and self.successful
            and bool(self.final_ids)
            and not self.after_reply
            and not self.input_pending
            and not self.options.delegations
            and not self.options.waits
        )

    def validate(self, action: NextAction, snapshot: PreparedContext, *, initial: bool) -> None:
        if initial and not isinstance(action, (UpdatePlan, AskUser)):
            errors.invalid_input("Initial planning requires a plan or a clarification")
        if not initial and self.finish_only and not isinstance(action, Finish):
            errors.invalid_input("Closed planning work requires a Finish completion review")
        evidence = {
            source.resource
            for source in snapshot.sources
            if source.disposition in {"included", "summarized"}
        }
        if isinstance(action, UpdatePlan):
            plan = action.payload
            if plan.plan_id != self.plan_id or plan.expected_revision != self.revision:
                errors.version_conflict("Planning candidate has another plan identity or version")
            if len(plan.steps) > len(self.step_ids) or any(
                step.id not in self.step_ids for step in plan.steps
            ):
                errors.invalid_input("Planning step identities were not allocated by the host")
            if any(
                step.run_refs or any(ref not in evidence for ref in step.evidence_refs)
                for step in plan.steps
            ):
                errors.forbidden("Plan contains unbound run or evidence references")
        elif isinstance(action, Finish):
            final_ids = action.payload.final_item_ids
            if (
                not final_ids
                or len(final_ids) > len(self.final_ids)
                or len(set(final_ids)) != len(final_ids)
            ):
                # This gate runs after ProviderFinish: the completed attempt is
                # not failed retry evidence, so do not authorize an automatic retry.
                raise runtime_failure(
                    "INCOMPLETE_MODEL_OUTPUT",
                    category="provider",
                    retryable=False,
                    public_message="The model proposed empty, duplicate, or too many final items.",
                )
            if (
                not self.complete
                or (action.payload.outcome == "success" and not self.successful)
                or any(item not in self.final_ids for item in action.payload.final_item_ids)
                or any(ref not in evidence for ref in action.payload.evidence_refs)
            ):
                errors.forbidden("Finish requires current sealed output and context evidence")
            if final_ids != self.final_ids:
                raise runtime_failure(
                    "INCOMPLETE_MODEL_OUTPUT",
                    category="provider",
                    retryable=False,
                    public_message="The model changed the host-bound final publication bundle.",
                )
        elif isinstance(action, Delegate):
            task = action.payload.task_package
            if not any(
                task.model_copy(
                    update={"objective": candidate.objective, "constraints": candidate.constraints}
                )
                == candidate
                for candidate in self.options.delegations
            ):
                errors.forbidden("Delegation differs from its host-bound task and budget")
        elif isinstance(action, Wait):
            if action.payload.wait_spec not in self.options.waits:
                errors.forbidden("Wait condition is not bound to a durable wake source")
        elif isinstance(action, AskUser):
            if len(action.payload.question) > 4096:
                errors.invalid_input("Clarification question exceeds its display bound")
        else:
            errors.invalid_input("Planning decisions cannot bypass native answer/tool channels")


class SqlPlanningSetupFactory:
    """Optional profile. Unregistered context/memory/knowledge are still rejected."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        options: PlanningOptionsPort | None = None,
        max_plan_steps: int = 32,
    ) -> None:
        self._engine, self._schema, self._options = engine, require_schema(schema), options
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        if type(max_plan_steps) is not int or not 1 <= max_plan_steps <= 256:
            raise ValueError("Planning step bound must be between 1 and 256")
        self._max_steps = max_plan_steps

    def validate(self, profile: MainAgentProfile) -> None:
        agent = profile.installed.agent
        if (
            agent.knowledge
            or agent.policy
            or agent.context is not None
            or agent.memory is not None
            or agent.input_schema is not None
            or agent.output_schema is not None
        ):
            raise ValueError("Planning Agent requires an unregistered host dependency")
        if set(agent.subagents) - (self._options.targets if self._options else frozenset()):
            raise ValueError("Planning Agent has unregistered delegation targets")

    def create(
        self, actor: ActorContext, profile: MainAgentProfile, skills: Mapping[CapabilityRef, str]
    ) -> LoopSetup:
        base = SqlMainLoopSetup(
            self._engine,
            self._schema,
            actor=actor,
            agent_ref=profile.installed.ref,
            agent=profile.installed.agent,
            model=profile.model,
            profile=profile.profile,
            prompt=profile.prompt,
            max_input_tokens=profile.max_input_tokens,
            history_items=profile.history_items,
            skill_instructions=skills,
            review_instructions="Review completed work against the goal and current plan.",
        )
        return _PlanningSetup(base, self)

    async def facts(self, scope_id: str, run_id: UUID) -> PlanningFacts:
        async with self._sessions() as db:
            await db.connection(execution_options={"isolation_level": "REPEATABLE READ"})
            row = (
                (
                    await db.execute(
                        scoped(
                            "SELECT r.task_id,r.session_id,s.owner_actor_id,p.id AS plan_id,"
                            "p.current_revision,pr.steps,c.state_ref,pr.id AS revision_id,"
                            "r.pending_input_version,r.applied_input_version "
                            "FROM {t}run r JOIN {t}session s "
                            "ON s.scope_id=r.scope_id AND s.id=r.session_id "
                            "JOIN {t}checkpoint c ON c.scope_id=r.scope_id "
                            "AND c.id=r.checkpoint_id "
                            "LEFT JOIN {t}plan p ON p.scope_id=r.scope_id AND p.task_id=r.task_id "
                            "LEFT JOIN {t}plan_revision pr ON pr.scope_id=p.scope_id "
                            "AND pr.plan_id=p.id AND pr.revision=p.current_revision "
                            "WHERE r.scope_id=:scope AND r.id=:run "
                            "AND r.parent_run_id IS NULL AND s.deleted_at IS NULL",
                            self._schema,
                        ),
                        {"scope": scope_id, "run": run_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                errors.forbidden("Planning Run is unavailable")
            ids = (
                (
                    await db.execute(
                        scoped(
                            "SELECT i.id,i.final_revision FROM {t}timeline_item i "
                            "WHERE i.scope_id=:scope AND i.run_id=:run "
                            "AND i.item_type='message' AND i.status='completed' AND EXISTS "
                            "(SELECT 1 FROM {t}output_seal os JOIN {t}model_attempt m "
                            "ON m.scope_id=os.scope_id "
                            "AND m.id=os.model_attempt_id WHERE os.scope_id=i.scope_id "
                            "AND os.item_id=i.id "
                            "AND os.content_last_delta_index IS NOT NULL "
                            "AND os.sealed_revision=i.final_revision AND m.status='completed') "
                            "ORDER BY i.source_seq,i.id",
                            self._schema,
                        ),
                        {"scope": scope_id, "run": run_id},
                    )
                )
                .mappings()
                .all()
            )
        plan_id = row["plan_id"] or uuid5(NAMESPACE_URL, f"task-plan:{scope_id}:{row['task_id']}")
        resource = (
            None
            if row["revision_id"] is None
            else ResourceRef(
                resource_type="plan_revision",
                resource_id=str(row["revision_id"]),
                resource_version=str(row["current_revision"]),
                scope=ResourceScope(
                    scope_id=scope_id,
                    session_id=str(row["session_id"]),
                    owner_id=row["owner_actor_id"],
                ),
            )
        )
        options = (
            await self._options.resolve(scope_id, run_id) if self._options else PlanningOptions()
        )
        checkpoint = json.loads(
            await SqlContextBlobBackend(self._engine, self._schema).get(
                scope_id, str(row["state_ref"])
            )
        )
        outputs = tuple(
            ResourceRef(
                resource_type="timeline_item",
                resource_id=str(item["id"]),
                resource_version=str(item["final_revision"]),
                scope=ResourceScope(
                    scope_id=scope_id,
                    session_id=str(row["session_id"]),
                    owner_id=row["owner_actor_id"],
                ),
            )
            for item in ids
        )
        known_steps = tuple(UUID(step["id"]) for step in (row["steps"] or ()))
        if len(known_steps) > self._max_steps:
            errors.invalid_input("Existing plan exceeds the installed profile step bound")
        step_ids = tuple(
            dict.fromkeys(
                (*known_steps, *(uuid5(plan_id, f"step:{i}") for i in range(self._max_steps)))
            )
        )[: self._max_steps]
        return PlanningFacts(
            plan_id,
            str(row["current_revision"] or 0),
            resource,
            tuple(item["id"] for item in ids),
            step_ids,
            options,
            resource is not None
            and all(
                step["status"] in {"completed", "failed", "cancelled"}
                for step in (row["steps"] or ())
            ),
            checkpoint.get("kind") == "human_input",
            outputs,
            resource is not None
            and all(step["status"] == "completed" for step in (row["steps"] or ())),
            row["pending_input_version"] > row["applied_input_version"],
        )

    async def output_format(
        self, request: ProviderRequest, snapshot: PreparedContext
    ) -> Mapping[str, JsonValue]:
        initial = request.output_schema == INITIAL_PLAN_SCHEMA
        if not initial and request.output_schema != PLAN_REVIEW_SCHEMA:
            errors.invalid_input("Unknown planning output profile")
        facts = await self.facts(request.scope_id, request.run_id)
        schema = (INITIAL_ACTIONS if initial else REVIEW_ACTIONS).json_schema(by_alias=True)
        plan = schema["$defs"]["UpdatePlanPayload"]["properties"]
        plan["planId"] = {"const": str(facts.plan_id)}
        plan["expectedRevision"] = {"const": facts.revision}
        schema["$defs"]["PlanStep"]["properties"]["id"] = {
            "enum": [str(id) for id in facts.step_ids]
        }
        schema["$defs"]["PlanStep"]["properties"]["runRefs"]["maxItems"] = 0
        evidence = [
            source.resource.model_dump(mode="json", by_alias=True)
            for source in snapshot.sources
            if source.disposition in {"included", "summarized"}
        ]
        evidence_items = {"enum": evidence} if evidence else False
        schema["$defs"]["PlanStep"]["properties"]["evidenceRefs"]["items"] = evidence_items
        if not initial:
            finish = schema["$defs"]["FinishPayload"]["properties"]
            finish["evidenceRefs"]["items"] = evidence_items
            finish["finalItemIds"] = {
                "type": "array",
                "const": [str(id) for id in facts.final_ids],
            }
            if not facts.successful:
                finish["outcome"] = {"const": "partial"}
            if not facts.complete or not facts.final_ids:
                schema["oneOf"] = [
                    item for item in schema["oneOf"] if item.get("$ref") != "#/$defs/Finish"
                ]
                schema["discriminator"]["mapping"].pop("finish", None)
            if not facts.options.delegations:
                schema["oneOf"] = [
                    item for item in schema["oneOf"] if item.get("$ref") != "#/$defs/Delegate"
                ]
                schema["discriminator"]["mapping"].pop("delegate", None)
            if not facts.options.waits:
                schema["oneOf"] = [
                    item for item in schema["oneOf"] if item.get("$ref") != "#/$defs/Wait"
                ]
                schema["discriminator"]["mapping"].pop("wait", None)
            if facts.finish_only:
                schema["oneOf"] = [{"$ref": "#/$defs/Finish"}]
                schema["discriminator"]["mapping"] = {"finish": "#/$defs/Finish"}
        return {
            "type": "json_schema",
            "json_schema": {"name": "planning_action", "strict": True, "schema": schema},
        }


class _PlanningSetup:
    def __init__(self, base: SqlMainLoopSetup, factory: SqlPlanningSetupFactory) -> None:
        self._base, self._factory = base, factory

    async def resolve(
        self, state: RunState, inputs: AppliedInputs, budget: BudgetView
    ) -> DecisionSetup:
        setup = await self._base.resolve(state, inputs, budget)
        facts = await self._factory.facts(state.scope_id, state.run_id)
        initial = facts.revision == "0"
        request = setup.context_request
        if facts.plan_ref is not None:
            request = request.model_copy(
                update={
                    "resource_refs": tuple(dict.fromkeys((*request.resource_refs, facts.plan_ref)))
                }
            )
        if (
            not initial
            and setup.mode == "native"
            and (not facts.complete or facts.after_reply or not facts.final_ids)
        ):
            request = request.model_copy(
                update={
                    "prompt_text": (request.prompt_text or "")
                    + "\nThe plan has already been persisted by Runtime. Read the bound plan "
                    "and execute the task using the available domain tools. Runtime maintains "
                    "plan progress in the later structured review stage; plan management is "
                    "not a missing business tool. Do not ask the user for internal plan "
                    "interfaces or permissions. AskUser remains available for genuinely "
                    "missing business facts."
                }
            )
            return DecisionSetup(request, setup.model)
        request = request.model_copy(
            update={
                "resource_refs": tuple(dict.fromkeys((*request.resource_refs, *facts.output_refs)))
            }
        )
        prompt = request.prompt_text or ""
        if initial:
            prompt += (
                "\nCreate a concise task-specific plan, or ask for genuinely missing information. "
                "An empty plan is permitted for a trivial task. Do not answer in this profile. "
            )
        elif facts.finish_only:
            prompt += (
                "\nThis is the final completion review: all plan steps are completed, "
                "an answer is sealed, and no delegation or wait is available. "
                "Select Finish only. Assess the sealed answer against the original goal: "
                "choose success if sufficient, or partial if incomplete. "
                "A completed plan alone does not establish answer correctness."
            )
        else:
            prompt += (
                "\nReview sealed work against the original goal and current plan. "
                "When planComplete is true and the sealed answer satisfies the goal, "
                "select finish. "
                "Do not rewrite an unchanged plan or repeat an already sufficient answer. "
                "Finish only after the plan is closed and the answer satisfies the goal; "
                "otherwise update plan progress, delegate an eligible subtask, "
                "wait for an actual registered condition, "
                "or ask for essential missing information. "
            )
        if not initial:
            prompt += (
                "\nUse the entire host-owned finalItemIds publication bundle in the given order. "
                "Do not select a subset or construct item IDs. The bundle may include earlier "
                "introductory text; assess the work as a whole and choose partial if insufficient. "
            )
        prompt += "\nHost-owned bindings (not permissions): " + canonical_json(
            {
                "planId": str(facts.plan_id),
                "expectedRevision": facts.revision,
                "planComplete": facts.complete,
                "planSuccessful": facts.successful,
                "stepIds": [str(id) for id in facts.step_ids],
                "finalItemIds": [str(id) for id in facts.final_ids],
                "evidenceRefs": [
                    resource.model_dump(mode="json", by_alias=True)
                    for resource in request.resource_refs
                ],
                "delegations": [
                    task.model_dump(mode="json", by_alias=True)
                    for task in facts.options.delegations
                ],
                "waits": [
                    wait.model_dump(mode="json", by_alias=True) for wait in facts.options.waits
                ],
            }
        )
        request = request.model_copy(update={"tools": (), "prompt_text": prompt})
        return DecisionSetup(
            request,
            setup.model,
            "structured",
            INITIAL_PLAN_SCHEMA if initial else PLAN_REVIEW_SCHEMA,
            lambda action, snapshot: facts.validate(action, snapshot, initial=initial),
        )

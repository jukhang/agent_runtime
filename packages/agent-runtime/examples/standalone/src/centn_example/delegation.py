"""An explicit review workflow: pinned child, durable wait and sealed collection."""

from dataclasses import replace
from uuid import NAMESPACE_URL, UUID, uuid5

from agent_runtime.contracts.decisions import Delegate, Finish, Wait
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.requests import SubmitRequest
from agent_runtime.contracts.subagents import ChildRunRef, CollectRequest, VerifiedResult
from agent_runtime.contracts.tasks import TaskPackage, WaitSpec
from agent_runtime.contracts.transactions import CommitGuard
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.delegation_types import DelegationPolicy
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.main_setup import SqlMainLoopSetup
from agent_runtime.storage.subagents import SqlSubagentRuntime
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import scoped
from centn_example.tools import ref

DELEGATE_SCHEMA = ref("delegate-action")
DELEGATE_REVIEW_SCHEMA = ref("delegate-partial-review")
DELEGATE_WAIT_SCHEMA = ref("delegate-result-wait")


class SealedReview:
    def __init__(self, schema, sources):
        self.schema, self.sources = schema, sources

    async def verify_in_transaction(self, db, request, proposal):
        if request.acceptance_spec != ref("sealed-review") or not proposal.artifact_refs:
            errors.forbidden("Review proposal has no registered acceptance contract or output")
        for resource in (*proposal.artifact_refs, *proposal.evidence_refs):
            if resource.resource_type != "timeline_item":
                errors.forbidden("Review only accepts sealed child text")
            row = (
                await db.execute(
                    scoped(
                        "SELECT i.run_id FROM {t}timeline_item i JOIN {t}output_seal s "
                        "ON s.scope_id=i.scope_id AND s.item_id=i.id "
                        "AND s.sealed_revision=i.final_revision JOIN {t}model_attempt m "
                        "ON m.scope_id=s.scope_id AND m.id=s.model_attempt_id "
                        "WHERE i.scope_id=:scope AND i.id=:item AND i.status='completed' "
                        "AND m.status='completed' AND i.final_revision=:revision",
                        self.schema,
                    ),
                    {
                        "scope": request.child.scope_id,
                        "item": UUID(resource.resource_id),
                        "revision": int(resource.resource_version),
                    },
                )
            ).scalar_one_or_none()
            if row != request.child.child_run_id:
                errors.forbidden("Review output is not sealed by this child")
            await self.sources.authorize_resource_in_transaction(
                db, request.parent_guard.actor, resource, session_id=request.child.session_id
            )
        return VerifiedResult(
            child=request.child,
            proposal_id=request.proposal_id,
            proposal_revision=request.expected_proposal_revision,
            acceptance_spec=request.acceptance_spec,
            disposition="accepted",
            evidence_refs=proposal.evidence_refs,
            result_refs=proposal.artifact_refs,
            public_reason="Child output ownership, revision and provider seal verified; "
            "the parent must assess its substantive conclusions.",
        )


class Delegation:
    def __init__(self, host):
        self.host = host
        self.engine, self.schema = host.engine, host.settings.schema

    async def policy(self, run_id):
        async with self.host.resources.sessions() as db:
            allocation = (
                await db.execute(
                    scoped(
                        "SELECT id FROM {t}budget_allocation WHERE scope_id=:scope "
                        "AND root_run_id=:run AND parent_id IS NULL",
                        self.schema,
                    ),
                    {"scope": self.host.settings.scope, "run": run_id},
                )
            ).scalar_one()
        return DelegationPolicy(
            agent=ref("reviewer"),
            agent_revision_id=self.host.child.revision_id,
            parent_allocation_id=allocation,
            model_calls=4,
            tool_calls=0,
            tokens=32768,
            tools=(),
            acceptance_spec=ref("sealed-review"),
            output_schema_ref="example:review-text:1",
            max_active_children=1,
        )

    async def reconcile(self):
        """Restart-safe settlement, including children that never proposed a result."""
        async with self.host.resources.sessions() as db:
            rows = (
                (
                    await db.execute(
                        scoped(
                            "SELECT c.id,c.session_id,c.parent_run_id,c.task_id,"
                            "w.id AS workspace_id,"
                            "b.id AS allocation_id FROM {t}run c JOIN {t}workspace w "
                            "ON w.scope_id=c.scope_id AND w.owner_run_id=c.id "
                            "JOIN {t}budget_allocation b ON b.scope_id=c.scope_id "
                            "AND b.parent_id IS NOT NULL AND b.id=CAST((SELECT "
                            "a.payload->'payload'->>'budgetAllocationId' FROM {t}agent_message a "
                            "WHERE a.scope_id=c.scope_id AND a.recipient_run_id=c.id "
                            "AND a.kind='task.assign') AS uuid) "
                            "WHERE c.scope_id=:scope "
                            "AND c.status IN ('completed','failed','cancelled') "
                            "AND NOT EXISTS(SELECT 1 FROM {t}budget_entry e "
                            "WHERE e.scope_id=c.scope_id "
                            "AND e.operation_key='delegate-settle:' || CAST(c.id AS text)) "
                            "ORDER BY b.updated_at,c.id LIMIT 100",
                            self.schema,
                        ),
                        {"scope": self.host.settings.scope},
                    )
                )
                .mappings()
                .all()
            )
        for row in rows:
            actor = self.host.actor()
            policy = await self.policy(row["parent_run_id"])
            runtime = SqlSubagentRuntime(
                self.engine,
                self.schema,
                actor=actor,
                journal=SqlJournal(self.engine, self.schema, actor),
                resources=self.host.sources,
                policies={policy.agent: policy},
                message_predicate=ref("message"),
                verifier=SealedReview(self.schema, self.host.sources),
            )
            try:
                await runtime.reconcile_budget(
                    ChildRunRef(
                        scope_id=actor.scope_id,
                        session_id=row["session_id"],
                        parent_run_id=row["parent_run_id"],
                        child_run_id=row["id"],
                        task_id=row["task_id"],
                        agent=ref("reviewer"),
                        budget_allocation_id=row["allocation_id"],
                        workspace_id=row["workspace_id"],
                    )
                )
            except AgentRuntimeError as exc:
                if exc.failure.code not in {"FORBIDDEN", "VERSION_CONFLICT"}:
                    raise
                # Preserve reservations and keep unrelated sessions maintainable.
                print(
                    {"childBudgetReconciliation": exc.failure.code, "childRunId": str(row["id"])},
                    flush=True,
                )
                async with self.host.resources.sessions() as db, db.begin():
                    await db.execute(
                        scoped(
                            "UPDATE {t}budget_allocation SET updated_at=clock_timestamp() "
                            "WHERE scope_id=:scope AND id=:allocation",
                            self.schema,
                        ),
                        {"scope": actor.scope_id, "allocation": row["allocation_id"]},
                    )

    async def candidate(self, run_id):
        async with self.host.resources.sessions() as db:
            row = (
                (
                    await db.execute(
                        scoped(
                            "SELECT r.deadline_at,t.input_ref,EXISTS(SELECT 1 FROM {t}run c "
                            "WHERE c.scope_id=r.scope_id AND c.parent_run_id=r.id) AS delegated "
                            "FROM {t}run r JOIN {t}turn t ON t.scope_id=r.scope_id "
                            "AND t.id=r.turn_id WHERE r.scope_id=:scope AND r.id=:run",
                            self.schema,
                        ),
                        {"scope": self.host.settings.scope, "run": run_id},
                    )
                )
                .mappings()
                .one()
            )
        if row["delegated"]:
            return None
        submitted = SubmitRequest.model_validate_json(
            await SqlContextBlobBackend(self.engine, self.schema).get(
                self.host.settings.scope, row["input_ref"]
            )
        )
        return TaskPackage(
            task_id=uuid5(NAMESPACE_URL, f"review-task:{run_id}"),
            target_agent=ref("reviewer"),
            objective="\n".join(part.text for part in submitted.inputs if part.kind == "text"),
            constraints=(
                "Independently check the supplied facts and calculations. "
                "Return findings, uncertainty and a concise conclusion, not invented data.",
            ),
            acceptance_spec_ref="example:sealed-review:1",
            output_schema_ref="example:review-text:1",
            resource_refs=(),
            budget_allocation_id=uuid5(NAMESPACE_URL, f"review-budget:{run_id}"),
            deadline_at=row["deadline_at"],
        )

    async def pending_wait(self, run_id, *, require_active=True):
        async with self.host.resources.sessions() as db:
            row = (
                await db.execute(
                    scoped(
                        "SELECT r.deadline_at,EXISTS(SELECT 1 FROM {t}run c "
                        "WHERE c.scope_id=r.scope_id AND c.parent_run_id=r.id "
                        "AND c.status NOT IN ('completed','failed','cancelled')) "
                        "AS active FROM {t}run r WHERE r.scope_id=:scope AND r.id=:run",
                        self.schema,
                    ),
                    {"scope": self.host.settings.scope, "run": run_id},
                )
            ).one()
        if require_active and not row.active:
            return None
        return WaitSpec(
            kind="message",
            subject_id=str(run_id),
            deadline_at=row.deadline_at,
            resume_predicate=ref("message"),
        )

    def validate(self, profile):
        agent = profile.installed.agent
        if set(agent.subagents) != {ref("reviewer")} or any(
            (
                agent.knowledge,
                agent.skills,
                agent.hooks,
                agent.policy,
                agent.context,
                agent.memory,
                agent.input_schema,
                agent.output_schema,
            )
        ):
            raise ValueError("Review profile only binds its registered child and default tools")

    def create(self, actor, profile, skills):
        base = SqlMainLoopSetup(
            self.engine,
            self.schema,
            actor=actor,
            agent_ref=profile.installed.ref,
            agent=profile.installed.agent,
            model=profile.model,
            profile=profile.profile,
            prompt=profile.prompt,
            max_input_tokens=profile.max_input_tokens,
            history_items=profile.history_items,
        )
        owner = self

        class Setup:
            async def resolve(self, state, inputs, budget):
                setup = await base.resolve(state, inputs, budget)
                candidate = await owner.candidate(state.run_id)
                if candidate is None:
                    waiting = await owner.pending_wait(state.run_id)
                    if waiting is not None:

                        def validate_wait(action, snapshot):
                            if not isinstance(action, Wait) or action.payload.wait_spec != waiting:
                                errors.forbidden("The required independent review is still running")

                        return replace(
                            setup,
                            mode="structured",
                            output_schema=DELEGATE_WAIT_SCHEMA,
                            validate_action=validate_wait,
                            context_request=setup.context_request.model_copy(
                                update={
                                    "tools": (),
                                    "prompt_text": "The required independent reviewer "
                                    "is still active. Wait using the exact registered WaitSpec. "
                                    "Do not answer, delegate again, or claim completion yet.",
                                }
                            ),
                        )
                    if any(message.kind == "task.failed" for message in inputs.messages):
                        setup = replace(
                            setup,
                            context_request=setup.context_request.model_copy(
                                update={
                                    "prompt_text": setup.context_request.prompt_text
                                    + "\nThis workflow requires an independent review. That child "
                                    "failed; it has not verified the result. Explain the cause "
                                    "from "
                                    "the applied runtime message, state what the supplied facts "
                                    "still support, and offer retrying the review as a next step. "
                                    "Do not claim complete independent verification."
                                }
                            ),
                        )
                        if setup.mode == "structured":

                            def partial(action, snapshot):
                                if (
                                    not isinstance(action, Finish)
                                    or action.payload.outcome != "partial"
                                ):
                                    errors.forbidden("Required independent review is incomplete")

                            setup = replace(
                                setup, output_schema=DELEGATE_REVIEW_SCHEMA, validate_action=partial
                            )
                    return setup

                def validate(action, snapshot):
                    if not isinstance(action, Delegate) or action.payload.task_package != candidate:
                        errors.forbidden(
                            "Delegate candidate differs from the host-bound assignment"
                        )

                context = setup.context_request.model_copy(
                    update={
                        "tools": (),
                        "prompt_text": "Delegate this review to the installed reviewer. "
                        "Use the exact host-bound TaskPackage in the supplied schema.",
                    }
                )
                return replace(
                    setup,
                    context_request=context,
                    mode="structured",
                    output_schema=DELEGATE_SCHEMA,
                    validate_action=validate,
                )

        return Setup()

    async def output_format(self, request, snapshot):
        if request.output_schema == DELEGATE_WAIT_SCHEMA:
            waiting = await self.pending_wait(request.run_id, require_active=False)
            schema = Wait.model_json_schema(by_alias=True)
            schema["$defs"]["WaitPayload"]["properties"]["waitSpec"] = {
                "enum": [waiting.model_dump(mode="json", by_alias=True)]
            }
            return {
                "type": "json_schema",
                "json_schema": {
                    "name": "wait_for_independent_review",
                    "strict": True,
                    "schema": schema,
                },
            }
        if request.output_schema == DELEGATE_REVIEW_SCHEMA:
            schema = Finish.model_json_schema(by_alias=True)
            finish = schema["$defs"]["FinishPayload"]["properties"]
            finish["outcome"] = {"enum": ["partial"]}
            evidence = [
                source.resource.model_dump(mode="json", by_alias=True)
                for source in snapshot.sources
                if source.disposition in {"included", "summarized"}
                and source.resource.resource_version is not None
            ]
            finish["evidenceRefs"]["items"] = {"enum": evidence} if evidence else False
            async with self.host.resources.sessions() as db:
                ids = (
                    (
                        await db.execute(
                            scoped(
                                "SELECT i.id FROM {t}timeline_item i JOIN {t}output_seal s "
                                "ON s.scope_id=i.scope_id AND s.item_id=i.id "
                                "AND s.sealed_revision=i.final_revision JOIN {t}model_attempt m "
                                "ON m.scope_id=s.scope_id AND m.id=s.model_attempt_id "
                                "WHERE i.scope_id=:scope AND i.run_id=:run "
                                "AND i.status='completed' AND i.item_type='message' "
                                "AND m.status='completed'",
                                self.schema,
                            ),
                            {"scope": request.scope_id, "run": request.run_id},
                        )
                    )
                    .scalars()
                    .all()
                )
            finish["finalItemIds"]["items"] = {"enum": [str(id) for id in ids]} if ids else False
            finish["finalItemIds"]["minItems"] = 1
            return {
                "type": "json_schema",
                "json_schema": {
                    "name": "incomplete_independent_review",
                    "strict": True,
                    "schema": schema,
                },
            }
        candidate = await self.candidate(request.run_id)
        if candidate is None:
            errors.version_conflict("Delegation was already dispatched")
        schema = Delegate.model_json_schema(by_alias=True)
        schema["$defs"]["DelegatePayload"]["properties"]["taskPackage"] = {
            "enum": [candidate.model_dump(mode="json", by_alias=True)]
        }
        return {
            "type": "json_schema",
            "json_schema": {"name": "delegate_review", "strict": True, "schema": schema},
        }

    async def collect(self, actor, lease, runtime):
        current = lease.current()
        async with self.host.resources.sessions() as db:
            row = (
                (
                    await db.execute(
                        scoped(
                            "SELECT p.id,p.sender_run_id,p.task_id,r.state_version,"
                            "a.payload FROM {t}agent_message p JOIN {t}run r "
                            "ON r.scope_id=p.scope_id AND r.id=p.recipient_run_id "
                            "JOIN {t}agent_message a ON a.scope_id=p.scope_id "
                            "AND a.recipient_run_id=p.sender_run_id AND a.kind='task.assign' "
                            "WHERE p.scope_id=:scope AND p.recipient_run_id=:run "
                            "AND p.kind='result.proposed' AND NOT EXISTS("
                            "SELECT 1 FROM {t}agent_message v "
                            "WHERE v.scope_id=p.scope_id AND v.causation_id=p.id "
                            "AND v.kind IN ('result.accepted','result.rejected')) "
                            "ORDER BY p.recipient_seq LIMIT 1",
                            self.schema,
                        ),
                        {"scope": actor.scope_id, "run": current.run_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return False
            workspace = (
                await db.execute(
                    scoped(
                        "SELECT id FROM {t}workspace WHERE scope_id=:scope AND owner_run_id=:child",
                        self.schema,
                    ),
                    {"scope": actor.scope_id, "child": row["sender_run_id"]},
                )
            ).scalar_one()
        payload = row["payload"].get("payload", row["payload"])
        child = ChildRunRef(
            scope_id=actor.scope_id,
            session_id=current.session_id,
            parent_run_id=current.run_id,
            child_run_id=row["sender_run_id"],
            task_id=row["task_id"],
            agent=ref("reviewer"),
            budget_allocation_id=UUID(payload["budgetAllocationId"]),
            workspace_id=workspace,
        )
        await runtime.collect(
            CollectRequest(
                parent_guard=CommitGuard(
                    actor=actor, lease=current, expected_state_version=str(row["state_version"])
                ),
                child=child,
                proposal_id=row["id"],
                expected_proposal_revision="1",
                acceptance_spec=ref("sealed-review"),
            )
        )
        return True

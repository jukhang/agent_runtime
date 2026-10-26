"""Host-owned external completion facts; notifications never directly advance a Run."""

from dataclasses import replace
from datetime import UTC, datetime
from typing import Annotated
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import StringConstraints

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.base import Contract
from agent_runtime.contracts.decisions import Wait
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.tasks import WaitSpec
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.main_setup import SqlMainLoopSetup
from agent_runtime.storage.recovery import SqlRecovery
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import scoped
from agent_runtime.storage.wake import SqlWaitWaker, WakeRequest
from centn_example.resources import ResourceCreate
from centn_example.tools import ref

WAIT_SCHEMA = ref("external-wait-action")
EXTERNAL = ref("external-result")


class ExternalResult(Contract):
    text: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=65536)]


class ExternalWait:
    def __init__(self, host):
        self.host = host

    def result_ref(self, actor, session_id, run_id):
        identity = uuid5(
            NAMESPACE_URL,
            f"host-resource:{actor.scope_id}:{actor.actor_id}:external-result:{run_id}",
        )
        return ResourceRef(
            resource_type="host_attachment",
            resource_id=str(identity),
            resource_version="1",
            scope=ResourceScope(
                scope_id=actor.scope_id, owner_id=actor.actor_id, session_id=str(session_id)
            ),
        )

    async def signal(self, actor, session_id, run_id, text):
        text = ExternalResult(text=text).text
        async with self.host.resources.sessions() as db, db.begin():
            # Same lock order as Runtime: Slot -> Run -> Session/resource.
            await db.execute(
                scoped(
                    "SELECT session_id FROM {t}session_slot WHERE scope_id=:scope "
                    "AND session_id=:session FOR UPDATE",
                    self.host.settings.schema,
                ),
                {"scope": actor.scope_id, "session": session_id},
            )
            row = (
                await db.execute(
                    scoped(
                        "SELECT status,deadline_at,desired_control FROM {t}run "
                        "WHERE scope_id=:scope AND session_id=:session "
                        "AND id=:run AND parent_run_id IS NULL FOR UPDATE",
                        self.host.settings.schema,
                    ),
                    {"scope": actor.scope_id, "session": session_id, "run": run_id},
                )
            ).one_or_none()
            if self.host.settings.agent_profile != "external_wait" or row is None:
                errors.forbidden("External result target is not installed in this host")
            await self.host.sources.authorize_session_in_transaction(db, actor, session_id)
            if row.status in {"completed", "failed", "cancelled", "cancelling"} or (
                row.desired_control is not None or row.deadline_at <= datetime.now(UTC)
            ):
                errors.version_conflict("External result target is already terminal")
            return await self.host.resources.create_in_transaction(
                db,
                actor,
                ResourceCreate(
                    session_id=session_id,
                    idempotency_key=f"external-result:{run_id}",
                    title="External completion result",
                    text=text,
                ),
            )

    async def verify(self, db, request, spec):
        resource = self.result_ref(request.actor, request.session_id, request.run_id)
        if spec.subject_id != resource.resource_id or spec.resume_predicate != EXTERNAL:
            errors.forbidden("External condition differs from the registered Run binding")
        await self.host.sources.authorize_session_in_transaction(
            db, request.actor, request.session_id
        )
        present = (
            await db.execute(
                scoped(
                    "SELECT id FROM {t}resource WHERE scope_id=:scope AND id=:id",
                    self.host.resources.schema,
                ),
                {"scope": request.actor.scope_id, "id": UUID(resource.resource_id)},
            )
        ).first()
        if present is None:
            return False
        await self.host.resources.require(
            db, request.actor, resource, session_id=request.session_id
        )
        return True

    async def candidate(self, run_id):
        async with self.host.resources.sessions() as db:
            row = (
                await db.execute(
                    scoped(
                        "SELECT deadline_at,wake_generation,session_id FROM {t}run "
                        "WHERE scope_id=:scope AND id=:run",
                        self.host.settings.schema,
                    ),
                    {"scope": self.host.settings.scope, "run": run_id},
                )
            ).one()
        if row.wake_generation:
            return None
        return WaitSpec(
            kind="external",
            subject_id=self.result_ref(self.host.actor(), row.session_id, run_id).resource_id,
            deadline_at=row.deadline_at,
            resume_predicate=EXTERNAL,
        )

    def validate(self, profile):
        if profile.installed.agent.subagents:
            raise ValueError("External wait example does not install child agents")

    def create(self, actor, profile, skills):
        base = SqlMainLoopSetup(
            self.host.engine,
            self.host.settings.schema,
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
                    resource = owner.result_ref(actor, state.session_id, state.run_id)
                    return replace(
                        setup,
                        context_request=setup.context_request.model_copy(
                            update={
                                "resource_refs": (*setup.context_request.resource_refs, resource)
                            }
                        ),
                    )

                def validate(action, snapshot):
                    if not isinstance(action, Wait) or action.payload.wait_spec != candidate:
                        errors.forbidden("Wait differs from the host-bound external condition")

                return replace(
                    setup,
                    mode="structured",
                    output_schema=WAIT_SCHEMA,
                    validate_action=validate,
                    context_request=setup.context_request.model_copy(
                        update={
                            "tools": (),
                            "prompt_text": "Wait for the registered external "
                            "result using exactly the supplied WaitSpec. Do not answer yet.",
                        }
                    ),
                )

        return Setup()

    async def output_format(self, request, snapshot):
        candidate = await self.candidate(request.run_id)
        if candidate is None:
            errors.version_conflict("External result was already consumed")
        schema = Wait.model_json_schema(by_alias=True)
        schema["$defs"]["WaitPayload"]["properties"]["waitSpec"] = {
            "enum": [candidate.model_dump(mode="json", by_alias=True)]
        }
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "external_wait",
                "strict": True,
                "schema": schema,
            },
        }

    async def scan(self):
        async with self.host.resources.sessions() as db:
            rows = (
                await db.execute(
                    scoped(
                        "SELECT r.id,r.session_id,r.state_version,r.deadline_at,r.waiting_spec "
                        "FROM {t}run r WHERE r.scope_id=:scope "
                        "AND r.status='waiting_tool' AND r.desired_control IS NULL "
                        "AND r.waiting_spec->>'kind'='external' "
                        "ORDER BY r.updated_at,r.id LIMIT 100",
                        self.host.settings.schema,
                    ),
                    {"scope": self.host.settings.scope},
                )
            ).all()
        actor = self.host.actor()
        waker = SqlWaitWaker(
            self.host.engine,
            self.host.settings.schema,
            actor=actor,
            journal=SqlJournal(self.host.engine, self.host.settings.schema, actor),
            payloads=BlobContextPayloadStore(
                SqlContextBlobBackend(self.host.engine, self.host.settings.schema)
            ),
            external_predicates={EXTERNAL: self.verify},
        )
        for row in rows:
            recovery = SqlRecovery(self.host.engine, self.host.settings.schema)
            now = datetime.now(UTC)
            spec = WaitSpec.model_validate(row.waiting_spec)
            if min(row.deadline_at, spec.deadline_at) <= now:
                await recovery.fail_waiting_external(
                    actor.scope_id,
                    row.id,
                    spec.subject_id,
                    row.state_version,
                    runtime_failure(
                        "DELIVERY_EXPIRED",
                        category="delivery",
                        public_message="外部结果未在等待期限内到达。请确认外部任务状态后重新提交，"
                        "不要在未核实副作用前重复执行。",
                    ).failure,
                )
                continue
            try:
                await waker.wake(
                    WakeRequest(
                        actor=actor,
                        session_id=row.session_id,
                        run_id=row.id,
                        expected_state_version=row.state_version,
                        kind="external",
                        subject_id=spec.subject_id,
                    )
                )
            except AgentRuntimeError as exc:
                if exc.failure.code == "FORBIDDEN":
                    await recovery.fail_waiting_external(
                        actor.scope_id, row.id, spec.subject_id, row.state_version, exc.failure
                    )
                    continue
                if exc.failure.code not in {"VERSION_CONFLICT", "FENCED", "CONTROL_PENDING"}:
                    raise
                # No result is still a checked condition. Persist scan rotation so
                # unready waits cannot hide later revocations across process restarts.
                async with self.host.resources.sessions() as db, db.begin():
                    await db.execute(
                        scoped(
                            "UPDATE {t}run SET updated_at=clock_timestamp() "
                            "WHERE scope_id=:scope AND id=:run AND state_version=:version "
                            "AND status='waiting_tool' AND desired_control IS NULL",
                            self.host.settings.schema,
                        ),
                        {"scope": actor.scope_id, "run": row.id, "version": row.state_version},
                    )


class HostWakeScanner:
    def __init__(self, host, approvals):
        self.host, self.approvals = host, approvals

    async def wake_ready(self, scope_id):
        count = await self.approvals.wake_ready(scope_id)
        if self.host.settings.agent_profile == "external_wait":
            await self.host.external_wait.scan()
        if self.host.settings.agent_profile == "delegate":
            await self.host.delegation.reconcile()
        return count

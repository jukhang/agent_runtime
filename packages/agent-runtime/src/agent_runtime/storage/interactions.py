"""Human replies and wait continuation committed atomically, without synthetic Runs."""

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.context_payloads import context_blob_key
from agent_runtime.contracts.canonical import canonical_json, content_digest
from agent_runtime.contracts.payloads import InputAccepted, InputRequired, RunStatusPayload
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.requests import InteractionReceipt, InteractionRequest
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.state_events import (
    InputAcceptedDraft,
    RunStatusDraft,
    ServiceEventAuthority,
    StateEventBatch,
)
from agent_runtime.contracts.tasks import WaitSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.views import ChannelState, TimelineItem
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.journal import ReadAuthorizer, SqlJournal, owner_read_authorizer
from agent_runtime.storage.uow import errors, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlInteractionService:
    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        authorizer: ReadAuthorizer = owner_read_authorizer,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._engine, self._schema = engine, require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._locks = locks.build_lock_statements(schema)
        self._blobs = SqlContextBlobBackend(engine, schema)
        self._authorize, self._clock = authorizer, clock

    async def submit(self, actor: ActorContext, request: InteractionRequest) -> InteractionReceipt:
        request = InteractionRequest.model_validate(request.model_dump())
        identity = uuid5(
            NAMESPACE_URL,
            canonical_json(
                [
                    "human-input",
                    actor.scope_id,
                    actor.actor_id,
                    str(request.run_id),
                    request.idempotency_key,
                ]
            ),
        )
        request_body = {
            "actorId": actor.actor_id,
            "request": request.model_dump(mode="json", by_alias=True),
        }
        request_hash = content_digest(request_body)
        now = self._clock()
        params = {
            "scope": actor.scope_id,
            "session": request.session_id,
            "run": request.run_id,
            "id": identity,
        }
        async with self._sessions() as db, db.begin():
            slot = await locks.lock_slot(db, self._locks, actor.scope_id, request.session_id)
            run = await locks.lock_run(db, self._locks, actor.scope_id, request.run_id)
            if slot is None or run is None or run["session_id"] != request.session_id:
                errors.forbidden("Reply target is unavailable")
            session = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}session WHERE scope_id=:scope "
                            "AND id=:session FOR UPDATE",
                            self._schema,
                        ),
                        params,
                    )
                )
                .mappings()
                .one_or_none()
            )
            if session is None or not await self._authorize(actor, dict(session), False):
                errors.forbidden("Reply authorization changed")
            previous = (
                (
                    await db.execute(
                        scoped(
                            "SELECT content_ref,source_seq FROM {t}timeline_item "
                            "WHERE scope_id=:scope "
                            "AND id=:id AND run_id=:run AND item_type='user_reply'",
                            self._schema,
                        ),
                        params,
                    )
                )
                .mappings()
                .one_or_none()
            )
            if previous is not None:
                stored = json.loads(
                    await self._blobs.get_in_transaction(
                        db, actor.scope_id, str(previous["content_ref"])
                    )
                )
                if stored["requestHash"] != request_hash:
                    errors.version_conflict("Reply idempotency key was reused with another payload")
                return InteractionReceipt(
                    session_id=request.session_id,
                    run_id=request.run_id,
                    input_id=identity,
                    state_version=str(int(request.expected_version) + 1),
                    accepted_seq=str(previous["source_seq"]),
                )
            if (
                run["parent_run_id"] is not None
                or run["status"] != "waiting_user"
                or run["desired_control"] is not None
                or run["owner_id"] is not None
                or run["deadline_at"] <= now
                or run["state_version"] != int(request.expected_version)
            ):
                errors.version_conflict("The question changed or is no longer awaiting a reply")
            await revalidate_authority(db, self._locks, actor, run=run, now=now)
            spec = WaitSpec.model_validate(run["waiting_spec"])
            if (
                spec.kind != "message"
                or spec.subject_id != str(request.run_id)
                or spec.deadline_at <= now
            ):
                errors.version_conflict("Run is not waiting for a human reply")
            question_body = (
                await db.execute(
                    scoped(
                        "SELECT payload FROM {t}event WHERE scope_id=:scope AND run_id=:run "
                        "AND kind='input.required' AND payload->>'stateVersion'=:version "
                        "ORDER BY session_seq DESC LIMIT 1",
                        self._schema,
                    ),
                    {**params, "version": request.expected_version},
                )
            ).scalar_one_or_none()
            if question_body is None:
                errors.version_conflict("The persisted question is missing")
            question = InputRequired.model_validate(question_body)
            deadline = run["deadline_at"]
            if spec.active_seconds_remaining is not None:
                deadline = now + timedelta(seconds=spec.active_seconds_remaining)
            version = int(request.expected_version) + 1
            body = {**request_body, "requestHash": request_hash, "question": question.question}
            content_ref = context_blob_key(actor.scope_id, content_digest(body))
            await self._blobs.put_in_transaction(
                db, actor.scope_id, content_ref, canonical_json(body).encode()
            )
            state = {"kind": "human_input", "inputId": str(identity), "stateVersion": str(version)}
            state_ref = context_blob_key(actor.scope_id, content_digest(state))
            await self._blobs.put_in_transaction(
                db, actor.scope_id, state_ref, canonical_json(state).encode()
            )
            checkpoint_id = uuid5(identity, "checkpoint")
            work_id = uuid5(identity, "advance")
            seq = int(session["event_seq"]) + 1
            await db.execute(
                scoped(
                    "INSERT INTO {t}checkpoint (id,scope_id,run_id,source_checkpoint_id,"
                    "state_version,representation_revision,event_seq,schema_revision,"
                    "runtime_build,state_ref,content_hash) VALUES (:checkpoint,:scope,:run,"
                    ":source,:version,0,:seq,1,'agent-runtime',:ref,:hash)",
                    self._schema,
                ),
                {
                    **params,
                    "checkpoint": checkpoint_id,
                    "source": run["checkpoint_id"],
                    "version": version,
                    "seq": seq,
                    "ref": state_ref,
                    "hash": content_digest(state),
                },
            )
            user = TimelineItem(
                id=identity,
                type="message",
                role="user",
                created_seq=str(seq),
                revision="1",
                status="completed",
                resource_refs=(),
                channels=(
                    ChannelState(
                        name="content",
                        text=request.text,
                        last_delta_index=None,
                        content_hash=hashlib.sha256(request.text.encode()).hexdigest(),
                    ),
                ),
            )
            common = {
                "occurred_at": now,
                "turn_id": run["turn_id"],
                "run_id": request.run_id,
                "parent_run_id": None,
                "step_id": None,
                "model_attempt_id": None,
            }
            receipt = await SqlJournal(
                self._engine, self._schema, actor
            ).append_state_in_transaction(
                db,
                StateEventBatch(
                    authority=ServiceEventAuthority(
                        kind="service",
                        actor=actor,
                        purpose="wake",
                        transaction_id=identity,
                        source=ResourceRef(
                            resource_type="run",
                            resource_id=str(request.run_id),
                            resource_version=request.expected_version,
                            scope=ResourceScope(
                                scope_id=actor.scope_id, session_id=str(request.session_id)
                            ),
                        ),
                    ),
                    session_id=request.session_id,
                    trace_id=str(identity),
                    events=(
                        InputAcceptedDraft(
                            **common,
                            item_id=identity,
                            kind="input.accepted",
                            producer_key=f"human-input:{identity}",
                            payload=InputAccepted(
                                state_version=str(version),
                                reply_to_run_id=request.run_id,
                                user_item=user,
                            ),
                        ),
                        RunStatusDraft(
                            **common,
                            item_id=None,
                            kind="run.status",
                            producer_key=f"run-status:{request.run_id}:{version}",
                            payload=RunStatusPayload(
                                state_version=str(version),
                                status=RunStatus.QUEUED,
                                phase="queued",
                                phase_started_at=now,
                                public_reason="Reply received; continuing the task.",
                            ),
                        ),
                    ),
                ),
            )
            if int(receipt.entries[0].session_seq) != seq:
                errors.version_conflict("Reply event watermark changed")
            await db.execute(
                scoped(
                    "INSERT INTO {t}timeline_item (id,scope_id,session_id,run_id,item_type,"
                    "content_ref,status,final_revision,source_seq) "
                    "VALUES (:id,:scope,:session,:run,'user_reply',:ref,"
                    "'completed',1,:seq)",
                    self._schema,
                ),
                {**params, "ref": content_ref, "seq": seq},
            )
            changed = await db.execute(
                scoped(
                    "UPDATE {t}run SET status='queued',phase='queued',phase_started_at=:now,"
                    "updated_at=:now,state_version=:version,checkpoint_id=:checkpoint,waiting_spec=NULL,"
                    "wake_generation=wake_generation+1,deadline_at=:deadline "
                    "WHERE scope_id=:scope AND id=:run "
                    "AND state_version=:expected AND status='waiting_user' RETURNING id",
                    self._schema,
                ),
                {
                    **params,
                    "now": now,
                    "version": version,
                    "checkpoint": checkpoint_id,
                    "deadline": deadline,
                    "expected": int(request.expected_version),
                },
            )
            if changed.scalar_one_or_none() is None:
                errors.version_conflict("Reply lost its wait version")
            await db.execute(
                scoped(
                    "INSERT INTO {t}work_item (id,scope_id,run_id,subject_ref,kind,dedup_key,"
                    "status,payload_ref,wake_reason,priority,lease_epoch,attempt_count,"
                    "available_at,deadline_at) "
                    "VALUES (:work,:scope,:run,'{{}}','advance_run',:dedup,'pending',"
                    ":ref,'message',"
                    "1000,0,0,:now,:deadline)",
                    self._schema,
                ),
                {
                    **params,
                    "work": work_id,
                    "dedup": f"human-input:{identity}",
                    "ref": state_ref,
                    "now": now,
                    "deadline": deadline,
                },
            )
            return InteractionReceipt(
                session_id=request.session_id,
                run_id=request.run_id,
                input_id=identity,
                state_version=str(version),
                accepted_seq=str(seq),
            )


__all__ = ["SqlInteractionService"]

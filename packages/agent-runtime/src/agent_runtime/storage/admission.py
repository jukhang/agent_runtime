"""Atomic, authenticated ingress. Scheduling authority is never granted by ingress."""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.agent import Agent
from agent_runtime.contracts.canonical import canonical_bytes, content_digest
from agent_runtime.contracts.payloads import TurnAccepted
from agent_runtime.contracts.references import CapabilityRef, ResourceRef, ResourceScope
from agent_runtime.contracts.requests import AcceptedTurn, ResourceInput, SubmitRequest
from agent_runtime.contracts.state_events import (
    ServiceEventAuthority,
    StateEventBatch,
    TurnAcceptedDraft,
)
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.views import ChannelState, TimelineItem
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.models.provider import BudgetAllocationRow
from agent_runtime.storage.models.session import RunRow, TimelineItemRow, TurnRow
from agent_runtime.storage.models.tools import CapabilityGrantRow
from agent_runtime.storage.models.work import CheckpointRow, TaskRow, WorkItemRow
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped


@dataclass(frozen=True, slots=True)
class AdmissionAgent:
    """Trusted installed configuration, not request-supplied permissions or limits."""

    ref: CapabilityRef
    revision_id: UUID
    agent: Agent
    max_tokens: int
    resource_grants: tuple[ResourceRef, ...] = ()

    def __post_init__(self) -> None:
        if self.max_tokens < 1 or self.ref.key.key != self.agent.id:
            raise ValueError("invalid installed Agent or token budget")


type AdmissionAuthorizer = Callable[
    [AsyncSession, ActorContext, SubmitRequest, AdmissionAgent], Awaitable[bool]
]


class SqlAdmission:
    """One Slot -> Session transaction owns idempotency and all initial durable facts."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        agents: Mapping[tuple[str, CapabilityRef], AdmissionAgent],
        authorize: AdmissionAuthorizer,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        human_wait_seconds: int | None = None,
    ) -> None:
        self._schema = require_schema(schema)
        self._engine = engine
        translated = engine.execution_options(schema_translate_map={None: self._schema})
        self._sessions = async_sessionmaker(translated, expire_on_commit=False)
        self._agents = dict(agents)
        self._authorize, self._clock = authorize, clock
        if human_wait_seconds is not None and not 1 <= human_wait_seconds <= 2592000:
            raise ValueError("Human wait retention must be between 1 second and 30 days")
        self._human_wait_seconds = human_wait_seconds or 0
        self._blobs = SqlContextBlobBackend(engine, schema)
        if any(ref != binding.ref for (_, ref), binding in self._agents.items()):
            raise ValueError("installed Agent mapping identity mismatch")

    async def accept(self, actor: ActorContext, request: SubmitRequest) -> AcceptedTurn:
        payload = request.model_dump(mode="json", by_alias=True)
        data = canonical_bytes(payload)
        if len(data) > 32768:
            errors.invalid_input("turn input exceeds 32 KiB; use resource references")
        request_hash = content_digest(payload)
        now = self._clock()
        async with self._sessions() as db, db.begin():
            # The session must already exist. Do not upsert a slot before checking it:
            # an unauthorized request must not create any persistent identities.
            params = {"scope": actor.scope_id, "session": request.session_id}
            session = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}session WHERE scope_id=:scope AND id=:session",
                            self._schema,
                        ),
                        params,
                    )
                )
                .mappings()
                .one_or_none()
            )
            self._require_session(session, actor)
            await db.execute(
                scoped(
                    "INSERT INTO {t}session_slot (scope_id,session_id,slot_version) "
                    "VALUES (:scope,:session,0) ON CONFLICT DO NOTHING",
                    self._schema,
                ),
                params,
            )
            await db.execute(
                scoped(
                    "SELECT session_id FROM {t}session_slot "
                    "WHERE scope_id=:scope AND session_id=:session FOR UPDATE",
                    self._schema,
                ),
                params,
            )
            session = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}session "
                            "WHERE scope_id=:scope AND id=:session FOR UPDATE",
                            self._schema,
                        ),
                        params,
                    )
                )
                .mappings()
                .one_or_none()
            )
            self._require_session(session, actor)
            previous = (
                (
                    await db.execute(
                        scoped(
                            "SELECT id,request_hash,primary_run_id FROM {t}turn "
                            "WHERE scope_id=:scope AND session_id=:session AND request_key=:key",
                            self._schema,
                        ),
                        {**params, "key": request.idempotency_key},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if previous is not None:
                if previous["request_hash"] != request_hash:
                    errors.version_conflict("idempotency key was used with different input")
                return await self._receipt(db, actor, request, previous["id"])
            binding = self._agents.get((actor.scope_id, request.agent))
            if binding is None:
                errors.forbidden("Agent revision is not installed in this scope")
            installed = (
                (
                    await db.execute(
                        scoped(
                            "SELECT a.key,r.revision,r.manifest,r.content_hash "
                            "FROM {t}agent_revision r JOIN {t}agent a "
                            "ON a.scope_id=r.scope_id AND a.id=r.agent_id "
                            "WHERE r.scope_id=:scope AND r.id=:id FOR SHARE OF a,r",
                            self._schema,
                        ),
                        {"scope": actor.scope_id, "id": binding.revision_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            manifest = binding.agent.model_dump(mode="json", by_alias=True)
            if (
                installed is None
                or installed["key"] != request.agent.key.key
                or str(installed["revision"]) != request.agent.revision
                or installed["manifest"] != manifest
                or installed["content_hash"] != content_digest(manifest)
            ):
                errors.forbidden("installed Agent manifest differs from the pinned revision")
            if any(
                isinstance(part, ResourceInput)
                and part.resource_ref.scope.scope_id != actor.scope_id
                for part in request.inputs
            ) or any(ref.scope.scope_id != actor.scope_id for ref in binding.resource_grants):
                errors.forbidden("input or granted resource belongs to another scope")
            if not await self._authorize(db, actor, request, binding):
                errors.forbidden("current Agent or input resource access was denied")
            return await self._create(db, actor, request, binding, request_hash, now)

    @staticmethod
    def _require_session(row: Mapping[Any, Any] | None, actor: ActorContext) -> None:
        if (
            row is None
            or row["owner_actor_id"] != actor.actor_id
            or row["deleted_at"] is not None
            or row["auth_revision"] != int(actor.auth_revision)
        ):
            errors.forbidden("session is unavailable or authorization changed")

    async def _blob(self, db: AsyncSession, actor: ActorContext, payload: object) -> str:
        digest = content_digest(payload)
        scope_hash = hashlib.sha256(actor.scope_id.encode()).hexdigest()
        ref = f"context-{scope_hash}-{digest}"
        await self._blobs.put_in_transaction(db, actor.scope_id, ref, canonical_bytes(payload))
        return ref

    async def _create(
        self,
        db: AsyncSession,
        actor: ActorContext,
        request: SubmitRequest,
        binding: AdmissionAgent,
        request_hash: str,
        now: datetime,
    ) -> AcceptedTurn:
        turn_id, run_id, task_id, checkpoint_id = (uuid4() for _ in range(4))
        user_id = uuid5(NAMESPACE_URL, f"turn-user:{turn_id}")
        assistant_id = uuid5(NAMESPACE_URL, f"turn-assistant:{run_id}")
        deadline = now + timedelta(seconds=binding.agent.budget.max_runtime_seconds)
        common = {"scope_id": actor.scope_id, "created_at": now, "updated_at": now}
        accepted_seq = (
            int(
                (
                    await db.execute(
                        scoped(
                            "SELECT event_seq FROM {t}session WHERE scope_id=:scope AND id=:id",
                            self._schema,
                        ),
                        {"scope": actor.scope_id, "id": request.session_id},
                    )
                ).scalar_one()
            )
            + 1
        )
        input_ref = await self._blob(db, actor, request.model_dump(mode="json", by_alias=True))
        state_ref = await self._blob(
            db,
            actor,
            {
                "kind": "initial_checkpoint",
                "runId": str(run_id),
                "stateVersion": "0",
                "inputRef": input_ref,
                "appliedInputSeq": "0",
            },
        )
        await db.execute(
            sa.insert(TurnRow).values(
                **common,
                id=turn_id,
                session_id=request.session_id,
                request_key=request.idempotency_key,
                request_hash=request_hash,
                input_ref=input_ref,
                primary_run_id=run_id,
            )
        )
        await db.execute(
            sa.insert(RunRow).values(
                **common,
                id=run_id,
                session_id=request.session_id,
                turn_id=turn_id,
                root_run_id=run_id,
                agent_revision_id=binding.revision_id,
                task_id=task_id,
                checkpoint_id=checkpoint_id,
                status="queued",
                phase="queued",
                state_version=0,
                owner_epoch=0,
                cancel_version=0,
                wake_generation=0,
                pending_input_version=0,
                applied_input_version=0,
                authority_revision=int(actor.auth_revision),
                phase_started_at=now,
                deadline_at=deadline,
            )
        )
        await db.execute(
            sa.insert(TaskRow).values(
                **common,
                id=task_id,
                root_run_id=run_id,
                acceptance_spec_ref=input_ref,
                deadline_at=deadline + timedelta(seconds=self._human_wait_seconds),
            )
        )
        await db.execute(
            sa.insert(CheckpointRow).values(
                **common,
                id=checkpoint_id,
                run_id=run_id,
                state_version=0,
                representation_revision=0,
                event_seq=accepted_seq,
                schema_revision=1,
                runtime_build="agent-runtime",
                state_ref=state_ref,
                content_hash=state_ref.rsplit("-", 1)[1],
            )
        )
        await db.execute(
            sa.insert(BudgetAllocationRow).values(
                **common,
                id=uuid4(),
                root_run_id=run_id,
                limits={
                    "model_calls": binding.agent.budget.max_model_calls,
                    "tool_calls": binding.agent.budget.max_tool_calls,
                    "tokens": binding.max_tokens,
                },
                reserved={},
                unknown_reserved={},
                settled={},
                usage_status="complete",
                unsettled_count=0,
                version=0,
            )
        )
        await db.execute(
            sa.insert(CapabilityGrantRow).values(
                **common,
                id=uuid4(),
                run_id=run_id,
                actor_id=actor.actor_id,
                revision=int(actor.auth_revision),
                allowed_scope={
                    "agents": [
                        ref.model_dump(mode="json", by_alias=True)
                        for ref in binding.agent.subagents
                    ],
                    "tools": [
                        ref.model_dump(mode="json", by_alias=True) for ref in binding.agent.tools
                    ],
                    "resources": [
                        ref.model_dump(mode="json", by_alias=True)
                        for ref in binding.resource_grants
                    ],
                },
                issued_at=now,
                expires_at=deadline + timedelta(seconds=self._human_wait_seconds),
            )
        )
        await db.execute(
            sa.insert(WorkItemRow).values(
                **common,
                id=uuid4(),
                run_id=run_id,
                subject_ref={},
                kind="advance_run",
                dedup_key=f"advance:{run_id}:0",
                status="pending",
                payload_ref=state_ref,
                wake_reason="start",
                priority=100,
                lease_epoch=0,
                attempt_count=0,
                available_at=now,
                deadline_at=deadline,
            )
        )
        text = "\n".join(part.text for part in request.inputs if part.kind == "text")
        refs = tuple(part.resource_ref for part in request.inputs if part.kind == "resource")
        user = TimelineItem(
            id=user_id,
            type="message",
            role="user",
            created_seq=str(accepted_seq),
            revision="0",
            status="completed",
            channels=(
                ChannelState(
                    name="content",
                    text=text,
                    last_delta_index=None,
                    content_hash=hashlib.sha256(text.encode()).hexdigest(),
                ),
            ),
            resource_refs=refs,
        )
        journal = SqlJournal(self._engine, self._schema, actor, clock=self._clock)
        receipt = await journal.append_state_in_transaction(
            db,
            StateEventBatch(
                authority=ServiceEventAuthority(
                    kind="service",
                    actor=actor,
                    purpose="admission",
                    transaction_id=turn_id,
                    source=ResourceRef(
                        resource_type="turn",
                        resource_id=str(turn_id),
                        resource_version="0",
                        scope=ResourceScope(
                            scope_id=actor.scope_id, session_id=str(request.session_id)
                        ),
                    ),
                ),
                session_id=request.session_id,
                trace_id=str(turn_id),
                events=(
                    TurnAcceptedDraft(
                        kind="turn.accepted",
                        producer_key=f"admission:{turn_id}",
                        occurred_at=now,
                        turn_id=turn_id,
                        run_id=run_id,
                        parent_run_id=None,
                        step_id=None,
                        item_id=None,
                        model_attempt_id=None,
                        payload=TurnAccepted(
                            turn_id=turn_id,
                            run_id=run_id,
                            user_item=user,
                            assistant_item_id=assistant_id,
                        ),
                    ),
                ),
            ),
        )
        seq = receipt.entries[0].session_seq
        if int(seq) != accepted_seq:
            errors.version_conflict("admission receipt watermark changed within its transaction")
        await db.execute(
            sa.insert(TimelineItemRow).values(
                **common,
                id=user_id,
                session_id=request.session_id,
                run_id=run_id,
                item_type="user_message",
                content_ref=input_ref,
                status="completed",
                final_revision=0,
                source_seq=int(seq),
            )
        )
        return AcceptedTurn(
            session_id=request.session_id,
            turn_id=turn_id,
            run_id=run_id,
            assistant_item_id=assistant_id,
            accepted_seq=seq,
        )

    async def _receipt(
        self,
        db: AsyncSession,
        actor: ActorContext,
        request: SubmitRequest,
        turn_id: UUID,
    ) -> AcceptedTurn:
        row = (
            (
                await db.execute(
                    scoped(
                        "SELECT payload,session_seq FROM {t}event WHERE scope_id=:scope "
                        "AND session_id=:session AND turn_id=:turn AND kind='turn.accepted'",
                        self._schema,
                    ),
                    {"scope": actor.scope_id, "session": request.session_id, "turn": turn_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            errors.version_conflict("accepted Turn is missing its durable receipt")
        accepted = TurnAccepted.model_validate(row["payload"])
        return AcceptedTurn(
            session_id=request.session_id,
            turn_id=turn_id,
            run_id=accepted.run_id,
            assistant_item_id=accepted.assistant_item_id,
            accepted_seq=str(row["session_seq"]),
        )


__all__ = ["AdmissionAgent", "AdmissionAuthorizer", "SqlAdmission"]

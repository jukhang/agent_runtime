"""Load a claimed root Run under the same fencing order as state transitions."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.agent_loop import LoopSetup
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.loop import AppliedInputs, RunState
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.scheduler import ClaimedWork
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.transactions import ActorContext, CommitGuard
from agent_runtime.storage.message_rows import parse_agent_message_row
from agent_runtime.storage.uow import SqlStateUnitOfWork, errors, facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped
from agent_runtime.worker import LeaseHandle


@dataclass(frozen=True, slots=True)
class MainAgentBinding:
    agent: CapabilityRef
    setup: LoopSetup
    manifest_hash: str | None = None


@dataclass(frozen=True, slots=True)
class LoadedMainRun:
    state: RunState
    inputs: AppliedInputs
    binding: MainAgentBinding
    turn_id: UUID
    budget_allocation_id: UUID


class SqlMainRunLoader:
    """One consistent, lease-fenced snapshot; later effects recheck authority."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        agents: Mapping[UUID, MainAgentBinding],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._schema = require_schema(schema)
        self._locks = locks.build_lock_statements(self._schema)
        self._actor = actor
        self._agents = dict(agents)
        self._clock = clock
        self._state = SqlStateUnitOfWork(engine, schema, actor)

    async def apply_notifications(self, guard: CommitGuard) -> None:
        """Commit only a contiguous non-effect prefix before host proposal collection.

        This safe point needs no model binding. A proposal/control/handoff is a
        boundary, not permission to skip a message or advance the watermark past it.
        """
        async with self._sessions() as db, db.begin():
            guarded = await self._state._lock_guard(db, guard)
            run = dict(guarded.run)
            if (
                guarded.work["kind"] != "advance_run"
                or run["parent_run_id"] is not None
                or run["status"] != "running"
                or run["desired_control"] is not None
                or run["deadline_at"] <= guarded.now
                or run["state_version"] != int(guard.expected_state_version)
            ):
                errors.control_pending("notification consumption requires the active root fence")
            if run["pending_input_version"] != run["applied_input_version"]:
                await self._apply_notifications(db, run, guarded.now)

    async def load(self, work: ClaimedWork, handle: LeaseHandle) -> LoadedMainRun:
        lease = handle.current()
        if work.lease.scope_id != self._actor.scope_id or (
            work.lease.scope_id,
            work.lease.session_id,
            work.lease.run_id,
            work.lease.work_id,
            work.lease.owner_id,
            work.lease.run_epoch,
            work.lease.work_epoch,
            work.lease.slot_version,
        ) != (
            lease.scope_id,
            lease.session_id,
            lease.run_id,
            lease.work_id,
            lease.owner_id,
            lease.run_epoch,
            lease.work_epoch,
            lease.slot_version,
        ):
            errors.forbidden("main claim changed identity")
        now = self._clock()
        facts.verify_lease(lease, now=now)
        async with self._sessions() as db, db.begin():
            slot = await locks.lock_slot(db, self._locks, lease.scope_id, lease.session_id)
            facts.verify_slot(slot, lease)
            run = dict(
                facts.verify_run(
                    await locks.lock_run(db, self._locks, lease.scope_id, lease.run_id),
                    self._actor,
                    lease,
                    now=now,
                )
            )
            await revalidate_authority(db, self._locks, self._actor, run=run, now=now)
            claimed = facts.verify_work(
                await locks.lock_work_item(db, self._locks, lease.scope_id, lease.work_id),
                lease,
                now=now,
            )
            if claimed["kind"] != "advance_run" or run["parent_run_id"] is not None:
                errors.forbidden("main loader accepts only a root advance_run")
            if (
                run["status"] != "running"
                or run["desired_control"] is not None
                or now >= run["deadline_at"]
            ):
                errors.control_pending("root run is not ready for a model decision")
            if int(run["state_version"]) != int(work.state_version):
                errors.version_conflict("root run changed since work was claimed")
            if int(run["pending_input_version"]) != int(run["applied_input_version"]):
                await self._apply_notifications(db, run, now)
                if int(run["pending_input_version"]) != int(run["applied_input_version"]):
                    errors.control_pending("root input requires ordered host handling")
            checkpoint_id = run["checkpoint_id"]
            if checkpoint_id is None or checkpoint_id != work.checkpoint_id:
                errors.version_conflict("root checkpoint changed since work was claimed")
            checkpoint = (
                (
                    await db.execute(
                        scoped(
                            "SELECT run_id,state_version,state_ref FROM {t}checkpoint "
                            "WHERE scope_id=:scope AND id=:id",
                            self._schema,
                        ),
                        {"scope": lease.scope_id, "id": checkpoint_id},
                    )
                )
                .mappings()
                .one_or_none()
            )
            if (
                checkpoint is None
                or checkpoint["run_id"] != lease.run_id
                or int(checkpoint["state_version"]) != int(run["state_version"])
            ):
                errors.version_conflict("root checkpoint is not the committed state")
            agent = (
                (
                    await db.execute(
                        scoped(
                            "SELECT a.key,r.revision,r.manifest,r.content_hash "
                            "FROM {t}agent_revision r "
                            "JOIN {t}agent a ON a.scope_id=r.scope_id AND a.id=r.agent_id "
                            "WHERE r.scope_id=:scope AND r.id=:id",
                            self._schema,
                        ),
                        {"scope": lease.scope_id, "id": run["agent_revision_id"]},
                    )
                )
                .mappings()
                .one_or_none()
            )
            binding = self._agents.get(run["agent_revision_id"])
            if (
                agent is None
                or binding is None
                or binding.agent.key.key != agent["key"]
                or binding.agent.revision != str(agent["revision"])
            ):
                errors.forbidden("root Agent revision is not installed")
            if binding.manifest_hash is not None and (
                binding.manifest_hash != agent["content_hash"]
                or binding.manifest_hash != content_digest(agent["manifest"])
            ):
                errors.forbidden("root Agent manifest differs from the installed host profile")
            messages = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}agent_message WHERE scope_id=:scope "
                            "AND recipient_run_id=:run AND status='applied' "
                            "ORDER BY recipient_seq",
                            self._schema,
                        ),
                        {"scope": lease.scope_id, "run": lease.run_id},
                    )
                )
                .mappings()
                .all()
            )
            envelopes = tuple(parse_agent_message_row(dict(row)) for row in messages)
            if len(envelopes) != int(run["applied_input_version"]):
                errors.version_conflict("root input watermark disagrees with applied messages")
            state = RunState(
                scope_id=lease.scope_id,
                session_id=lease.session_id,
                run_id=lease.run_id,
                state_version=str(run["state_version"]),
                checkpoint_ref=str(checkpoint["state_ref"]),
                agent=binding.agent,
                status=RunStatus.RUNNING,
                desired_control=None,
                cancel_version=str(run["cancel_version"]),
                pending_input_seq=str(run["pending_input_version"]),
                applied_input_seq=str(run["applied_input_version"]),
            )
            inputs = AppliedInputs(
                run_id=lease.run_id,
                checkpoint_ref=state.checkpoint_ref,
                previous_seq="0",
                applied_seq=str(len(envelopes)),
                messages=envelopes,
            )
            return LoadedMainRun(
                state=state,
                inputs=inputs,
                binding=binding,
                turn_id=run["turn_id"],
                budget_allocation_id=work.budget_allocation_id,
            )

    async def _apply_notifications(
        self, db: AsyncSession, run: dict[str, Any], now: datetime
    ) -> None:
        """Consume non-effect notifications under the already validated root fence.

        Proposals still require acceptance verification. Control and handoff
        messages cannot be downgraded into ordinary model context.
        """
        scope, identity = run["scope_id"], run["id"]
        cursor = await locks.lock_mailbox_cursor(db, self._locks, scope, identity)
        previous, pending = int(run["applied_input_version"]), int(run["pending_input_version"])
        if (
            cursor is None
            or cursor["applied_seq"] != previous
            or cursor["allocated_seq"] != pending
        ):
            errors.version_conflict("root notification watermarks disagree")
        messages = (
            (
                await db.execute(
                    scoped(
                        "SELECT * FROM {t}agent_message WHERE scope_id=:scope "
                        "AND recipient_run_id=:run AND recipient_seq>:previous "
                        "AND recipient_seq<=:pending ORDER BY recipient_seq FOR UPDATE",
                        self._schema,
                    ),
                    {"scope": scope, "run": identity, "previous": previous, "pending": pending},
                )
            )
            .mappings()
            .all()
        )
        if len(messages) != pending - previous:
            errors.version_conflict("root notification sequence has a gap")
        allowed = {
            "task.accepted",
            "task.rejected",
            "input.append",
            "clarification.request",
            "progress.updated",
            "task.failed",
        }
        previous_applied = previous
        for sequence, row in enumerate(messages, start=previous + 1):
            if row["recipient_seq"] != sequence:
                errors.version_conflict("root notification sequence is not contiguous")
            if row["kind"] not in allowed:
                break
            if row["status"] != "pending" or row["deadline_at"] <= now:
                errors.control_pending("root input requires ordered host handling")
            parse_agent_message_row(dict(row))
            previous_applied = sequence
        if not messages or messages[0]["kind"] not in allowed:
            return
        pending = previous_applied
        await db.execute(
            scoped(
                "UPDATE {t}agent_message SET status='applied',applied_checkpoint_id=:checkpoint "
                "WHERE scope_id=:scope AND recipient_run_id=:run AND recipient_seq>:previous "
                "AND recipient_seq<=:pending",
                self._schema,
            ),
            {
                "scope": scope,
                "run": identity,
                "previous": previous,
                "pending": pending,
                "checkpoint": run["checkpoint_id"],
            },
        )
        for table, selector in (("mailbox_cursor", "recipient_run_id"), ("run", "id")):
            column = "applied_seq" if table == "mailbox_cursor" else "applied_input_version"
            await db.execute(
                scoped(
                    f"UPDATE {{t}}{table} SET {column}=:pending,updated_at=:now "
                    f"WHERE scope_id=:scope AND {selector}=:run",
                    self._schema,
                ),
                {"scope": scope, "run": identity, "pending": pending, "now": now},
            )
        run["applied_input_version"] = pending


__all__ = ["LoadedMainRun", "MainAgentBinding", "SqlMainRunLoader"]

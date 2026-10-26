"""Current Run authority and immutable Agent binding checks for Skill reads."""

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.agent import Agent
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.uow import errors, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlRunSkillAccess:
    """Loading bytes grants no tool effects; ToolHost still authorizes every execution."""

    def __init__(self, engine: AsyncEngine, schema: str) -> None:
        self._schema = require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._locks = locks.build_lock_statements(self._schema)

    async def require_skill(self, actor: ActorContext, run_id: UUID, ref: CapabilityRef) -> None:
        async with self._sessions() as db, db.begin():
            session_id = (
                await db.execute(
                    self._locks.run_session, {"scope_id": actor.scope_id, "run_id": run_id}
                )
            ).scalar_one_or_none()
            if session_id is None:
                errors.forbidden("Skill Run is unavailable")
            await locks.lock_slot(db, self._locks, actor.scope_id, session_id)
            run = await locks.lock_run(db, self._locks, actor.scope_id, run_id)
            if run is None or run["session_id"] != session_id:
                errors.forbidden("Skill Run is unavailable")
            now = datetime.now(UTC)
            await revalidate_authority(db, self._locks, actor, run=run, now=now)
            if (
                run["status"] != "running"
                or run["desired_control"] is not None
                or now >= run["deadline_at"]
            ):
                errors.control_pending("Skill Run is not ready to execute")
            row = (
                await db.execute(
                    scoped(
                        "SELECT manifest,content_hash FROM {t}agent_revision "
                        "WHERE scope_id=:scope AND id=:revision",
                        self._schema,
                    ),
                    {"scope": actor.scope_id, "revision": run["agent_revision_id"]},
                )
            ).mappings().one_or_none()
            if row is None or content_digest(row["manifest"]) != row["content_hash"]:
                errors.version_conflict("Run Agent manifest integrity check failed")
            agent = Agent.model_validate(row["manifest"])
            if ref not in agent.skills:
                errors.forbidden("Skill revision is not bound to the Run Agent")


__all__ = ["SqlRunSkillAccess"]

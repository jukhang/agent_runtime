"""Resolve SQL Skill bundles using the current claim's explicit Run identity."""

from uuid import UUID

from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.skills.git import VerifiedSkillBundle
from agent_runtime.storage.main_factory import MainAgentProfile
from agent_runtime.storage.skill_bundles import SqlSkillBundleStore


class SqlSkillExtensionResolver:
    def __init__(self, store: SqlSkillBundleStore) -> None:
        self._store = store

    async def skills(
        self, actor: ActorContext, run_id: UUID, profile: MainAgentProfile
    ) -> dict[CapabilityRef, VerifiedSkillBundle]:
        return {
            ref: await self._store.load(actor, run_id, ref)
            for ref in profile.installed.agent.skills
        }


__all__ = ["SqlSkillExtensionResolver"]

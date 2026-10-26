"""Install immutable host Agent revisions without granting execution authority."""

from uuid import NAMESPACE_URL, UUID, uuid5

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.agent import Agent
from agent_runtime.contracts.canonical import canonical_json, content_digest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.storage.models.registry import AgentRevisionRow, AgentRow
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema


class SqlAgentInstaller:
    """A host explicitly owns one namespace in this registry.

    AgentRow predates namespaced capabilities and stores only a key. Namespace is
    included in stable IDs so an accidental second namespace fails closed rather
    than reusing another installation.
    """

    def __init__(self, engine: AsyncEngine, schema: str, *, namespace: str) -> None:
        self._namespace = namespace
        self._sessions = async_sessionmaker(
            engine.execution_options(schema_translate_map={None: require_schema(schema)}),
            expire_on_commit=False,
        )

    async def install(self, scope_id: str, ref: CapabilityRef, agent: Agent) -> UUID:
        if ref.key.namespace != self._namespace or ref.key.key != agent.id:
            errors.invalid_input("Agent installation identity differs from its host namespace")
        agent_id = uuid5(
            NAMESPACE_URL, canonical_json(["agent", scope_id, ref.key.namespace, ref.key.key])
        )
        revision_id = uuid5(agent_id, ref.revision)
        manifest = agent.model_dump(mode="json", by_alias=True)
        digest = content_digest(manifest)
        async with self._sessions() as db, db.begin():
            await db.execute(
                insert(AgentRow)
                .values(id=agent_id, scope_id=scope_id, key=agent.id)
                .on_conflict_do_nothing()
            )
            installed_id = (
                await db.execute(
                    sa.select(AgentRow.id)
                    .where(AgentRow.scope_id == scope_id, AgentRow.key == agent.id)
                    .with_for_update()
                )
            ).scalar_one()
            if installed_id != agent_id:
                errors.version_conflict("Agent key is owned by another namespace or installation")
            await db.execute(
                insert(AgentRevisionRow)
                .values(
                    id=revision_id,
                    scope_id=scope_id,
                    agent_id=agent_id,
                    revision=int(ref.revision),
                    manifest=manifest,
                    content_hash=digest,
                )
                .on_conflict_do_nothing()
            )
            row = (
                await db.execute(
                    sa.select(AgentRevisionRow).where(
                        AgentRevisionRow.scope_id == scope_id,
                        AgentRevisionRow.agent_id == agent_id,
                        AgentRevisionRow.revision == int(ref.revision),
                    )
                )
            ).scalar_one()
            if row.id != revision_id or row.manifest != manifest or row.content_hash != digest:
                errors.version_conflict("An immutable Agent revision already has different content")
            # Do not downgrade the active pointer when an older pinned revision is installed.
            current = (
                await db.execute(
                    sa.select(AgentRow.published_revision_id).where(AgentRow.id == agent_id)
                )
            ).scalar_one()
            current_revision = (
                None
                if current is None
                else (
                    await db.execute(
                        sa.select(AgentRevisionRow.revision).where(AgentRevisionRow.id == current)
                    )
                ).scalar_one()
            )
            if current_revision is None or current_revision < int(ref.revision):
                await db.execute(
                    sa.update(AgentRow)
                    .where(AgentRow.id == agent_id)
                    .values(published_revision_id=revision_id, updated_at=sa.func.now())
                )
        return revision_id


__all__ = ["SqlAgentInstaller"]

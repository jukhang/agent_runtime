"""Durable capability revisions and run-bound ToolSpec resolution."""

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.contracts.canonical import canonical_json, content_digest
from agent_runtime.contracts.decisions import ToolCallRequest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.registry import CapabilityManifest, ToolCapability
from agent_runtime.contracts.tools import ToolSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.models.registry import CapabilityRevisionRow, CapabilityRow
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema


class CapabilityAuthority(Protocol):
    async def manage(self, actor: ActorContext, ref: CapabilityRef) -> None:
        """Recheck host permission to install, publish or revoke this revision."""
        ...

    async def use_tool(self, actor: ActorContext, run_id: UUID, ref: CapabilityRef) -> None:
        """Recheck current Run binding, ACL and authorization revision."""
        ...


def _capability_id(scope_id: str, ref: CapabilityRef) -> UUID:
    return uuid5(
        NAMESPACE_URL,
        canonical_json(["capability", scope_id, ref.key.namespace, ref.key.key]),
    )


def _revision_id(scope_id: str, ref: CapabilityRef) -> UUID:
    return uuid5(_capability_id(scope_id, ref), ref.revision)


@dataclass(frozen=True, slots=True)
class ToolCatalog:
    specs: tuple[ToolSpec, ...]
    version: str


class SqlCapabilityRegistry:
    """Published manifests live in SQL; handlers and credentials remain host owned."""

    def __init__(self, engine: AsyncEngine, schema: str, *, authority: CapabilityAuthority) -> None:
        self.engine, self.schema = engine, require_schema(schema)
        self._authority = authority
        self._sessions = async_sessionmaker(
            engine.execution_options(schema_translate_map={None: self.schema}),
            expire_on_commit=False,
        )

    async def require_manage(self, actor: ActorContext, ref: CapabilityRef) -> None:
        await self._authority.manage(actor, ref)

    async def install(self, actor: ActorContext, manifest: CapabilityManifest) -> UUID:
        async with self._sessions() as db, db.begin():
            return await self.install_in_transaction(db, actor, manifest)

    async def install_in_transaction(
        self, db: AsyncSession, actor: ActorContext, manifest: CapabilityManifest
    ) -> UUID:
        """Install with related immutable bytes in the caller's schema-bound transaction."""
        if not db.in_transaction():
            raise ValueError("Capability installation requires an active transaction")
        if manifest.status != "draft":
            errors.invalid_input("Capabilities must be installed as drafts")
        await self._authority.manage(actor, manifest.ref)
        ref = manifest.ref
        identity = _capability_id(actor.scope_id, ref)
        revision_id = _revision_id(actor.scope_id, ref)
        payload = manifest.model_dump(mode="json", by_alias=True)
        await db.execute(
            insert(CapabilityRow)
            .values(
                id=identity,
                scope_id=actor.scope_id,
                namespace=ref.key.namespace,
                key=ref.key.key,
                kind=manifest.spec.kind,
            )
            .on_conflict_do_nothing()
        )
        owner = (
            await db.execute(
                sa.select(CapabilityRow)
                .where(
                    CapabilityRow.scope_id == actor.scope_id,
                    CapabilityRow.namespace == ref.key.namespace,
                    CapabilityRow.key == ref.key.key,
                )
                .with_for_update()
            )
        ).scalar_one()
        if owner.id != identity or owner.kind != manifest.spec.kind:
            errors.version_conflict("Capability identity or kind conflicts")
        await db.execute(
            insert(CapabilityRevisionRow)
            .values(
                id=revision_id,
                scope_id=actor.scope_id,
                capability_id=identity,
                revision=int(ref.revision),
                content_digest=manifest.content_digest,
                status="draft",
                manifest=payload,
            )
            .on_conflict_do_nothing()
        )
        revision = (
            await db.execute(
                sa.select(CapabilityRevisionRow).where(
                    CapabilityRevisionRow.scope_id == actor.scope_id,
                    CapabilityRevisionRow.capability_id == identity,
                    CapabilityRevisionRow.revision == int(ref.revision),
                )
            )
        ).scalar_one()
        if (
            revision.id != revision_id
            or revision.content_digest != manifest.content_digest
            or revision.manifest.get("spec") != payload["spec"]
        ):
            errors.version_conflict("Immutable capability revision has different content")
        return revision_id

    async def publish(self, actor: ActorContext, ref: CapabilityRef) -> CapabilityManifest:
        await self._authority.manage(actor, ref)
        async with self._sessions() as db, db.begin():
            identity = _capability_id(actor.scope_id, ref)
            owner = (
                await db.execute(
                    sa.select(CapabilityRow)
                    .where(CapabilityRow.scope_id == actor.scope_id, CapabilityRow.id == identity)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if owner is None:
                errors.forbidden("Capability is not installed")
            revision = (
                await db.execute(
                    sa.select(CapabilityRevisionRow)
                    .where(
                        CapabilityRevisionRow.scope_id == actor.scope_id,
                        CapabilityRevisionRow.capability_id == identity,
                        CapabilityRevisionRow.revision == int(ref.revision),
                    )
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if revision is None or revision.status == "revoked":
                errors.forbidden("Capability revision is unavailable")
            if revision.status != "active":
                active = (
                    (
                        await db.execute(
                            sa.select(CapabilityRevisionRow).where(
                                CapabilityRevisionRow.scope_id == actor.scope_id,
                                CapabilityRevisionRow.capability_id == identity,
                                CapabilityRevisionRow.status == "active",
                            )
                        )
                    )
                    .scalars()
                    .one_or_none()
                )
                if active is not None:
                    old = dict(active.manifest)
                    old["status"] = "superseded"
                    await db.execute(
                        sa.update(CapabilityRevisionRow)
                        .where(CapabilityRevisionRow.id == active.id)
                        .values(status="superseded", manifest=old, updated_at=sa.func.now())
                    )
                current = dict(revision.manifest)
                current["status"] = "active"
                await db.execute(
                    sa.update(CapabilityRevisionRow)
                    .where(CapabilityRevisionRow.id == revision.id)
                    .values(status="active", manifest=current, updated_at=sa.func.now())
                )
                revision.manifest = current
            return CapabilityManifest.model_validate(revision.manifest)

    async def revoke(self, actor: ActorContext, ref: CapabilityRef) -> None:
        await self._authority.manage(actor, ref)
        async with self._sessions() as db, db.begin():
            identity = _capability_id(actor.scope_id, ref)
            owner = (
                await db.execute(
                    sa.select(CapabilityRow.id)
                    .where(CapabilityRow.scope_id == actor.scope_id, CapabilityRow.id == identity)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if owner is None:
                errors.forbidden("Capability is not installed")
            row = (
                await db.execute(
                    sa.select(CapabilityRevisionRow).where(
                        CapabilityRevisionRow.scope_id == actor.scope_id,
                        CapabilityRevisionRow.capability_id == identity,
                        CapabilityRevisionRow.revision == int(ref.revision),
                    )
                )
            ).scalar_one_or_none()
            if row is None:
                errors.forbidden("Capability revision is unavailable")
            if row.status != "revoked":
                current = dict(row.manifest)
                current["status"] = "revoked"
                await db.execute(
                    sa.update(CapabilityRevisionRow)
                    .where(CapabilityRevisionRow.id == row.id)
                    .values(status="revoked", manifest=current, updated_at=sa.func.now())
                )

    async def resolve_tool(self, actor: ActorContext, run_id: UUID, ref: CapabilityRef) -> ToolSpec:
        await self._authority.use_tool(actor, run_id, ref)
        async with self._sessions() as db:
            row = (
                await db.execute(
                    sa.select(CapabilityRevisionRow, CapabilityRow.kind)
                    .join(
                        CapabilityRow,
                        sa.and_(
                            CapabilityRow.scope_id == CapabilityRevisionRow.scope_id,
                            CapabilityRow.id == CapabilityRevisionRow.capability_id,
                        ),
                    )
                    .where(
                        CapabilityRevisionRow.scope_id == actor.scope_id,
                        CapabilityRevisionRow.id == _revision_id(actor.scope_id, ref),
                    )
                )
            ).one_or_none()
        if row is None or row[1] != "tool" or row[0].status not in {"active", "superseded"}:
            errors.forbidden("Tool revision is unavailable")
        manifest = CapabilityManifest.model_validate(row[0].manifest)
        if manifest.ref != ref or not isinstance(manifest.spec, ToolCapability):
            errors.version_conflict("Tool registry manifest does not match its identity")
        return manifest.spec.spec

    async def active_tools(
        self, actor: ActorContext, run_id: UUID, allowed: Iterable[CapabilityRef]
    ) -> ToolCatalog:
        specs: list[ToolSpec] = []
        refs = tuple(allowed)
        if len(refs) != len(set(refs)):
            errors.invalid_input("Duplicate Tool bindings")
        for ref in refs:
            await self._authority.use_tool(actor, run_id, ref)
        by_id = {_revision_id(actor.scope_id, ref): ref for ref in refs}
        async with self._sessions() as db:
            rows = (
                (
                    await db.execute(
                        sa.select(CapabilityRevisionRow).where(
                            CapabilityRevisionRow.scope_id == actor.scope_id,
                            CapabilityRevisionRow.id.in_(by_id),
                            CapabilityRevisionRow.status == "active",
                        )
                    )
                )
                .scalars()
                .all()
            )
            for row in rows:
                manifest = CapabilityManifest.model_validate(row.manifest)
                if not isinstance(manifest.spec, ToolCapability) or manifest.ref != by_id[row.id]:
                    errors.version_conflict("Tool registry manifest conflicts with binding")
                specs.append(manifest.spec.spec)
        groups: dict[str, int] = {}
        for spec in specs:
            previous = groups.setdefault(spec.concurrency_group, spec.max_concurrency)
            if previous != spec.max_concurrency:
                errors.version_conflict("Shared Tool concurrency group has conflicting limits")
        specs.sort(
            key=lambda item: (item.ref.key.namespace, item.ref.key.key, int(item.ref.revision))
        )
        return ToolCatalog(
            specs=tuple(specs),
            version=content_digest([spec.model_dump(mode="json", by_alias=True) for spec in specs]),
        )


class PinnedToolSpecResolver:
    """Bind one trusted actor/Run to the dispatcher ToolSpecResolver port."""

    def __init__(
        self,
        registry: SqlCapabilityRegistry,
        actor: ActorContext,
        run_id: UUID,
        pinned_refs: Iterable[CapabilityRef],
    ) -> None:
        self._registry, self._actor, self._run_id = registry, actor, run_id
        self._pinned_refs = frozenset(pinned_refs)

    async def get(self, call: ToolCallRequest) -> ToolSpec:
        if call.tool not in self._pinned_refs:
            errors.forbidden("Tool is not pinned to this Run")
        return await self._registry.resolve_tool(self._actor, self._run_id, call.tool)

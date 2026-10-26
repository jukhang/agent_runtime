"""Persist verified Skill bytes and reload pinned revisions for a claimed Run."""

import hashlib
from typing import Protocol
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.registry import CapabilityManifest, SkillCapability, SkillSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.skills.git import GitSkillInstaller, VerifiedSkillBundle
from agent_runtime.storage.capability_registry import SqlCapabilityRegistry, _revision_id
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.models.registry import CapabilityRevisionRow, CapabilityRow
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema


class SkillAccess(Protocol):
    async def require_skill(self, actor: ActorContext, run_id: UUID, ref: CapabilityRef) -> None:
        """Recheck current Run binding, actor ACL and authorization revision."""


class SqlSkillBundleStore:
    """Skill manifests are SQL revisions; executable bytes live in scoped immutable blobs."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        registry: SqlCapabilityRegistry,
        access: SkillAccess,
        verifier: GitSkillInstaller | None = None,
    ) -> None:
        if registry.engine is not engine or registry.schema != require_schema(schema):
            raise ValueError("Skill store and Registry must use the same engine and schema")
        self.engine, self.schema = engine, require_schema(schema)
        self._sessions = async_sessionmaker(
            engine.execution_options(schema_translate_map={None: require_schema(schema)}),
            expire_on_commit=False,
        )
        self._blobs = SqlContextBlobBackend(engine, schema)
        self._registry, self._access = registry, access
        self._verifier = verifier or GitSkillInstaller()

    async def install(self, actor: ActorContext, bundle: VerifiedSkillBundle) -> UUID:
        bundle = VerifiedSkillBundle(bundle.spec, dict(bundle.files))
        self._verifier.verify_bundle(bundle)
        await self._registry.require_manage(actor, bundle.spec.ref)
        files = []
        for item in bundle.spec.files:
            data = bundle.files[item.path]
            key = self._blob_key(actor.scope_id, data)
            files.append(item.model_copy(update={"blob_ref": key}))
        spec = bundle.spec.model_copy(update={"files": tuple(files)})
        manifest = CapabilityManifest(
            ref=spec.ref,
            status="draft",
            content_digest=content_digest(spec.model_dump(mode="json", by_alias=True)),
            spec=SkillCapability(kind="skill", spec=spec),
        )
        async with self._sessions() as db, db.begin():
            for item in spec.files:
                await self._blobs.put_in_transaction(
                    db, actor.scope_id, item.blob_ref, bundle.files[item.path]
                )
            return await self._registry.install_in_transaction(db, actor, manifest)

    async def load(
        self, actor: ActorContext, run_id: UUID, ref: CapabilityRef
    ) -> VerifiedSkillBundle:
        await self._access.require_skill(actor, run_id, ref)
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
        if row is None or row[1] != "skill" or row[0].status not in {"active", "superseded"}:
            errors.forbidden("Skill revision is unavailable")
        manifest = CapabilityManifest.model_validate(row[0].manifest)
        if (
            manifest.ref != ref
            or manifest.status != row[0].status
            or manifest.content_digest != row[0].content_digest
            or not isinstance(manifest.spec, SkillCapability)
        ):
            errors.version_conflict("Skill manifest differs from the requested revision")
        spec: SkillSpec = manifest.spec.spec
        try:
            self._verifier.verify_manifest(spec)
        except ValueError as exc:
            errors.version_conflict(f"Skill manifest integrity check failed: {exc}")
        files: dict[str, bytes] = {}
        for item in spec.files:
            data = await self._blobs.get(actor.scope_id, item.blob_ref)
            if self._blob_key(actor.scope_id, data) != item.blob_ref:
                errors.version_conflict("Skill file references a different scoped blob")
            files[item.path] = data
        try:
            return self._verifier.verify_bundle(VerifiedSkillBundle(spec=spec, files=files))
        except ValueError as exc:
            errors.version_conflict(f"Skill bundle integrity check failed: {exc}")

    @staticmethod
    def _blob_key(scope_id: str, data: bytes) -> str:
        return (
            f"context-{hashlib.sha256(scope_id.encode('utf-8')).hexdigest()}-"
            f"{hashlib.sha256(data).hexdigest()}"
        )


__all__ = ["SkillAccess", "SqlSkillBundleStore"]

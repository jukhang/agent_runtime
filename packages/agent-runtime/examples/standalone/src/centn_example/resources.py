"""Small SQL resource host: immutable bytes, explicit ACL, no implicit RAG framework."""

import hashlib
from collections.abc import Mapping
from typing import Annotated, Any, Literal, Self
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import StringConstraints, model_validator
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.context_runtime import LoadedSource
from agent_runtime.contracts.base import Contract, MachineKey
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.context import ContextRequest
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.child_access import require_child_resources
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.context_sources import SqlRuntimeSourceReader
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import scoped
from centn_example.resource_migrate import resource_schema


class ResourceCreate(Contract):
    session_id: UUID
    idempotency_key: MachineKey
    kind: Literal["attachment", "knowledge", "report"] = "attachment"
    title: Annotated[str, StringConstraints(min_length=1, max_length=160)]
    text: Annotated[str, StringConstraints(min_length=1, max_length=1048576)]

    @model_validator(mode="after")
    def safe_content(self) -> Self:
        if not self.title.strip() or any(ord(c) < 32 or c in "/\\" for c in self.title):
            raise ValueError("title must be a display name, not a path or control sequence")
        if not self.text.strip() or "\x00" in self.text or len(self.text.encode()) > 1048576:
            raise ValueError("text must be nonblank UTF-8 within 1 MiB")
        return self


class ResourceRead(Contract):
    resource_ref: ResourceRef


class ResourceView(Contract):
    resource_ref: ResourceRef
    title: str
    mime: str
    size_bytes: int
    digest: str
    kind: Literal["attachment", "knowledge", "report"]


class Resources:
    def __init__(self, engine: AsyncEngine, runtime_schema: str) -> None:
        self.engine, self.runtime_schema = engine, runtime_schema
        self.schema = resource_schema(runtime_schema)
        self.sessions = async_sessionmaker(engine, expire_on_commit=False)
        self.runtime = SqlRuntimeSourceReader(
            engine, runtime_schema, blobs=SqlContextBlobBackend(engine, runtime_schema)
        )

    async def create(self, actor: ActorContext, request: ResourceCreate) -> ResourceView:
        async with self.sessions() as db, db.begin():
            return await self.create_in_transaction(db, actor, request)

    async def create_in_transaction(
        self, db: AsyncSession, actor: ActorContext, request: ResourceCreate
    ) -> ResourceView:
        """Participate in the caller's effect ledger transaction, never commit here."""
        request = ResourceCreate.model_validate(request.model_dump())
        return await self.create_content_in_transaction(
            db, actor, request, request.text.encode(), "text/plain; charset=utf-8"
        )

    async def create_content_in_transaction(
        self,
        db: AsyncSession,
        actor: ActorContext,
        request: ResourceCreate,
        raw: bytes,
        mime: str,
    ) -> ResourceView:
        """Trusted sealed-output port; never exposed as a binary upload endpoint."""
        if not db.in_transaction() or not 1 <= len(raw) <= 1048576:
            raise ValueError("Bounded content and an active transaction are required")
        if mime not in {"text/plain; charset=utf-8", "image/png"}:
            raise ValueError("Unsupported sealed output MIME")
        if mime == "image/png" and (
            request.kind != "report" or not raw.startswith(b"\x89PNG\r\n\x1a\n")
        ):
            raise ValueError("Only sealed PNG reports are accepted")
        identity = uuid5(
            NAMESPACE_URL,
            f"host-resource:{actor.scope_id}:{actor.actor_id}:{request.idempotency_key}",
        )
        digest = hashlib.sha256(raw).hexdigest()
        request_hash = (
            content_digest(request.model_dump(mode="json", by_alias=True))
            if mime.startswith("text/") and raw == request.text.encode()
            else content_digest(
                {
                    "request": request.model_dump(mode="json", by_alias=True),
                    "digest": digest,
                    "mime": mime,
                }
            )
        )
        params = {
            "scope": actor.scope_id,
            "id": identity,
            "actor": actor.actor_id,
            "session": request.session_id,
            "kind": request.kind,
            "title": request.title,
            "body": raw,
            "digest": digest,
            "size": len(raw),
            "hash": request_hash,
            "mime": mime,
        }
        await self.runtime.authorize_session_in_transaction(db, actor, request.session_id)
        await db.execute(
            scoped(
                "INSERT INTO {t}resource (scope_id,id,owner_actor_id,session_id,"
                "version,kind,title,"
                "mime,content,digest,request_hash,size_bytes) "
                "VALUES (:scope,:id,:actor,:session,1,"
                ":kind,:title,:mime,:body,:digest,:hash,:size) "
                "ON CONFLICT DO NOTHING",
                self.schema,
            ),
            params,
        )
        row = await self._row(db, actor, identity, session_id=request.session_id)
        if row["request_hash"] != request_hash:
            errors.version_conflict("Resource idempotency key has another payload")
        return self._view(row)

    async def list(self, actor: ActorContext, session_id: UUID) -> tuple[ResourceView, ...]:
        async with self.sessions() as db, db.begin():
            await self.runtime.authorize_session_in_transaction(db, actor, session_id)
            rows = (
                (
                    await db.execute(
                        scoped(
                            "SELECT scope_id,id,owner_actor_id,session_id,version,kind,title,mime,"
                            "size_bytes,digest FROM {t}resource WHERE scope_id=:scope "
                            "AND owner_actor_id=:actor AND session_id=:session "
                            "AND revoked_at IS NULL ORDER BY created_at,id LIMIT 100",
                            self.schema,
                        ),
                        {"scope": actor.scope_id, "actor": actor.actor_id, "session": session_id},
                    )
                )
                .mappings()
                .all()
            )
            return tuple(self._view(row) for row in rows)

    async def get(self, actor: ActorContext, ref: ResourceRef) -> tuple[ResourceView, bytes]:
        async with self.sessions() as db, db.begin():
            row = await self.require(db, actor, ref)
            return self._view(row), bytes(row["content"])

    async def revoke(self, actor: ActorContext, ref: ResourceRef) -> None:
        async with self.sessions() as db, db.begin():
            row = await self.require(db, actor, ref, for_update=True)
            await db.execute(
                scoped(
                    "UPDATE {t}resource SET revoked_at=CURRENT_TIMESTAMP "
                    "WHERE scope_id=:scope AND id=:id",
                    self.schema,
                ),
                {"scope": actor.scope_id, "id": row["id"]},
            )

    async def require(
        self,
        db: AsyncSession,
        actor: ActorContext,
        ref: ResourceRef,
        *,
        session_id: UUID | None = None,
        for_update: bool = False,
    ) -> Mapping[str, Any]:
        if (
            ref.scope.scope_id != actor.scope_id
            or ref.scope.owner_id != actor.actor_id
            or ref.scope.session_id is None
            or ref.resource_version != "1"
        ):
            errors.forbidden("Resource scope, owner or version is unavailable")
        try:
            identity, session = UUID(ref.resource_id), UUID(ref.scope.session_id)
        except ValueError:
            errors.forbidden("Resource identity is invalid")
        if session_id is not None and session_id != session:
            errors.forbidden("Resource is not in this Session")
        await self.runtime.authorize_session_in_transaction(db, actor, session)
        row = await self._row(db, actor, identity, session_id=session, for_update=for_update)
        if ref != self._view(row).resource_ref:
            errors.forbidden("Resource reference differs from its stored identity")
        return row

    async def _row(
        self,
        db: AsyncSession,
        actor: ActorContext,
        identity: UUID,
        *,
        session_id: UUID,
        for_update: bool = False,
    ) -> Mapping[str, Any]:
        row = (
            (
                await db.execute(
                    scoped(
                        "SELECT * FROM {t}resource WHERE scope_id=:scope AND id=:id "
                        + ("FOR UPDATE" if for_update else "FOR SHARE"),
                        self.schema,
                    ),
                    {"scope": actor.scope_id, "id": identity},
                )
            )
            .mappings()
            .one_or_none()
        )
        if (
            row is None
            or row["owner_actor_id"] != actor.actor_id
            or row["session_id"] != session_id
            or row["revoked_at"] is not None
        ):
            errors.forbidden("Resource is unavailable or revoked")
        data = bytes(row["content"])
        if len(data) != row["size_bytes"] or hashlib.sha256(data).hexdigest() != row["digest"]:
            errors.invalid_input("Stored resource content failed integrity verification")
        return row

    def _view(self, row: Mapping[str, Any]) -> ResourceView:
        return ResourceView(
            resource_ref=ResourceRef(
                resource_type=f"host_{row['kind']}",
                resource_id=str(row["id"]),
                resource_version=str(row["version"]),
                scope=ResourceScope(
                    scope_id=row["scope_id"],
                    session_id=str(row["session_id"]),
                    owner_id=row["owner_actor_id"],
                ),
            ),
            title=row["title"],
            mime=row["mime"],
            size_bytes=row["size_bytes"],
            digest=row["digest"],
            kind=row["kind"],
        )


class Sources:
    """All host reads reuse the SQL ACL in the caller's transaction when committing effects."""

    def __init__(self, resources: Resources) -> None:
        self.resources, self.runtime = resources, resources.runtime

    async def authorize_session(self, actor: ActorContext, session_id: UUID) -> None:
        await self.runtime.authorize_session(actor, session_id)

    async def authorize_session_in_transaction(
        self, db: AsyncSession, actor: ActorContext, session_id: UUID
    ) -> None:
        await self.runtime.authorize_session_in_transaction(db, actor, session_id)

    async def authorize_resource(self, actor: ActorContext, ref: ResourceRef) -> None:
        if ref.resource_type in {"host_attachment", "host_knowledge", "host_report"}:
            await self.resources.get(actor, ref)
        else:
            await self.runtime.authorize_resource(actor, ref)

    async def authorize_resource_in_transaction(
        self, db: AsyncSession, actor: ActorContext, ref: ResourceRef, *, session_id: UUID
    ) -> None:
        if ref.resource_type in {"host_attachment", "host_knowledge", "host_report"}:
            await self.resources.require(db, actor, ref, session_id=session_id)
        else:
            await self.runtime.authorize_resource_in_transaction(
                db, actor, ref, session_id=session_id
            )

    async def read(self, request: ContextRequest, ref: ResourceRef) -> LoadedSource:
        if ref.resource_type not in {"host_attachment", "host_knowledge", "host_report"}:
            return await self.runtime.read(request, ref)
        async with self.resources.sessions() as db, db.begin():
            await require_child_resources(
                db, self.resources.runtime_schema, request.actor, request.run_id, (ref,)
            )
            row = await self.resources.require(
                db, request.actor, ref, session_id=request.session_id
            )
            if row["mime"] == "image/png":
                return LoadedSource(
                    resource=ref, content=row["title"] + " (PNG chart)", required=True
                )
            return LoadedSource(
                resource=ref,
                content=row["title"] + "\n" + bytes(row["content"]).decode("utf-8"),
                required=True,
            )

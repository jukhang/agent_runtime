"""Authorized, version-pinned Runtime goal and plan context sources."""

from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.context_runtime import LoadedSource
from agent_runtime.contracts.canonical import canonical_json
from agent_runtime.contracts.context import ContextRequest
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.requests import SubmitRequest
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlRuntimeSourceReader:
    """Owner-only ACL until explicit sharing grants are introduced."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        blobs: SqlContextBlobBackend,
        optional_history: bool = False,
    ) -> None:
        schema = require_schema(schema)
        self._schema = schema
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._blobs = blobs
        self._optional_history = optional_history
        self._session = scoped(
            "SELECT owner_actor_id, auth_revision, deleted_at FROM {t}session "
            "WHERE scope_id = :scope_id AND id = :session_id",
            schema,
        )
        self._goal = scoped(
            "SELECT g.revision, g.objective, g.constraints, g.focus_refs, "
            "r.session_id, s.owner_actor_id, s.auth_revision, s.deleted_at "
            "FROM {t}goal_revision g "
            "JOIN {t}task t ON t.scope_id = g.scope_id AND t.id = g.task_id "
            "JOIN {t}run r ON r.scope_id = t.scope_id AND r.id = t.root_run_id "
            "JOIN {t}session s ON s.scope_id = r.scope_id AND s.id = r.session_id "
            "WHERE g.scope_id = :scope_id AND g.id = :resource_id",
            schema,
        )
        self._plan = scoped(
            "SELECT pr.revision, pr.steps, pr.public_reason, "
            "r.session_id, s.owner_actor_id, s.auth_revision, s.deleted_at "
            "FROM {t}plan_revision pr "
            "JOIN {t}plan p ON p.scope_id = pr.scope_id AND p.id = pr.plan_id "
            "JOIN {t}task t ON t.scope_id = p.scope_id AND t.id = p.task_id "
            "JOIN {t}run r ON r.scope_id = t.scope_id AND r.id = t.root_run_id "
            "JOIN {t}session s ON s.scope_id = r.scope_id AND s.id = r.session_id "
            "WHERE pr.scope_id = :scope_id AND pr.id = :resource_id",
            schema,
        )
        self._tool_result = scoped(
            "SELECT tc.status, tc.result_ref, r.session_id, s.owner_actor_id, "
            "s.auth_revision, s.deleted_at FROM {t}tool_call tc "
            "JOIN {t}run r ON r.scope_id = tc.scope_id AND r.id = tc.run_id "
            "JOIN {t}session s ON s.scope_id = r.scope_id AND s.id = r.session_id "
            "WHERE tc.scope_id = :scope_id AND tc.id = :resource_id",
            schema,
        )
        self._output = scoped(
            "SELECT i.final_revision AS revision,i.status,i.content_ref AS result_ref,"
            "i.source_seq AS context_priority,"
            "r.id AS source_run_id,r.session_id,s.owner_actor_id,s.auth_revision,s.deleted_at "
            "FROM {t}timeline_item i JOIN {t}run r ON r.scope_id=i.scope_id AND r.id=i.run_id "
            "JOIN {t}session s ON s.scope_id=r.scope_id AND s.id=r.session_id "
            "WHERE i.scope_id=:scope_id AND i.id=:resource_id "
            "AND EXISTS (SELECT 1 FROM {t}output_seal os WHERE os.scope_id=i.scope_id "
            "AND os.item_id=i.id AND os.run_id=i.run_id AND os.sealed_revision=i.final_revision "
            "AND EXISTS (SELECT 1 FROM {t}model_attempt m WHERE m.scope_id=os.scope_id "
            "AND m.id=os.model_attempt_id AND m.run_id=i.run_id AND m.status='completed'))",
            schema,
        )
        self._turn_input = scoped(
            "SELECT 0 AS revision,t.input_ref AS result_ref,r.id AS source_run_id,r.session_id,"
            "(SELECT max(u.source_seq) FROM {t}timeline_item u WHERE u.scope_id=t.scope_id "
            "AND u.run_id=t.primary_run_id AND u.item_type='user_message') AS context_priority,"
            "s.owner_actor_id,s.auth_revision,s.deleted_at "
            "FROM {t}turn t JOIN {t}run r ON r.scope_id=t.scope_id AND r.id=t.primary_run_id "
            "JOIN {t}session s ON s.scope_id=r.scope_id AND s.id=r.session_id "
            "WHERE t.scope_id=:scope_id AND t.id=:resource_id",
            schema,
        )
        self._interaction = scoped(
            "SELECT i.final_revision AS revision,i.status,i.content_ref AS result_ref,"
            "i.source_seq AS context_priority,r.id AS source_run_id,r.session_id,"
            "s.owner_actor_id,s.auth_revision,s.deleted_at FROM {t}timeline_item i "
            "JOIN {t}run r ON r.scope_id=i.scope_id AND r.id=i.run_id "
            "JOIN {t}session s ON s.scope_id=r.scope_id AND s.id=r.session_id "
            "WHERE i.scope_id=:scope_id AND i.id=:resource_id AND i.item_type='user_reply'",
            schema,
        )

    async def authorize_session(self, actor: ActorContext, session_id: UUID) -> None:
        async with self._sessions() as session:
            await self.authorize_session_in_transaction(session, actor, session_id)

    async def authorize_session_in_transaction(
        self, session: AsyncSession, actor: ActorContext, session_id: UUID
    ) -> None:
        row = (
            (
                await session.execute(
                    sa.text(self._session.text + " FOR SHARE"),
                    {"scope_id": actor.scope_id, "session_id": session_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None or row["deleted_at"] is not None or row["owner_actor_id"] != actor.actor_id:
            raise self._denied()
        if row["auth_revision"] != int(actor.auth_revision):
            raise self._stale()

    async def authorize_resource(self, actor: ActorContext, resource: ResourceRef) -> None:
        await self._load(actor, resource, session_id=None)

    async def authorize_resource_in_transaction(
        self,
        session: AsyncSession,
        actor: ActorContext,
        resource: ResourceRef,
        *,
        session_id: UUID,
    ) -> None:
        await self._load(actor, resource, session_id=session_id, transaction=session)

    async def read(self, request: ContextRequest, resource: ResourceRef) -> LoadedSource:
        from agent_runtime.storage.child_access import require_child_resources

        async with self._sessions() as session:
            await require_child_resources(
                session, self._schema, request.actor, request.run_id, (resource,)
            )
        row = await self._load(request.actor, resource, session_id=request.session_id)
        if resource.resource_type == "goal_revision":
            content = canonical_json(
                {
                    "objective": row["objective"],
                    "constraints": row["constraints"],
                    "focusRefs": row["focus_refs"],
                }
            )
        elif resource.resource_type == "plan_revision":
            content = canonical_json({"steps": row["steps"], "publicReason": row["public_reason"]})
        else:
            blob = await self._blobs.get(request.actor.scope_id, str(row["result_ref"]))
            content = blob.decode("utf-8")
        required = True
        if (
            self._optional_history
            and resource.resource_type in {"timeline_item", "turn_input", "interaction_input"}
            and row["source_run_id"] != request.run_id
        ):
            # Only automatically loaded history is optional. An explicitly named
            # historical resource is still part of the user's required task input.
            async with self._sessions() as session:
                input_ref = (
                    await session.execute(
                        scoped(
                            "SELECT input_ref FROM {t}turn WHERE scope_id=:scope "
                            "AND primary_run_id=:run AND session_id=:session",
                            self._schema,
                        ),
                        {
                            "scope": request.actor.scope_id,
                            "run": request.run_id,
                            "session": request.session_id,
                        },
                    )
                ).scalar_one_or_none()
            if input_ref is not None:
                submitted = SubmitRequest.model_validate_json(
                    await self._blobs.get(request.actor.scope_id, str(input_ref))
                )
                explicit = [
                    part.resource_ref for part in submitted.inputs if part.kind == "resource"
                ]
                required = any(
                    (ref.resource_type, ref.resource_id)
                    == (resource.resource_type, resource.resource_id)
                    for ref in explicit
                )
        priority = row.get("context_priority") or 0
        if not isinstance(priority, int) or isinstance(priority, bool) or priority < 0:
            raise runtime_failure(
                "INVALID_INPUT", category="context", public_message="Invalid history priority."
            )
        return LoadedSource(
            resource=resource,
            content=content,
            required=required,
            priority=priority if not required else 0,
        )

    async def _load(
        self,
        actor: ActorContext,
        resource: ResourceRef,
        *,
        session_id: UUID | None,
        transaction: AsyncSession | None = None,
    ) -> dict[str, object]:
        if resource.scope.scope_id != actor.scope_id:
            raise self._denied()
        statement = {
            "goal_revision": self._goal,
            "plan_revision": self._plan,
            "tool_call_result": self._tool_result,
            "timeline_item": self._output,
            "turn_input": self._turn_input,
            "interaction_input": self._interaction,
        }.get(resource.resource_type)
        if statement is None:
            raise self._denied()
        try:
            resource_id = UUID(resource.resource_id)
            expected_revision = int(resource.resource_version or "")
        except ValueError as exc:
            raise self._denied() from exc
        params = {"scope_id": actor.scope_id, "resource_id": resource_id}
        if transaction is not None:
            if session_id is None:
                raise self._denied()
            params["bound_session_id"] = session_id
            row = (
                (
                    await transaction.execute(
                        sa.text(statement.text + " AND r.session_id=:bound_session_id FOR SHARE"),
                        params,
                    )
                )
                .mappings()
                .one_or_none()
            )
        else:
            async with self._sessions() as session:
                row = (await session.execute(statement, params)).mappings().one_or_none()
        if row is None or row["deleted_at"] is not None or row["owner_actor_id"] != actor.actor_id:
            raise self._denied()
        if resource.resource_type == "tool_call_result":
            if row["status"] not in {"completed", "failed", "cancelled", "not_executed"}:
                raise self._stale()
        if (
            resource.resource_type in {"timeline_item", "interaction_input"}
            and row["status"] != "completed"
        ):
            raise self._stale()
        if resource.resource_type in {
            "tool_call_result",
            "timeline_item",
            "turn_input",
            "interaction_input",
        }:
            if row["result_ref"] is None:
                raise self._stale()
        if session_id is not None and row["session_id"] != session_id:
            raise self._denied()
        if resource.scope.session_id is not None and resource.scope.session_id != str(
            row["session_id"]
        ):
            raise self._denied()
        if resource.scope.owner_id is not None and resource.scope.owner_id != actor.actor_id:
            raise self._denied()
        actual_revision = 1 if resource.resource_type == "tool_call_result" else row["revision"]
        if row["auth_revision"] != int(actor.auth_revision) or actual_revision != expected_revision:
            raise self._stale()
        return dict(row)

    @staticmethod
    def _denied() -> Exception:
        return runtime_failure(
            "FORBIDDEN", category="authorization", public_message="Context resource is unavailable."
        )

    @staticmethod
    def _stale() -> Exception:
        return runtime_failure(
            "VERSION_CONFLICT",
            category="context",
            public_message="Context resource version changed.",
        )


__all__ = ["SqlRuntimeSourceReader"]

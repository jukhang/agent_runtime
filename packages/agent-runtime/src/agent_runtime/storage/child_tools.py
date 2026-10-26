"""Final child grant check on the executor's own effect/ledger transaction."""

import json
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.canonical import canonical_bytes, content_digest
from agent_runtime.contracts.ownership import ChildCommitGuard
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.tools import ToolInvocation
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.storage.child_access import require_child_resources
from agent_runtime.storage.child_authority import require_child
from agent_runtime.storage.context_access import TransactionalResourceAccess
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlChildToolAuthorizer:
    """Additional child fence, not a replacement for ToolHost policy or approval.

    The executor derives resources from its trusted tool schema and actual effect
    targets, then calls this on the same transaction as its ledger/database write.
    A model-supplied or incomplete target list is not an authorization proof.
    External effects still require their own idempotency/reconciliation boundary.
    """

    def __init__(
        self,
        schema: str,
        actor: ActorContext,
        resources: TransactionalResourceAccess,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._schema = require_schema(schema)
        self._actor, self._resources = actor, resources
        self._clock = clock

    async def require_in_transaction(
        self,
        db: AsyncSession,
        guard: ChildCommitGuard,
        invocation: ToolInvocation,
        resources: tuple[ResourceRef, ...],
    ) -> None:
        now = self._clock()
        if (
            guard.actor != self._actor
            or invocation.actor != self._actor
            or invocation.run_id != guard.lease.run_id
            or invocation.session_id != guard.lease.session_id
            or invocation.deadline_at <= now
        ):
            errors.forbidden("child tool invocation identity or deadline changed")
        run = await require_child(db, self._schema, guard, now=now)
        await require_child_resources(
            db, self._schema, self._actor, invocation.run_id, resources, (invocation.call.tool,)
        )
        row = (
            (
                await db.execute(
                    scoped(
                        "SELECT c.run_id,c.attempt_id,c.spec_revision,c.input_hash,c.status,"
                        "m.context_snapshot_id,m.status AS model_status,s.context_version "
                        "FROM {t}tool_call c "
                        "JOIN {t}model_attempt m ON m.scope_id=c.scope_id AND m.id=c.attempt_id "
                        "JOIN {t}context_snapshot s ON s.scope_id=m.scope_id "
                        "AND s.id=m.context_snapshot_id AND s.run_id=c.run_id "
                        "WHERE c.scope_id=:scope AND c.id=:id FOR UPDATE OF c",
                        self._schema,
                    ),
                    {"scope": self._actor.scope_id, "id": invocation.call.tool_call_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if (
            row is None
            or row["run_id"] != invocation.run_id
            or row["attempt_id"] != invocation.model_attempt_id
            or row["context_snapshot_id"] != invocation.context_snapshot_id
            or int(row["context_version"]) != int(invocation.context_version)
            or row["model_status"] != "completed"
            or row["status"] not in {"pending", "running"}
            or row["spec_revision"] != invocation.call.tool.model_dump(mode="json", by_alias=True)
            or row["input_hash"] != content_digest(invocation.call.arguments)
        ):
            errors.forbidden("child tool differs from its persisted decision")
        await self._require_staged_call(db, guard, invocation, run["checkpoint_id"])
        await self._resources.authorize_session_in_transaction(
            db, self._actor, invocation.session_id
        )
        for ref in resources:
            await self._resources.authorize_resource_in_transaction(
                db, self._actor, ref, session_id=invocation.session_id
            )

    async def _require_staged_call(
        self,
        db: AsyncSession,
        guard: ChildCommitGuard,
        invocation: ToolInvocation,
        checkpoint_id: UUID | None,
    ) -> None:
        row = (
            (
                await db.execute(
                    scoped(
                        "SELECT c.content_hash,b.digest,b.size_bytes,b.content "
                        "FROM {t}checkpoint c JOIN {t}context_blob b "
                        "ON b.scope_id=c.scope_id AND b.blob_key=c.state_ref "
                        "WHERE c.scope_id=:scope AND c.id=:id AND c.run_id=:run "
                        "AND c.state_version=:version FOR KEY SHARE OF b",
                        self._schema,
                    ),
                    {
                        "scope": self._actor.scope_id,
                        "id": checkpoint_id,
                        "run": invocation.run_id,
                        "version": int(guard.expected_state_version),
                    },
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            errors.version_conflict("child tool has no current staged checkpoint")
        content = bytes(row["content"])
        try:
            payload = json.loads(content)
            if (
                not isinstance(payload, dict)
                or row["size_bytes"] != len(content)
                or canonical_bytes(payload) != content
                or content_digest(payload) != row["digest"]
                or row["digest"] != row["content_hash"]
            ):
                errors.version_conflict("child decision checkpoint is corrupt")
        except (UnicodeDecodeError, ValueError):
            errors.version_conflict("child decision checkpoint is invalid")
        action = payload.get("action")
        expected = invocation.call.model_dump(mode="json", by_alias=True)
        if (
            payload.get("kind") != "staged_decision"
            or payload.get("runId") != str(invocation.run_id)
            or payload.get("stateVersion") != guard.expected_state_version
            or payload.get("attemptId") != str(invocation.model_attempt_id)
            or payload.get("contextSnapshotId") != str(invocation.context_snapshot_id)
            or not isinstance(action, dict)
            or action.get("kind") != "invoke_tools"
            or not isinstance(action.get("payload"), dict)
            or not isinstance(action["payload"].get("validatedCalls"), list)
            or expected not in action["payload"].get("validatedCalls", [])
        ):
            errors.forbidden("child tool is not in the current staged decision")


__all__ = ["SqlChildToolAuthorizer"]

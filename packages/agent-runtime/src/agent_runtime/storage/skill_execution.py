"""Bind Skill revocation and Agent dependencies to the sandbox intent transaction."""

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.agent import Agent
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.registry import CapabilityManifest, SkillCapability
from agent_runtime.contracts.tools import ToolInvocation
from agent_runtime.sandbox.runtime import ExecutionStart
from agent_runtime.storage.sandbox_start import ExecutionAuthorizer
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlSkillExecutionAuthorizer:
    """Wrap the host's image/workspace authorizer; live Run authority is checked first.

    SqlSandboxStartAuthority invokes this inside the dispatch intent transaction,
    after its lease/ACL authorizer. The shared revision lock orders dispatch against
    Registry revoke; loading a bundle alone does not authorize an execution.
    """

    def __init__(
        self, schema: str, *, skill: CapabilityRef, authorize: ExecutionAuthorizer
    ) -> None:
        self._schema, self._skill, self._authorize = require_schema(schema), skill, authorize

    async def __call__(
        self, db: AsyncSession, invocation: ToolInvocation, request: ExecutionStart
    ) -> None:
        if not db.in_transaction():
            raise ValueError("Skill execution authorization requires an active transaction")
        await self._authorize(db, invocation, request)
        row = (
            await db.execute(
                scoped(
                    "SELECT cr.status,cr.manifest,cr.content_digest FROM {t}capability_revision cr "
                    "JOIN {t}capability c ON c.scope_id=cr.scope_id AND c.id=cr.capability_id "
                    "WHERE cr.scope_id=:scope AND c.namespace=:namespace AND c.key=:key "
                    "AND cr.revision=:revision AND c.kind='skill' FOR SHARE OF cr",
                    self._schema,
                ),
                {"scope": invocation.actor.scope_id, "namespace": self._skill.key.namespace,
                 "key": self._skill.key.key, "revision": int(self._skill.revision)},
            )
        ).mappings().one_or_none()
        if row is None or row["status"] not in {"active", "superseded"}:
            errors.forbidden("Skill revision was revoked or is unavailable before dispatch")
        manifest = CapabilityManifest.model_validate(row["manifest"])
        if (
            manifest.ref != self._skill or not isinstance(manifest.spec, SkillCapability)
            or manifest.content_digest != row["content_digest"]
            or manifest.status != row["status"]
        ):
            errors.version_conflict("Skill execution manifest integrity check failed")
        agent_row = (
            await db.execute(
                scoped(
                    "SELECT ar.manifest,ar.content_hash FROM {t}run r "
                    "JOIN {t}agent_revision ar ON ar.scope_id=r.scope_id "
                    "AND ar.id=r.agent_revision_id WHERE r.scope_id=:scope AND r.id=:run",
                    self._schema,
                ),
                {"scope": invocation.actor.scope_id, "run": invocation.run_id},
            )
        ).mappings().one_or_none()
        if agent_row is None or content_digest(agent_row["manifest"]) != agent_row["content_hash"]:
            errors.version_conflict("Skill Run Agent integrity check failed")
        agent = Agent.model_validate(agent_row["manifest"])
        if self._skill not in agent.skills or not set(manifest.spec.spec.required_tools) <= set(
            agent.tools
        ):
            errors.forbidden("Skill or its required Tools are not bound to the Run Agent")


__all__ = ["SqlSkillExecutionAuthorizer"]

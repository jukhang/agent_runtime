"""Published capability identity and pinned tool resolution on real PostgreSQL."""

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

pytest.importorskip("sqlalchemy")

import sqlalchemy as sa
from test_storage_uow import DSN, engine_for, migrate, run

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.decisions import ToolCallRequest
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.references import CapabilityKey, CapabilityRef
from agent_runtime.contracts.registry import CapabilityManifest, SkillSpec, ToolCapability
from agent_runtime.contracts.tools import McpToolBinding, ToolSpec
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.mcp.client import McpToolInfo
from agent_runtime.mcp.install import McpCapabilityInstaller
from agent_runtime.skills.git import VerifiedSkillBundle
from agent_runtime.storage.capability_registry import (
    PinnedToolSpecResolver,
    SqlCapabilityRegistry,
)
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.migrate import execute
from agent_runtime.storage.skill_bundles import SqlSkillBundleStore

pytestmark = pytest.mark.skipif(not DSN, reason="AGENT_RUNTIME_TEST_DSN is not configured")


@pytest.fixture(scope="module")
def registry_schema():
    name = f"p04_registry_{uuid4().hex[:12]}"
    assert migrate("upgrade", name).status == "applied"
    yield name

    async def drop() -> None:
        engine = engine_for(name)
        try:
            async with engine.begin() as connection:
                await connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))
        finally:
            await engine.dispose()

    run(drop)


def ref(key: str, revision: str = "1", namespace: str = "host") -> CapabilityRef:
    return CapabilityRef(key=CapabilityKey(namespace=namespace, key=key), revision=revision)


def tool(key: str, revision: str = "1", *, concurrency: int = 2) -> ToolSpec:
    identity = ref(key, revision)
    return ToolSpec(
        ref=identity,
        description=f"Tool {key}",
        source=ref("source"),
        kind="read",
        side_effect="none",
        input_schema=ref("input"),
        output_schema=ref("output"),
        permissions=(ref("permission"),),
        approval_policy=ref("approval"),
        idempotency="read_only",
        timeout_seconds=10,
        concurrency_group="shared",
        max_concurrency=concurrency,
        cancellation="stop",
        public_result_policy="full",
    )


def manifest(spec: ToolSpec) -> CapabilityManifest:
    return CapabilityManifest(
        ref=spec.ref,
        status="draft",
        content_digest=content_digest(spec.model_dump(mode="json", by_alias=True)),
        spec=ToolCapability(kind="tool", spec=spec),
    )


def test_mcp_catalog_install_publish_drift_and_revoke(registry_schema: str) -> None:
    input_schema = {"type": "object", "properties": {"query": {"type": "string"}},
                    "required": ["query"], "additionalProperties": False}
    output_schema = {"type": "object", "required": ["resultType", "content"]}
    spec = ToolSpec(
        ref=ref("search", namespace="mcp.reports"), description="Search reports",
        source=ref("reports", namespace="server"), kind="read", side_effect="none",
        input_schema=ref(content_digest(input_schema), namespace="mcp.input"),
        output_schema=ref(content_digest(output_schema), namespace="mcp.output"),
        permissions=(ref("search_permission"),), approval_policy=ref("approval"),
        idempotency="read_only", timeout_seconds=10, concurrency_group="mcp-reports",
        max_concurrency=2, cancellation="stop", public_result_policy="full",
        mcp=McpToolBinding(protocol_version="2026-07-28", tool_name="search",
                           destination=ref("search", namespace="egress.reports"),
                           input_schema=input_schema, output_schema=output_schema),
    )

    class Catalog:
        schema = input_schema

        async def test_connection(self):
            return (McpToolInfo("search", "Remote search", self.schema),)

    async def exercise() -> None:
        engine = engine_for(registry_schema, search_path=False)
        actor = ActorContext(scope_id="scope-a", actor_id="owner", auth_revision="1",
                             grants_ref="owner")
        catalog, authority = Catalog(), Authority()
        registry = SqlCapabilityRegistry(engine, registry_schema, authority=authority)
        installer = McpCapabilityInstaller(catalog, registry, source=spec.source)
        try:
            first = await installer.install(actor, spec)
            assert await installer.install(actor, spec) == first
            assert (await registry.publish(actor, spec.ref)).status == "active"
            assert await registry.resolve_tool(actor, uuid4(), spec.ref) == spec
            changed = spec.model_copy(update={"description": "Different owner approval"})
            with pytest.raises(AgentRuntimeError, match="different content"):
                await installer.install(actor, changed)
            catalog.schema = {"type": "object"}
            with pytest.raises(ValueError, match="catalog differs"):
                await installer.install(actor, spec)
            outsider = actor.model_copy(update={"scope_id": "scope-b"})
            with pytest.raises(AgentRuntimeError):
                await registry.resolve_tool(outsider, uuid4(), spec.ref)
            await registry.revoke(actor, spec.ref)
            with pytest.raises(AgentRuntimeError):
                await registry.resolve_tool(actor, uuid4(), spec.ref)
        finally:
            await engine.dispose()

    run(exercise)


class Authority:
    def __init__(self) -> None:
        self.revoked: set[CapabilityRef] = set()
        self.deny_manage = False

    async def manage(self, actor: ActorContext, capability: CapabilityRef) -> None:
        if self.deny_manage:
            from agent_runtime.storage.uow import errors

            errors.forbidden("Skill installation is not authorized")

    async def use_tool(self, actor: ActorContext, run_id: UUID, capability: CapabilityRef) -> None:
        if capability in self.revoked:
            from agent_runtime.storage.uow import errors

            errors.forbidden("Tool use not authorized")


def test_skill_bundle_sql_install_reload_acl_gc_and_revoke(registry_schema: str) -> None:
    data = b"# Verified Skill\n"
    file = {
        "path": "SKILL.md", "blobRef": "untrusted-install-ref",
        "digest": hashlib.sha256(data).hexdigest(), "sizeBytes": len(data),
        "executable": False,
    }
    bundle_digest = content_digest([{
        "path": file["path"], "digest": file["digest"],
        "sizeBytes": file["sizeBytes"], "executable": file["executable"],
    }])
    spec = SkillSpec.model_validate({
        "ref": ref("skill_sql"), "source": ref("skill_source"),
        "gitCommit": "a" * 40, "gitSubdir": "skills/report",
        "bundleDigest": bundle_digest, "instructionPath": "SKILL.md",
        "files": [file], "requiredTools": [],
    })

    class Access:
        revoked = False

        async def require_skill(self, actor, run_id, capability):
            if self.revoked or run_id != expected_run or capability != spec.ref:
                from agent_runtime.storage.uow import errors

                errors.forbidden("Skill binding is not authorized")

    expected_run = uuid4()

    async def exercise() -> None:
        engine = engine_for(registry_schema, search_path=False)
        actor = ActorContext(scope_id="scope-skill", actor_id="owner", auth_revision="1",
                             grants_ref="owner")
        access = Access()
        authority = Authority()
        registry = SqlCapabilityRegistry(engine, registry_schema, authority=authority)
        installer = SqlSkillBundleStore(engine, registry_schema, registry=registry, access=access)
        worker = SqlSkillBundleStore(engine, registry_schema, registry=registry, access=access)
        blobs = SqlContextBlobBackend(engine, registry_schema)
        try:
            bundle = VerifiedSkillBundle(spec, {"SKILL.md": data})
            authority.deny_manage = True
            with pytest.raises(AgentRuntimeError, match="not authorized"):
                await installer.install(actor, bundle)
            with pytest.raises(AgentRuntimeError, match="not found"):
                await blobs.get(actor.scope_id, installer._blob_key(actor.scope_id, data))
            authority.deny_manage = False
            installed = await installer.install(actor, bundle)
            assert installed == await installer.install(actor, bundle)
            changed = b"# Different Skill\n"
            changed_file = {
                **file, "digest": hashlib.sha256(changed).hexdigest(),
                "sizeBytes": len(changed),
            }
            changed_spec = SkillSpec.model_validate({
                **spec.model_dump(mode="json", by_alias=True), "files": [changed_file],
                "bundleDigest": content_digest([{
                    key: changed_file[key]
                    for key in ("path", "digest", "sizeBytes", "executable")
                }]),
            })
            with pytest.raises(AgentRuntimeError, match="different content"):
                await installer.install(actor, VerifiedSkillBundle(
                    changed_spec, {"SKILL.md": changed}
                ))
            with pytest.raises(AgentRuntimeError, match="not found"):
                await blobs.get(actor.scope_id, installer._blob_key(actor.scope_id, changed))
            await registry.publish(actor, spec.ref)
            loaded = await worker.load(actor, expected_run, spec.ref)
            assert loaded.files == {"SKILL.md": data}
            assert loaded.spec.files[0].blob_ref != "untrusted-install-ref"
            assert await blobs.collect_unreferenced(
                before=datetime.now(UTC) + timedelta(minutes=1)
            ) == 0
            assert (await worker.load(actor, expected_run, spec.ref)).files == loaded.files
            with pytest.raises(AgentRuntimeError, match="not authorized"):
                await worker.load(actor, uuid4(), spec.ref)
            access.revoked = True
            with pytest.raises(AgentRuntimeError, match="not authorized"):
                await worker.load(actor, expected_run, spec.ref)
            access.revoked = False
            await registry.revoke(actor, spec.ref)
            with pytest.raises(AgentRuntimeError, match="unavailable"):
                await worker.load(actor, expected_run, spec.ref)
            assert await blobs.collect_unreferenced(
                before=datetime.now(UTC) + timedelta(minutes=1)
            ) == 1
        finally:
            await engine.dispose()

    run(exercise)


@pytest.mark.parametrize(
    "case", ["install", "publish", "resolve", "catalog", "scope", "downgrade"]
)
def test_capability_registry(registry_schema: str, case: str) -> None:
    async def exercise() -> None:
        engine = engine_for(registry_schema, search_path=False)
        actor = ActorContext(
            scope_id="scope-a", actor_id="owner", auth_revision="1", grants_ref="owner"
        )
        authority = Authority()
        registry = SqlCapabilityRegistry(engine, registry_schema, authority=authority)
        first = tool(f"lookup_{case}")
        newer = tool(f"lookup_{case}", "2")
        try:
            ids = await asyncio.gather(
                *(registry.install(actor, manifest(first)) for _ in range(4))
            )
            assert len(set(ids)) == 1
            if case == "install":
                with pytest.raises(AgentRuntimeError, match="different content"):
                    await registry.install(
                        actor, manifest(first.model_copy(update={"description": "Changed"}))
                    )
                with pytest.raises(AgentRuntimeError):
                    await registry.install(
                        actor, manifest(tool(f"lookup_{case}", "1", concurrency=3))
                    )
                return
            await registry.publish(actor, first.ref)
            if case == "publish":
                assert (await registry.publish(actor, first.ref)).status == "active"
                assert await registry.install(actor, manifest(first)) == ids[0]
                await registry.install(actor, manifest(newer))
                results = await asyncio.gather(
                    registry.publish(actor, newer.ref), registry.publish(actor, newer.ref)
                )
                assert all(item.status == "active" for item in results)
                assert await registry.resolve_tool(actor, uuid4(), first.ref) == first
                await registry.revoke(actor, first.ref)
                with pytest.raises(AgentRuntimeError):
                    await registry.resolve_tool(actor, uuid4(), first.ref)
                with pytest.raises(AgentRuntimeError):
                    await registry.publish(actor, first.ref)
                return
            if case == "downgrade":
                await registry.install(actor, manifest(newer))
                await registry.publish(actor, newer.ref)
                return
            if case == "resolve":
                run_id = uuid4()
                resolver = PinnedToolSpecResolver(registry, actor, run_id, (first.ref,))
                assert (
                    await resolver.get(
                        ToolCallRequest(tool_call_id=uuid4(), tool=first.ref, arguments={})
                    )
                    == first
                )
                with pytest.raises(AgentRuntimeError):
                    await resolver.get(
                        ToolCallRequest(tool_call_id=uuid4(), tool=newer.ref, arguments={})
                    )
                authority.revoked.add(first.ref)
                with pytest.raises(AgentRuntimeError):
                    await resolver.get(
                        ToolCallRequest(tool_call_id=uuid4(), tool=first.ref, arguments={})
                    )
                return
            if case == "scope":
                outsider = ActorContext(
                    scope_id="scope-b", actor_id="other", auth_revision="1", grants_ref="other"
                )
                with pytest.raises(AgentRuntimeError, match="unavailable"):
                    await registry.resolve_tool(outsider, uuid4(), first.ref)
                return
            second = tool("search")
            await registry.install(actor, manifest(second))
            await registry.publish(actor, second.ref)
            run_id = uuid4()
            one = await registry.active_tools(actor, run_id, (second.ref, first.ref))
            two = await registry.active_tools(actor, run_id, (first.ref, second.ref))
            assert one == two
            assert len(one.specs) == 2
            mismatched = tool("other", concurrency=3)
            await registry.install(actor, manifest(mismatched))
            await registry.publish(actor, mismatched.ref)
            with pytest.raises(AgentRuntimeError, match="concurrency"):
                await registry.active_tools(actor, run_id, (first.ref, mismatched.ref))
        finally:
            await engine.dispose()

    run(exercise)
    if case == "downgrade":
        assert DSN is not None
        with pytest.raises(RuntimeError, match="superseded"):
            run(lambda: execute("downgrade", DSN, registry_schema, "0006_credential_service", 60))

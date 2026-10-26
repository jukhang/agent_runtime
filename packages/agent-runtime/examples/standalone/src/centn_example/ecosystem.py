"""Optional independent-host MCP/Git Skill/OpenSandbox assembly and supervision."""

import asyncio
import base64
import json
import shlex
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, uuid5

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.tools import ToolInvocation
from agent_runtime.sandbox.opensandbox import (
    confirm_opensandbox_deleted,
    connect_opensandbox,
    inspect_opensandbox_resources,
)
from agent_runtime.sandbox.runtime import ExecutionBinding
from agent_runtime.sandbox.supervision import ExecutionLimits
from agent_runtime.skills.git import PublicGitFetcher
from agent_runtime.storage.capability_registry import PinnedToolSpecResolver
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.sandbox_results import SqlSandboxResultCollector
from agent_runtime.storage.sandbox_worker import SqlSandboxMaintenanceWorker
from agent_runtime.storage.skill_access import SqlRunSkillAccess
from agent_runtime.storage.skill_bundles import SqlSkillBundleStore
from agent_runtime.storage.skill_tool import SkillSandboxExecution, SqlSkillScriptTool
from agent_runtime.storage.tool_host import ToolEffect
from agent_runtime.storage.uow import errors, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import scoped
from agent_runtime.storage.wake import SqlWaitWaker
from centn_example.ecosystem_config import IMAGE
from centn_example.resources import ResourceCreate
from centn_example.tools import ref

LIMITS = ExecutionLimits(
    log_bytes=65536, workspace_bytes=1048576, artifact_bytes=1048576, artifact_count=16
)


async def require_mcp_data(db, host, invocation):
    args = host.tools.arguments.validate_python(invocation.call.arguments)
    if len(args.argv[0].encode()) > 16384:
        errors.invalid_input("Chart dataset exceeds its bound")
    data = json.loads(args.argv[0])
    rows = (
        (
            await db.execute(
                scoped(
                    "SELECT result_ref FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run "
                    "AND status='completed' AND spec_revision=CAST(:spec AS jsonb)",
                    host.settings.schema,
                ),
                {
                    "scope": invocation.actor.scope_id,
                    "run": invocation.run_id,
                    "spec": ref("read-metrics").model_dump_json(by_alias=True),
                },
            )
        )
        .scalars()
        .all()
    )
    blobs = SqlContextBlobBackend(host.engine, host.settings.schema)
    for key in rows:
        observation = json.loads(await blobs.get_in_transaction(db, invocation.actor.scope_id, key))
        outcome = observation.get("outcome", observation)
        result = outcome.get("result", {})
        if result.get("data", {}).get("structuredContent") == data:
            return data
    errors.forbidden("Chart data must exactly match this Run's sealed MCP result")


class ReportCollector(SqlSandboxResultCollector):
    def __init__(self, *args, host, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = host

    async def seal(self, db, row, result):
        await super().seal(db, row, result)
        if result.outcome.status != "completed":
            return
        collection = await self._payloads.get(
            result.invocation.actor.scope_id, result.collection_ref
        )
        data = collection["data"]
        for suffix, raw, mime in (
            ("chart.png", base64.b64decode(data["png_base64"], validate=True), "image/png"),
            ("report.txt", data["report"].encode(), "text/plain; charset=utf-8"),
        ):
            request = ResourceCreate(
                session_id=result.invocation.session_id,
                kind="report",
                idempotency_key=f"ecosystem:{result.invocation.operation_id}:{suffix}",
                title=suffix,
                text="Sealed sandbox output",
            )
            await self.host.resources.create_content_in_transaction(
                db, result.invocation.actor, request, raw, mime
            )


class Sandboxes:
    def __init__(self, ecosystem, lease):
        self.ecosystem = ecosystem
        self.lease = lease
        self.allocated = {}
        self.intents = {}

    async def resolve(self, invocation, grant):
        from opensandbox.models.sandboxes import NetworkPolicy
        from opensandbox.sandbox import Sandbox

        host = self.ecosystem.host
        async with host.resources.sessions() as db, db.begin():
            await require_mcp_data(db, host, invocation)
            recorded = (
                await db.execute(
                    scoped(
                        "SELECT sandbox_id FROM {t}execution WHERE scope_id=:scope "
                        "AND operation_id=:id",
                        host.settings.schema,
                    ),
                    {"scope": invocation.actor.scope_id, "id": invocation.operation_id},
                )
            ).first()
            if recorded is not None:
                errors.version_conflict(
                    "Existing sandbox intent requires reconciliation, not redispatch"
                )
        allocation = await self.ecosystem.allocations.reserve(invocation, self.lease())
        self.intents[invocation.operation_id] = allocation
        metadata = {"centn.ecosystem": self.ecosystem.config.deployment_id, **allocation.metadata}
        sandbox = await Sandbox.create(
            IMAGE,
            connection_config=self.ecosystem.connection,
            timeout=timedelta(minutes=5),
            resource={"cpu": "500m", "memory": "256Mi"},
            network_policy=NetworkPolicy(default_action="deny", egress=[]),
            entrypoint=["sleep", "300"],
            metadata=metadata,
        )
        try:
            await self.ecosystem.allocations.created(allocation, sandbox.id)
            backend = await connect_opensandbox(sandbox.id, self.ecosystem.connection)
        except BaseException:
            # The durable allocation owns cleanup even if saving the acknowledgement
            # failed. Never create a second environment after an ambiguous response.
            await sandbox.close()
            raise
        binding = ExecutionBinding(
            invocation.actor.scope_id,
            uuid5(
                NAMESPACE_URL, f"execution:{invocation.actor.scope_id}:{invocation.operation_id}"
            ),
            1,
            1,
            IMAGE,
            sandbox.id,
        )
        result = SkillSandboxExecution(sandbox, backend, binding, LIMITS, 60)
        self.allocated[invocation.operation_id] = result
        return result

    async def authorize(self, db, invocation, request):
        expected = self.allocated.get(invocation.operation_id)
        command = shlex.join(
            [
                "python",
                "-E",
                "-s",
                f"/workspace/skills/{invocation.operation_id}/run.py",
                *invocation.call.arguments["argv"],
            ]
        )
        if (
            expected is None
            or request.binding != expected.binding
            or request.command != command
            or request.limits != LIMITS
            or request.working_directory != "/workspace"
            or request.timeout_seconds != 60
        ):
            errors.forbidden("Sandbox image, generation, limits or command changed")
        await require_mcp_data(db, self.ecosystem.host, invocation)
        await self.ecosystem.allocations.bind_in_transaction(
            db, self.intents[invocation.operation_id], invocation, request
        )


class ManagedSkill(SqlSkillScriptTool):
    async def execute(self, invocation, grant):
        try:
            return await super().execute(invocation, grant)
        finally:
            allocated = self._sandboxes.allocated.pop(invocation.operation_id, None)
            self._sandboxes.intents.pop(invocation.operation_id, None)
            if allocated is not None:
                try:
                    host = self._sandboxes.ecosystem.host
                    async with host.engine.connect() as db:
                        exists = (
                            await db.execute(
                                scoped(
                                    "SELECT sandbox_id FROM {t}execution WHERE scope_id=:scope "
                                    "AND operation_id=:id",
                                    host.settings.schema,
                                ),
                                {"scope": invocation.actor.scope_id, "id": invocation.operation_id},
                            )
                        ).first()
                    if exists is None or exists[0] != allocated.sandbox.id:
                        await allocated.sandbox.destroy()
                finally:
                    await allocated.backend.aclose()
                    await allocated.sandbox.close()


class Ecosystem:
    def __init__(self, host, config):
        from opensandbox.config import ConnectionConfig
        from opensandbox.transport.retry import RetryPolicy

        from centn_example.ecosystem_allocations import SqlSandboxAllocations

        self.host, self.config = host, config
        self.connection = ConnectionConfig(
            domain=config.sandbox_domain,
            api_key=config.sandbox_key,
            retry_policy=RetryPolicy.disabled(),
        )
        self.payloads = BlobContextPayloadStore(
            SqlContextBlobBackend(host.engine, host.settings.schema)
        )
        self.transports = []
        self.allocations = SqlSandboxAllocations(
            host.engine,
            host.settings.schema,
            deployment_id=config.deployment_id,
            resources=host.sources,
        )

    async def install(self, registry):
        config = self.config
        if config.skill.required_tools != (ref("read-metrics"),) or config.skill.ref != ref(
            "chart-skill"
        ):
            raise ValueError("Skill must bind the reviewed MCP dependency")
        fetcher = PublicGitFetcher(
            allowed_hosts=frozenset({urlsplit(config.git_url).hostname}),
            proxy_url=config.git_proxy,
            ca_bundle=config.git_ca,
        )
        bundle = await asyncio.to_thread(
            fetcher.install, config.skill, repository_url=config.git_url
        )
        reviewed = Path(__file__).parent / "ecosystem_assets"
        if bundle.files != {
            name: (reviewed / name).read_bytes() for name in ("SKILL.md", "run.py")
        }:
            raise ValueError(
                "This host only executes its reviewed report Skill, not arbitrary scripts"
            )
        self.store = SqlSkillBundleStore(
            self.host.engine,
            self.host.settings.schema,
            registry=registry,
            access=SqlRunSkillAccess(self.host.engine, self.host.settings.schema),
        )
        await self.store.install(self.host.actor(), bundle)
        await registry.publish(self.host.actor(), config.skill.ref)

    def collector(self, actor, capacity):
        host, schema = self.host, self.host.settings.schema
        journal = SqlJournal(host.engine, schema, actor)

        async def authorize(db, invocation):
            run = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}run WHERE scope_id=:scope AND id=:id FOR UPDATE",
                            schema,
                        ),
                        {"scope": actor.scope_id, "id": invocation.run_id},
                    )
                )
                .mappings()
                .one()
            )
            await revalidate_authority(
                db, locks.build_lock_statements(schema), actor, run=run, now=datetime.now(UTC)
            )
            if (
                run["status"] not in {"running", "waiting_tool"}
                or run["desired_control"] is not None
            ):
                errors.forbidden("Report Run is no longer accepting results")
            grant = await locks.current_grant(
                db,
                locks.build_lock_statements(schema),
                actor.scope_id,
                invocation.run_id,
                actor.actor_id,
            )
            if grant is None or invocation.call.tool.model_dump(
                mode="json", by_alias=True
            ) not in grant["allowed_scope"].get("tools", []):
                errors.forbidden("Report tool permission was revoked")
            revisions = (
                (
                    await db.execute(
                        scoped(
                            "SELECT cr.status FROM {t}capability_revision cr JOIN {t}capability c "
                            "ON c.scope_id=cr.scope_id AND c.id=cr.capability_id "
                            "WHERE cr.scope_id=:scope AND c.namespace='example' "
                            "AND c.key IN ('chart-report','chart-skill') "
                            "AND cr.revision=1 FOR SHARE OF cr",
                            schema,
                        ),
                        {"scope": actor.scope_id},
                    )
                )
                .scalars()
                .all()
            )
            if len(revisions) != 2 or any(
                status not in {"active", "superseded"} for status in revisions
            ):
                errors.forbidden("Report tool or Skill revision was revoked")
            await host.sources.authorize_session_in_transaction(db, actor, invocation.session_id)
            await require_mcp_data(db, host, invocation)

        async def read(backend, payload):
            if payload["executionStatus"] != "completed":
                return ToolEffect({}, "Sandbox failed; no report was published.")
            # The independent guard measured artifact bytes/count and stopped every user process.
            raw = await backend._sandbox.files.read_bytes("/artifacts/chart.png")
            report = await backend._sandbox.files.read_bytes("/artifacts/report.json")
            if len(raw) > 1048576 or len(report) > 65536:
                raise ValueError("Sandbox output exceeded its bound")
            data = json.loads(report)
            invocation = ToolInvocation.model_validate(payload["invocation"])
            source = json.loads(invocation.call.arguments["argv"][0])
            if (
                data["data"] != source
                or data["planned"] != sum(r["planned"] for r in source["rows"])
                or data["actual"] != sum(r["actual"] for r in source["rows"])
            ):
                raise ValueError("Report differs from the authorized source data")
            if data["uid"] != 1000 or not raw.startswith(b"\x89PNG\r\n\x1a\n"):
                raise ValueError("Untrusted report or invalid PNG")
            result = {
                "png_base64": base64.b64encode(raw).decode(),
                "report": json.dumps(data, ensure_ascii=False, indent=2),
                "data": data["data"],
                "uid": data["uid"],
            }
            summary = {
                "datasetId": source["datasetId"],
                **{name: data[name] for name in ("planned", "actual", "ratio")},
            }
            return ToolEffect(result, "Sealed chart.png and report.txt. " + json.dumps(summary))

        return ReportCollector(
            host.engine,
            schema,
            host=host,
            actor=actor,
            payloads=self.payloads,
            journal=journal,
            schemas=host.tools.schemas,
            reader=read,
            authorize=authorize,
            waker=SqlWaitWaker(
                host.engine, schema, actor=actor, journal=journal, payloads=self.payloads
            ),
            capacity=capacity,
        )

    def script(self, actor, lease, access, capacity, gate):
        host = self.host
        sandboxes = Sandboxes(self, lease)
        resolver = PinnedToolSpecResolver(
            host.registry, actor, lease().run_id, tuple(s.ref for s in host.tools.specs)
        )
        script = ManagedSkill(
            capacity=capacity,
            store=self.store,
            skill=self.config.skill.ref,
            executable_path="run.py",
            interpreter="python",
            specs=resolver,
            resources=host.tools,
            authorize=access.authorize,
            authorize_execution=sandboxes.authorize,
            execution_gate=gate,
            sandboxes=sandboxes,
            payloads=self.payloads,
        )
        return resolver, script

    def maintenance(self, capacity):
        async def connect(sid):
            return await connect_opensandbox(sid, self.connection)

        async def deleted(sid):
            return await confirm_opensandbox_deleted(sid, self.connection)

        async def supervise(work):
            async with self.host.engine.connect() as db:
                key = (
                    await db.execute(
                        scoped(
                            "SELECT w.payload_ref FROM {t}execution e JOIN {t}work_item w "
                            "ON w.scope_id=e.scope_id AND w.id=e.supervisor_work_id "
                            "WHERE e.scope_id=:scope AND e.id=:id",
                            self.host.settings.schema,
                        ),
                        {"scope": work.lease.scope_id, "id": work.subject_ref.resource_id},
                    )
                ).scalar_one()
            payload = await self.payloads.get(work.lease.scope_id, key)
            invocation = ToolInvocation.model_validate(payload["invocation"])
            await self.host.authorize_actor(
                None, invocation.actor.model_copy(update={"grants_ref": "example-owner"})
            )
            handler = SqlSandboxMaintenanceWorker(
                self.host.engine,
                self.host.settings.schema,
                connect=connect,
                confirm_deleted=deleted,
                payloads=self.payloads,
                results=self.collector(invocation.actor, capacity),
                inspect_resources=inspect_opensandbox_resources,
                retry_seconds=1,
            )
            await handler(work)

        return supervise

    async def aclose(self):
        for transport in self.transports:
            await transport.aclose()

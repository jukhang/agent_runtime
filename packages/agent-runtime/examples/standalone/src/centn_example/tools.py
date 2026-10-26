"""A real deterministic calculation tool, registered through SQL ToolHost."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from agent_runtime.contracts.policy import PolicyAllow, PolicyAsk
from agent_runtime.contracts.references import CapabilityRef, ResourceRef, ResourceScope
from agent_runtime.contracts.tools import ToolSpec
from agent_runtime.policy_validation import policy_input_digest
from agent_runtime.storage.context_snapshots import SqlContextSnapshotStore
from agent_runtime.storage.tool_host import ToolEffect
from agent_runtime.storage.uow.schema import scoped
from agent_runtime.tool_names import provider_tool_name
from agent_runtime.tool_schemas import PinnedToolSchemas
from centn_example.resources import ResourceCreate, ResourceView


def ref(key):
    return CapabilityRef.model_validate(
        {"key": {"namespace": "example", "key": key}, "revision": "1"}
    )


class MultiplyInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    left: float = Field(ge=-1e12, le=1e12, allow_inf_nan=False)
    right: float = Field(ge=-1e12, le=1e12, allow_inf_nan=False)


class MultiplyResult(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    value: float = Field(allow_inf_nan=False)


class ReportInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    title: str = Field(min_length=1, max_length=160)
    text: str = Field(min_length=1, max_length=65536)


def report_spec():
    return ToolSpec(
        ref=ref("save-report"),
        kind="write",
        description="Save a UTF-8 report in this Session, only after human approval.",
        source=ref("host"),
        input_schema=ref("report-input"),
        output_schema=ref("report-output"),
        side_effect="database",
        permissions=(ref("write"),),
        approval_policy=ref("report-policy"),
        idempotency="transactional",
        timeout_seconds=10,
        concurrency_group="example-write",
        max_concurrency=2,
        cancellation="stop",
        public_result_policy="full",
    )


class ReportWriter:
    def __init__(self, resources):
        self.resources = resources

    async def invoke(self, db, invocation):
        if invocation.operation_id is None:
            raise ValueError("Report write requires an operation identity")
        args = ReportInput.model_validate(invocation.call.arguments)
        report = await self.resources.create_in_transaction(
            db,
            invocation.actor,
            ResourceCreate(
                session_id=invocation.session_id,
                idempotency_key=f"report:{invocation.operation_id}",
                kind="report",
                title=args.title,
                text=args.text,
            ),
        )
        return ToolEffect(
            report.model_dump(mode="json", by_alias=True),
            "Report saved after approval.",
            (report.resource_ref,),
        )


class ToolPolicy:
    def __init__(self, tools, approvals):
        self.tools, self.approvals = tools, approvals

    async def evaluate(self, request):
        spec = await self.tools.get(request.invocation.call)
        if spec.ref != ref("save-report"):
            return await self.tools.evaluate(request)
        async with self.tools.engine.connect() as db:
            deadline = (
                await db.execute(
                    scoped(
                        "SELECT deadline_at FROM {t}run WHERE scope_id=:scope AND id=:run",
                        self.tools.schema,
                    ),
                    {"scope": request.invocation.actor.scope_id, "run": request.invocation.run_id},
                )
            ).scalar_one()
        approval = await self.approvals.request(
            request,
            spec,
            public_summary="保存本轮生成的报告到当前会话",
            expires_at=min(datetime.now(UTC) + timedelta(minutes=15), deadline),
        )
        return PolicyAsk(
            decision="ask",
            decision_id=uuid4(),
            policy=request.policy,
            input_digest=policy_input_digest(request),
            evidence_refs=request.resources,
            evaluated_at=datetime.now(UTC),
            expires_at=request.invocation.deadline_at,
            approval=approval.request,
        )


def multiply_spec():
    return ToolSpec(
        ref=ref("multiply"),
        kind="read",
        description="Multiply two bounded numbers exactly as supplied.",
        source=ref("host"),
        input_schema=ref("multiply-input"),
        output_schema=ref("multiply-output"),
        side_effect="none",
        permissions=(ref("read"),),
        approval_policy=ref("read-policy"),
        idempotency="read_only",
        timeout_seconds=60,
        concurrency_group="example-read",
        max_concurrency=8,
        cancellation="stop",
        public_result_policy="full",
    )


class Tools:
    def __init__(self, engine, schema):
        self.engine, self.schema = engine, schema
        self.spec = multiply_spec()
        self.specs = (self.spec, report_spec())
        self.schemas = PinnedToolSchemas(
            {
                ref("multiply-input"): TypeAdapter(MultiplyInput),
                ref("multiply-output"): TypeAdapter(MultiplyResult),
                ref("report-input"): TypeAdapter(ReportInput),
                ref("report-output"): TypeAdapter(ResourceView),
            }
        )

    async def definition(self, capability):
        spec = next((spec for spec in self.specs if spec.ref == capability), None)
        if spec is None:
            raise ValueError("Tool is not installed")
        return {
            "type": "function",
            "function": {
                "name": provider_tool_name(capability),
                "description": spec.description,
                "parameters": (
                    MultiplyInput if capability == self.spec.ref else ReportInput
                ).model_json_schema(),
            },
        }

    async def get(self, call):
        spec = next((spec for spec in self.specs if spec.ref == call.tool), None)
        if spec is None:
            raise ValueError("Tool revision is not installed")
        return spec

    async def resources(self, invocation, spec):
        if spec not in self.specs:
            raise ValueError("Tool revision changed")
        async with self.engine.connect() as db:
            turn = (
                await db.execute(
                    scoped(
                        "SELECT id FROM {t}turn WHERE scope_id=:scope AND primary_run_id=:run",
                        self.schema,
                    ),
                    {"scope": invocation.actor.scope_id, "run": invocation.run_id},
                )
            ).scalar_one()
        snapshot = await SqlContextSnapshotStore(self.engine, self.schema).get(
            invocation.actor.scope_id, invocation.context_snapshot_id
        )
        targets = {
            (source.resource.resource_type, source.resource.resource_id): source.resource
            for source in snapshot.sources
            if source.disposition in {"included", "summarized"}
        }
        targets.setdefault(
            ("turn_input", str(turn)),
            ResourceRef(
                resource_type="turn_input",
                resource_id=str(turn),
                resource_version="0",
                scope=ResourceScope(
                    scope_id=invocation.actor.scope_id,
                    session_id=str(invocation.session_id),
                    owner_id=invocation.actor.actor_id,
                ),
            ),
        )
        return tuple(targets.values())

    async def evaluate(self, request):
        if (
            request.invocation.call.tool != self.spec.ref
            or request.policy != self.spec.approval_policy
        ):
            raise ValueError("No policy for this capability")
        return PolicyAllow(
            decision="allow",
            decision_id=uuid4(),
            policy=request.policy,
            input_digest=policy_input_digest(request),
            evidence_refs=request.resources,
            evaluated_at=datetime.now(UTC),
            expires_at=request.invocation.deadline_at,
            grant_ref="example-read-policy",
        )

    async def invoke(self, invocation):
        args = MultiplyInput.model_validate(invocation.call.arguments)
        return ToolEffect({"value": args.left * args.right}, "Multiplication completed.")

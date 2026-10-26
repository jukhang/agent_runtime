"""Claim-scoped SQL summary generation using existing blobs, snapshots and audits."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from agent_runtime.compression import COMPRESSION_PROMPT, render_source, verified_extraction
from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.context_runtime import (
    CompressedSource,
    ContextRuntime,
    LoadedSource,
    SourceReader,
    TokenEstimator,
)
from agent_runtime.context_validation import validate_context_binding
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.context import ContextRequest, PreparedContext
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.model import ModelConfig, ProviderRequest
from agent_runtime.contracts.provider import (
    ContentDelta,
    ProviderError,
    ProviderFinish,
    ProviderUsage,
)
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.results import Usage
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.provider import (
    AttemptStart,
    AttemptTerminal,
    CapabilityResolver,
    ProviderInvoker,
)
from agent_runtime.providers.openai_compatible import (
    ConnectionResolver,
    OpenAIAdapterProfile,
    OpenAICompatibleProvider,
    ResolvedProviderInput,
)
from agent_runtime.storage.context_access import ContextResourceAccess, SqlContextAccessVerifier
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.context_snapshots import SqlContextSnapshotStore
from agent_runtime.storage.main_cancel import SqlMainCancelSignal
from agent_runtime.storage.model_attempts import SqlModelAttemptStore
from agent_runtime.storage.uow import errors, facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped


def render_summary_payload(
    payload: dict[str, object],
    model: ModelConfig,
    adapter: OpenAIAdapterProfile,
) -> dict[str, object]:
    """Persist the exact provider messages, not a different generic input rendering."""
    prompt, user = payload.get("promptText"), payload.get("userInput")
    if not isinstance(prompt, str) or not isinstance(user, str):
        errors.invalid_input("Summary prompt and JSON input must be explicit strings")
    rendered: dict[str, object] = {
        "messages": [{"role": "system", "content": prompt}, {"role": "user", "content": user}],
        "modelConfig": model.model_dump(mode="json", by_alias=True),
        "adapterProfile": asdict(adapter),
    }
    return {
        **payload,
        "renderedRequest": rendered,
        "renderedRequestDigest": content_digest(rendered),
    }


class SqlCompressionAttempts(SqlModelAttemptStore):
    """Audit first, then authorize worst-case spend before opening the network."""

    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        lease: Callable[[], LeaseToken],
        answer_reserve: Callable[[], int],
    ) -> None:
        super().__init__(engine, schema, actor=actor, lease=lease)
        self._answer_reserve = answer_reserve

    async def begin(self, request: ProviderRequest, *, started_at: datetime) -> AttemptStart:
        result = await super().begin(request, started_at=started_at)
        if result != "started":
            return result
        try:
            async with self._sessions() as db, db.begin():
                assert self._actor is not None and self._lease is not None
                lease = self._lease()
                facts.verify_slot(
                    await locks.lock_slot(db, self._locks, lease.scope_id, lease.session_id), lease
                )
                run = facts.verify_run(
                    await locks.lock_run(db, self._locks, lease.scope_id, lease.run_id),
                    self._actor,
                    lease,
                    now=datetime.now(UTC),
                )
                await revalidate_authority(
                    db, self._locks, self._actor, run=run, now=datetime.now(UTC)
                )
                allocation = (
                    (
                        await db.execute(
                            scoped(
                                "SELECT * FROM {t}budget_allocation "
                                "WHERE scope_id=:scope AND id=:id "
                                "AND root_run_id=:run AND parent_id IS NULL FOR UPDATE",
                                self._schema,
                            ),
                            {
                                "scope": request.scope_id,
                                "run": request.run_id,
                                "id": request.budget_allocation_id,
                            },
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if allocation is None:
                    errors.forbidden("Compression allocation is unavailable")
                rows = (
                    (
                        await db.execute(
                            scoped(
                                "SELECT id,status,usage FROM {t}model_attempt "
                                "WHERE scope_id=:scope AND run_id=:run",
                                self._schema,
                            ),
                            {"scope": request.scope_id, "run": request.run_id},
                        )
                    )
                    .mappings()
                    .all()
                )
                tokens = 0
                for row in rows:
                    if row["id"] == request.attempt_id:
                        continue
                    usage = None if row["usage"] is None else Usage.model_validate(row["usage"])
                    if row["status"] == "running" or usage is None or usage.status != "complete":
                        raise runtime_failure(
                            "USAGE_UNRESOLVED",
                            category="budget",
                            public_message=(
                                "Previous model usage is unresolved; verify provider usage "
                                "and settle the unknown reservation before retry."
                            ),
                        )
                    assert usage is not None
                    tokens += int(usage.total_tokens or "0")
                for key, own in (
                    ("model_calls", len(rows) + 1),  # Keep one call for the actual answer.
                    (
                        "tokens",
                        tokens
                        + request.estimated_input_tokens
                        + request.model.max_output_tokens
                        + self._answer_reserve(),
                    ),
                ):
                    available = int(allocation["limits"][key]) - sum(
                        int(allocation[field].get(key, 0))
                        for field in ("settled", "reserved", "unknown_reserved")
                    )
                    if own > available:
                        self._exhausted("Compression would exceed the allocated model budget.")
        except AgentRuntimeError as exc:
            # No HTTP request has been made. Zero here is known, not an estimate.
            await self.finish(
                request,
                AttemptTerminal(
                    status="failed",
                    finish_reason=None,
                    usage=Usage(
                        status="complete",
                        reason="provider_reported",
                        input_tokens="0",
                        output_tokens="0",
                        total_tokens="0",
                    ),
                    failure=exc.failure,
                    finished_at=datetime.now(UTC),
                ),
            )
            raise
        return result

    @staticmethod
    def _exhausted(message: str) -> None:
        raise runtime_failure("BUDGET_EXHAUSTED", category="context", public_message=message)


class SqlContextCompression:
    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        lease: Callable[[], LeaseToken],
        reader: SourceReader,
        resources: ContextResourceAccess,
        estimator: TokenEstimator,
        model: ModelConfig,
        client: httpx.AsyncClient,
        connections: ConnectionResolver,
        capabilities: CapabilityResolver,
        adapter: OpenAIAdapterProfile | None = None,
        max_input_tokens: int = 200_000,
        timeout_seconds: float = 60,
    ) -> None:
        if max_input_tokens < 1 or timeout_seconds <= 0:
            raise ValueError("compression bounds must be positive")
        self._schema = require_schema(schema)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._engine, self._actor, self._lease = engine, actor, lease
        self._reader, self._resources, self._model = reader, resources, model
        self._input_limit, self._timeout = max_input_tokens, timeout_seconds
        self._answer_reserve = 0
        adapter = adapter or OpenAIAdapterProfile()
        self._payloads = BlobContextPayloadStore(SqlContextBlobBackend(engine, schema))
        self._raw = ContextRuntime(
            reader=reader,
            estimator=estimator,
            payloads=self._payloads,
            snapshots=SqlContextSnapshotStore(engine, schema),
            payload_renderer=lambda payload: render_summary_payload(payload, model, adapter),
        )
        self._access = SqlContextAccessVerifier(engine, schema, resources=resources)

        async def summary_input(call: ProviderRequest) -> ResolvedProviderInput:
            bound, payload = await self._raw.load(call.scope_id, call.context_snapshot_id)
            validate_context_binding(bound, call)
            await self._access.verify(actor, bound)
            expected = render_summary_payload(payload, call.model, adapter)
            rendered = payload.get("renderedRequest")
            if (
                not isinstance(rendered, dict)
                or content_digest(rendered) != expected["renderedRequestDigest"]
                or payload.get("renderedRequestDigest") != expected["renderedRequestDigest"]
            ):
                errors.invalid_input("Summary rendered request differs from its snapshot")
            entries = payload.get("sources")
            if not isinstance(entries, list) or len(entries) != 1:
                errors.invalid_input("Summary input must contain exactly one original source")
            entry = entries[0]
            if not isinstance(entry, dict) or not isinstance(entry.get("content"), str):
                errors.invalid_input("Summary source content is invalid")
            return ResolvedProviderInput(
                input_ref=bound.payload_ref,
                input_digest=bound.content_hash,
                messages=(
                    {"role": "system", "content": str(payload["promptText"])},
                    {"role": "user", "content": str(payload["userInput"])},
                ),
            )

        self._provider = ProviderInvoker(
            OpenAICompatibleProvider(
                client,
                inputs=summary_input,
                connections=connections,
                profile=adapter,
            ),
            capabilities=capabilities,
            attempts=SqlCompressionAttempts(
                engine,
                schema,
                actor=actor,
                lease=lease,
                answer_reserve=lambda: self._answer_reserve,
            ),
        )
        self._lock = asyncio.Lock()

    async def compress(self, request: ContextRequest, source: LoadedSource) -> CompressedSource:
        if source.required or request.actor != self._actor or request.model != self._model.ref:
            errors.forbidden("Compression cannot replace required or differently bound facts")
        async with self._lock:
            self._answer_reserve = (
                request.budget.max_input_tokens + request.budget.reserved_output_tokens
            )
            summary_request = request.model_copy(
                update={
                    "resource_refs": (source.resource,),
                    "tools": (),
                    "prompt_text": COMPRESSION_PROMPT,
                    "user_input": render_source(source.content, task=request.user_input),
                    "budget": request.budget.model_copy(
                        update={
                            "max_input_tokens": self._input_limit,
                            "reserved_output_tokens": self._model.max_output_tokens,
                        }
                    ),
                }
            )
            snapshot = await self._raw.prepare(summary_request)
            _, summary_payload = await self._raw.load(snapshot.scope_id, snapshot.snapshot_id)
            if any(item.disposition != "included" for item in snapshot.sources):
                SqlCompressionAttempts._exhausted(
                    "Original source exceeds the summary input limit."
                )
            await self._access.verify(self._actor, snapshot)
            async with self._sessions() as db:
                current_lease = self._lease()
                deadline = (
                    await db.execute(
                        scoped(
                            "SELECT LEAST(r.deadline_at,t.deadline_at,w.deadline_at) "
                            "FROM {t}run r JOIN {t}task t "
                            "ON t.scope_id=r.scope_id AND t.id=r.task_id "
                            "JOIN {t}work_item w ON w.scope_id=r.scope_id AND w.run_id=r.id "
                            "WHERE r.scope_id=:scope AND r.id=:run AND w.id=:work",
                            self._schema,
                        ),
                        {
                            "scope": current_lease.scope_id,
                            "run": current_lease.run_id,
                            "work": current_lease.work_id,
                        },
                    )
                ).scalar_one()
            call = ProviderRequest(
                scope_id=snapshot.scope_id,
                session_id=snapshot.session_id,
                run_id=snapshot.run_id,
                attempt_id=uuid4(),
                context_snapshot_id=snapshot.snapshot_id,
                context_version=snapshot.context_version,
                input_ref=snapshot.payload_ref,
                input_digest=snapshot.content_hash,
                estimated_input_tokens=snapshot.estimated_input_tokens,
                budget_allocation_id=request.budget.allocation_id,
                deadline_at=min(deadline, datetime.now(UTC) + timedelta(seconds=self._timeout)),
                model=self._model,
                tools=(),
                output_schema=None,
            )
            cancel = SqlMainCancelSignal(
                self._engine,
                self._schema,
                self._lease,
                actor=self._actor,
            )
            watcher = asyncio.create_task(cancel.watch())
            content: list[str] = []
            usage: Usage | None = None
            completed = False
            try:
                async for event in self._provider.stream(call, cancel=cancel):
                    if isinstance(event, ContentDelta):
                        content.append(event.text)
                    elif isinstance(event, ProviderError):
                        raise AgentRuntimeError(event.failure)
                    elif isinstance(event, ProviderUsage):
                        usage = event.usage
                    elif isinstance(event, ProviderFinish):
                        completed = event.reason == "stop"
            finally:
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)
                await self._record_candidate(call, "".join(content))
            if not completed or usage is None or usage.status != "complete":
                raise runtime_failure(
                    "INCOMPLETE_MODEL_OUTPUT",
                    category="context",
                    public_message="Summary or usage incomplete; original facts were not replaced.",
                )
            text = verified_extraction(source.content, "".join(content))
            current = await self._reader.read(request, source.resource)
            if current != source:
                errors.version_conflict("Original context changed during compression")
            await self._access.verify(self._actor, snapshot)
            envelope: dict[str, object] = {
                "kind": "task-relevant-extract-v2",
                "version": "2",
                "coverage": "model_selected_subset_not_semantically_complete",
                "source": source.resource.model_dump(mode="json", by_alias=True),
                "sourceDigest": content_digest({"content": source.content}),
                "text": text,
                "rawOutput": "".join(content),
                "request": request.model_dump(mode="json", by_alias=True),
                "model": self._model.model_dump(mode="json", by_alias=True),
                "attemptId": str(call.attempt_id),
                "inputSnapshotId": str(snapshot.snapshot_id),
                "renderedRequestDigest": summary_payload["renderedRequestDigest"],
                "usage": usage.model_dump(mode="json", by_alias=True),
            }
            ref = await self._payloads.put(
                request.actor.scope_id, envelope, content_digest(envelope)
            )
            # Anchor the summary in existing GC roots. Revalidate the claim before publishing.
            async with self._sessions() as db, db.begin():
                lease = self._lease()
                lock_sql = locks.build_lock_statements(self._schema)
                facts.verify_slot(
                    await locks.lock_slot(db, lock_sql, lease.scope_id, lease.session_id), lease
                )
                run = facts.verify_run(
                    await locks.lock_run(db, lock_sql, lease.scope_id, lease.run_id),
                    self._actor,
                    lease,
                    now=datetime.now(UTC),
                )
                await revalidate_authority(
                    db, lock_sql, self._actor, run=run, now=datetime.now(UTC)
                )
                if run["desired_control"] is not None or run["status"] != "running":
                    errors.control_pending("Compression was cancelled before publication")
                await db.execute(
                    scoped(
                        "UPDATE {t}model_attempt SET output_ref=:ref WHERE scope_id=:scope "
                        "AND id=:id AND status='completed'",
                        self._schema,
                    ),
                    {"scope": request.actor.scope_id, "id": call.attempt_id, "ref": ref},
                )
            return CompressedSource(
                content=text,
                summary_ref=ResourceRef(
                    resource_type="context_summary",
                    resource_id=ref,
                    resource_version="2",
                    scope=source.resource.scope,
                ),
            )

    async def _record_candidate(self, call: ProviderRequest, text: str) -> None:
        """Keep even rejected/partial output private and durable for reconciliation."""
        candidate: dict[str, object] = {
            "kind": "compression-candidate-v1",
            "text": text,
            "attemptId": str(call.attempt_id),
            "model": call.model.model_dump(mode="json", by_alias=True),
        }
        ref = await self._payloads.put(call.scope_id, candidate, content_digest(candidate))
        async with self._sessions() as db, db.begin():
            await db.execute(
                scoped(
                    "UPDATE {t}model_attempt SET output_ref=:ref "
                    "WHERE scope_id=:scope AND run_id=:run AND id=:id "
                    "AND input_ref=:input AND output_ref IS NULL",
                    self._schema,
                ),
                {
                    "scope": call.scope_id,
                    "run": call.run_id,
                    "id": call.attempt_id,
                    "input": call.input_ref,
                    "ref": ref,
                },
            )

    async def read_summary(self, actor: ActorContext, resource: ResourceRef) -> dict[str, object]:
        if actor != self._actor or resource.scope.scope_id != actor.scope_id:
            errors.forbidden("Summary principal changed")
        envelope = await self._payloads.get(actor.scope_id, resource.resource_id)
        request = ContextRequest.model_validate(envelope.get("request"))
        original = ResourceRef.model_validate(envelope.get("source"))
        if (
            envelope.get("kind") != "task-relevant-extract-v2"
            or resource.resource_version != "2"
            or request.actor != actor
            or resource.scope != original.scope
        ):
            errors.forbidden("Summary provenance is invalid")
        current = await self._reader.read(request, original)
        if content_digest({"content": current.content}) != envelope.get("sourceDigest"):
            errors.version_conflict("Summary source content changed")
        if verified_extraction(current.content, str(envelope.get("rawOutput"))) != envelope.get(
            "text"
        ):
            errors.invalid_input("Summary extraction is corrupt")
        input_snapshot, input_payload = await self._raw.load(
            actor.scope_id, UUID(str(envelope["inputSnapshotId"]))
        )
        rendered = input_payload.get("renderedRequest")
        if (
            not isinstance(rendered, dict)
            or content_digest(rendered) != envelope.get("renderedRequestDigest")
            or input_payload.get("renderedRequestDigest") != envelope.get("renderedRequestDigest")
            or rendered.get("modelConfig") != envelope.get("model")
            or input_snapshot.run_id != request.run_id
        ):
            errors.invalid_input("Summary rendered model input audit is invalid")
        async with self._sessions() as db:
            audit = (
                (
                    await db.execute(
                        scoped(
                            "SELECT output_ref,context_snapshot_id,usage FROM {t}model_attempt "
                            "WHERE scope_id=:scope AND id=:id AND run_id=:run "
                            "AND status='completed'",
                            self._schema,
                        ),
                        {
                            "scope": actor.scope_id,
                            "id": UUID(str(envelope["attemptId"])),
                            "run": request.run_id,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
        if (
            audit is None
            or audit["output_ref"] != resource.resource_id
            or str(audit["context_snapshot_id"]) != envelope.get("inputSnapshotId")
            or audit["usage"] != envelope.get("usage")
            or Usage.model_validate(audit["usage"]).status != "complete"
        ):
            errors.invalid_input("Summary model audit is unavailable")
        return envelope

    async def validate(self, snapshot: PreparedContext, payload: dict[str, object]) -> None:
        for source in snapshot.sources:
            if source.summary_ref is None:
                continue
            envelope = await self.read_summary(self._actor, source.summary_ref)
            request = ContextRequest.model_validate(envelope["request"])
            entries = payload.get("sources")
            if not isinstance(entries, list):
                errors.invalid_input("Summary payload is invalid")
            entry = next(
                (
                    item
                    for item in entries
                    if isinstance(item, dict) and item.get("sourceId") == source.source_id
                ),
                None,
            )
            if (
                request.session_id != snapshot.session_id
                or request.run_id != snapshot.run_id
                or envelope["source"] != source.resource.model_dump(mode="json", by_alias=True)
                or entry is None
                or entry.get("content") != envelope["text"]
                or entry.get("summaryRef")
                != source.summary_ref.model_dump(mode="json", by_alias=True)
            ):
                errors.invalid_input("Summary binding differs from its snapshot")


class SummaryResourceAccess:
    """Delegate originals to the host; summaries have no independent read grant."""

    def __init__(self, original: ContextResourceAccess, compression: SqlContextCompression) -> None:
        self._original, self._compression = original, compression

    async def authorize_session(self, actor: ActorContext, session_id: UUID) -> None:
        await self._original.authorize_session(actor, session_id)

    async def authorize_resource(self, actor: ActorContext, resource: ResourceRef) -> None:
        if resource.resource_type == "context_summary":
            await self._compression.read_summary(actor, resource)
        else:
            await self._original.authorize_resource(actor, resource)

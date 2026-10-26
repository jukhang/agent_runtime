"""Context preparation cannot silently lose mandatory facts or reuse stale payloads."""

from collections.abc import AsyncIterator
from copy import deepcopy
from uuid import UUID, uuid4

import pytest

from agent_runtime.context_runtime import ContextBoundProvider, ContextRuntime, LoadedSource
from agent_runtime.contracts.canonical import canonical_bytes, content_digest
from agent_runtime.contracts.context import ContextRequest, PreparedContext
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.contracts.provider import ProviderEvent
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.ports import CancelSignal

REF = {"key": {"namespace": "test", "key": "model"}, "revision": "1"}


def request(*, max_input_tokens: int = 10000, refs: int = 1) -> ContextRequest:
    scope = "scope"
    return ContextRequest.model_validate(
        {
            "actor": {
                "scopeId": scope,
                "actorId": "user",
                "authRevision": "1",
                "grantsRef": "grant",
            },
            "sessionId": str(uuid4()),
            "runId": str(uuid4()),
            "profile": REF,
            "model": REF,
            "prompt": {"key": {"namespace": "test", "key": "prompt"}, "revision": "1"},
            "tools": [],
            "resourceRefs": [
                {
                    "resourceType": "attachment",
                    "resourceId": f"file-{i}",
                    "resourceVersion": "1",
                    "scope": {"scopeId": scope},
                }
                for i in range(refs)
            ],
            "budget": {
                "allocationId": str(uuid4()),
                "maxInputTokens": max_input_tokens,
                "reservedOutputTokens": 100,
            },
        }
    )


class Reader:
    def __init__(self, *, required: bool = True, changed: bool = False) -> None:
        self.required = required
        self.changed = changed

    async def read(self, request: ContextRequest, resource: ResourceRef) -> LoadedSource:
        if self.changed:
            resource = resource.model_copy(update={"resource_version": "2"})
        return LoadedSource(resource=resource, content="contents", required=self.required)


class Estimator:
    def count(self, text: str) -> int:
        return len(text)


class Payloads:
    def __init__(self) -> None:
        self.data: dict[str, dict[str, object]] = {}

    async def put(self, scope_id: str, payload: dict[str, object], digest: str) -> str:
        assert content_digest(payload) == digest
        ref = f"payload-{digest}"
        self.data[ref] = deepcopy(payload)
        return ref

    async def get(self, scope_id: str, ref: str) -> dict[str, object]:
        return deepcopy(self.data[ref])


class Snapshots:
    def __init__(self) -> None:
        self.data: dict[UUID, PreparedContext] = {}

    async def save(self, draft: PreparedContext) -> PreparedContext:
        result = draft.model_copy(update={"context_version": "1"})
        self.data[result.snapshot_id] = result
        return result

    async def get(self, scope_id: str, snapshot_id: UUID) -> PreparedContext:
        result = self.data[snapshot_id]
        assert result.scope_id == scope_id
        return result


def runtime(
    *, required: bool = True, changed: bool = False
) -> tuple[ContextRuntime, Payloads, Snapshots]:
    payloads = Payloads()
    snapshots = Snapshots()
    return (
        ContextRuntime(
            reader=Reader(required=required, changed=changed),
            estimator=Estimator(),
            payloads=payloads,
            snapshots=snapshots,
        ),
        payloads,
        snapshots,
    )


@pytest.mark.asyncio
async def test_prepare_load_and_digest_integrity() -> None:
    service, payloads, _ = runtime()
    context = await service.prepare(request())
    recovered, payload = await service.load(context.scope_id, context.snapshot_id)
    assert recovered == context
    sources = payload["sources"]
    assert isinstance(sources, list)
    assert sources[0]["content"] == "contents"
    assert context.estimated_input_tokens == len(canonical_bytes(payload).decode())
    payloads.data[context.payload_ref]["sources"] = []
    with pytest.raises(AgentRuntimeError):
        await service.load(context.scope_id, context.snapshot_id)


@pytest.mark.asyncio
async def test_optional_source_omitted_but_required_source_fails() -> None:
    empty, _, _ = runtime()
    base = await empty.prepare(request(refs=0))
    max_tokens = base.estimated_input_tokens + 5
    optional, _, _ = runtime(required=False)
    result = await optional.prepare(request(max_input_tokens=max_tokens))
    assert result.sources[0].disposition == "omitted"
    assert result.sources[0].reason == "input_token_budget"
    required, payloads, _ = runtime()
    with pytest.raises(AgentRuntimeError):
        await required.prepare(request(max_input_tokens=max_tokens))
    assert not payloads.data


@pytest.mark.asyncio
async def test_rejects_stale_or_cross_scope_sources() -> None:
    service, _, _ = runtime(changed=True)
    with pytest.raises(AgentRuntimeError):
        await service.prepare(request())
    service, _, _ = runtime()
    req = request()
    wrong = req.resource_refs[0].model_copy(
        update={"scope": req.resource_refs[0].scope.model_copy(update={"scope_id": "other"})}
    )
    with pytest.raises(AgentRuntimeError):
        await service.prepare(req.model_copy(update={"resource_refs": (wrong,)}))


@pytest.mark.asyncio
async def test_provider_never_opens_stream_for_unbound_snapshot() -> None:
    service, _, _ = runtime()
    req = request()
    context = await service.prepare(req)
    opened = 0

    class Access:
        denied = False
        checked = 0

        async def verify(self, actor: ActorContext, snapshot: PreparedContext) -> None:
            assert actor == req.actor
            assert snapshot == context
            self.checked += 1
            if self.denied:
                raise runtime_failure(
                    "FORBIDDEN", category="authorization", public_message="Access revoked."
                )

    class Backend:
        async def stream(
            self, request: ProviderRequest, *, cancel: CancelSignal | None = None
        ) -> AsyncIterator[ProviderEvent]:
            nonlocal opened
            opened += 1
            if False:
                yield  # pragma: no cover

    access = Access()
    provider = ContextBoundProvider(service, Backend(), actor=req.actor, access=access)
    model_request = ProviderRequest.model_validate(
        {
            "scopeId": context.scope_id,
            "sessionId": str(context.session_id),
            "runId": str(context.run_id),
            "attemptId": str(uuid4()),
            "contextSnapshotId": str(context.snapshot_id),
            "contextVersion": context.context_version,
            "inputRef": context.payload_ref,
            "inputDigest": context.content_hash,
            "estimatedInputTokens": context.estimated_input_tokens,
            "budgetAllocationId": str(context.budget.allocation_id),
            "deadlineAt": "2027-09-14T11:00:00Z",
            "model": {
                "ref": REF,
                "connection": REF,
                "modelName": "model",
                "maxOutputTokens": 100,
                "thinking": "default",
                "providerOptions": {},
            },
            "tools": [],
            "outputSchema": None,
        }
    )
    async for _ in provider.stream(model_request):
        pass
    assert opened == 1
    assert access.checked == 1
    access.denied = True
    with pytest.raises(AgentRuntimeError):
        async for _ in provider.stream(model_request):
            pass
    assert opened == 1
    assert access.checked == 2
    tampered = model_request.model_copy(update={"input_digest": "b" * 64})
    with pytest.raises(AgentRuntimeError):
        async for _ in provider.stream(tampered):
            pass
    assert opened == 1
    assert access.checked == 2


@pytest.mark.asyncio
async def test_required_sources_reserve_budget_before_optional_sources():
    req = request(max_input_tokens=1500, refs=2)

    class MixedReader:
        async def read(self, request, resource):
            required = resource.resource_id == "file-1"
            return LoadedSource(
                resource=resource, content="mandatory" if required else "x" * 800, required=required
            )

    payloads, snapshots = Payloads(), Snapshots()
    context = ContextRuntime(
        reader=MixedReader(), estimator=Estimator(), payloads=payloads, snapshots=snapshots
    )
    snapshot = await context.prepare(req)
    assert [record.disposition for record in snapshot.sources] == ["omitted", "included"]
    assert snapshot.sources[0].reason == "input_token_budget"
    assert snapshot.sources[1].required


@pytest.mark.asyncio
async def test_smaller_budget_creates_distinct_snapshot_without_mutating_old_payload():
    req = request(max_input_tokens=3000, refs=1)

    class OptionalReader:
        async def read(self, request, resource):
            return LoadedSource(resource=resource, content="x" * 900, required=False)

    payloads, snapshots = Payloads(), Snapshots()
    context = ContextRuntime(
        reader=OptionalReader(), estimator=Estimator(), payloads=payloads, snapshots=snapshots
    )
    before = await context.prepare(req)
    after = await context.prepare(
        req.model_copy(update={"budget": req.budget.model_copy(update={"max_input_tokens": 1000})})
    )
    assert before.snapshot_id != after.snapshot_id
    assert before.content_hash != after.content_hash
    assert before.sources[0].disposition == "included"
    assert after.sources[0].disposition == "omitted"
    _, old = await context.load(req.actor.scope_id, before.snapshot_id)
    assert old["sources"][0]["content"] == "x" * 900


@pytest.mark.asyncio
async def test_optional_priority_keeps_recent_history_before_old_history():
    req = request(max_input_tokens=1500, refs=2)

    class HistoryReader:
        async def read(self, request, resource):
            return LoadedSource(
                resource=resource,
                content="x" * 700,
                required=False,
                priority=2 if resource.resource_id == "file-1" else 1,
            )

    context = ContextRuntime(
        reader=HistoryReader(), estimator=Estimator(), payloads=Payloads(), snapshots=Snapshots()
    )
    snapshot = await context.prepare(req)
    assert [source.disposition for source in snapshot.sources] == ["omitted", "included"]

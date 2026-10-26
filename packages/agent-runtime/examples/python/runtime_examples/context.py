"""In-memory ContextRuntime ports for learning, not durable/multi-worker storage."""

import asyncio
from copy import deepcopy
from uuid import UUID, uuid4

from agent_runtime.context_runtime import ContextRuntime, LoadedSource
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.context import ContextBudget, ContextRequest, PreparedContext
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.transactions import ActorContext
from runtime_examples.agent_config import ref


def actor() -> ActorContext:
    return ActorContext(
        scope_id="tutorial", actor_id="developer", auth_revision="1", grants_ref="local-owner"
    )


class ByteEstimator:
    def count(self, text: str) -> int:
        # Conservative illustrative bound, not a provider tokenizer or bill.
        return len(text.encode("utf-8"))


class MemorySources:
    def __init__(self, principal: ActorContext) -> None:
        self.actor = principal
        self.sources: dict[ResourceRef, LoadedSource] = {}

    async def read(self, request: ContextRequest, resource: ResourceRef) -> LoadedSource:
        if request.actor != self.actor or resource.scope.scope_id != self.actor.scope_id:
            raise runtime_failure(
                "FORBIDDEN", category="authorization", public_message="Wrong actor"
            )
        source = self.sources.get(resource)
        if source is None:
            raise runtime_failure(
                "VERSION_CONFLICT", category="context", public_message="Source revoked or changed"
            )
        return source

    async def verify(self, principal: ActorContext, snapshot: PreparedContext) -> None:
        if principal != self.actor or (snapshot.scope_id, snapshot.auth_revision) != (
            principal.scope_id,
            principal.auth_revision,
        ):
            raise runtime_failure(
                "FORBIDDEN", category="authorization", public_message="Wrong actor"
            )
        for source in snapshot.sources:
            if source.resource not in self.sources:
                raise runtime_failure(
                    "FORBIDDEN", category="authorization", public_message="Source revoked"
                )


class MemoryPayloads:
    def __init__(self) -> None:
        self.data: dict[tuple[str, str], dict[str, object]] = {}

    async def put(self, scope_id: str, payload: dict[str, object], digest: str) -> str:
        if content_digest(payload) != digest:
            raise ValueError("Payload digest mismatch")
        key = f"payload-{digest}"
        self.data[scope_id, key] = deepcopy(payload)
        return key

    async def get(self, scope_id: str, ref: str) -> dict[str, object]:
        return deepcopy(self.data[scope_id, ref])


class MemorySnapshots:
    def __init__(self) -> None:
        self.data: dict[tuple[str, UUID], PreparedContext] = {}
        self.versions: dict[tuple[str, UUID, UUID], int] = {}

    async def save(self, draft: PreparedContext) -> PreparedContext:
        key = draft.scope_id, draft.session_id, draft.run_id
        version = self.versions.get(key, 0) + 1
        if (draft.scope_id, draft.snapshot_id) in self.data:
            raise ValueError("Cannot overwrite a snapshot")
        value = draft.model_copy(update={"context_version": str(version)})
        self.data[draft.scope_id, draft.snapshot_id] = value
        self.versions[key] = version
        return value

    async def get(self, scope_id: str, snapshot_id: UUID) -> PreparedContext:
        return self.data[scope_id, snapshot_id]


def request(text: str, resources: tuple[ResourceRef, ...] = ()) -> ContextRequest:
    return ContextRequest(
        actor=actor(),
        session_id=uuid4(),
        run_id=uuid4(),
        profile=ref("answer-profile"),
        model=ref("model"),
        prompt=ref("prompt"),
        tools=(),
        resource_refs=resources,
        prompt_text="Answer using verified reference data; explain missing evidence.",
        user_input=text,
        budget=ContextBudget(
            allocation_id=uuid4(), max_input_tokens=20000, reserved_output_tokens=2048
        ),
    )


def build_context() -> tuple[ContextRuntime, MemorySources]:
    sources = MemorySources(actor())
    context = ContextRuntime(
        reader=sources,
        estimator=ByteEstimator(),
        payloads=MemoryPayloads(),
        snapshots=MemorySnapshots(),
    )
    return context, sources


async def demo() -> PreparedContext:
    context, sources = build_context()
    resource = ResourceRef(
        resource_type="attachment",
        resource_id="production-hours",
        resource_version="1",
        scope=ResourceScope(scope_id=actor().scope_id),
    )
    sources.sources[resource] = LoadedSource(
        resource=resource, content="Planned hours: 1000. Actual hours: 800.", required=True
    )
    snapshot = await context.prepare(request("What is the actual/planned ratio?", (resource,)))
    restored, payload = await context.load(snapshot.scope_id, snapshot.snapshot_id)
    await sources.verify(actor(), restored)
    assert restored == snapshot and payload["userInput"] == "What is the actual/planned ratio?"
    return snapshot


if __name__ == "__main__":
    print(asyncio.run(demo()).model_dump_json(by_alias=True, indent=2))

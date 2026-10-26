"""A real, audited Qwen answer from either original or restored compressed input."""

import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import sqlalchemy as sa

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.contracts.model import ProviderRequest
from agent_runtime.contracts.provider import ContentDelta, ProviderError, ProviderFinish
from agent_runtime.provider import ProviderInvoker
from agent_runtime.providers.context_input import ContextOpenAIInputResolver
from agent_runtime.providers.openai_compatible import OpenAIAdapterProfile, OpenAICompatibleProvider
from agent_runtime.storage.model_attempts import SqlModelAttemptStore


async def answer(
    *,
    engine,
    schema,
    runtime,
    access,
    actor,
    lease,
    prepared,
    model,
    client,
    connection,
    capabilities,
    tools,
    payloads,
):
    request = ProviderRequest(
        scope_id=prepared.scope_id,
        session_id=prepared.session_id,
        run_id=prepared.run_id,
        attempt_id=uuid4(),
        context_snapshot_id=prepared.snapshot_id,
        context_version=prepared.context_version,
        input_ref=prepared.payload_ref,
        input_digest=prepared.content_hash,
        estimated_input_tokens=prepared.estimated_input_tokens,
        budget_allocation_id=prepared.budget.allocation_id,
        deadline_at=datetime.now(UTC) + timedelta(seconds=90),
        model=model,
        tools=(),
        output_schema=None,
    )
    invoker = ProviderInvoker(
        OpenAICompatibleProvider(
            client,
            inputs=ContextOpenAIInputResolver(
                context=runtime,
                actor=actor,
                access=access,
                tools=tools,
            ),
            connections=connection,
            profile=OpenAIAdapterProfile.qwen(),
        ),
        capabilities=capabilities,
        attempts=SqlModelAttemptStore(engine, schema, actor=actor, lease=lease),
    )
    chunks = []
    finished = False
    async for event in invoker.stream(request):
        if isinstance(event, ContentDelta):
            chunks.append(event.text)
        elif isinstance(event, ProviderError):
            raise AgentRuntimeError(event.failure)
        elif isinstance(event, ProviderFinish):
            finished = event.reason == "stop"
    assert finished
    text = "".join(chunks)
    payload = {
        "kind": "compression-semantic-answer",
        "text": text,
        "attemptId": str(request.attempt_id),
        "model": model.model_dump(mode="json", by_alias=True),
    }
    ref = await payloads.put(actor.scope_id, payload, content_digest(payload))
    async with engine.begin() as db:
        await db.execute(
            sa.text(f"UPDATE {schema}.model_attempt SET output_ref=:ref WHERE id=:id"),
            {"id": request.attempt_id, "ref": ref},
        )
    lower = text.lower()
    values, percentages = set(), set()
    for match in re.finditer(
        r"(?<![\w])(?P<value>(?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)(?:\.[0-9]+)?)"
        r"(?P<unit>\s*%)?",
        text,
    ):
        value = Decimal(match["value"].replace(",", ""))
        values.add(value)
        if match["unit"]:
            percentages.add(value)
    assert {Decimal(1000), Decimal(800), Decimal(2300)} <= values, text
    assert Decimal(80) in percentages, text
    assert "legal" in lower and "signature" in lower, text
    assert "not" in lower or "without" in lower, text
    return {"attemptId": str(request.attempt_id), "text": text, "outputRef": ref}

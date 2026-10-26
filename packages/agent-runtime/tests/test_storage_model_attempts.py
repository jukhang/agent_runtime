"""Real PostgreSQL ModelAttempt CAS tests; each case owns a disposable schema."""

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy", reason="requires agent-runtime[storage]")

import sqlalchemy as sa  # noqa: E402
from test_storage_uow import _seed, engine_for, migrate, run  # noqa: E402

from agent_runtime.contracts.failure import AgentRuntimeError, FailureEnvelope  # noqa: E402
from agent_runtime.contracts.model import (  # noqa: E402
    ProviderCapabilities,
    ProviderRequest,
)
from agent_runtime.contracts.provider import ProviderFinish  # noqa: E402
from agent_runtime.contracts.references import CapabilityRef  # noqa: E402
from agent_runtime.contracts.results import Usage  # noqa: E402
from agent_runtime.provider import AttemptTerminal, ProviderInvoker  # noqa: E402
from agent_runtime.storage.model_attempts import SqlModelAttemptStore  # noqa: E402

DSN = os.environ.get("AGENT_RUNTIME_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="AGENT_RUNTIME_TEST_DSN is not configured")


@pytest.fixture(scope="module")
def attempt_world() -> Any:
    schema = f"p03_provider_{uuid.uuid4().hex[:12]}"
    assert migrate("upgrade", schema).status == "applied"
    world = run(lambda: _seed(schema))
    snapshot_id = uuid.uuid4()
    budget_id = uuid.uuid4()

    async def prepare() -> None:
        engine = engine_for(schema)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {schema}.context_snapshot "
                        "(id, scope_id, session_id, run_id, auth_revision, context_version, "
                        "estimated_input_tokens, schema_revision, task_type, payload_ref, "
                        "content_hash, source_refs, degradation, model_ref, prompt_ref, "
                        "tool_refs, budget) VALUES (:id, :scope, :session, :run, 1, 1, 10, 1, "
                        "'chat', 'input', :hash, '[]', '{}', '{}', '{}', '[]', '{}')"
                    ),
                    {
                        "id": snapshot_id,
                        "scope": world.scope,
                        "session": world.session_id,
                        "run": world.run_id,
                        "hash": "3" * 64,
                    },
                )
                await connection.execute(
                    sa.text(
                        f"INSERT INTO {schema}.budget_allocation "
                        "(id, scope_id, root_run_id, limits, reserved, unknown_reserved, "
                        "settled, usage_status, unsettled_count, version) VALUES "
                        "(:id, :scope, :run, '{}', '{}', '{}', '{}', 'complete', 0, 1)"
                    ),
                    {"id": budget_id, "scope": world.scope, "run": world.run_id},
                )
        finally:
            await engine.dispose()

    run(prepare)
    world.values["snapshot_id"] = snapshot_id
    world.values["budget_id"] = budget_id
    yield world

    async def drop() -> None:
        engine = engine_for(schema)
        try:
            async with engine.begin() as connection:
                await connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        finally:
            await engine.dispose()

    run(drop)


@pytest.fixture(scope="module", autouse=True)
def no_provider_schema_leaks() -> Any:
    yield

    async def leftovers() -> str:
        engine = engine_for("public")
        try:
            async with engine.connect() as connection:
                return str(
                    (
                        await connection.execute(
                            sa.text(
                                "SELECT coalesce(string_agg(nspname, ','), '') "
                                "FROM pg_namespace WHERE nspname LIKE 'p03_provider_%'"
                            )
                        )
                    ).scalar_one()
                )
        finally:
            await engine.dispose()

    assert not run(leftovers), "provider test schemas were left behind"


def provider_request(world: Any, *, attempt_id: uuid.UUID | None = None) -> ProviderRequest:
    return ProviderRequest.model_validate(
        {
            "scopeId": world.scope,
            "sessionId": str(world.session_id),
            "runId": str(world.run_id),
            "attemptId": str(attempt_id or uuid.uuid4()),
            "contextSnapshotId": str(world.values["snapshot_id"]),
            "contextVersion": "1",
            "inputRef": "input",
            "inputDigest": "a" * 64,
            "estimatedInputTokens": 10,
            "budgetAllocationId": str(world.values["budget_id"]),
            "deadlineAt": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
            "model": {
                "ref": {"key": {"namespace": "test", "key": "model"}, "revision": "1"},
                "connection": {
                    "key": {"namespace": "test", "key": "connection"},
                    "revision": "1",
                },
                "modelName": "model",
                "maxOutputTokens": 100,
                "thinking": "default",
                "providerOptions": {},
            },
            "tools": [],
            "outputSchema": None,
        }
    )


def completed(at: datetime | None = None) -> AttemptTerminal:
    return AttemptTerminal(
        status="completed",
        finish_reason="stop",
        usage=Usage(
            status="complete",
            reason="provider_reported",
            input_tokens="3",
            output_tokens="2",
            total_tokens="5",
        ),
        failure=None,
        finished_at=at or datetime.now(UTC),
    )


def failed(code: str = "PROVIDER_TIMEOUT") -> AttemptTerminal:
    return AttemptTerminal(
        status="failed",
        finish_reason=None,
        usage=Usage(
            status="unknown",
            reason="provider_not_reported",
            input_tokens=None,
            output_tokens=None,
            total_tokens=None,
        ),
        failure=FailureEnvelope(
            code=code,
            category="provider",
            retryable=True,
            public_message="Provider failed.",
        ),
        finished_at=datetime.now(UTC),
    )


def test_begin_and_same_terminal_are_idempotent(attempt_world: Any) -> None:
    async def scenario() -> tuple[str, dict[str, Any]]:
        engine = engine_for(attempt_world.schema, search_path=False)
        request = provider_request(attempt_world)
        store = SqlModelAttemptStore(engine, attempt_world.schema)
        try:
            now = datetime.now(UTC)
            await store.begin(request, started_at=now)
            await store.begin(request, started_at=now + timedelta(seconds=1))
            terminal = completed()
            await store.finish(request, terminal)
            await store.finish(request, completed(terminal.finished_at + timedelta(seconds=5)))
            async with engine.connect() as connection:
                row = (
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT status, usage FROM {attempt_world.schema}.model_attempt "
                                "WHERE id = :id"
                            ),
                            {"id": request.attempt_id},
                        )
                    )
                    .mappings()
                    .one()
                )
            return str(row["status"]), dict(row["usage"])
        finally:
            await engine.dispose()

    status, usage = run(scenario)
    assert status == "completed"
    assert usage["totalTokens"] == "5"


def test_terminal_cannot_be_overwritten_or_restarted(attempt_world: Any) -> None:
    async def scenario() -> tuple[str, str]:
        engine = engine_for(attempt_world.schema, search_path=False)
        request = provider_request(attempt_world)
        store = SqlModelAttemptStore(engine, attempt_world.schema)
        try:
            await store.begin(request, started_at=datetime.now(UTC))
            await store.finish(request, completed())
            with pytest.raises(AgentRuntimeError) as overwrite:
                await store.finish(request, failed())
            with pytest.raises(AgentRuntimeError) as restart:
                await store.begin(request, started_at=datetime.now(UTC))
            return overwrite.value.failure.code, restart.value.failure.code
        finally:
            await engine.dispose()

    assert run(scenario) == ("VERSION_CONFLICT", "VERSION_CONFLICT")


def test_concurrent_different_terminals_have_one_winner(attempt_world: Any) -> None:
    async def scenario() -> tuple[int, int, str]:
        engine = engine_for(attempt_world.schema, pool_size=2, search_path=False)
        request = provider_request(attempt_world)
        store = SqlModelAttemptStore(engine, attempt_world.schema)
        try:
            await store.begin(request, started_at=datetime.now(UTC))

            async def settle(terminal: AttemptTerminal) -> str:
                try:
                    await store.finish(request, terminal)
                    return "won"
                except AgentRuntimeError as exc:
                    return exc.failure.code

            outcomes = await asyncio.gather(settle(completed()), settle(failed()))
            async with engine.connect() as connection:
                status = str(
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT status FROM {attempt_world.schema}.model_attempt "
                                "WHERE id = :id"
                            ),
                            {"id": request.attempt_id},
                        )
                    ).scalar_one()
                )
            return outcomes.count("won"), outcomes.count("VERSION_CONFLICT"), status
        finally:
            await engine.dispose()

    won, conflicted, status = run(scenario)
    assert (won, conflicted) == (1, 1)
    assert status in {"completed", "failed"}


def test_concurrent_begin_is_idempotent_but_rejects_changed_identity(
    attempt_world: Any,
) -> None:
    async def scenario() -> tuple[int, list[str], str]:
        engine = engine_for(attempt_world.schema, pool_size=2, search_path=False)
        attempt_id = uuid.uuid4()
        request = provider_request(attempt_world, attempt_id=attempt_id)
        store = SqlModelAttemptStore(engine, attempt_world.schema)
        try:
            now = datetime.now(UTC)
            starts = await asyncio.gather(
                store.begin(request, started_at=now),
                store.begin(request, started_at=now),
            )
            changed = request.model_copy(update={"input_ref": "other-input"})
            with pytest.raises(AgentRuntimeError) as conflict:
                await store.begin(changed, started_at=now)
            async with engine.connect() as connection:
                count = int(
                    (
                        await connection.execute(
                            sa.text(
                                f"SELECT count(*) FROM {attempt_world.schema}.model_attempt "
                                "WHERE id = :id"
                            ),
                            {"id": attempt_id},
                        )
                    ).scalar_one()
                )
            return count, sorted(starts), conflict.value.failure.code
        finally:
            await engine.dispose()

    count, starts, code = run(scenario)
    assert (count, starts, code) == (
        1,
        ["already_running", "started"],
        "IDENTITY_MISMATCH",
    )


def test_begin_identity_digest_covers_the_complete_provider_request(attempt_world: Any) -> None:
    async def scenario() -> list[str]:
        engine = engine_for(attempt_world.schema, search_path=False)
        request = provider_request(attempt_world)
        store = SqlModelAttemptStore(engine, attempt_world.schema)
        try:
            await store.begin(request, started_at=datetime.now(UTC))
            alternate_tool = CapabilityRef.model_validate(
                {"key": {"namespace": "test", "key": "tool"}, "revision": "1"}
            )
            variants = (
                request.model_copy(update={"input_digest": "b" * 64}),
                request.model_copy(update={"context_version": "2"}),
                request.model_copy(update={"estimated_input_tokens": 11}),
                request.model_copy(update={"budget_allocation_id": uuid.uuid4()}),
                request.model_copy(
                    update={"deadline_at": request.deadline_at + timedelta(seconds=1)}
                ),
                request.model_copy(
                    update={
                        "model": request.model.model_copy(
                            update={"max_output_tokens": request.model.max_output_tokens + 1}
                        )
                    }
                ),
                request.model_copy(
                    update={"model": request.model.model_copy(update={"thinking": "enabled"})}
                ),
                request.model_copy(
                    update={
                        "model": request.model.model_copy(
                            update={"provider_options": {"temperature": 0.5}}
                        )
                    }
                ),
                request.model_copy(
                    update={
                        "model": request.model.model_copy(
                            update={
                                "connection": request.model.connection.model_copy(
                                    update={"revision": "2"}
                                )
                            }
                        )
                    }
                ),
                request.model_copy(update={"tools": (alternate_tool,)}),
                request.model_copy(update={"output_schema": alternate_tool}),
            )
            results: list[str] = []
            for variant in variants:
                try:
                    await store.begin(variant, started_at=datetime.now(UTC))
                except AgentRuntimeError as exc:
                    results.append(exc.failure.code)
            return results
        finally:
            await engine.dispose()

    assert run(scenario) == ["IDENTITY_MISMATCH"] * 11


def test_concurrent_invokers_issue_exactly_one_provider_request(attempt_world: Any) -> None:
    async def scenario() -> tuple[int, list[str]]:
        engine = engine_for(attempt_world.schema, pool_size=2, search_path=False)
        request = provider_request(attempt_world)
        store = SqlModelAttemptStore(engine, attempt_world.schema)
        started = asyncio.Event()
        release = asyncio.Event()

        class CountingBackend:
            def __init__(self) -> None:
                self.calls = 0

            async def stream(self, _request: ProviderRequest, *, cancel: Any = None):
                self.calls += 1
                started.set()
                await release.wait()
                yield ProviderFinish(
                    attempt_id=request.attempt_id,
                    sequence="0",
                    kind="finish",
                    reason="stop",
                    raw_reason="stop",
                )

        async def capabilities(_: ProviderRequest) -> ProviderCapabilities:
            return ProviderCapabilities(
                model=request.model.ref,
                streaming="supported",
                tool_calls="supported",
                structured_output="supported",
                thinking_control="supported",
                tools_with_structured_output="supported",
                tools_with_thinking="supported",
                usage="unknown",
                cancellation="supported",
                context_window=4096,
                max_output_tokens=1024,
                allowed_option_keys=(),
            )

        backend = CountingBackend()
        invoker = ProviderInvoker(backend, capabilities=capabilities, attempts=store)

        async def invoke() -> str:
            try:
                async for _ in invoker.stream(request):
                    pass
                return "completed"
            except AgentRuntimeError as exc:
                return exc.failure.code

        try:
            first = asyncio.create_task(invoke())
            await started.wait()
            second = asyncio.create_task(invoke())
            await asyncio.sleep(0)
            release.set()
            results = await asyncio.gather(first, second)
            return backend.calls, sorted(results)
        finally:
            await engine.dispose()

    assert run(scenario) == (1, ["FENCED", "completed"])

"""Service failure replays committed declarations without inventing producer output."""

import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from test_storage_child_recovery import pair  # noqa: F401
from test_storage_uow import _seed_attempt, _seed_item, actor, engine_for, lease, migrate, run

from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.ownership import ChildCommitGuard, ChildLeaseToken
from agent_runtime.contracts.payloads import ItemStarted
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.state_events import (
    ChildEventAuthority,
    ItemStartedDraft,
    StateEventBatch,
)
from agent_runtime.contracts.work_claims import ChildClaimedWork
from agent_runtime.projection import SessionProjection, display_event
from agent_runtime.storage import child_recovery
from agent_runtime.storage.child_output import ChildProducerBatch
from agent_runtime.storage.journal import SqlJournal, _row_event
from agent_runtime.storage.recovery import SqlRecovery

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENT_RUNTIME_TEST_DSN"), reason="requires guarded real PostgreSQL"
)


@pytest.fixture(scope="module")
def recovery_schema():
    assert sa.make_url(os.environ["AGENT_RUNTIME_TEST_DSN"]).database == "aletai_wangxh"
    schema = f"r05_terminal_empty_{uuid4().hex[:12]}"
    try:
        assert migrate("upgrade", schema).status == "applied"
        yield schema
    finally:

        async def drop():
            engine = engine_for(schema, search_path=False)
            try:
                async with engine.begin() as db:
                    await db.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            finally:
                await engine.dispose()

        run(drop)


@pytest.mark.parametrize(
    "mode,channels",
    [
        ("zero_delta", ("content", "analysis")),
        ("analysis_only", ("content", "analysis")),
        ("content", ("content", "analysis")),
        ("content_only_zero", ("content",)),
        ("analysis_only_zero", ("analysis",)),
    ],
)
def test_service_failure_replays_empty_channels_atomically(
    pair,  # noqa: F811
    monkeypatch,
    mode,
    channels,
):
    parent, child, engine, _, execute, rows = pair
    now = datetime.now(UTC)
    future = now + timedelta(minutes=10)
    item = run(lambda: _seed_item(child))
    attempt = run(lambda: _seed_attempt(child))
    for table, identity in (("run", child.run_id), ("work_item", child.work_id)):
        run(
            lambda table=table, identity=identity: execute(
                f"UPDATE {{t}}{table} SET lease_until=:future WHERE id=:id",
                future=future,
                id=identity,
            )
        )
    run(
        lambda: execute(
            "UPDATE {t}timeline_item SET status='streaming',final_revision=7 WHERE id=:id", id=item
        )
    )
    token = ChildLeaseToken.model_validate(lease(child).model_dump(exclude={"slot_version"}))
    guard = ChildCommitGuard(
        actor=actor(world=child),
        lease=token,
        expected_state_version="1",
        root_run_id=parent.run_id,
        expected_root_cancel_version="0",
        workspace_owner_run_id=child.run_id,
        workspace_generation="1",
        workspace_ref=ResourceRef(
            resource_type="workspace",
            resource_id=str(child.workspace_id),
            resource_version="1",
            scope=ResourceScope(scope_id=child.scope),
        ),
    )
    journal = SqlJournal(engine, child.schema, guard.actor)

    async def start():
        async with AsyncSession(engine) as db, db.begin():
            receipt = await journal.append_state_in_transaction(
                db,
                StateEventBatch(
                    authority=ChildEventAuthority(kind="child", guard=guard),
                    session_id=child.session_id,
                    trace_id="terminal-empty-start",
                    events=(
                        ItemStartedDraft(
                            kind="item.started",
                            producer_key=f"terminal-start:{item}",
                            occurred_at=now,
                            turn_id=child.turn_id,
                            run_id=child.run_id,
                            parent_run_id=parent.run_id,
                            step_id=None,
                            item_id=item,
                            model_attempt_id=attempt,
                            payload=ItemStarted(
                                item_type="message",
                                role="assistant",
                                channels=channels,
                                item_revision="7",
                            ),
                        ),
                    ),
                ),
            )
            await db.execute(
                sa.text(f'UPDATE "{child.schema}".timeline_item SET source_seq=:seq WHERE id=:id'),
                {"id": item, "seq": int(receipt.entries[0].session_seq)},
            )

    run(start)
    content = "durable partial answer" if mode == "content" else ""
    analysis = "durable analysis" if mode == "analysis_only" else ""
    drafts = [
        {
            "kind": "assistant.delta" if channel == "content" else "analysis.delta",
            "occurredAt": now,
            "payload": {"channel": channel, "deltaIndex": "0", "itemRevision": "7", "text": text},
        }
        for channel, text in (("content", content), ("analysis", analysis))
        if text
    ]
    if drafts:
        run(
            lambda: journal.append(
                ChildProducerBatch.model_validate(
                    {
                        "guard": guard,
                        "turn_id": child.turn_id,
                        "attempt_id": attempt,
                        "item_id": item,
                        "trace_id": "terminal-empty-output",
                        "events": drafts,
                    }
                )
            )
        )
    else:
        # The declaration, not a stale materialized item revision, owns completion identity.
        run(lambda: execute("UPDATE {t}timeline_item SET final_revision=1 WHERE id=:id", id=item))
    failure = runtime_failure(
        "BUDGET_EXHAUSTED",
        category="budget",
        public_message="Delegated deadline expired.",
        deadline_at=now,
    )
    run(
        lambda: execute(
            "UPDATE {t}model_attempt SET status='failed',finished_at=:now,"
            "usage_status='unknown',usage_reason='collection_failed',usage=CAST(:usage AS jsonb),"
            "failure=CAST(:failure AS jsonb) WHERE id=:id",
            id=attempt,
            now=now,
            usage=json.dumps(
                {
                    "status": "unknown",
                    "reason": "collection_failed",
                    "inputTokens": None,
                    "outputTokens": None,
                    "totalTokens": None,
                }
            ),
            failure=failure.failure.model_dump_json(by_alias=True),
        )
    )
    for table, identity in (("run", child.run_id), ("work_item", child.work_id)):
        run(
            lambda table=table, identity=identity: execute(
                f"UPDATE {{t}}{table} SET deadline_at=:now WHERE id=:id", now=now, id=identity
            )
        )
    run(
        lambda: execute(
            "UPDATE {t}capability_grant SET expires_at=:now WHERE run_id=:id",
            now=now,
            id=child.run_id,
        )
    )
    allocation = uuid4()
    run(
        lambda: execute(
            "INSERT INTO {t}budget_allocation "
            "(id,scope_id,root_run_id,limits,reserved,unknown_reserved,settled,"
            "usage_status,unsettled_count,version) VALUES "
            "(:id,:scope,:root,'{}',:held,:held,'{}','unknown',1,1)",
            id=allocation,
            scope=child.scope,
            root=parent.run_id,
            held=json.dumps({"tokens": 123}),
        )
    )
    claimed = ChildClaimedWork(
        disposition="child_claimed",
        lease=token,
        root_run_id=parent.run_id,
        root_cancel_version="0",
        state_version="1",
        checkpoint_id=child.checkpoint_id,
        budget_allocation_id=allocation,
        workspace_ref=guard.workspace_ref,
        workspace_generation="1",
    )
    prefix = run(lambda: rows("SELECT * FROM {t}event ORDER BY session_seq"))
    budget_before = run(
        lambda: rows("SELECT * FROM {t}budget_allocation WHERE id=:id", id=allocation)
    )
    audit_before = run(lambda: rows("SELECT * FROM {t}model_attempt WHERE id=:id", id=attempt))
    work_before = run(lambda: rows("SELECT * FROM {t}work_item WHERE id=:id", id=child.work_id))
    recovery = SqlRecovery(engine, child.schema)
    original_message = child_recovery.message

    async def fail_after_notice(*args, **kwargs):
        await original_message(*args, **kwargs)
        raise RuntimeError("rollback terminal empty output")

    with monkeypatch.context() as patch:
        patch.setattr(child_recovery, "message", fail_after_notice)
        with pytest.raises(RuntimeError, match="rollback terminal empty output"):
            run(lambda: recovery.handle_worker_failure(claimed, failure))
    assert run(lambda: rows("SELECT * FROM {t}event ORDER BY session_seq")) == prefix
    assert (
        run(lambda: rows("SELECT * FROM {t}work_item WHERE id=:id", id=child.work_id))
        == work_before
    )
    assert run(lambda: rows("SELECT status FROM {t}timeline_item WHERE id=:id", id=item)) == [
        {"status": "streaming"}
    ]
    assert not run(lambda: rows("SELECT id FROM {t}output_seal WHERE item_id=:id", id=item))
    assert not run(
        lambda: rows("SELECT id FROM {t}agent_message WHERE sender_run_id=:id", id=child.run_id)
    )
    assert run(lambda: recovery.handle_worker_failure(claimed, failure)).disposition == "failed"
    after = run(lambda: rows("SELECT * FROM {t}event ORDER BY session_seq"))
    assert after[: len(prefix)] == prefix
    assert run(lambda: recovery.handle_worker_failure(claimed, failure)).disposition == "fenced"
    assert run(lambda: rows("SELECT * FROM {t}event ORDER BY session_seq")) == after
    completions = [row for row in after if row["kind"] == "item.completed"]
    assert len(completions) == 1
    completion = _row_event(completions[0])
    assert completion.model_attempt_id == attempt and completion.payload.item_revision == "7"
    texts = {channel: content if channel == "content" else analysis for channel in channels}
    assert {part.channel: part.digest for part in completion.payload.channel_digests} == {
        channel: hashlib.sha256(text.encode()).hexdigest() for channel, text in texts.items()
    }
    assert {
        part.channel: part.last_delta_index for part in completion.payload.last_delta_indexes
    } == {channel: "0" if text else None for channel, text in texts.items()}
    assert len(
        run(lambda: rows("SELECT id FROM {t}output_seal WHERE item_id=:id", id=item))
    ) == int(bool(drafts))
    for include_analysis in (False, True):
        projection = SessionProjection(child.session_id, include_analysis=include_analysis)
        for row in after:
            event = display_event(_row_event(row), include_analysis=include_analysis)
            if event is not None:
                projection.apply(event)
        saved = projection.items[item]
        assert saved.status == "failed" and saved.revision == "7"
        assert {part.name: part.text for part in saved.channels} == {
            channel: text
            for channel, text in texts.items()
            if include_analysis or channel != "analysis"
        }
    strict = SessionProjection(child.session_id, include_analysis=True)
    for row in prefix:
        event = display_event(_row_event(row), include_analysis=True)
        if event is not None:
            strict.apply(event)
    with pytest.raises(ValueError, match="Completion omits declared output channels"):
        strict.apply(
            completion.model_copy(
                update={
                    "payload": completion.payload.model_copy(
                        update={"channel_digests": (), "last_delta_indexes": ()}
                    )
                }
            )
        )
    assert (
        run(lambda: rows("SELECT * FROM {t}budget_allocation WHERE id=:id", id=allocation))
        == budget_before
    )
    assert (
        run(lambda: rows("SELECT * FROM {t}model_attempt WHERE id=:id", id=attempt)) == audit_before
    )
    assert not run(lambda: rows("SELECT id FROM {t}budget_entry WHERE kind='settle'"))
    assert (
        len([row for row in after if row["kind"] == "run.failed" and row["run_id"] == child.run_id])
        == 1
    )
    assert not [row for row in after if row["kind"] in {"run.completed", "run.cancelled"}]
    assert (
        len(
            run(
                lambda: rows(
                    "SELECT id FROM {t}agent_message WHERE sender_run_id=:id "
                    "AND kind='task.failed'",
                    id=child.run_id,
                )
            )
        )
        == 1
    )
    saved = run(
        lambda: rows(
            "SELECT convert_from(b.content,'UTF8')::jsonb AS payload FROM {t}timeline_item i "
            "JOIN {t}context_blob b ON b.scope_id=i.scope_id AND b.blob_key=i.content_ref "
            "WHERE i.id=:id",
            id=item,
        )
    )[0]["payload"]
    assert saved == {"runId": str(child.run_id), "text": content}

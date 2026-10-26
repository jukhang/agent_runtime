"""Replay cancelled child items whose committed declaration precedes any delta."""

import hashlib
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession
from test_storage_child_recovery import pair  # noqa: F401
from test_storage_uow import _seed_attempt, _seed_item, actor, engine_for, lease, migrate, run

from agent_runtime.contracts.ownership import ChildCommitGuard, ChildLeaseToken
from agent_runtime.contracts.payloads import ItemStarted
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.state_events import (
    ChildEventAuthority,
    ItemStartedDraft,
    StateEventBatch,
)
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
    name = f"r05_child_empty_{uuid4().hex[:12]}"
    try:
        assert migrate("upgrade", name).status == "applied"
        yield name
    finally:

        async def drop():
            engine = engine_for(name, search_path=False)
            try:
                async with engine.begin() as db:
                    await db.execute(sa.text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))
            finally:
                await engine.dispose()

        run(drop)


@pytest.mark.parametrize("mode", ["zero_delta", "analysis_only", "content"])
def test_cancelled_child_replays_declared_channels_without_inventing_output(
    pair, monkeypatch, mode  # noqa: F811
):
    parent, child, engine, now, execute, rows = pair
    item = run(lambda: _seed_item(child))
    attempt = run(lambda: _seed_attempt(child))
    future = datetime.now(UTC) + timedelta(hours=1)

    async def prepare():
        async with engine.begin() as db:
            for table in ("run", "work_item"):
                identity = child.run_id if table == "run" else child.work_id
                await db.execute(
                    sa.text(
                        f'UPDATE "{child.schema}".{table} SET lease_until=:future WHERE id=:id'
                    ),
                    {"future": future, "id": identity},
                )
            await db.execute(
                sa.text(
                    f'UPDATE "{child.schema}".timeline_item '
                    "SET status='streaming',final_revision=1 "
                    "WHERE id=:id"
                ),
                {"id": item},
            )
            await db.execute(
                sa.text(
                    f'UPDATE "{child.schema}".model_attempt '
                    "SET status='running',finished_at=NULL "
                    "WHERE id=:id"
                ),
                {"id": attempt},
            )

    run(prepare)
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
                    trace_id="empty-child-start",
                    events=(
                        ItemStartedDraft(
                            kind="item.started",
                            producer_key=f"child-item:{item}",
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
                                channels=("content", "analysis"),
                                item_revision="1",
                            ),
                        ),
                    ),
                ),
            )
            await db.execute(
                sa.text(f'UPDATE "{child.schema}".timeline_item SET source_seq=:seq WHERE id=:id'),
                {"seq": int(receipt.entries[0].session_seq), "id": item},
            )

    run(start)
    content = "durable answer" if mode == "content" else ""
    analysis = "private analysis" if mode != "zero_delta" else ""
    drafts = [
        {
            "kind": "assistant.delta" if channel == "content" else "analysis.delta",
            "occurredAt": now,
            "payload": {"channel": channel, "deltaIndex": "0", "itemRevision": "1", "text": text},
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
                        "trace_id": "empty-child-output",
                        "events": drafts,
                    }
                )
            )
        )
    prefix = run(lambda: rows("SELECT * FROM {t}event ORDER BY session_seq"))
    expired = datetime.now(UTC) - timedelta(seconds=1)
    run(
        lambda: execute(
            "UPDATE {t}run SET lease_until=:expired WHERE id=:id", id=child.run_id, expired=expired
        )
    )
    run(
        lambda: execute(
            "UPDATE {t}work_item SET lease_until=:expired WHERE id=:id",
            id=child.work_id,
            expired=expired,
        )
    )
    run(
        lambda: execute(
            "UPDATE {t}run SET status='cancelled',waiting_spec=NULL WHERE id=:id", id=parent.run_id
        )
    )
    recovery = SqlRecovery(engine, child.schema)
    original_message = child_recovery.message

    async def fail_after_notice(*args, **kwargs):
        await original_message(*args, **kwargs)
        raise RuntimeError("rollback empty child completion")

    with monkeypatch.context() as patch:
        patch.setattr(child_recovery, "message", fail_after_notice)
        with pytest.raises(RuntimeError, match="rollback empty child"):
            run(lambda: recovery.recover_cancelled_children(child.scope))
    assert run(lambda: rows("SELECT * FROM {t}event ORDER BY session_seq")) == prefix
    assert not run(lambda: rows("SELECT id FROM {t}output_seal"))
    assert run(lambda: recovery.recover_cancelled_children(child.scope)) == 1
    assert run(lambda: recovery.recover_cancelled_children(child.scope)) == 0
    events = run(lambda: rows("SELECT * FROM {t}event ORDER BY session_seq"))
    assert events[: len(prefix)] == prefix
    completed = [row for row in events if row["kind"] == "item.completed"]
    assert len(completed) == 1 and completed[0]["model_attempt_id"] == attempt
    completion = _row_event(completed[0])
    assert {entry.channel: entry.digest for entry in completion.payload.channel_digests} == {
        channel: hashlib.sha256(text.encode()).hexdigest()
        for channel, text in (("content", content), ("analysis", analysis))
    }
    assert {
        entry.channel: entry.last_delta_index for entry in completion.payload.last_delta_indexes
    } == {
        "content": "0" if content else None,
        "analysis": "0" if analysis else None,
    }
    assert len(run(lambda: rows("SELECT id FROM {t}output_seal"))) == int(bool(drafts))
    for include_analysis in (False, True):
        projection = SessionProjection(child.session_id, include_analysis=include_analysis)
        for row in events:
            event = display_event(_row_event(row), include_analysis=include_analysis)
            if event is not None:
                projection.apply(event)
        result = projection.items[item]
        assert result.status == "cancelled"
        assert {channel.name: channel.text for channel in result.channels} == (
            {"content": content, "analysis": analysis} if include_analysis else {"content": content}
        )
    # Retain strict declared-channel validation: replay of the old payload still fails.
    strict = SessionProjection(child.session_id, include_analysis=True)
    for row in events:
        if row["id"] == completed[0]["id"]:
            break
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
    saved = run(
        lambda: rows(
            "SELECT convert_from(b.content,'UTF8')::jsonb AS payload FROM {t}timeline_item i "
            "JOIN {t}context_blob b ON b.scope_id=i.scope_id AND b.blob_key=i.content_ref "
            "WHERE i.id=:id",
            id=item,
        )
    )[0]["payload"]
    assert saved == {"runId": str(child.run_id), "text": content}
    terminal = next(row for row in events if row["kind"] == "run.cancelled")
    assert len(terminal["payload"]["partialResultRefs"]) == int(bool(content))
    assert run(
        lambda: rows("SELECT usage_status FROM {t}model_attempt WHERE id=:id", id=attempt)
    ) == [{"usage_status": "unknown"}]

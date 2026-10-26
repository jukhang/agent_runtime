"""R05 recovery transactions on random schemas in aletai_wangxh only."""

import asyncio
import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa
from test_storage_uow import (
    _seed,
    _seed_attempt,
    _seed_event,
    _seed_execution,
    _seed_item,
    actor,
    engine_for,
    lease,
    migrate,
    run,
)

from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.ownership import ChildCommitGuard, ChildLeaseToken
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.state_events import ChildEventAuthority
from agent_runtime.storage import child_recovery
from agent_runtime.storage.child_authority import require_child
from agent_runtime.storage.child_recovery import seal_child_output
from agent_runtime.storage.journal import SqlJournal
from agent_runtime.storage.recovery import SqlRecovery

pytestmark = pytest.mark.skipif(
    not os.environ.get("AGENT_RUNTIME_TEST_DSN"), reason="requires guarded real PostgreSQL"
)


@pytest.fixture(scope="module")
def recovery_schema():
    assert sa.make_url(os.environ["AGENT_RUNTIME_TEST_DSN"]).database == "aletai_wangxh"
    schema = f"r05_recovery_{uuid4().hex[:12]}"
    assert migrate("upgrade", schema).status == "applied"
    try:
        yield schema
    finally:

        async def drop():
            engine = engine_for(schema, search_path=False)
            try:
                async with engine.begin() as db:
                    await db.execute(sa.text(f'DROP SCHEMA "{schema}" CASCADE'))
            finally:
                await engine.dispose()

        run(drop)


@pytest.fixture
def pair(recovery_schema):
    schema = recovery_schema

    async def reset():
        engine = engine_for(schema, search_path=False)
        try:
            async with engine.begin() as db:
                tables = (
                    (
                        await db.execute(
                            sa.text(
                                "SELECT tablename FROM pg_tables WHERE schemaname=:schema "
                                "AND tablename<>'alembic_version' ORDER BY tablename"
                            ),
                            {"schema": schema},
                        )
                    )
                    .scalars()
                    .all()
                )
                names = ",".join(f'"{schema}"."{name}"' for name in tables)
                await db.execute(sa.text(f"TRUNCATE {names} CASCADE"))
        finally:
            await engine.dispose()

    run(reset)
    parent = run(lambda: _seed(schema, suffix="parent"))
    child = run(lambda: _seed(schema, suffix="child"))
    child = replace(child, session_id=parent.session_id, turn_id=parent.turn_id)
    engine = engine_for(schema, search_path=False)
    now = datetime.now(UTC)

    async def execute(sql, **params):
        async with engine.begin() as db:
            await db.execute(sa.text(sql.replace("{t}", f'"{schema}".')), params)

    async def rows(sql, **params):
        async with engine.connect() as db:
            return [
                dict(row)
                for row in (
                    await db.execute(sa.text(sql.replace("{t}", f'"{schema}".')), params)
                ).mappings()
            ]

    spec = {
        "kind": "message",
        "subjectId": str(parent.run_id),
        "deadlineAt": (now + timedelta(hours=1)).isoformat(),
        "resumePredicate": {"key": {"namespace": "builtin", "key": "message"}, "revision": "1"},
    }
    run(
        lambda: execute(
            "UPDATE {t}run SET status='waiting_user',phase='waiting',owner_id=NULL,"
            "lease_until=NULL,"
            "waiting_spec=CAST(:spec AS jsonb) WHERE id=:id",
            spec=json.dumps(spec),
            id=parent.run_id,
        )
    )
    run(
        lambda: execute(
            "UPDATE {t}work_item SET status='completed',owner_id=NULL,lease_until=NULL "
            "WHERE id=:id",
            id=parent.work_id,
        )
    )
    run(
        lambda: execute(
            "UPDATE {t}run SET parent_run_id=:parent,root_run_id=:parent,"
            "session_id=:session,turn_id=:turn,lease_until=:expired WHERE id=:id",
            parent=parent.run_id,
            session=parent.session_id,
            turn=parent.turn_id,
            expired=now - timedelta(seconds=1),
            id=child.run_id,
        )
    )
    run(
        lambda: execute(
            "UPDATE {t}work_item SET lease_until=:expired,attempt_count=5 WHERE id=:id",
            expired=now - timedelta(seconds=1),
            id=child.work_id,
        )
    )
    run(
        lambda: execute(
            "UPDATE {t}checkpoint SET state_version=1 WHERE id IN (:parent,:child)",
            parent=parent.checkpoint_id,
            child=child.checkpoint_id,
        )
    )
    run(
        lambda: execute(
            "UPDATE {t}turn SET primary_run_id=:run WHERE id=:id",
            run=parent.run_id,
            id=parent.turn_id,
        )
    )
    try:
        yield parent, child, engine, now, execute, rows
    finally:
        run(engine.dispose)


@pytest.mark.parametrize("source", ["leased", "pending", "worker"])
def test_child_failure_atomically_notifies_and_wakes_parent_once(pair, source):
    parent, child, engine, now, execute, rows = pair
    if source == "pending":
        run(
            lambda: execute(
                "UPDATE {t}work_item SET status='pending',owner_id=NULL,"
                "lease_until=NULL,deadline_at=:now WHERE id=:id",
                now=now,
                id=child.work_id,
            )
        )
        run(
            lambda: execute(
                "UPDATE {t}run SET status='queued',owner_id=NULL,lease_until=NULL WHERE id=:id",
                id=child.run_id,
            )
        )
    elif source == "worker":
        run(
            lambda: execute(
                "UPDATE {t}work_item SET lease_until=:future WHERE id=:id",
                future=now + timedelta(hours=1),
                id=child.work_id,
            )
        )
        run(
            lambda: execute(
                "UPDATE {t}run SET lease_until=:future WHERE id=:id",
                future=now + timedelta(hours=1),
                id=child.run_id,
            )
        )
    recovery = SqlRecovery(engine, parent.schema, clock=lambda: now)

    async def recover():
        if source == "worker":
            token = ChildLeaseToken.model_validate(
                lease(child).model_dump(exclude={"slot_version"})
            )
            return await recovery._settle_child_failure(
                token,
                FailureEnvelope(
                    code="INTERNAL", category="internal", retryable=False, public_message="Failed."
                ),
            )
        return await recovery.recover_expired_leases(parent.scope)

    async def compete():
        return await asyncio.gather(recover(), recover())

    run(compete)
    c = run(lambda: rows("SELECT * FROM {t}run WHERE id=:id", id=child.run_id))[0]
    p = run(lambda: rows("SELECT * FROM {t}run WHERE id=:id", id=parent.run_id))[0]
    assert (c["status"], c["state_version"]) == ("failed", 2)
    assert (p["status"], p["state_version"], p["pending_input_version"], p["wake_generation"]) == (
        "queued",
        2,
        1,
        1,
    )
    assert c["checkpoint_id"] != child.checkpoint_id and p["checkpoint_id"] != parent.checkpoint_id
    notices = run(lambda: rows("SELECT * FROM {t}agent_message WHERE kind='task.failed'"))
    assert len(notices) == 1 and notices[0]["status"] == "pending"
    assert (
        run(
            lambda: rows(
                "SELECT allocated_seq FROM {t}mailbox_cursor WHERE recipient_run_id=:id",
                id=parent.run_id,
            )
        )[0]["allocated_seq"]
        == 1
    )
    assert (
        len(
            run(
                lambda: rows(
                    "SELECT id FROM {t}event WHERE run_id=:id AND kind='run.failed'",
                    id=child.run_id,
                )
            )
        )
        == 1
    )
    assert len(run(lambda: rows("SELECT id FROM {t}outbox"))) == 3
    assert (
        len(
            run(
                lambda: rows(
                    "SELECT id FROM {t}work_item WHERE run_id=:id AND status='pending'",
                    id=parent.run_id,
                )
            )
        )
        == 1
    )
    assert not run(
        lambda: rows(
            "SELECT id FROM {t}work_item WHERE run_id=:id AND status IN ('pending','leased')",
            id=child.run_id,
        )
    )


def test_child_recovery_rolls_back_events_checkpoint_notice_and_wake(pair, monkeypatch):
    parent, child, engine, now, execute, rows = pair
    original = SqlJournal.append_state_in_transaction

    async def fail_wake(self, db, batch):
        result = await original(self, db, batch)
        if batch.events[0].run_id == parent.run_id:
            raise RuntimeError("injected after parent wake event")
        return result

    monkeypatch.setattr(SqlJournal, "append_state_in_transaction", fail_wake)
    recovery = SqlRecovery(engine, parent.schema, clock=lambda: now)
    with pytest.raises(RuntimeError, match="injected"):
        run(lambda: recovery.recover_expired_leases(parent.scope))
    assert not run(lambda: rows("SELECT id FROM {t}event"))
    assert not run(lambda: rows("SELECT id FROM {t}outbox"))
    assert not run(lambda: rows("SELECT id FROM {t}agent_message"))
    assert run(
        lambda: rows(
            "SELECT status,state_version,checkpoint_id FROM {t}run WHERE id=:id", id=child.run_id
        )
    )[0] == {"status": "running", "state_version": 1, "checkpoint_id": child.checkpoint_id}
    assert (
        run(lambda: rows("SELECT status FROM {t}work_item WHERE id=:id", id=child.work_id))[0][
            "status"
        ]
        == "leased"
    )
    monkeypatch.setattr(SqlJournal, "append_state_in_transaction", original)
    run(lambda: recovery.recover_expired_leases(parent.scope))
    assert len(run(lambda: rows("SELECT id FROM {t}agent_message"))) == 1


@pytest.mark.parametrize("source", ["worker", "leased", "pending"])
def test_child_cancel_keeps_claimable_work_after_error_or_expiry(pair, source):
    parent, child, engine, now, execute, rows = pair
    command = uuid4()
    run(
        lambda: execute(
            "INSERT INTO {t}command (id,scope_id,run_id,actor_id,request_key,request_hash,kind,"
            "status,expected_state_version) VALUES (:id,:scope,:run,:actor,:key,:hash,'cancel',"
            "'accepted',1)",
            id=command,
            scope=child.scope,
            run=child.run_id,
            actor=child.actor_id,
            key=f"cancel:{command}",
            hash="a" * 64,
        )
    )
    run(
        lambda: execute(
            "UPDATE {t}run SET status='cancelling',desired_control='cancel',"
            "control_command_id=:command WHERE id=:id",
            command=command,
            id=child.run_id,
        )
    )
    if source == "worker":
        run(
            lambda: execute(
                "UPDATE {t}run SET lease_until=:future WHERE id=:id",
                future=now + timedelta(hours=1),
                id=child.run_id,
            )
        )
        run(
            lambda: execute(
                "UPDATE {t}work_item SET lease_until=:future WHERE id=:id",
                future=now + timedelta(hours=1),
                id=child.work_id,
            )
        )
    elif source == "pending":
        run(
            lambda: execute(
                "UPDATE {t}run SET owner_id=NULL,lease_until=NULL WHERE id=:id", id=child.run_id
            )
        )
        run(
            lambda: execute(
                "UPDATE {t}work_item SET status='pending',owner_id=NULL,lease_until=NULL,"
                "deadline_at=:now WHERE id=:id",
                now=now,
                id=child.work_id,
            )
        )
    recovery = SqlRecovery(engine, parent.schema, clock=lambda: now)
    if source == "worker":
        token = ChildLeaseToken.model_validate(lease(child).model_dump(exclude={"slot_version"}))
        failure = FailureEnvelope(
            code="INTERNAL", category="internal", retryable=False, public_message="Control failed."
        )
        assert run(lambda: recovery._settle_child_failure(token, failure)).disposition == "requeued"
        assert run(lambda: recovery._settle_child_failure(token, failure)).disposition == "fenced"
    else:
        run(lambda: recovery.recover_expired_leases(parent.scope))
    work = run(lambda: rows("SELECT * FROM {t}work_item WHERE id=:id", id=child.work_id))[0]
    assert work["status"] == "pending" and work["owner_id"] is None
    assert work["available_at"] < work["deadline_at"] and work["deadline_at"] > now
    c = run(
        lambda: rows(
            "SELECT status,desired_control,owner_id FROM {t}run WHERE id=:id", id=child.run_id
        )
    )[0]
    assert c == {"status": "cancelling", "desired_control": "cancel", "owner_id": None}
    assert not run(lambda: rows("SELECT id FROM {t}agent_message"))


def test_unknown_effect_cannot_replay_and_unknown_model_usage_is_retained(pair):
    parent, child, engine, now, execute, rows = pair
    execution = run(lambda: _seed_execution(child, status="unknown"))
    attempt = run(lambda: _seed_attempt(child))
    run(
        lambda: execute(
            "UPDATE {t}model_attempt SET status='running',finished_at=NULL WHERE id=:id", id=attempt
        )
    )
    allocation = uuid4()
    run(
        lambda: execute(
            "INSERT INTO {t}budget_allocation "
            "(id,scope_id,root_run_id,limits,reserved,unknown_reserved,settled,"
            "usage_status,unsettled_count,version) "
            "VALUES (:id,:scope,:root,'{}','{}',CAST(:unknown AS jsonb),'{}','unknown',1,1)",
            id=allocation,
            scope=parent.scope,
            root=parent.run_id,
            unknown=json.dumps({"tokens": 123}),
        )
    )
    recovery = SqlRecovery(engine, parent.schema, clock=lambda: now)
    run(lambda: recovery.recover_expired_leases(parent.scope))
    work = run(lambda: rows("SELECT * FROM {t}work_item WHERE id=:id", id=child.work_id))[0]
    assert work["status"] == "pending" and work["available_at"] == work["deadline_at"]
    assert not run(lambda: rows("SELECT id FROM {t}agent_message"))
    now = work["deadline_at"] + timedelta(seconds=1)
    run(lambda: recovery.recover_expired_leases(parent.scope))
    assert not run(lambda: rows("SELECT id FROM {t}agent_message"))
    run(lambda: execute("UPDATE {t}execution SET status='failed' WHERE id=:id", id=execution))
    now += timedelta(seconds=3)
    run(lambda: recovery.recover_expired_leases(parent.scope))
    assert len(run(lambda: rows("SELECT id FROM {t}agent_message"))) == 1
    usage = run(
        lambda: rows("SELECT status,usage_status FROM {t}model_attempt WHERE id=:id", id=attempt)
    )[0]
    assert usage == {"status": "interrupted", "usage_status": "unknown"}
    budget = run(
        lambda: rows(
            "SELECT unknown_reserved,usage_status FROM {t}budget_allocation WHERE id=:id",
            id=allocation,
        )
    )[0]
    assert budget == {"unknown_reserved": {"tokens": 123}, "usage_status": "unknown"}
    assert not run(lambda: rows("SELECT id FROM {t}budget_entry WHERE kind='settle'"))


@pytest.mark.parametrize("deadline", ["run", "wait"])
def test_parent_message_wait_expiry_finishes_after_child_and_is_idempotent(pair, deadline):
    parent, child, engine, now, execute, rows = pair
    if deadline == "run":
        run(
            lambda: execute(
                "UPDATE {t}run SET deadline_at=:now WHERE id=:id", now=now, id=parent.run_id
            )
        )
    else:
        run(
            lambda: execute(
                "UPDATE {t}run SET waiting_spec=jsonb_set(waiting_spec,"
                "'{deadlineAt}',to_jsonb(CAST(:now AS text))) WHERE id=:id",
                now=now.isoformat(),
                id=parent.run_id,
            )
        )
    recovery = SqlRecovery(engine, parent.schema, clock=lambda: now)
    assert (
        run(lambda: recovery.expire_message_waits(parent.scope)) == 0
    )  # live child blocks terminal
    run(lambda: recovery.reconcile_scope(parent.scope))
    p = run(
        lambda: rows(
            "SELECT status,failure,waiting_spec FROM {t}run WHERE id=:id", id=parent.run_id
        )
    )[0]
    assert p["status"] == "failed" and p["failure"]["code"] == "DELIVERY_EXPIRED"
    assert p["waiting_spec"] is None
    assert (
        run(lambda: rows("SELECT outcome_run_id FROM {t}turn WHERE id=:id", id=parent.turn_id))[0][
            "outcome_run_id"
        ]
        == parent.run_id
    )
    assert (
        run(
            lambda: rows(
                "SELECT owner_run_id FROM {t}session_slot WHERE session_id=:id",
                id=parent.session_id,
            )
        )[0]["owner_run_id"]
        is None
    )
    assert not run(
        lambda: rows(
            "SELECT id FROM {t}work_item WHERE run_id IN (:parent,:child) "
            "AND status IN ('pending','leased')",
            parent=parent.run_id,
            child=child.run_id,
        )
    )
    baseline = len(run(lambda: rows("SELECT id FROM {t}event")))
    run(lambda: recovery.reconcile_scope(parent.scope))
    assert len(run(lambda: rows("SELECT id FROM {t}event"))) == baseline


@pytest.mark.parametrize("status", ["queued", "waiting_tool", "waiting_user", "paused"])
def test_service_cancels_unowned_children_of_cancelled_parent(pair, status):
    parent, child, engine, now, execute, rows = pair
    run(
        lambda: execute(
            "UPDATE {t}run SET status='cancelled',phase='terminal',waiting_spec=NULL WHERE id=:id",
            id=parent.run_id,
        )
    )
    run(
        lambda: execute(
            "UPDATE {t}run SET status=:status,owner_id=NULL,lease_until=NULL WHERE id=:id",
            status=status,
            id=child.run_id,
        )
    )
    run(
        lambda: execute(
            "UPDATE {t}work_item SET status='pending',owner_id=NULL,lease_until=NULL WHERE id=:id",
            id=child.work_id,
        )
    )
    recovery = SqlRecovery(engine, parent.schema, clock=lambda: now)

    async def compete():
        return await asyncio.gather(
            recovery.recover_cancelled_children(parent.scope),
            recovery.recover_cancelled_children(parent.scope),
        )

    assert sum(run(compete)) == 1
    child_row = run(
        lambda: rows(
            "SELECT status,state_version,desired_control FROM {t}run WHERE id=:id", id=child.run_id
        )
    )[0]
    assert child_row == {"status": "cancelled", "state_version": 3, "desired_control": None}
    notices = run(lambda: rows("SELECT status FROM {t}agent_message WHERE kind='task.failed'"))
    assert notices == [{"status": "expired"}]
    assert (
        run(
            lambda: rows("SELECT pending_input_version FROM {t}run WHERE id=:id", id=parent.run_id)
        )[0]["pending_input_version"]
        == 1
    )
    assert run(lambda: rows("SELECT status FROM {t}command WHERE run_id=:id", id=child.run_id)) == [
        {"status": "applied"}
    ]
    assert not run(
        lambda: rows(
            "SELECT id FROM {t}work_item WHERE run_id=:id AND status IN ('pending','leased')",
            id=child.run_id,
        )
    )
    assert (
        len(
            run(
                lambda: rows(
                    "SELECT id FROM {t}event WHERE run_id=:id AND kind='run.cancelled'",
                    id=child.run_id,
                )
            )
        )
        == 1
    )


def test_parent_cancel_marks_live_child_without_stealing_lease_then_converges(pair):
    parent, child, engine, now, execute, rows = pair
    run(
        lambda: execute(
            "UPDATE {t}run SET status='cancelled',waiting_spec=NULL WHERE id=:id", id=parent.run_id
        )
    )
    future = now + timedelta(minutes=5)
    run(
        lambda: execute(
            "UPDATE {t}run SET lease_until=:future WHERE id=:id", future=future, id=child.run_id
        )
    )
    run(
        lambda: execute(
            "UPDATE {t}work_item SET lease_until=:future WHERE id=:id",
            future=future,
            id=child.work_id,
        )
    )
    attempt = run(lambda: _seed_attempt(child))
    run(
        lambda: execute(
            "UPDATE {t}model_attempt SET status='running',finished_at=NULL WHERE id=:id", id=attempt
        )
    )
    recovery = SqlRecovery(engine, parent.schema, clock=lambda: now)
    assert run(lambda: recovery.recover_cancelled_children(parent.scope)) == 0
    c = run(
        lambda: rows(
            "SELECT status,desired_control,owner_epoch,owner_id,lease_until "
            "FROM {t}run WHERE id=:id",
            id=child.run_id,
        )
    )[0]
    assert (
        c["status"],
        c["desired_control"],
        c["owner_epoch"],
        c["owner_id"],
        c["lease_until"],
    ) == ("cancelling", "cancel", 1, child.owner, future)
    assert run(lambda: rows("SELECT status FROM {t}model_attempt WHERE id=:id", id=attempt)) == [
        {"status": "running"}
    ]
    assert not run(lambda: rows("SELECT id FROM {t}agent_message"))
    now = future + timedelta(seconds=1)
    assert run(lambda: recovery.recover_cancelled_children(parent.scope)) == 1
    assert run(
        lambda: rows("SELECT status,usage_status FROM {t}model_attempt WHERE id=:id", id=attempt)
    ) == [{"status": "interrupted", "usage_status": "unknown"}]


def test_parent_cancel_keeps_unknown_external_supervision_until_stopped(pair):
    parent, child, engine, now, execute, rows = pair
    run(
        lambda: execute(
            "UPDATE {t}run SET status='cancelled',waiting_spec=NULL WHERE id=:id", id=parent.run_id
        )
    )
    execution = run(lambda: _seed_execution(child, status="unknown"))
    recovery = SqlRecovery(engine, parent.schema, clock=lambda: now)
    run(lambda: recovery.reconcile_scope(parent.scope))
    assert run(
        lambda: rows("SELECT status,desired_control FROM {t}run WHERE id=:id", id=child.run_id)
    ) == [{"status": "cancelling", "desired_control": "cancel"}]
    supervision = run(
        lambda: rows(
            "SELECT id,status FROM {t}work_item WHERE run_id=:id AND kind='reconcile_execution'",
            id=child.run_id,
        )
    )
    assert len(supervision) == 1 and supervision[0]["status"] == "pending"
    assert not run(lambda: rows("SELECT id FROM {t}agent_message"))
    run(
        lambda: execute(
            "UPDATE {t}execution SET status='cancelled',cleanup_status='completed' WHERE id=:id",
            id=execution,
        )
    )
    run(
        lambda: execute(
            "UPDATE {t}work_item SET status='completed' WHERE id=:id", id=supervision[0]["id"]
        )
    )
    assert run(lambda: recovery.recover_cancelled_children(parent.scope)) == 1
    assert len(run(lambda: rows("SELECT id FROM {t}agent_message"))) == 1


def test_cancelled_child_seals_only_durable_prefix_and_rolls_back_notice(pair, monkeypatch):
    parent, child, engine, now, execute, rows = pair
    run(
        lambda: execute(
            "UPDATE {t}run SET status='cancelled',waiting_spec=NULL WHERE id=:id", id=parent.run_id
        )
    )
    item = run(lambda: _seed_item(child))
    attempt = run(lambda: _seed_attempt(child))
    run(lambda: _seed_event(child, sequence=1, item_id=item, attempt_id=attempt))
    run(
        lambda: execute(
            "UPDATE {t}event SET payload=CAST(:payload AS jsonb) WHERE item_id=:id",
            payload=json.dumps(
                {
                    "channel": "content",
                    "deltaIndex": "0",
                    "itemRevision": "1",
                    "text": "durable prefix",
                }
            ),
            id=item,
        )
    )
    run(lambda: execute("UPDATE {t}session SET event_seq=1 WHERE id=:id", id=parent.session_id))
    run(lambda: execute("UPDATE {t}timeline_item SET status='streaming' WHERE id=:id", id=item))
    original = child_recovery.message

    async def fail_message(*args, **kwargs):
        await original(*args, **kwargs)
        raise RuntimeError("injected after task.failed")

    monkeypatch.setattr(child_recovery, "message", fail_message)
    recovery = SqlRecovery(engine, parent.schema, clock=lambda: now)
    with pytest.raises(RuntimeError, match="injected"):
        run(lambda: recovery.recover_cancelled_children(parent.scope))
    assert not run(lambda: rows("SELECT id FROM {t}output_seal"))
    assert not run(lambda: rows("SELECT id FROM {t}command"))
    assert not run(lambda: rows("SELECT id FROM {t}agent_message"))
    assert run(
        lambda: rows("SELECT state_version,status FROM {t}run WHERE id=:id", id=child.run_id)
    ) == [{"state_version": 1, "status": "running"}]
    monkeypatch.setattr(child_recovery, "message", original)
    assert run(lambda: recovery.recover_cancelled_children(parent.scope)) == 1
    assert len(run(lambda: rows("SELECT id FROM {t}output_seal"))) == 1
    assert run(lambda: rows("SELECT status FROM {t}timeline_item WHERE id=:id", id=item)) == [
        {"status": "cancelled"}
    ]
    stored = run(
        lambda: rows(
            "SELECT convert_from(b.content,'UTF8')::jsonb AS payload "
            "FROM {t}timeline_item i JOIN {t}context_blob b "
            "ON b.scope_id=i.scope_id AND b.blob_key=i.content_ref WHERE i.id=:id",
            id=item,
        )
    )[0]["payload"]
    assert stored["text"] == "durable prefix"
    assert run(lambda: recovery.recover_cancelled_children(parent.scope)) == 0


@pytest.mark.parametrize("status", ["failed", "cancelled"])
def test_shared_child_output_sealer_accepts_worker_authority_without_duplicate_terminal(
    pair, status
):
    parent, child, engine, now, execute, rows = pair
    item = run(lambda: _seed_item(child))
    attempt = run(lambda: _seed_attempt(child))
    run(lambda: _seed_event(child, sequence=1, item_id=item, attempt_id=attempt))
    run(
        lambda: execute(
            "UPDATE {t}event SET payload=CAST(:payload AS jsonb) WHERE item_id=:id",
            payload=json.dumps(
                {
                    "channel": "content",
                    "deltaIndex": "0",
                    "itemRevision": "1",
                    "text": "stopped prefix",
                }
            ),
            id=item,
        )
    )
    run(lambda: execute("UPDATE {t}session SET event_seq=1 WHERE id=:id", id=parent.session_id))
    run(lambda: execute("UPDATE {t}timeline_item SET status='streaming' WHERE id=:id", id=item))
    run(
        lambda: execute(
            "UPDATE {t}run SET lease_until=:future WHERE id=:id",
            future=now + timedelta(hours=1),
            id=child.run_id,
        )
    )
    run(
        lambda: execute(
            "UPDATE {t}work_item SET lease_until=:future WHERE id=:id",
            future=now + timedelta(hours=1),
            id=child.work_id,
        )
    )
    principal = actor(world=child)
    token = ChildLeaseToken.model_validate(lease(child).model_dump(exclude={"slot_version"}))
    guard = ChildCommitGuard(
        actor=principal,
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

    async def seal():
        from sqlalchemy.ext.asyncio import async_sessionmaker

        async with async_sessionmaker(engine)() as db, db.begin():
            current = await require_child(db, child.schema, guard, now=now, allow_control=True)
            await db.execute(
                sa.text(f'SELECT id FROM "{child.schema}".session WHERE id=:id FOR UPDATE'),
                {"id": parent.session_id},
            )
            return await seal_child_output(
                db,
                child.schema,
                SqlJournal(engine, child.schema, principal),
                ChildEventAuthority(kind="child", guard=guard),
                current,
                now,
                status=status,
            )

    refs = run(seal)
    assert len(refs) == 1 and refs[0].resource_id == str(item)
    assert run(lambda: rows("SELECT status FROM {t}timeline_item WHERE id=:id", id=item)) == [
        {"status": status}
    ]
    assert len(run(lambda: rows("SELECT id FROM {t}output_seal"))) == 1
    assert not run(
        lambda: rows("SELECT id FROM {t}event WHERE kind IN ('run.failed','run.cancelled')")
    )
    assert run(lambda: rows("SELECT status FROM {t}run WHERE id=:id", id=child.run_id)) == [
        {"status": "running"}
    ]
    assert run(seal) == ()

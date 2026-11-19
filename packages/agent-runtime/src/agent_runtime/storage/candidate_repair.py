"""Lease-fenced correction counts in immutable checkpoint representations."""

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from agent_runtime.agent_loop import DecisionFrame
from agent_runtime.candidate_repair import CandidateObservation, CandidateRejected
from agent_runtime.context_payloads import context_blob_key
from agent_runtime.contracts.canonical import canonical_bytes, content_digest
from agent_runtime.contracts.decisions import Finish
from agent_runtime.contracts.failure import AgentRuntimeError, runtime_failure
from agent_runtime.contracts.loop import RunState
from agent_runtime.contracts.results import Usage
from agent_runtime.contracts.tools import ToolFailed, ToolNotExecuted, tool_outcome_adapter
from agent_runtime.contracts.transactions import ActorContext, LeaseToken
from agent_runtime.storage.context_blobs import SqlContextBlobBackend
from agent_runtime.storage.tool_recovery_evidence import invalid_evidence, verify_tool_recoveries
from agent_runtime.storage.uow import errors, facts, locks
from agent_runtime.storage.uow.authority import revalidate_authority
from agent_runtime.storage.uow.schema import require_schema, scoped


class SqlCandidateRepairs:
    def __init__(
        self,
        engine: AsyncEngine,
        schema: str,
        *,
        actor: ActorContext,
        lease: Callable[[], LeaseToken],
    ) -> None:
        self._schema = require_schema(schema)
        self._actor, self._lease = actor, lease
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._blobs = SqlContextBlobBackend(engine, schema)
        self._locks = locks.build_lock_statements(schema)

    async def validate(self, state: RunState, frame: DecisionFrame) -> None:
        if not isinstance(frame.action, Finish):
            return
        if frame.actor != self._actor or (
            frame.context.scope_id,
            frame.context.session_id,
            frame.context.run_id,
        ) != (state.scope_id, state.session_id, state.run_id):
            errors.forbidden("completion candidate belongs to another actor or Run")
        assessments = frame.action.payload.recovered_tool_failures
        visible = {
            s.resource.resource_id
            for s in frame.context.sources
            if s.resource.resource_type == "tool_call_result"
            and s.disposition in {"included", "summarized"}
        }
        failures: set[UUID] = set()
        try:
            if any(
                str(identity) not in visible
                for a in assessments
                for identity in (a.failed_tool_call_id, *a.recovery_tool_call_ids)
            ):
                invalid_evidence(
                    "Only current Run tool evidence delivered in this context can be cited."
                )
            async with self._sessions() as db, db.begin():
                failures = set(
                    (
                        await db.execute(
                            scoped(
                                "SELECT id FROM {t}tool_call WHERE scope_id=:scope AND run_id=:run "
                                "AND status IN ('failed','cancelled','not_executed')",
                                self._schema,
                            ),
                            {"scope": state.scope_id, "run": state.run_id},
                        )
                    ).scalars()
                )
                complete = await verify_tool_recoveries(
                    db,
                    self._schema,
                    scope_id=state.scope_id,
                    run_id=state.run_id,
                    failures=failures,
                    assessments=assessments,
                )
                if frame.action.payload.outcome == "success" and not complete:
                    covered = {a.failed_tool_call_id for a in assessments}
                    missing_count = len(failures - covered)
                    invalid_evidence(
                        "Success requires one recovery assessment for EVERY unsuccessful call. "
                        f"Missing assessments: {missing_count}."
                    )
        except AgentRuntimeError as exc:
            if exc.failure.code != "INVALID_EVIDENCE":
                raise
            raise CandidateRejected(
                attempt_id=frame.attempt_id,
                snapshot_id=frame.context.snapshot_id,
                reason="invalid_evidence",
                detail=recovery_feedback(exc.failure.public_message, failures, visible),
            ) from exc

    async def _history(self, db: AsyncSession, state: RunState) -> dict[str, Any] | None:
        rows = (
            (
                await db.execute(
                    scoped(
                        "WITH RECURSIVE chain AS (SELECT c.*,0 AS depth FROM {t}checkpoint c "
                        "JOIN {t}run r ON r.scope_id=c.scope_id AND r.checkpoint_id=c.id "
                        "WHERE r.scope_id=:scope AND r.id=:run UNION ALL "
                        "SELECT p.*,c.depth+1 FROM chain c JOIN {t}checkpoint p "
                        "ON p.scope_id=c.scope_id AND p.id=c.source_checkpoint_id "
                        "AND p.run_id=:run WHERE c.depth<1024) "
                        "SELECT c.state_ref,c.content_hash,c.depth FROM chain c "
                        "ORDER BY c.depth",
                        self._schema,
                    ),
                    {"scope": state.scope_id, "run": state.run_id},
                )
            )
            .mappings()
            .all()
        )
        if not rows:
            return None
        import json

        history = None
        for row in rows:
            # Old checkpoints can reference host state, not a Runtime context Blob.
            if not row["state_ref"].startswith("context-"):
                continue
            try:
                raw = await self._blobs.get_in_transaction(db, state.scope_id, row["state_ref"])
                payload = json.loads(raw)
                if content_digest(payload) != row["content_hash"]:
                    raise ValueError("checkpoint digest differs")
                history = payload.get("candidateRepair")
            except (ValueError, TypeError, AgentRuntimeError) as exc:
                raise runtime_failure(
                    "AUDIT_UNAVAILABLE",
                    category="storage",
                    public_message="Correction history is unavailable or corrupt.",
                ) from exc
            if history is not None:
                break
        if history is None:
            if rows[-1]["depth"] == 1024:
                raise runtime_failure(
                    "AUDIT_UNAVAILABLE",
                    category="storage",
                    public_message="Correction history exceeds its verified traversal bound.",
                )
            return None
        if (
            not isinstance(history, dict)
            or history.get("runId") != str(state.run_id)
            or type(history.get("total")) is not int
            or not 1 <= history["total"] <= 4
            or not isinstance(history.get("counts"), dict)
            or any(type(n) is not int or not 1 <= n <= 2 for n in history["counts"].values())
            or sum(history["counts"].values()) != history["total"]
            or not isinstance(history.get("seen"), dict)
            or len(history["seen"]) != history["total"]
            or any(
                not isinstance(key, str) or value not in history["counts"]
                for key, value in history["seen"].items()
            )
            or any(
                sum(value == fingerprint for value in history["seen"].values()) != count
                for fingerprint, count in history["counts"].items()
            )
        ):
            errors.version_conflict("candidate repair counters are invalid")
        if "observation" in history:
            observation = CandidateObservation.model_validate(history["observation"])
            if (
                str(observation.run_id) != history["runId"]
                or str(observation.attempt_id) != history["attemptId"]
                or str(observation.snapshot_id) != history["snapshotId"]
                or observation.fingerprint != history["fingerprint"]
                or observation.total != history["total"]
                or observation.fingerprint_count != history["counts"][observation.fingerprint]
            ):
                errors.version_conflict("candidate rejection observation differs from counters")
        return history

    async def latest(self, state: RunState) -> CandidateRejected | None:
        if state.scope_id != self._actor.scope_id:
            errors.forbidden("candidate correction scope differs")
        lease = self._lease()
        if (state.scope_id, state.session_id, state.run_id) != (
            lease.scope_id,
            lease.session_id,
            lease.run_id,
        ):
            errors.forbidden("candidate feedback belongs to another claim")
        async with self._sessions() as db, db.begin():
            now = datetime.now(UTC)
            facts.verify_lease(lease, now=now)
            facts.verify_slot(
                await locks.lock_slot(db, self._locks, state.scope_id, state.session_id), lease
            )
            run = facts.verify_run(
                await locks.lock_run(db, self._locks, state.scope_id, state.run_id),
                self._actor,
                lease,
                now=now,
            )
            await revalidate_authority(db, self._locks, self._actor, run=run, now=now)
            work = facts.verify_work(
                await locks.lock_work_item(db, self._locks, state.scope_id, lease.work_id),
                lease,
                now=now,
            )
            now = datetime.now(UTC)
            facts.verify_lease(lease, now=now)
            facts.verify_run(run, self._actor, lease, now=now)
            facts.verify_work(work, lease, now=now)
            await revalidate_authority(db, self._locks, self._actor, run=run, now=now)
            if run["parent_run_id"] is not None or work["kind"] != "advance_run":
                errors.forbidden("candidate correction requires a main claim")
            if run["desired_control"] is not None or run["status"] != "running":
                errors.control_pending("candidate feedback")
            if (
                int(state.state_version) != run["state_version"]
                or int(state.pending_input_seq) != run["pending_input_version"]
                or int(state.applied_input_seq) != run["applied_input_version"]
                or int(state.cancel_version) != run["cancel_version"]
            ):
                errors.version_conflict("candidate feedback state changed")
            history = await self._history(db, state)
            if history is None:
                return None
            latest = (
                await db.execute(
                    scoped(
                        "SELECT id FROM {t}model_attempt WHERE scope_id=:scope AND run_id=:run "
                        "ORDER BY started_at DESC NULLS LAST,id DESC LIMIT 1",
                        self._schema,
                    ),
                    {"scope": state.scope_id, "run": state.run_id},
                )
            ).scalar_one_or_none()
            if str(latest) != history["attemptId"]:
                return None
            return CandidateRejected(
                attempt_id=UUID(history["attemptId"]),
                snapshot_id=UUID(history["snapshotId"]),
                reason=history["reason"],
                detail=history["detail"],
                tool_call_id=UUID(history["toolCallId"]) if history.get("toolCallId") else None,
            )

    async def record(self, state: RunState, rejection: CandidateRejected) -> RunState:
        import json

        lease, now = self._lease(), datetime.now(UTC)
        if lease.scope_id != state.scope_id or (state.scope_id, state.session_id, state.run_id) != (
            self._actor.scope_id,
            lease.session_id,
            lease.run_id,
        ):
            errors.forbidden("candidate correction belongs to another claim")
        async with self._sessions() as db, db.begin():
            facts.verify_lease(lease, now=now)
            facts.verify_slot(
                await locks.lock_slot(db, self._locks, state.scope_id, state.session_id), lease
            )
            run = facts.verify_run(
                await locks.lock_run(db, self._locks, state.scope_id, state.run_id),
                self._actor,
                lease,
                now=now,
            )
            await revalidate_authority(db, self._locks, self._actor, run=run, now=now)
            work = facts.verify_work(
                await locks.lock_work_item(db, self._locks, state.scope_id, lease.work_id),
                lease,
                now=now,
            )
            now = datetime.now(UTC)
            facts.verify_lease(lease, now=now)
            facts.verify_run(run, self._actor, lease, now=now)
            facts.verify_work(work, lease, now=now)
            await revalidate_authority(db, self._locks, self._actor, run=run, now=now)
            if run["parent_run_id"] is not None or work["kind"] != "advance_run":
                errors.forbidden("candidate correction requires a main claim")
            if run["status"] != "running" or run["desired_control"] is not None:
                errors.control_pending("candidate correction")
            if int(state.state_version) != run["state_version"]:
                errors.version_conflict("candidate correction state changed")
            if (
                int(state.pending_input_seq) != run["pending_input_version"]
                or int(state.applied_input_seq) != run["applied_input_version"]
                or int(state.cancel_version) != run["cancel_version"]
            ):
                errors.version_conflict("candidate correction input or control watermark changed")
            current = (
                (
                    await db.execute(
                        scoped(
                            "SELECT * FROM {t}checkpoint WHERE scope_id=:scope "
                            "AND id=:id AND run_id=:run",
                            self._schema,
                        ),
                        {"scope": state.scope_id, "id": run["checkpoint_id"], "run": state.run_id},
                    )
                )
                .mappings()
                .one()
            )
            history = await self._history(db, state)
            fingerprint = content_digest({"reason": rejection.reason, "detail": rejection.detail})
            key = f"{rejection.attempt_id}:{rejection.tool_call_id or 'candidate'}"
            if history and key in history.get("seen", {}):
                if history["seen"][key] != fingerprint:
                    errors.version_conflict("candidate correction attempt binding changed")
                return state.model_copy(update={"checkpoint_ref": current["state_ref"]})
            if current["state_ref"] != state.checkpoint_ref:
                errors.version_conflict("candidate correction checkpoint changed")
            source = (
                (
                    await db.execute(
                        scoped(
                            "SELECT m.status,m.context_snapshot_id,m.usage_status,m.usage,"
                            "m.request_key,m.input_ref,c.payload_ref,c.content_hash,c.auth_revision,"
                            "EXISTS(SELECT 1 FROM {t}tool_call t WHERE t.scope_id=m.scope_id "
                            "AND t.run_id=m.run_id AND t.attempt_id=m.id) AS has_calls "
                            "FROM {t}model_attempt m JOIN {t}context_snapshot c "
                            "ON c.scope_id=m.scope_id AND c.id=m.context_snapshot_id "
                            "WHERE m.scope_id=:scope AND m.id=:id AND m.run_id=:run "
                            "AND c.run_id=:run AND c.session_id=:session",
                            self._schema,
                        ),
                        {
                            "scope": state.scope_id,
                            "id": rejection.attempt_id,
                            "run": state.run_id,
                            "session": state.session_id,
                        },
                    )
                )
                .mappings()
                .one_or_none()
            )
            from agent_runtime.storage.model_attempts import claim_attempt_suffix

            if (
                source is None
                or source["status"] != "completed"
                or (source["has_calls"] and rejection.tool_call_id is None)
                or source["usage_status"] != "complete"
                or source["context_snapshot_id"] != rejection.snapshot_id
                or source["input_ref"] != source["payload_ref"]
                or str(source["auth_revision"]) != self._actor.auth_revision
                or not source["request_key"].endswith(claim_attempt_suffix(lease))
            ):
                errors.forbidden("candidate correction has no intact no-dispatch model evidence")
            try:
                usage = Usage.model_validate(source["usage"])
                if usage.status != "complete" or usage.reason != "provider_reported":
                    raise ValueError("incomplete usage")
                prepared_payload = json.loads(
                    await self._blobs.get_in_transaction(db, state.scope_id, source["payload_ref"])
                )
                if content_digest(prepared_payload) != source["content_hash"]:
                    raise ValueError("model input digest differs")
            except (ValueError, TypeError) as exc:
                raise runtime_failure(
                    "AUDIT_UNAVAILABLE",
                    category="storage",
                    public_message="Candidate correction requires intact model input and usage.",
                ) from exc
            effect: Literal["none", "rolled_back"] = "none"
            if rejection.tool_call_id is not None:
                result_ref = (
                    await db.execute(
                        scoped(
                            "SELECT result_ref FROM {t}tool_call WHERE scope_id=:scope "
                            "AND run_id=:run AND id=:call AND attempt_id=:attempt "
                            "AND status IN ('failed','not_executed')",
                            self._schema,
                        ),
                        {
                            "scope": state.scope_id,
                            "run": state.run_id,
                            "call": rejection.tool_call_id,
                            "attempt": rejection.attempt_id,
                        },
                    )
                ).scalar_one_or_none()
                if result_ref is None:
                    errors.forbidden("candidate correction has no terminal tool rejection")
                outcome = tool_outcome_adapter.validate_python(
                    json.loads(
                        await self._blobs.get_in_transaction(db, state.scope_id, result_ref)
                    ).get("outcome")
                )
                if (
                    not isinstance(outcome, (ToolFailed, ToolNotExecuted))
                    or outcome.effect not in {"none", "rolled_back"}
                    or outcome.failure.category != "validation"
                ):
                    errors.forbidden("tool failure cannot be repaired as a candidate defect")
                if outcome.tool_call_id != rejection.tool_call_id or outcome.run_id != state.run_id:
                    errors.forbidden("candidate rejection outcome identity differs")
                effect = outcome.effect
            counts = {} if history is None else dict(history["counts"])
            total = 0 if history is None else history["total"]
            if total >= 4 or counts.get(fingerprint, 0) >= 2:
                raise runtime_failure(
                    "INVALID_INPUT",
                    category="validation",
                    public_message="本轮操作候选在有限次数内仍未修正，未执行该操作。请明确目标或缩小范围后重试。",
                )
            counts[fingerprint] = counts.get(fingerprint, 0) + 1
            payload = json.loads(
                await self._blobs.get_in_transaction(db, state.scope_id, current["state_ref"])
            )
            if content_digest(payload) != current["content_hash"]:
                errors.version_conflict("candidate correction source checkpoint is corrupt")
            payload["candidateRepair"] = {
                "runId": str(state.run_id),
                "attemptId": str(rejection.attempt_id),
                "snapshotId": str(rejection.snapshot_id),
                "reason": rejection.reason,
                "detail": rejection.detail,
                "fingerprint": fingerprint,
                "total": total + 1,
                "counts": counts,
                "toolCallId": str(rejection.tool_call_id) if rejection.tool_call_id else None,
                "seen": {**({} if history is None else history.get("seen", {})), key: fingerprint},
                "observation": CandidateObservation(
                    observation_id=uuid5(
                        NAMESPACE_URL, f"candidate-observation:{state.run_id}:{key}"
                    ),
                    run_id=state.run_id,
                    attempt_id=rejection.attempt_id,
                    snapshot_id=rejection.snapshot_id,
                    tool_call_id=rejection.tool_call_id,
                    reason=rejection.reason,
                    fingerprint=fingerprint,
                    effect=effect,
                    total=total + 1,
                    fingerprint_count=counts[fingerprint],
                ).model_dump(mode="json", by_alias=True),
            }
            digest = content_digest(payload)
            ref = context_blob_key(state.scope_id, digest)
            identity = uuid5(
                NAMESPACE_URL, f"candidate-repair:{state.run_id}:{rejection.attempt_id}:{digest}"
            )
            await self._blobs.put_in_transaction(db, state.scope_id, ref, canonical_bytes(payload))
            await db.execute(
                scoped(
                    "INSERT INTO {t}checkpoint (id,scope_id,run_id,source_checkpoint_id,"
                    "state_version,representation_revision,event_seq,schema_revision,"
                    "runtime_build,state_ref,content_hash) VALUES "
                    "(:id,:scope,:run,:source,:version,:representation,:seq,"
                    "1,'agent-runtime',:ref,:hash)",
                    self._schema,
                ),
                {
                    "id": identity,
                    "scope": state.scope_id,
                    "run": state.run_id,
                    "source": current["id"],
                    "version": current["state_version"],
                    "representation": current["representation_revision"] + 1,
                    "seq": current["event_seq"],
                    "ref": ref,
                    "hash": digest,
                },
            )
            changed = (
                await db.execute(
                    scoped(
                        "UPDATE {t}run SET checkpoint_id=:new WHERE scope_id=:scope AND id=:run "
                        "AND checkpoint_id=:old AND state_version=:version RETURNING id",
                        self._schema,
                    ),
                    {
                        "new": identity,
                        "scope": state.scope_id,
                        "run": state.run_id,
                        "old": current["id"],
                        "version": run["state_version"],
                    },
                )
            ).scalar_one_or_none()
            if changed is None:
                errors.fenced("candidate correction lost checkpoint compare-and-swap")
        return state.model_copy(update={"checkpoint_ref": ref})


def recovery_feedback(detail: str, failures: set[UUID], visible: set[str]) -> str:
    """Offer bounded locating hints, never an authorization or semantic recovery verdict."""
    failed_ids = {str(identity) for identity in failures}
    delivered = sorted(failed_ids & visible)
    others = sorted(visible - failed_ids)
    return (
        detail[:180] + f" Failed calls: {len(failed_ids)} total; visible failedToolCallIds "
        f"(showing up to 6): {delivered[:6]}. "
        f"Other visible result IDs (up to 3, candidates NOT recovery proof): {others[:3]}. "
        "Return the FULL recoveredToolFailures array, not a delta. Use only actual later "
        "completed related receipts and explain goal verification. Unresolved means partial; "
        "never invent IDs or waive unknown effects."
    )[:1000]

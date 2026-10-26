"""Sealed producer and execution re-enumeration (review §11.2 P1-1/P1-4, Q45).

``event.producer_digest`` is a *single event* digest and event existence does not prove a
producer finished flushing: the second repair therefore persists one immutable
``output_seal`` row per sealed producer, holding both channels' **cumulative** digests,
both channel heads and the durable journal watermark. The state commits re-enumerate those
rows inside their transaction and require **set equivalence** with the request's proofs:

* a proof must match a stored seal exactly (identity, cumulative digests, channel heads,
  watermark);
* ``NoProducersConfirmed`` is accepted only when the run has neither seal rows nor event
  deltas, proven while holding the run row that the journal writer must also hold (the
  shared fence that closes the check-then-append window).

Executions are enumerated by their own ``run_id`` (never by the shared task), and an
unresolved execution must reference a supervision work item that is persisted, belongs to
the same run and has the right kind/status - a request cannot fabricate that link.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.transitions import FlushEvidence, VerifiedEvidence
from agent_runtime.storage.uow import errors
from agent_runtime.storage.uow.schema import scoped

#: Database execution statuses that mean "the process is confirmed stopped".
STOPPED_STATUSES = frozenset({"completed", "failed", "cancelled"})
#: Database statuses that mean "not running, but not yet confirmed stopped".
ISOLATED_STATUSES = frozenset({"pending", "starting", "running", "cancelling"})
UNKNOWN_STATUS = "unknown"

#: Supervision work kinds that may keep an unresolved execution persistent.
SUPERVISION_KINDS = ("reconcile_execution", "recover_scope")
SUPERVISION_STATUSES = ("pending", "leased")


@dataclass(frozen=True, slots=True)
class EvidenceStatements:
    seals: sa.TextClause
    deltas: sa.TextClause
    executions: sa.TextClause
    supervision: sa.TextClause


def build_evidence_statements(schema: str) -> EvidenceStatements:
    return EvidenceStatements(
        seals=scoped(
            "SELECT proof_id, model_attempt_id, item_id, content_digest, analysis_digest, "
            "content_last_delta_index, content_last_session_seq, analysis_last_delta_index, "
            "analysis_last_session_seq, durable_seq, sealed_revision FROM {t}output_seal "
            "WHERE scope_id = :scope_id AND run_id = :run_id "
            "ORDER BY sealed_revision, model_attempt_id, item_id",
            schema,
        ),
        deltas=scoped(
            "SELECT count(*) FROM {t}event WHERE scope_id = :scope_id AND run_id = :run_id "
            "AND kind IN ('assistant.delta', 'analysis.delta')",
            schema,
        ),
        executions=scoped(
            "SELECT id, generation, status, supervisor_work_id FROM {t}execution "
            "WHERE scope_id = :scope_id AND run_id = :run_id ORDER BY id",
            schema,
        ),
        supervision=scoped(
            "SELECT id, kind, status, run_id FROM {t}work_item "
            "WHERE scope_id = :scope_id AND id = :work_id",
            schema,
        ),
    )


def state_bucket(status: str) -> str:
    """Map a persisted execution status onto the request's vocabulary."""
    if status in STOPPED_STATUSES:
        return "stopped"
    if status == UNKNOWN_STATUS:
        return "unknown"
    return "isolated"


async def producer_facts(
    session: AsyncSession, sql: EvidenceStatements, run: Mapping[str, Any]
) -> list[dict[str, Any]]:
    result = await session.execute(sql.seals, {"scope_id": run["scope_id"], "run_id": run["id"]})
    return [dict(row) for row in result.mappings()]


async def producer_delta_count(
    session: AsyncSession, sql: EvidenceStatements, run: Mapping[str, Any]
) -> int:
    result = await session.execute(sql.deltas, {"scope_id": run["scope_id"], "run_id": run["id"]})
    return int(result.scalar_one())


async def execution_facts(
    session: AsyncSession, sql: EvidenceStatements, run: Mapping[str, Any]
) -> list[dict[str, Any]]:
    result = await session.execute(
        sql.executions, {"scope_id": run["scope_id"], "run_id": run["id"]}
    )
    return [dict(row) for row in result.mappings()]


async def verify_flush_evidence(
    session: AsyncSession,
    sql: EvidenceStatements,
    run: Mapping[str, Any],
    evidence: FlushEvidence,
) -> None:
    """Require the submitted evidence to match the persisted sealed producers exactly."""
    seals = await producer_facts(session, sql, run)
    watermark = max((int(row["durable_seq"]) for row in seals), default=0)
    if not isinstance(evidence, VerifiedEvidence):
        if seals:
            errors.invalid_input("no-producer confirmation contradicts persisted seals")
        if await producer_delta_count(session, sql, run):
            errors.invalid_input("no-producer confirmation contradicts persisted deltas")
        return
    persisted: dict[tuple[Any, Any], Mapping[str, Any]] = {
        (row["model_attempt_id"], row["item_id"]): row for row in seals
    }
    claimed: dict[tuple[Any, Any], Any] = {}
    for proof in evidence.proofs:
        key = (proof.attempt_id, proof.item_id)
        if key in claimed:
            errors.invalid_input("duplicate flush proof for one producer")
        claimed[key] = proof
    if set(persisted) - set(claimed):
        errors.invalid_input("flush evidence omits a sealed output producer")
    if set(claimed) - set(persisted):
        errors.invalid_input("flush evidence contains an unsealed output producer")
    if int(evidence.enumerated_session_seq) < watermark:
        errors.version_conflict("flush evidence was enumerated behind sealed producers")
    for key, proof in claimed.items():
        seal = persisted[key]
        if (
            proof.proof_id != seal["proof_id"]
            or proof.scope_id != run["scope_id"]
            or proof.run_id != run["id"]
            or proof.session_id != run["session_id"]
        ):
            # Identity is a stored fact: a request may not relabel another run's producer.
            errors.invalid_input("flush proof identity does not match the sealed producer")
        channels = {channel.channel: channel for channel in proof.channels}
        expected = {
            "content": (
                seal["content_digest"],
                seal["content_last_delta_index"],
                seal["content_last_session_seq"],
            ),
            "analysis": (
                seal["analysis_digest"],
                seal["analysis_last_delta_index"],
                seal["analysis_last_session_seq"],
            ),
        }
        for channel, (digest, delta_index, session_seq) in expected.items():
            actual = channels[channel]
            if actual.digest != digest:
                errors.version_conflict("flush proof digest does not match the sealed channel")
            expected_delta = None if delta_index is None else str(delta_index)
            expected_seq = None if session_seq is None else str(session_seq)
            if actual.last_delta_index != expected_delta or actual.last_session_seq != expected_seq:
                errors.version_conflict("flush proof channel head does not match the seal")
        if int(proof.durable_seq) < int(seal["durable_seq"]):
            errors.version_conflict("flush proof is behind the sealed watermark")


async def verify_executions(
    session: AsyncSession,
    sql: EvidenceStatements,
    run: Mapping[str, Any],
    executions: Sequence[Any],
) -> None:
    """Require the submitted execution resolutions to match the run's rows exactly."""
    facts = {str(row["id"]): row for row in await execution_facts(session, sql, run)}
    claimed = {str(item.execution_ref.resource_id): item for item in executions}
    if len(claimed) != len(executions):
        errors.invalid_input("duplicate execution resolution")
    if set(facts) - set(claimed):
        errors.invalid_input("execution evidence omits a persisted execution")
    if set(claimed) - set(facts):
        errors.invalid_input("execution evidence references an unknown execution")
    for resource_id, item in claimed.items():
        row = facts[resource_id]
        if item.execution_ref.scope.scope_id != run["scope_id"]:
            errors.invalid_input("execution resolution belongs to another scope")
        if int(item.generation) != int(row["generation"]):
            errors.version_conflict("execution generation changed")
        if item.state != state_bucket(str(row["status"])):
            errors.version_conflict("execution state changed in the database")
        if item.state == "stopped":
            continue
        if item.supervision_work_id is None:
            errors.invalid_input("unresolved execution requires persistent supervision")
        if row["supervisor_work_id"] != item.supervision_work_id:
            # The link is a stored fact: a request may not invent or swap supervision.
            errors.invalid_input("execution supervision identity does not match the database")
        await _require_supervision_work(session, sql, run, item.supervision_work_id)


async def _require_supervision_work(
    session: AsyncSession, sql: EvidenceStatements, run: Mapping[str, Any], work_id: Any
) -> None:
    result = await session.execute(
        sql.supervision, {"scope_id": run["scope_id"], "work_id": work_id}
    )
    row = result.mappings().one_or_none()
    if row is None:
        errors.invalid_input("supervision work item does not exist")
    if row["run_id"] != run["id"]:
        errors.invalid_input("supervision work item belongs to another run")
    if str(row["kind"]) not in SUPERVISION_KINDS:
        errors.invalid_input("supervision work item has the wrong kind")
    if str(row["status"]) not in SUPERVISION_STATUSES:
        errors.invalid_input("supervision work item is not pending or leased")


__all__ = [
    "ISOLATED_STATUSES",
    "STOPPED_STATUSES",
    "SUPERVISION_KINDS",
    "SUPERVISION_STATUSES",
    "UNKNOWN_STATUS",
    "EvidenceStatements",
    "build_evidence_statements",
    "execution_facts",
    "producer_delta_count",
    "producer_facts",
    "state_bucket",
    "verify_executions",
    "verify_flush_evidence",
]

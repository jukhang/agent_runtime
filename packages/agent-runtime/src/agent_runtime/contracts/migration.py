"""Migration orchestration entry (DS-P02-00; review P1-3).

The host chain and the runtime chain are separate Alembic chains over the same
PostgreSQL instance. This contract freezes *what the deployment job may claim and what
it must report*; it does not execute DDL.

Forward-only responsibility is split deliberately:

- the contract only rejects a step that does not move a chain (``from == to``) and does
  not allow a ``downgrade`` step to be expressed at all;
- whether ``to_revision`` is a *successor of the current head* is a trusted check on the
  migration registry / script directory, performed at execution time after re-reading
  the live head; Alembic revisions are DAG identities, so no string comparison here can
  prove ancestry;
- a plan therefore carries the observed heads and every chain's steps must be continuous
  from that head, which makes a jump back to an older revision detectable by the
  orchestrator and replayable only with the same plan identity;
- results echo the plan identity and report structured step references, so a completed
  step proves its chain and from/to revisions instead of a bare key.

Recovery semantics stay frozen:

- the runtime chain never runs when the host chain failed;
- a runtime failure leaves the successful host migration in place (no automatic
  rollback) and is retryable with the same plan identity;
- application start-up failure is outside this entry: the forward-compatible schema is
  kept and the application image is rolled back.

Concurrent deployments take an advisory lock; the loser reports ``deferred`` with
``lock_held`` instead of racing the same revision.
"""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, TypeAdapter, model_validator

from agent_runtime.contracts.base import Contract, Identifier, MachineKey, Revision
from agent_runtime.contracts.failure import FailureEnvelope
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.views import Digest

type MigrationChain = Literal["host", "runtime"]
#: Only forward steps exist in this contract; a rollback cannot be expressed.
type MigrationStepKind = Literal["upgrade"]
type MigrationDeferralReason = Literal["host_failed", "runtime_failed", "lock_held", "incompatible"]


class MigrationHeads(Contract):
    """Live head of each chain, re-read by the orchestrator before executing."""

    host_revision: Identifier
    runtime_revision: Identifier


class MigrationStep(Contract):
    chain: MigrationChain
    kind: MigrationStepKind
    from_revision: Identifier
    to_revision: Identifier
    idempotency_key: MachineKey
    deadline_at: AwareDatetime

    @model_validator(mode="after")
    def moves_the_chain(self) -> Self:
        if self.from_revision == self.to_revision:
            raise ValueError("a migration step must change the chain revision")
        return self


class MigrationPlanRequest(Contract):
    plan_id: UUID
    plan_revision: Revision
    deployment_id: Identifier
    actor: ActorContext
    heads: MigrationHeads
    steps: Annotated[tuple[MigrationStep, ...], Field(min_length=1, max_length=64)]
    idempotency_key: MachineKey
    deadline_at: AwareDatetime

    @model_validator(mode="after")
    def ordered_continuous_plan(self) -> Self:
        keys = [step.idempotency_key for step in self.steps]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate migration idempotency key")
        chains = [step.chain for step in self.steps]
        if "runtime" in chains and "host" in chains:
            last_host = max(index for index, name in enumerate(chains) if name == "host")
            first_runtime = min(index for index, name in enumerate(chains) if name == "runtime")
            if first_runtime < last_host:
                raise ValueError("runtime steps cannot precede host steps")
        for chain, head in (
            ("host", self.heads.host_revision),
            ("runtime", self.heads.runtime_revision),
        ):
            previous = head
            for step in (item for item in self.steps if item.chain == chain):
                if step.from_revision != previous:
                    raise ValueError(f"{chain} steps must be continuous from the observed head")
                previous = step.to_revision
        return self


class MigrationStepRef(Contract):
    """Structured proof of one applied step; a bare key cannot prove ancestry."""

    chain: MigrationChain
    from_revision: Identifier
    to_revision: Identifier
    idempotency_key: MachineKey

    @classmethod
    def of(cls, step: MigrationStep) -> "MigrationStepRef":
        return cls(
            chain=step.chain,
            from_revision=step.from_revision,
            to_revision=step.to_revision,
            idempotency_key=step.idempotency_key,
        )


def _require_continuous(steps: tuple[MigrationStepRef, ...]) -> None:
    for chain in {step.chain for step in steps}:
        chain_steps = [step for step in steps if step.chain == chain]
        for previous, current in zip(chain_steps, chain_steps[1:], strict=False):
            if previous.to_revision != current.from_revision:
                raise ValueError(f"{chain} completed steps are not continuous")


class MigrationSucceeded(Contract):
    disposition: Literal["succeeded"]
    plan_id: UUID
    plan_digest: Digest
    deployment_id: Identifier
    host_revision: Identifier | None
    runtime_revision: Identifier | None
    applied: Annotated[tuple[MigrationStepRef, ...], Field(min_length=1)]

    @model_validator(mode="after")
    def applied_is_continuous(self) -> Self:
        _require_continuous(self.applied)
        return self


class MigrationDeferred(Contract):
    """Retryable state: nothing was rolled back and the same plan may be replayed."""

    disposition: Literal["deferred"]
    plan_id: UUID
    plan_digest: Digest
    deployment_id: Identifier
    reason: MigrationDeferralReason
    completed: tuple[MigrationStepRef, ...] = ()
    retry_after_seconds: Annotated[int, Field(strict=True, ge=0)] | None = None

    @model_validator(mode="after")
    def honest_completion(self) -> Self:
        _require_continuous(self.completed)
        if self.reason == "runtime_failed" and not any(
            step.chain == "host" for step in self.completed
        ):
            raise ValueError("a runtime failure must report the completed host steps")
        if self.reason == "host_failed" and self.completed:
            raise ValueError("no step may complete after a host failure")
        return self


class MigrationRejected(Contract):
    disposition: Literal["rejected"]
    plan_id: UUID
    deployment_id: Identifier
    failure: FailureEnvelope


type MigrationOutcome = Annotated[
    MigrationSucceeded | MigrationDeferred | MigrationRejected, Field(discriminator="disposition")
]
migration_adapter: TypeAdapter[MigrationOutcome] = TypeAdapter(MigrationOutcome)

"""Restricted audit queries (DS-P02-00).

The audit port reads restricted journal facts for recovery, support, security and
retention work. It is deliberately separate from the public event subscription: the
request carries an explicit authorization context, the result is marked ``restricted``,
and every entry references a stored payload instead of inline content, so a public
stream or a public client can never obtain these rows through this path.
"""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.views import Digest

type AuditVisibility = Literal["public", "restricted"]
type AuditPurpose = Literal["recovery", "support", "security", "retention"]

MAX_AUDIT_ENTRIES = 256


class AuditQueryRequest(Contract):
    """Explicit authorization context is mandatory; there is no anonymous audit read."""

    actor: ActorContext
    purpose: AuditPurpose
    session_id: UUID | None = None
    run_id: UUID | None = None
    after_seq: Revision = "0"
    kinds: Annotated[tuple[Identifier, ...], Field(min_length=1, max_length=16)]
    limit: Annotated[int, Field(strict=True, ge=1, le=MAX_AUDIT_ENTRIES)] = 64
    deadline_at: AwareDatetime

    @model_validator(mode="after")
    def bounded_scope(self) -> Self:
        if self.session_id is None and self.run_id is None:
            raise ValueError("an audit query must target a session or a run")
        if len(self.kinds) != len(set(self.kinds)):
            raise ValueError("duplicate audit event kind")
        return self


class AuditEntry(Contract):
    session_seq: Revision
    kind: Identifier
    visibility: AuditVisibility
    recorded_at: AwareDatetime
    payload_ref: Identifier
    content_digest: Digest


class AuditQueryResult(Contract):
    scope_id: Identifier
    session_id: UUID
    entries: Annotated[tuple[AuditEntry, ...], Field(max_length=MAX_AUDIT_ENTRIES)] = ()
    next_seq: Revision
    restricted: Literal[True] = True

    @model_validator(mode="after")
    def ordered_page(self) -> Self:
        seqs = [int(entry.session_seq) for entry in self.entries]
        if seqs != sorted(set(seqs)):
            raise ValueError("audit entries must be strictly ordered")
        if seqs and int(self.next_seq) <= seqs[-1]:
            raise ValueError("audit cursor must advance past the last entry")
        if not seqs and int(self.next_seq) < 0:
            raise ValueError("audit cursor must not be negative")
        return self

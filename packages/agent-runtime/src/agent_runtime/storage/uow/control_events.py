"""Control status projection participates in the owning State UoW transaction."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import AsyncSession

from agent_runtime.contracts.payloads import CommandUpdated, RunCancelled, RunStatusPayload
from agent_runtime.contracts.state import RunStatus
from agent_runtime.contracts.state_events import (
    CommandUpdatedDraft,
    MainEventAuthority,
    RunCancelledDraft,
    RunStatusDraft,
    StateEventBatch,
    StateEventDraft,
)
from agent_runtime.contracts.transitions import PauseCommitRequest, TerminalCommitRequest
from agent_runtime.storage.uow import errors
from agent_runtime.terminal_guidance import terminal_guidance

if TYPE_CHECKING:
    from agent_runtime.storage.journal import SqlJournal
    from agent_runtime.storage.uow.state import _Guarded


async def append_control_events(
    db: AsyncSession,
    journal: SqlJournal,
    guarded: _Guarded,
    request: PauseCommitRequest | TerminalCommitRequest,
    *,
    status: str,
) -> None:
    run = guarded.run
    command = request.guard.control_command_id
    if command is None or status not in {"paused", "cancelled"}:
        errors.invalid_input("control event projection requires pause or cancellation")
    common = {
        "occurred_at": guarded.now,
        "turn_id": run["turn_id"],
        "run_id": run["id"],
        "parent_run_id": run["parent_run_id"],
        "step_id": None,
        "item_id": None,
        "model_attempt_id": None,
    }
    version = str(int(run["state_version"]) + 1)
    guidance = (
        terminal_guidance("cancelled", retained_count=len(request.result.partial_results))
        if isinstance(request, TerminalCommitRequest)
        else None
    )
    events: list[StateEventDraft] = [
        RunStatusDraft(
            kind="run.status",
            producer_key=f"control-status:{command}",
            payload=RunStatusPayload(
                state_version=version,
                status=RunStatus(status),
                phase="paused" if status == "paused" else "terminal",
                phase_started_at=guarded.now,
                public_reason="Task paused." if guidance is None else guidance.message,
            ),
            **common,
        ),
        CommandUpdatedDraft(
            kind="command.updated",
            producer_key=f"control-applied:{command}",
            payload=CommandUpdated(
                command_id=command,
                status="applied",
                public_reason="Control applied at a verified safe point.",
            ),
            **common,
        ),
    ]
    if isinstance(request, TerminalCommitRequest):
        events.append(
            RunCancelledDraft(
                kind="run.cancelled",
                producer_key=f"control-cancelled:{command}",
                payload=RunCancelled(
                    state_version=version,
                    final_item_ids=tuple(item.id for item in request.result.final_items),
                    partial_result_refs=request.result.partial_results,
                    outcome=None,
                    failure=None,
                    public_message=guidance.message if guidance is not None else None,
                    next_steps=guidance.next_steps if guidance is not None else (),
                ),
                **common,
            )
        )
    await journal.append_state_in_transaction(
        db,
        StateEventBatch(
            authority=MainEventAuthority(kind="main", guard=request.guard),
            session_id=run["session_id"],
            trace_id=str(command),
            events=tuple(events),
        ),
    )

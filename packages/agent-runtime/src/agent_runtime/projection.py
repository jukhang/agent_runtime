"""Public event projection shared by snapshot reconstruction and non-Web clients."""

from __future__ import annotations

import hashlib
from typing import Literal
from uuid import UUID

from agent_runtime.contracts.base import Contract
from agent_runtime.contracts.events import EventEnvelope
from agent_runtime.contracts.payloads import RunCancelled, RunCompleted, RunFailed
from agent_runtime.contracts.references import ResourceRef
from agent_runtime.contracts.views import (
    ApprovalView,
    ArtifactView,
    ChannelState,
    InputRequestView,
    PlanView,
    SnapshotEnvelope,
    TimelineItem,
    ToolActivity,
)


def display_event(event: EventEnvelope, *, include_analysis: bool) -> EventEnvelope | None:
    """Project one authorized display view without rewriting its stored Journal fact."""
    if event.visibility != "public":
        if not (include_analysis and event.kind == "analysis.delta"):
            return None
    if not include_analysis:
        if event.kind == "analysis.delta":
            return None
        if event.kind == "item.started":
            payload = event.payload.model_copy(
                update={"channels": tuple(c for c in event.payload.channels if c != "analysis")}
            )
            return event.model_copy(update={"payload": payload})
        if event.kind == "item.completed":
            completed = event.payload.model_copy(
                update={
                    "channel_digests": tuple(
                        c for c in event.payload.channel_digests if c.channel != "analysis"
                    ),
                    "last_delta_indexes": tuple(
                        c for c in event.payload.last_delta_indexes if c.channel != "analysis"
                    ),
                }
            )
            return event.model_copy(update={"payload": completed})
    return event


class ProjectionState(Contract):
    snapshot: SnapshotEnvelope
    item_runs: dict[UUID, UUID | None]
    terminal_events: tuple[EventEnvelope, ...]


class SessionProjection:
    """Consumes ordered public events. SQL or transport owns the full-page watermark."""

    def __init__(self, session_id: UUID, *, include_analysis: bool = False) -> None:
        self.session_id = session_id
        self._include_analysis = include_analysis
        self.cursor = 0
        self.items: dict[UUID, TimelineItem] = {}
        self.item_runs: dict[UUID, UUID | None] = {}
        self.plans: dict[UUID, PlanView] = {}
        self.artifacts: dict[UUID | None, list[ResourceRef]] = {}
        self.partial_results: dict[UUID | None, tuple[ResourceRef, ...]] = {}
        self.final_items: dict[UUID | None, tuple[UUID, ...]] = {}
        self.tools: dict[UUID, ToolActivity] = {}
        self.artifact_views: dict[ResourceRef, ArtifactView] = {}
        self.input_requests: dict[UUID, InputRequestView] = {}
        self.approvals: dict[ResourceRef, ApprovalView] = {}
        self.terminals: dict[UUID | None, RunCompleted | RunFailed | RunCancelled] = {}
        self.terminal_events: dict[UUID | None, EventEnvelope] = {}

    @classmethod
    def restore(cls, state: ProjectionState, *, include_analysis: bool) -> SessionProjection:
        snapshot = state.snapshot
        if snapshot.projection_version != 1:
            raise ValueError("Unsupported materialized projection version")
        view = cls(snapshot.session_id, include_analysis=include_analysis)
        view.cursor = int(snapshot.snapshot_seq)
        view.items = {
            item.id: item.model_copy(
                update={
                    "channels": tuple(
                        channel
                        for channel in item.channels
                        if include_analysis or channel.name != "analysis"
                    )
                }
            )
            for item in snapshot.items
        }
        if set(view.items) != set(state.item_runs):
            raise ValueError("Cached item ownership is incomplete")
        view.item_runs = dict(state.item_runs)
        view.plans = {plan.id: plan for plan in snapshot.plans}
        view.tools = {tool.id: tool for tool in snapshot.tools}
        view.artifact_views = {artifact.resource_ref: artifact for artifact in snapshot.artifacts}
        for artifact in snapshot.artifacts:
            view.artifacts.setdefault(artifact.run_id, []).append(artifact.resource_ref)
        view.input_requests = {question.run_id: question for question in snapshot.input_requests}
        view.approvals = {approval.approval_ref: approval for approval in snapshot.approvals}
        for event in state.terminal_events:
            if (
                event.session_id != snapshot.session_id
                or int(event.session_seq) > view.cursor
                or event.kind not in {"run.completed", "run.failed", "run.cancelled"}
                or event.run_id is None
                or event.run_id in view.terminal_events
            ):
                raise ValueError("Cached terminal event is invalid")
            assert event.kind in {"run.completed", "run.failed", "run.cancelled"}
            if any(
                item_id not in view.items
                or view.item_runs[item_id] != event.run_id
                or view.items[item_id].role != "assistant"
                or view.items[item_id].status in {"pending", "streaming"}
                for item_id in event.payload.final_item_ids
            ):
                raise ValueError("Cached terminal output ownership or status is invalid")
            view.terminal_events[event.run_id] = event
            view.terminals[event.run_id] = event.payload
            view.final_items[event.run_id] = event.payload.final_item_ids
            view.partial_results[event.run_id] = event.payload.partial_result_refs
        return view

    def apply(self, event: EventEnvelope) -> bool:
        if event.session_id != self.session_id or (
            event.visibility != "public"
            and not (self._include_analysis and event.kind == "analysis.delta")
        ):
            raise ValueError("Projection requires public events from the same session")
        seq = int(event.session_seq)
        if seq <= self.cursor:
            return False
        if event.kind == "turn.accepted":
            payload = event.payload
            if payload.user_item.id in self.items:
                raise ValueError("Duplicate input item")
            self.items[payload.user_item.id] = payload.user_item
            self.item_runs[payload.user_item.id] = event.run_id
        elif event.kind == "item.started":
            started = event.payload
            if event.item_id is None:
                raise ValueError("Item event requires identity")
            if event.item_id in self.items:
                raise ValueError("Duplicate output item")
            self.items[event.item_id] = TimelineItem(
                id=event.item_id,
                type=started.item_type,
                role=started.role,
                created_seq=event.session_seq,
                revision=started.item_revision,
                status="streaming",
                resource_refs=(),
                channels=tuple(self._channel(name, "", None) for name in started.channels),
            )
            self.item_runs[event.item_id] = event.run_id
        elif event.kind in {"assistant.delta", "analysis.delta"}:
            delta = event.payload
            if event.item_id is None or event.item_id not in self.items:
                raise ValueError("Delta requires an existing item")
            item = self.items[event.item_id]
            if item.revision != delta.item_revision or item.status != "streaming":
                raise ValueError("Delta item revision/status changed")
            channels = {part.name: part for part in item.channels}
            if delta.channel not in channels:
                raise ValueError("Undeclared output channel")
            previous = channels[delta.channel]
            index = int(delta.delta_index)
            expected = (
                0 if previous.last_delta_index is None else int(previous.last_delta_index) + 1
            )
            if index != expected:
                raise ValueError("Output delta gap")
            channels[delta.channel] = self._channel(
                delta.channel, previous.text + delta.text, delta.delta_index
            )
            self.items[item.id] = item.model_copy(update={"channels": tuple(channels.values())})
        elif event.kind == "item.completed":
            completed = event.payload
            if event.item_id is None or event.item_id not in self.items:
                raise ValueError("Completion requires an existing item")
            item = self.items[event.item_id]
            if item.status != "streaming" and item.status != completed.status:
                raise ValueError("Completion cannot rewrite a terminal item status")
            channels = {part.name: part for part in item.channels}
            if item.revision != completed.item_revision:
                raise ValueError("Completion item revision changed")
            if {part.channel for part in completed.channel_digests} != set(channels):
                raise ValueError("Completion omits declared output channels")
            for digest in completed.channel_digests:
                if digest.channel not in channels or (
                    channels[digest.channel].content_hash != digest.digest
                ):
                    raise ValueError("Output channel digest mismatch")
            for channel_index in completed.last_delta_indexes:
                if channel_index.channel not in channels or (
                    channels[channel_index.channel].last_delta_index
                    != channel_index.last_delta_index
                ):
                    raise ValueError("Output channel watermark mismatch")
            self.items[item.id] = item.model_copy(update={"status": completed.status})
        elif event.kind == "plan.updated":
            plan = event.payload
            self.plans[plan.plan_id] = PlanView(
                id=plan.plan_id, revision=plan.revision, steps=plan.steps
            )
        elif event.kind == "input.required":
            assert event.run_id is not None
            self.input_requests[event.run_id] = InputRequestView(
                id=event.event_id,
                run_id=event.run_id,
                **event.payload.model_dump(exclude={"reply_to_run_id"}),
            )
        elif event.kind == "input.accepted":
            reply = event.payload
            if reply.user_item.id in self.items:
                raise ValueError("Duplicate reply item")
            self.items[reply.user_item.id] = reply.user_item
            self.item_runs[reply.user_item.id] = event.run_id
            self.input_requests.pop(reply.reply_to_run_id, None)
        elif event.kind == "tool.started":
            assert event.run_id is not None
            tool = event.payload
            if tool.tool_call_id in self.tools:
                raise ValueError("Duplicate tool start")
            self.tools[tool.tool_call_id] = ToolActivity(
                id=tool.tool_call_id,
                run_id=event.run_id,
                name=tool.name,
                public_arguments=tool.public_arguments,
                status="running",
                summary="",
                result_refs=(),
                failure=None,
                created_seq=event.session_seq,
            )
        elif event.kind in {"tool.progress", "tool.finished"}:
            previous_tool = self.tools.get(event.payload.tool_call_id)
            if previous_tool is None or previous_tool.run_id != event.run_id:
                raise ValueError("Tool event requires its start in the same Run")
            if event.kind == "tool.progress":
                self.tools[previous_tool.id] = previous_tool.model_copy(
                    update={"summary": event.payload.summary}
                )
            else:
                self.tools[previous_tool.id] = previous_tool.model_copy(
                    update={
                        "status": event.payload.status,
                        "failure": event.payload.failure,
                        "result_refs": event.payload.result_refs,
                    }
                )
        elif event.kind == "approval.required":
            assert event.run_id is not None
            if any(
                ref.resource_id == event.payload.approval_ref.resource_id
                for ref in self.approvals
            ):
                raise ValueError("Approval request cannot overwrite an existing decision")
            self.approvals[event.payload.approval_ref] = ApprovalView(
                run_id=event.run_id,
                revision="0",
                **event.payload.model_dump(),
            )
        elif event.kind == "approval.updated":
            assert event.run_id is not None
            approval_update = event.payload
            previous_key = next(
                (
                    ref
                    for ref in self.approvals
                    if ref.resource_id == approval_update.approval_ref.resource_id
                ),
                None,
            )
            prior_approval = self.approvals.get(previous_key) if previous_key is not None else None
            if prior_approval is not None and (
                prior_approval.run_id != event.run_id
                or prior_approval.status != "pending"
                or int(approval_update.revision) != int(prior_approval.revision) + 1
            ):
                raise ValueError("Approval decision cannot overwrite an existing terminal decision")
            if previous_key is not None:
                del self.approvals[previous_key]
            self.approvals[approval_update.approval_ref] = ApprovalView(
                run_id=event.run_id,
                approval_ref=approval_update.approval_ref,
                summary=prior_approval.summary if prior_approval else approval_update.public_reason,
                expires_at=approval_update.expires_at,
                allowed_decisions=(),
                status=approval_update.status,
                revision=approval_update.revision,
                public_reason=approval_update.public_reason,
            )
        elif event.kind == "artifact.created":
            resources = self.artifacts.setdefault(event.run_id, [])
            if event.payload.resource_ref not in resources:
                resources.append(event.payload.resource_ref)
            self.artifact_views[event.payload.resource_ref] = ArtifactView(
                run_id=event.run_id, **event.payload.model_dump()
            )
        elif event.kind == "resource.revoked":
            revoked = event.payload.resource_id
            self.artifact_views = {
                ref: value
                for ref, value in self.artifact_views.items()
                if ref.resource_id != revoked
            }
            for run_id, refs in self.artifacts.items():
                self.artifacts[run_id] = [ref for ref in refs if ref.resource_id != revoked]
            for item_id, item in self.items.items():
                self.items[item_id] = item.model_copy(
                    update={
                        "resource_refs": tuple(
                            ref for ref in item.resource_refs if ref.resource_id != revoked
                        )
                    }
                )
            for tool_id, activity in self.tools.items():
                self.tools[tool_id] = activity.model_copy(
                    update={
                        "result_refs": tuple(
                            ref for ref in activity.result_refs if ref.resource_id != revoked
                        )
                    }
                )
            self.approvals = {
                ref: value for ref, value in self.approvals.items() if ref.resource_id != revoked
            }
            for owner, terminal_payload in self.terminals.items():
                remaining_refs = tuple(
                    ref
                    for ref in terminal_payload.partial_result_refs
                    if ref.resource_id != revoked
                )
                changes: dict[str, object] = {"partial_result_refs": remaining_refs}
                if terminal_payload.failure is not None:
                    changes["failure"] = terminal_payload.failure.model_copy(
                        update={
                            "partial_result_refs": tuple(
                                ref
                                for ref in terminal_payload.failure.partial_result_refs
                                if ref.resource_id != revoked
                            )
                        }
                    )
                projected_terminal = terminal_payload.model_copy(update=changes)
                self.terminals[owner] = projected_terminal
                self.partial_results[owner] = remaining_refs
                self.terminal_events[owner] = self.terminal_events[owner].model_copy(
                    update={"payload": projected_terminal}
                )
        elif event.kind in {"run.failed", "run.cancelled", "run.completed"}:
            final_ids = event.payload.final_item_ids
            if len(set(final_ids)) != len(final_ids) or any(
                identity not in self.items
                or self.item_runs[identity] != event.run_id
                or self.items[identity].role != "assistant"
                for identity in final_ids
            ):
                raise ValueError("Terminal result references an unknown or non-output item")
            terminal: Literal["failed", "cancelled", "completed"] = (
                "failed"
                if event.kind == "run.failed"
                else "cancelled"
                if event.kind == "run.cancelled"
                else "completed"
            )
            for identity, item in self.items.items():
                if self.item_runs[identity] == event.run_id and item.status in {
                    "pending",
                    "streaming",
                }:
                    if terminal == "completed":
                        raise ValueError("Successful run has unfinished output")
                    self.items[identity] = item.model_copy(update={"status": terminal})
            self.partial_results[event.run_id] = event.payload.partial_result_refs
            self.final_items[event.run_id] = final_ids
            self.terminals[event.run_id] = event.payload
            self.terminal_events[event.run_id] = event
            if event.run_id is not None:
                self.input_requests.pop(event.run_id, None)
        self.cursor = seq
        return True

    @staticmethod
    def _channel(
        name: Literal["content", "analysis"], text: str, index: str | None
    ) -> ChannelState:
        return ChannelState(
            name=name,
            text=text,
            last_delta_index=index,
            content_hash=hashlib.sha256(text.encode()).hexdigest(),
        )


__all__ = ["SessionProjection", "display_event"]

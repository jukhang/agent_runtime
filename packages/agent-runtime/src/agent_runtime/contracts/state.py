"""Finite state vocabularies; these do not authorize transitions."""

from enum import StrEnum


class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_TOOL = "waiting_tool"
    WAITING_USER = "waiting_user"
    PAUSED = "paused"
    CANCELLING = "cancelling"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DesiredControl(StrEnum):
    PAUSE = "pause"
    CANCEL = "cancel"


class WakeReason(StrEnum):
    START = "start"
    TOOL_RESULT = "tool_result"
    MESSAGE = "message"
    RESUME = "resume"
    HANDOFF = "handoff"
    RECOVERY = "recovery"
    CONTROL = "control"

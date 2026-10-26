"""Trusted resource measurements and fail-closed stop decisions, not OS isolation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from agent_runtime.sandbox.runtime import RemoteStatus


@dataclass(frozen=True, slots=True)
class ExecutionLimits:
    log_bytes: int
    workspace_bytes: int
    artifact_bytes: int
    artifact_count: int

    def __post_init__(self) -> None:
        if any(type(value) is not int or value < 1 for value in asdict(self).values()):
            raise ValueError("execution limits must be positive integers")

    def payload(self) -> dict[str, int]:
        return asdict(self)

    @classmethod
    def parse(cls, value: object) -> ExecutionLimits:
        if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
            raise ValueError("invalid execution limits")
        return cls(**value)


@dataclass(frozen=True, slots=True)
class ExecutionUsage:
    sandbox_id: str
    command_id: str
    log_bytes: int
    workspace_bytes: int
    artifact_bytes: int
    artifact_count: int
    process_tree_stopped: bool
    stop_reason: str | None = None

    def __post_init__(self) -> None:
        counts = (self.log_bytes, self.workspace_bytes, self.artifact_bytes, self.artifact_count)
        if any(type(value) is not int or value < 0 for value in counts):
            raise ValueError("execution counters must be nonnegative integers")
        if type(self.process_tree_stopped) is not bool:
            raise ValueError("process tree evidence must be explicit")
        if self.stop_reason is not None and self.stop_reason not in STOP_CATEGORIES:
            raise ValueError("invalid trusted stop reason")


def quota_failure(
    limits: ExecutionLimits, usage: ExecutionUsage, *, sandbox_id: str, command_id: str
) -> dict[str, Any] | None:
    if usage.sandbox_id != sandbox_id or usage.command_id != command_id:
        raise ValueError("resource measurement belongs to another execution")
    if usage.stop_reason is not None:
        return {"category": usage.stop_reason, "source": "execution_guard"}
    exceeded = {
        key: {"limit": limit, "observed": getattr(usage, key)}
        for key, limit in limits.payload().items()
        if getattr(usage, key) > limit
    }
    return {"category": "quota_exceeded", "resources": exceeded} if exceeded else None


def observed_status(
    remote: RemoteStatus, *, stop_requested: bool, tree_stopped: bool | None = None
) -> str:
    if remote.running is True:
        return "unknown" if stop_requested else "running"
    if remote.running is False and type(remote.exit_code) is int:
        if tree_stopped is False:
            return "unknown"
        return "completed" if remote.exit_code == 0 and not stop_requested else "failed"
    return "unknown"


STOP_CATEGORIES = frozenset(
    {"quota_exceeded", "quota_inspection_unavailable", "execution_deadline", "execution_cancelled"}
)

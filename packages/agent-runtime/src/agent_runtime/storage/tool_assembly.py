"""Fail-closed binding of tool execution and its terminal maintenance handlers."""

from dataclasses import dataclass

from agent_runtime.storage.execution_supervisor import SqlExecutionSupervisor
from agent_runtime.storage.sandbox_results import SqlSandboxResultCollector
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.tool_host import SqlToolHost


@dataclass(frozen=True, slots=True)
class SqlToolRuntimeAssembly:
    host: SqlToolHost
    capacity: SqlToolCapacity
    supervisor: SqlExecutionSupervisor | None = None
    results: SqlSandboxResultCollector | None = None

    def __post_init__(self) -> None:
        if self.host.capacity is not self.capacity:
            raise ValueError("ToolHost and assembly must share one capacity adapter")
        if self.host.engine is not self.capacity.engine or self.host.schema != self.capacity.schema:
            raise ValueError("ToolHost capacity must use the same database and schema")
        if self.host.requires_external_supervisor and self.supervisor is None:
            raise ValueError("external tools require a capacity-bound execution supervisor")
        if self.supervisor is not None and self.supervisor.capacity is not self.capacity:
            raise ValueError("execution supervisor uses another capacity adapter")
        if self.supervisor is not None and (
            self.supervisor.engine is not self.capacity.engine
            or self.supervisor.schema != self.capacity.schema
        ):
            raise ValueError("execution supervisor uses another database or schema")
        if self.host.requires_sandbox_collector and self.results is None:
            raise ValueError("background tools require a capacity-bound result collector")
        if self.results is not None and self.results.capacity is not self.capacity:
            raise ValueError("sandbox result collector uses another capacity adapter")
        if self.results is not None and (
            self.results.engine is not self.capacity.engine
            or self.results.schema != self.capacity.schema
        ):
            raise ValueError("sandbox result collector uses another database or schema")


__all__ = ["SqlToolRuntimeAssembly"]

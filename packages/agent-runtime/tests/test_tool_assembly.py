"""ToolHost cannot be mounted without its terminal capacity handlers."""

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from agent_runtime.storage.execution_supervisor import SqlExecutionSupervisor
from agent_runtime.storage.tool_assembly import SqlToolRuntimeAssembly
from agent_runtime.storage.tool_capacity import SqlToolCapacity
from agent_runtime.storage.tool_execution_gate import SqlToolExecutionGate
from agent_runtime.storage.tool_host import SqlToolHost
from agent_runtime.storage.uow.capacity import SqlSystemCapacityStore


def test_external_tool_assembly_requires_shared_supervisor_capacity():
    engine = create_async_engine("postgresql+asyncpg://unused:unused@127.0.0.1/unused")
    capacity = SqlToolCapacity(SqlSystemCapacityStore(engine, "agent_runtime"), deployment_id="d")
    other = SqlToolCapacity(SqlSystemCapacityStore(engine, "agent_runtime"), deployment_id="d")

    async def authorize(db, invocation, refs):
        return None

    host = SqlToolHost(
        engine,
        "agent_runtime",
        specs=object(),
        resources=object(),
        schemas=object(),
        authorize=authorize,
        capacity=capacity,
        execution_gate=SqlToolExecutionGate(
            "agent_runtime", approval_required=lambda invocation, spec, resources: False
        ),
        external_tools={object(): object()},
    )

    class Inspector:
        async def inspect(self, key):
            return "unknown"

        async def cancel(self, key):
            return None

    with pytest.raises(ValueError, match="external tools require"):
        SqlToolRuntimeAssembly(host, capacity)
    wrong = SqlExecutionSupervisor(engine, "agent_runtime", inspector=Inspector(), capacity=other)
    with pytest.raises(ValueError, match="another capacity"):
        SqlToolRuntimeAssembly(host, capacity, supervisor=wrong)
    matching = SqlExecutionSupervisor(
        engine, "agent_runtime", inspector=Inspector(), capacity=capacity
    )
    assert SqlToolRuntimeAssembly(host, capacity, supervisor=matching).host is host


def test_background_tool_assembly_requires_shared_result_collector():
    engine = create_async_engine("postgresql+asyncpg://unused:unused@127.0.0.1/unused")
    capacity = SqlToolCapacity(SqlSystemCapacityStore(engine, "agent_runtime"), deployment_id="d")

    async def authorize(db, invocation, refs):
        return None

    class Background:
        def __init__(self, bound_capacity):
            self.capacity = bound_capacity

    host = SqlToolHost(
        engine,
        "agent_runtime",
        specs=object(),
        resources=object(),
        schemas=object(),
        authorize=authorize,
        capacity=capacity,
        execution_gate=SqlToolExecutionGate(
            "agent_runtime", approval_required=lambda invocation, spec, resources: False
        ),
        background_tools={object(): Background(capacity)},
    )
    with pytest.raises(ValueError, match="background tools require"):
        SqlToolRuntimeAssembly(host, capacity)

    class Collector:
        def __init__(self, bound_capacity):
            self.capacity = bound_capacity
            self.engine = engine
            self.schema = "agent_runtime"

    other = SqlToolCapacity(SqlSystemCapacityStore(engine, "agent_runtime"), deployment_id="d")
    with pytest.raises(ValueError, match="dispatch capacity"):
        SqlToolHost(
            engine,
            "agent_runtime",
            specs=object(),
            resources=object(),
            schemas=object(),
            authorize=authorize,
            capacity=capacity,
            execution_gate=host._execution_gate,
            background_tools={object(): Background(other)},
        )
    with pytest.raises(ValueError, match="another capacity"):
        SqlToolRuntimeAssembly(host, capacity, results=Collector(other))
    assert SqlToolRuntimeAssembly(host, capacity, results=Collector(capacity)).host is host

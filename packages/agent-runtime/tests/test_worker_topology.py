"""Role boundaries fail at assembly, before a Worker can claim a row."""

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from agent_runtime.contracts.work_claims import WorkClaimRequest
from agent_runtime.storage.worker_topology import SqlWorkerTopology


def request(lanes, kinds=()):
    return WorkClaimRequest.model_validate(
        {
            "scopeId": "scope-a",
            "worker": {
                "workerId": "role-worker",
                "runtimeRevision": {
                    "key": {"namespace": "builtin", "key": "runtime"},
                    "revision": "1",
                },
                "capabilities": [],
            },
            "leaseSeconds": 30,
            "lanes": lanes,
            "maintenanceKinds": kinds,
        }
    )


def test_worker_roles_require_separate_claim_lanes_and_maintenance_ports():
    engine = create_async_engine("postgresql+asyncpg://unused:unused@127.0.0.1/unused")
    try:
        topology = SqlWorkerTopology(engine, "agent_runtime", scope_id="scope-a")
        main = topology.main(request(["main", "control"]), handlers=object())
        child = topology.child(request(["child"]), handlers=object(), errors=object())
        maintenance = topology.maintenance(
            request(["maintenance"], ["reconcile_execution"]),
            execution=object(), approval_wakes=object(),
        )
        assert len({id(main), id(child), id(maintenance)}) == 3
        assert maintenance._approval_wakes is not None
        with pytest.raises(ValueError, match="scope and lanes"):
            topology.main(request(["child"]), handlers=object())
        with pytest.raises(ValueError, match="scope and lanes"):
            topology.child(request(["main", "control"]), handlers=object(), errors=object())
        with pytest.raises(ValueError, match="supervise executions"):
            topology.maintenance(
                request(["maintenance"], ["rebuild_projection"]),
                execution=object(), approval_wakes=object(),
            )
    finally:
        import asyncio

        asyncio.run(engine.dispose())

"""Control wrappers must preserve unresolved decisions, with bounded lineage."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from agent_runtime.contracts.failure import AgentRuntimeError
from agent_runtime.storage.decisions import SqlDecisionStore


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["resume", "cycle", "another_run", "too_deep"])
async def test_control_lineage_is_bounded_and_run_owned(scenario):
    run_id = uuid4()
    terminal = {"kind": "staged_decision", "runId": str(run_id), "stateVersion": "1"}
    source = {"kind": "control_checkpoint", "runId": str(run_id), "sourceStateRef": "original"}
    blobs = {"original": terminal}
    if scenario == "cycle":
        blobs["original"] = source
    elif scenario == "another_run":
        source["runId"] = str(uuid4())
    elif scenario == "too_deep":
        blobs = {
            f"ref-{i}": {
                "kind": "control_checkpoint",
                "runId": str(run_id),
                "sourceStateRef": f"ref-{i + 1}",
            }
            for i in range(70)
        }
        source["sourceStateRef"] = "ref-0"

    class Store:
        _lease = SimpleNamespace(run_id=run_id)

        async def _read_blob(self, db, ref):
            return blobs[ref]

    if scenario == "resume":
        assert await SqlDecisionStore._unwrap_control(Store(), None, source) == terminal
    else:
        with pytest.raises(AgentRuntimeError) as failure:
            await SqlDecisionStore._unwrap_control(Store(), None, source)
        assert failure.value.failure.code == "VERSION_CONFLICT"

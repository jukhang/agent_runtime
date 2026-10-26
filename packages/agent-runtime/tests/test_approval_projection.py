"""Approval state is identical in live replay and restored snapshots."""

import copy
import json
from importlib.resources import files

import pytest

from agent_runtime.contracts.events import event_adapter
from agent_runtime.projection import SessionProjection


@pytest.mark.parametrize("status", ["approved", "rejected", "expired", "cancelled"])
def test_approval_decision_replay_and_conflict(status):
    fixtures = json.loads(
        files("agent_runtime.contracts").joinpath("fixtures/events.json").read_text()
    )
    required = copy.deepcopy(
        next(e for e in fixtures["events"] if e["kind"] == "approval.required")
    )
    required["sessionSeq"] = "1"
    initial = event_adapter.validate_python(required)
    update = {**required, "kind": "approval.updated", "sessionSeq": "2"}
    resource = {**required["payload"]["approvalRef"], "resourceVersion": "1"}
    update["payload"] = {
        "approvalRef": resource,
        "revision": "1",
        "status": status,
        "expiresAt": required["payload"]["expiresAt"],
        "publicReason": "Decision recorded",
    }
    decision = event_adapter.validate_python(update)
    projection = SessionProjection(initial.session_id)
    projection.apply(initial)
    projection.apply(decision)
    assert not projection.apply(decision)
    approval = next(iter(projection.approvals.values()))
    assert approval.status == status and approval.allowed_decisions == ()
    assert approval.revision == "1"
    changed = copy.deepcopy(update)
    changed["sessionSeq"] = "3"
    changed["payload"]["revision"] = "2"
    changed["payload"]["approvalRef"]["resourceVersion"] = "2"
    with pytest.raises(ValueError, match="terminal"):
        projection.apply(event_adapter.validate_python(changed))
    assert projection.cursor == 2
    late_request = {**required, "sessionSeq": "3"}
    with pytest.raises(ValueError, match="overwrite"):
        projection.apply(event_adapter.validate_python(late_request))
    assert projection.cursor == 2

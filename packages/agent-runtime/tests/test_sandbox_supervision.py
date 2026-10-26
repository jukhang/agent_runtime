"""Counter/identity and process-tree evidence boundaries; not vendor quota proof."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from agent_runtime.sandbox.runtime import RemoteStatus
from agent_runtime.sandbox.supervision import (
    ExecutionLimits,
    ExecutionUsage,
    observed_status,
    quota_failure,
)
from agent_runtime.storage.scheduler import SqlScheduler


@pytest.mark.parametrize(
    "field", ["log_bytes", "workspace_bytes", "artifact_bytes", "artifact_count"]
)
def test_each_limit_and_equal_boundary(field):
    limits = ExecutionLimits(10, 10, 10, 10)
    usage = ExecutionUsage("sandbox", "command", 10, 10, 10, 10, True)
    assert quota_failure(limits, usage, sandbox_id="sandbox", command_id="command") is None
    failure = quota_failure(
        limits, replace(usage, **{field: 11}), sandbox_id="sandbox", command_id="command"
    )
    assert failure == {
        "category": "quota_exceeded",
        "resources": {field: {"limit": 10, "observed": 11}},
    }


@pytest.mark.parametrize("value", [True, 0, -1, 1.2, float("nan"), "10", None])
def test_limits_reject_nonpositive_or_noninteger(value):
    with pytest.raises(ValueError):
        ExecutionLimits(value, 1, 1, 1)


@pytest.mark.parametrize("value", [True, -1, 1.2, float("inf"), "0", None])
def test_usage_cannot_hide_unknown_counters(value):
    with pytest.raises(ValueError):
        ExecutionUsage("sandbox", "command", value, 0, 0, 0, True)


@pytest.mark.parametrize("field", ["sandbox_id", "command_id"])
def test_measurements_are_execution_bound(field):
    usage = ExecutionUsage("sandbox", "command", 0, 0, 0, 0, True)
    with pytest.raises(ValueError):
        quota_failure(
            ExecutionLimits(1, 1, 1, 1),
            replace(usage, **{field: "other"}),
            sandbox_id="sandbox",
            command_id="command",
        )


@pytest.mark.parametrize(
    "remote,stop,tree,expected",
    [
        (RemoteStatus(True, None), False, False, "running"),
        (RemoteStatus(True, None), True, False, "unknown"),
        (RemoteStatus(False, 0), True, True, "failed"),
        (RemoteStatus(False, 0), False, True, "completed"),
        (RemoteStatus(False, 7), False, True, "failed"),
        (RemoteStatus(False, 0), True, False, "unknown"),
        (RemoteStatus(False, 7), False, False, "unknown"),
        (RemoteStatus(False, True), True, True, "unknown"),
        (RemoteStatus(None, 0), True, True, "unknown"),
    ],
)
def test_stop_is_not_success_and_exit_is_not_tree_evidence(remote, stop, tree, expected):
    assert observed_status(remote, stop_requested=stop, tree_stopped=tree) == expected


def test_limits_round_trip_and_unknown_fields_rejected():
    limits = ExecutionLimits(10, 20, 30, 40)
    assert ExecutionLimits.parse(limits.payload()) == limits
    for bad in ({}, None, {**limits.payload(), "extra": 1}):
        with pytest.raises(ValueError):
            ExecutionLimits.parse(bad)


@pytest.mark.parametrize("control", [None, "pause", "cancel"])
@pytest.mark.parametrize("kind", ["advance_run", "reconcile_execution", "domain_job"])
def test_control_does_not_steal_maintenance_lane(control, kind):
    candidate = SimpleNamespace(
        kind=kind,
        desired_control=control,
        parent_run_id=None,
        payload_ref="context-dispatch",
        subject_ref={"resourceType": "execution"},
    )
    expected = "maintenance" if kind != "advance_run" else "control" if control else "main"
    assert SqlScheduler._lane(candidate) == expected

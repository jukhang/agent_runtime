"""A4: the loop's authoritative run snapshot mirrors the state-write guard."""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent_runtime.contracts.loop import RunState
from agent_runtime.contracts.state import DesiredControl, RunStatus

BASE = {
    "scopeId": "scope",
    "sessionId": str(uuid4()),
    "runId": str(uuid4()),
    "stateVersion": "3",
    "checkpointRef": "checkpoint-1",
    "agent": {"key": {"namespace": "builtin", "key": "agent"}, "revision": "1"},
    "cancelVersion": "0",
    "pendingInputSeq": "2",
    "appliedInputSeq": "1",
}


def state(**changes):
    return RunState.model_validate({**BASE, **changes})


@pytest.mark.parametrize(
    ("status", "control"),
    [
        ("queued", None),
        ("queued", "pause"),
        ("running", None),
        ("running", "pause"),
        ("waiting_tool", "pause"),
        ("waiting_user", "pause"),
        ("paused", None),
        ("cancelling", "cancel"),
        ("completed", None),
        ("failed", None),
        ("cancelled", None),
    ],
)
def test_legal_control_lifecycles(status, control):
    parsed = state(status=status, desiredControl=control)
    assert parsed.status == RunStatus(status)
    assert parsed.desired_control == (DesiredControl(control) if control else None)


@pytest.mark.parametrize(
    ("status", "control"),
    [
        # cancelling without the cancel command, or a cancel command on an active run
        ("cancelling", None),
        ("running", "cancel"),
        ("queued", "cancel"),
        ("waiting_tool", "cancel"),
        # terminal states never hold a pending control
        ("completed", "cancel"),
        ("failed", "pause"),
        ("cancelled", "cancel"),
        # pause is consumed when it is applied
        ("paused", "pause"),
    ],
)
def test_illegal_control_lifecycles_rejected(status, control):
    with pytest.raises(ValidationError):
        state(status=status, desiredControl=control)


def test_watermark_order_is_still_checked():
    with pytest.raises(ValidationError):
        state(pendingInputSeq="1", appliedInputSeq="2")

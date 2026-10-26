"""Execution maintenance retry recognition, independent of external services."""

from dataclasses import replace
from uuid import uuid4

import pytest

pytest.importorskip("sqlalchemy")

from agent_runtime.storage.recovery import _external_supervision  # noqa: E402
from agent_runtime.storage.scheduler import SqlScheduler, _Candidate  # noqa: E402


def test_cancel_convergence_is_control_but_sandbox_supervision_stays_maintenance():
    candidate = _Candidate(
        work_id=uuid4(),
        run_id=uuid4(),
        session_id=uuid4(),
        parent_run_id=None,
        kind="reconcile_execution",
        subject_ref={"kind": "cancel", "commandId": str(uuid4())},
        payload_ref="cancel-convergence",
        handler_namespace=None,
        handler_key=None,
        handler_revision=None,
        desired_control="cancel",
    )
    assert SqlScheduler._lane(candidate) == "control"
    assert (
        SqlScheduler._lane(
            replace(
                candidate, payload_ref="context-dispatch", subject_ref={"resourceType": "execution"}
            )
        )
        == "maintenance"
    )
    assert SqlScheduler._lane(replace(candidate, desired_control=None)) == "maintenance"
    assert SqlScheduler._lane(replace(candidate, subject_ref={"kind": "other"})) == "maintenance"


@pytest.mark.parametrize("payload", ["execution:exec-1", "context-sealed-dispatch"])
def test_legacy_and_sandbox_dispatch_keep_supervision_obligation(payload):
    assert _external_supervision(
        {
            "kind": "reconcile_execution",
            "subject_ref": {"resourceType": "execution", "resourceId": "exec-1"},
            "payload_ref": payload,
        }
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"kind": "advance_run"},
        {"subject_ref": {"resourceType": "run", "resourceId": "exec-1"}},
        {"subject_ref": None},
        {"payload_ref": "execution:other"},
        {"payload_ref": "arbitrary"},
        {"payload_ref": None},
    ],
)
def test_non_execution_or_unrecognized_work_does_not_gain_unbounded_retry(changes):
    assert not _external_supervision(
        {
            "kind": "reconcile_execution",
            "subject_ref": {"resourceType": "execution", "resourceId": "exec-1"},
            "payload_ref": "context-sealed-dispatch",
            **changes,
        }
    )

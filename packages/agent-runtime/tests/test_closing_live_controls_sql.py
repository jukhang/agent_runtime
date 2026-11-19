"""Real Qwen controls and explicit streamed timeout injection, with durable SQL."""

import os

import pytest
from test_storage_uow import _exercise_main_worker_answer, count
from test_storage_uow import schema as schema
from test_storage_uow import world as world

pytestmark = pytest.mark.skipif(
    not os.getenv("AGENT_RUNTIME_TEST_DSN") or os.getenv("AGENT_RUNTIME_LIVE_MODEL") != "1",
    reason="requires PostgreSQL and explicit real-model opt-in",
)


@pytest.mark.parametrize("case", ["cancel-unused", "cancel-closing", "timeout-closing"])
def test_live_controls_settle_only_their_actual_closing_purpose(world, case):
    closing = not case.endswith("unused")
    _exercise_main_worker_answer(
        world,
        live=True,
        finalization="closing" if closing else "unused",
        interrupt="cancel" if case.startswith("cancel") else None,
        live_fault="timeout" if case.startswith("timeout") else None,
    )
    assert count(world, "budget_entry", "kind='reserve'") == 1
    assert count(world, "budget_entry", "kind='adjust'") == (1 if closing else 0)
    assert count(world, "budget_entry", "kind='settle'") == (1 if closing else 0)
    assert count(world, "budget_entry", "kind='release'") == (0 if closing else 1)

"""Real TCP refusal through the root Worker and terminal UoW; no mock transport."""

import os
import socket

import pytest
from test_storage_uow import _exercise_main_worker_answer, count
from test_storage_uow import schema as schema
from test_storage_uow import world as world

pytestmark = pytest.mark.skipif(
    not os.getenv("AGENT_RUNTIME_TEST_DSN"), reason="requires PostgreSQL"
)


def test_closing_network_refusal_is_terminal_and_does_not_retry_or_refund_unknown(
    world, monkeypatch
):
    with socket.socket() as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        port = unavailable.getsockname()[1]
        # macOS may silently drop SYNs for a bound, non-listening socket. Close it
        # so this scenario exercises TCP refusal, not the distinct timeout branch.
        unavailable.close()
        monkeypatch.setenv("BASE_URL", f"http://127.0.0.1:{port}/v1")
        monkeypatch.setenv("MODEL_NAME", "unreachable-probe")
        _exercise_main_worker_answer(
            world, live=True, live_fault="unreachable", finalization="closing"
        )
    assert count(world, "budget_entry", "kind='reserve'") == 1
    assert count(world, "budget_entry", "kind='adjust'") == 1
    assert count(world, "budget_entry", "kind='settle'") == 1
    assert count(world, "budget_entry", "kind='release'") == 0

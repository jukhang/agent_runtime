"""Real Qwen in an independent root Worker-handler process; explicit SQL fixture."""

import json
import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import text
from test_storage_uow import engine_for, run

pytestmark = pytest.mark.skipif(
    not os.getenv("AGENT_RUNTIME_TEST_DSN") or os.getenv("AGENT_RUNTIME_LIVE_MODEL") != "1",
    reason="requires PostgreSQL and explicit real-model opt-in",
)

WORKER = """
import sys
from test_storage_uow import migrate,run,_seed,_exercise_main_worker_answer
schema=sys.argv[1]
assert migrate('upgrade',schema).status=='applied'
world=run(lambda:_seed(schema))
_exercise_main_worker_answer(world,live=True,finalization='closing')
print('real-closing-worker-completed',flush=True)
"""


def test_real_provider_closing_finishes_once_in_independent_root_worker(tmp_path):
    schema = "p03_closing_live_" + uuid4().hex[:12]
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join([str(Path(__file__).parent), env.get("PYTHONPATH", "")])
    try:
        with (tmp_path / "worker.log").open("w") as log:
            result = subprocess.run(
                [sys.executable, "-c", WORKER, schema],
                env=env,
                stdout=log,
                stderr=log,
                timeout=360,
                check=False,
            )
        assert result.returncode == 0, (tmp_path / "worker.log").read_text()
        assert "real-closing-worker-completed" in (tmp_path / "worker.log").read_text()

        async def verify():
            engine = engine_for(schema)
            try:
                async with engine.connect() as db:
                    rows = (
                        await db.execute(
                            text("SELECT kind,count(*) FROM budget_entry GROUP BY kind")
                        )
                    ).all()
                    assert dict(rows) == {"reserve": 1, "adjust": 1, "settle": 1}
                    assert (
                        await db.execute(
                            text(
                                "SELECT count(*) FROM model_attempt WHERE status='completed' "
                                "AND usage_status='complete'"
                            )
                        )
                    ).scalar_one() == 1
                    assert (
                        await db.execute(
                            text("SELECT count(*) FROM work_item WHERE status='leased'")
                        )
                    ).scalar_one() == 0
                    held = (
                        await db.execute(text("SELECT reserved FROM budget_allocation"))
                    ).scalar_one()
                    assert all(value == 0 for value in held.values())
                print(
                    json.dumps(
                        {
                            "schema": schema,
                            "fixture": True,
                            "realProvider": True,
                            "independentRootWorkerHandler": True,
                            "modelCalls": 1,
                            "doubleCharge": False,
                        }
                    )
                )
            finally:
                await engine.dispose()

        run(verify)
    finally:

        async def drop():
            engine = engine_for(schema)
            try:
                async with engine.begin() as db:
                    await db.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            finally:
                await engine.dispose()

        run(drop)

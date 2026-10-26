"""Actual create-after-ack SIGKILLs, recovered by the independent inventory process."""

import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine
from test_live import deployment, submit, wait_result  # noqa: F401

pytestmark = pytest.mark.skipif(
    os.environ.get("CENTN_ECOSYSTEM_LIVE") != "1", reason="Real ecosystem services required"
)


@pytest.fixture(params=["create_ack", "recorded_ack"])
def crash_injection(request, monkeypatch, tmp_path):
    original = subprocess.Popen
    script = Path(__file__).resolve().parents[5] / "scripts/runtime_ecosystem_crash_host.py"
    state = {"stage": request.param, "marker": tmp_path / "create-ack.json", "armed": True}

    def launch(command, *args, **kwargs):
        role = command[3] if command[1:3] == ["-m", "centn_example"] else None
        if role == "worker" and state["armed"]:
            command = [
                sys.executable,
                str(script),
                "--fault-stage",
                state["stage"],
                "--fault-marker",
                str(state["marker"]),
                *command[3:],
            ]
        child = original(command, *args, **kwargs)
        if role == "api":
            state["api"] = child
        return child

    monkeypatch.setattr(subprocess, "Popen", launch)
    return state


@pytest.fixture
def crash_deployment(crash_injection, request):
    # Resolve only after installing the fault bootstrap; the underlying deployment
    # is the unchanged standalone API/Worker/maintenance fixture.
    return request.getfixturevalue("deployment")


def test_sigkill_allocation_inventory_before_execution(crash_injection, crash_deployment):
    from centn_example.ecosystem_config import EcosystemSettings
    from centn_example.resource_migrate import resource_schema
    from opensandbox.exceptions import SandboxApiException
    from opensandbox.manager import SandboxManager

    target = crash_deployment
    receipt = submit(
        target,
        "请调用 read-metrics 获取车间最新数据，再将返回的 structuredContent 完整原样序列化为 "
        "一个 JSON 字符串，放入 chart-report 的 argv 数组。argv 必须是只含一个字符串的数组，"
        "不是对象，也不是字符串本身。运行已安装的 Git Skill 生成图表和报告，不用 save-report。",
    )
    marker = crash_injection["marker"]
    deadline = time.monotonic() + 240
    while not marker.exists() and time.monotonic() < deadline:
        assert target["worker"].poll() is None, "Read the isolated Worker log"
        time.sleep(0.25)
    assert marker.exists(), "The actual remote create did not reach the injected window"
    created = json.loads(marker.read_text())
    assert created["pid"] == target["worker"].pid
    config = EcosystemSettings.from_env()

    async def remote_info():
        from opensandbox.config import ConnectionConfig

        async with await SandboxManager.create(
            ConnectionConfig(
                domain=config.sandbox_domain,
                api_key=config.sandbox_key,
            )
        ) as manager:
            try:
                return await manager.get_sandbox_info(created["sandboxId"])
            except SandboxApiException as exc:
                if exc.status_code == 404:
                    return None
                raise

    remote = asyncio.run(remote_info())
    assert remote is not None and remote.expires_at > datetime.now(UTC)
    assert all(remote.metadata.get(k) == v for k, v in created["metadata"].items())
    target["worker"].kill()
    target["worker"].wait(10)
    assert target["worker"].returncode == -9
    crash_injection["api"].terminate()
    crash_injection["api"].wait(20)
    assert target["maintenance"].poll() is None

    async def sql_state():
        engine = create_async_engine(target["dsn"])
        try:
            async with engine.connect() as db:
                allocations = [
                    dict(row)
                    for row in (
                        await db.execute(
                            sa.text(
                                "SELECT * FROM "
                                f'"{resource_schema(target["schema"])}".sandbox_allocation'
                            )
                        )
                    ).mappings()
                ]
                executions = (
                    await db.execute(
                        sa.text(f'SELECT count(*) FROM "{target["schema"]}".execution')
                    )
                ).scalar_one()
                return allocations, executions
        finally:
            await engine.dispose()

    started = time.monotonic()
    while time.monotonic() - started < 150:
        assert target["maintenance"].poll() is None
        allocations, executions = asyncio.run(sql_state())
        if asyncio.run(remote_info()) is None and any(
            row["status"] == "cleaned" for row in allocations
        ):
            break
        time.sleep(1)
    else:
        pytest.fail("Independent inventory did not converge before the bounded crash deadline")
    assert datetime.now(UTC) < remote.expires_at, "TTL expiry is not inventory acceptance"
    assert len(allocations) == 1 and allocations[0]["status"] == "cleaned"
    assert allocations[0]["sandbox_id"] == created["sandboxId"] and executions == 0
    crash_injection["armed"] = False
    target["launch"]("api")
    target["launch"]("worker")
    ready = time.monotonic() + 90
    while time.monotonic() < ready:
        try:
            if target["client"].get("/api/config").status_code == 200:
                break
        except httpx.TransportError:
            pass
        time.sleep(0.5)
    else:
        pytest.fail("Restarted API did not become ready")
    result = wait_result(target, receipt, timeout=180)
    assert result["status"] in {"failed", "cancelled"}
    assert result.get("failure") and result["failure"].get("recoveryOptions")
    after, executions = asyncio.run(sql_state())
    assert len(after) == 1 and executions == 0
    evidence = Path(os.environ["CENTN_ECOSYSTEM_EVIDENCE"])
    (evidence / f"allocation-crash-{crash_injection['stage']}.json").write_text(
        json.dumps(
            {
                "stage": crash_injection["stage"],
                "runId": receipt["runId"],
                "remote": created,
                "allocation": after,
                "result": result,
                "secondsToClean": time.monotonic() - started,
                "checks": {
                    "realSigkill": True,
                    "apiStoppedDuringCleanup": True,
                    "independentInventory": True,
                    "cleanedBeforeTtl": True,
                    "noExecutionOrRedispatch": True,
                    "terminalAfterRestart": True,
                },
            },
            default=str,
            ensure_ascii=False,
            indent=2,
        )
    )

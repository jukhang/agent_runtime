"""Kernel profile and trusted helper command boundaries without Docker."""

import json
import shlex
from dataclasses import replace
from types import SimpleNamespace

import pytest
from test_sandbox_runtime import request

from agent_runtime.sandbox import opensandbox as module
from agent_runtime.sandbox.docker_profile import docker_limits
from agent_runtime.sandbox.opensandbox import OpenSandboxBackend, guard_command
from agent_runtime.sandbox.supervision import ExecutionLimits, ExecutionUsage, quota_failure

LIMITS = ExecutionLimits(65536, 1048576, 1048576, 8)


def test_profile_is_readonly_and_kernel_bounded():
    profile = docker_limits(LIMITS)
    assert profile["read_only"] is True
    assert "size=1048576" in profile["tmpfs"]["/workspace"]
    assert "nr_inodes=9" in profile["tmpfs"]["/artifacts"]
    assert "mode=0700" in profile["tmpfs"]["/run/centn"]
    assert not {"privileged", "binds", "cap_add"} & profile.keys()


@pytest.mark.parametrize(
    "limits", [ExecutionLimits(2000000, 4096, 4096, 1), ExecutionLimits(1, 1, 1, 1)]
)
def test_invalid_mount_or_log_limits_refused(limits):
    with pytest.raises(ValueError):
        docker_limits(limits)


def test_model_command_is_data_not_root_script():
    malicious = "'; id; $(touch /root/x); #"
    args = shlex.split(guard_command("start", {"command": malicious}))
    assert args[:3] == ["python", "-I", "-c"]
    assert malicious not in args[3]
    assert json.loads(args[5])["command"] == malicious


def test_guard_reason_survives_bounded_output_counter():
    usage = ExecutionUsage("sandbox", "cmd", LIMITS.log_bytes, 0, 0, 0, True, "quota_exceeded")
    assert (
        quota_failure(LIMITS, usage, sandbox_id="sandbox", command_id="cmd")["category"]
        == "quota_exceeded"
    )


@pytest.mark.asyncio
async def test_limited_start_uses_trusted_root_wrapper_and_deadline_grace(monkeypatch):
    async def run(command, *, opts):
        assert opts.uid == opts.gid == 0
        assert opts.timeout.total_seconds() == 8
        args = shlex.split(command)
        payload = json.loads(args[5])
        assert payload["timeout"] == 3 and payload["limits"] == LIMITS.payload()
        assert payload["command"] == "echo user"
        return SimpleNamespace(id="cmd")

    monkeypatch.setattr(
        module,
        "import_module",
        lambda _: SimpleNamespace(
            RunCommandOpts=lambda **kw: SimpleNamespace(**kw),
        ),
    )
    backend = OpenSandboxBackend(SimpleNamespace(id="sandbox", commands=SimpleNamespace(run=run)))

    async def bind(mode, payload):
        assert mode == "bind" and payload["command_id"] == "cmd"
        return "bound"

    monkeypatch.setattr(backend, "_guard", bind)
    req = replace(request(), command="echo user", working_directory="/workspace", limits=LIMITS)
    assert await backend.start(req, 3) == "cmd"
    with pytest.raises(ValueError):
        await backend.start(replace(req, working_directory="/tmp"), 3)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "executionId", "remoteExecutionId", "limits"])
async def test_inspector_verifies_all_persisted_bindings(monkeypatch, changed):
    payload = {
        "sandboxId": "sandbox",
        "executionId": "execution",
        "remoteExecutionId": "cmd",
        "limits": LIMITS.payload(),
    }
    state = {
        "execution_id": "execution",
        "command_id": "cmd",
        "limits": LIMITS.payload(),
        "log_bytes": 0,
        "workspace_bytes": 0,
        "artifact_bytes": 0,
        "artifact_count": 0,
        "process_tree_stopped": True,
    }
    backend = OpenSandboxBackend(SimpleNamespace(id="sandbox"))

    async def inspect(mode):
        assert mode == "inspect"
        return json.dumps(state)

    monkeypatch.setattr(backend, "_guard", inspect)
    if changed is not None:
        payload[changed] = (
            ExecutionLimits(1, 4096, 4096, 1).payload() if changed == "limits" else "other"
        )
        with pytest.raises(ValueError):
            await backend.inspect_resources(payload)
    else:
        assert (await backend.inspect_resources(payload)).process_tree_stopped is True

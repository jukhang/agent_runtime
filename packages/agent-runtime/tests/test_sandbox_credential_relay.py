"""Guard filesystem safety and Relay binding; no live credentials in tests."""

import base64
import json
import os
import sys
from uuid import uuid4

import pytest
from test_credential_ingress import filtered, proxy_request

from agent_runtime.sandbox import guard
from agent_runtime.sandbox.credential_relay import SandboxCredentialRelay


@pytest.fixture
def channel(tmp_path, monkeypatch):
    root, workspace = tmp_path / "control", tmp_path / "workspace"
    root.mkdir()
    workspace.mkdir()
    identity = uuid4()
    (root / "binding").write_text(json.dumps({"execution_id": str(identity)}))
    monkeypatch.setattr(guard, "ROOT", root)
    monkeypatch.setattr(guard, "UID", os.getuid())
    real_open = os.open

    def opened(path, *args, **kwargs):
        return real_open(str(workspace) if path == "/workspace" else path, *args, **kwargs)

    monkeypatch.setattr(guard.os, "open", opened)

    def call(mode, **fields):
        monkeypatch.setattr(
            sys, "argv", ["guard", mode, json.dumps({"execution_id": str(identity), **fields})]
        )
        guard.main()

    return workspace, call


@pytest.mark.parametrize("kind", ["symlink", "fifo", "directory", "oversized"])
def test_guard_rejects_unsafe_request_files(channel, kind):
    workspace, call = channel
    path = workspace / "credential-request.json"
    if kind == "symlink":
        path.symlink_to("/etc/passwd")
    elif kind == "fifo":
        os.mkfifo(path)
    elif kind == "directory":
        path.mkdir()
    else:
        path.write_bytes(b"x" * 32769)
    with pytest.raises((OSError, ValueError)):
        call("credential_read")


def test_guard_response_is_create_only_and_path_is_not_caller_controlled(channel):
    workspace, call = channel
    identity = uuid4()
    args = {"request_id": str(identity), "response": base64.b64encode(b'{"ok":true}').decode()}
    call("credential_write", **args)
    assert json.loads((workspace / f"credential-response-{identity}.json").read_bytes()) == {
        "ok": True
    }
    with pytest.raises(FileExistsError):
        call("credential_write", **args)
    with pytest.raises(ValueError):
        call("credential_write", **{**args, "request_id": "../escape"})
    assert not list(workspace.glob(".credential-*"))


@pytest.mark.asyncio
async def test_relay_rejects_workload_grant_substitution_before_network():
    expected, supplied = proxy_request(), proxy_request()

    class Backend:
        async def _guard(self, mode, payload):
            return base64.b64encode(supplied.model_dump_json(by_alias=True).encode()).decode()

    class Client:
        async def proxy(self, request):
            raise AssertionError("must not reach network")

    with pytest.raises(ValueError, match="unauthorized grant"):
        await SandboxCredentialRelay(Backend(), Client()).exchange(
            expected.grant.execution_id, expected.grant
        )


@pytest.mark.asyncio
async def test_relay_writes_only_filtered_result_reference():
    request = proxy_request()
    writes = []

    class Backend:
        async def _guard(self, mode, payload):
            if mode == "credential_read":
                return base64.b64encode(request.model_dump_json(by_alias=True).encode()).decode()
            writes.append(payload)
            return "published"

    class Client:
        async def proxy(self, actual):
            assert actual == request
            return filtered(request)

    result = await SandboxCredentialRelay(Backend(), Client()).exchange(
        request.grant.execution_id, request.grant
    )
    assert json.loads(base64.b64decode(writes[0]["response"]))["bodyRef"] == result.body_ref

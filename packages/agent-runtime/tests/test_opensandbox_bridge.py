"""Pinned SDK bridge edge cases without importing the optional SDK."""

import asyncio
from types import SimpleNamespace

import pytest
from test_sandbox_runtime import request

from agent_runtime.sandbox import opensandbox as module
from agent_runtime.sandbox.opensandbox import OpenSandboxBackend


@pytest.mark.parametrize("uid,gid", ((0, 1000), (1000, 0), (-1, 1000)))
def test_root_identity_is_rejected(uid, gid):
    with pytest.raises(ValueError):
        OpenSandboxBackend(SimpleNamespace(), uid=uid, gid=gid)


def test_start_is_background_nonroot_bounded_and_without_env(monkeypatch):
    async def scenario():
        async def run(command, *, opts):
            assert opts.background and opts.uid == opts.gid == 1000
            assert opts.timeout.total_seconds() == 3 and opts.working_directory == "/tmp"
            assert not hasattr(opts, "envs")
            return SimpleNamespace(id="command")

        monkeypatch.setattr(
            module,
            "import_module",
            lambda _: SimpleNamespace(RunCommandOpts=lambda **kw: SimpleNamespace(**kw)),
        )
        backend = OpenSandboxBackend(
            SimpleNamespace(id="sandbox", commands=SimpleNamespace(run=run))
        )
        assert await backend.start(request(), 3) == "command"

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ("present", "not_found", "transport", "untyped_404"))
def test_reconnect_distinguishes_deleted_from_unreachable(monkeypatch, mode):
    class ApiError(Exception):
        status_code = 404

    closed = []

    async def close():
        closed.append(True)

    async def connect(sandbox, connection):
        assert sandbox == "sandbox"
        if mode == "not_found":
            raise ApiError()
        if mode == "transport":
            raise OSError("connection lost")
        if mode == "untyped_404":
            raise ValueError("404")
        return SimpleNamespace(aclose=close)

    monkeypatch.setattr(module, "connect_opensandbox", connect)
    monkeypatch.setattr(
        module, "import_module", lambda _: SimpleNamespace(SandboxApiException=ApiError)
    )
    if mode in {"transport", "untyped_404"}:
        with pytest.raises((OSError, ValueError)):
            asyncio.run(module.confirm_opensandbox_deleted("sandbox", object()))
    else:
        assert asyncio.run(module.confirm_opensandbox_deleted("sandbox", object())) is (
            mode == "not_found"
        )
    assert closed == ([True] if mode == "present" else [])


@pytest.mark.parametrize("mode", ("accepted", "not_found", "transport", "untyped_404"))
def test_cleanup_needs_typed_deletion_evidence(monkeypatch, mode):
    class ApiError(Exception):
        status_code = 404

    async def scenario():
        async def kill():
            pass

        async def info():
            if mode == "not_found":
                raise ApiError()
            if mode == "transport":
                raise OSError("unreachable")
            if mode == "untyped_404":
                raise ValueError("404")
            return SimpleNamespace()

        monkeypatch.setattr(
            module, "import_module", lambda _: SimpleNamespace(SandboxApiException=ApiError)
        )
        backend = OpenSandboxBackend(SimpleNamespace(kill=kill, get_info=info))
        if mode in {"transport", "untyped_404"}:
            with pytest.raises((OSError, ValueError)):
                await backend.destroy()
        else:
            assert await backend.destroy() is (mode == "not_found")

    asyncio.run(scenario())

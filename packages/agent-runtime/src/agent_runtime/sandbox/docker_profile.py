"""Pinned, dedicated OpenSandbox Docker pool with kernel-enforced writable mounts."""

from importlib import import_module
from importlib.metadata import version
from typing import Any

from agent_runtime.sandbox.supervision import ExecutionLimits


def docker_limits(limits: ExecutionLimits) -> dict[str, Any]:
    if limits.workspace_bytes % 4096 or limits.artifact_bytes % 4096:
        raise ValueError("tmpfs limits must be page aligned")
    if limits.log_bytes > 1024 * 1024:
        raise ValueError("guarded log ceiling is 1 MiB")
    return {
        "read_only": True,
        "tmpfs": {
            "/workspace": f"rw,nosuid,nodev,size={limits.workspace_bytes},"
            "uid=1000,gid=1000,mode=0700",
            "/artifacts": f"rw,nosuid,nodev,size={limits.artifact_bytes},"
            f"nr_inodes={limits.artifact_count + 1},uid=1000,gid=1000,mode=0700",
            "/run/centn": "rw,nosuid,nodev,noexec,size=2097152,mode=0700",
            "/tmp": "rw,nosuid,nodev,size=16777216,mode=1777",
        },
        "shm_size": 1024 * 1024,
    }


def install_docker_profile(limits: ExecutionLimits) -> None:
    """Server-process-only extension. Never install in the API/model process.

    The vendor has no tmpfs pool setting in this pinned release. Keep this small
    integration seam explicit and refuse upgrades instead of editing vendor files.
    """
    if version("opensandbox-server") != "0.2.3":
        raise ValueError("Docker profile requires OpenSandbox server 0.2.3")
    profile = docker_limits(limits)
    service = import_module(
        "opensandbox_server.services.docker.docker_service"
    ).DockerSandboxService
    original = service._base_host_config_kwargs
    if getattr(original, "centn_profile", False):
        raise ValueError("Docker profile already installed")

    def configured(self: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
        result: dict[str, Any] = original(self, *args, **kwargs)
        result.update(profile)
        result["security_opt"] = [*result.get("security_opt", []), "no-new-privileges:true"]
        return result

    configured.centn_profile = True  # type: ignore[attr-defined]
    service._base_host_config_kwargs = configured
    ensure = service._ensure_directory

    def mounted_directory(self: Any, container: Any, path: str, sandbox_id: Any = None) -> None:
        if path.rstrip("/") == "/opt/opensandbox":
            # Docker creates the vendor runtime volume. Vendor tar-to-"/"
            # is incompatible with a read-only root; never relax the root instead.
            return
        ensure(self, container, path, sandbox_id)

    service._ensure_directory = mounted_directory

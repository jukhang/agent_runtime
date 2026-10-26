"""Trusted Linux helper sent only by the host; user code always runs as uid 1000.

Runs with Python isolated mode in the sandbox. No SDK or host credentials needed.
One workload per dedicated sandbox. All writable counters come from kernel state.
"""

import base64
import ctypes
import json
import os
import platform
import selectors
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

ROOT = Path("/run/centn")
UID = 1000


def restrict_network() -> None:
    """Inherited syscall filter: user code must not call the local execd root API."""
    machine = platform.machine()
    if machine == "x86_64":
        arch, socket_nr, pair_nr = 0xC000003E, 41, 53
    elif machine == "aarch64":
        arch, socket_nr, pair_nr = 0xC00000B7, 198, 199
    else:
        raise RuntimeError("unsupported guarded execution architecture")

    class Filter(ctypes.Structure):
        _fields_ = [
            ("code", ctypes.c_ushort),
            ("jt", ctypes.c_ubyte),
            ("jf", ctypes.c_ubyte),
            ("k", ctypes.c_uint),
        ]

    class Program(ctypes.Structure):
        _fields_ = [("len", ctypes.c_ushort), ("filter", ctypes.POINTER(Filter))]

    # Validate ABI, reject x32/compat and io_uring socket creation, then allow
    # ordinary computation/files/processes. No existing network fds are inherited.
    instructions = [
        Filter(0x20, 0, 0, 4),
        Filter(0x15, 1, 0, arch),
        Filter(0x06, 0, 0, 0x80000000),
        Filter(0x20, 0, 0, 0),
        Filter(0x35, 0, 1, 0x40000000),
        Filter(0x06, 0, 0, 0x00050001),
    ]
    for number in (socket_nr, pair_nr, 425):
        instructions.extend([Filter(0x15, 0, 1, number), Filter(0x06, 0, 0, 0x00050001)])
    instructions.append(Filter(0x06, 0, 0, 0x7FFF0000))
    filters = (Filter * len(instructions))(*instructions)
    program = Program(len(instructions), filters)
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(38, 1, 0, 0, 0) != 0 or libc.prctl(22, 2, ctypes.byref(program), 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "failed to install guarded syscall policy")


def alive() -> list[int]:
    found = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            if (
                entry.stat().st_uid == UID
                and entry.joinpath("stat").read_text().rsplit(") ", 1)[1][0] != "Z"
            ):
                found.append(int(entry.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
    return found


def stop() -> None:
    # pidfds prevent PID reuse from redirecting a signal to an unrelated process.
    until = time.monotonic() + 3
    while time.monotonic() < until:
        pids = alive()
        if not pids:
            return
        for pid in pids:
            try:
                fd = getattr(os, "pidfd_open")(pid)  # noqa: B009 -- Linux-only binding.
                try:
                    if Path(f"/proc/{pid}").stat().st_uid == UID:
                        getattr(signal, "pidfd_send_signal")(fd, signal.SIGKILL)  # noqa: B009
                finally:
                    os.close(fd)
            except (FileNotFoundError, ProcessLookupError):
                pass
        time.sleep(0.01)
    raise RuntimeError("process tree stop unconfirmed")


def usage() -> dict[str, int | bool]:
    workspace = os.statvfs("/workspace")
    artifacts = os.statvfs("/artifacts")
    return {
        "workspace_bytes": (workspace.f_blocks - workspace.f_bfree) * workspace.f_frsize,
        "artifact_bytes": (artifacts.f_blocks - artifacts.f_bfree) * artifacts.f_frsize,
        "artifact_count": artifacts.f_files - artifacts.f_ffree - 1,
        "process_tree_stopped": not alive(),
    }


def verify(limits: dict[str, int]) -> None:
    if os.geteuid() != 0 or not os.statvfs("/").f_flag & os.ST_RDONLY:
        raise ValueError("guard requires root control and read-only workload root")
    for path, key in (("/workspace", "workspace_bytes"), ("/artifacts", "artifact_bytes")):
        stat = os.statvfs(path)
        if stat.f_blocks * stat.f_frsize != limits[key]:
            raise ValueError("kernel capacity does not match authorized limit")
    artifacts = os.statvfs("/artifacts")
    if artifacts.f_files != limits["artifact_count"] + 1:
        raise ValueError("kernel inode ceiling does not match authorized limit")
    if not 0 < limits["log_bytes"] <= 1024 * 1024:
        raise ValueError("invalid log bound")
    if alive():
        raise ValueError("dedicated sandbox already has an executing user")


def execute(request: dict[str, Any]) -> int:
    verify(request["limits"])
    # O_EXCL makes an ambiguous/repeated start fail closed, including after reconnect.
    ROOT.mkdir(mode=0o700, exist_ok=True)
    with ROOT.joinpath(".binding").open("x") as binding:
        json.dump(request, binding)
    os.link(ROOT / ".binding", ROOT / "binding")
    ROOT.joinpath(".binding").unlink()
    written = 0
    reason = None
    deadline = time.monotonic() + request["timeout"]
    child = subprocess.Popen(
        ["/bin/sh", "-c", request["command"]],
        cwd="/workspace",
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        user=UID,
        group=UID,
        extra_groups=(),
        preexec_fn=restrict_network,
        env={
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "HOME": "/workspace",
            "TMPDIR": "/tmp",
            "PYTHONDONTWRITEBYTECODE": "1",
        },
    )
    assert child.stdout is not None
    selector = selectors.DefaultSelector()
    selector.register(child.stdout, selectors.EVENT_READ)
    try:
        with ROOT.joinpath("output").open("xb") as output:
            while True:
                if time.monotonic() >= deadline:
                    reason = "execution_deadline"
                    break
                for key, _ in selector.select(0.02):
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    room = request["limits"]["log_bytes"] - written
                    output.write(chunk[:room])
                    written += min(room, len(chunk))
                    output.flush()
                    if len(chunk) > room:
                        reason = "quota_exceeded"
                        break
                if reason or (child.poll() is not None and not selector.get_map()):
                    break
            # Natural parent exit is not permission for background descendants to live.
            stop()
            code = child.wait(timeout=3)
    finally:
        selector.close()
        child.stdout.close()
        stop()
    state = {
        **usage(),
        "log_bytes": written,
        "reason": reason,
        "exit_code": 125 if reason else code,
        "execution_id": request["execution_id"],
    }
    ROOT.joinpath("result").write_text(json.dumps(state))
    # SDK logs contain only a bounded summary, not unbounded user stdout.
    print(json.dumps(state), flush=True)
    return int(state["exit_code"])


def main() -> None:
    mode = sys.argv[1]
    if mode == "start":
        raise SystemExit(execute(json.loads(sys.argv[2])))
    if mode == "stop":
        if not ROOT.joinpath("command").exists():
            print("legacy")
            return
        command = json.loads(ROOT.joinpath("command").read_text())
        if command["command_id"] != json.loads(sys.argv[2])["command_id"]:
            raise ValueError("stop command identity mismatch")
        stop()
        print("stopped")
        return
    if mode == "bind":
        until = time.monotonic() + 3
        while not ROOT.joinpath("binding").exists() and time.monotonic() < until:
            time.sleep(0.01)
    binding = json.loads(ROOT.joinpath("binding").read_text())
    if mode in {"credential_read", "credential_write"}:
        request = json.loads(sys.argv[2])
        if request["execution_id"] != binding["execution_id"]:
            raise ValueError("credential execution binding mismatch")
        directory = os.open("/workspace", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            if mode == "credential_read":
                fd = os.open(
                    "credential-request.json",
                    os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                    dir_fd=directory,
                )
                try:
                    info = os.fstat(fd)
                    if not stat.S_ISREG(info.st_mode) or info.st_uid != UID:
                        raise ValueError("invalid credential request file")
                    payload = os.read(fd, 32769)
                    if not 0 < len(payload) <= 32768:
                        raise ValueError("credential request exceeds bound")
                    print(base64.b64encode(payload).decode("ascii"))
                finally:
                    os.close(fd)
            else:
                identity = str(UUID(request["request_id"]))
                data = base64.b64decode(request["response"], validate=True)
                if not 0 < len(data) <= 32768:
                    raise ValueError("credential response exceeds bound")
                temporary = f".credential-{uuid4().hex}"
                fd = os.open(
                    temporary,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o644,
                    dir_fd=directory,
                )
                try:
                    with os.fdopen(fd, "wb") as output:
                        output.write(data)
                        output.flush()
                        os.fsync(output.fileno())
                    os.link(
                        temporary,
                        f"credential-response-{identity}.json",
                        src_dir_fd=directory,
                        dst_dir_fd=directory,
                        follow_symlinks=False,
                    )
                finally:
                    os.unlink(temporary, dir_fd=directory)
                print("published")
        finally:
            os.close(directory)
        return
    if mode == "bind":
        identity = json.loads(sys.argv[2])
        if identity["execution_id"] != binding["execution_id"]:
            raise ValueError("command binding mismatch")
        with ROOT.joinpath("command").open("x") as command:
            json.dump(identity, command)
        print("bound")
        return
    state = (
        json.loads(ROOT.joinpath("result").read_text()) if ROOT.joinpath("result").exists() else {}
    )
    state.update(usage())
    state["log_bytes"] = (
        ROOT.joinpath("output").stat().st_size if ROOT.joinpath("output").exists() else 0
    )
    state["execution_id"] = binding["execution_id"]
    state["limits"] = binding["limits"]
    state["command_id"] = json.loads(ROOT.joinpath("command").read_text())["command_id"]
    if mode == "output":
        print(base64.b64encode(ROOT.joinpath("output").read_bytes()).decode("ascii"))
    elif mode == "inspect":
        print(json.dumps(state))
    else:
        raise ValueError("unknown guard operation")


if __name__ == "__main__":
    main()

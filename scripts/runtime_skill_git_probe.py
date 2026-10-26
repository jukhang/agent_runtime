"""Disposable real Git HTTPS/CONNECT fixture for R03; no host Skill code is executed."""

import hashlib
import os
import select
import socket
import socketserver
import ssl
import subprocess
import threading
from contextlib import AbstractContextManager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.references import CapabilityKey, CapabilityRef
from agent_runtime.contracts.registry import SkillSpec
from agent_runtime.skills.git import GitSkillInstaller, PublicGitFetcher
from credential_test_support import certificates

SCRIPT = b"""import json, os, pathlib, subprocess, sys, time
assert os.getuid() == 1000 and os.getgroups() == []
assert not pathlib.Path('/var/run/docker.sock').exists()
assert 'OPENSANDBOX_SERVER_API_KEY' not in os.environ
mode = sys.argv[1]
counter = pathlib.Path('/workspace/skill-effects')
with counter.open('a') as stream:
    stream.write(mode + '\\n')
pathlib.Path('/artifacts/report.json').write_text(json.dumps({
    'mode': mode, 'uid': os.getuid(), 'executions': len(counter.read_text().splitlines()),
    'total': 391,
}))
if mode in ('success', 'approved'):
    print('391', flush=True)
elif mode == 'nonzero':
    print('partial', flush=True)
    raise SystemExit(7)
elif mode == 'quota':
    os.write(1, b'x' * 2000000)
elif mode in ('deadline', 'cancel'):
    subprocess.Popen(['sleep', '80'], start_new_session=True)
    print('ready', flush=True)
    time.sleep(80)
elif mode == 'takeover':
    print('once', flush=True)
    time.sleep(40)
elif mode == 'rollback':
    print('sealed-once', flush=True)
elif mode == 'delete':
    print('deleted-once', flush=True)
elif mode == 'lost':
    print('dispatched-once', flush=True)
else:
    raise ValueError('unknown fixture scenario')
"""
FILES = {
    "SKILL.md": b"# Report Skill\nRun the pinned script with the approved mode.\n",
    "run.py": SCRIPT,
}


def ref(key: str) -> CapabilityRef:
    return CapabilityRef(key=CapabilityKey(namespace="test.skill", key=key), revision="1")


class GitSkillFixture(AbstractContextManager):
    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.targets: list[str] = []
        self.stopped = threading.Event()
        self.servers: list = []
        self.threads: list[threading.Thread] = []

    def __enter__(self):
        self.directory.mkdir(parents=True)
        certificates(self.directory / "tls")
        self.ca = self.directory / "tls/ca.pem"
        env = {"PATH": os.defpath, "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull}
        repository = self.directory / "checkout"
        repository.mkdir()

        def git(*args):
            return (
                subprocess.run(
                    [
                        "git",
                        "-c",
                        "core.hooksPath=/dev/null",
                        "-c",
                        "commit.gpgsign=false",
                        "-c",
                        "user.name=R03 Probe",
                        "-c",
                        "user.email=probe@example.invalid",
                        "-C",
                        str(repository),
                        *args,
                    ],
                    env=env,
                    check=True,
                    capture_output=True,
                    timeout=30,
                )
                .stdout.decode()
                .strip()
            )

        git("init", "--quiet")
        for base in (repository, repository / "skills/report"):
            base.mkdir(parents=True, exist_ok=True)
            for path, data in FILES.items():
                (base / path).write_bytes(data)
                (base / path).chmod(0o755 if path.endswith(".py") else 0o644)
        (repository / "skills/link").symlink_to("report", target_is_directory=True)
        git("add", ".")
        git("commit", "--quiet", "-m", "Pinned disposable Skill fixture")
        self.commit = git("rev-parse", "HEAD")
        project_root = self.directory / "repositories"
        project_root.mkdir()
        git("clone", "--bare", "--quiet", str(repository), str(project_root / "skills.git"))
        fixture = self

        class GitHttp(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def do_GET(self):
                self.serve_git()

            def do_POST(self):
                self.serve_git()

            def serve_git(self):
                parsed = urlsplit(self.path)
                length = int(self.headers.get("Content-Length", "0"))
                if not parsed.path.startswith("/skills.git/") or not 0 <= length <= 65536:
                    self.send_error(400)
                    return
                response = subprocess.run(
                    ["git", "http-backend"],
                    input=self.rfile.read(length),
                    env={
                        **env,
                        "GIT_PROJECT_ROOT": str(project_root),
                        "GIT_HTTP_EXPORT_ALL": "1",
                        "PATH_INFO": parsed.path,
                        "QUERY_STRING": parsed.query,
                        "REQUEST_METHOD": self.command,
                        "CONTENT_TYPE": self.headers.get("Content-Type", ""),
                        "CONTENT_LENGTH": str(length),
                        "HTTP_GIT_PROTOCOL": self.headers.get("Git-Protocol", ""),
                        "SERVER_PROTOCOL": "HTTP/1.1",
                        "REMOTE_ADDR": "127.0.0.1",
                    },
                    capture_output=True,
                    check=False,
                    timeout=30,
                )
                if response.returncode != 0:
                    self.send_error(422, "Git upload request was rejected")
                    return
                response = response.stdout
                headers, body = response.split(b"\r\n\r\n", 1)
                fields = [line.decode().split(":", 1) for line in headers.split(b"\r\n")]
                status = next(
                    (int(value.split()[0]) for name, value in fields if name.lower() == "status"),
                    200,
                )
                self.send_response(status)
                for name, value in fields:
                    if name.lower() not in {"status", "content-length"}:
                        self.send_header(name, value.strip())
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        tls = ThreadingHTTPServer(("127.0.0.1", 0), GitHttp)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.load_cert_chain(
            self.directory / "tls/egress.pem", self.directory / "tls/egress.key"
        )
        tls.socket = context.wrap_socket(tls.socket, server_side=True)
        self.servers.append(tls)

        class Proxy(socketserver.StreamRequestHandler):
            def handle(self):
                self.connection.settimeout(30)
                parts = self.rfile.readline(4096).decode().split()
                while self.rfile.readline(4096) not in {b"\r\n", b"\n", b""}:
                    pass
                if len(parts) != 3 or parts[:2] != ["CONNECT", "egress.test:443"]:
                    self.wfile.write(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                    return
                fixture.targets.append(parts[1])
                with socket.create_connection(tls.server_address, timeout=10) as upstream:
                    self.wfile.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    self.wfile.flush()
                    while not fixture.stopped.is_set():
                        readers, _, _ = select.select((self.connection, upstream), (), (), 0.2)
                        for current in readers:
                            data = current.recv(65536)
                            if not data:
                                return
                            (upstream if current is self.connection else self.connection).sendall(
                                data
                            )

        class ProxyServer(socketserver.ThreadingTCPServer):
            daemon_threads = True
            allow_reuse_address = True

            def handle_error(self, request, client_address):
                pass

        proxy = ProxyServer(("127.0.0.1", 0), Proxy)
        self.servers.append(proxy)
        self.proxy = f"http://127.0.0.1:{proxy.server_address[1]}"
        for server in self.servers:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.threads.append(thread)
        return self

    def __exit__(self, *args):
        self.stopped.set()
        for server in reversed(self.servers):
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join(timeout=5)
        return False

    def spec(self, *, subdir="skills/report", required_tools=()) -> SkillSpec:
        entries = [
            {
                "path": path,
                "blobRef": path,
                "digest": hashlib.sha256(data).hexdigest(),
                "sizeBytes": len(data),
                "executable": path.endswith(".py"),
            }
            for path, data in sorted(FILES.items())
        ]
        return SkillSpec.model_validate(
            {
                "ref": ref("report"),
                "source": ref("git"),
                "gitCommit": self.commit,
                "gitSubdir": subdir,
                "bundleDigest": content_digest(
                    [
                        {key: entry[key] for key in ("path", "digest", "sizeBytes", "executable")}
                        for entry in entries
                    ]
                ),
                "instructionPath": "SKILL.md",
                "files": entries,
                "requiredTools": required_tools,
            }
        )

    def fetch(self, spec=None):
        return PublicGitFetcher(
            allowed_hosts=frozenset({"egress.test"}),
            proxy_url=self.proxy,
            ca_bundle=self.ca,
        ).install(spec or self.spec(), repository_url="https://egress.test/skills.git")


def verify_git_fixture(fixture: GitSkillFixture) -> dict:
    bundle = fixture.fetch()
    assert bundle.files == FILES
    assert fixture.fetch(fixture.spec(subdir=None)).files == FILES
    checks = {"pinnedSubdir": True, "pinnedRoot": True}
    for name, execute in {
        "untrustedCa": lambda: PublicGitFetcher(
            allowed_hosts=frozenset({"egress.test"}), proxy_url=fixture.proxy
        ).install(fixture.spec(), repository_url="https://egress.test/skills.git"),
        "symlink": lambda: fixture.fetch(fixture.spec(subdir="skills/link")),
        "wrongCommit": lambda: fixture.fetch(
            fixture.spec().model_copy(update={"git_commit": "b" * 40})
        ),
        "unknownHost": lambda: PublicGitFetcher(
            allowed_hosts=frozenset({"egress.test"}),
            proxy_url=fixture.proxy,
            ca_bundle=fixture.ca,
        ).install(fixture.spec(), repository_url="https://blocked.test/skills.git"),
        "oversized": lambda: PublicGitFetcher(
            allowed_hosts=frozenset({"egress.test"}),
            proxy_url=fixture.proxy,
            ca_bundle=fixture.ca,
            installer=GitSkillInstaller(max_file_bytes=1, max_bundle_bytes=1),
        ).install(fixture.spec(), repository_url="https://egress.test/skills.git"),
    }.items():
        try:
            execute()
        except ValueError:
            checks[name] = True
        else:
            raise AssertionError(f"Git boundary was not enforced: {name}")
    changed = fixture.spec().model_dump(mode="json", by_alias=True)
    changed["files"][0]["digest"] = "0" * 64
    changed["bundleDigest"] = content_digest(
        [
            {key: entry[key] for key in ("path", "digest", "sizeBytes", "executable")}
            for entry in changed["files"]
        ]
    )
    try:
        fixture.fetch(SkillSpec.model_validate(changed))
    except ValueError as exc:
        assert "digest" in str(exc)
        checks["digestDrift"] = True
    else:
        raise AssertionError("Git byte drift was accepted")
    with socket.create_connection(
        ("127.0.0.1", int(urlsplit(fixture.proxy).port)), timeout=5
    ) as client:
        client.sendall(b"CONNECT blocked.test:443 HTTP/1.1\r\nHost: blocked.test:443\r\n\r\n")
        assert b"403 Forbidden" in client.recv(4096)
    checks["proxyDeniedTarget"] = True
    assert fixture.targets and set(fixture.targets) == {"egress.test:443"}
    return {
        "checks": checks,
        "commit": fixture.commit,
        "bundleDigest": bundle.spec.bundle_digest,
        "proxyConnections": len(fixture.targets),
        "targets": sorted(set(fixture.targets)),
    }

"""Validate an exact Git revision before a Skill can enter a sandbox workspace."""

import hashlib
import os
import stat
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.registry import SkillSpec


@dataclass(frozen=True, slots=True)
class VerifiedSkillBundle:
    spec: SkillSpec
    files: dict[str, bytes]


class GitSkillInstaller:
    """The checkout is provided by a trusted Git/egress adapter, never by the model."""

    def __init__(
        self, *, max_file_bytes: int = 2_097_152, max_bundle_bytes: int = 20_971_520
    ) -> None:
        if max_file_bytes <= 0 or max_bundle_bytes < max_file_bytes:
            raise ValueError("invalid Skill bundle limits")
        self._max_file = max_file_bytes
        self._max_bundle = max_bundle_bytes

    def verify_checkout(
        self, spec: SkillSpec, *, checkout: Path, resolved_commit: str
    ) -> VerifiedSkillBundle:
        if spec.git_commit is None or resolved_commit.lower() != spec.git_commit:
            raise ValueError("Skill checkout is not the pinned Git commit")
        self.verify_manifest(spec)
        root = checkout.resolve(strict=True)
        if not root.is_dir():
            raise ValueError("Skill checkout is not a directory")
        expected = {item.path: item for item in spec.files}
        files: dict[str, bytes] = {}
        for path, item in expected.items():
            candidate = root.joinpath(*path.split("/"))
            if not candidate.is_relative_to(root):
                raise ValueError("Skill path escapes its checkout")
            cursor = candidate
            while cursor != root:
                if stat.S_ISLNK(cursor.lstat().st_mode):
                    raise ValueError("Skill path contains a symbolic link")
                cursor = cursor.parent
            mode = candidate.lstat().st_mode
            if not stat.S_ISREG(mode):
                raise ValueError("Skill manifest contains a non-regular file")
            if bool(mode & 0o111) != item.executable:
                raise ValueError("Skill executable permission differs from its manifest")
            size = candidate.stat().st_size
            if size != item.size_bytes or size > self._max_file:
                raise ValueError("Skill file exceeds its pinned size or bundle limit")
            data = candidate.read_bytes()
            files[path] = data
        return self.verify_bundle(VerifiedSkillBundle(spec=spec, files=files))

    def verify_bundle(self, bundle: VerifiedSkillBundle) -> VerifiedSkillBundle:
        """Use the same byte and manifest checks at install and on every Worker load."""
        spec, files = bundle.spec, bundle.files
        self.verify_manifest(spec)
        if set(files) != {item.path for item in spec.files}:
            raise ValueError("Skill bytes differ from the pinned file list")
        total = 0
        for item in spec.files:
            data = files[item.path]
            total += len(data)
            if (
                len(data) != item.size_bytes
                or len(data) > self._max_file
                or total > self._max_bundle
            ):
                raise ValueError("Skill file exceeds its pinned size or bundle limit")
            if hashlib.sha256(data).hexdigest() != item.digest:
                raise ValueError("Skill file digest differs from its manifest")
        return VerifiedSkillBundle(spec=spec, files=files)

    def verify_manifest(self, spec: SkillSpec) -> None:
        """Reject over-limit manifests before fetching or allocating file content."""
        if (
            any(item.size_bytes > self._max_file for item in spec.files)
            or sum(item.size_bytes for item in spec.files) > self._max_bundle
        ):
            raise ValueError("Skill file exceeds its pinned size or bundle limit")
        manifest = [
            {
                "path": item.path,
                "digest": item.digest,
                "sizeBytes": item.size_bytes,
                "executable": item.executable,
            }
            for item in sorted(spec.files, key=lambda entry: entry.path)
        ]
        if content_digest(manifest) != spec.bundle_digest:
            raise ValueError("Skill bundle digest differs from its manifest")


class PublicGitFetcher:
    """Fetch public HTTPS commits through a mandatory host-controlled egress proxy."""

    def __init__(
        self,
        *,
        allowed_hosts: frozenset[str],
        proxy_url: str,
        timeout_seconds: int = 120,
        installer: GitSkillInstaller | None = None,
        ca_bundle: Path | None = None,
    ) -> None:
        proxy = urlsplit(proxy_url)
        if (
            not allowed_hosts
            or proxy.scheme != "http"
            or not proxy.hostname
            or proxy.username is not None
            or proxy.password is not None
            or proxy.path not in {"", "/"}
            or proxy.query
            or proxy.fragment
        ):
            raise ValueError("Git requires an explicit trusted egress proxy")
        if timeout_seconds <= 0:
            raise ValueError("Git timeout must be positive")
        self._hosts, self._proxy = allowed_hosts, proxy_url
        self._timeout, self._installer = timeout_seconds, installer or GitSkillInstaller()
        self._ca = ca_bundle.resolve(strict=True) if ca_bundle is not None else None
        if self._ca is not None and not self._ca.is_file():
            raise ValueError("Git CA bundle must be a host-owned certificate file")

    def install(self, spec: SkillSpec, *, repository_url: str) -> VerifiedSkillBundle:
        url = urlsplit(repository_url)
        if (
            url.scheme != "https"
            or url.hostname not in self._hosts
            or url.username is not None
            or url.password is not None
            or url.port not in {None, 443}
            or url.query
            or url.fragment
            or not url.path.endswith(".git")
            or spec.git_commit is None
        ):
            raise ValueError("Git source is not an allowlisted public HTTPS repository")
        self._installer.verify_manifest(spec)
        env = {
            "PATH": os.defpath,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_LFS_SKIP_SMUDGE": "1",
            "NO_PROXY": "",
            "no_proxy": "",
        }
        options = (
            "-c",
            "protocol.allow=never",
            "-c",
            "protocol.https.allow=always",
            "-c",
            f"http.proxy={self._proxy}",
            "-c",
            "http.followRedirects=false",
            "-c",
            "http.sslVerify=true",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "submodule.recurse=false",
            *(("-c", f"http.sslCAInfo={self._ca}") if self._ca is not None else ()),
        )

        def git(root: Path, *args: str) -> str:
            try:
                result = subprocess.run(
                    ("git", *options, "-C", str(root), *args),
                    env=env,
                    capture_output=True,
                    check=True,
                    text=True,
                    timeout=self._timeout,
                )
            except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                raise ValueError("Trusted Git fetch failed") from exc
            return result.stdout.strip()

        with tempfile.TemporaryDirectory(prefix="agent-skill-") as directory:
            root = Path(directory)
            git(
                root,
                "init",
                "--quiet",
                *(("--object-format=sha256",) if len(spec.git_commit) == 64 else ()),
            )
            git(root, "fetch", "--quiet", "--depth=1", repository_url, spec.git_commit)
            git(root, "checkout", "--quiet", "--detach", "FETCH_HEAD")
            commit = git(root, "rev-parse", "HEAD")
            checkout = root
            if spec.git_subdir is not None:
                for part in spec.git_subdir.split("/"):
                    checkout = checkout / part
                    mode = checkout.lstat().st_mode
                    if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
                        raise ValueError("Skill Git subdirectory is not a regular directory")
            return self._installer.verify_checkout(spec, checkout=checkout, resolved_commit=commit)


__all__ = ["GitSkillInstaller", "PublicGitFetcher", "VerifiedSkillBundle"]

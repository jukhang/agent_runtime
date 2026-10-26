import hashlib
import json
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from types import SimpleNamespace
from uuid import uuid4

import pytest

from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.policy import PolicyAllow
from agent_runtime.contracts.references import ResourceRef, ResourceScope
from agent_runtime.contracts.registry import SkillSpec
from agent_runtime.contracts.tools import ToolInvocation
from agent_runtime.sandbox.runtime import ExecutionBinding, ExecutionHandle
from agent_runtime.sandbox.supervision import ExecutionLimits
from agent_runtime.skills.git import GitSkillInstaller, PublicGitFetcher
from agent_runtime.skills.sandbox import OpenSandboxSkillRunner
from agent_runtime.skills.script import InstalledSkillScriptTool, SkillScriptTool

REF = {"key": {"namespace": "builtin", "key": "skill"}, "revision": "1"}


def _spec(data: bytes, *, executable: bool = False, git_subdir: str | None = None) -> SkillSpec:
    file = {
        "path": "SKILL.md",
        "blobRef": "private-blob",
        "digest": hashlib.sha256(data).hexdigest(),
        "sizeBytes": len(data),
        "executable": executable,
    }
    bundle = content_digest(
        [
            {
                "path": file["path"],
                "digest": file["digest"],
                "sizeBytes": file["sizeBytes"],
                "executable": file["executable"],
            }
        ]
    )
    return SkillSpec.model_validate(
        {
            "ref": REF,
            "source": REF,
            "gitCommit": "a" * 40,
            "gitSubdir": git_subdir,
            "bundleDigest": bundle,
            "instructionPath": "SKILL.md",
            "files": [file],
            "requiredTools": [],
        }
    )


def test_skill_checkout_requires_pinned_commit_and_file_digest(tmp_path):
    data = b"# Verified skill\n"
    path = tmp_path / "SKILL.md"
    path.write_bytes(data)
    installer = GitSkillInstaller()
    bundle = installer.verify_checkout(_spec(data), checkout=tmp_path, resolved_commit="a" * 40)
    assert bundle.files == {"SKILL.md": data}
    with pytest.raises(ValueError, match="commit"):
        installer.verify_checkout(_spec(data), checkout=tmp_path, resolved_commit="b" * 40)
    path.write_bytes(b"# Modified skill\n")
    with pytest.raises(ValueError, match="digest"):
        installer.verify_checkout(_spec(data), checkout=tmp_path, resolved_commit="a" * 40)


def test_skill_checkout_rejects_symlink_and_permission_mismatch(tmp_path):
    data = b"# Verified skill\n"
    target = tmp_path / "target"
    target.write_bytes(data)
    (tmp_path / "SKILL.md").symlink_to(target)
    with pytest.raises(ValueError, match="symbolic link"):
        GitSkillInstaller().verify_checkout(
            _spec(data), checkout=tmp_path, resolved_commit="a" * 40
        )
    (tmp_path / "SKILL.md").unlink()
    (tmp_path / "SKILL.md").write_bytes(data)
    with pytest.raises(ValueError, match="executable"):
        GitSkillInstaller().verify_checkout(
            _spec(data, executable=True), checkout=tmp_path, resolved_commit="a" * 40
        )


def test_skill_subdir_and_bundle_bytes_are_strict(tmp_path):
    data = b"# Nested skill\n"
    nested = tmp_path / "skills" / "sample"
    nested.mkdir(parents=True)
    (nested / "SKILL.md").write_bytes(data)
    spec = _spec(data, git_subdir="skills/sample")
    with pytest.raises(ValueError, match="bundle limit"):
        GitSkillInstaller(max_file_bytes=2, max_bundle_bytes=2).verify_checkout(
            spec, checkout=tmp_path / "missing", resolved_commit="a" * 40
        )
    bundle = GitSkillInstaller().verify_checkout(spec, checkout=nested, resolved_commit="a" * 40)
    with pytest.raises(ValueError, match="file list"):
        GitSkillInstaller().verify_bundle(
            type(bundle)(spec=spec, files={"SKILL.md": data, "extra": b"bad"})
        )
    with pytest.raises(ValueError, match="digest"):
        GitSkillInstaller().verify_bundle(
            type(bundle)(spec=spec, files={"SKILL.md": b"x" + data[1:]})
        )
    for subdir in ("../other", "/absolute", "skills//sample", "skills/./sample"):
        with pytest.raises(ValueError, match="subdirectory"):
            _spec(data, git_subdir=subdir)


@pytest.mark.parametrize("symlink", [False, True])
def test_git_fetcher_selects_only_regular_pinned_subdir(monkeypatch, symlink):
    from pathlib import Path

    data = b"# Subdirectory Skill\n"

    def git(args, **kwargs):
        assert "http.proxy=http://127.0.0.1:18080" in args
        assert "http.followRedirects=false" in args
        assert kwargs["env"]["GIT_TERMINAL_PROMPT"] == "0"
        root = Path(args[args.index("-C") + 1])
        if "checkout" in args:
            (root / "skills").mkdir()
            (root / "target").mkdir()
            if symlink:
                (root / "skills/sample").symlink_to(root / "target", target_is_directory=True)
            else:
                (root / "skills/sample").mkdir()
            (root / "skills/sample/SKILL.md").write_bytes(data)
        return SimpleNamespace(stdout="a" * 40 if "rev-parse" in args else "")

    monkeypatch.setattr("agent_runtime.skills.git.subprocess.run", git)
    fetcher = PublicGitFetcher(
        allowed_hosts=frozenset({"example.org"}), proxy_url="http://127.0.0.1:18080"
    )
    spec = _spec(data, git_subdir="skills/sample")
    if symlink:
        with pytest.raises(ValueError, match="subdirectory"):
            fetcher.install(spec, repository_url="https://example.org/sample.git")
    else:
        assert fetcher.install(
            spec, repository_url="https://example.org/sample.git"
        ).files == {"SKILL.md": data}


@pytest.mark.asyncio
async def test_skill_script_stages_verified_bytes_before_sandbox_start(tmp_path):
    instructions = b"# Skill\n"
    script = b"#!/bin/sh\necho sandbox\n"
    (tmp_path / "SKILL.md").write_bytes(instructions)
    (tmp_path / "run.sh").write_bytes(script)
    (tmp_path / "run.sh").chmod(0o755)
    entries = [
        {
            "path": path,
            "blobRef": path,
            "digest": hashlib.sha256(data).hexdigest(),
            "sizeBytes": len(data),
            "executable": executable,
        }
        for path, data, executable in (
            ("SKILL.md", instructions, False),
            ("run.sh", script, True),
        )
    ]
    bundle_digest = content_digest(
        [
            {
                "path": entry["path"],
                "digest": entry["digest"],
                "sizeBytes": entry["sizeBytes"],
                "executable": entry["executable"],
            }
            for entry in entries
        ]
    )
    spec = SkillSpec.model_validate(
        {
            "ref": REF,
            "source": REF,
            "gitCommit": "a" * 40,
            "bundleDigest": bundle_digest,
            "instructionPath": "SKILL.md",
            "files": entries,
            "requiredTools": [],
        }
    )
    bundle = GitSkillInstaller().verify_checkout(
        spec, checkout=tmp_path, resolved_commit="a" * 40
    )
    data = json.loads(files("agent_runtime.contracts").joinpath("fixtures/tools.json").read_text())
    invocation = ToolInvocation.model_validate(
        {
            **data["invocation"],
            "deadlineAt": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
            "call": {**data["invocation"]["call"], "arguments": {"argv": ["two words"]}},
        }
    )
    evidence = ResourceRef(
        resource_type="skill", resource_id="skill-1", resource_version="1",
        scope=ResourceScope(scope_id="scope"),
    )
    grant = PolicyAllow(
        decision="allow", decision_id=uuid4(), policy=invocation.call.tool,
        input_digest="a" * 64, evidence_refs=(evidence,),
        evaluated_at=datetime.now(UTC), expires_at=invocation.deadline_at,
        grant_ref="skill-grant",
    )
    binding = ExecutionBinding(
        scope_id="scope", execution_id=uuid4(), generation=1,
        workspace_generation=1, image_digest="python@sha256:" + "b" * 64,
        sandbox_id="sandbox-1",
    )

    class Files:
        stored = {}

        async def write_files(self, values):
            self.stored = {item.path: item.data for item in values}

        async def read_bytes(self, path):
            return self.stored[path]

    class Sandbox:
        id = "sandbox-1"
        files = Files()

    class Runtime:
        calls = 0

        async def start(self, request):
            self.calls += 1
            assert Sandbox.files.stored
            assert request.command.startswith("sh /workspace/skills/")
            assert request.command.endswith("'two words'")
            assert request.limits is not None
            return ExecutionHandle(binding, "command-1")

    async def resolve(current, decision):
        assert current == invocation and decision == grant
        return binding

    runtime = Runtime()
    runner = OpenSandboxSkillRunner(
        sandbox=Sandbox(), runtime=runtime, binding=resolve,
        limits=ExecutionLimits(1024, 1024, 1024, 1), timeout_seconds=30,
        interpreter="sh",
        file_entry=lambda **values: SimpleNamespace(**values),
    )
    class Loader:
        revoked = False

        async def load(self, actor, run_id, ref):
            assert actor == invocation.actor and run_id == invocation.run_id and ref == spec.ref
            if self.revoked:
                raise ValueError("Skill revision was revoked")
            return bundle

    loader = Loader()
    tool = InstalledSkillScriptTool(
        skill=spec.ref, loader=loader, executable_path="run.sh",
        sandbox=runner, available_tools=frozenset(),
    )
    outcome = await tool.execute(invocation, grant)
    assert outcome.status == "pending_execution"
    assert outcome.execution_ref.resource_id == str(binding.execution_id)
    loader.revoked = True
    with pytest.raises(ValueError, match="revoked"):
        await tool.execute(invocation, grant)
    assert runtime.calls == 1
    with pytest.raises(ValueError, match="dependencies"):
        SkillScriptTool(
            bundle=type(bundle)(
                spec.model_copy(update={"required_tools": (spec.ref,)}), bundle.files
            ),
            executable_path="run.sh", sandbox=runner, available_tools=frozenset(),
        )

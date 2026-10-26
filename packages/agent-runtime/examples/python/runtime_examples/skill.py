"""Verified Skill bytes, explicit online installation and supervised script binding."""

import hashlib
from pathlib import Path

from agent_runtime import Agent
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.references import CapabilityRef
from agent_runtime.contracts.registry import SkillSpec
from agent_runtime.contracts.workspace import WorkspaceFile
from agent_runtime.skills.git import GitSkillInstaller, PublicGitFetcher, VerifiedSkillBundle
from agent_runtime.skills.script import (
    InstalledSkillScriptTool,
    SandboxScriptRunner,
    SkillBundleLoader,
)
from runtime_examples.agent_config import build_agent, ref


def local_bundle() -> VerifiedSkillBundle:
    files = {
        "SKILL.md": b"# Factory report\nUse verified data; run run.py in the supervised sandbox.\n",
        "run.py": b"#!/usr/bin/env python3\nprint('Report from the sandbox')\n",
    }
    entries = tuple(
        WorkspaceFile(
            path=name,
            blob_ref=f"tutorial-{name}",
            digest=hashlib.sha256(data).hexdigest(),
            size_bytes=len(data),
            executable=name == "run.py",
        )
        for name, data in sorted(files.items())
    )
    manifest = [
        {
            "path": item.path,
            "digest": item.digest,
            "sizeBytes": item.size_bytes,
            "executable": item.executable,
        }
        for item in entries
    ]
    spec = SkillSpec(
        ref=ref("factory-report"),
        source=ref("reviewed-skill-source"),
        git_commit=None,
        bundle_digest=content_digest(manifest),
        instruction_path="SKILL.md",
        files=entries,
        required_tools=(ref("multiply"),),
    )
    return GitSkillInstaller().verify_bundle(VerifiedSkillBundle(spec=spec, files=files))


def configured_agent() -> Agent:
    return build_agent().model_copy(update={"skills": (local_bundle().spec.ref,)})


def install_public_git(
    spec: SkillSpec,
    *,
    repository_url: str,
    proxy_url: str,
    allowed_hosts: frozenset[str],
    ca_bundle: Path | None = None,
) -> VerifiedSkillBundle:
    # Use a reviewed manifest: exact commit, optional git_subdir and every file's hash.
    return PublicGitFetcher(
        allowed_hosts=allowed_hosts, proxy_url=proxy_url, ca_bundle=ca_bundle
    ).install(spec, repository_url=repository_url)


def make_script_handler(
    skill: CapabilityRef,
    loader: SkillBundleLoader,
    sandbox: SandboxScriptRunner,
    available_tools: frozenset[CapabilityRef],
) -> InstalledSkillScriptTool:
    # Reload and authorize the pinned revision for each Run, not once at process startup.
    return InstalledSkillScriptTool(
        skill=skill,
        loader=loader,
        executable_path="run.py",
        sandbox=sandbox,
        available_tools=available_tools,
    )


if __name__ == "__main__":
    bundle = local_bundle()
    print("Verified local bytes only; no Git fetch or host script execution occurred.")
    print(bundle.spec.model_dump_json(by_alias=True, indent=2))
    print(configured_agent().model_dump_json(by_alias=True, indent=2))

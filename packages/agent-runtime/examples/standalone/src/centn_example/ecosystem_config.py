"""Explicit, host-owned ecosystem deployment configuration; never loads dotenv."""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from agent_runtime.contracts.registry import SkillSpec

IMAGE = (
    "mirror.gcr.io/library/python@sha256:"
    "57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de"
)


@dataclass(frozen=True)
class EcosystemSettings:
    mcp_port: int
    mcp_ca: Path
    mcp_token: str = field(repr=False)
    sandbox_domain: str
    sandbox_key: str = field(repr=False)
    git_url: str
    git_proxy: str
    git_ca: Path
    skill: SkillSpec
    deployment_id: str

    @classmethod
    def from_env(cls):
        if not os.environ.get("CENTN_ECOSYSTEM_CONFIG"):
            return None
        # This is an explicit private deployment file, not the repository root .env.
        value = json.loads(Path(os.environ["CENTN_ECOSYSTEM_CONFIG"]).read_text())
        return cls(
            mcp_port=int(value["mcpPort"]),
            mcp_ca=Path(value["mcpCa"]),
            mcp_token=value["mcpToken"],
            sandbox_domain=value["sandboxDomain"],
            sandbox_key=value["sandboxKey"],
            git_url=value["gitUrl"],
            git_proxy=value["gitProxy"],
            git_ca=Path(value["gitCa"]),
            skill=SkillSpec.model_validate(value["skill"]),
            deployment_id=value["deploymentId"],
        )

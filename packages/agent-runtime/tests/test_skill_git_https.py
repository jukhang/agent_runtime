"""Real git-http-backend, TLS and fixed-target CONNECT, with no mocked Git calls."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

from runtime_skill_git_probe import GitSkillFixture, verify_git_fixture  # noqa: E402


def test_real_git_https_controlled_proxy_install(tmp_path):
    with GitSkillFixture(tmp_path / "git") as fixture:
        result = verify_git_fixture(fixture)
        assert set(result["checks"]) == {
            "pinnedRoot",
            "pinnedSubdir",
            "untrustedCa",
            "symlink",
            "wrongCommit",
            "unknownHost",
            "oversized",
            "digestDrift",
            "proxyDeniedTarget",
        }
        assert all(result["checks"].values())

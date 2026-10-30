"""Private test driver only: root .env never reaches SDK configuration loading."""

import hashlib
import json
import os
import secrets
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from dotenv import dotenv_values
from sqlalchemy.engine import URL


def main():
    root = Path(__file__).resolve().parents[5]
    example = Path(__file__).resolve().parents[1]
    settings = dotenv_values(root / ".env")
    dsn = URL.create(
        "postgresql+asyncpg",
        username=settings["DB_USER"],
        password=settings["DB_PASSWORD"],
        host=settings["DB_HOST"],
        port=int(settings["DB_PORT"] or "5432"),
        database="aletai_wangxh",
    ).render_as_string(hide_password=False)
    environment = {
        **os.environ,
        "CENTN_LIVE": "1",
        "CENTN_DATABASE_URL": dsn,
        "AGENT_RUNTIME_TEST_DSN": dsn,
        "CENTN_HOST_TOKEN": secrets.token_hex(32),
        "CENTN_PROVIDER_URL": os.environ.get("CENTN_PROVIDER_URL")
        or settings.get("CENTN_PROVIDER_URL")
        or "",
        "CENTN_PROVIDER_KEY": settings["API_KEY"],
        "CENTN_MODEL": settings["MODEL_NAME"],
        "CENTN_SCOPE": "p05-compression",
        "CENTN_ACTOR": "release-owner",
        "CENTN_SCHEMA": "p05_driver_unused",
        "CENTN_THINKING": "false",
        "PYTHONPATH": os.pathsep.join(
            [
                str(example / "src"),
                str(root / "packages/agent-runtime/src"),
                str(root / "packages/agent-runtime/tests"),
            ]
        ),
    }
    if not environment["CENTN_PROVIDER_URL"]:
        raise ValueError("Set CENTN_PROVIDER_URL explicitly before running live acceptance")
    output = root / "output/p05/compression"
    output.mkdir(parents=True, exist_ok=True)
    matrix = "--matrix" in sys.argv
    host = "--host" in sys.argv
    if host:
        environment["CENTN_CONTEXT_INPUT_TOKENS"] = "100000"
    label = "host" if host else "matrix" if matrix else "live"
    label += "-" + datetime.now(UTC).strftime("%H%M%S")
    environment["CENTN_RELEASE_EVIDENCE_DIR"] = str(output / (label + "-evidence"))
    command = [
        str(example / ".venv/bin/python"),
        "-m",
        "pytest",
        str(
            example
            / (
                "tests/test_compression_host_live.py"
                if host
                else "tests/test_compression_release_live.py"
                if matrix
                else "tests/test_compression_live.py"
            )
        ),
        "-q",
        "--tb=short",
        "--basetemp",
        str(output / label),
        "--junitxml",
        str(output / (label + ".xml")),
    ]
    if "--core" in sys.argv:
        command.extend(["-k", "sigkill" if matrix else "summary"])
    if not matrix:
        for case in (
            "summary",
            "timeout",
            "disconnect",
            "budget",
            "empty",
            "truncated",
            "context_limit",
        ):
            if "--case=" + case in sys.argv:
                command.extend(["-k", case])
    if "--faults" in sys.argv:
        selection = "independent_host_provider_fault"
        for case in ("empty", "truncated", "disconnect", "timeout"):
            if "--case=" + case in sys.argv:
                selection += " and " + case
        command.extend(["-k", selection])
    if "--performance" in sys.argv:
        command.extend(["-k", "first_persistent_delta"])

    def fingerprints():
        files = sorted((root / "packages/agent-runtime/src").rglob("*.py"))
        files += sorted((example / "src").rglob("*.py"))
        return {
            str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in files
        }

    before = fingerprints()
    result = subprocess.run(command, cwd=root, env=environment)
    after = fingerprints()
    (output / (label + "-source.json")).write_text(
        json.dumps(
            {
                "model": environment["CENTN_MODEL"],
                "exitCode": result.returncode,
                "before": before,
                "after": after,
                "sourceStableDuringRun": before == after,
                "providerLoad": "Other agents may concurrently call the same Qwen endpoint",
            },
            indent=2,
        )
    )
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()

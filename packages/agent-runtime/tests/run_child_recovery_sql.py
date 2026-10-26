"""Private configuration is read only here; tests only receive the guarded test DSN."""

import os
import sys
from pathlib import Path

import pytest
from dotenv import dotenv_values
from sqlalchemy.engine import URL, make_url


def main() -> int:
    root = Path(__file__).resolve().parents[3]
    config = {**dotenv_values(root / ".env"), **os.environ}
    supplied = os.environ.get("AGENT_RUNTIME_TEST_DSN")
    if supplied:
        url = make_url(supplied)
        if url.database != "aletai_wangxh":
            raise ValueError("Recovery SQL tests require aletai_wangxh")
    else:
        url = URL.create(
            "postgresql+asyncpg",
            username=config.get("DB_USER", "postgres"),
            password=config.get("DB_PASSWORD", ""),
            host=config.get("DB_HOST", "127.0.0.1"),
            port=int(config.get("DB_PORT", "5432")),
            database="aletai_wangxh",
        )
    os.environ["AGENT_RUNTIME_TEST_DSN"] = url.render_as_string(hide_password=False)
    os.environ["AGENT_RUNTIME_EXPECTED_DATABASE"] = "aletai_wangxh"
    # SQLAlchemy diagnostics are intentionally not printed: bind parameters may be private.
    return pytest.main(
        [
            str(Path(__file__).with_name("test_storage_child_recovery.py")),
            "-q",
            "--tb=no",
            "--junitxml=output/playwright/r05-child-recovery-sql.xml",
            *sys.argv[1:],
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())

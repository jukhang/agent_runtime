"""An already-current Runtime must not prevent host resource migration retries."""

from types import SimpleNamespace

import pytest
from centn_example import __main__ as command


@pytest.mark.parametrize("status", ["applied", "up-to-date"])
def test_repeat_migration_still_upgrades_host(monkeypatch, status):
    settings = SimpleNamespace(dsn="postgresql+asyncpg://unused/unused", schema="tutorial")
    calls = []

    async def execute(*args):
        return SimpleNamespace(status=status)

    async def upgrade(dsn, schema):
        calls.append((dsn, schema))

    monkeypatch.setattr(command.Settings, "from_env", lambda: settings)
    monkeypatch.setattr(command, "execute", execute)
    monkeypatch.setattr(command, "upgrade_resources", upgrade)
    monkeypatch.setattr("sys.argv", ["centn_example", "migrate"])
    command.main()
    assert calls == [(settings.dsn, settings.schema)]

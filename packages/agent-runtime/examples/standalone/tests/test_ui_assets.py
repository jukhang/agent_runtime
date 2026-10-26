"""The standalone UI serves only its explicit, local module dependencies."""

import pytest
from centn_example.api import create_app
from centn_example.config import Settings
from fastapi.testclient import TestClient


@pytest.mark.parametrize(
    "path,status",
    [
        ("/", 200),
        ("/ui/app.mjs", 200),
        ("/ui/journal.mjs", 200),
        ("/ui/composer.mjs", 200),
        ("/ui/style.css", 200),
        ("/ui/config.py", 404),
        ("/ui/index.html", 404),
    ],
)
def test_ui_dependencies_are_explicit_and_available(path: str, status: int) -> None:
    settings = Settings(
        dsn="postgresql+asyncpg://unused/unused",
        token="test-token-not-a-real-credential",
        scope="test",
        actor="test",
        schema="test",
        provider_url="http://invalid.test",
        provider_key="unused",
        model="unused",
        thinking=False,
    )
    client = TestClient(create_app(settings))
    try:
        response = client.get(path)
        assert response.status_code == status
        if path == "/ui/composer.mjs":
            assert "export function composerKeyboard" in response.text
    finally:
        client.close()

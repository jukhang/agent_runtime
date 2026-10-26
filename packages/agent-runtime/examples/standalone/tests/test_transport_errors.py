"""Uncertain transport failures never fabricate a Run result or replay a request."""

import errno
from types import SimpleNamespace

import pytest
from centn_example.api import create_app
from centn_example.config import Settings
from fastapi.testclient import TestClient
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as SqlTimeoutError


@pytest.mark.parametrize(
    "error",
    [
        OSError(errno.ENETUNREACH, "private-error"),
        ConnectionResetError(errno.ECONNRESET, "private-error"),
        TimeoutError("private-error"),
        SqlTimeoutError("private-error"),
        DBAPIError(
            "private-statement", {}, RuntimeError("private-error"), connection_invalidated=True
        ),
        OSError(errno.EACCES, "private-error"),
        IntegrityError("private-statement", {}, RuntimeError("private-error")),
    ],
)
def test_host_dependency_error_is_explicit_and_does_not_retry_service(error):
    calls = []

    class Sessions:
        async def list(self, principal):
            calls.append(principal)
            raise error

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
    app = create_app(settings)
    app.state.host = SimpleNamespace(actor=lambda: "principal", sessions=Sessions())
    client = TestClient(app, raise_server_exceptions=False)
    try:
        response = client.get(
            "/api/sessions", headers={"Authorization": "Bearer " + settings.token}
        )
    finally:
        client.close()
    assert calls == ["principal"]
    assert "private-error" not in response.text and "private-statement" not in response.text
    if isinstance(error, IntegrityError) or getattr(error, "errno", None) == errno.EACCES:
        assert response.status_code == 500
    else:
        assert response.status_code == 503 and response.headers["retry-after"] == "1"
        body = response.json()
        assert body["code"] == "DEPENDENCY_UNAVAILABLE" and body["retryable"] is True
        assert "尚未确认" in body["message"] and "原幂等键" in body["message"]
        assert body["recoveryOptions"][0]["action"] == "retry_same"

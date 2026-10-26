"""Host configuration is explicit and never reads the surrounding project's .env."""

import os
from dataclasses import dataclass, field

import sqlalchemy as sa


@dataclass(frozen=True)
class Settings:
    dsn: str = field(repr=False)
    token: str = field(repr=False)
    scope: str
    actor: str
    schema: str
    provider_url: str
    provider_key: str = field(repr=False)
    model: str
    thinking: bool
    agent_profile: str = "conversation"
    context_input_tokens: int = 100000
    compression_max_input_tokens: int = 200000
    compression_timeout_seconds: float = 90

    def __post_init__(self):
        if not 1 <= self.context_input_tokens <= 200000:
            raise ValueError("Context input budget must be between 1 and 200000")
        if not 1 <= self.compression_max_input_tokens <= 240000:
            raise ValueError("Compression input bound must be between 1 and 240000")
        if not 0 < self.compression_timeout_seconds <= 240:
            raise ValueError("Compression timeout must be between 0 and 240 seconds")

    @classmethod
    def from_env(cls):
        def required(name):
            value = os.environ.get(name, "").strip()
            if not value:
                raise ValueError(f"Missing required setting: {name}")
            return value

        token = required("CENTN_HOST_TOKEN")
        if len(token) < 32:
            raise ValueError("CENTN_HOST_TOKEN must have at least 32 characters")
        dsn = required("CENTN_DATABASE_URL")
        if sa.make_url(dsn).drivername != "postgresql+asyncpg":
            raise ValueError("CENTN_DATABASE_URL requires postgresql+asyncpg")
        from agent_runtime.storage.uow.schema import require_schema

        agent_profile = os.environ.get("CENTN_AGENT_PROFILE", "conversation")
        if agent_profile not in {"conversation", "planner", "delegate", "external_wait"}:
            raise ValueError("Unknown CENTN_AGENT_PROFILE")

        return cls(
            dsn,
            token,
            required("CENTN_SCOPE"),
            required("CENTN_ACTOR"),
            require_schema(required("CENTN_SCHEMA")),
            required("CENTN_PROVIDER_URL"),
            required("CENTN_PROVIDER_KEY"),
            required("CENTN_MODEL"),
            os.environ.get("CENTN_THINKING", "false").lower() == "true",
            agent_profile,
            context_input_tokens=int(os.environ.get("CENTN_CONTEXT_INPUT_TOKENS", "100000")),
            compression_max_input_tokens=int(
                os.environ.get("CENTN_COMPRESSION_MAX_INPUT_TOKENS", "200000")
            ),
            compression_timeout_seconds=float(
                os.environ.get("CENTN_COMPRESSION_TIMEOUT_SECONDS", "90")
            ),
        )

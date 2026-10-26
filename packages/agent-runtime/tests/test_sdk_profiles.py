"""Profile extensions are explicitly bound and cannot replace built-in review."""

from uuid import uuid4

import httpx
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from agent_runtime import Agent
from agent_runtime.contracts.model import ModelConfig
from agent_runtime.contracts.references import CapabilityKey, CapabilityRef
from agent_runtime.storage.admission import AdmissionAgent
from agent_runtime.storage.main_factory import MainAgentProfile, SqlMainHandlerFactory
from agent_runtime.storage.main_setup import MAIN_DECISION_SCHEMA


def ref(key):
    return CapabilityRef(key=CapabilityKey(namespace="test", key=key), revision="1")


@pytest.mark.asyncio
async def test_profile_must_be_bound_validated_and_cannot_override_builtin_review():
    engine = create_async_engine("postgresql+asyncpg://unused:unused@127.0.0.1/unused")
    agent = Agent(id="assistant", name="Assistant", model=ref("model"), knowledge=(ref("kb"),))
    model = ModelConfig(
        ref=ref("model"),
        connection=ref("connection"),
        model_name="test",
        max_output_tokens=64,
        thinking="disabled",
        provider_options={},
    )
    installed = AdmissionAgent(ref("assistant"), uuid4(), agent, 1000)
    profile = MainAgentProfile(installed, model, ref("profile"), ref("prompt"))
    validated = []

    class Setup:
        def validate(self, current):
            validated.append(current)

    async with httpx.AsyncClient() as client:
        args = dict(
            engine=engine,
            schema="agent_runtime",
            client=client,
            estimator=object(),
            agents={("scope", installed.revision_id): profile},
            ports=object(),
            connections=object(),
            capabilities=object(),
            adapter=object(),
            allowed_predicates=(),
        )
        with pytest.raises(ValueError, match="host binding"):
            SqlMainHandlerFactory(**args)
        SqlMainHandlerFactory(**args, setups={profile.profile: Setup()})
        assert validated == [profile]
        with pytest.raises(ValueError, match="not bound"):
            SqlMainHandlerFactory(**args, setups={ref("foreign"): Setup()})
        with pytest.raises(ValueError, match="built-in"):
            SqlMainHandlerFactory(**args, output_formats={MAIN_DECISION_SCHEMA: {}})
    await engine.dispose()

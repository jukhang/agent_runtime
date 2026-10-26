import pytest
from pydantic import ValidationError

from agent_runtime import Agent
from agent_runtime.contracts.registry import HookSpec, RemoteAgentSpec, SkillSpec

REF = {"key": {"namespace": "builtin", "key": "capability"}, "revision": "1"}


def test_agent_schema_refs_roundtrip_without_function_serialization():
    agent = Agent.model_validate(
        {"id": "research", "name": "Research", "inputSchema": REF, "outputSchema": REF}
    )
    assert Agent.model_validate_json(agent.model_dump_json(by_alias=True)) == agent
    with pytest.raises(ValidationError):
        Agent(id="research", name="Research", hooks=(lambda: None,))


@pytest.mark.parametrize(
    "stage", ["input", "context", "tool_arguments", "before_execute", "output"]
)
def test_safety_hooks_cannot_fail_open(stage):
    with pytest.raises(ValidationError):
        HookSpec.model_validate(
            {
                "ref": REF,
                "handler": REF,
                "stage": stage,
                "order": 0,
                "timeoutSeconds": 5,
                "failurePolicy": "notify_retry",
                "replay": "pure",
            }
        )


def test_skill_must_pin_revision_and_instruction_file():
    data = {
        "ref": REF,
        "source": REF,
        "gitCommit": "a" * 40,
        "bundleDigest": "b" * 64,
        "instructionPath": "SKILL.md",
        "requiredTools": [],
        "files": [{"path": "SKILL.md", "blobRef": "blob", "digest": "c" * 64, "sizeBytes": 8}],
    }
    SkillSpec.model_validate(data)
    for key, value in [
        ("gitCommit", "main"),
        ("instructionPath", "other.md"),
        ("instructionPath", "../SKILL.md"),
    ]:
        with pytest.raises(ValidationError):
            SkillSpec.model_validate({**data, key: value})


def test_remote_lookup_claim_requires_actual_capability():
    with pytest.raises(ValidationError):
        RemoteAgentSpec.model_validate(
            {
                "ref": REF,
                "endpoint": REF,
                "protocol": REF,
                "cardDigest": "a" * 64,
                "credentialPolicy": REF,
                "canQueryTask": False,
                "canCancelTask": False,
                "idempotency": "query",
            }
        )

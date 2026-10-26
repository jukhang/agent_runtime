"""Run: python -m runtime_examples.agent_config"""

from agent_runtime import Agent, Budget, CapabilityKey, CapabilityRef, ModelConfig


def ref(name: str, namespace: str = "tutorial") -> CapabilityRef:
    return CapabilityRef(key=CapabilityKey(namespace=namespace, key=name), revision="1")


def build_agent() -> Agent:
    # These are immutable bindings, not automatically registered executable objects.
    return Agent(
        id="analyst",
        name="Manufacturing analyst",
        description="Answer questions from verified production facts.",
        model=ref("model"),
        prompt=ref("prompt"),
        instructions=(
            "Answer the actual question. Use supplied facts and explain missing evidence.",
            "Use tools for calculations; never invent tool results.",
        ),
        tools=(ref("multiply"),),
        context=ref("context"),
        budget=Budget(max_model_calls=16, max_tool_calls=24, max_runtime_seconds=600),
        add_datetime_to_context=True,
        timezone="Asia/Shanghai",
    )


def model_config(name: str = "configured-model", *, thinking: bool = False) -> ModelConfig:
    return ModelConfig(
        ref=ref("model"),
        connection=ref("provider"),
        model_name=name,
        max_output_tokens=2048,
        thinking="enabled" if thinking else "disabled",
        provider_options={"temperature": 0.2},
    )


if __name__ == "__main__":
    print(build_agent().model_dump_json(by_alias=True, indent=2))
    print(model_config().model_dump_json(by_alias=True, indent=2))

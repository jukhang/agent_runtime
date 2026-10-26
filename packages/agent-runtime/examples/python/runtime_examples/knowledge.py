"""Knowledge config is a binding; the host implements retrieval, ACL and SourceReader."""

from agent_runtime import Agent
from agent_runtime.contracts.registry import KnowledgeBinding
from runtime_examples.agent_config import build_agent, ref


def knowledge_binding() -> KnowledgeBinding:
    return KnowledgeBinding(
        ref=ref("factory-knowledge"),
        source=ref("factory-rag"),
        retrieval_policy=ref("hybrid-retrieval"),
        citation_policy=ref("versioned-citations"),
        access_policy=ref("knowledge-acl"),
    )


def configured_agent() -> Agent:
    return build_agent().model_copy(update={"knowledge": (knowledge_binding().ref,)})


if __name__ == "__main__":
    print(knowledge_binding().model_dump_json(by_alias=True, indent=2))
    print(configured_agent().model_dump_json(by_alias=True, indent=2))
    print("No RAG query occurred. See context.py for injecting version-pinned retrieved sources.")

"""Subagent declarations; actual dispatch and budget reservation belong to SQL Runtime."""

from datetime import datetime
from uuid import UUID

from agent_runtime import Agent, Budget
from agent_runtime.contracts.tasks import TaskPackage
from runtime_examples.agent_config import build_agent, ref


def agents() -> tuple[Agent, Agent]:
    reviewer = Agent(
        id="reviewer",
        name="Independent reviewer",
        model=ref("model"),
        instructions=("Verify supplied facts independently. Explain missing evidence.",),
        tools=(),
        budget=Budget(max_model_calls=4, max_tool_calls=0, max_runtime_seconds=180),
    )
    parent = build_agent().model_copy(update={"subagents": (ref("reviewer"),)})
    return parent, reviewer


def task(task_id: UUID, allocation_id: UUID, deadline: datetime) -> TaskPackage:
    return TaskPackage(
        task_id=task_id,
        target_agent=ref("reviewer"),
        objective="Verify that 800 actual hours out of 1000 planned hours equals 80%.",
        constraints=("Do not execute tools or create external effects.",),
        acceptance_spec_ref="review-facts",
        output_schema_ref="review-result",
        resource_refs=(),
        budget_allocation_id=allocation_id,
        deadline_at=deadline,
    )


if __name__ == "__main__":
    for agent in agents():
        print(agent.model_dump_json(by_alias=True, indent=2))
    print("Definitions only. Use the durable delegate profile for budgeted child execution.")

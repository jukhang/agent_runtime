"""Human replies and canonical approval decisions, without an HTTP API."""

from typing import Literal
from uuid import UUID

from agent_runtime import Runtime
from agent_runtime.contracts.requests import InteractionReceipt, InteractionRequest
from agent_runtime.contracts.tools import ApprovalDecision
from agent_runtime.contracts.transactions import ActorContext


async def reply(
    runtime: Runtime,
    actor: ActorContext,
    *,
    session_id: UUID,
    run_id: UUID,
    expected_version: str,
    key: str,
    text: str,
) -> InteractionReceipt:
    return await runtime.interact(
        actor,
        InteractionRequest(
            session_id=session_id,
            run_id=run_id,
            expected_version=expected_version,
            idempotency_key=key,
            text=text,
        ),
    )


def approval_decision(
    approval_id: UUID,
    expected_version: str,
    decision: Literal["approved", "rejected", "cancelled"],
) -> ApprovalDecision:
    # centn_example.approvals.resolve checks this exact idempotency identity.
    return ApprovalDecision(
        approval_id=approval_id,
        expected_version=expected_version,
        decision=decision,
        idempotency_key=f"approval:{approval_id}:{expected_version}:{decision}",
    )


if __name__ == "__main__":
    from uuid import uuid4

    print(approval_decision(uuid4(), "0", "approved").model_dump_json(by_alias=True, indent=2))
    print("Declaration only. Pass this to the authorized host approvals.resolve port.")

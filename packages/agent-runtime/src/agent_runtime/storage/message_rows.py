"""Validate one applied inter-agent mailbox row before it enters a model context."""

from collections.abc import Mapping

from agent_runtime.contracts.messages import AgentEnvelope, message_adapter
from agent_runtime.storage.uow import errors


def parse_agent_message_row(row: Mapping[str, object]) -> AgentEnvelope:
    body = row["payload"]
    if not isinstance(body, dict) or set(body) != {"payload", "resourceRefs"}:
        errors.version_conflict("agent mailbox payload has an invalid envelope")
    return message_adapter.validate_python(
        {
            "kind": row["kind"],
            "protocolRevision": row["protocol_revision"],
            "messageId": row["id"],
            "correlationId": row["correlation_id"],
            "causationId": row["causation_id"],
            "senderRunId": row["sender_run_id"],
            "recipientRunId": row["recipient_run_id"],
            "taskId": row["task_id"],
            "recipientSeq": str(row["recipient_seq"]),
            "createdAt": row["created_at"],
            "deadlineAt": row["deadline_at"],
            "resourceRefs": body["resourceRefs"],
            "traceId": row["trace_id"],
            "payload": body["payload"],
        }
    )


__all__ = ["parse_agent_message_row"]

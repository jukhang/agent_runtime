"""Stable admission fingerprints independent of bearer credentials."""

from agent_runtime.contracts.canonical import CanonicalJSONError, content_digest
from agent_runtime.contracts.failure import runtime_failure
from agent_runtime.contracts.requests import SubmitRequest
from agent_runtime.contracts.transactions import ActorContext


def submission_digest(actor: ActorContext, request: SubmitRequest) -> str:
    actor = ActorContext.model_validate(actor.model_dump())
    request = SubmitRequest.model_validate(request.model_dump())
    payload = {
        "scopeId": actor.scope_id,
        "actorId": actor.actor_id,
        "request": request.model_dump(mode="json", by_alias=True, exclude={"idempotency_key"}),
    }
    try:
        return content_digest(payload)
    except CanonicalJSONError as exc:
        raise runtime_failure(
            "INVALID_INPUT",
            category="validation",
            public_message="The submission request has no canonical JSON encoding.",
        ) from exc

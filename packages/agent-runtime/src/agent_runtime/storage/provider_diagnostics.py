"""Private, content-addressed stream diagnostics anchored by ModelAttempt.failure."""

from agent_runtime.context_payloads import BlobContextPayloadStore
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.model import ProviderRequest


class BlobProviderDiagnostics:
    def __init__(self, payloads: BlobContextPayloadStore) -> None:
        self._payloads = payloads

    async def __call__(self, request: ProviderRequest, detail: dict[str, object]) -> str:
        record = {**detail, "runId": str(request.run_id), "model": request.model.model_name}
        return await self._payloads.put(request.scope_id, record, content_digest(record))

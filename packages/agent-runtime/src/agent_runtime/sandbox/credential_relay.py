"""Host-side file relay; workload has no network or service credentials."""

import base64
from uuid import UUID

from agent_runtime.contracts.credentials import FilteredResponse, GrantRef, ProxyRequest
from agent_runtime.credentials.ingress import CredentialServiceClient
from agent_runtime.sandbox.opensandbox import OpenSandboxBackend


class SandboxCredentialRelay:
    def __init__(self, backend: OpenSandboxBackend, client: CredentialServiceClient) -> None:
        self._backend, self._client = backend, client

    async def exchange(self, execution_id: UUID, grant: GrantRef) -> FilteredResponse:
        if execution_id != grant.execution_id:
            raise ValueError("relay execution/grant mismatch")
        payload = base64.b64decode(
            (
                await self._backend._guard("credential_read", {"execution_id": str(execution_id)})
            ).strip(),
            validate=True,
        )
        if len(payload) > 32768:
            raise ValueError("relay request exceeds bound")
        request = ProxyRequest.model_validate_json(payload)
        if request.grant != grant:
            raise ValueError("workload selected an unauthorized grant")
        response = await self._client.proxy(request)
        await self._backend._guard(
            "credential_write",
            {
                "execution_id": str(execution_id),
                "request_id": str(request.request_id),
                "response": base64.b64encode(
                    response.model_dump_json(by_alias=True).encode()
                ).decode(),
            },
        )
        return response

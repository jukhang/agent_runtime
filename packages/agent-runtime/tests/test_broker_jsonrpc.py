import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from agent_runtime.contracts.credentials import FilteredResponse, GrantRef
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.credentials.jsonrpc import BrokerJsonRpcTransport


@pytest.mark.asyncio
async def test_broker_jsonrpc_uses_private_blob_and_matches_response_id():
    actor = ActorContext(scope_id="scope", actor_id="user", auth_revision="1", grants_ref="g")
    now = datetime.now(UTC)
    grant = GrantRef(
        grant_id=uuid4(), scope_id="scope", run_id=uuid4(), execution_id=uuid4(),
        execution_generation="1", revision="1", issued_at=now,
        expires_at=now + timedelta(minutes=1),
    )

    class Blobs:
        stored = {}

        async def put_private(self, caller, request_id, data):
            assert caller == actor
            self.stored[str(request_id)] = data
            return str(request_id)

        async def read(self, caller, ref, max_bytes):
            assert caller == actor
            return self.stored[ref]

    blobs = Blobs()

    class Broker:
        async def proxy(self, caller, request):
            assert caller == actor and request.grant == grant and request.action == "mcp"
            sent = blobs.stored[request.request_payload_ref]
            assert hashlib.sha256(sent).hexdigest() == request.request_digest
            rpc = json.loads(sent)
            assert rpc["params"]["_meta"] == {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientInfo": {"name": "centn-runtime", "version": "1"},
                "io.modelcontextprotocol/clientCapabilities": {},
            }
            response = json.dumps(
                {"jsonrpc": "2.0", "id": rpc["id"], "result": {"tools": []}}
            ).encode()
            blobs.stored["reply"] = response
            return FilteredResponse(
                request_id=request.request_id, grant_id=grant.grant_id,
                status_code=200, body_ref="reply",
                body_digest=hashlib.sha256(response).hexdigest(), size_bytes=len(response),
                filter_policy={"key": {"namespace": "test", "key": "filter"}, "revision": "1"},
                audit_ref="audit-1", disposition="allowed",
            )

    transport = BrokerJsonRpcTransport(
        broker=Broker(), blobs=blobs, actor=actor, grant=grant, action="mcp",
        mcp_client_info={"name": "centn-runtime", "version": "1"},
    )
    assert await transport.request("tools/list", {}) == {"tools": []}
    with pytest.raises(ValueError, match="host-owned"):
        await transport.request("tools/list", {"_meta": {}})

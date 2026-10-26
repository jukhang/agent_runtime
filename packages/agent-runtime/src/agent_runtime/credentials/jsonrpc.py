"""Fixed-destination JSON-RPC through the Credential Broker."""

import hashlib
import json
from collections.abc import Mapping
from uuid import uuid4

from pydantic import JsonValue, TypeAdapter

from agent_runtime.contracts.credentials import GrantRef, ProxyRequest
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.credentials.broker import CredentialBroker
from agent_runtime.credentials.ports import BrokerBlobs


class BrokerJsonRpcTransport:
    """Network mediation only; effectful calls still require a durable ToolHost ledger."""

    def __init__(
        self,
        *,
        broker: CredentialBroker,
        blobs: BrokerBlobs,
        actor: ActorContext,
        grant: GrantRef,
        action: str,
        max_response_bytes: int = 4_194_304,
        mcp_client_info: Mapping[str, JsonValue] | None = None,
    ) -> None:
        self._broker, self._blobs = broker, blobs
        self._actor, self._grant, self._action = actor, grant, action
        self._max_response_bytes = max_response_bytes
        self._mcp_client_info = dict(mcp_client_info) if mcp_client_info is not None else None

    async def request(self, method: str, params: Mapping[str, JsonValue]) -> JsonValue:
        request_id = uuid4()
        request_params = dict(params)
        if self._mcp_client_info is not None:
            if "_meta" in request_params:
                raise ValueError("MCP reserved request metadata is host-owned")
            request_params["_meta"] = {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientInfo": self._mcp_client_info,
                "io.modelcontextprotocol/clientCapabilities": {},
            }
        rpc = {"jsonrpc": "2.0", "method": method, "params": request_params, "id": str(request_id)}
        payload = json.dumps(rpc, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        digest = hashlib.sha256(payload).hexdigest()
        ref = await self._blobs.put_private(self._actor, request_id, payload)
        response = await self._broker.proxy(
            self._actor,
            ProxyRequest(
                grant=self._grant,
                request_id=request_id,
                action=self._action,
                request_payload_ref=ref,
                request_digest=digest,
            ),
        )
        if response.disposition != "allowed" or not 200 <= response.status_code < 300:
            raise ValueError("Credential Broker blocked or rejected the request")
        body = await self._blobs.read(self._actor, response.body_ref, self._max_response_bytes)
        if hashlib.sha256(body).hexdigest() != response.body_digest:
            raise ValueError("JSON-RPC response digest differs from the Broker record")
        if body.startswith(b"event:") or body.startswith(b"data:"):
            data_lines = [
                line[5:].strip() for line in body.splitlines() if line.startswith(b"data:")
            ]
            if len(data_lines) != 1:
                raise ValueError("JSON-RPC stream returned an ambiguous response")
            body = data_lines[0]
        try:
            message = json.loads(body)
        except (ValueError, UnicodeDecodeError) as exc:
            raise ValueError("Response is not valid JSON-RPC") from exc
        if (
            not isinstance(message, dict)
            or message.get("jsonrpc") != "2.0"
            or message.get("id") != str(request_id)
        ):
            raise ValueError("JSON-RPC response identity does not match its request")
        if "error" in message:
            raise ValueError("Remote endpoint returned a protocol error")
        if "result" not in message:
            raise ValueError("JSON-RPC response has no result")
        return TypeAdapter(JsonValue).validate_python(message["result"])


__all__ = ["BrokerJsonRpcTransport"]

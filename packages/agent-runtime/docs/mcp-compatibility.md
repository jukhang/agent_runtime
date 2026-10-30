# MCP Compatibility (0.2.0)

Standard MCP uses initialize, supported-version negotiation and initialized
notification. Streamable HTTP accepts JSON or SSE, tracks session IDs, and closes
the session on exit. These mechanisms are delegated to the optional official
Python MCP client, not reimplemented in an application. No arbitrary-version
acceptance, automatic write retry, fabricated resultType or fallback downgrade.

`agent_runtime.mcp.connect_mcp(url, http_client=owned_client)` is the standard
entry point. It negotiates the version supported by the official client and the
server; `expected_version` optionally pins a reviewed deployment. Tools are
discovery-only unless explicitly included in `allowed_tools`. Endpoint approval,
identity, credentials and egress remain host responsibilities; ToolHost remains
mandatory for Agent execution, ACL, approvals and effects. The provided HTTP
client must disable redirects. Closing the connection does not prove a remote
effect stopped. Request deadlines are absolute and tool calls are not retried.

The former server/discover + resultType protocol is retained only through
`McpClient(..., profile="legacy-discovery")`, for explicitly configured existing
Broker deployments. The legacy Broker handler is not a standard MCP session
adapter. Never select this profile because standard initialization failed.

McpToolBinding accepts a protocol date rather than one fixed literal. Installation
must verify the negotiated version equals the immutable reviewed binding.
Changing endpoint, credentials, schemas or protocol requires independent review.
No schema database migration is required for broadening this JSON contract.

Upgrade: install `agent-runtime[mcp,provider,storage]` from the new wheel. Core
imports do not require the official MCP package. Applications no longer need a
separate MCP dependency or a private protocol shim. Default McpClient transports
must now implement standard initialization; old transports explicitly select the
legacy profile. This API behavior change is why the release is 0.2.0.

Verification must cover standard protocol versions, initialization notification,
session cleanup, SSE/JSON, explicit pins, unknown versions, denied tool names,
closed sessions, errors, plus legacy Broker regression and the real supplied
service. Native Broker session support and application Agent registration are
separate integration concerns; do not claim they passed from discovery alone.

## Verified Upgrade

Local 0.2.0 wheel: full SDK tests 1450 passed / 397 external-service-gated skipped;
focused standard/legacy MCP tests 16 passed; MCP types and Ruff passed. An isolated
installation of only the mcp extra imports without SQLAlchemy or Example packages.
Real service discovery from outside the repository negotiated 2025-11-25 and
listed 21 tools. The reviewed read call returned a server business serialization
error, which correctly remained failure rather than being converted to success.
Centn Agent was upgraded from its wheel dependency, its 28 tests passed, and its
real Qwen/SQL ToolHost round-trip completed. See the consuming application's
sdk-capability-tests.md for run identities and the final artifact digest.

"""MCP adapters use a trusted transport; credentials never reach model arguments."""

from agent_runtime.credentials.jsonrpc import BrokerJsonRpcTransport
from agent_runtime.mcp.client import McpClient, McpReadTool, McpToolInfo
from agent_runtime.mcp.session import connect_mcp
from agent_runtime.mcp.write import McpIdempotentWriteTool, McpKeyedEffectClient

__all__ = [
    "BrokerJsonRpcTransport",
    "McpClient",
    "McpReadTool",
    "McpToolInfo",
    "McpIdempotentWriteTool",
    "McpKeyedEffectClient",
    "connect_mcp",
]

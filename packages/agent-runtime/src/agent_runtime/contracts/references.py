"""Reference shape validation is separate from registry and access checks.

DS-P02-00 (Q41): a capability is identified by ``(namespace, key, revision)``. The
namespace and key are persistent machine keys, so two registries can publish the same
key without colliding, and the revision is the only mutable-selection dimension. The
capability *kind* is a semantic type, never a transport (MCP/A2A) or an execution mode
(code): a remote tool is still a ``tool`` and its transport lives in its manifest.
"""

from typing import Literal

from agent_runtime.contracts.base import Contract, Identifier, MachineKey, Revision

type CapabilityKind = Literal["model", "tool", "skill", "prompt", "hook", "knowledge", "agent"]

#: Single source of truth shared with the storage catalog's ``capability.kind`` CHECK.
CAPABILITY_KINDS: tuple[CapabilityKind, ...] = (
    "model",
    "tool",
    "skill",
    "prompt",
    "hook",
    "knowledge",
    "agent",
)


class CapabilityKey(Contract):
    """Stable non-scoped identity of a capability inside one namespace."""

    namespace: MachineKey
    key: MachineKey


class CapabilityRef(Contract):
    """Public reference to one fixed capability revision."""

    key: CapabilityKey
    revision: Revision


class ResourceScope(Contract):
    scope_id: Identifier
    session_id: Identifier | None = None
    owner_id: Identifier | None = None


class ResourceRef(Contract):
    resource_type: Identifier
    resource_id: Identifier
    resource_version: Identifier | None = None
    scope: ResourceScope

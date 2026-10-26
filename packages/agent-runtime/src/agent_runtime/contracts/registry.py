"""Published capability manifests contain references, never executable objects.

DS-P02-00 (Q41): ``CapabilityManifest`` freezes what ``capability_revision.manifest``
may contain. The kind is a semantic capability type and the payload is a discriminated
spec, so a manifest cannot silently declare a kind that disagrees with its content, and
the recorded ``content_digest`` is the canonical digest of that payload.
"""

from pathlib import PurePosixPath
from typing import Annotated, Literal, Self

from pydantic import Field, TypeAdapter, field_validator, model_validator

from agent_runtime.contracts.base import Contract, Identifier, Revision
from agent_runtime.contracts.canonical import content_digest
from agent_runtime.contracts.model import ModelConfig
from agent_runtime.contracts.references import CapabilityKey, CapabilityRef
from agent_runtime.contracts.tools import ToolSpec
from agent_runtime.contracts.views import Digest
from agent_runtime.contracts.workspace import WorkspaceFile


class HookSpec(Contract):
    ref: CapabilityRef
    handler: CapabilityRef
    stage: Literal["input", "context", "tool_arguments", "before_execute", "output", "terminal"]
    order: Annotated[int, Field(strict=True, ge=0)]
    timeout_seconds: Annotated[int, Field(strict=True, ge=1)]
    failure_policy: Literal["block", "notify_retry"]
    replay: Literal["pure", "ledger"]

    @model_validator(mode="after")
    def no_fail_open_safety_hook(self) -> Self:
        if self.failure_policy == "notify_retry" and self.stage != "terminal":
            raise ValueError("only terminal notifications may fail without blocking execution")
        return self


class PromptSpec(Contract):
    ref: CapabilityRef
    template_ref: Identifier
    content_digest: Digest
    variables: tuple[Identifier, ...]

    @model_validator(mode="after")
    def unique_variables(self) -> Self:
        if len(self.variables) != len(set(self.variables)):
            raise ValueError("duplicate prompt variable")
        return self


class KnowledgeBinding(Contract):
    ref: CapabilityRef
    source: CapabilityRef
    retrieval_policy: CapabilityRef
    citation_policy: CapabilityRef
    access_policy: CapabilityRef


class SkillSpec(Contract):
    ref: CapabilityRef
    source: CapabilityRef
    git_commit: Annotated[str, Field(pattern=r"^(?:[a-f0-9]{40}|[a-f0-9]{64})$")] | None
    git_subdir: str | None = None
    bundle_digest: Digest
    instruction_path: str
    files: Annotated[tuple[WorkspaceFile, ...], Field(min_length=1)]
    required_tools: tuple[CapabilityRef, ...]

    @field_validator("git_subdir")
    @classmethod
    def safe_git_subdir(cls, value: str | None) -> str | None:
        if value is None:
            return None
        path = PurePosixPath(value)
        if (
            not value
            or path.is_absolute()
            or "\\" in value
            or ":" in value
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
            or any(part in {"", ".", ".."} for part in value.split("/"))
        ):
            raise ValueError("Skill Git subdirectory must be a canonical relative POSIX path")
        return value

    @model_validator(mode="after")
    def instruction_is_in_bundle(self) -> Self:
        paths = [entry.path for entry in self.files]
        if len(paths) != len(set(paths)) or self.instruction_path not in paths:
            raise ValueError("skill requires distinct files and a bundled instruction path")
        keys = [tool.key for tool in self.required_tools]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate skill tool binding")
        return self


class RemoteAgentSpec(Contract):
    ref: CapabilityRef
    endpoint: CapabilityRef
    protocol: CapabilityRef
    card_digest: Digest
    credential_policy: CapabilityRef
    can_query_task: Annotated[bool, Field(strict=True)]
    can_cancel_task: Annotated[bool, Field(strict=True)]
    idempotency: Literal["provider_key", "query", "unknown"]

    @model_validator(mode="after")
    def query_requires_capability(self) -> Self:
        if self.idempotency == "query" and not self.can_query_task:
            raise ValueError("query recovery requires remote task lookup")
        return self


class ModelCapability(Contract):
    kind: Literal["model"]
    spec: ModelConfig


class ToolCapability(Contract):
    kind: Literal["tool"]
    spec: ToolSpec


class SkillCapability(Contract):
    kind: Literal["skill"]
    spec: SkillSpec


class PromptCapability(Contract):
    kind: Literal["prompt"]
    spec: PromptSpec


class HookCapability(Contract):
    kind: Literal["hook"]
    spec: HookSpec


class KnowledgeCapability(Contract):
    kind: Literal["knowledge"]
    spec: KnowledgeBinding


class AgentCapability(Contract):
    kind: Literal["agent"]
    spec: RemoteAgentSpec


type CapabilitySpec = Annotated[
    ModelCapability
    | ToolCapability
    | SkillCapability
    | PromptCapability
    | HookCapability
    | KnowledgeCapability
    | AgentCapability,
    Field(discriminator="kind"),
]
capability_spec_adapter: TypeAdapter[CapabilitySpec] = TypeAdapter(CapabilitySpec)


class CapabilityManifest(Contract):
    """One immutable capability revision: identity, semantic kind, payload and digest."""

    schema_revision: Literal[1] = 1
    ref: CapabilityRef
    status: Literal["draft", "active", "superseded", "revoked"]
    content_digest: Digest
    spec: CapabilitySpec

    @model_validator(mode="after")
    def spec_matches_manifest(self) -> Self:
        if self.spec.spec.ref != self.ref:
            raise ValueError("capability manifest spec belongs to another revision")
        payload = self.spec.spec.model_dump(mode="json", by_alias=True)
        if content_digest(payload) != self.content_digest:
            raise ValueError("capability manifest digest does not match its spec payload")
        return self


class CapabilityAvailability(Contract):
    """Which revisions of one capability key are currently resolvable."""

    key: CapabilityKey
    active_revision: Revision | None
    revoked_revisions: tuple[Revision, ...] = ()

    @model_validator(mode="after")
    def revoked_is_not_active(self) -> Self:
        if self.active_revision is not None and self.active_revision in self.revoked_revisions:
            raise ValueError("the active revision cannot be revoked")
        if len(self.revoked_revisions) != len(set(self.revoked_revisions)):
            raise ValueError("duplicate revoked revision")
        return self

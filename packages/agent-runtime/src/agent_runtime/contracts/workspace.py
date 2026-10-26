"""Portable file checkpoints, intentionally excluding interpreter memory."""

from pathlib import PurePosixPath
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from agent_runtime.contracts.base import Contract, Identifier, MachineKey, Revision
from agent_runtime.contracts.transactions import ActorContext
from agent_runtime.contracts.views import Digest


class WorkspaceVersion(Contract):
    scope_id: Identifier
    workspace_id: UUID
    owner_run_id: UUID
    generation: Revision
    revision: Revision


class WorkspaceFile(Contract):
    path: str
    blob_ref: Identifier
    digest: Digest
    size_bytes: Annotated[int, Field(strict=True, ge=0)]
    executable: Annotated[bool, Field(strict=True)] = False

    @field_validator("path")
    @classmethod
    def safe_relative_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if (
            not value
            or path.is_absolute()
            or "\\" in value
            or ":" in value
            or any(ord(char) < 32 or ord(char) == 127 for char in value)
            or any(part in {"", ".", ".."} for part in value.split("/"))
        ):
            raise ValueError("checkpoint path must be a canonical relative POSIX path")
        return value


class ReadyCheckpoint(Contract):
    checkpoint_id: UUID
    workspace: WorkspaceVersion
    manifest_ref: Identifier
    manifest_digest: Digest
    files: tuple[WorkspaceFile, ...]
    total_bytes: Annotated[int, Field(strict=True, ge=0)]
    status: Literal["ready"]

    @model_validator(mode="after")
    def consistent_manifest(self) -> Self:
        paths = {item.path for item in self.files}
        if len(paths) != len(self.files):
            raise ValueError("duplicate checkpoint path")
        if any(str(parent) in paths for path in paths for parent in PurePosixPath(path).parents):
            raise ValueError("checkpoint file cannot be a parent directory")
        if sum(item.size_bytes for item in self.files) != self.total_bytes:
            raise ValueError("checkpoint byte count mismatch")
        return self


class WorkspaceCheckpointRequest(Contract):
    actor: ActorContext
    expected: WorkspaceVersion
    idempotency_key: MachineKey

    @model_validator(mode="after")
    def same_scope(self) -> Self:
        if self.actor.scope_id != self.expected.scope_id:
            raise ValueError("workspace actor scope mismatch")
        return self


class WorkspaceRestoreRequest(WorkspaceCheckpointRequest):
    checkpoint: ReadyCheckpoint
    target_generation: Revision

    @model_validator(mode="after")
    def same_workspace_new_generation(self) -> Self:
        source = self.checkpoint.workspace
        if (source.scope_id, source.workspace_id, source.owner_run_id) != (
            self.expected.scope_id,
            self.expected.workspace_id,
            self.expected.owner_run_id,
        ):
            raise ValueError("restore requires the same workspace owner")
        if int(self.target_generation) <= max(
            int(source.generation), int(self.expected.generation)
        ):
            raise ValueError("restore must fence the old execution generation")
        if int(source.revision) > int(self.expected.revision):
            raise ValueError("checkpoint revision is newer than the expected workspace")
        return self


class RestoredWorkspaceRef(Contract):
    checkpoint_id: UUID
    workspace: WorkspaceVersion
    verified_manifest_digest: Digest
    verification_ref: Identifier
    status: Literal["ready"]

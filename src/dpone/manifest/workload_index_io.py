"""Bounded, project-identity-confined workload-index reads."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.contracts.workload_index import (
    MAX_WORKLOAD_INDEX_BYTES,
    MAX_WORKLOAD_INDEX_DEPTH,
    MAX_WORKLOAD_INDEX_NODES,
    MAX_WORKLOAD_INDEX_TOKENS,
)
from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import (
    ConfinedFileError,
    project_relative_path,
    read_confined_file_snapshot,
)
from dpone.manifest.project_root import (
    ProjectRootError,
    ProjectRootIdentity,
    verify_project_root,
)

WORKLOAD_INDEX_LIMITS = BoundedYamlLimits(
    max_bytes=MAX_WORKLOAD_INDEX_BYTES,
    max_tokens=MAX_WORKLOAD_INDEX_TOKENS,
    max_depth=MAX_WORKLOAD_INDEX_DEPTH,
    max_nodes=MAX_WORKLOAD_INDEX_NODES,
)


class WorkloadIndexReadError(ValueError):
    """A workload index could not be read through its captured project root."""

    def __init__(self, message: str, *, confinement: bool) -> None:
        super().__init__(message)
        self.confinement = confinement


@dataclass(frozen=True, slots=True)
class LoadedWorkloadIndex:
    """Stable project-confined bytes and their parsed mapping."""

    relative_path: str
    payload: Mapping[str, Any]
    content: bytes
    content_sha256: str


def load_workload_index(
    root_identity: ProjectRootIdentity,
    path: str | Path,
    *,
    label: str,
) -> LoadedWorkloadIndex:
    """Read one bounded workload index through one captured project identity."""

    relative = workload_index_relative_path(root_identity, path)
    try:
        snapshot = read_confined_file_snapshot(
            root_identity.path,
            relative,
            max_bytes=WORKLOAD_INDEX_LIMITS.max_bytes,
            root_identity=root_identity,
        )
    except ConfinedFileError as exc:
        raise WorkloadIndexReadError(
            f"{label} workload index could not be opened through the captured project root.",
            confinement=True,
        ) from exc
    try:
        payload = load_bounded_yaml(snapshot.content, limits=WORKLOAD_INDEX_LIMITS)
    except BoundedYamlError as exc:
        raise WorkloadIndexReadError(
            f"{label} workload index is not valid bounded YAML/JSON.",
            confinement=False,
        ) from exc
    if not isinstance(payload, Mapping):
        raise WorkloadIndexReadError(
            f"{label} workload index must be a JSON/YAML object.",
            confinement=False,
        )
    return LoadedWorkloadIndex(
        relative_path=relative,
        payload=payload,
        content=snapshot.content,
        content_sha256=snapshot.sha256,
    )


def workload_index_relative_path(
    root_identity: ProjectRootIdentity,
    path: str | Path,
) -> str:
    """Validate one workload-index path against a captured project identity."""

    try:
        verify_project_root(root_identity)
        return project_relative_path(root_identity.path, Path(path))
    except (ConfinedFileError, ProjectRootError) as exc:
        raise WorkloadIndexReadError(
            "Workload-index path is outside the captured project authority.",
            confinement=True,
        ) from exc


__all__ = [
    "LoadedWorkloadIndex",
    "ProjectRootIdentity",
    "WORKLOAD_INDEX_LIMITS",
    "WorkloadIndexReadError",
    "load_workload_index",
    "workload_index_relative_path",
]

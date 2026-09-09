"""Connector-neutral port for immutable artifact registry access."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol, runtime_checkable

_SHA256_PREFIX = "sha256:"


class ArtifactRegistryError(RuntimeError):
    """Base error that does not expose backend-specific exception payloads."""


class ArtifactRegistryKeyError(ValueError):
    """Raised before I/O when a logical registry key is unsafe or mutable."""


class ArtifactRegistryObjectNotFound(ArtifactRegistryError):
    """Raised when an exact pinned object is absent."""


class ArtifactRegistryUnavailable(ArtifactRegistryError):
    """Raised when a bounded registry operation cannot complete."""


class ArtifactRegistryReadLimitExceeded(ArtifactRegistryError):
    """Raised when an object body exceeds its preflight metadata limit."""


def artifact_registry_scope_id(
    *,
    kind: str,
    attributes: Mapping[str, str | None],
) -> str:
    """Return the historical v1 registry identity."""

    return _artifact_registry_scope_id(
        schema="dpone.artifact-registry-scope.v1",
        kind=kind,
        attributes=attributes,
    )


def exact_artifact_registry_scope_id(
    *,
    kind: str,
    attributes: Mapping[str, str | None],
) -> str:
    """Return the endpoint-bound v2 registry identity."""

    return _artifact_registry_scope_id(
        schema="dpone.artifact-registry-scope.v2",
        kind=kind,
        attributes=attributes,
    )


def _artifact_registry_scope_id(
    *,
    schema: str,
    kind: str,
    attributes: Mapping[str, str | None],
) -> str:
    payload = {
        "schema": schema,
        "kind": kind,
        **attributes,
    }
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return _SHA256_PREFIX + hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True, slots=True)
class ArtifactMetadata:
    """Non-secret metadata for one exact registry object."""

    key: PurePosixPath
    size_bytes: int
    advertised_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class CreateResult:
    """Result of an atomic create-if-absent operation."""

    created: bool
    metadata: ArtifactMetadata


class ArtifactRegistryReader(Protocol):
    """Read boundary for exact immutable artifact objects."""

    def stat(self, key: PurePosixPath) -> ArtifactMetadata:
        """Return bounded metadata for one exact object without listing."""

    def download_file(self, key: PurePosixPath, destination: Path, *, max_bytes: int) -> None:
        """Download one exact object to a caller-owned destination."""


class ArtifactRegistryWriter(Protocol):
    """Write boundary for immutable create-if-absent publication."""

    def create_file(self, key: PurePosixPath, source: Path) -> CreateResult:
        """Create one object or report the already-existing object."""


class ArtifactRegistry(ArtifactRegistryReader, ArtifactRegistryWriter, Protocol):
    """Compatibility port combining reader and writer capabilities."""


@runtime_checkable
class ArtifactRegistryAuthority(Protocol):
    """Capability exposing a credential-free exact registry identity."""

    @property
    def authority_scope_id(self) -> str:
        """Return the endpoint-bound identity of the constructed registry adapter."""


__all__ = [
    "ArtifactMetadata",
    "ArtifactRegistry",
    "ArtifactRegistryAuthority",
    "ArtifactRegistryReader",
    "ArtifactRegistryWriter",
    "ArtifactRegistryError",
    "ArtifactRegistryKeyError",
    "ArtifactRegistryObjectNotFound",
    "ArtifactRegistryReadLimitExceeded",
    "ArtifactRegistryUnavailable",
    "CreateResult",
    "artifact_registry_scope_id",
    "exact_artifact_registry_scope_id",
]

"""Shared fail-closed helpers for Airflow artifact delivery use cases."""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import stat
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, cast

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.ports.artifact_registry import (
    ArtifactRegistry,
    ArtifactRegistryAuthority,
    ArtifactRegistryError,
    ArtifactRegistryKeyError,
    ArtifactRegistryObjectNotFound,
    ArtifactRegistryReader,
    ArtifactRegistryReadLimitExceeded,
)
from dpone.runtime.airflow_artifact_delivery_models import AirflowArtifactDeliveryError, MaterializeRequest
from dpone.runtime.deployment_cache_common import DeploymentCacheError


def require_registry_ref(
    deployment: Mapping[str, Any],
    index: Mapping[str, Any],
    expected: str,
) -> None:
    refs: set[object] = set()
    for value in (deployment.get("runtime_artifact_delivery"), index.get("runtime_artifact_delivery")):
        if isinstance(value, Mapping):
            refs.add(value.get("artifact_registry_ref"))
            source = value.get("source")
            if isinstance(source, Mapping):
                refs.add(source.get("artifact_registry_ref"))
    if refs != {expected}:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_REF_MISMATCH",
            "requested artifact registry differs from deployment configuration",
        )


def download(
    registry: ArtifactRegistryReader,
    key: PurePosixPath,
    destination: Path,
    *,
    max_bytes: int,
) -> None:
    try:
        registry.download_file(key, destination, max_bytes=max_bytes)
    except ArtifactRegistryObjectNotFound as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            "pinned artifact object disappeared during download",
        ) from exc
    except ArtifactRegistryReadLimitExceeded as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_CHECKSUM_MISMATCH",
            "pinned artifact body exceeds its declared metadata size",
        ) from exc
    except ArtifactRegistryError as exc:
        raise registry_unavailable() from exc


def verify_download(
    path: Path,
    *,
    expected_size: int,
    expected_sha256: str | None,
    max_object_bytes: int,
) -> None:
    size = path.stat().st_size
    if size > max_object_bytes:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_OBJECT_TOO_LARGE",
            "downloaded artifact exceeds the configured per-object limit",
        )
    if size != expected_size:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_CHECKSUM_MISMATCH",
            "downloaded artifact size differs from remote metadata",
        )
    if expected_sha256 is not None and file_sha256(path).lower() != expected_sha256.lower():
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_CHECKSUM_MISMATCH",
            "downloaded artifact checksum differs from its declared digest",
        )


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            "downloaded artifact manifest is invalid",
        ) from exc
    if not isinstance(payload, dict):
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
            "downloaded artifact manifest must be an object",
        )
    return payload


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def from_cache_error(exc: DeploymentCacheError) -> AirflowArtifactDeliveryError:
    return AirflowArtifactDeliveryError(exc.code, str(exc), details=dict(exc.details))


def registry_unavailable() -> AirflowArtifactDeliveryError:
    return AirflowArtifactDeliveryError(
        "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE",
        "artifact registry operation is unavailable",
    )


def require_registry_scope(
    registry: ArtifactRegistry,
    expected_scope_id: str | None,
) -> str:
    """Require explicit exact-authority capability before publication I/O."""

    if inspect.getattr_static(registry, "authority_scope_id", None) is None:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_SCOPE_INVALID",
            "artifact registry does not expose exact authority identity",
        )
    authority = cast(ArtifactRegistryAuthority, registry)
    try:
        scope_id = authority.authority_scope_id
    except ArtifactRegistryKeyError as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_SCOPE_INVALID",
            "artifact registry returned an invalid non-secret scope identity",
        ) from exc
    if not is_canonical_sha256_digest(scope_id):
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_SCOPE_INVALID",
            "artifact registry returned an invalid non-secret scope identity",
        )
    if expected_scope_id is not None and scope_id != expected_scope_id:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_SCOPE_MISMATCH",
            "configured artifact registry differs from the expected trusted scope",
        )
    return scope_id


def validate_materialization_target(request: MaterializeRequest) -> None:
    """Reject an existing unsafe cache root before registry or filesystem writes."""

    try:
        mode = request.cache_root.lstat().st_mode
    except FileNotFoundError:
        return
    except OSError as exc:
        raise AirflowArtifactDeliveryError(
            "DPONE_CACHE_PATH_ESCAPE",
            "materialization cache root metadata could not be inspected safely",
        ) from exc
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise AirflowArtifactDeliveryError(
            "DPONE_CACHE_PATH_ESCAPE",
            "materialization cache root must be a regular directory and must not be a symlink",
        )


@dataclass(frozen=True, slots=True)
class PinnedCacheRoot:
    """One open cache-root identity shared by the complete local install."""

    path: Path
    descriptor: int

    def __call__(self) -> None:
        try:
            current = os.stat(self.path, follow_symlinks=False)
        except OSError as exc:
            raise AirflowArtifactDeliveryError(
                "DPONE_CACHE_PATH_ESCAPE",
                "materialization cache root changed during execution",
            ) from exc
        opened = os.fstat(self.descriptor)
        if not stat.S_ISDIR(current.st_mode) or (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
            raise AirflowArtifactDeliveryError(
                "DPONE_CACHE_PATH_ESCAPE",
                "materialization cache root changed during execution",
            )


@contextmanager
def guard_cache_root(cache_root: Path) -> Iterator[PinnedCacheRoot]:
    """Pin one cache-root inode for checks and descriptor-relative installs."""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(cache_root, flags)
    guard = PinnedCacheRoot(cache_root, descriptor)

    try:
        guard()
        yield guard
    finally:
        os.close(descriptor)


__all__ = [
    "ArtifactRegistry",
    "ArtifactRegistryError",
    "ArtifactRegistryObjectNotFound",
    "ArtifactRegistryReader",
    "download",
    "file_sha256",
    "from_cache_error",
    "guard_cache_root",
    "PinnedCacheRoot",
    "read_json",
    "registry_unavailable",
    "require_registry_ref",
    "validate_materialization_target",
    "verify_download",
]

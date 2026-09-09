"""Public init-fetch facade and local ArtifactRegistry compatibility adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.ports.artifact_registry import CreateResult


import os
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from dpone.ports.artifact_registry import (
    ArtifactMetadata,
    ArtifactRegistryError,
    ArtifactRegistryKeyError,
    ArtifactRegistryObjectNotFound,
    ArtifactRegistryReadLimitExceeded,
    ArtifactRegistryUnavailable,
)
from dpone.runtime.deployment_cache_common import DeploymentCacheError, open_regular_file
from dpone.runtime.init_fetch_contract import (
    InitFetchedArtifact,
    InitFetchError,
    InitFetchPlan,
    InitFetchResult,
    RuntimeArtifactRef,
    build_init_fetch_plan,
    cache_relative_path,
)
from dpone.runtime.init_fetch_execution import (
    DEFAULT_MAX_ARTIFACT_COUNT,
    DEFAULT_MAX_TOTAL_BYTES,
    InitFetchAttestationVerifier,
    InitFetchExecutor,
    StagedInitFetchArtifact,
)
from dpone.runtime.runtime_init_fetch_plan import RuntimeInitFetchPlan
from dpone.runtime.runtime_init_fetch_plan_codec import decode_runtime_init_fetch_plan
from dpone.runtime.runtime_init_fetch_service import (
    RuntimeInitFetchAttestationVerifier,
    RuntimeInitFetchExecutor,
    StagedRuntimeArtifact,
)
from dpone.runtime.verified_pack_launcher import VerifiedPackCommand, VerifiedPackLauncher

DEFAULT_MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
_READ_CHUNK_BYTES = 64 * 1024


class LocalArtifactRegistry:
    """Read-only local adapter preserving the legacy init-fetch registry API."""

    def __init__(
        self,
        root: str | Path,
        *,
        max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES,
    ) -> None:
        self._root = Path(root).absolute()
        self._max_artifact_bytes = _positive_limit(
            "max_artifact_bytes",
            max_artifact_bytes,
        )

    def create_file(self, key: PurePosixPath, source: Path) -> CreateResult:
        """Reject writes because this compatibility adapter is read-only."""

        del key, source
        raise ArtifactRegistryUnavailable("local init-fetch registry adapter is read-only")

    def stat(self, key: PurePosixPath) -> ArtifactMetadata:
        """Return metadata for one exact no-follow local object."""

        path = self._path_for_key(key)
        try:
            descriptor = _open_local_file(path, root=self._root)
        except DeploymentCacheError as exc:
            raise _port_error(exc) from exc
        try:
            return ArtifactMetadata(key=key, size_bytes=os.fstat(descriptor).st_size)
        finally:
            os.close(descriptor)

    def download_file(
        self,
        key: PurePosixPath,
        destination: Path,
        *,
        max_bytes: int,
    ) -> None:
        """Stream one exact no-follow local object to caller-owned staging."""

        limit = min(_positive_limit("max_bytes", max_bytes), self._max_artifact_bytes)
        path = self._path_for_key(key)
        try:
            descriptor = _open_local_file(path, root=self._root)
        except DeploymentCacheError as exc:
            raise _port_error(exc) from exc
        created = False
        completed = False
        try:
            if os.fstat(descriptor).st_size > limit:
                raise ArtifactRegistryReadLimitExceeded("pinned artifact exceeds the read limit")
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
            target_descriptor = os.open(destination, flags, 0o600)
            created = True
            with os.fdopen(target_descriptor, "wb") as target:
                _copy_descriptor(descriptor, target, max_bytes=limit)
            completed = True
        except ArtifactRegistryError:
            raise
        except OSError as exc:
            raise ArtifactRegistryUnavailable("local artifact registry download failed") from exc
        finally:
            os.close(descriptor)
            if created and not completed:
                destination.unlink(missing_ok=True)

    def read_bytes(
        self,
        artifact_ref: str,
        *,
        declared_bytes: int | None = None,
    ) -> bytes:
        """Return one bounded descriptor snapshot through the legacy API."""

        relative = cache_relative_path(artifact_ref)
        if declared_bytes is not None:
            try:
                declared_bytes = _positive_limit("declared_bytes", declared_bytes)
            except ValueError as exc:
                raise InitFetchError(
                    "DPONE_CACHE_ARTIFACT_SIZE_INVALID",
                    "cache artifact declared bytes must be a positive integer",
                    artifact_ref=artifact_ref,
                ) from exc
            if declared_bytes > self._max_artifact_bytes:
                raise InitFetchError(
                    "DPONE_CACHE_ARTIFACT_TOO_LARGE",
                    "cache artifact declared size exceeds configured size limit",
                    artifact_ref=artifact_ref,
                )
        path = self._root.joinpath(*relative.parts)
        try:
            descriptor = _open_local_file(path, root=self._root)
        except DeploymentCacheError as exc:
            raise InitFetchError(exc.code, str(exc), artifact_ref=artifact_ref) from exc
        try:
            return _read_descriptor_snapshot(
                descriptor,
                artifact_ref=artifact_ref,
                declared_bytes=declared_bytes,
                max_artifact_bytes=self._max_artifact_bytes,
            )
        finally:
            os.close(descriptor)

    def resolve(self, artifact_ref: str) -> Path:
        """Return one existing no-follow in-registry path for compatibility."""

        relative = cache_relative_path(artifact_ref)
        path = self._root.joinpath(*relative.parts)
        try:
            descriptor = _open_local_file(path, root=self._root)
        except DeploymentCacheError as exc:
            raise InitFetchError(exc.code, str(exc), artifact_ref=artifact_ref) from exc
        os.close(descriptor)
        return path

    def _path_for_key(self, key: PurePosixPath) -> Path:
        if not isinstance(key, PurePosixPath):
            raise ArtifactRegistryKeyError("artifact registry key must be a relative POSIX path")
        try:
            relative = cache_relative_path(f"cache://{key.as_posix()}")
        except InitFetchError as exc:
            raise ArtifactRegistryKeyError("artifact registry key is invalid or mutable") from exc
        return self._root.joinpath(*relative.parts)


def _read_descriptor_snapshot(
    descriptor: int,
    *,
    artifact_ref: str,
    declared_bytes: int | None,
    max_artifact_bytes: int,
) -> bytes:
    size = os.fstat(descriptor).st_size
    if size > max_artifact_bytes:
        raise InitFetchError(
            "DPONE_CACHE_ARTIFACT_TOO_LARGE",
            "cache artifact exceeds configured size limit",
            artifact_ref=artifact_ref,
        )
    if declared_bytes is not None and size != declared_bytes:
        raise InitFetchError(
            "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH",
            "cache artifact size does not match init-fetch plan",
            artifact_ref=artifact_ref,
        )
    limit = declared_bytes if declared_bytes is not None else max_artifact_bytes
    chunks: list[bytes] = []
    total = 0
    try:
        while total <= limit:
            chunk = os.read(descriptor, min(_READ_CHUNK_BYTES, limit + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
    except OSError as exc:
        raise InitFetchError(
            "DPONE_CACHE_ARTIFACT_READ_FAILED",
            "cache artifact could not be read",
            artifact_ref=artifact_ref,
        ) from exc
    if total > max_artifact_bytes:
        raise InitFetchError(
            "DPONE_CACHE_ARTIFACT_TOO_LARGE",
            "cache artifact exceeds configured size limit",
            artifact_ref=artifact_ref,
        )
    if declared_bytes is not None and total != declared_bytes:
        raise InitFetchError(
            "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH",
            "cache artifact size does not match init-fetch plan",
            artifact_ref=artifact_ref,
        )
    return b"".join(chunks)


def _copy_descriptor(descriptor: int, target: BinaryIO, *, max_bytes: int) -> None:
    total = 0
    while total <= max_bytes:
        chunk = os.read(descriptor, min(_READ_CHUNK_BYTES, max_bytes + 1 - total))
        if not chunk:
            return
        total += len(chunk)
        if total > max_bytes:
            raise ArtifactRegistryReadLimitExceeded("pinned artifact exceeds the read limit")
        target.write(chunk)


def _open_local_file(path: Path, *, root: Path) -> int:
    return open_regular_file(
        path,
        missing_code="DPONE_CACHE_ARTIFACT_NOT_FOUND",
        invalid_code="DPONE_CACHE_PATH_ESCAPE",
        label="cache artifact",
        root=root,
    )


def _port_error(exc: DeploymentCacheError) -> ArtifactRegistryError:
    if exc.code == "DPONE_CACHE_ARTIFACT_NOT_FOUND":
        return ArtifactRegistryObjectNotFound("pinned artifact is not present")
    return ArtifactRegistryUnavailable("local artifact registry path is unavailable")


def _positive_limit(name: str, value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


__all__ = [
    "build_init_fetch_plan",
    "DEFAULT_MAX_ARTIFACT_BYTES",
    "DEFAULT_MAX_ARTIFACT_COUNT",
    "DEFAULT_MAX_TOTAL_BYTES",
    "InitFetchedArtifact",
    "InitFetchAttestationVerifier",
    "InitFetchError",
    "InitFetchExecutor",
    "InitFetchPlan",
    "InitFetchResult",
    "LocalArtifactRegistry",
    "RuntimeArtifactRef",
    "RuntimeInitFetchAttestationVerifier",
    "RuntimeInitFetchExecutor",
    "RuntimeInitFetchPlan",
    "decode_runtime_init_fetch_plan",
    "StagedRuntimeArtifact",
    "StagedInitFetchArtifact",
    "VerifiedPackCommand",
    "VerifiedPackLauncher",
]

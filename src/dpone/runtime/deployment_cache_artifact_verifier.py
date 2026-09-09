"""Constrained local artifact verification for Airflow deployment caches."""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path, PurePosixPath

from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    open_regular_file,
    require_path_without_symlinks,
)

DEFAULT_MAX_CACHE_ARTIFACT_BYTES = 256 * 1024 * 1024


class DeploymentCacheArtifactVerifier:
    """Resolve and verify pinned cache artifacts without external I/O."""

    def __init__(self, cache_root: Path, *, max_artifact_bytes: int) -> None:
        self._cache_root = cache_root
        self._max_artifact_bytes = max_artifact_bytes

    def resolve(self, *, release_dir: Path, artifact_ref: str, error_path: Path) -> Path:
        """Return a confined local path for one syntactically safe pinned reference."""

        if not artifact_ref.startswith("cache://"):
            raise DeploymentCacheError(
                "DPONE_CACHE_REFERENCE_INVALID",
                "indexed artifact reference must use cache://",
                path=error_path.as_posix(),
            )
        relative = PurePosixPath(artifact_ref.removeprefix("cache://"))
        if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
            raise DeploymentCacheError(
                "DPONE_CACHE_PATH_ESCAPE",
                "indexed artifact reference is unsafe",
                path=error_path.as_posix(),
            )
        if "current" in relative.parts:
            raise DeploymentCacheError(
                "DPONE_CACHE_UNPINNED_REFERENCE",
                "indexed artifact reference must not resolve current",
                path=error_path.as_posix(),
            )
        candidate = self._cache_root / relative.as_posix()
        require_path_without_symlinks(candidate, root=self._cache_root, error_path=error_path)
        _require_inside_root(
            candidate,
            root=self._cache_root,
            message="indexed artifact resolves outside the configured cache root",
            error_path=error_path,
        )
        _require_inside_root(
            candidate,
            root=release_dir,
            message="indexed artifact must resolve inside its pinned release directory",
            error_path=error_path,
        )
        return candidate

    def verify_file(self, path: Path, *, expected_sha256: str, declared_bytes: int | None) -> None:
        """Verify one already confined regular file against size and digest contracts."""

        try:
            descriptor = open_regular_file(
                path,
                missing_code="DPONE_CACHE_ARTIFACT_NOT_FOUND",
                invalid_code="DPONE_CACHE_ARTIFACT_READ_FAILED",
                label="indexed cache artifact",
                root=self._cache_root,
            )
        except DeploymentCacheError:
            raise
        try:
            file_stat = os.fstat(descriptor)
            if not stat.S_ISREG(file_stat.st_mode):
                raise DeploymentCacheError(
                    "DPONE_CACHE_ARTIFACT_NOT_FOUND",
                    "indexed cache artifact is not a regular file",
                    path=path.as_posix(),
                )
            if declared_bytes is not None and file_stat.st_size != declared_bytes:
                raise DeploymentCacheError(
                    "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH",
                    "indexed cache artifact size does not match the deployment index",
                    path=path.as_posix(),
                )
            if file_stat.st_size > self._max_artifact_bytes:
                raise DeploymentCacheError(
                    "DPONE_CACHE_ARTIFACT_TOO_LARGE",
                    "indexed cache artifact exceeds the configured size limit",
                    path=path.as_posix(),
                )
            actual_sha256 = _sha256_descriptor(descriptor, max_bytes=self._max_artifact_bytes)
        except _ArtifactTooLarge as exc:
            raise DeploymentCacheError(
                "DPONE_CACHE_ARTIFACT_TOO_LARGE",
                "indexed cache artifact exceeds the configured size limit",
                path=path.as_posix(),
            ) from exc
        except OSError as exc:
            raise DeploymentCacheError(
                "DPONE_CACHE_ARTIFACT_READ_FAILED",
                "indexed cache artifact could not be read",
                path=path.as_posix(),
            ) from exc
        finally:
            os.close(descriptor)
        if actual_sha256.lower() != expected_sha256.lower():
            raise DeploymentCacheError(
                "DPONE_CACHE_CHECKSUM_MISMATCH",
                "indexed cache artifact checksum does not match the release-set",
                path=path.as_posix(),
            )


class _ArtifactTooLarge(RuntimeError):
    pass


def _sha256_descriptor(descriptor: int, *, max_bytes: int) -> str:
    digest = hashlib.sha256()
    bytes_read = 0
    with os.fdopen(os.dup(descriptor), "rb") as handle:
        while chunk := handle.read(1024 * 1024):
            bytes_read += len(chunk)
            if bytes_read > max_bytes:
                raise _ArtifactTooLarge
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _require_inside_root(candidate: Path, *, root: Path, message: str, error_path: Path) -> None:
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise DeploymentCacheError(
            "DPONE_CACHE_PATH_ESCAPE",
            message,
            path=error_path.as_posix(),
        ) from exc


__all__ = ["DEFAULT_MAX_CACHE_ARTIFACT_BYTES", "DeploymentCacheArtifactVerifier"]

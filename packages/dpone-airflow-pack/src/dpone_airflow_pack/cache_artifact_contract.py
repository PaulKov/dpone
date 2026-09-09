"""Confined cache references and bounded artifact integrity checks."""

from __future__ import annotations

import errno
import hashlib
import os
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from dpone_airflow_pack.cache_activation_contract import (
    CacheActivationIdentity,
    pinned_cache_parts,
)
from dpone_airflow_pack.deployment_index_errors import AirflowDeploymentIndexError

DEFAULT_MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
_DIRECTORY_FLAGS = (
    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
_MUTABLE_ARTIFACT_ALIASES = frozenset({"current", "latest"})


@dataclass(frozen=True, slots=True)
class ConfinedCacheRead:
    """One confined read and its optional active-snapshot identity."""

    content: bytes
    activation: CacheActivationIdentity | None
    modified_at_epoch: float


def infer_cache_root(index_path: Path) -> Path:
    """Infer the cache root from a deployment index path.

    Prefer the deployment-cache root that owns ``current`` / ``activations`` /
    ``deployments`` over a parent directory that merely happens to be named
    ``.dpone-cache``. Nested smoke-only layouts such as
    ``/opt/airflow/.dpone-cache/smoke-v2-deployment/current/airflow-index.json``
    must resolve to ``…/smoke-v2-deployment``, not the pack-cache parent.
    """

    lexical_path = index_path.absolute()
    if lexical_path.parent.name == "current":
        return lexical_path.parent.parent
    for parent in lexical_path.parents:
        if parent.name in {"activations", "deployments"}:
            return parent.parent
    for parent in lexical_path.parents:
        if parent.name == ".dpone-cache":
            return parent
    return lexical_path.parent


def resolve_cache_artifact(
    artifact_ref: str,
    *,
    cache_root: str | Path,
) -> Path:
    """Resolve a ``cache://`` artifact ref while preventing path escape."""

    lexical_root = Path(cache_root).absolute()
    root = lexical_root.resolve(strict=False)
    relative = _cache_relative_path(artifact_ref)
    candidate = lexical_root / relative
    try:
        resolved = candidate.resolve(strict=False)
        resolved.relative_to(root)
    except ValueError as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_REF_UNSAFE",
            "cache artifact resolves outside the configured cache root",
            path=artifact_ref,
        ) from exc
    return candidate


def read_confined_cache_file(
    path: Path,
    *,
    cache_root: str | Path,
    max_bytes: int,
    allow_current_pointer: bool = False,
) -> bytes:
    """Read one cache file through a root descriptor with a hard byte limit."""

    return read_confined_cache_file_with_identity(
        path,
        cache_root=cache_root,
        max_bytes=max_bytes,
        allow_current_pointer=allow_current_pointer,
    ).content


def read_confined_cache_file_with_identity(
    path: Path,
    *,
    cache_root: str | Path,
    max_bytes: int,
    allow_current_pointer: bool = False,
) -> ConfinedCacheRead:
    """Read one cache file and retain identity from the same ``current`` resolution."""

    if max_bytes <= 0:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_SIZE_LIMIT_INVALID",
            "cache artifact size limit must be positive",
            path=path.as_posix(),
        )
    lexical_root = Path(cache_root).absolute()
    root = lexical_root.resolve(strict=False)
    lexical_path = path.absolute()
    try:
        relative_parts = lexical_path.relative_to(lexical_root).parts
    except ValueError as exc:
        try:
            relative_parts = lexical_path.relative_to(root).parts
        except ValueError:
            raise AirflowDeploymentIndexError(
                "DPONE_CACHE_ARTIFACT_REF_UNSAFE",
                "cache artifact path is outside the configured cache root",
                path=path.as_posix(),
            ) from exc
    if not relative_parts or any(part in {"", ".", ".."} for part in relative_parts):
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_REF_UNSAFE",
            "cache artifact path must be normalized inside the configured cache root",
            path=path.as_posix(),
        )
    try:
        root_descriptor = os.open(root, _DIRECTORY_FLAGS)
    except FileNotFoundError as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_MISSING",
            "configured cache root is missing",
            path=path.as_posix(),
        ) from exc
    except OSError as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_READ_FAILED",
            "configured cache root could not be opened safely",
            path=path.as_posix(),
        ) from exc
    try:
        confined_parts, activation = pinned_cache_parts(
            root_descriptor,
            relative_parts,
            allow_current_pointer=allow_current_pointer,
            path=path,
        )
        content, modified_at_epoch = _read_regular_file_at(
            root_descriptor,
            confined_parts,
            max_bytes=max_bytes,
            path=path,
        )
        return ConfinedCacheRead(
            content=content,
            activation=activation,
            modified_at_epoch=modified_at_epoch,
        )
    finally:
        os.close(root_descriptor)


def verify_cache_artifact(
    path: Path,
    *,
    expected_sha256: str,
    declared_bytes: int | None,
    max_artifact_bytes: int,
    cache_root: str | Path | None = None,
) -> int:
    """Read one cache artifact once and verify its size and digest."""

    if declared_bytes is not None and declared_bytes > max_artifact_bytes:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_TOO_LARGE",
            "cache artifact declared size exceeds configured size limit",
            path=path.as_posix(),
        )
    raw = read_confined_cache_file(
        path,
        cache_root=cache_root or infer_cache_root(path),
        max_bytes=max_artifact_bytes,
    )
    if declared_bytes is not None and len(raw) != declared_bytes:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH",
            "cache artifact size does not match index",
            path=path.as_posix(),
        )
    actual_sha256 = "sha256:" + hashlib.sha256(raw).hexdigest()
    if actual_sha256 != expected_sha256:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_CHECKSUM_MISMATCH",
            "cache artifact checksum does not match index",
            path=path.as_posix(),
        )
    return len(raw)


def _read_regular_file_at(
    root_descriptor: int,
    parts: tuple[str, ...],
    *,
    max_bytes: int,
    path: Path,
) -> tuple[bytes, float]:
    parent_descriptor = os.dup(root_descriptor)
    file_descriptor: int | None = None
    try:
        for part in parts[:-1]:
            next_descriptor = os.open(part, _DIRECTORY_FLAGS, dir_fd=parent_descriptor)
            os.close(parent_descriptor)
            parent_descriptor = next_descriptor
        file_descriptor = os.open(parts[-1], _FILE_FLAGS, dir_fd=parent_descriptor)
        before = os.fstat(file_descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise AirflowDeploymentIndexError(
                "DPONE_CACHE_ARTIFACT_REF_UNSAFE",
                "cache artifact must be a regular file",
                path=path.as_posix(),
            )
        if before.st_size > max_bytes:
            raise AirflowDeploymentIndexError(
                "DPONE_CACHE_ARTIFACT_TOO_LARGE",
                "cache artifact exceeds configured size limit",
                path=path.as_posix(),
            )
        raw = _read_at_most(file_descriptor, max_bytes + 1)
        after = os.fstat(file_descriptor)
        if _file_identity(before) != _file_identity(after):
            raise AirflowDeploymentIndexError(
                "DPONE_CACHE_ARTIFACT_READ_FAILED",
                "cache artifact changed while it was read",
                path=path.as_posix(),
            )
        if len(raw) > max_bytes:
            raise AirflowDeploymentIndexError(
                "DPONE_CACHE_ARTIFACT_TOO_LARGE",
                "cache artifact exceeds configured size limit",
                path=path.as_posix(),
            )
        return raw, after.st_mtime
    except AirflowDeploymentIndexError:
        raise
    except FileNotFoundError as exc:
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_MISSING",
            "cache artifact is missing",
            path=path.as_posix(),
        ) from exc
    except OSError as exc:
        code = (
            "DPONE_CACHE_ARTIFACT_REF_UNSAFE"
            if exc.errno in {errno.ELOOP, errno.ENOTDIR}
            else "DPONE_CACHE_ARTIFACT_READ_FAILED"
        )
        raise AirflowDeploymentIndexError(
            code,
            "cache artifact could not be opened through the confined cache root",
            path=path.as_posix(),
        ) from exc
    finally:
        if file_descriptor is not None:
            os.close(file_descriptor)
        os.close(parent_descriptor)


def _read_at_most(descriptor: int, limit: int) -> bytes:
    chunks: list[bytes] = []
    remaining = limit
    while remaining:
        chunk = os.read(descriptor, min(64 * 1024, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _file_identity(metadata: os.stat_result) -> tuple[int, int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns


def _cache_relative_path(artifact_ref: str) -> Path:
    if not artifact_ref.startswith("cache://"):
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_REF_INVALID",
            "cache artifact references must use cache://",
            path=artifact_ref,
        )
    raw = artifact_ref.removeprefix("cache://")
    parsed = PurePosixPath(raw)
    if parsed.is_absolute() or any(part in {"", ".", ".."} for part in parsed.parts):
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_ARTIFACT_REF_UNSAFE",
            "cache artifact reference must be a relative path inside the cache root",
            path=artifact_ref,
        )
    if any(part.lower() in _MUTABLE_ARTIFACT_ALIASES for part in parsed.parts):
        raise AirflowDeploymentIndexError(
            "DPONE_CACHE_UNPINNED_REFERENCE",
            "cache artifact references must point at immutable release content, not mutable aliases",
            path=artifact_ref,
        )
    return Path(*parsed.parts)


__all__ = [
    "CacheActivationIdentity",
    "ConfinedCacheRead",
    "DEFAULT_MAX_ARTIFACT_BYTES",
    "infer_cache_root",
    "read_confined_cache_file",
    "read_confined_cache_file_with_identity",
    "resolve_cache_artifact",
    "verify_cache_artifact",
]

"""Filesystem and registry-integrity primitives for strict runtime init-fetch."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.ports.artifact_registry import ArtifactMetadata
    from dpone.runtime.runtime_init_fetch_plan import RuntimeArtifactDescriptor, RuntimeWorkloadPackRef


import hashlib
import os
import secrets
import stat
from pathlib import Path, PurePosixPath

from dpone.runtime.deployment_cache_common import DeploymentCacheError, open_regular_file
from dpone.runtime.init_fetch_contract import InitFetchError

_READ_BYTES = 64 * 1024


class ReadyPublicationError(InitFetchError):
    """Ready publication failed after the final name may have committed."""

    def __init__(self, message: str, *, may_have_committed: bool) -> None:
        super().__init__("DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED", message)
        self.may_have_committed = may_have_committed


def read_bounded_regular_file(
    path: Path,
    *,
    root: Path,
    max_bytes: int,
    missing_code: str,
    invalid_code: str,
    label: str,
) -> bytes:
    """Read one confined regular file without crossing its byte budget."""

    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 0:
        raise ValueError("max_bytes must be a non-negative integer")
    try:
        descriptor = open_regular_file(
            path,
            missing_code=missing_code,
            invalid_code=invalid_code,
            label=label,
            root=root,
        )
    except DeploymentCacheError as exc:
        raise InitFetchError(exc.code, str(exc)) from exc
    chunks: list[bytes] = []
    total = 0
    try:
        while total <= max_bytes:
            chunk = os.read(descriptor, min(_READ_BYTES, max_bytes + 1 - total))
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
            total += len(chunk)
    finally:
        os.close(descriptor)
    raise _integrity_error(f"{label} exceeds its byte limit")


def verify_registry_metadata(
    metadata: ArtifactMetadata,
    *,
    key: PurePosixPath,
    descriptor: RuntimeArtifactDescriptor | RuntimeWorkloadPackRef,
) -> None:
    """Check untrusted registry metadata against the pinned plan descriptor."""

    if (
        metadata.key != key
        or isinstance(metadata.size_bytes, bool)
        or not isinstance(metadata.size_bytes, int)
        or metadata.size_bytes != descriptor.bytes
    ):
        raise _integrity_error("artifact registry metadata does not match the pinned descriptor")
    if metadata.advertised_sha256 is not None and metadata.advertised_sha256.casefold() != descriptor.sha256:
        raise _integrity_error("artifact registry checksum metadata does not match the pinned descriptor")


def read_verified_file(
    path: Path,
    *,
    expected_sha256: str,
    expected_bytes: int,
    root: Path,
) -> bytes:
    """Read one regular, confined file and re-verify its exact content identity."""

    try:
        descriptor = open_regular_file(
            path,
            missing_code="DPONE_CACHE_ARTIFACT_NOT_FOUND",
            invalid_code="DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED",
            label="runtime artifact",
            root=root,
        )
    except DeploymentCacheError as exc:
        raise InitFetchError(exc.code, str(exc)) from exc
    try:
        size = os.fstat(descriptor).st_size
        if size != expected_bytes:
            raise _integrity_error("runtime artifact size changed after verification")
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, _READ_BYTES):
            digest.update(chunk)
            chunks.append(chunk)
        if "sha256:" + digest.hexdigest() != expected_sha256:
            raise _integrity_error("runtime artifact checksum changed after verification")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def write_ready_last(path: Path, payload: bytes) -> None:
    """Publish an immutable ready record only after every prior check succeeds."""

    if not path.name or path.name in {".", ".."} or "/" in path.name or "\\" in path.name:
        raise _integrity_error("runtime ready manifest name is unsafe")
    directory = -1
    descriptor = -1
    temporary_name: str | None = None
    final_name_committed = False
    try:
        directory = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        if _ready_exists(directory, path.name):
            _require_matching_ready_at(directory, path.name, payload)
            return
        temporary_name, descriptor = _open_ready_temporary(directory, path.name)
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(
                temporary_name,
                path.name,
                src_dir_fd=directory,
                dst_dir_fd=directory,
                follow_symlinks=False,
            )
            final_name_committed = True
        except FileExistsError:
            _require_matching_ready_at(directory, path.name, payload)
            final_name_committed = True
        os.fsync(directory)
    except OSError as exc:
        raise ReadyPublicationError(
            "runtime ready manifest cannot be published safely",
            may_have_committed=final_name_committed,
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if directory >= 0:
            if temporary_name is not None:
                try:
                    os.unlink(temporary_name, dir_fd=directory)
                except FileNotFoundError:
                    pass
                except OSError:
                    pass
            os.close(directory)


def require_safe_directory(path: Path) -> None:
    if path.is_symlink() or not path.is_dir():
        raise _integrity_error("runtime artifact root is unsafe")


def _ready_exists(directory: int, name: str) -> bool:
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
            dir_fd=directory,
        )
    except FileNotFoundError:
        return False
    else:
        os.close(descriptor)
        return True


def _open_ready_temporary(directory: int, final_name: str) -> tuple[str, int]:
    for _ in range(4):
        name = f".{final_name}.tmp.{os.getpid()}.{secrets.token_hex(8)}"
        try:
            descriptor = os.open(
                name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory,
            )
        except FileExistsError:
            continue
        return name, descriptor
    raise _integrity_error("runtime ready manifest temporary file could not be allocated")


def _require_matching_ready_at(directory: int, name: str, expected: bytes) -> None:
    descriptor = os.open(
        name,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
        dir_fd=directory,
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != len(expected):
            raise _integrity_error("runtime ready manifest already contains different content")
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, _READ_BYTES):
            chunks.append(chunk)
        actual = b"".join(chunks)
    finally:
        os.close(descriptor)
    if actual != expected:
        raise _integrity_error("runtime ready manifest already contains different content")


def _integrity_error(message: str) -> InitFetchError:
    return InitFetchError("DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED", message)


__all__ = [
    "read_bounded_regular_file",
    "read_verified_file",
    "require_safe_directory",
    "verify_registry_metadata",
    "write_ready_last",
]

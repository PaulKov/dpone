"""Create-only confined storage for provider-produced dev evidence."""

from __future__ import annotations

import errno
import hashlib
import os
import re
import stat
import uuid
from pathlib import Path
from typing import Final

MAX_DEV_EVIDENCE_FILE_BYTES: Final = 16 * 1024 * 1024
_DIRECTORY_FLAGS: Final = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_READ_FLAGS: Final = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_WRITE_FLAGS: Final = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
_DIGEST: Final = re.compile(r"^sha256:[0-9a-f]{64}$")


class DevEvidenceStoreError(ValueError):
    """Evidence bytes cannot be installed below the configured root safely."""


class DevEvidenceConfinedStore:
    """Install immutable files through no-follow directory descriptors."""

    def __init__(self, root: Path) -> None:
        candidate = Path(root)
        if not candidate.is_absolute():
            raise DevEvidenceStoreError("dev evidence root must be absolute")
        try:
            metadata = candidate.lstat()
        except OSError as exc:
            raise DevEvidenceStoreError("dev evidence root must be an existing directory") from exc
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise DevEvidenceStoreError("dev evidence root must be a non-symlink directory")
        normalized = Path(os.path.abspath(candidate))
        if candidate.resolve(strict=True) != normalized:
            raise DevEvidenceStoreError("dev evidence root must not traverse symlinks")
        self.root = normalized
        self._root_identity = (metadata.st_dev, metadata.st_ino)

    def install(
        self,
        *,
        directory_parts: tuple[str, ...],
        filename: str,
        payload: bytes,
    ) -> bool:
        """Create one file; return ``True`` only when identical bytes existed."""

        _validate_components((*directory_parts, filename))
        if not payload or len(payload) > MAX_DEV_EVIDENCE_FILE_BYTES:
            raise DevEvidenceStoreError("dev evidence file size is invalid")
        root_fd = _open_directory(self.root, expected_identity=self._root_identity)
        leaf_fd = root_fd
        try:
            for component in directory_parts:
                next_fd = _open_or_create_directory(leaf_fd, component)
                if leaf_fd != root_fd:
                    os.close(leaf_fd)
                leaf_fd = next_fd
            return _install_create_only(leaf_fd, filename, payload)
        finally:
            if leaf_fd != root_fd:
                os.close(leaf_fd)
            os.close(root_fd)

    def ensure_directory(
        self,
        *,
        directory_parts: tuple[str, ...],
    ) -> None:
        """Create one confined directory chain for a scoped PVC mount."""

        _validate_components(directory_parts)
        root_fd = _open_directory(
            self.root,
            expected_identity=self._root_identity,
        )
        leaf_fd = root_fd
        try:
            for component in directory_parts:
                next_fd = _open_or_create_directory(leaf_fd, component)
                if leaf_fd != root_fd:
                    os.close(leaf_fd)
                leaf_fd = next_fd
        finally:
            if leaf_fd != root_fd:
                os.close(leaf_fd)
            os.close(root_fd)

    def read(
        self,
        *,
        directory_parts: tuple[str, ...],
        filename: str,
        expected_sha256: str,
        expected_bytes: int,
    ) -> bytes:
        """Read one exact immutable file without following links."""

        _validate_components((*directory_parts, filename))
        if (
            _DIGEST.fullmatch(expected_sha256) is None
            or isinstance(expected_bytes, bool)
            or not isinstance(expected_bytes, int)
            or expected_bytes < 1
            or expected_bytes > MAX_DEV_EVIDENCE_FILE_BYTES
        ):
            raise DevEvidenceStoreError("dev evidence descriptor is invalid")
        root_fd = _open_directory(self.root, expected_identity=self._root_identity)
        leaf_fd = root_fd
        try:
            for component in directory_parts:
                next_fd = _open_existing_directory(leaf_fd, component)
                if leaf_fd != root_fd:
                    os.close(leaf_fd)
                leaf_fd = next_fd
            payload = _existing_payload(leaf_fd, filename)
            if payload is None:
                raise DevEvidenceStoreError("dev evidence file is missing")
            if len(payload) != expected_bytes or _sha256(payload) != expected_sha256:
                raise DevEvidenceStoreError("dev evidence file differs from its descriptor")
            return payload
        finally:
            if leaf_fd != root_fd:
                os.close(leaf_fd)
            os.close(root_fd)

    def read_payload(
        self,
        *,
        directory_parts: tuple[str, ...],
        filename: str,
    ) -> bytes:
        """Read one bounded immutable file when its digest is in the payload."""

        _validate_components((*directory_parts, filename))
        root_fd = _open_directory(
            self.root,
            expected_identity=self._root_identity,
        )
        leaf_fd = root_fd
        try:
            for component in directory_parts:
                next_fd = _open_existing_directory(leaf_fd, component)
                if leaf_fd != root_fd:
                    os.close(leaf_fd)
                leaf_fd = next_fd
            payload = _existing_payload(leaf_fd, filename)
            if payload is None:
                raise DevEvidenceStoreError("dev evidence file is missing")
            return payload
        finally:
            if leaf_fd != root_fd:
                os.close(leaf_fd)
            os.close(root_fd)


def evidence_set_directory_parts(
    release_id: str,
    deployment_id: str,
    evidence_set_id: str,
) -> tuple[str, ...]:
    """Return the canonical identity-only evidence-set directory."""

    for value in (release_id, deployment_id, evidence_set_id):
        if _DIGEST.fullmatch(value) is None:
            raise DevEvidenceStoreError("dev evidence identity is invalid")
    return (
        "releases",
        _digest_component(release_id),
        "deployments",
        _digest_component(deployment_id),
        "sets",
        _digest_component(evidence_set_id),
    )


def logical_evidence_filename(value: str) -> str:
    """Project one bounded logical ID into a path-safe content name."""

    if not value or len(value.encode("utf-8")) > 1024:
        raise DevEvidenceStoreError("logical evidence identity is invalid")
    return f"sha256-{hashlib.sha256(value.encode('utf-8')).hexdigest()}.json"


def _open_directory(path: Path, *, expected_identity: tuple[int, int]) -> int:
    try:
        descriptor = os.open(path, _DIRECTORY_FLAGS)
    except OSError as exc:
        raise DevEvidenceStoreError("dev evidence root cannot be opened safely") from exc
    metadata = os.fstat(descriptor)
    if (metadata.st_dev, metadata.st_ino) != expected_identity:
        os.close(descriptor)
        raise DevEvidenceStoreError("dev evidence root identity changed")
    return descriptor


def _open_or_create_directory(parent_fd: int, name: str) -> int:
    try:
        os.mkdir(name, mode=0o750, dir_fd=parent_fd)
        os.fsync(parent_fd)
    except FileExistsError:
        pass
    except OSError as exc:
        raise DevEvidenceStoreError("dev evidence directory cannot be created") from exc
    try:
        return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except OSError as exc:
        raise DevEvidenceStoreError("dev evidence directory is unsafe") from exc


def _open_existing_directory(parent_fd: int, name: str) -> int:
    try:
        return os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except OSError as exc:
        raise DevEvidenceStoreError("dev evidence directory is missing or unsafe") from exc


def _install_create_only(directory_fd: int, filename: str, payload: bytes) -> bool:
    if existing := _existing_payload(directory_fd, filename):
        if existing == payload:
            return True
        raise DevEvidenceStoreError("existing dev evidence conflicts with current attempt")
    temporary = f".tmp-{uuid.uuid4().hex}"
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary,
            _WRITE_FLAGS,
            0o640,
            dir_fd=directory_fd,
        )
        _write_all(descriptor, payload)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        try:
            os.link(
                temporary,
                filename,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except FileExistsError:
            existing = _existing_payload(directory_fd, filename)
            if existing != payload:
                raise DevEvidenceStoreError("existing dev evidence conflicts with current attempt") from None
            return True
        os.fsync(directory_fd)
        return False
    except DevEvidenceStoreError:
        raise
    except OSError as exc:
        raise DevEvidenceStoreError("dev evidence file could not be installed atomically") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            os.unlink(temporary, dir_fd=directory_fd)
        except FileNotFoundError:
            pass


def _existing_payload(directory_fd: int, filename: str) -> bytes | None:
    try:
        descriptor = os.open(filename, _READ_FLAGS, dir_fd=directory_fd)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise DevEvidenceStoreError("existing dev evidence file is unsafe") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size <= 0
            or metadata.st_size > MAX_DEV_EVIDENCE_FILE_BYTES
        ):
            raise DevEvidenceStoreError("existing dev evidence file is invalid")
        chunks: list[bytes] = []
        remaining = int(metadata.st_size)
        while remaining:
            chunk = os.read(descriptor, min(remaining, 64 * 1024))
            if not chunk:
                raise DevEvidenceStoreError("existing dev evidence file changed while reading")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    offset = 0
    while offset < len(view):
        written = os.write(descriptor, view[offset:])
        if written <= 0:
            raise OSError(errno.EIO, "short write")
        offset += written


def _validate_components(values: tuple[str, ...]) -> None:
    if not values or any(
        not value
        or value in {".", ".."}
        or "/" in value
        or "\\" in value
        or "\x00" in value
        or len(value.encode("utf-8")) > 255
        for value in values
    ):
        raise DevEvidenceStoreError("dev evidence path component is invalid")


def _digest_component(value: str) -> str:
    return value.replace(":", "-", 1)


def _sha256(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


__all__ = [
    "DevEvidenceConfinedStore",
    "DevEvidenceStoreError",
    "MAX_DEV_EVIDENCE_FILE_BYTES",
    "evidence_set_directory_parts",
    "logical_evidence_filename",
]

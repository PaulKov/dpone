"""Bounded inspection, extraction and reconciliation of dbt bundle archives."""

from __future__ import annotations

import gzip
import hashlib
import io
import os
import shutil
import stat
import tarfile
import unicodedata
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import IO

from dpone.contracts.dbt_contract_validation import (
    DbtPublishingError,
    sha256_bytes,
)
from dpone.contracts.dbt_project_bundle import (
    DbtProjectBundle,
    DbtProjectBundleLimits,
    DbtProjectFile,
)
from dpone.runtime.dbt_project_bundle_safety import (
    FILE_FLAGS,
    READ_BYTES,
    identity,
    limit_error,
    open_at,
    open_directory,
    open_parent_at,
    source_changed,
)


@dataclass(slots=True)
class _ArchiveValidationState:
    limits: DbtProjectBundleLimits
    file_count: int = 0
    extracted_bytes: int = 0
    previous_path: str | None = None
    normalized_paths: set[str] = field(default_factory=set)
    directories: set[str] = field(default_factory=set)

    def accept(self, member: tarfile.TarInfo) -> str:
        self.file_count += 1
        if self.file_count > self.limits.max_files:
            raise limit_error("dbt project archive exceeds the file-count limit")
        path = _safe_member_path(member)
        self.directories.update(
            parent.as_posix() for parent in PurePosixPath(path).parents if parent != PurePosixPath(".")
        )
        if len(self.directories) > self.limits.max_files:
            raise limit_error("dbt project archive exceeds the directory-count limit")
        if self.previous_path is not None and path <= self.previous_path:
            raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project archive paths are not canonical")
        normalized = unicodedata.normalize("NFC", path).casefold()
        if normalized in self.normalized_paths:
            raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project contains colliding paths")
        if member.size > self.limits.max_file_bytes:
            raise limit_error("dbt project archive member exceeds the per-file byte limit")
        self.extracted_bytes += member.size
        if self.extracted_bytes > self.limits.max_extracted_bytes:
            raise limit_error("dbt project archive exceeds the extracted-byte limit")
        self.previous_path = path
        self.normalized_paths.add(normalized)
        return path

    def require_non_empty(self) -> None:
        if self.file_count == 0:
            raise limit_error("dbt project archive file count is invalid")


class _BoundedArchiveReader:
    """Count every decompressed tar byte, including extension metadata."""

    def __init__(self, source: IO[bytes], *, max_bytes: int) -> None:
        self._source = source
        self._max_bytes = max_bytes
        self._observed = 0

    def read(self, size: int = -1) -> bytes:
        remaining = self._max_bytes - self._observed
        requested = remaining + 1 if size < 0 else min(size, remaining + 1)
        chunk = self._source.read(requested)
        self._observed += len(chunk)
        if self._observed > self._max_bytes:
            raise limit_error("dbt project archive metadata exceeds the stream-byte limit")
        return chunk


@contextmanager
def _open_archive(
    data: bytes,
    limits: DbtProjectBundleLimits,
) -> Iterator[tarfile.TarFile]:
    stream_limit = limits.max_extracted_bytes + limits.max_files * 1024 + 1024 * 1024
    with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as compressed:
        reader = _BoundedArchiveReader(compressed, max_bytes=stream_limit)
        with tarfile.open(fileobj=reader, mode="r|") as archive:
            yield archive


def archive_bytes(value: bytes | Path, limits: DbtProjectBundleLimits) -> bytes:
    if isinstance(value, bytes):
        if len(value) > limits.max_archive_bytes:
            raise limit_error("dbt project archive exceeds the compressed byte limit")
        return value
    if not isinstance(value, Path):
        raise TypeError("bundle_path_or_bytes must be bytes or Path")
    try:
        descriptor = os.open(value, FILE_FLAGS)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise OSError
        data = _read_descriptor(descriptor, limits.max_archive_bytes)
        if identity(os.fstat(descriptor)) != identity(metadata):
            raise source_changed()
        return data
    except DbtPublishingError:
        raise
    except OSError as exc:
        raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project archive path is unsafe") from exc
    finally:
        if "descriptor" in locals():
            os.close(descriptor)


def inspect_archive(data: bytes, limits: DbtProjectBundleLimits) -> DbtProjectBundle:
    files: list[DbtProjectFile] = []
    state = _ArchiveValidationState(limits)
    try:
        with _open_archive(data, limits) as archive:
            for member in archive:
                path = state.accept(member)
                source = archive.extractfile(member)
                if source is None:
                    raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project archive member is unreadable")
                files.append(DbtProjectFile(path, _stream_digest(source, member.size), member.size))
        state.require_non_empty()
    except DbtPublishingError:
        raise
    except (OSError, EOFError, tarfile.TarError) as exc:
        raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project archive is invalid") from exc
    if "dbt_project.yml" not in {item.path for item in files}:
        raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project archive lacks dbt_project.yml")
    return DbtProjectBundle(
        archive_sha256=sha256_bytes(data),
        archive_bytes=len(data),
        extracted_bytes=sum(item.bytes for item in files),
        files=tuple(files),
    )


def extract_archive(data: bytes, destination: Path, limits: DbtProjectBundleLimits) -> None:
    state = _ArchiveValidationState(limits)
    try:
        descriptor = open_directory(destination)
        try:
            with _open_archive(data, limits) as archive:
                for member in archive:
                    state.accept(member)
                    _extract_member(archive, member, descriptor)
            state.require_non_empty()
        finally:
            os.close(descriptor)
    except BaseException:
        clear_directory(destination)
        raise


def observed_tree(
    destination: Path,
    limits: DbtProjectBundleLimits,
) -> tuple[tuple[DbtProjectFile, ...], set[str]]:
    descriptor = open_directory(destination, code="DPONE_DBT_BUNDLE_TREE_MISMATCH")
    files: list[DbtProjectFile] = []
    directories: set[str] = set()
    try:
        _visit_tree(descriptor, PurePosixPath(), files, directories, limits)
    finally:
        os.close(descriptor)
    files.sort(key=lambda item: item.path)
    return tuple(files), directories


def require_empty_directory(path: Path) -> None:
    descriptor = open_directory(path)
    try:
        if os.listdir(descriptor):
            raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project destination must be empty")
    finally:
        os.close(descriptor)


def clear_directory(path: Path) -> None:
    for child in path.iterdir():
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink(missing_ok=True)


def _safe_member_path(member: tarfile.TarInfo) -> str:
    raw = member.name
    path = PurePosixPath(raw)
    if (
        not member.isreg()
        or bool(member.pax_headers)
        or member.mode != 0o644
        or member.mtime != 0
        or member.uid != 0
        or member.gid != 0
        or member.uname
        or member.gname
        or not raw
        or len(raw.encode("utf-8")) > 255
        or len(path.parts) > 64
        or "\\" in raw
        or path.is_absolute()
        or path.as_posix() != raw
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project archive member is unsafe")
    try:
        tarfile.TarInfo(raw).tobuf(format=tarfile.USTAR_FORMAT)
    except (UnicodeError, ValueError) as exc:
        raise DbtPublishingError(
            "DPONE_DBT_BUNDLE_INVALID",
            "dbt project archive member is not canonical USTAR",
        ) from exc
    return raw


def _extract_member(archive: tarfile.TarFile, member: tarfile.TarInfo, root_descriptor: int) -> None:
    relative = PurePosixPath(member.name)
    parent = open_parent_at(root_descriptor, relative.parts[:-1], create=True)
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        target_descriptor = os.open(relative.name, flags, 0o600, dir_fd=parent)
        source = archive.extractfile(member)
        if source is None:
            os.close(target_descriptor)
            raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project archive member is unreadable")
        written = 0
        with source, os.fdopen(target_descriptor, "wb") as target:
            while chunk := source.read(READ_BYTES):
                written += len(chunk)
                if written > member.size:
                    raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project member exceeds its size")
                target.write(chunk)
            target.flush()
            os.fsync(target.fileno())
        if written != member.size:
            raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project archive member is truncated")
    finally:
        os.close(parent)


def _visit_tree(
    descriptor: int,
    prefix: PurePosixPath,
    files: list[DbtProjectFile],
    directories: set[str],
    limits: DbtProjectBundleLimits,
) -> None:
    for name in sorted(os.listdir(descriptor)):
        metadata = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        relative = prefix / name
        if len(relative.parts) > 64:
            raise limit_error("extracted dbt project exceeds the path-depth limit")
        if stat.S_ISDIR(metadata.st_mode):
            directories.add(relative.as_posix())
            if len(directories) > limits.max_files:
                raise limit_error("extracted dbt project exceeds the directory-count limit")
            child = open_at(descriptor, name, directory=True)
            try:
                _visit_tree(child, relative, files, directories, limits)
            finally:
                os.close(child)
        elif stat.S_ISREG(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o600:
                raise _tree_error("dbt project file mode changed")
            child = open_at(descriptor, name, directory=False)
            try:
                files.append(
                    DbtProjectFile(relative.as_posix(), _descriptor_digest(child, metadata.st_size), metadata.st_size)
                )
            finally:
                os.close(child)
        else:
            raise _tree_error("extracted dbt project contains a forbidden entry type")
        if len(files) > limits.max_files or sum(item.bytes for item in files) > limits.max_extracted_bytes:
            raise limit_error("extracted dbt project exceeds its limits")


def _read_descriptor(descriptor: int, max_bytes: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while chunk := os.read(descriptor, READ_BYTES):
        total += len(chunk)
        if total > max_bytes:
            raise limit_error("dbt project archive exceeds the compressed byte limit")
        chunks.append(chunk)
    return b"".join(chunks)


def _stream_digest(source: IO[bytes], expected_bytes: int) -> str:
    digest = hashlib.sha256()
    observed = 0
    with source:
        while chunk := source.read(READ_BYTES):
            observed += len(chunk)
            if observed > expected_bytes:
                raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project archive member exceeds its size")
            digest.update(chunk)
    if observed != expected_bytes:
        raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project archive member is truncated")
    return "sha256:" + digest.hexdigest()


def _descriptor_digest(descriptor: int, expected_bytes: int) -> str:
    before = identity(os.fstat(descriptor))
    digest = hashlib.sha256()
    observed = 0
    while chunk := os.read(descriptor, READ_BYTES):
        observed += len(chunk)
        digest.update(chunk)
    if observed != expected_bytes or identity(os.fstat(descriptor)) != before:
        raise _tree_error("dbt project file changed during verification")
    return "sha256:" + digest.hexdigest()


def _tree_error(message: str) -> DbtPublishingError:
    return DbtPublishingError("DPONE_DBT_BUNDLE_TREE_MISMATCH", message)


__all__ = [
    "archive_bytes",
    "clear_directory",
    "extract_archive",
    "inspect_archive",
    "observed_tree",
    "require_empty_directory",
]

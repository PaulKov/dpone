"""Bounded extraction of the structured payload carried by a verified pack."""

from __future__ import annotations

import base64
import binascii
import hashlib
import io
import os
import shutil
import stat
import tarfile
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.runtime.deployment_cache_common import DeploymentCacheError, open_regular_file
from dpone.runtime.init_fetch_contract import InitFetchError

RUNTIME_PAYLOAD_SCHEMA = "dpone.airflow-runtime-payload.v1"
DEFAULT_MAX_RUNTIME_ARCHIVE_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_RUNTIME_EXPANDED_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_RUNTIME_FILES = 10_000
_READ_BYTES = 64 * 1024


@dataclass(frozen=True, slots=True)
class RuntimePayloadArchive:
    data: bytes
    sha256: str
    declared_bytes: int


def runtime_payload_archive(pack: Mapping[str, Any]) -> RuntimePayloadArchive:
    """Decode exact archive bytes from a verified workload pack."""

    payload = _mapping(pack.get("runtime_payload"), "runtime_payload")
    if set(payload) != {"schema", "archive"} or payload.get("schema") != RUNTIME_PAYLOAD_SCHEMA:
        raise _payload_error("runtime payload schema is invalid")
    archive = _mapping(payload.get("archive"), "runtime_payload.archive")
    if set(archive) != {"encoding", "format", "sha256", "bytes", "data"}:
        raise _payload_error("runtime payload archive contains unknown or missing fields")
    if archive.get("encoding") != "base64" or archive.get("format") != "tar+gzip":
        raise _payload_error("runtime payload archive encoding is unsupported")
    expected_sha256 = archive.get("sha256")
    if not is_canonical_sha256_digest(expected_sha256):
        raise _payload_error("runtime payload archive sha256 is invalid")
    declared_bytes = archive.get("bytes")
    if isinstance(declared_bytes, bool) or not isinstance(declared_bytes, int) or declared_bytes <= 0:
        raise _payload_error("runtime payload archive bytes must be positive")
    encoded = archive.get("data")
    if not isinstance(encoded, str) or not encoded:
        raise _payload_error("runtime payload archive data is missing")
    try:
        data = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise _payload_error("runtime payload archive data is not valid base64") from exc
    if len(data) != declared_bytes:
        raise _payload_error("runtime payload archive size does not match its descriptor")
    actual_sha256 = "sha256:" + hashlib.sha256(data).hexdigest()
    if actual_sha256 != expected_sha256:
        raise _payload_error("runtime payload archive checksum does not match its descriptor")
    if base64.b64encode(data).decode("ascii") != encoded:
        raise _payload_error("runtime payload archive base64 is not canonical")
    return RuntimePayloadArchive(
        data=data,
        sha256=actual_sha256,
        declared_bytes=declared_bytes,
    )


def extract_runtime_payload(
    archive: RuntimePayloadArchive,
    destination: Path,
    *,
    max_archive_bytes: int = DEFAULT_MAX_RUNTIME_ARCHIVE_BYTES,
    max_expanded_bytes: int = DEFAULT_MAX_RUNTIME_EXPANDED_BYTES,
    max_files: int = DEFAULT_MAX_RUNTIME_FILES,
) -> None:
    """Validate the complete tar inventory, then extract without links or traversal."""

    limits = (
        _positive_limit(max_archive_bytes, "max_archive_bytes"),
        _positive_limit(max_expanded_bytes, "max_expanded_bytes"),
        _positive_limit(max_files, "max_files"),
    )
    if archive.declared_bytes > limits[0]:
        raise _payload_error("runtime payload archive exceeds the compressed byte limit")
    destination = destination.absolute()
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    _require_empty_regular_directory(destination)
    try:
        with tarfile.open(fileobj=io.BytesIO(archive.data), mode="r:gz") as bundle:
            members = bundle.getmembers()
            paths = _validate_members(
                members,
                max_expanded_bytes=limits[1],
                max_files=limits[2],
            )
            for member, relative in zip(members, paths, strict=True):
                _extract_member(bundle, member, relative, destination=destination)
    except InitFetchError:
        _clear_directory(destination)
        raise
    except (OSError, EOFError, tarfile.TarError) as exc:
        _clear_directory(destination)
        raise _payload_error("runtime payload archive could not be extracted safely") from exc


def discard_runtime_payload(destination: Path) -> None:
    """Remove an uncommitted extracted payload so the same Pod can retry safely."""

    if not os.path.lexists(destination):
        return
    mode = destination.lstat().st_mode
    if not stat.S_ISDIR(mode) or destination.is_symlink():
        raise _payload_error("runtime worktree root is unsafe")
    _clear_directory(destination)


def verify_runtime_payload_tree(
    archive: RuntimePayloadArchive,
    destination: Path,
    *,
    external_roots: tuple[str, ...] = (),
) -> None:
    """Reconcile the inline tree while reserving independently verified subtrees."""

    expected = _archive_tree_entries(archive)
    ignored = _external_root_names(external_roots, expected=expected)
    expected_bytes = sum(item[2] for item in expected if item[1] == "file")
    observed = _directory_tree_entries(
        destination.absolute(),
        max_entries=len(expected),
        max_bytes=expected_bytes,
        ignored_top_level=ignored,
    )
    if observed != expected:
        raise _payload_error("runtime payload worktree differs from the verified archive")


_TreeEntry = tuple[str, str, int, str | None]


def _archive_tree_entries(archive: RuntimePayloadArchive) -> tuple[_TreeEntry, ...]:
    entries: dict[str, _TreeEntry] = {}
    try:
        with tarfile.open(fileobj=io.BytesIO(archive.data), mode="r:gz") as bundle:
            members = bundle.getmembers()
            paths = _validate_members(
                members,
                max_expanded_bytes=DEFAULT_MAX_RUNTIME_EXPANDED_BYTES,
                max_files=DEFAULT_MAX_RUNTIME_FILES,
            )
            for member, relative in zip(members, paths, strict=True):
                for parent in relative.parents:
                    if parent != PurePosixPath("."):
                        label = parent.as_posix()
                        entries.setdefault(label, (label, "directory", 0, None))
                label = relative.as_posix()
                if member.isdir():
                    entries[label] = (label, "directory", 0, None)
                    continue
                source = bundle.extractfile(member)
                if source is None:
                    raise _payload_error("runtime payload archive file body is missing")
                try:
                    digest = hashlib.sha256()
                    size = 0
                    while chunk := source.read(_READ_BYTES):
                        size += len(chunk)
                        digest.update(chunk)
                finally:
                    source.close()
                if size != member.size:
                    raise _payload_error("runtime payload archive file size is truncated")
                entries[label] = (label, "file", size, digest.hexdigest())
    except InitFetchError:
        raise
    except (OSError, EOFError, tarfile.TarError) as exc:
        raise _payload_error("runtime payload archive could not be verified safely") from exc
    return tuple(entries[key] for key in sorted(entries))


def _directory_tree_entries(
    root: Path,
    *,
    max_entries: int,
    max_bytes: int,
    ignored_top_level: frozenset[str] = frozenset(),
) -> tuple[_TreeEntry, ...]:
    _require_directory(root)
    entries: list[_TreeEntry] = []
    total_bytes = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            children = sorted(os.scandir(directory), key=lambda item: item.name)
        except OSError as exc:
            raise _payload_error("runtime payload worktree cannot be inspected safely") from exc
        for child in children:
            path = Path(child.path)
            try:
                relative = path.relative_to(root).as_posix()
                if directory == root and child.name in ignored_top_level:
                    if child.is_symlink() or not child.is_dir(follow_symlinks=False):
                        raise _payload_error("external runtime payload root is unsafe")
                    continue
                if child.is_symlink():
                    raise _payload_error("runtime payload worktree contains a forbidden entry type")
                if child.is_dir(follow_symlinks=False):
                    entries.append((relative, "directory", 0, None))
                    pending.append(path)
                elif child.is_file(follow_symlinks=False):
                    size, digest = _regular_file_identity(path, root=root)
                    total_bytes += size
                    entries.append((relative, "file", size, digest))
                else:
                    raise _payload_error("runtime payload worktree contains a forbidden entry type")
            except OSError as exc:
                raise _payload_error("runtime payload worktree cannot be inspected safely") from exc
            if len(entries) > max_entries or total_bytes > max_bytes:
                raise _payload_error("runtime payload worktree exceeds its verified inventory")
    return tuple(sorted(entries))


def _external_root_names(
    values: tuple[str, ...],
    *,
    expected: tuple[_TreeEntry, ...],
) -> frozenset[str]:
    roots: set[str] = set()
    inline_top_level = {PurePosixPath(item[0]).parts[0] for item in expected}
    for value in values:
        path = PurePosixPath(value)
        if not value or "\\" in value or path.is_absolute() or len(path.parts) != 1 or path.parts[0] in {"", ".", ".."}:
            raise _payload_error("external runtime payload root is unsafe")
        if value in roots or value in inline_top_level:
            raise _payload_error("external runtime payload root conflicts with the inline payload")
        roots.add(value)
    return frozenset(roots)


def _regular_file_identity(path: Path, *, root: Path) -> tuple[int, str]:
    try:
        descriptor = open_regular_file(
            path,
            missing_code="DPONE_RUNTIME_WORKTREE_INCOMPLETE",
            invalid_code="DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED",
            label="runtime payload file",
            root=root,
        )
    except DeploymentCacheError as exc:
        raise InitFetchError(exc.code, str(exc)) from exc
    try:
        metadata = os.fstat(descriptor)
        digest = hashlib.sha256()
        size = 0
        while chunk := os.read(descriptor, _READ_BYTES):
            size += len(chunk)
            digest.update(chunk)
        if size != metadata.st_size:
            raise _payload_error("runtime payload file changed during verification")
        return size, digest.hexdigest()
    finally:
        os.close(descriptor)


def _validate_members(
    members: list[tarfile.TarInfo],
    *,
    max_expanded_bytes: int,
    max_files: int,
) -> tuple[PurePosixPath, ...]:
    if not members or len(members) > max_files:
        raise _payload_error("runtime payload archive file count is invalid")
    seen: set[PurePosixPath] = set()
    directories: dict[PurePosixPath, bool] = {}
    paths: list[PurePosixPath] = []
    expanded_bytes = 0
    for member in members:
        relative = _safe_member_path(member.name)
        if relative in seen:
            raise _payload_error("runtime payload archive contains duplicate paths")
        if not member.isdir() and not member.isreg():
            raise _payload_error("runtime payload archive contains a forbidden entry type")
        if member.isreg():
            if member.size < 0:
                raise _payload_error("runtime payload archive contains an invalid file size")
            expanded_bytes += member.size
            if expanded_bytes > max_expanded_bytes:
                raise _payload_error("runtime payload archive exceeds the expanded byte limit")
        seen.add(relative)
        directories[relative] = member.isdir()
        paths.append(relative)
    for relative in paths:
        for parent in relative.parents:
            if parent != PurePosixPath(".") and parent in directories and not directories[parent]:
                raise _payload_error("runtime payload archive path conflicts with a file")
        if not directories[relative] and any(
            other != relative and other.parts[: len(relative.parts)] == relative.parts for other in paths
        ):
            raise _payload_error("runtime payload archive file conflicts with a child path")
    return tuple(paths)


def _extract_member(
    bundle: tarfile.TarFile,
    member: tarfile.TarInfo,
    relative: PurePosixPath,
    *,
    destination: Path,
) -> None:
    path = destination.joinpath(*relative.parts)
    if member.isdir():
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        _require_directory(path)
        return
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    _require_directory(path.parent)
    source = bundle.extractfile(member)
    if source is None:
        raise _payload_error("runtime payload archive file body is missing")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    written = 0
    try:
        with os.fdopen(descriptor, "wb") as target:
            descriptor = -1
            while chunk := source.read(_READ_BYTES):
                written += len(chunk)
                if written > member.size:
                    raise _payload_error("runtime payload archive file exceeds its declared size")
                target.write(chunk)
            target.flush()
            os.fsync(target.fileno())
    finally:
        source.close()
        if descriptor >= 0:
            os.close(descriptor)
    if written != member.size:
        raise _payload_error("runtime payload archive file size is truncated")


def _safe_member_path(raw: str) -> PurePosixPath:
    relative = PurePosixPath(raw)
    if (
        not raw
        or "\\" in raw
        or relative.is_absolute()
        or relative.as_posix() != raw
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise _payload_error("runtime payload archive path is unsafe")
    return relative


def _require_empty_regular_directory(path: Path) -> None:
    mode = path.lstat().st_mode
    if not stat.S_ISDIR(mode) or path.is_symlink():
        raise _payload_error("runtime worktree root is unsafe")
    if any(path.iterdir()):
        raise _payload_error("runtime worktree root must be empty")


def _require_directory(path: Path) -> None:
    mode = path.lstat().st_mode
    if not stat.S_ISDIR(mode) or path.is_symlink():
        raise _payload_error("runtime payload destination directory is unsafe")


def _clear_directory(path: Path) -> None:
    for child in path.iterdir():
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink(missing_ok=True)


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _payload_error(f"{field} must be an object")
    return value


def _positive_limit(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _payload_error(message: str) -> InitFetchError:
    return InitFetchError("DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED", message)


__all__ = [
    "DEFAULT_MAX_RUNTIME_ARCHIVE_BYTES",
    "DEFAULT_MAX_RUNTIME_EXPANDED_BYTES",
    "DEFAULT_MAX_RUNTIME_FILES",
    "discard_runtime_payload",
    "extract_runtime_payload",
    "RUNTIME_PAYLOAD_SCHEMA",
    "RuntimePayloadArchive",
    "runtime_payload_archive",
    "verify_runtime_payload_tree",
]

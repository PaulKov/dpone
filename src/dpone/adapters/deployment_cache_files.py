"""Safe local-file adapter shared by deployment cache use cases."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any


class DeploymentCacheError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        path: str | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.path = path
        self.details = dict(details or {})


class AtomicJsonWriteError(OSError):
    """Report whether an atomic JSON target may already have changed."""

    def __init__(self, message: str, *, state_may_have_changed: bool) -> None:
        super().__init__(message)
        self.state_may_have_changed = state_may_have_changed


@dataclass(frozen=True, slots=True)
class RegularJsonDocument:
    """One JSON object and the exact identity of bytes read from its descriptor."""

    payload: dict[str, Any]
    sha256: str
    size_bytes: int


def read_regular_json_object(
    path: Path,
    *,
    missing_code: str,
    invalid_code: str,
    label: str,
    root: Path | None = None,
    max_bytes: int | None = None,
    oversized_code: str | None = None,
    unsafe_code: str | None = None,
    required_owner_uid: int | None = None,
    forbid_group_world_permissions: bool = False,
) -> dict[str, Any]:
    """Read one UTF-8 JSON object through a no-follow regular-file descriptor."""

    return read_regular_json_document(
        path,
        missing_code=missing_code,
        invalid_code=invalid_code,
        label=label,
        root=root,
        max_bytes=max_bytes,
        oversized_code=oversized_code,
        unsafe_code=unsafe_code,
        required_owner_uid=required_owner_uid,
        forbid_group_world_permissions=forbid_group_world_permissions,
    ).payload


def read_regular_json_document(
    path: Path,
    *,
    missing_code: str,
    invalid_code: str,
    label: str,
    root: Path | None = None,
    max_bytes: int | None = None,
    oversized_code: str | None = None,
    unsafe_code: str | None = None,
    required_owner_uid: int | None = None,
    forbid_group_world_permissions: bool = False,
) -> RegularJsonDocument:
    """Read one JSON object and bind its exact source-byte digest."""

    descriptor = open_regular_file(
        path,
        missing_code=missing_code,
        invalid_code=invalid_code,
        label=label,
        root=root,
    )
    try:
        metadata = os.fstat(descriptor)
        if required_owner_uid is not None and metadata.st_uid != required_owner_uid:
            raise DeploymentCacheError(
                unsafe_code or invalid_code,
                f"{label} must be owned by the runtime user",
                path=path.as_posix(),
            )
        if forbid_group_world_permissions and stat.S_IMODE(metadata.st_mode) & 0o077:
            raise DeploymentCacheError(
                unsafe_code or invalid_code,
                f"{label} must not grant group or world permissions",
                path=path.as_posix(),
            )
        if max_bytes is not None and (max_bytes <= 0 or metadata.st_size > max_bytes):
            raise DeploymentCacheError(
                oversized_code or invalid_code,
                f"{label} exceeds its configured byte limit",
                path=path.as_posix(),
            )
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            encoded = handle.read() if max_bytes is None else handle.read(max_bytes + 1)
        if max_bytes is not None and len(encoded) > max_bytes:
            raise DeploymentCacheError(
                oversized_code or invalid_code,
                f"{label} exceeds its configured byte limit",
                path=path.as_posix(),
            )
        payload = json.loads(encoded.decode("utf-8"))
    except (json.JSONDecodeError, RecursionError, UnicodeError) as exc:
        raise DeploymentCacheError(invalid_code, f"{label} JSON is invalid: {exc}", path=path.as_posix()) from exc
    except OSError as exc:
        raise DeploymentCacheError(invalid_code, f"{label} could not be read", path=path.as_posix()) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if not isinstance(payload, dict):
        raise DeploymentCacheError(invalid_code, f"{label} JSON must be an object", path=path.as_posix())
    return RegularJsonDocument(
        payload=payload,
        sha256="sha256:" + hashlib.sha256(encoded).hexdigest(),
        size_bytes=len(encoded),
    )


def open_regular_file(
    path: Path,
    *,
    missing_code: str,
    invalid_code: str,
    label: str,
    root: Path | None = None,
) -> int:
    """Open one regular file without following any component below ``root``."""

    try:
        descriptor = _open_read_only(path, root=root)
    except FileNotFoundError as exc:
        raise DeploymentCacheError(missing_code, f"required {label} is missing", path=path.as_posix()) from exc
    except OSError as exc:
        raise DeploymentCacheError(
            invalid_code,
            f"{label} must be a readable regular file and must not be a symlink",
            path=path.as_posix(),
        ) from exc
    if stat.S_ISREG(os.fstat(descriptor).st_mode):
        return descriptor
    os.close(descriptor)
    raise DeploymentCacheError(invalid_code, f"{label} must be a regular file", path=path.as_posix())


def resolve_mount_projected_regular_file(path: Path) -> Path:
    """Resolve a kubelet ConfigMap/Secret projection symlink to a regular file.

    Directory-mounted ConfigMap keys are projected as ``key -> ..data/key`` with
    ``..data -> ..<timestamp>``. Callers then open the resolved path with
    no-follow semantics while staying inside the mount directory.
    """

    mount_root = path.parent.resolve(strict=False)
    try:
        if not mount_root.is_dir():
            raise OSError("mount directory is not a directory")
        resolved = path.resolve(strict=True)
        resolved.relative_to(mount_root)
    except FileNotFoundError:
        raise
    except (OSError, ValueError) as exc:
        raise OSError("projected path must resolve to a regular file under its mount directory") from exc
    try:
        mode = resolved.lstat().st_mode
    except OSError as exc:
        raise OSError("projected path metadata could not be read") from exc
    if not stat.S_ISREG(mode):
        raise OSError("projected path must resolve to a regular file")
    return resolved


def require_path_without_symlinks(candidate: Path, *, root: Path, error_path: Path) -> None:
    """Reject any existing symlink component below a configured cache root."""

    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise DeploymentCacheError(
            "DPONE_CACHE_PATH_ESCAPE",
            "cache path resolves outside the configured cache root",
            path=error_path.as_posix(),
        ) from exc
    current = root
    for part in relative.parts:
        current /= part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            return
        except OSError as exc:
            raise DeploymentCacheError(
                "DPONE_CACHE_ARTIFACT_READ_FAILED",
                "cache path metadata could not be read",
                path=current.as_posix(),
            ) from exc
        if stat.S_ISLNK(mode):
            raise DeploymentCacheError(
                "DPONE_CACHE_PATH_ESCAPE",
                "cache path must not contain symlinks",
                path=current.as_posix(),
            )


def resolve_relative_current_symlink(cache_root: Path) -> Path:
    """Resolve ``current`` only from one canonical relative in-cache symlink."""

    root = cache_root.resolve(strict=False)
    current = root / "current"
    try:
        mode = current.lstat().st_mode
    except FileNotFoundError as exc:
        raise DeploymentCacheError(
            "DPONE_CURRENT_PATH_MISSING",
            "current deployment path is missing",
            path=current.as_posix(),
        ) from exc
    except OSError as exc:
        raise DeploymentCacheError(
            "DPONE_CURRENT_PATH_UNSAFE",
            "current deployment path metadata could not be read",
            path=current.as_posix(),
        ) from exc
    if not stat.S_ISLNK(mode):
        raise DeploymentCacheError(
            "DPONE_CURRENT_PATH_UNSAFE",
            "current deployment path must be a relative symlink",
            path=current.as_posix(),
        )
    try:
        raw_target = os.readlink(current)
    except OSError as exc:
        raise DeploymentCacheError(
            "DPONE_CURRENT_PATH_UNSAFE",
            "current deployment symlink target could not be read",
            path=current.as_posix(),
        ) from exc
    logical = PurePosixPath(raw_target)
    canonical = logical.as_posix()
    if (
        logical.is_absolute()
        or not logical.parts
        or any(part in {"", ".", ".."} for part in logical.parts)
        or canonical != raw_target
    ):
        raise DeploymentCacheError(
            "DPONE_CURRENT_PATH_UNSAFE",
            "current deployment symlink target must be canonical and relative",
            path=current.as_posix(),
        )
    lexical_target = root / Path(*logical.parts)
    require_path_without_symlinks(lexical_target, root=root, error_path=current)
    target = lexical_target.resolve(strict=False)
    try:
        target.relative_to(root)
    except ValueError as exc:
        raise DeploymentCacheError(
            "DPONE_CURRENT_PATH_OUTSIDE_CACHE_ROOT",
            "current deployment path resolves outside the configured cache root",
            path=current.as_posix(),
        ) from exc
    return target


def atomic_write_json(
    path: Path,
    payload: dict[str, Any],
    *,
    fsync_directory: Callable[[Path], None] | None = None,
) -> None:
    """Durably replace one JSON control file without following symlinks."""

    path.parent.mkdir(parents=True, exist_ok=True)
    _reject_symlink(path)
    encoded = encode_json_bytes(payload)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    replaced = False
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        _reject_symlink(path)
        os.replace(temporary, path)
        replaced = True
        (fsync_directory or durable_fsync_directory)(path.parent)
    except OSError as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise AtomicJsonWriteError(
            "atomic JSON publication could not be committed durably",
            state_may_have_changed=replaced,
        ) from exc
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def encode_json_bytes(payload: dict[str, Any]) -> bytes:
    """Encode exactly the bytes persisted by :func:`atomic_write_json`."""

    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _open_read_only(path: Path, *, root: Path | None) -> int:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    if root is None:
        return os.open(path, flags)
    root = root.absolute()
    path = path.absolute()
    try:
        parts = path.relative_to(root).parts
    except ValueError as exc:
        raise OSError("path is outside the configured cache root") from exc
    if not parts:
        raise OSError("regular file path must be below the configured cache root")
    directory_flags = flags | getattr(os, "O_DIRECTORY", 0)
    directory_descriptor = os.open(root, directory_flags)
    try:
        for part in parts[:-1]:
            next_descriptor = os.open(part, directory_flags, dir_fd=directory_descriptor)
            os.close(directory_descriptor)
            directory_descriptor = next_descriptor
        return os.open(parts[-1], flags, dir_fd=directory_descriptor)
    finally:
        os.close(directory_descriptor)


def _reject_symlink(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return
    if stat.S_ISLNK(mode):
        raise OSError(f"refusing symlinked control file: {path}")


def durable_fsync_directory(path: Path) -> None:
    """Persist one directory entry update after an atomic filesystem mutation."""

    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "AtomicJsonWriteError",
    "DeploymentCacheError",
    "RegularJsonDocument",
    "atomic_write_json",
    "durable_fsync_directory",
    "encode_json_bytes",
    "open_regular_file",
    "read_regular_json_object",
    "read_regular_json_document",
    "require_path_without_symlinks",
    "resolve_mount_projected_regular_file",
    "resolve_relative_current_symlink",
]

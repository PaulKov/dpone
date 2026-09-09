"""Bounded, no-follow file I/O for route-attestation composition roots."""

from __future__ import annotations

import json
import os
import secrets
import stat
from pathlib import Path
from typing import Any

_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | _NOFOLLOW
_READ_FLAGS = os.O_RDONLY | _NOFOLLOW
_CREATE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NOFOLLOW
_TEMP_ATTEMPTS = 16
_DIR_FD_SUPPORTED = all(operation in os.supports_dir_fd for operation in (os.open, os.mkdir, os.link, os.unlink))


class RouteAttestationFileError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def read_bounded_file(path: str | Path, *, max_bytes: int, label: str) -> bytes:
    """Read one regular file through a descriptor-pinned, no-follow path."""

    source = Path(path)
    parent_descriptor: int | None = None
    descriptor: int | None = None
    try:
        parent_descriptor, name = _open_parent_directory(source, create=False)
        descriptor = os.open(name, _READ_FLAGS, dir_fd=parent_descriptor)
    except (OSError, ValueError) as exc:
        raise RouteAttestationFileError(
            "DPONE_ROUTE_ATTESTATION_INPUT_UNAVAILABLE",
            f"{label} could not be opened as a regular local file.",
        ) from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > max_bytes:
            raise RouteAttestationFileError(
                "DPONE_ROUTE_ATTESTATION_INPUT_INVALID",
                f"{label} must be a regular file no larger than {max_bytes} bytes.",
            )
        data = bytearray()
        while len(data) <= max_bytes:
            chunk = os.read(descriptor, min(64 * 1024, max_bytes + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
        if len(data) > max_bytes:
            raise RouteAttestationFileError(
                "DPONE_ROUTE_ATTESTATION_INPUT_INVALID",
                f"{label} exceeds the allowed size.",
            )
        try:
            _require_stable_path(source, parent_descriptor=parent_descriptor, file_descriptor=descriptor)
        except (OSError, ValueError) as exc:
            raise RouteAttestationFileError(
                "DPONE_ROUTE_ATTESTATION_INPUT_UNAVAILABLE",
                f"{label} changed while it was being read.",
            ) from exc
        return bytes(data)
    finally:
        os.close(descriptor)
        os.close(parent_descriptor)


def read_strict_json_mapping(path: str | Path, *, max_bytes: int, label: str) -> tuple[dict[str, Any], bytes]:
    """Read and parse one JSON object while rejecting duplicate keys."""

    raw = read_bounded_file(path, max_bytes=max_bytes, label=label)

    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
        raise RouteAttestationFileError(
            "DPONE_ROUTE_ATTESTATION_INPUT_INVALID",
            f"{label} must be a UTF-8 JSON object without duplicate keys.",
        ) from exc
    if not isinstance(payload, dict):
        raise RouteAttestationFileError(
            "DPONE_ROUTE_ATTESTATION_INPUT_INVALID",
            f"{label} must contain one JSON object.",
        )
    return payload, raw


def write_create_only(path: str | Path, data: bytes) -> Path:
    """Atomically publish exact bytes through one pinned parent descriptor."""

    output = Path(path)
    parent_descriptor: int | None = None
    temporary_name: str | None = None
    temporary_descriptor: int | None = None
    published = False
    try:
        parent_descriptor, output_name = _open_parent_directory(output, create=True)
        temporary_name, temporary_descriptor = _open_temporary(parent_descriptor, output_name=output_name)
        _write_all(temporary_descriptor, data)
        os.fchmod(temporary_descriptor, 0o644)
        os.fsync(temporary_descriptor)
        os.close(temporary_descriptor)
        temporary_descriptor = None
        os.link(
            temporary_name,
            output_name,
            src_dir_fd=parent_descriptor,
            dst_dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        published = True
        os.fsync(parent_descriptor)
        _require_stable_path(output, parent_descriptor=parent_descriptor)
    except FileExistsError:
        raise
    except (OSError, ValueError) as exc:
        if published and parent_descriptor is not None:
            _unlink_quietly(output.name, dir_fd=parent_descriptor)
        raise RouteAttestationFileError(
            "DPONE_ROUTE_ATTESTATION_OUTPUT_UNSAFE",
            "Route-attestation output could not be published through a trusted local path.",
        ) from exc
    finally:
        if temporary_descriptor is not None:
            os.close(temporary_descriptor)
        if temporary_name is not None and parent_descriptor is not None:
            _unlink_quietly(temporary_name, dir_fd=parent_descriptor)
        if parent_descriptor is not None:
            os.close(parent_descriptor)
    return output


def json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"unsupported JSON constant: {value}")


def _open_parent_directory(path: Path, *, create: bool) -> tuple[int, str]:
    if _NOFOLLOW == 0 or not _DIR_FD_SUPPORTED:
        raise OSError("descriptor-relative no-follow file I/O is unavailable")
    parts = _safe_parts(path)
    descriptor = os.open(path.anchor or ".", _DIRECTORY_FLAGS)
    try:
        for part in parts[:-1]:
            try:
                child = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                try:
                    os.mkdir(part, mode=0o755, dir_fd=descriptor)
                except FileExistsError:
                    pass
                os.fsync(descriptor)
                child = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor, parts[-1]
    except BaseException:
        os.close(descriptor)
        raise


def _safe_parts(path: Path) -> tuple[str, ...]:
    parts = path.parts[1:] if path.is_absolute() else path.parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("unsafe local path")
    return tuple(parts)


def _open_temporary(parent_descriptor: int, *, output_name: str) -> tuple[str, int]:
    for _ in range(_TEMP_ATTEMPTS):
        name = f".{output_name}.{secrets.token_hex(16)}.tmp"
        try:
            return name, os.open(name, _CREATE_FLAGS, 0o600, dir_fd=parent_descriptor)
        except FileExistsError:
            continue
    raise OSError("could not allocate a private temporary output")


def _require_stable_path(
    path: Path,
    *,
    parent_descriptor: int,
    file_descriptor: int | None = None,
) -> None:
    reopened_parent, name = _open_parent_directory(path, create=False)
    pinned_file: int | None = None
    reopened_file: int | None = None
    try:
        if _identity(os.fstat(reopened_parent)) != _identity(os.fstat(parent_descriptor)):
            raise OSError("parent directory changed")
        pinned_file = file_descriptor
        if pinned_file is None:
            pinned_file = os.open(name, _READ_FLAGS, dir_fd=parent_descriptor)
        reopened_file = os.open(name, _READ_FLAGS, dir_fd=reopened_parent)
        if _identity(os.fstat(reopened_file)) != _identity(os.fstat(pinned_file)):
            raise OSError("file changed")
    finally:
        if reopened_file is not None:
            os.close(reopened_file)
        if file_descriptor is None and pinned_file is not None:
            os.close(pinned_file)
        os.close(reopened_parent)


def _identity(metadata: os.stat_result) -> tuple[int, int]:
    return metadata.st_dev, metadata.st_ino


def _write_all(descriptor: int, data: bytes) -> None:
    view = memoryview(data)
    written = 0
    while written < len(view):
        count = os.write(descriptor, view[written:])
        if count <= 0:
            raise OSError("short write")
        written += count


def _unlink_quietly(name: str, *, dir_fd: int) -> None:
    try:
        os.unlink(name, dir_fd=dir_fd)
    except FileNotFoundError:
        return


__all__ = [
    "RouteAttestationFileError",
    "json_bytes",
    "read_bounded_file",
    "read_strict_json_mapping",
    "write_create_only",
]

"""Shared no-follow filesystem primitives for dbt project bundles."""

from __future__ import annotations

import os
import stat
import unicodedata
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.manifest.bounded_yaml import (
    BoundedYamlError,
    BoundedYamlLimits,
    load_bounded_yaml,
)

DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
READ_BYTES = 1024 * 1024


def open_directory(path: Path, *, code: str = "DPONE_DBT_BUNDLE_INVALID") -> int:
    try:
        return os.open(path, DIRECTORY_FLAGS)
    except OSError as exc:
        raise DbtPublishingError(code, "dbt project directory is unsafe") from exc


def open_at(parent: int, name: str, *, directory: bool) -> int:
    try:
        descriptor = os.open(name, DIRECTORY_FLAGS if directory else FILE_FLAGS, dir_fd=parent)
        mode = os.fstat(descriptor).st_mode
        if (directory and stat.S_ISDIR(mode)) or (not directory and stat.S_ISREG(mode)):
            return descriptor
        os.close(descriptor)
        raise OSError
    except OSError as exc:
        raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project entry is unsafe") from exc


def open_parent_at(root_descriptor: int, parts: tuple[str, ...], *, create: bool) -> int:
    current = os.dup(root_descriptor)
    try:
        for part in parts:
            if create:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=current)
                except FileExistsError:
                    pass
            child = open_at(current, part, directory=True)
            os.close(current)
            current = child
        return current
    except BaseException:
        os.close(current)
        raise


def open_relative_file(root_descriptor: int, relative: PurePosixPath) -> int:
    parent = open_parent_at(root_descriptor, relative.parts[:-1], create=False)
    try:
        return open_at(parent, relative.name, directory=False)
    finally:
        os.close(parent)


def root_regular_file(root_descriptor: int, name: str) -> bool:
    """Return whether one root entry is a regular file; reject unsafe entries."""

    try:
        metadata = os.stat(name, dir_fd=root_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise DbtPublishingError(
            "DPONE_DBT_BUNDLE_INVALID",
            "dbt project metadata cannot be inspected safely",
        ) from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise DbtPublishingError(
            "DPONE_DBT_BUNDLE_INVALID",
            "dbt project metadata entry is not a regular file",
        )
    return True


def read_root_yaml_mapping(
    root_descriptor: int,
    name: str,
    *,
    maximum: int,
    error_code: str,
) -> Mapping[str, Any]:
    """Read one stable bounded root YAML mapping without following links."""

    content = _read_root_file_bytes(
        root_descriptor,
        name,
        maximum=maximum,
    )
    try:
        payload = yaml.safe_load(content.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise DbtPublishingError(error_code, f"{name} is invalid") from exc
    if not isinstance(payload, Mapping):
        raise DbtPublishingError(error_code, f"{name} must contain a mapping")
    return payload


def read_root_bounded_yaml_mapping(
    root_descriptor: int,
    name: str,
    *,
    maximum: int,
    error_code: str,
) -> Mapping[str, Any]:
    """Read duplicate-rejecting bounded YAML without following links."""

    content = _read_root_file_bytes(
        root_descriptor,
        name,
        maximum=maximum,
    )
    try:
        payload = load_bounded_yaml(
            content,
            limits=BoundedYamlLimits(max_bytes=maximum),
        )
    except BoundedYamlError as exc:
        raise DbtPublishingError(error_code, f"{name} is invalid: {exc.code}") from exc
    if not isinstance(payload, Mapping):
        raise DbtPublishingError(error_code, f"{name} must contain a mapping")
    return payload


def _read_root_file_bytes(
    root_descriptor: int,
    name: str,
    *,
    maximum: int,
) -> bytes:
    descriptor = open_at(root_descriptor, name, directory=False)
    try:
        metadata = os.fstat(descriptor)
        if metadata.st_size > maximum:
            raise limit_error("dbt project file exceeds the per-file byte limit")
        chunks: list[bytes] = []
        observed = 0
        while chunk := os.read(descriptor, READ_BYTES):
            observed += len(chunk)
            if observed > metadata.st_size:
                raise source_changed()
            chunks.append(chunk)
        if observed != metadata.st_size or identity(os.fstat(descriptor)) != identity(metadata):
            raise source_changed()
    finally:
        os.close(descriptor)
    return b"".join(chunks)


def resolved_package_names(
    root_descriptor: int,
    relative: PurePosixPath,
) -> tuple[str, ...]:
    """Return exact resolved package directories containing a dbt project file."""

    current = os.dup(root_descriptor)
    try:
        for part in relative.parts:
            try:
                metadata = os.stat(part, dir_fd=current, follow_symlinks=False)
            except FileNotFoundError:
                return ()
            if not stat.S_ISDIR(metadata.st_mode):
                raise DbtPublishingError(
                    "DPONE_DBT_BUNDLE_INVALID",
                    "dbt project path is not a safe directory",
                )
            child = open_at(current, part, directory=True)
            os.close(current)
            current = child
        names: list[str] = []
        for entry in sorted(os.scandir(current), key=lambda item: item.name):
            metadata = entry.stat(follow_symlinks=False)
            if entry.name.startswith(".") or stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                raise DbtPublishingError(
                    "DPONE_DBT_PACKAGES_NOT_RESOLVED",
                    "dbt packages tree contains a non-package entry; run dbt deps && dbt parse",
                )
            child = open_at(current, entry.name, directory=True)
            try:
                if not root_regular_file(child, "dbt_project.yml"):
                    raise DbtPublishingError(
                        "DPONE_DBT_PACKAGES_NOT_RESOLVED",
                        "dbt package is incomplete; run dbt deps && dbt parse",
                    )
            finally:
                os.close(child)
            names.append(entry.name)
        return tuple(names)
    except OSError as exc:
        raise DbtPublishingError(
            "DPONE_DBT_BUNDLE_INVALID",
            "dbt project directory cannot be inspected safely",
        ) from exc
    finally:
        os.close(current)


def identity(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns


def path_matches(path: Path, expected: tuple[int, int, int, int, int]) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    return stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode) and identity(metadata) == expected


def require_collision_free(paths: list[str]) -> None:
    normalized = [unicodedata.normalize("NFC", path).casefold() for path in paths]
    if len(normalized) != len(set(normalized)):
        raise DbtPublishingError("DPONE_DBT_BUNDLE_INVALID", "dbt project contains colliding paths")


def limit_error(message: str) -> DbtPublishingError:
    return DbtPublishingError("DPONE_DBT_BUNDLE_LIMIT_EXCEEDED", message)


def source_changed() -> DbtPublishingError:
    return DbtPublishingError("DPONE_DBT_SOURCE_CHANGED", "dbt project changed while its bundle was built")


__all__ = [
    "DIRECTORY_FLAGS",
    "FILE_FLAGS",
    "READ_BYTES",
    "identity",
    "limit_error",
    "open_at",
    "open_directory",
    "open_parent_at",
    "open_relative_file",
    "path_matches",
    "read_root_bounded_yaml_mapping",
    "read_root_yaml_mapping",
    "require_collision_free",
    "resolved_package_names",
    "root_regular_file",
    "source_changed",
]

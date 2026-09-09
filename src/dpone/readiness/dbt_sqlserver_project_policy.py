"""Bounded build-plane acquisition for SQL Server dbt project policy."""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.adapters.dbt_sqlserver_project_policy import (
    MAX_DBT_SQLSERVER_PROJECT_YAML_BYTES,
    sqlserver_project_policy_input_issue,
    validate_sqlserver_project_policy,
)
from dpone.manifest.bounded_yaml import (
    BoundedYamlError,
    BoundedYamlLimits,
    load_bounded_yaml,
)

if TYPE_CHECKING:
    from dpone.contracts.dbt_publish_models import DbtPublishIssue

_PROJECT_FILE = "dbt_project.yml"
_READ_CHUNK_BYTES = 64 * 1024
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)


class DbtSqlserverProjectPolicyValidator:
    """Read and validate the exact authoring project before publication."""

    def __init__(
        self,
        *,
        project_root: Path | None = None,
        limits: BoundedYamlLimits = BoundedYamlLimits(
            max_bytes=MAX_DBT_SQLSERVER_PROJECT_YAML_BYTES,
        ),
    ) -> None:
        self._project_root = project_root.absolute() if project_root is not None else None
        self._limits = limits

    def validate_manifest(
        self,
        manifest_path: str | Path,
    ) -> tuple[DbtPublishIssue, ...]:
        root = self._project_root or _discover_project_root(Path(manifest_path))
        return self.validate_root(root)

    def validate_root(
        self,
        project_root: Path,
    ) -> tuple[DbtPublishIssue, ...]:
        project_file = project_root.absolute() / _PROJECT_FILE
        try:
            content = _read_project_file(project_root.absolute(), maximum=self._limits.max_bytes)
            payload = load_bounded_yaml(content, limits=self._limits)
        except BoundedYamlError as exc:
            return (
                sqlserver_project_policy_input_issue(
                    exc.code,
                    project_file=project_file.as_posix(),
                ),
            )
        except (FileNotFoundError, OSError):
            return (
                sqlserver_project_policy_input_issue(
                    "unsafe_or_missing_file",
                    project_file=project_file.as_posix(),
                ),
            )
        if not isinstance(payload, Mapping):
            return (
                sqlserver_project_policy_input_issue(
                    "root_mapping_required",
                    project_file=project_file.as_posix(),
                ),
            )
        return validate_sqlserver_project_policy(
            payload,
            project_file=project_file.as_posix(),
        ).issues


def _discover_project_root(manifest_path: Path) -> Path:
    current = manifest_path.absolute().parent
    for candidate in (current, *current.parents):
        project_file = candidate / _PROJECT_FILE
        try:
            metadata = project_file.lstat()
        except OSError:
            continue
        if stat.S_ISREG(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
            return candidate
    return current


def _read_project_file(project_root: Path, *, maximum: int) -> bytes:
    root_descriptor = os.open(project_root, _DIRECTORY_FLAGS)
    try:
        descriptor = os.open(_PROJECT_FILE, _FILE_FLAGS, dir_fd=root_descriptor)
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
                raise OSError("unsafe or oversized dbt project file")
            chunks: list[bytes] = []
            observed = 0
            while chunk := os.read(descriptor, _READ_CHUNK_BYTES):
                observed += len(chunk)
                if observed > before.st_size or observed > maximum:
                    raise OSError("dbt project file changed while being read")
                chunks.append(chunk)
            after = os.fstat(descriptor)
            if observed != before.st_size or _identity(after) != _identity(before):
                raise OSError("dbt project file changed while being read")
            return b"".join(chunks)
        finally:
            os.close(descriptor)
    finally:
        os.close(root_descriptor)


def _identity(metadata: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


__all__ = ["DbtSqlserverProjectPolicyValidator"]

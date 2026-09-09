"""Bounded, descriptor-confined discovery without dbt, secrets or policy loading."""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from dpone.contracts.dbt_publish_models import DbtPublishIssue
from dpone.contracts.dbt_workspace import DbtWorkspaceDiscoveryReport, DbtWorkspaceProject
from dpone.contracts.dbt_workspace_paths import (
    validate_dbt_project_layout,
    validate_dbt_project_name,
    validate_dbt_project_path,
)
from dpone.manifest.bounded_yaml import load_bounded_yaml
from dpone.manifest.confined_files import read_confined_leaf
from dpone.manifest.dbt_publish_profiles import DEFAULT_PROFILE_PATHS

_IGNORED = frozenset(
    {".git", ".worktrees", ".venv", "venv", "node_modules", "target", "logs", "dbt_packages", ".dpone-cache", ".ci"}
)
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
_OVERRIDE = "DPONE_DBT_PUBLISH_PROFILES"


@dataclass(slots=True)
class _Scan:
    entries: int = 0
    projects: list[DbtWorkspaceProject] = field(default_factory=list)
    issues: list[DbtPublishIssue] = field(default_factory=list)


class _LimitExceeded(ValueError):
    pass


class DbtWorkspaceDiscovery:
    """Discover a complete bounded inventory with an explicitly injected environment.

    No policy contents, profiles.yml, manifest or SQL files are opened. Each
    directory is enumerated through an open no-follow descriptor; bounds apply
    while enumerating, before sorting or descending. Failures invalidate the
    whole inventory but retain already observed rows for diagnostics.
    """

    def __init__(
        self, *, environment: Mapping[str, str], max_entries: int = 100_000, max_projects: int = 64, max_depth: int = 32
    ) -> None:
        for value, maximum in ((max_entries, 100_000), (max_projects, 64), (max_depth, 32)):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError("discovery limits must be positive and cannot exceed production bounds")
        self._has_override = _OVERRIDE in environment
        self._max_entries, self._max_projects, self._max_depth = max_entries, max_projects, max_depth

    def discover(self, root: Path) -> DbtWorkspaceDiscoveryReport:
        if self._has_override:
            return DbtWorkspaceDiscoveryReport(
                blockers=(
                    _issue(
                        "DPONE_DBT_WORKSPACE_PROFILE_OVERRIDE",
                        ".",
                        "Workspace publishing cannot inherit a global profile override",
                        "Unset DPONE_DBT_PUBLISH_PROFILES; use one standard policy inside each project.",
                    ),
                )
            )
        scan = _Scan()
        try:
            descriptor = os.open(root.absolute(), _DIRECTORY_FLAGS)
            try:
                self._walk(descriptor, ".", 0, (), scan)
            finally:
                os.close(descriptor)
        except _LimitExceeded:
            scan.issues.append(
                _issue(
                    "DPONE_DBT_WORKSPACE_DISCOVERY_LIMIT",
                    ".",
                    "Workspace exceeds the entry, project or directory-depth bound",
                    "Choose the smallest complete workspace root; exclude generated dependencies using standard dbt paths.",
                )
            )
        except (OSError, ValueError):
            scan.issues.append(_invalid(".", "Workspace root cannot be read safely"))
        projects = tuple(sorted(scan.projects, key=lambda row: row.project_path))
        try:
            validate_dbt_project_layout(
                tuple((row.project_path, row.project_name) for row in projects if row.project_name is not None)
            )
        except ValueError as exc:
            scan.issues.append(
                _issue(
                    "DPONE_DBT_WORKSPACE_IDENTITY_COLLISION",
                    ".",
                    str(exc),
                    "Keep distinct project names and nonoverlapping roots; dependency projects belong in packages-install-path.",
                )
            )
        return DbtWorkspaceDiscoveryReport(projects=projects, blockers=tuple(scan.issues))

    def _walk(self, descriptor: int, relative: str, depth: int, excluded: tuple[str, ...], scan: _Scan) -> None:
        entries: list[tuple[str, int]] = []
        with os.scandir(descriptor) as stream:
            for entry in stream:
                scan.entries += 1
                if scan.entries > self._max_entries:
                    raise _LimitExceeded
                entries.append((entry.name, entry.stat(follow_symlinks=False).st_mode))
        if any(name == "dbt_project.yml" for name, _ in entries):
            if len(scan.projects) >= self._max_projects:
                raise _LimitExceeded
            try:
                row, generated = _read_project(descriptor, relative)
                excluded += tuple(_join(relative, path) for path in generated)
            except (OSError, ValueError):
                row = DbtWorkspaceProject(relative, None, False, None, None, "invalid")
                scan.issues.append(_invalid(relative, "Project metadata or local policy path is invalid or ambiguous"))
            scan.projects.append(row)
        for name, mode in sorted(entries):
            path = _join(relative, name)
            if name.casefold() in _IGNORED or path in excluded:
                continue
            if stat.S_ISLNK(mode):
                scan.issues.append(_invalid(path, "Non-ignored symlinks are not supported in a workspace"))
            elif stat.S_ISDIR(mode):
                if depth >= self._max_depth:
                    raise _LimitExceeded
                try:
                    child = os.open(name, _DIRECTORY_FLAGS, dir_fd=descriptor)
                    try:
                        self._walk(child, path, depth + 1, excluded, scan)
                    finally:
                        os.close(child)
                except _LimitExceeded:
                    raise
                except OSError:
                    scan.issues.append(_invalid(path, "Directory changed or cannot be read safely"))


def _read_project(descriptor: int, relative: str) -> tuple[DbtWorkspaceProject, tuple[str, str]]:
    validate_dbt_project_path(relative)
    raw = read_confined_leaf(descriptor, "dbt_project.yml", max_bytes=1024 * 1024).content
    payload = load_bounded_yaml(raw)
    if not isinstance(payload, Mapping):
        raise ValueError("project metadata must be a mapping")
    name = payload.get("name")
    if not isinstance(name, str):
        raise ValueError("project name must be literal text")
    validate_dbt_project_name(name)
    packages = _configured_path(payload.get("packages-install-path", "dbt_packages"))
    target = _configured_path(payload.get("target-path", "target"))
    policies = tuple(path for path in DEFAULT_PROFILE_PATHS if _regular_policy(descriptor, path))
    if len(policies) > 1:
        raise ValueError("multiple project-local policy files")
    return DbtWorkspaceProject(
        relative,
        name,
        bool(policies),
        policies[0] if policies else None,
        f"{target}/manifest.json",
        "policy_present" if policies else "not_configured",
    ), (packages, target)


def _regular_policy(descriptor: int, path: str) -> bool:
    parent, leaf = PurePosixPath(path).parts
    try:
        child = os.open(parent, _DIRECTORY_FLAGS, dir_fd=descriptor)
    except FileNotFoundError:
        return False
    try:
        try:
            metadata = os.stat(leaf, dir_fd=child, follow_symlinks=False)
        except FileNotFoundError:
            return False
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("publishing policy must be a regular file")
        return True
    finally:
        os.close(child)


def _configured_path(value: object) -> str:
    if not isinstance(value, str) or value == "." or any(char in value for char in "{}$"):
        raise ValueError("generated path must be a literal relative path")
    validate_dbt_project_path(value)
    return value


def _join(parent: str, child: str) -> str:
    return child if parent == "." else f"{parent}/{child}"


def _invalid(path: str, message: str) -> DbtPublishIssue:
    return _issue(
        "DPONE_DBT_WORKSPACE_DISCOVERY_INVALID",
        path,
        message,
        "Use regular project files, a literal dbt name and confined generated paths; keep exactly one local publishing policy.",
    )


def _issue(code: str, path: str, message: str, remediation: str) -> DbtPublishIssue:
    return DbtPublishIssue(code=code, path=path, message=message, remediation=remediation)

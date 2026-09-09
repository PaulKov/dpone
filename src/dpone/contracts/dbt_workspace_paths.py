"""Portable project identities shared by discovery and signed source inventories."""

from __future__ import annotations

import re
import unicodedata
from pathlib import PurePosixPath

DBT_PROJECT_NAME_PATTERN = r"[A-Za-z_][A-Za-z0-9_]{0,255}"
_PROJECT_NAME = re.compile(DBT_PROJECT_NAME_PATTERN)


def validate_dbt_project_name(value: str) -> None:
    if not isinstance(value, str) or _PROJECT_NAME.fullmatch(value) is None:
        raise ValueError("dbt project name is invalid")


def validate_dbt_project_path(value: str) -> None:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 1024:
        raise ValueError("dbt project path is invalid")
    if value == ".":
        return
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or path.as_posix() != value
        or not path.parts
        or len(path.parts) > 32
        or any(part.casefold() in {".", "..", ".git", ".worktrees"} for part in path.parts)
        or "\\" in value
        or unicodedata.normalize("NFC", value) != value
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError("dbt project path is unsafe or noncanonical")


def validate_dbt_project_layout(projects: tuple[tuple[str, str], ...]) -> None:
    """Reject ambiguous names/roots without auto-renaming or reordering inputs."""

    dbt_workspace_directories(tuple(path for path, _ in projects))
    seen_names: dict[str, str] = {}
    seen_paths: list[str] = []
    for path, name in projects:
        validate_dbt_project_path(path)
        validate_dbt_project_name(name)
        if name.casefold() in seen_names:
            raise ValueError(f"Project name collision: {seen_names[name.casefold()]} and {path}")
        for previous in seen_paths:
            left, right = previous.casefold(), path.casefold()
            if left == right or "." in (left, right) or left.startswith(right + "/") or right.startswith(left + "/"):
                raise ValueError(f"Project roots overlap: {previous} and {path}")
        seen_names[name.casefold()] = path
        seen_paths.append(path)


def dbt_workspace_directories(project_paths: tuple[str, ...]) -> frozenset[str]:
    """Include structural parents and reject case aliases before building a tree."""

    directories = {
        parent.as_posix() for path in project_paths for parent in (PurePosixPath(path), *PurePosixPath(path).parents)
    }
    if len({path.casefold() for path in directories}) != len(directories):
        raise ValueError("workspace directory paths have portable case collisions")
    return frozenset(directories)

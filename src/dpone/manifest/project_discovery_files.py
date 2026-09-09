"""Confined file measurements used by project discovery."""

from __future__ import annotations

from pathlib import Path

from dpone.manifest.confined_files import ConfinedFileError, project_relative_path, sha256_confined_file
from dpone.manifest.project_selection_contracts import ProjectCheckedSource


def confined_size(root: Path, relative_path: str) -> int | None:
    """Measure one previously confined input for aggregate discovery budgets."""

    try:
        safe = project_relative_path(root, Path(relative_path))
        return (root / safe).stat(follow_symlinks=False).st_size
    except (ConfinedFileError, OSError):
        return None


def relative_label(root: Path, path: Path) -> str:
    """Return one confined project label for structured discovery issues."""

    return project_relative_path(root, path)


def checked_inputs(checked: ProjectCheckedSource) -> tuple[tuple[str, str], ...]:
    """Return the pinned primary source and compiler dependencies."""

    return (
        (checked.source_label, checked.source_sha256),
        *((dependency.path, dependency.sha256) for dependency in checked.compilation.dependencies),
    )


def digest_matches(root: Path, path: str, expected: str) -> bool:
    """Compare one confined regular file with its expected digest."""

    try:
        return sha256_confined_file(root, path) == expected
    except (ConfinedFileError, OSError):
        return False


__all__ = ["checked_inputs", "confined_size", "digest_matches", "relative_label"]

"""Project-confined path policy for Studio query operations."""

from __future__ import annotations

from pathlib import Path

from dpone.manifest.confined_files import ConfinedFileError, project_relative_path
from dpone.readiness.studio_errors import StudioError


class StudioProjectPathPolicy:
    """Normalize project paths while rejecting traversal and symlink escape."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve(strict=True)

    def relative(self, value: str) -> str:
        if value.startswith("-"):
            raise StudioError(
                "DPONE_STUDIO_PATH_UNSAFE",
                "Studio paths cannot begin with an option prefix.",
            )
        candidate = Path(value)
        if not candidate.is_absolute():
            candidate = self._root / candidate
        try:
            relative = project_relative_path(self._root, candidate)
        except ConfinedFileError as exc:
            raise StudioError(
                "DPONE_STUDIO_PATH_UNSAFE",
                "Studio paths must stay inside the project root.",
            ) from exc
        if _path_has_symlink_component(self._root, relative):
            raise StudioError(
                "DPONE_STUDIO_PATH_UNSAFE",
                "Studio paths cannot traverse symlinks.",
            )
        return relative


def _path_has_symlink_component(root: Path, relative: str) -> bool:
    current = root
    for part in Path(relative).parts:
        current /= part
        if current.is_symlink():
            return True
    return False


__all__ = ["StudioProjectPathPolicy"]

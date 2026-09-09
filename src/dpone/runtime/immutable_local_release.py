"""Compatibility wrapper for atomic content-addressed local release trees."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dpone.runtime.immutable_local_tree import ImmutableLocalTreeError, materialize_immutable_local_tree


class ImmutableLocalReleaseError(RuntimeError):
    """Raised when an existing release differs from the requested content."""

    def __init__(self, message: str, *, path: Path) -> None:
        super().__init__(message)
        self.path = path


def materialize_immutable_local_release(release_dir: Path, files: Mapping[str, bytes]) -> str:
    """Publish a complete release once, or prove that its existing tree is equal."""

    cache_root = release_dir.parent.parent
    if release_dir.parent.name != "releases":
        raise ValueError("local release must use <cache-root>/releases/<release-id>")
    try:
        return materialize_immutable_local_tree(
            release_dir,
            files,
            allowed_parent=release_dir.parent,
            root=cache_root,
        )
    except ImmutableLocalTreeError as exc:
        raise ImmutableLocalReleaseError(
            str(exc).replace("immutable tree", "immutable release"), path=exc.path
        ) from exc


__all__ = ["ImmutableLocalReleaseError", "materialize_immutable_local_release"]

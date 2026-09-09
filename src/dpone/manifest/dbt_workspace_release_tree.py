"""Exact canonical workspace release closure, independent of checksum generation."""

from __future__ import annotations

import os
import stat
from collections.abc import Callable, Mapping
from pathlib import Path, PurePosixPath

from dpone.contracts.dbt_runtime_release_binding import DbtReleaseArtifactIndex
from dpone.manifest.confined_files import read_confined_file

_SUBJECT = "release-subjects.sha256"
_SIDECARS = frozenset({"release-set.json", "_dbt/dbt-source-snapshot.json"})


def canonical_dbt_workspace_descriptors(release: Mapping[str, object]) -> dict[str, Mapping[str, object]]:
    """Compatibility projection of the canonical complete metadata index."""

    return {
        path: {key: list(value) if isinstance(value, tuple) else value for key, value in row.items()}
        for path, row in DbtReleaseArtifactIndex.from_release(release).by_path.items()
    }


def verify_canonical_dbt_workspace_tree(
    root: Path,
    release: Mapping[str, object] | DbtReleaseArtifactIndex,
    *,
    read_file: Callable[..., bytes] = read_confined_file,
) -> None:
    """Reject all orphan files/directories, links, missing artifacts and byte drift.

    The fixed integrity subject may be present or absent: source verification is
    also used before generating it. Its cryptographic/integrity verification is
    a separate boundary and cannot expand this exact canonical inventory.
    """

    index = release if isinstance(release, DbtReleaseArtifactIndex) else DbtReleaseArtifactIndex.from_release(release)
    descriptors = index.by_path
    expected = set(descriptors) | _SIDECARS
    _verify_entries(root, expected)
    for path, descriptor in descriptors.items():
        size = descriptor["bytes"]
        assert isinstance(size, int)
        body = read_file(root, path, max_bytes=size)
        index.require_bytes(path, body)


def _verify_entries(root: Path, expected: set[str]) -> None:
    directories = {
        parent.as_posix() for path in expected for parent in PurePosixPath(path).parents if parent != PurePosixPath(".")
    }
    found: set[str] = set()
    visited: set[str] = set()
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW

    def walk(descriptor: int, prefix: str) -> None:
        with os.scandir(descriptor) as entries:
            for entry in entries:
                path = f"{prefix}/{entry.name}" if prefix else entry.name
                metadata = entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(metadata.st_mode):
                    if path not in directories:
                        raise ValueError("workspace release contains an unreferenced directory")
                    child = os.open(entry.name, flags, dir_fd=descriptor)
                    try:
                        walk(child, path)
                    finally:
                        os.close(child)
                    visited.add(path)
                elif stat.S_ISREG(metadata.st_mode):
                    if path not in expected and path != _SUBJECT:
                        raise ValueError("workspace release contains an unreferenced file")
                    if path in expected:
                        found.add(path)
                else:
                    raise ValueError("workspace release contains a link or special file")

    descriptor = os.open(root, flags)
    try:
        walk(descriptor, "")
    finally:
        os.close(descriptor)
    if found != expected or visited != directories:
        raise ValueError("workspace release canonical tree is incomplete")


__all__ = ["canonical_dbt_workspace_descriptors", "verify_canonical_dbt_workspace_tree"]

"""Canonical locator rules for artifacts owned by a dpone release-set."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Any

RELEASE_ARTIFACT_PATH_PATTERN = r"^(?!/)(?!.*//)(?!.*(?:^|/)\.{1,2}(?:/|$))(?!.*[\u0000-\u0020\u007f\\:])[^\s\\:]+$"


class ReleaseArtifactLocatorError(ValueError):
    """Raised when a release artifact has an ambiguous or unsafe locator."""


def release_artifact_path(item: Mapping[str, Any]) -> PurePosixPath:
    """Return the one traversal-free relative path declared by a release artifact.

    ``path`` is canonical. ``artifact_ref`` remains accepted as a legacy field,
    but its value follows the same relative-path grammar. Cache URIs belong to
    deployment indexes because release-set identities cannot refer to themselves.
    """

    has_path = "path" in item
    has_legacy_path = "artifact_ref" in item
    if has_path == has_legacy_path:
        raise ReleaseArtifactLocatorError("release artifact must contain exactly one of path or artifact_ref")
    path = _required_text(item.get("path")) if has_path else None
    legacy_path = _required_text(item.get("artifact_ref")) if has_legacy_path else None
    value = path or legacy_path
    assert value is not None
    raw_parts = value.split("/")
    candidate = PurePosixPath(value)
    if (
        candidate.is_absolute()
        or any(part in {"", ".", ".."} for part in raw_parts)
        or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value)
        or "\\" in value
        or ":" in value
        or candidate.as_posix() != value
    ):
        raise ReleaseArtifactLocatorError("release artifact locator must be a traversal-free relative POSIX path")
    return candidate


def _required_text(value: object) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ReleaseArtifactLocatorError("release artifact locator must be a non-empty canonical string")
    return value


__all__ = ["RELEASE_ARTIFACT_PATH_PATTERN", "ReleaseArtifactLocatorError", "release_artifact_path"]

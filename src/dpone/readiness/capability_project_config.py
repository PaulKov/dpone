"""Bounded project-owned configuration for capability evidence discovery."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from dpone.manifest.project_config import ProjectConfigError, ProjectConfigReader

_CONFIG_KEY = "capability_discovery"
_EVIDENCE_KEY = "certification_evidence"
_EVIDENCE_KEYS = frozenset(
    {
        "matrix_path",
        "expected_commit",
        "evidence_dirs",
        "max_age_hours",
    }
)
_REQUIRED_EVIDENCE_KEYS = frozenset(
    {
        "matrix_path",
        "expected_commit",
        "evidence_dirs",
    }
)
_MAX_EVIDENCE_DIRECTORIES = 128


@dataclass(frozen=True, slots=True)
class CapabilityEvidenceSettings:
    """Explicit project-relative inputs used to revalidate route evidence."""

    matrix_path: Path
    expected_commit: str
    evidence_dirs: tuple[Path, ...]
    max_age_hours: int


class CapabilityEvidenceConfigError(ValueError):
    """Safe failure for one malformed project evidence authority."""


def load_capability_evidence_settings(
    root: Path,
) -> CapabilityEvidenceSettings | None:
    """Read one optional config section without discovering evidence files."""

    try:
        snapshot = ProjectConfigReader(root).read(required=False)
    except ProjectConfigError as exc:
        raise CapabilityEvidenceConfigError from exc
    if snapshot is None:
        return None
    discovery = snapshot.payload.get(_CONFIG_KEY)
    if discovery is None:
        return None
    if not isinstance(discovery, Mapping):
        raise CapabilityEvidenceConfigError
    raw = discovery.get(_EVIDENCE_KEY)
    if raw is None:
        return None
    if not isinstance(raw, Mapping) or set(raw) - _EVIDENCE_KEYS or not _REQUIRED_EVIDENCE_KEYS.issubset(raw):
        raise CapabilityEvidenceConfigError
    matrix_path = _relative_path(raw.get("matrix_path"))
    expected_commit = raw.get("expected_commit")
    evidence_dirs = _relative_paths(raw.get("evidence_dirs"))
    max_age_hours = raw.get("max_age_hours", 168)
    if (
        matrix_path is None
        or not isinstance(expected_commit, str)
        or not expected_commit
        or evidence_dirs is None
        or isinstance(max_age_hours, bool)
        or not isinstance(max_age_hours, int)
        or not 1 <= max_age_hours <= 8760
    ):
        raise CapabilityEvidenceConfigError
    return CapabilityEvidenceSettings(
        matrix_path=Path(matrix_path),
        expected_commit=expected_commit,
        evidence_dirs=tuple(Path(item) for item in evidence_dirs),
        max_age_hours=max_age_hours,
    )


def _relative_paths(raw: object) -> tuple[str, ...] | None:
    if not isinstance(raw, list) or len(raw) > _MAX_EVIDENCE_DIRECTORIES:
        return None
    paths = tuple(_relative_path(item) for item in raw)
    if any(item is None for item in paths) or len(paths) != len(set(paths)):
        return None
    return tuple(item for item in paths if item is not None)


def _relative_path(raw: object) -> str | None:
    if not isinstance(raw, str) or not raw or "\\" in raw:
        return None
    path = PurePosixPath(raw)
    if path.is_absolute() or not path.parts or "." in path.parts or ".." in path.parts:
        return None
    return path.as_posix()


__all__ = [
    "CapabilityEvidenceConfigError",
    "CapabilityEvidenceSettings",
    "load_capability_evidence_settings",
]
